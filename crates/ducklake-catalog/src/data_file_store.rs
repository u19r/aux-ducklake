use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::runtime_read_context::CatalogCurrentFilePartitionValuesContext;
use crate::{
    AttachedDataFile, CatalogId, CatalogResult, DataFileChangeKind, DataFileId, DataFileRow,
    DeleteFileId, DeleteFileRow, FilePartitionValueRow, InlineTableFlush, KvBatch,
    MutableCatalogKv, OrderedCatalogKv, RangeDirection, TableId,
    conflict::write_data_file_change,
    conflict_watermarks::stage_max_file_id_watermark,
    file_partitions::{
        delete_partition_lookups_for_data_file, delete_partition_values_for_data_file,
        list_file_partition_values_for_data_files,
    },
    ids::{CatalogOrderId, incomplete_fdb_order},
    inline_data::stage_flush_inline_table_payloads,
    keys::{
        KeyFamily, current_data_file_key, current_data_file_prefix, current_delete_file_key,
        data_file_begin_key, data_file_begin_prefix, data_file_begin_scan_end, data_file_end_key,
        data_file_key, decode_order_delete_file_change_table_id, delete_file_end_key,
        delete_file_key, delete_file_timeline_key, delete_file_timeline_order_from_key,
        delete_file_timeline_prefix, delete_file_timeline_scan_end, family_prefix,
        order_delete_file_change_key, order_delete_file_change_prefix,
        order_delete_file_change_scan_end, table_delete_file_change_key,
    },
    store::{
        invalidate_runtime_read_context, latest_snapshot, snapshot_row_for_next_sequence,
        stage_snapshot,
    },
};

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_method_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_method_stage(_operation: &str, _started: RuntimeMetricStage) {}

pub fn commit_append_data_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    rows: Vec<DataFileRow>,
) -> CatalogResult<Vec<DataFileRow>> {
    commit_append_data_files_with_inline_flushes(kv, catalog, rows, &[])
}

pub fn commit_append_data_files_with_inline_flushes(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut rows: Vec<DataFileRow>,
    inline_flushes: &[InlineTableFlush],
) -> CatalogResult<Vec<DataFileRow>> {
    if rows.is_empty() && inline_flushes.is_empty() {
        return Ok(rows);
    }
    let latest = latest_snapshot(kv, catalog)?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    for row in &mut rows {
        row.validity = crate::ValidityWindow::new(order, None);
    }
    reject_current_data_file_row_id_overlaps(kv, catalog, &rows)?;
    for row in &rows {
        stage_append_data_file(kv, &mut batch, catalog, row)?;
    }
    for flush in inline_flushes {
        stage_flush_inline_table_payloads(kv, &mut batch, catalog, *flush, order)?;
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub fn append_data_file(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    row: DataFileRow,
) -> CatalogResult<DataFileRow> {
    let mut batch = KvBatch::new();
    stage_append_data_file(kv, &mut batch, catalog, &row)?;
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(row)
}

pub(crate) fn stage_append_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    stage_append_data_file_without_change(kv, batch, catalog, row)?;
    write_data_file_change(
        batch,
        catalog,
        row.table_id,
        row.validity.begin_order,
        DataFileChangeKind::Added,
        row.data_file_id,
    );
    Ok(())
}

pub(crate) fn reject_current_data_file_row_id_overlaps(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DataFileRow],
) -> CatalogResult<()> {
    reject_current_data_file_row_id_overlaps_except(kv, catalog, rows, &BTreeSet::new())
}

pub(crate) fn reject_current_data_file_row_id_overlaps_except(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DataFileRow],
    allowed_current_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<()> {
    reject_current_data_file_row_id_overlaps_except_with_latest_order(
        kv,
        catalog,
        rows,
        allowed_current_file_ids,
        None,
    )
}

pub(crate) fn reject_current_data_file_row_id_overlaps_except_with_latest_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DataFileRow],
    allowed_current_file_ids: &BTreeSet<DataFileId>,
    latest_order: Option<CatalogOrderId>,
) -> CatalogResult<()> {
    for (index, row) in rows.iter().enumerate() {
        for other in rows.iter().skip(index + 1) {
            reject_row_id_overlap(row, other)?;
        }
    }

    for (table_id, table_rows) in data_files_by_table(rows) {
        let overlap_candidates = table_rows
            .into_iter()
            .filter(|row| row_id_overlap_check_needed(row))
            .collect::<Vec<_>>();
        if overlap_candidates.is_empty() {
            continue;
        }
        for existing in
            list_current_data_files_for_mutation_validation(kv, catalog, table_id, latest_order)?
        {
            if allowed_current_file_ids.contains(&existing.data_file_id) {
                continue;
            }
            for row in &overlap_candidates {
                reject_row_id_overlap(row, &existing)?;
            }
        }
    }
    Ok(())
}

fn data_files_by_table(rows: &[DataFileRow]) -> Vec<(TableId, Vec<&DataFileRow>)> {
    let mut by_table = Vec::<(TableId, Vec<&DataFileRow>)>::new();
    let mut table_indexes = BTreeMap::<TableId, usize>::new();
    for row in rows {
        if let Some(index) = table_indexes.get(&row.table_id) {
            by_table[*index].1.push(row);
            continue;
        }
        let index = by_table.len();
        by_table.push((row.table_id, vec![row]));
        table_indexes.insert(row.table_id, index);
    }
    by_table
}

fn row_id_overlap_check_needed(row: &DataFileRow) -> bool {
    row.row_id_start_known && row.record_count > 0
}

fn reject_row_id_overlap(left: &DataFileRow, right: &DataFileRow) -> CatalogResult<()> {
    if left.table_id != right.table_id
        || !left.row_id_start_known
        || !right.row_id_start_known
        || left.record_count == 0
        || right.record_count == 0
    {
        return Ok(());
    }
    let left_end = left.row_id_start.saturating_add(left.record_count);
    let right_end = right.row_id_start.saturating_add(right.record_count);
    if left.row_id_start < right_end && right.row_id_start < left_end {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: data file {} row ids [{}..{}) overlap data file {} row ids [{}..{})",
            left.data_file_id.0,
            left.row_id_start,
            left_end,
            right.data_file_id.0,
            right.row_id_start,
            right_end
        )));
    }
    Ok(())
}

pub(crate) fn stage_append_data_file_without_change(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    batch.put(data_file_key(catalog, row.data_file_id), row.encode());
    batch.put(
        current_data_file_key(catalog, row.table_id, row.data_file_id),
        row.encode(),
    );
    batch.put(
        data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        ),
        row.encode(),
    );
    stage_max_file_id_watermark(kv, batch, catalog, row.data_file_id.0)?;
    delete_partition_values_for_data_file(kv, batch, catalog, row.data_file_id)
}

pub fn expire_data_file(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    end_order: CatalogOrderId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    let mut row = DataFileRow::decode(&value)?;
    if end_order <= row.validity.begin_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "data file {} cannot end at or before its begin order",
            data_file_id.0
        )));
    }
    row.validity.end_order = Some(end_order);

    let mut batch = KvBatch::new();
    stage_expire_data_file(kv, &mut batch, catalog, &row, end_order)?;
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(row)
}

pub(crate) fn stage_expire_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DataFileRow,
    end_order: CatalogOrderId,
) -> CatalogResult<()> {
    batch.put(data_file_key(catalog, row.data_file_id), row.encode());
    batch.put(
        data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        ),
        row.encode(),
    );
    batch.delete(current_data_file_key(
        catalog,
        row.table_id,
        row.data_file_id,
    ));
    batch.put(
        data_file_end_key(catalog, row.table_id, end_order, row.data_file_id),
        row.data_file_id.0.to_be_bytes().to_vec(),
    );
    write_data_file_change(
        batch,
        catalog,
        row.table_id,
        end_order,
        DataFileChangeKind::Removed,
        row.data_file_id,
    );
    delete_partition_lookups_for_data_file(kv, batch, catalog, row.data_file_id)?;
    Ok(())
}

pub fn register_delete_file(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut row: DeleteFileRow,
) -> CatalogResult<DeleteFileRow> {
    let mut batch = KvBatch::new();
    let close_order = row.validity.begin_order;
    stage_register_delete_file(kv, &mut batch, catalog, &mut row, close_order)?;
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(row)
}

pub fn commit_register_delete_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut rows: Vec<DeleteFileRow>,
) -> CatalogResult<Vec<DeleteFileRow>> {
    if rows.is_empty() {
        return Ok(rows);
    }
    let latest = latest_snapshot(kv, catalog)?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    for row in &mut rows {
        row.validity = crate::ValidityWindow::new(order, None);
        stage_register_delete_file(kv, &mut batch, catalog, row, order)?;
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub(crate) fn stage_register_delete_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &mut DeleteFileRow,
    close_order: CatalogOrderId,
) -> CatalogResult<()> {
    let data_file = load_data_file(kv, catalog, row.data_file_id)?;
    stage_register_delete_file_for_data_file(kv, batch, catalog, &data_file, row, close_order)
}

pub(crate) fn stage_register_delete_file_for_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file: &DataFileRow,
    row: &mut DeleteFileRow,
    close_order: CatalogOrderId,
) -> CatalogResult<()> {
    inherit_current_delete_begin_order(kv, catalog, row)?;
    stage_close_current_delete_file(kv, batch, catalog, data_file, row, close_order)?;
    batch.put(delete_file_key(catalog, row.delete_file_id), row.encode());
    batch.put(
        current_delete_file_key(catalog, row.data_file_id),
        row.encode(),
    );
    batch.put(
        delete_file_timeline_key(catalog, row.data_file_id, close_order, row.delete_file_id),
        row.encode(),
    );
    batch.put(
        table_delete_file_change_key(catalog, data_file.table_id, close_order, row.delete_file_id),
        Vec::new(),
    );
    batch.put(
        order_delete_file_change_key(catalog, close_order, data_file.table_id, row.delete_file_id),
        Vec::new(),
    );
    stage_max_file_id_watermark(kv, batch, catalog, row.delete_file_id.0)?;
    Ok(())
}

pub fn list_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return Ok(
            crate::runtime_read_context::CatalogCurrentFilesContext::for_current_files(
                kv, catalog,
            )?
            .current_data_files_for_table(table_id),
        );
    }
    let started = RuntimeMetricStage::start();
    let rows = scan_current_data_files(kv, catalog, table_id)?;
    record_runtime_method_stage("method.data_file_store.list_current_data_files", started);
    Ok(rows)
}

fn list_current_data_files_for_mutation_validation(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    _latest_order: Option<CatalogOrderId>,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return match _latest_order {
            Some(latest_order) => TableCurrentDataFilesContext::for_table_at_order(
                kv,
                catalog,
                table_id,
                latest_order,
            ),
            None => TableCurrentDataFilesContext::for_table(kv, catalog, table_id),
        }
        .map(|context| context.rows);
    }
    scan_current_data_files(kv, catalog, table_id)
}

fn scan_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &current_data_file_prefix(catalog, table_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_current_index_value(kv, catalog, &item.value)?;
        if row.validity.end_order.is_none() {
            rows.push(row);
        }
    }
    Ok(rows)
}

#[derive(Clone)]
struct TableCurrentDataFilesContext {
    rows: Vec<DataFileRow>,
}

impl TableCurrentDataFilesContext {
    fn for_table(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
    ) -> CatalogResult<Self> {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return Ok(Self { rows: Vec::new() });
        };
        Self::for_table_at_order(kv, catalog, table_id, latest.order)
    }

    fn for_table_at_order(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let key = TableCurrentDataFilesContextKey {
            catalog,
            table_id,
            latest_order,
        };
        let cache = table_current_data_files_context_cache();
        if let Some(context) = cache.get(key) {
            return Ok(context);
        }
        let context = Self {
            rows: scan_current_data_files(kv, catalog, table_id)?,
        };
        cache.insert(key, context.clone());
        Ok(context)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TableCurrentDataFilesContextKey {
    catalog: CatalogId,
    table_id: TableId,
    latest_order: CatalogOrderId,
}

pub(crate) fn list_all_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::CurrentDataFile),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_current_index_value(kv, catalog, &item.value)?;
        if row.validity.end_order.is_none() {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0));
    record_runtime_method_stage(
        "method.data_file_store.list_all_current_data_files",
        started,
    );
    Ok(rows)
}

pub fn list_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = kv
        .scan_prefix(
            &crate::keys::family_prefix(catalog, crate::keys::KeyFamily::DataFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DataFileRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.data_file_id.0);
    Ok(rows)
}

pub fn list_current_data_files_with_deletes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    attach_current_delete_files(kv, catalog, list_current_data_files(kv, catalog, table_id)?)
}

pub(crate) fn attach_current_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    if data_files.is_empty() {
        record_runtime_method_stage(
            "method.data_file_store.attach_current_delete_files",
            started,
        );
        return Ok(Vec::new());
    }
    if let Some(latest) = latest_snapshot(kv, catalog)?
        && !delete_file_changes_exist_for_files_at(kv, catalog, &data_files, latest.order)?
    {
        record_runtime_method_stage(
            "method.data_file_store.attach_current_delete_files",
            started,
        );
        return Ok(attach_data_files_without_deletes(data_files));
    }
    let keys = data_files
        .iter()
        .map(|file| current_delete_file_key(catalog, file.data_file_id))
        .collect::<Vec<_>>();
    let delete_pointers = kv.batch_get(&keys)?;
    let mut attached = Vec::with_capacity(data_files.len());
    for (data_file, pointer) in data_files.into_iter().zip(delete_pointers) {
        let delete_file = pointer
            .map(|value| current_delete_file_from_index_value(kv, catalog, &value))
            .transpose()?
            .and_then(|row| row.validity.end_order.is_none().then_some(row));
        attached.push(AttachedDataFile::new(data_file, delete_file));
    }
    record_runtime_method_stage(
        "method.data_file_store.attach_current_delete_files",
        started,
    );
    Ok(attached)
}

pub(crate) fn attach_data_files_without_deletes(
    data_files: Vec<DataFileRow>,
) -> Vec<AttachedDataFile> {
    data_files
        .into_iter()
        .map(|data_file| AttachedDataFile::new(data_file, None))
        .collect()
}

pub(crate) fn attach_delete_file_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file: DataFileRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<AttachedDataFile> {
    let started = RuntimeMetricStage::start();
    let delete_file = load_delete_file_at(kv, catalog, data_file.data_file_id, snapshot_order)?;
    record_runtime_method_stage("method.data_file_store.attach_delete_file_at", started);
    Ok(AttachedDataFile::new(data_file, delete_file))
}

pub fn list_data_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return Ok(TableDataFilesAtContext::for_table_snapshot(
            kv,
            catalog,
            table_id,
            snapshot_order,
        )?
        .visible_rows());
    }
    list_data_files_at_uncached(kv, catalog, table_id, snapshot_order)
}

fn list_data_files_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let rows = TableDataFilesAtContext::load(kv, catalog, table_id, snapshot_order)?.visible_rows();
    record_runtime_method_stage("method.data_file_store.list_data_files_at", started);
    Ok(rows)
}

#[derive(Clone)]
struct TableDataFilesAtContext {
    visible_rows: Vec<DataFileRow>,
}

impl TableDataFilesAtContext {
    #[cfg_attr(test, allow(dead_code))]
    fn for_table_snapshot(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = TableDataFilesAtContextKey {
                catalog,
                table_id,
                snapshot_order,
            };
            let cache = table_data_files_at_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog, table_id, snapshot_order)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog, table_id, snapshot_order)
        }
    }

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let mut seen = BTreeSet::new();
        let mut rows = Vec::new();
        for item in kv.scan_range(
            &data_file_begin_prefix(catalog, table_id),
            &data_file_begin_scan_end(catalog, table_id, snapshot_order),
            RangeDirection::Forward,
            usize::MAX,
        )? {
            let row = data_file_from_begin_index_item(kv, catalog, &item.key, &item.value)?;
            if data_file_visible_at(&row, snapshot_order) {
                seen.insert(row.data_file_id);
                rows.push(row);
            }
        }
        let current_rows = list_current_data_files(kv, catalog, table_id)?;
        for row in &current_rows {
            if !seen.contains(&row.data_file_id) && data_file_visible_at(row, snapshot_order) {
                rows.push(row.clone());
            }
        }
        Ok(Self {
            visible_rows: without_backfilled_source_duplicates(kv, catalog, rows)?,
        })
    }

    fn visible_rows(&self) -> Vec<DataFileRow> {
        self.visible_rows.clone()
    }
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TableDataFilesAtContextKey {
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static TABLE_DATA_FILES_AT_CONTEXT_CACHE: OnceLock<
    BoundedCache<TableDataFilesAtContextKey, TableDataFilesAtContext>,
> = OnceLock::new();

static TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE: OnceLock<
    BoundedCache<TableCurrentDataFilesContextKey, TableCurrentDataFilesContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn table_data_files_at_context_cache()
-> &'static BoundedCache<TableDataFilesAtContextKey, TableDataFilesAtContext> {
    static_bounded_cache(&TABLE_DATA_FILES_AT_CONTEXT_CACHE, 512)
}

fn table_current_data_files_context_cache()
-> &'static BoundedCache<TableCurrentDataFilesContextKey, TableCurrentDataFilesContext> {
    static_bounded_cache(&TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE, 1024)
}

#[cfg(not(test))]
pub(crate) fn invalidate_data_file_read_context(catalog: CatalogId) {
    if let Some(cache) = TABLE_DATA_FILES_AT_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = DELETE_FILE_CHANGED_TABLES_CACHE.get() {
        cache.retain(|key, _| key.0 != catalog);
    }
    if let Some(cache) = TABLE_DELETE_FILE_CHANGES_EXIST_CACHE.get() {
        cache.retain(|key, _| key.0 != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_data_file_read_context(catalog: CatalogId) {
    if let Some(cache) = TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

pub(crate) fn data_file_visible_at(row: &DataFileRow, snapshot_order: CatalogOrderId) -> bool {
    row.validity.is_visible_at(snapshot_order)
}

pub(crate) fn without_backfilled_source_duplicates(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: Vec<DataFileRow>,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let replacements = rows
        .iter()
        .filter(|row| is_backfilled_replacement_file(row))
        .map(DataFileCoverage::from)
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        record_runtime_method_stage(
            "method.data_file_store.without_backfilled_source_duplicates",
            started,
        );
        return Ok(rows);
    }
    let data_file_ids = rows
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let partition_keys = if replacements_need_partition_values(&replacements, &rows) {
        let comparison_count = replacements.len().saturating_mul(rows.len());
        PartitionKeyLookup::new(
            partition_values_for_duplicate_suppression(kv, catalog, &rows, &data_file_ids)?,
            comparison_count,
        )
    } else {
        PartitionKeyLookup::default()
    };

    let rows = rows
        .into_iter()
        .filter(|row| {
            replacements.iter().all(|replacement| {
                row.data_file_id == replacement.data_file_id
                    || !replacement.covers(row, &partition_keys)
            })
        })
        .collect();
    record_runtime_method_stage(
        "method.data_file_store.without_backfilled_source_duplicates",
        started,
    );
    Ok(rows)
}

fn partition_values_for_duplicate_suppression(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DataFileRow],
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    if rows.iter().all(|row| row.validity.end_order.is_none()) {
        return Ok(
            CatalogCurrentFilePartitionValuesContext::for_current_file_partition_values(
                kv, catalog,
            )?
            .current_file_partition_values()
            .iter()
            .filter(|row| data_file_ids.contains(&row.data_file_id))
            .cloned()
            .collect(),
        );
    }
    list_file_partition_values_for_data_files(kv, catalog, data_file_ids)
}

fn replacements_need_partition_values(
    replacements: &[DataFileCoverage],
    rows: &[DataFileRow],
) -> bool {
    replacements.iter().any(|replacement| {
        rows.iter().any(|row| {
            row.data_file_id != replacement.data_file_id
                && replacement.might_cover_by_partition(row)
        })
    })
}

fn is_backfilled_replacement_file(row: &DataFileRow) -> bool {
    row.max_partial_order
        .is_some_and(|max_partial_order| max_partial_order != row.validity.begin_order)
}

#[derive(Debug, Clone)]
struct DataFileCoverage {
    data_file_id: DataFileId,
    table_id: TableId,
    path: String,
    begin_order: crate::CatalogOrderId,
    row_id_start: u64,
    row_id_end: u64,
    row_id_start_known: bool,
    record_count: u64,
    max_partial_order: CatalogOrderId,
}

impl DataFileCoverage {
    fn covers(&self, row: &DataFileRow, partition_keys: &PartitionKeyLookup) -> bool {
        if row.table_id != self.table_id || row.record_count == 0 {
            return false;
        }
        if row.validity.begin_order > self.max_partial_order {
            return false;
        }
        if self.covers_row_id_range(row) {
            return !partition_paths_are_known_and_different(&self.path, &row.path);
        }
        if self.row_id_start_known && row.row_id_start_known {
            return false;
        }
        if self.record_count <= row.record_count {
            return false;
        }
        if row.validity.begin_order < self.begin_order {
            return false;
        }
        partition_keys.same_non_empty_key(self.data_file_id, row.data_file_id)
    }

    fn might_cover_by_partition(&self, row: &DataFileRow) -> bool {
        row.table_id == self.table_id
            && row.record_count != 0
            && row.validity.begin_order <= self.max_partial_order
            && row.validity.begin_order >= self.begin_order
            && !self.covers_row_id_range(row)
            && (!self.row_id_start_known || !row.row_id_start_known)
            && self.record_count > row.record_count
    }

    fn covers_row_id_range(&self, row: &DataFileRow) -> bool {
        if !self.row_id_start_known || !row.row_id_start_known {
            return false;
        }
        let row_end = row.row_id_start.saturating_add(row.record_count);
        self.row_id_start <= row.row_id_start && row_end <= self.row_id_end
    }
}

impl From<&DataFileRow> for DataFileCoverage {
    fn from(row: &DataFileRow) -> Self {
        Self {
            data_file_id: row.data_file_id,
            table_id: row.table_id,
            path: row.path.clone(),
            begin_order: row.validity.begin_order,
            row_id_start: row.row_id_start,
            row_id_end: row.row_id_start.saturating_add(row.record_count),
            row_id_start_known: row.row_id_start_known,
            record_count: row.record_count,
            max_partial_order: row.max_partial_order.unwrap_or(row.validity.begin_order),
        }
    }
}

fn partition_paths_are_known_and_different(left: &str, right: &str) -> bool {
    let left = hive_partition_path_key(left);
    let right = hive_partition_path_key(right);
    !left.is_empty() && !right.is_empty() && left != right
}

fn hive_partition_path_key(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|component| {
            component
                .split_once('=')
                .is_some_and(|(key, value)| !key.is_empty() && !value.is_empty())
        })
        .collect()
}

const PARTITION_KEY_LOOKUP_INDEX_SCAN_MAX_PRODUCT: usize = 256;

#[derive(Default)]
enum PartitionKeyLookup {
    #[default]
    Empty,
    Linear(Vec<FilePartitionValueRow>),
    Indexed(BTreeMap<DataFileId, Vec<(u32, String)>>),
}

impl PartitionKeyLookup {
    fn new(partition_values: Vec<FilePartitionValueRow>, comparison_count: usize) -> Self {
        if partition_values.is_empty() {
            return Self::Empty;
        }
        let scan_product = partition_values.len().saturating_mul(comparison_count);
        if scan_product <= PARTITION_KEY_LOOKUP_INDEX_SCAN_MAX_PRODUCT {
            return Self::Linear(partition_values);
        }
        let mut keys_by_file = BTreeMap::<DataFileId, Vec<(u32, String)>>::new();
        for row in partition_values {
            keys_by_file
                .entry(row.data_file_id)
                .or_default()
                .push((row.partition_key_index.0, row.partition_value));
        }
        for values in keys_by_file.values_mut() {
            values.sort();
        }
        Self::Indexed(keys_by_file)
    }

    fn same_non_empty_key(&self, left: DataFileId, right: DataFileId) -> bool {
        match self {
            Self::Empty => false,
            Self::Linear(partition_values) => {
                let left_key = partition_key_from_rows(partition_values, left);
                !left_key.is_empty() && left_key == partition_key_from_rows(partition_values, right)
            }
            Self::Indexed(keys_by_file) => keys_by_file.get(&left).is_some_and(|left_key| {
                !left_key.is_empty() && Some(left_key) == keys_by_file.get(&right)
            }),
        }
    }
}

fn partition_key_from_rows(
    partition_values: &[FilePartitionValueRow],
    data_file_id: DataFileId,
) -> Vec<(u32, String)> {
    let mut values = partition_values
        .iter()
        .filter(|row| row.data_file_id == data_file_id)
        .map(|row| (row.partition_key_index.0, row.partition_value.clone()))
        .collect::<Vec<_>>();
    values.sort();
    values
}

pub fn list_data_files_with_deletes_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    let rows = attach_delete_files_at(
        kv,
        catalog,
        list_data_files_at(kv, catalog, table_id, snapshot_order)?,
        snapshot_order,
    )?;
    record_runtime_method_stage(
        "method.data_file_store.list_data_files_with_deletes_at",
        started,
    );
    Ok(rows)
}

pub(crate) fn attach_delete_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    if !delete_file_changes_exist_for_files_at(kv, catalog, &data_files, snapshot_order)? {
        record_runtime_method_stage("method.data_file_store.attach_delete_files_at", started);
        return Ok(attach_data_files_without_deletes(data_files));
    }
    let mut attached = Vec::with_capacity(data_files.len());
    for data_file in data_files {
        let delete_file = load_delete_file_at(kv, catalog, data_file.data_file_id, snapshot_order)?;
        attached.push(AttachedDataFile::new(data_file, delete_file));
    }
    record_runtime_method_stage("method.data_file_store.attach_delete_files_at", started);
    Ok(attached)
}

fn delete_file_changes_exist_for_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: &[DataFileRow],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    let mut table_ids = data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<Vec<_>>();
    table_ids.sort_unstable();
    table_ids.dedup();
    for table_id in table_ids {
        if table_delete_file_changes_exist_at(kv, catalog, table_id, snapshot_order)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_delete_file_changes_exist_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    #[cfg(not(test))]
    {
        return Ok(delete_file_changed_tables_at(kv, catalog, snapshot_order)?.contains(&table_id));
    }
    #[cfg(test)]
    {
        Ok(
            delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)?
                .contains(&table_id),
        )
    }
}

#[cfg_attr(test, allow(dead_code))]
fn delete_file_changed_tables_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    #[cfg(not(test))]
    {
        if let Some(tables) = cached_delete_file_changed_tables_at(catalog, snapshot_order) {
            return Ok(tables);
        }
        let tables = delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)?;
        cache_delete_file_changed_tables_at(catalog, snapshot_order, tables.clone());
        Ok(tables)
    }
    #[cfg(test)]
    {
        delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)
    }
}

fn delete_file_changed_tables_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    let prefix = order_delete_file_change_prefix(catalog);
    let mut tables = BTreeSet::new();
    for item in kv.scan_range(
        &prefix,
        &order_delete_file_change_scan_end(catalog, snapshot_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        tables.insert(decode_order_delete_file_change_table_id(
            &prefix, &item.key,
        )?);
    }
    Ok(tables)
}

#[cfg(not(test))]
static DELETE_FILE_CHANGED_TABLES_CACHE: OnceLock<
    BoundedCache<(CatalogId, CatalogOrderId), BTreeSet<TableId>>,
> = OnceLock::new();

#[cfg(not(test))]
static TABLE_DELETE_FILE_CHANGES_EXIST_CACHE: OnceLock<
    BoundedCache<(CatalogId, TableId, CatalogOrderId), bool>,
> = OnceLock::new();

#[cfg(not(test))]
fn delete_file_changed_tables_cache()
-> &'static BoundedCache<(CatalogId, CatalogOrderId), BTreeSet<TableId>> {
    static_bounded_cache(&DELETE_FILE_CHANGED_TABLES_CACHE, 512)
}

#[cfg(not(test))]
fn table_delete_file_changes_exist_cache()
-> &'static BoundedCache<(CatalogId, TableId, CatalogOrderId), bool> {
    static_bounded_cache(&TABLE_DELETE_FILE_CHANGES_EXIST_CACHE, 4096)
}

#[cfg(not(test))]
pub(crate) fn cached_delete_file_changed_tables_at(
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> Option<BTreeSet<TableId>> {
    DELETE_FILE_CHANGED_TABLES_CACHE
        .get()
        .and_then(|cache| cache.get((catalog, snapshot_order)))
}

#[cfg(test)]
pub(crate) fn cached_delete_file_changed_tables_at(
    _catalog: CatalogId,
    _snapshot_order: CatalogOrderId,
) -> Option<BTreeSet<TableId>> {
    None
}

#[cfg(not(test))]
pub(crate) fn cache_delete_file_changed_tables_at(
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    tables: BTreeSet<TableId>,
) {
    delete_file_changed_tables_cache().insert((catalog, snapshot_order), tables);
}

#[cfg(test)]
pub(crate) fn cache_delete_file_changed_tables_at(
    _catalog: CatalogId,
    _snapshot_order: CatalogOrderId,
    _tables: BTreeSet<TableId>,
) {
}

#[cfg(not(test))]
pub(crate) fn cached_table_delete_file_changes_exist_at(
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> Option<bool> {
    TABLE_DELETE_FILE_CHANGES_EXIST_CACHE
        .get()
        .and_then(|cache| cache.get((catalog, table_id, snapshot_order)))
}

#[cfg(test)]
pub(crate) fn cached_table_delete_file_changes_exist_at(
    _catalog: CatalogId,
    _table_id: TableId,
    _snapshot_order: CatalogOrderId,
) -> Option<bool> {
    None
}

#[cfg(not(test))]
pub(crate) fn cache_table_delete_file_changes_exist_at(
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    exists: bool,
) {
    table_delete_file_changes_exist_cache().insert((catalog, table_id, snapshot_order), exists);
}

#[cfg(test)]
pub(crate) fn cache_table_delete_file_changes_exist_at(
    _catalog: CatalogId,
    _table_id: TableId,
    _snapshot_order: CatalogOrderId,
    _exists: bool,
) {
}

fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "data file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DataFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("data file id pointer is truncated".to_owned()),
    )?)))
}

fn decode_delete_file_id(bytes: &[u8]) -> CatalogResult<DeleteFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DeleteFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("delete file id pointer is truncated".to_owned()),
    )?)))
}

fn data_file_from_current_index_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(row) => Ok(row),
        Err(_) => load_data_file(kv, catalog, decode_data_file_id(value)?),
    }
}

fn data_file_from_begin_index_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(mut row) => {
            row.validity.begin_order = data_file_begin_order_from_key(
                catalog,
                row.table_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            Ok(row)
        }
        Err(_) => load_data_file(kv, catalog, decode_data_file_id(value)?),
    }
}

fn data_file_begin_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    kind: crate::CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = data_file_begin_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(crate::CatalogError::InvalidKey(
            "data file begin key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(crate::CatalogError::InvalidKey(format!(
            "data file begin key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("data file begin order is truncated".to_owned())
        })?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

fn load_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

fn load_delete_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

fn stage_close_current_delete_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file: &DataFileRow,
    next_delete_file: &DeleteFileRow,
    close_order: CatalogOrderId,
) -> CatalogResult<()> {
    let Some(value) = kv.get(&current_delete_file_key(
        catalog,
        next_delete_file.data_file_id,
    ))?
    else {
        return Ok(());
    };
    let mut row = current_delete_file_from_index_value(kv, catalog, &value)?;
    if row.delete_file_id == next_delete_file.delete_file_id {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} is already current",
            row.delete_file_id.0
        )));
    }
    if close_order <= row.validity.begin_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} cannot close newer delete file {}",
            next_delete_file.delete_file_id.0, row.delete_file_id.0
        )));
    }
    if next_delete_file.validity.begin_order < row.validity.begin_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} cannot replace newer delete file {}",
            next_delete_file.delete_file_id.0, row.delete_file_id.0
        )));
    }
    row.validity.end_order = Some(close_order);
    batch.put(delete_file_key(catalog, row.delete_file_id), row.encode());
    if let Some(key) = delete_file_timeline_key_for_row(kv, catalog, &row)? {
        batch.put(key, row.encode());
    }
    batch.put(
        delete_file_end_key(catalog, data_file.table_id, close_order, row.delete_file_id),
        row.delete_file_id.0.to_be_bytes().to_vec(),
    );
    Ok(())
}

fn inherit_current_delete_begin_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    next_delete_file: &mut DeleteFileRow,
) -> CatalogResult<()> {
    let Some(pointer) = kv.get(&current_delete_file_key(
        catalog,
        next_delete_file.data_file_id,
    ))?
    else {
        return Ok(());
    };
    let current = current_delete_file_from_index_value(kv, catalog, &pointer)?;
    if current.delete_file_id == next_delete_file.delete_file_id {
        return Ok(());
    }
    next_delete_file.validity.begin_order = current.validity.begin_order;
    Ok(())
}

fn load_delete_file_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<DeleteFileRow>> {
    let started = RuntimeMetricStage::start();
    for item in kv.scan_range(
        &delete_file_timeline_prefix(catalog, data_file_id),
        &delete_file_timeline_scan_end(catalog, data_file_id, snapshot_order),
        RangeDirection::Reverse,
        1,
    )? {
        let row =
            delete_file_from_timeline_item(kv, catalog, data_file_id, &item.key, &item.value)?;
        let timeline_order = delete_file_timeline_order_from_key(
            catalog,
            data_file_id,
            &item.key,
            row.validity.begin_order.kind(),
        )?;
        if delete_file_can_answer_snapshot(&row, timeline_order, snapshot_order) {
            record_runtime_method_stage("method.data_file_store.load_delete_file_at", started);
            return Ok(Some(row));
        }
    }
    record_runtime_method_stage("method.data_file_store.load_delete_file_at", started);
    Ok(None)
}

pub(crate) fn delete_file_can_answer_snapshot(
    row: &DeleteFileRow,
    timeline_order: CatalogOrderId,
    snapshot_order: CatalogOrderId,
) -> bool {
    if !row.validity.is_visible_at(snapshot_order) {
        return false;
    }
    timeline_order <= snapshot_order
}

pub(crate) fn current_delete_file_from_index_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<DeleteFileRow> {
    match DeleteFileRow::decode(value) {
        Ok(row) => Ok(row),
        Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(value)?),
    }
}

fn delete_file_from_timeline_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DeleteFileRow> {
    match DeleteFileRow::decode(value) {
        Ok(mut row) => {
            let timeline_order = delete_file_timeline_order_from_key(
                catalog,
                data_file_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            if row.validity.begin_order == incomplete_fdb_order() {
                row.validity.begin_order = timeline_order;
            }
            if row.max_partial_order == Some(incomplete_fdb_order()) {
                row.max_partial_order = Some(timeline_order);
            }
            Ok(row)
        }
        Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(value)?),
    }
}

fn delete_file_timeline_key_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<Option<Vec<u8>>> {
    let started = RuntimeMetricStage::start();
    for item in kv.scan_prefix(
        &delete_file_timeline_prefix(catalog, row.data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let delete_file_id = match DeleteFileRow::decode(&item.value) {
            Ok(existing) => existing.delete_file_id,
            Err(_) => decode_delete_file_id(&item.value)?,
        };
        if delete_file_id == row.delete_file_id {
            record_runtime_method_stage(
                "method.data_file_store.delete_file_timeline_key_for_row",
                started,
            );
            return Ok(Some(item.key));
        }
    }
    record_runtime_method_stage(
        "method.data_file_store.delete_file_timeline_key_for_row",
        started,
    );
    Ok(None)
}

#[cfg(test)]
#[path = "data_file_store_tests.rs"]
mod data_file_store_tests;
