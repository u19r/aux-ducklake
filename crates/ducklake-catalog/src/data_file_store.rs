use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogId, CatalogResult, DataFileChangeKind, DataFileId, DataFileRow, DeleteFileRow,
    InlineTableFlush, KvBatch, MutableCatalogKv, OrderedCatalogKv, TableId,
    conflict::write_data_file_change,
    conflict_watermarks::stage_max_file_id_watermark,
    file_partitions::{
        delete_partition_lookups_for_data_file, delete_partition_values_for_data_file,
    },
    ids::CatalogOrderId,
    inline_data::stage_flush_inline_table_payloads,
    keys::{
        current_data_file_key, current_delete_file_key, data_file_begin_key, data_file_end_key,
        data_file_key, delete_file_key, delete_file_timeline_key, order_delete_file_change_key,
        table_delete_file_change_key,
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

mod current_files;
mod delete_attachment;
mod duplicate_suppression;
mod storage_rows;

pub use current_files::*;
pub use delete_attachment::*;
pub(super) use duplicate_suppression::*;
pub(super) use storage_rows::*;
#[cfg(test)]
#[path = "data_file_store_tests.rs"]
mod data_file_store_tests;
