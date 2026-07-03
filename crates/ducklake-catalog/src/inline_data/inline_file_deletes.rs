use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataFileId,
    InlineTableFlush, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection, TableId,
    ValidityWindow,
    conflict_watermarks::stage_max_file_id_watermark,
    keys::{
        KeyFamily, family_prefix, inline_file_deletion_file_prefix, inline_file_deletion_key,
        inline_file_deletion_table_prefix,
    },
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    store::{
        latest_snapshot, snapshot_by_raw_sequence, snapshot_row_for_next_sequence, stage_snapshot,
    },
};

const MAX_FILE_SCOPED_INLINE_DELETION_PREFIX_SCANS: usize = 8;

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
fn record_inline_file_deletes_stage(operation: &'static str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_inline_file_deletes_stage(_operation: &'static str, _started: RuntimeMetricStage) {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineFileDeletionRow {
    pub table_id: TableId,
    pub data_file_id: DataFileId,
    pub row_id: u64,
    pub validity: ValidityWindow,
}

impl InlineFileDeletionRow {
    const VERSION: u8 = 2;
    #[cfg(feature = "foundationdb")]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 8 + STORED_ORDER_LEN + 1 + 1;

    #[must_use]
    pub const fn new(
        table_id: TableId,
        data_file_id: DataFileId,
        row_id: u64,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            table_id,
            data_file_id,
            row_id,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 8 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN);
        out.push(Self::VERSION);
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.row_id.to_be_bytes());
        encode_stored_order(&mut out, self.validity.begin_order);
        match self.validity.end_order {
            Some(order) => {
                out.push(1);
                encode_stored_order(&mut out, order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let version = match bytes.first().copied() {
            Some(1..=Self::VERSION) => bytes[0],
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported inline file deletion row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "inline file deletion row is too short: 0 bytes".to_owned(),
                ));
            }
        };
        let end_order_len = if version >= 2 { STORED_ORDER_LEN } else { 0 };
        let expected_len = 1 + 8 + 8 + 8 + STORED_ORDER_LEN + 1 + end_order_len;
        if bytes.len() != expected_len {
            return Err(CatalogError::Decode(format!(
                "inline file deletion row must be {expected_len} bytes, got {}",
                bytes.len()
            )));
        }
        let table_start = 1;
        let file_start = table_start + 8;
        let row_start = file_start + 8;
        let begin_start = row_start + 8;
        let end_flag = begin_start + STORED_ORDER_LEN;
        let end_order_start = end_flag + 1;
        let end_order = match bytes[end_flag] {
            0 => None,
            1 if version >= 2 => Some(decode_stored_order(
                &bytes[end_order_start..end_order_start + STORED_ORDER_LEN],
                "inline deletion end order",
            )?),
            1 => {
                return Err(CatalogError::Decode(
                    "inline file deletion v1 cannot carry end order".to_owned(),
                ));
            }
            other => {
                return Err(CatalogError::Decode(format!(
                    "unsupported inline file deletion end-order flag {other}"
                )));
            }
        };
        Ok(Self {
            table_id: TableId(u64::from_be_bytes(
                bytes[table_start..file_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("inline table id is truncated".to_owned()))?,
            )),
            data_file_id: DataFileId(u64::from_be_bytes(
                bytes[file_start..row_start].try_into().map_err(|_| {
                    CatalogError::Decode("inline data file id is truncated".to_owned())
                })?,
            )),
            row_id: u64::from_be_bytes(
                bytes[row_start..begin_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("inline row id is truncated".to_owned()))?,
            ),
            validity: ValidityWindow::new(
                decode_stored_order(&bytes[begin_start..end_flag], "inline deletion begin order")?,
                end_order,
            ),
        })
    }
}

pub fn commit_inline_file_deletions(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut rows: Vec<InlineFileDeletionRow>,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    if rows.is_empty() {
        return Ok(rows);
    }
    let latest = latest_snapshot(kv, catalog)?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    for row in &mut rows {
        row.validity = ValidityWindow::new(order, None);
        stage_inline_file_deletion(&mut batch, catalog, row);
    }
    stage_max_file_id_watermark(kv, &mut batch, catalog, snapshot.sequence.0)?;
    kv.commit(batch)?;
    crate::store::invalidate_runtime_read_context(catalog);
    crate::runtime_read_context::invalidate_inline_deletion_read_context(catalog);
    Ok(rows)
}

pub fn list_inline_file_deletions_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeMap<DataFileId, BTreeSet<u64>>> {
    let started = RuntimeMetricStage::start();
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        let grouped = crate::runtime_read_context::inline_file_deletions_at(
            kv,
            catalog,
            table_id,
            snapshot_order,
        )?;
        record_inline_file_deletes_stage(
            "method.inline_file_deletes.list_inline_file_deletions_at",
            started,
        );
        return Ok(grouped);
    }
    let mut grouped = BTreeMap::<DataFileId, BTreeSet<u64>>::new();
    for row in list_inline_file_deletion_rows_for_table_at(kv, catalog, table_id, snapshot_order)? {
        grouped
            .entry(row.data_file_id)
            .or_default()
            .insert(row.row_id);
    }
    record_inline_file_deletes_stage(
        "method.inline_file_deletes.list_inline_file_deletions_at",
        started,
    );
    Ok(grouped)
}

pub(crate) fn list_inline_file_deletions_for_data_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<BTreeMap<DataFileId, BTreeSet<u64>>> {
    if data_file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut grouped = BTreeMap::<DataFileId, BTreeSet<u64>>::new();
    for row in list_inline_file_deletion_rows_for_data_files_at(
        kv,
        catalog,
        table_id,
        snapshot_order,
        data_file_ids,
    )? {
        grouped
            .entry(row.data_file_id)
            .or_default()
            .insert(row.row_id);
    }
    Ok(grouped)
}

pub(crate) fn list_inline_file_deletion_rows_for_data_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    if data_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    if data_file_ids.len() > MAX_FILE_SCOPED_INLINE_DELETION_PREFIX_SCANS {
        return Ok(list_inline_file_deletion_rows_for_table_at(
            kv,
            catalog,
            table_id,
            snapshot_order,
        )?
        .into_iter()
        .filter(|row| data_file_ids.contains(&row.data_file_id))
        .collect());
    }
    let mut rows = Vec::new();
    for data_file_id in data_file_ids {
        rows.extend(list_inline_file_deletion_rows_for_data_file_at_uncached(
            kv,
            catalog,
            table_id,
            *data_file_id,
            snapshot_order,
        )?);
    }
    rows.sort_by_key(|row| (row.data_file_id, row.row_id, row.validity.begin_order));
    Ok(rows)
}

pub(crate) fn list_inline_file_deletion_rows_for_table_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return Ok(
            crate::runtime_read_context::InlineDeletionReadContext::for_table(
                kv, catalog, table_id,
            )?
            .rows_at(snapshot_order),
        );
    }
    list_inline_file_deletion_rows_for_table_at_uncached(kv, catalog, table_id, snapshot_order)
}

pub(crate) fn list_inline_file_deletion_rows_for_table_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    Ok(
        list_all_inline_file_deletion_rows_for_table_uncached(kv, catalog, table_id)?
            .into_iter()
            .filter(|row| row.validity.is_visible_at(snapshot_order))
            .collect(),
    )
}

pub(crate) fn list_all_inline_file_deletion_rows_for_table_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let started = RuntimeMetricStage::start();
    let mut result = Vec::new();
    for item in kv.scan_prefix(
        &inline_file_deletion_table_prefix(catalog, table_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let mut row = InlineFileDeletionRow::decode(&item.value)?;
        row.validity.begin_order =
            inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
        result.push(row);
    }
    result.sort_by_key(|row| (row.data_file_id, row.row_id, row.validity.begin_order));
    record_inline_file_deletes_stage("method.inline_file_deletes.list_rows_for_table_at", started);
    Ok(result)
}

pub(crate) fn list_inline_file_deletion_rows_for_data_file_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let mut result = Vec::new();
    for item in kv.scan_prefix(
        &inline_file_deletion_file_prefix(catalog, table_id, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let mut row = InlineFileDeletionRow::decode(&item.value)?;
        row.validity.begin_order =
            inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
        if row.validity.is_visible_at(snapshot_order) {
            result.push(row);
        }
    }
    result.sort_by_key(|row| (row.row_id, row.validity.begin_order));
    Ok(result)
}

pub(crate) fn inline_file_deletion_rows_for_flush(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    flush: InlineTableFlush,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let started = RuntimeMetricStage::start();
    let Some(flush_snapshot) =
        snapshot_by_raw_sequence(kv, catalog, flush.flush_snapshot_sequence)?
    else {
        return Err(CatalogError::InvalidMutation(format!(
            "inline file deletion flush references missing snapshot {}",
            flush.flush_snapshot_sequence
        )));
    };
    let rows = list_inline_file_deletion_rows_for_table_at(
        kv,
        catalog,
        flush.table_id,
        flush_snapshot.order,
    )?;
    record_inline_file_deletes_stage("method.inline_file_deletes.rows_for_flush", started);
    Ok(rows)
}

pub(crate) fn stage_flush_inline_file_deletions(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    flush: InlineTableFlush,
    end_order: CatalogOrderId,
) -> CatalogResult<usize> {
    let started = RuntimeMetricStage::start();
    let rows = inline_file_deletion_rows_for_flush(kv, catalog, flush)?;
    for mut row in rows.iter().cloned() {
        row.validity.end_order = Some(end_order);
        batch.put(
            inline_file_deletion_key(
                catalog,
                row.table_id,
                row.data_file_id,
                row.validity.begin_order,
                row.row_id,
            ),
            row.encode(),
        );
    }
    record_inline_file_deletes_stage("method.inline_file_deletes.stage_flush", started);
    Ok(rows.len())
}

pub fn list_inline_file_deletions_between(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &inline_file_deletion_table_prefix(catalog, table_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let mut row = InlineFileDeletionRow::decode(&item.value)?;
        row.validity.begin_order =
            inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
        if start_order <= row.validity.begin_order && row.validity.begin_order <= end_order {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| (row.validity.begin_order, row.data_file_id, row.row_id));
    record_inline_file_deletes_stage("method.inline_file_deletes.list_between", started);
    Ok(rows)
}

pub(crate) fn inline_file_deletion_changed_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    let mut tables = BTreeSet::new();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineFileDeletion),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = InlineFileDeletionRow::decode(&item.value)?;
        let begin_order =
            inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
        if begin_order == order {
            tables.insert(row.table_id);
        }
    }
    Ok(tables)
}

pub(crate) fn inline_file_deletion_begin_order(
    catalog: CatalogId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = inline_file_deletion_table_prefix(catalog, TableId(0));
    let family_and_catalog_len = prefix.len().saturating_sub(8 + 1);
    let begin_start = family_and_catalog_len
        .saturating_add(8)
        .saturating_add(1)
        .saturating_add(8)
        .saturating_add(1);
    let begin_end = begin_start.saturating_add(CatalogOrderId::LEN);
    if key.len() < begin_end {
        return Err(CatalogError::Decode(
            "inline file deletion key is truncated before begin order".to_owned(),
        ));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        key[begin_start..begin_end].try_into().map_err(|_| {
            CatalogError::Decode("inline file deletion begin order is truncated".to_owned())
        })?;
    let kind = if value_order.as_bytes() == bytes {
        value_order.kind()
    } else {
        CatalogOrderKind::FdbVersionstamp
    };
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

pub(crate) fn stage_inline_file_deletion(
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &InlineFileDeletionRow,
) {
    batch.put(
        inline_file_deletion_key(
            catalog,
            row.table_id,
            row.data_file_id,
            row.validity.begin_order,
            row.row_id,
        ),
        row.encode(),
    );
}
