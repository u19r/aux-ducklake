use super::{
    inline_end_orders::{
        InlineTableEndOrders, inline_chunk_visible_at, inline_table_all_end_orders,
        inline_table_end_orders, inline_table_end_orders_at,
    },
    inline_group_orders::assemble_inline_payload,
};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::runtime_metrics::RuntimeMetricStage;
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogError, CatalogId, CatalogResult, DuckLakeSnapshotId, KvBatch, MutableCatalogKv,
    OrderedCatalogKv, RangeDirection, SchemaId, SnapshotRow, TableId, TableRow, ValidityWindow,
    conflict_watermarks::{stage_max_catalog_id_watermark, stage_max_file_id_watermark},
    ids::{CatalogOrderId, CatalogOrderKind, RawSnapshotSequence, incomplete_fdb_order},
    inline_change_feed::{InlineRowChangeKind, stage_inline_row_changes_for_payload},
    inline_data::{
        INLINE_CHUNK_BYTES, chunk_bounds, chunk_count, decode_inline_end_order,
        encode_inline_end_order, validate_inline_table_rows_fit_fdb,
    },
    keys::{InlineFlushWatermarkKey, KeyFamily, family_prefix, table_object_key},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    store::{
        invalidate_runtime_read_context, latest_snapshot, snapshot_by_raw_sequence,
        snapshot_row_for_next_sequence, stage_snapshot,
    },
    table_store::{load_current_table_row, stage_current_table_row, stage_table_visibility_row},
};

#[cfg(feature = "runtime-metrics")]
pub(super) fn record_inline_tables_stage(operation: &'static str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
pub(super) fn record_inline_tables_stage(_operation: &'static str, _started: RuntimeMetricStage) {}

fn incomplete_order() -> CatalogOrderId {
    incomplete_fdb_order()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineTableChunkRow {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub validity: ValidityWindow,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineTablePayloadRow {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub begin_order: CatalogOrderId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlineTableFlush {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub flush_snapshot_sequence: RawSnapshotSequence,
    /// Whether this flush also materializes and hides inlined data rows.
    ///
    /// A file-deletion flush only ends the inlined file-deletion rows that
    /// were written as delete files. It must not hide inlined data that no
    /// file materialized.
    pub flush_inline_data: bool,
}

impl InlineTableFlush {
    #[must_use]
    pub const fn new(
        table_id: TableId,
        schema_id: SchemaId,
        flush_snapshot_sequence: RawSnapshotSequence,
    ) -> Self {
        Self {
            table_id,
            schema_id,
            flush_snapshot_sequence,
            flush_inline_data: true,
        }
    }

    #[must_use]
    pub const fn file_deletions_only(
        table_id: TableId,
        flush_snapshot_sequence: RawSnapshotSequence,
    ) -> Self {
        Self {
            table_id,
            schema_id: SchemaId(0),
            flush_snapshot_sequence,
            flush_inline_data: false,
        }
    }
}

impl InlineTableChunkRow {
    const VERSION: u8 = 1;
    #[cfg(feature = "foundationdb")]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + STORED_ORDER_LEN + 1 + 1;

    #[must_use]
    pub fn new(
        table_id: TableId,
        schema_id: SchemaId,
        validity: ValidityWindow,
        chunk_index: u32,
        chunk_count: u32,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            table_id,
            schema_id,
            validity,
            chunk_index,
            chunk_count,
            payload,
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(1 + 8 + 8 + STORED_ORDER_LEN * 2 + 10 + self.payload.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.schema_id.0.to_be_bytes());
        encode_stored_order(&mut out, self.validity.begin_order);
        encode_inline_end_order(&mut out, self.validity);
        out.extend_from_slice(&self.chunk_index.to_be_bytes());
        out.extend_from_slice(&self.chunk_count.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        const HEADER_LEN: usize = 1 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN + 4 + 4 + 4;
        if bytes.len() < HEADER_LEN {
            return Err(CatalogError::Decode(format!(
                "inline table chunk row is too short: {} bytes",
                bytes.len()
            )));
        }
        if bytes[0] != Self::VERSION {
            return Err(CatalogError::Decode(format!(
                "unsupported inline table chunk row version {}",
                bytes[0]
            )));
        }
        let table_start = 1;
        let schema_start = table_start + 8;
        let begin_start = schema_start + 8;
        let end_flag = begin_start + STORED_ORDER_LEN;
        let end_start = end_flag + 1;
        let chunk_index_start = end_start + STORED_ORDER_LEN;
        let chunk_count_start = chunk_index_start + 4;
        let payload_len_start = chunk_count_start + 4;
        let payload_start = payload_len_start + 4;
        let payload_len =
            u32::from_be_bytes(bytes[payload_len_start..payload_start].try_into().map_err(
                |_| CatalogError::Decode("inline payload length is truncated".to_owned()),
            )?) as usize;
        let payload_end = payload_start.saturating_add(payload_len);
        if bytes.len() != payload_end {
            return Err(CatalogError::Decode(format!(
                "inline table chunk payload must be {payload_len} bytes, got {}",
                bytes.len().saturating_sub(payload_start)
            )));
        }
        Ok(Self {
            table_id: TableId(u64::from_be_bytes(
                bytes[table_start..schema_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("inline table id is truncated".to_owned()))?,
            )),
            schema_id: SchemaId(u64::from_be_bytes(
                bytes[schema_start..begin_start].try_into().map_err(|_| {
                    CatalogError::Decode("inline schema id is truncated".to_owned())
                })?,
            )),
            validity: ValidityWindow::new(
                decode_stored_order(&bytes[begin_start..end_flag], "inline begin order")?,
                decode_inline_end_order(bytes[end_flag], &bytes[end_start..chunk_index_start])?,
            ),
            chunk_index: u32::from_be_bytes(
                bytes[chunk_index_start..chunk_count_start]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode("inline chunk index is truncated".to_owned())
                    })?,
            ),
            chunk_count: u32::from_be_bytes(
                bytes[chunk_count_start..payload_len_start]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode("inline chunk count is truncated".to_owned())
                    })?,
            ),
            payload: bytes[payload_start..payload_end].to_vec(),
        })
    }
}

pub fn register_inline_table_payload(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    payload: Vec<u8>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    validate_inline_table_rows_fit_fdb(&payload)?;
    reject_duplicate_inline_row_ids_in_payload(&payload)?;
    let latest = latest_snapshot(kv, catalog)?;
    let begin_order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, begin_order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_inline_row_changes_for_payload(
        &mut batch,
        catalog,
        table_id,
        schema_id,
        begin_order,
        InlineRowChangeKind::Inserted,
        &payload,
    )?;
    let rows = inline_table_chunks(table_id, schema_id, begin_order, payload)?;
    for row in &rows {
        batch.put(
            inline_table_chunk_key(catalog, table_id, schema_id, begin_order, row.chunk_index),
            row.encode(),
        );
    }
    stage_max_file_id_watermark(kv, &mut batch, catalog, snapshot.sequence.0)?;
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub fn register_inline_table_payload_with_table(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table: TableRow,
    schema_id: SchemaId,
    payload: Vec<u8>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    register_inline_table_payload_with_table_at_snapshot(
        kv, catalog, table, schema_id, payload, None,
    )
}

pub fn register_inline_table_payload_with_table_at_snapshot(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table: TableRow,
    schema_id: SchemaId,
    payload: Vec<u8>,
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    validate_inline_table_rows_fit_fdb(&payload)?;
    reject_duplicate_inline_row_ids_in_payload(&payload)?;
    let latest = latest_snapshot(kv, catalog)?;
    let _ = latest
        .as_ref()
        .ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut previous_table = load_current_table_row(kv, catalog, table.table_id)?
        .ok_or(CatalogError::NotFound("table"))?;
    let target = inline_commit_target(kv, latest, commit_snapshot)?;
    let mut next_table = previous_table.clone();
    previous_table.validity.end_order = Some(target.snapshot.order);
    next_table.schema_id = table.schema_id;
    next_table.inlined_data_tables = table.inlined_data_tables;
    next_table.validity = ValidityWindow::new(target.snapshot.order, None);
    let mut batch = KvBatch::new();
    if target.stage_snapshot {
        stage_snapshot(&mut batch, catalog, &target.snapshot);
    }
    stage_inline_row_changes_for_payload(
        &mut batch,
        catalog,
        table.table_id,
        schema_id,
        target.snapshot.order,
        InlineRowChangeKind::Inserted,
        &payload,
    )?;
    let rows = inline_table_chunks(table.table_id, schema_id, target.snapshot.order, payload)?;
    if previous_table.validity.begin_order == target.snapshot.order {
        batch.put(
            table_object_key(catalog, table.table_id, target.snapshot.order),
            next_table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &next_table);
    } else {
        batch.put(
            table_object_key(
                catalog,
                previous_table.table_id,
                previous_table.validity.begin_order,
            ),
            previous_table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &previous_table);
        batch.put(
            table_object_key(catalog, table.table_id, target.snapshot.order),
            next_table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &next_table);
    }
    for row in &rows {
        batch.put(
            inline_table_chunk_key(
                catalog,
                table.table_id,
                schema_id,
                target.snapshot.order,
                row.chunk_index,
            ),
            row.encode(),
        );
    }
    stage_current_table_row(&mut batch, catalog, &next_table);
    stage_max_catalog_id_watermark(kv, &mut batch, catalog, next_table.table_id.0)?;
    stage_max_file_id_watermark(kv, &mut batch, catalog, target.snapshot.sequence.0)?;
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

fn reject_duplicate_inline_row_ids_in_payload(payload: &[u8]) -> CatalogResult<()> {
    let mut seen = BTreeSet::new();
    for row_id in inline_payload_row_ids(payload)? {
        if !seen.insert(row_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "inline row id {row_id} appears more than once in one payload"
            )));
        }
    }
    Ok(())
}

fn inline_payload_row_ids(payload: &[u8]) -> CatalogResult<BTreeSet<u64>> {
    let text = std::str::from_utf8(payload)
        .map_err(|error| CatalogError::Decode(format!("inline payload is not utf-8: {error}")))?;
    let mut row_ids = BTreeSet::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("row\t") else {
            return Err(CatalogError::Decode(format!(
                "inline payload row has invalid shape: {line}"
            )));
        };
        let row_id = rest.split('\t').next().unwrap_or_default();
        let row_id = row_id.parse::<u64>().map_err(|error| {
            CatalogError::Decode(format!("inline row id {row_id} is invalid: {error}"))
        })?;
        row_ids.insert(row_id);
    }
    Ok(row_ids)
}

struct InlineCommitTarget {
    snapshot: SnapshotRow,
    stage_snapshot: bool,
}

fn inline_commit_target(
    kv: &mut impl MutableCatalogKv,
    latest: Option<SnapshotRow>,
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<InlineCommitTarget> {
    let Some(commit_snapshot) = commit_snapshot else {
        let begin_order = kv.generated_order_id()?;
        return Ok(InlineCommitTarget {
            snapshot: snapshot_row_for_next_sequence(latest, begin_order),
            stage_snapshot: true,
        });
    };
    let latest_sequence = latest
        .ok_or(CatalogError::NotFound("catalog snapshot"))?
        .sequence;
    let latest_commit = DuckLakeSnapshotId(latest_sequence.0);
    let next_commit = DuckLakeSnapshotId(latest_sequence.next().0);
    if commit_snapshot != latest_commit && commit_snapshot != next_commit {
        return Err(CatalogError::InvalidMutation(format!(
            "inline commit snapshot {} does not match latest DuckLake snapshot {} or next DuckLake snapshot {}",
            commit_snapshot.0, latest_commit.0, next_commit.0
        )));
    }
    let begin_order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(begin_order, RawSnapshotSequence(commit_snapshot.0));
    Ok(InlineCommitTarget {
        snapshot,
        stage_snapshot: true,
    })
}

pub(crate) fn stage_flush_inline_table_payloads(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    flush: InlineTableFlush,
    end_order: CatalogOrderId,
) -> CatalogResult<()> {
    crate::inline_data::stage_flush_inline_file_deletions(kv, batch, catalog, flush, end_order)?;
    if !flush.flush_inline_data {
        return Ok(());
    }
    crate::snapshot_operations::stage_snapshot_operation(
        batch,
        catalog,
        end_order,
        crate::snapshot_operations::SnapshotOperationKind::InlineFlush,
        flush.table_id,
    );
    let flushed = flush_inline_table_payload_rows(kv, catalog, flush, end_order)?;
    if let Some(max_begin_order) = flushed.iter().map(|row| row.validity.begin_order).max() {
        batch.put(
            InlineFlushWatermarkKey {
                table_id: flush.table_id,
                end_order,
                schema_id: flush.schema_id,
                max_begin_order,
            }
            .encode(catalog),
            Vec::new(),
        );
    }
    stage_max_file_id_watermark(kv, batch, catalog, flush.flush_snapshot_sequence.0)?;
    Ok(())
}

pub(crate) fn flush_inline_table_payload_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    flush: InlineTableFlush,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    let Some(flush_snapshot) =
        snapshot_by_raw_sequence(kv, catalog, flush.flush_snapshot_sequence)?
    else {
        return Err(CatalogError::InvalidMutation(format!(
            "inline flush references missing snapshot {}",
            flush.flush_snapshot_sequence
        )));
    };
    flush_inline_table_payload_rows_at_snapshot_order(
        kv,
        catalog,
        flush,
        flush_snapshot.order,
        end_order,
    )
}

pub(crate) fn flush_inline_table_payload_rows_at_snapshot_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    flush: InlineTableFlush,
    flush_snapshot_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    let end_orders = if end_order == incomplete_order() {
        inline_table_all_end_orders(
            kv,
            catalog,
            flush.table_id,
            flush.schema_id,
            flush_snapshot_order,
        )?
    } else {
        inline_table_end_orders_at(kv, catalog, flush.table_id, flush.schema_id, end_order)?
    };
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &inline_table_prefix(catalog, flush.table_id, flush.schema_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        if row.validity.begin_order > flush_snapshot_order {
            continue;
        }
        if let Some(existing_end) = row
            .validity
            .end_order
            .or_else(|| end_orders.end_order_for(row.validity.begin_order))
        {
            if existing_end > flush_snapshot_order {
                return Err(CatalogError::InvalidMutation(format!(
                    "inline flush for table {} schema {} at snapshot {} is stale; row beginning at {} was already ended at {}",
                    flush.table_id.0,
                    flush.schema_id.0,
                    flush.flush_snapshot_sequence,
                    row.validity.begin_order,
                    existing_end
                )));
            }
            continue;
        }
        let mut row = row;
        row.validity.end_order = Some(end_order);
        rows.push(row);
    }
    Ok(rows)
}

pub fn load_inline_table_payload_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<Vec<u8>>> {
    let started = RuntimeMetricStage::start();
    let end_orders = inline_table_end_orders_at(kv, catalog, table_id, schema_id, snapshot_order)?;
    let mut visible = BTreeMap::<CatalogOrderId, Vec<InlineTableChunkRow>>::new();
    for item in kv.scan_prefix(
        &inline_table_prefix(catalog, table_id, schema_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        if inline_chunk_visible_at(&row, &end_orders, snapshot_order) {
            visible
                .entry(row.validity.begin_order)
                .or_default()
                .push(row);
        }
    }
    let Some((_, rows)) = visible.into_iter().next_back() else {
        record_inline_tables_stage("method.inline_tables.load_inline_table_payload_at", started);
        return Ok(None);
    };
    let payload = assemble_inline_payload(rows).map(Some);
    record_inline_tables_stage("method.inline_tables.load_inline_table_payload_at", started);
    payload
}

pub fn list_inline_table_payloads_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    let started = RuntimeMetricStage::start();
    let rows = list_inline_table_payloads_for_visibility(
        kv,
        catalog,
        table_id,
        schema_id,
        snapshot_order,
        InlineTablePayloadVisibility::VisibleAtSnapshot,
    );
    record_inline_tables_stage(
        "method.inline_tables.list_inline_table_payloads_at",
        started,
    );
    rows
}

pub(crate) fn list_unflushed_inline_table_payloads_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    let started = RuntimeMetricStage::start();
    let rows = list_inline_table_payloads_for_visibility(
        kv,
        catalog,
        table_id,
        schema_id,
        snapshot_order,
        InlineTablePayloadVisibility::UnflushedAtSnapshot,
    );
    record_inline_tables_stage(
        "method.inline_tables.list_unflushed_inline_table_payloads_at",
        started,
    );
    rows
}

fn list_inline_table_payloads_for_visibility(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    visibility: InlineTablePayloadVisibility,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    // Unflushed membership changes when another process materializes this
    // snapshot. Cache only historical visibility bounded by the snapshot.
    if !visibility.only_ended_at_or_before_snapshot() {
        return list_inline_table_payloads_for_visibility_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            visibility,
        );
    }
    #[cfg(test)]
    {
        list_inline_table_payloads_for_visibility_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            visibility,
        )
    }
    #[cfg(not(test))]
    {
        let key = InlineTablePayloadCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            table_id,
            schema_id,
            snapshot_order,
        };
        let cache = inline_table_payload_cache();
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_inline_table_payloads_for_visibility_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            visibility,
        )?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

fn list_inline_table_payloads_for_visibility_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    visibility: InlineTablePayloadVisibility,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    let end_orders = inline_table_end_orders(
        kv,
        catalog,
        table_id,
        schema_id,
        snapshot_order,
        visibility.only_ended_at_or_before_snapshot(),
    )?;
    list_inline_table_payloads_for_visibility_from_end_orders(
        kv,
        catalog,
        table_id,
        schema_id,
        snapshot_order,
        &end_orders,
    )
}

fn list_inline_table_payloads_for_visibility_from_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    end_orders: &InlineTableEndOrders,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    let mut visible = BTreeMap::<CatalogOrderId, Vec<InlineTableChunkRow>>::new();
    for item in kv.scan_prefix(
        &inline_table_prefix(catalog, table_id, schema_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        if inline_chunk_visible_at(&row, end_orders, snapshot_order) {
            visible
                .entry(row.validity.begin_order)
                .or_default()
                .push(row);
        }
    }

    visible
        .into_iter()
        .map(|(begin_order, rows)| {
            assemble_inline_payload(rows).map(|payload| InlineTablePayloadRow {
                table_id,
                schema_id,
                begin_order,
                payload,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum InlineTablePayloadVisibility {
    VisibleAtSnapshot,
    UnflushedAtSnapshot,
}

impl InlineTablePayloadVisibility {
    const fn only_ended_at_or_before_snapshot(self) -> bool {
        matches!(self, Self::VisibleAtSnapshot)
    }
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineTablePayloadCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_TABLE_PAYLOAD_CACHE: OnceLock<
    BoundedCache<InlineTablePayloadCacheKey, Vec<InlineTablePayloadRow>>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_table_payload_cache()
-> &'static BoundedCache<InlineTablePayloadCacheKey, Vec<InlineTablePayloadRow>> {
    static_bounded_cache(&INLINE_TABLE_PAYLOAD_CACHE, 256)
}

#[cfg(not(test))]
pub(crate) fn invalidate_inline_table_payload_read_context(catalog: CatalogId) {
    if let Some(cache) = INLINE_TABLE_PAYLOAD_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    super::inline_end_orders::invalidate_inline_end_orders(catalog);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_inline_table_payload_read_context(_catalog: CatalogId) {}

pub(crate) fn inline_table_chunks(
    table_id: TableId,
    schema_id: SchemaId,
    begin_order: CatalogOrderId,
    payload: Vec<u8>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    let chunk_count = chunk_count(payload.len())?;
    let validity = ValidityWindow::new(begin_order, None);
    let mut rows = Vec::with_capacity(chunk_count as usize);
    for chunk_index in 0..chunk_count {
        let (start, end) = chunk_bounds(payload.len(), chunk_index);
        let chunk_payload = payload[start..end].to_vec();
        assert!(
            chunk_payload.len() <= INLINE_CHUNK_BYTES,
            "inline chunk payload exceeded configured FDB-safe chunk size"
        );
        rows.push(InlineTableChunkRow::new(
            table_id,
            schema_id,
            validity,
            chunk_index,
            chunk_count,
            chunk_payload,
        ));
    }
    Ok(rows)
}

pub(crate) fn inline_table_prefix(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::InlineTable);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key
}

pub(crate) fn inline_table_payload_prefix(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    begin_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = inline_table_prefix(catalog, table_id, schema_id);
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key
}

pub(crate) fn inline_table_chunk_key(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    begin_order: CatalogOrderId,
    chunk_index: u32,
) -> Vec<u8> {
    let mut key = inline_table_payload_prefix(catalog, table_id, schema_id, begin_order);
    key.extend_from_slice(&chunk_index.to_be_bytes());
    key
}

pub(crate) fn decode_inline_table_item(
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<InlineTableChunkRow> {
    let mut row = InlineTableChunkRow::decode(value)?;
    row.validity.begin_order = inline_table_order_from_key(
        catalog,
        row.table_id,
        row.schema_id,
        key,
        row.validity.begin_order,
    )?;
    Ok(row)
}

fn inline_table_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = inline_table_prefix(catalog, table_id, schema_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "inline table chunk key has wrong prefix".to_owned(),
        ));
    };
    let order_end = CatalogOrderId::LEN;
    if tail.len() < order_end.saturating_add(1 + 4) {
        return Err(CatalogError::InvalidKey(format!(
            "inline table chunk key order must be at least {} bytes, got {}",
            CatalogOrderId::LEN,
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] = tail[..order_end].try_into().map_err(|_| {
        CatalogError::InvalidKey("inline table chunk key order is truncated".to_owned())
    })?;
    let kind = if value_order.as_bytes() == bytes {
        value_order.kind()
    } else {
        CatalogOrderKind::FdbVersionstamp
    };
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

#[cfg(test)]
#[path = "inline_tables_tests.rs"]
mod inline_tables_tests;
