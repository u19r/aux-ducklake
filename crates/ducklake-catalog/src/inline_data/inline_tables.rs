use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileRow, DuckLakeSnapshotId, KvBatch,
    MutableCatalogKv, OrderedCatalogKv, RangeDirection, SchemaId, SnapshotRow, TableId, TableRow,
    ValidityWindow, commit_append_data_files,
    conflict_watermarks::{stage_max_catalog_id_watermark, stage_max_file_id_watermark},
    ids::{CatalogOrderId, CatalogOrderKind, RawSnapshotSequence, incomplete_fdb_order},
    inline_change_feed::{InlineRowChangeKind, stage_inline_row_changes_for_payload},
    inline_data::{
        INLINE_CHUNK_BYTES, chunk_bounds, chunk_count, decode_inline_end_order,
        encode_inline_end_order, validate_contiguous_chunks, validate_inline_table_rows_fit_fdb,
    },
    keys::{KeyFamily, family_prefix, inline_table_end_key, table_object_key},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    store::{
        invalidate_runtime_read_context, latest_snapshot, snapshot_by_raw_sequence,
        snapshot_row_for_next_sequence, stage_snapshot,
    },
    table_store::{load_current_table_row, stage_current_table_row, stage_table_visibility_row},
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
fn record_inline_tables_stage(operation: &'static str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_inline_tables_stage(_operation: &'static str, _started: RuntimeMetricStage) {}

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
pub enum InlineTablePayloadCommit {
    Inlined(Vec<InlineTableChunkRow>),
    FileBacked(Vec<DataFileRow>),
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

pub fn route_inline_table_payload_or_data_file(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    payload: Vec<u8>,
    fallback_file: DataFileRow,
) -> CatalogResult<InlineTablePayloadCommit> {
    if fallback_file.table_id != table_id {
        return Err(CatalogError::InvalidMutation(format!(
            "inline fallback file table {} does not match inline table {}",
            fallback_file.table_id.0, table_id.0
        )));
    }
    if validate_inline_table_rows_fit_fdb(&payload).is_ok() {
        return register_inline_table_payload(kv, catalog, table_id, schema_id, payload)
            .map(InlineTablePayloadCommit::Inlined);
    }
    commit_append_data_files(kv, catalog, vec![fallback_file])
        .map(InlineTablePayloadCommit::FileBacked)
}

pub(crate) fn stage_flush_inline_table_payloads(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    flush: InlineTableFlush,
    end_order: CatalogOrderId,
) -> CatalogResult<()> {
    crate::inline_data::stage_flush_inline_file_deletions(kv, batch, catalog, flush, end_order)?;
    for row in flush_inline_table_payload_rows(kv, catalog, flush, end_order)? {
        if row.chunk_index == 0 {
            batch.put(
                inline_table_end_key(
                    catalog,
                    flush.table_id,
                    end_order,
                    flush.schema_id,
                    row.validity.begin_order,
                ),
                row.validity.begin_order.as_bytes().to_vec(),
            );
        }
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
            .or_else(|| end_orders.get(&row.validity.begin_order).copied())
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

pub(crate) fn inline_table_flushes_ending_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    end_order: CatalogOrderId,
) -> CatalogResult<std::collections::BTreeSet<TableId>> {
    let prefix = family_prefix(catalog, KeyFamily::EndOrder);
    let mut tables = std::collections::BTreeSet::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let tail = &item.key[prefix.len()..];
        let minimum_len = 8 + 1 + CatalogOrderId::LEN + 1 + 2 + 8 + 1 + CatalogOrderId::LEN;
        if tail.len() != minimum_len || tail[8] != b'/' {
            continue;
        }
        let order_start = 9;
        let object_start = order_start + CatalogOrderId::LEN + 1;
        if tail[order_start + CatalogOrderId::LEN] != b'/'
            || tail[object_start] != b'i'
            || tail[object_start + 1] != b'/'
        {
            continue;
        }
        let marker_order = CatalogOrderId::from_bytes(
            end_order.kind(),
            tail[order_start..order_start + CatalogOrderId::LEN]
                .try_into()
                .map_err(|_| {
                    CatalogError::InvalidKey("inline end order is truncated".to_owned())
                })?,
        );
        if marker_order == end_order {
            tables.insert(TableId(u64::from_be_bytes(tail[0..8].try_into().map_err(
                |_| CatalogError::InvalidKey("inline end table id is truncated".to_owned()),
            )?)));
        }
    }
    Ok(tables)
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

pub(crate) fn list_unflushed_inline_table_payloads_at_with_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    end_orders: &BTreeMap<CatalogOrderId, CatalogOrderId>,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
    let started = RuntimeMetricStage::start();
    let rows = list_inline_table_payloads_for_visibility_from_end_orders(
        kv,
        catalog,
        table_id,
        schema_id,
        snapshot_order,
        end_orders,
    );
    record_inline_tables_stage(
        "method.inline_tables.list_unflushed_inline_table_payloads_at_with_end_orders",
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
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            visibility,
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
    end_orders: &BTreeMap<CatalogOrderId, CatalogOrderId>,
) -> CatalogResult<Vec<InlineTablePayloadRow>> {
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
    let rows = visible
        .into_iter()
        .map(|(begin_order, rows)| {
            assemble_inline_payload(rows).map(|payload| InlineTablePayloadRow {
                table_id,
                schema_id,
                begin_order,
                payload,
            })
        })
        .collect();
    rows
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
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    visibility: InlineTablePayloadVisibility,
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
    if let Some(cache) = INLINE_TABLE_END_ORDERS_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_inline_table_payload_read_context(_catalog: CatalogId) {}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineTableEndOrdersCacheKey {
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    only_ended_at_or_before_snapshot: bool,
}

#[cfg(not(test))]
static INLINE_TABLE_END_ORDERS_CACHE: OnceLock<
    BoundedCache<InlineTableEndOrdersCacheKey, BTreeMap<CatalogOrderId, CatalogOrderId>>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_table_end_orders_cache()
-> &'static BoundedCache<InlineTableEndOrdersCacheKey, BTreeMap<CatalogOrderId, CatalogOrderId>> {
    static_bounded_cache(&INLINE_TABLE_END_ORDERS_CACHE, 256)
}

pub(crate) fn inline_table_end_orders_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeMap<CatalogOrderId, CatalogOrderId>> {
    inline_table_end_orders(kv, catalog, table_id, schema_id, snapshot_order, true)
}

pub(crate) fn inline_table_all_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    order_kind_source: CatalogOrderId,
) -> CatalogResult<BTreeMap<CatalogOrderId, CatalogOrderId>> {
    inline_table_end_orders(kv, catalog, table_id, schema_id, order_kind_source, false)
}

fn inline_table_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    only_ended_at_or_before_snapshot: bool,
) -> CatalogResult<BTreeMap<CatalogOrderId, CatalogOrderId>> {
    #[cfg(test)]
    {
        inline_table_end_orders_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            only_ended_at_or_before_snapshot,
        )
    }
    #[cfg(not(test))]
    {
        let key = InlineTableEndOrdersCacheKey {
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            only_ended_at_or_before_snapshot,
        };
        let cache = inline_table_end_orders_cache();
        if let Some(end_orders) = cache.get(key) {
            return Ok(end_orders);
        }
        let end_orders = inline_table_end_orders_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            only_ended_at_or_before_snapshot,
        )?;
        cache.insert(key, end_orders.clone());
        Ok(end_orders)
    }
}

fn inline_table_end_orders_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    only_ended_at_or_before_snapshot: bool,
) -> CatalogResult<BTreeMap<CatalogOrderId, CatalogOrderId>> {
    let started = RuntimeMetricStage::start();
    let mut prefix = family_prefix(catalog, KeyFamily::EndOrder);
    prefix.extend_from_slice(&table_id.0.to_be_bytes());
    prefix.push(b'/');
    let mut end_orders = BTreeMap::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let Some((end_order, marker_schema_id, begin_order)) =
            decode_inline_end_marker(&prefix, &item.key, snapshot_order)?
        else {
            continue;
        };
        if marker_schema_id == schema_id
            && (!only_ended_at_or_before_snapshot || end_order <= snapshot_order)
        {
            end_orders.insert(begin_order, end_order);
        }
    }
    record_inline_tables_stage("method.inline_tables.inline_table_end_orders", started);
    Ok(end_orders)
}

pub(crate) fn inline_chunk_visible_at(
    row: &InlineTableChunkRow,
    end_orders: &BTreeMap<CatalogOrderId, CatalogOrderId>,
    snapshot_order: CatalogOrderId,
) -> bool {
    let end_order = row
        .validity
        .end_order
        .or_else(|| end_orders.get(&row.validity.begin_order).copied());
    row.validity.begin_order <= snapshot_order
        && end_order.is_none_or(|end_order| snapshot_order < end_order)
}

fn decode_inline_end_marker(
    prefix: &[u8],
    key: &[u8],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<(CatalogOrderId, SchemaId, CatalogOrderId)>> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Ok(None);
    };
    let expected_len = CatalogOrderId::LEN + 1 + 2 + 8 + 1 + CatalogOrderId::LEN;
    if tail.len() != expected_len {
        return Ok(None);
    }
    let object_start = CatalogOrderId::LEN + 1;
    if tail[CatalogOrderId::LEN] != b'/'
        || tail[object_start] != b'i'
        || tail[object_start + 1] != b'/'
        || tail[object_start + 10] != b'/'
    {
        return Ok(None);
    }
    let end_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[..CatalogOrderId::LEN]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline end order is truncated".to_owned()))?,
    );
    let schema_start = object_start + 2;
    let begin_start = schema_start + 8 + 1;
    let schema_id = SchemaId(u64::from_be_bytes(
        tail[schema_start..schema_start + 8]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline end schema is truncated".to_owned()))?,
    ));
    let begin_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[begin_start..]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline begin order is truncated".to_owned()))?,
    );
    Ok(Some((end_order, schema_id, begin_order)))
}

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

pub(crate) fn assemble_inline_payload(
    mut rows: Vec<InlineTableChunkRow>,
) -> CatalogResult<Vec<u8>> {
    if rows.is_empty() {
        return Err(CatalogError::Decode(
            "inline payload has no chunks".to_owned(),
        ));
    }
    rows.sort_by_key(|row| row.chunk_index);
    let chunk_count = rows[0].chunk_count;
    validate_contiguous_chunks(rows.len(), chunk_count)?;
    let payload_len = rows.iter().map(|row| row.payload.len()).sum();
    let mut payload = Vec::with_capacity(payload_len);
    for (expected_index, row) in rows.into_iter().enumerate() {
        if row.chunk_count != chunk_count || row.chunk_index != expected_index as u32 {
            return Err(CatalogError::Decode(
                "inline payload chunks are not contiguous".to_owned(),
            ));
        }
        payload.extend_from_slice(&row.payload);
    }
    validate_inline_table_rows_fit_fdb(&payload)?;
    Ok(payload)
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
