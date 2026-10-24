use super::{
    accumulator::reconstruct_inline_current_rows_streaming,
    capture_cache::{ByteBoundedCache, CaptureFlightRegistry},
    history::InlineHistoryRequest,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, CatalogResult,
    FdbOrderedCatalogKv, SchemaId, TableId, TableRow,
    inline_data::{InlineCurrentRow, list_inline_current_row_metadata_bounded},
    runtime_snapshots::public_snapshot_sequences_by_order,
    store::{active_runtime_read_context_id, latest_snapshot},
};

// Captures stay pinned until EndRuntimeReadContext. The byte limit bounds total
// retained state and rejects new captures when pinned entries fill the cache.
// The configured data-inlining row
// limit is only a C++ routing threshold; direct/legacy Rust writers can validly
// commit rows in batches that cross that threshold.
pub(super) const MAX_CAPTURE_CACHE_BYTES: usize = 128 * 1024 * 1024;
// Account conservatively for the fixed Rust values surrounding each captured
// row. The variable portion is charged from vector capacities below.
pub(super) const CAPTURE_ALLOCATION_OVERHEAD_BYTES: usize = 256;
const CAPTURE_ROW_FIXED_BYTES: usize =
    std::mem::size_of::<(u64, InlineCurrentRow)>() + CAPTURE_ALLOCATION_OVERHEAD_BYTES;

pub(super) fn capture_bounds_for_bytes(max_bytes: usize) -> (usize, usize) {
    (
        max_bytes / CAPTURE_ROW_FIXED_BYTES,
        max_bytes.saturating_sub(CAPTURE_ALLOCATION_OVERHEAD_BYTES),
    )
}

#[derive(Clone)]
pub(super) struct CapturedInlineRowsBySchema {
    pub(super) rows: BTreeMap<SchemaId, CapturedInlineRows>,
}

#[derive(Clone)]
pub(super) struct CapturedInlineRows {
    pub(super) rows: Vec<(u64, InlineCurrentRow)>,
    pub(super) next_row_id: u64,
}

pub(super) type CapturedInlineRowsPayload = (Vec<(u64, InlineCurrentRow)>, u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CaptureKey {
    pub(super) namespace: CatalogCacheNamespace,
    pub(super) catalog: CatalogId,
    pub(super) read_context_id: u64,
    pub(super) table_id: TableId,
    pub(super) snapshot_order: CatalogOrderId,
}

impl CaptureKey {
    pub(super) fn belongs_to(
        &self,
        namespace: CatalogCacheNamespace,
        catalog: CatalogId,
        read_context_id: u64,
    ) -> bool {
        self.namespace == namespace
            && self.catalog == catalog
            && self.read_context_id == read_context_id
    }
}

static INLINE_CURRENT_ROWS_CAPTURE_CACHE: OnceLock<
    ByteBoundedCache<CaptureKey, CapturedInlineRowsBySchema>,
> = OnceLock::new();
static INLINE_CURRENT_ROWS_CAPTURE_FLIGHTS: OnceLock<
    CaptureFlightRegistry<CaptureKey, CapturedInlineRowsBySchema>,
> = OnceLock::new();

pub(crate) fn capture_inline_current_rows_at_read_version(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table: &TableRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    let request = InlineCaptureRequest {
        kv,
        catalog,
        table,
        snapshot_order,
    };
    let Some(key) = request.find_capture_key()? else {
        return Ok(false);
    };
    let cache = inline_current_rows_capture_cache();
    if cache.contains(&key) {
        return Ok(true);
    }
    inline_current_rows_capture_flights().get_or_build(key, || {
        // Recheck after election: a delayed worker must reuse the completed capture.
        if let Some(capture) = cache.get(&key) {
            return Ok(capture);
        }
        let capture = Arc::new(request.build_capture()?);
        let weight = capture_weight(&capture)?;
        if !cache.insert_shared(key, Arc::clone(&capture), weight, true) {
            return Err(CatalogError::Backend(
                "inline read snapshot capture cannot fit while the active context is pinned"
                    .to_owned(),
            ));
        }
        Ok(capture)
    })?;
    Ok(true)
}

struct InlineCaptureRequest<'a> {
    kv: &'a FdbOrderedCatalogKv,
    catalog: CatalogId,
    table: &'a TableRow,
    snapshot_order: CatalogOrderId,
}

impl InlineCaptureRequest<'_> {
    fn find_capture_key(&self) -> CatalogResult<Option<CaptureKey>> {
        let Some(read_context_id) = active_runtime_read_context_id() else {
            return Ok(None);
        };
        if self.table.inlined_data_tables.is_empty() {
            return Ok(None);
        }
        // Reconstruct the logical snapshot without pinning an old FDB read version.
        if latest_snapshot(self.kv, self.catalog)?
            .is_none_or(|latest| latest.order != self.snapshot_order)
        {
            return Ok(None);
        }
        Ok(Some(CaptureKey {
            namespace: self.kv.catalog_cache_namespace(),
            catalog: self.catalog,
            read_context_id,
            table_id: self.table.table_id,
            snapshot_order: self.snapshot_order,
        }))
    }

    fn build_capture(&self) -> CatalogResult<CapturedInlineRowsBySchema> {
        let mut rows = BTreeMap::new();
        let sequences = public_snapshot_sequences_by_order(self.kv, self.catalog)?;
        for table in &self.table.inlined_data_tables {
            let remaining = MAX_CAPTURE_CACHE_BYTES
                .checked_sub(capture_weight_for_rows(&rows)?)
                .ok_or_else(|| {
                    CatalogError::Backend(
                        "inline read snapshot capture exceeded its byte budget".to_owned(),
                    )
                })?;
            let schema = SchemaId(table.schema_version);
            rows.insert(schema, self.capture_schema(schema, &sequences, remaining)?);
        }
        let capture = CapturedInlineRowsBySchema { rows };
        let weight = capture_weight(&capture)?;
        if weight > MAX_CAPTURE_CACHE_BYTES {
            return Err(CatalogError::Backend(format!(
                "inline read snapshot capture is {weight} bytes, over cache bound {MAX_CAPTURE_CACHE_BYTES}"
            )));
        }
        Ok(capture)
    }

    fn capture_schema(
        &self,
        schema: SchemaId,
        sequences: &BTreeMap<CatalogOrderId, u64>,
        remaining: usize,
    ) -> CatalogResult<CapturedInlineRows> {
        let (max_rows, max_payload_bytes) = capture_bounds_for_bytes(remaining);
        if max_rows == 0 {
            return Err(CatalogError::Backend(
                "inline read snapshot capture has no remaining row budget".to_owned(),
            ));
        }
        // Current-row metadata supplies canonical sequences absent from the history index.
        let current = list_inline_current_row_metadata_bounded(
            self.kv,
            self.catalog,
            self.table.table_id,
            schema,
            max_rows,
        )?;
        reconstruct_inline_current_rows_streaming(
            |accumulator| {
                InlineHistoryRequest {
                    catalog: self.catalog,
                    table: self.table.table_id,
                    schema,
                    snapshot: self.snapshot_order,
                }
                .stream(self.kv, accumulator)
            },
            self.snapshot_order,
            sequences,
            &current,
            max_rows,
            max_payload_bytes,
            remaining,
        )
    }
}

pub(super) fn ensure_inline_capture_bound(
    row_count: usize,
    payload_bytes: usize,
    max_rows: usize,
    max_payload_bytes: usize,
) -> CatalogResult<()> {
    if row_count > max_rows {
        return Err(CatalogError::Backend(format!(
            "inline read snapshot has {row_count} current rows, over capture budget {max_rows}"
        )));
    }
    if payload_bytes > max_payload_bytes {
        return Err(CatalogError::Backend(format!(
            "inline read snapshot payload is {payload_bytes} bytes, over capture budget \
             {max_payload_bytes}"
        )));
    }
    Ok(())
}

pub(crate) fn captured_inline_current_rows(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> Option<CapturedInlineRowsPayload> {
    let read_context_id = active_runtime_read_context_id()?;
    let key = CaptureKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        read_context_id,
        table_id,
        snapshot_order,
    };
    let capture = inline_current_rows_capture_cache().get(&key)?;
    let capture = capture.rows.get(&schema_id)?.clone();
    let rows = capture
        .rows
        .into_iter()
        .filter(|(_, row)| row.begin_order <= snapshot_order)
        .collect::<Vec<_>>();
    let next_row_id = rows
        .iter()
        .map(|(row_id, _)| row_id.saturating_add(1))
        .max()
        .unwrap_or(capture.next_row_id);
    Some((rows, next_row_id))
}

/// Select the immutable capture for a context-bound read, or the legacy
/// latest-only path when there is no active context. Any required-capture miss
/// fails closed because reading current FDB rows could combine two snapshots.
pub(crate) fn captured_inline_current_rows_or_latest<F>(
    captured: Option<CapturedInlineRowsPayload>,
    current_snapshot_capture_required: bool,
    fallback: F,
) -> CatalogResult<Option<CapturedInlineRowsPayload>>
where
    F: FnOnce() -> CatalogResult<Option<CapturedInlineRowsPayload>>,
{
    match captured {
        Some(captured) => Ok(Some(captured)),
        None if current_snapshot_capture_required => Err(CatalogError::Backend(
            "inline read snapshot capture is unavailable for the active runtime read context"
                .to_owned(),
        )),
        None => fallback(),
    }
}

pub(crate) fn end_runtime_read_context(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    read_context_id: u64,
) {
    let namespace = kv.catalog_cache_namespace();
    crate::runtime_file_listing::end_file_listing_read_context(namespace, catalog, read_context_id);
    crate::runtime_foundationdb_inline::end_inline_global_stats_batch_read_context(
        namespace,
        catalog,
        read_context_id,
    );
    crate::store::end_runtime_read_context(namespace, catalog, read_context_id);
    if let Some(cache) = INLINE_CURRENT_ROWS_CAPTURE_CACHE.get() {
        cache.retain(|key| !key.belongs_to(namespace, catalog, read_context_id));
    }
}

fn inline_current_rows_capture_cache()
-> &'static ByteBoundedCache<CaptureKey, CapturedInlineRowsBySchema> {
    INLINE_CURRENT_ROWS_CAPTURE_CACHE.get_or_init(|| ByteBoundedCache::new(MAX_CAPTURE_CACHE_BYTES))
}

fn inline_current_rows_capture_flights()
-> &'static CaptureFlightRegistry<CaptureKey, CapturedInlineRowsBySchema> {
    INLINE_CURRENT_ROWS_CAPTURE_FLIGHTS.get_or_init(CaptureFlightRegistry::new)
}

pub(super) fn capture_weight(capture: &CapturedInlineRowsBySchema) -> CatalogResult<usize> {
    capture_weight_for_rows(&capture.rows)
}

fn capture_weight_for_rows(
    rows_by_schema: &BTreeMap<SchemaId, CapturedInlineRows>,
) -> CatalogResult<usize> {
    let mut weight = CAPTURE_ALLOCATION_OVERHEAD_BYTES
        .checked_add(std::mem::size_of::<CapturedInlineRowsBySchema>())
        .ok_or_else(|| {
            CatalogError::Backend("inline read snapshot capture weight overflowed".to_owned())
        })?;
    for rows in rows_by_schema.values() {
        let rows_storage = rows
            .rows
            .capacity()
            .checked_mul(std::mem::size_of::<(u64, InlineCurrentRow)>())
            .ok_or_else(|| {
                CatalogError::Backend("inline read snapshot capture weight overflowed".to_owned())
            })?;
        weight = weight
            .checked_add(CAPTURE_ALLOCATION_OVERHEAD_BYTES)
            .and_then(|weight| weight.checked_add(std::mem::size_of::<CapturedInlineRows>()))
            .and_then(|weight| weight.checked_add(rows_storage))
            .ok_or_else(|| {
                CatalogError::Backend("inline read snapshot capture weight overflowed".to_owned())
            })?;
        for (_, row) in &rows.rows {
            weight = weight
                .checked_add(CAPTURE_ALLOCATION_OVERHEAD_BYTES)
                .and_then(|weight| weight.checked_add(row.payload.capacity()))
                .ok_or_else(|| {
                    CatalogError::Backend(
                        "inline read snapshot capture weight overflowed".to_owned(),
                    )
                })?;
        }
    }
    Ok(weight)
}
