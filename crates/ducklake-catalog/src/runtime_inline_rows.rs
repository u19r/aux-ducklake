#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogResult, OrderedCatalogKv, SchemaId, SnapshotRow, TableRow,
    inline_data::{InlineTablePayloadRow, list_unflushed_inline_table_payloads_at},
    inline_read_schema::table_row_for_inline_schema,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot, ReadSnapshot},
};

#[cfg(feature = "foundationdb")]
const MAX_INLINE_GLOBAL_STATS_WORKERS: usize = 8;

#[derive(Clone)]
pub(crate) struct ReadInlineRowsPayload {
    pub(crate) table_name: String,
    pub(crate) snapshot: Option<ReadSnapshot>,
    pub(crate) include_flushed: bool,
    pub(crate) include_deleted: bool,
}

#[derive(Clone)]
pub(crate) struct InlineRowChangesPayload {
    pub(crate) table_name: String,
    pub(crate) start_snapshot: ChangeFeedStartSnapshot,
    pub(crate) end_snapshot: ChangeFeedEndSnapshot,
}

#[derive(Clone)]
pub(crate) struct InlineFlushDeletePositionsPayload {
    pub(crate) table_name: String,
    pub(crate) snapshot: ReadSnapshot,
    pub(crate) file_order: Option<String>,
    pub(crate) partition_filter: Option<String>,
    pub(crate) position_start: Option<u64>,
    pub(crate) position_end: Option<u64>,
}

pub(crate) fn read_inline_rows_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    read_inline_rows_payload_with_stats_request(
        kv,
        catalog,
        payload,
        InlineStatsRequest::Conservative,
    )
}

#[cfg(test)]
pub(crate) fn read_inline_rows_exact_stats_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    read_inline_rows_payload_with_stats_request(kv, catalog, payload, InlineStatsRequest::Exact)
}

pub(crate) fn read_inline_rows_global_stats_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    read_inline_rows_payload_with_stats_request_and_mode(
        kv,
        catalog,
        payload,
        InlineStatsRequest::Global,
        InlineStatsMode::Conservative,
    )
}

pub(crate) fn read_inline_rows_aggregate_stats_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let metric_prefix = "ReadInlineRowsForAggregateStats";
    let started = RuntimeMetricStage::start();
    let snapshot = inline_read_snapshot(kv, catalog, payload.snapshot, InlineStatsRequest::Global)?
        .ok_or_else(|| crate::CatalogError::Decode("catalog snapshot does not exist".to_owned()))?;
    record_inline_stage(metric_prefix, "Snapshot", started);
    let started = RuntimeMetricStage::start();
    let context = match InlineTableSnapshotContext::load_for_table_name(
        kv,
        catalog,
        &payload.table_name,
        snapshot,
    ) {
        Ok(context) => context,
        Err(crate::CatalogError::NotFound(_)) => {
            return Ok(b"inline_aggregate_stats\t0\n".to_vec());
        }
        Err(error) => return Err(error),
    };
    record_inline_stage(metric_prefix, "Schema", started);
    let started = RuntimeMetricStage::start();
    let mut payloads = context.payloads(kv, catalog, payload.include_flushed)?;
    record_inline_stage(metric_prefix, "Payloads", started);
    render_aggregate_inline_stats_payload(kv, catalog, &context, &mut payloads, &payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn read_inline_rows_global_stats_batch_payload(
    kv: crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payloads: Vec<ReadInlineRowsPayload>,
) -> CatalogResult<Vec<u8>> {
    let mut out = Vec::new();
    for chunk in payloads.chunks(MAX_INLINE_GLOBAL_STATS_WORKERS) {
        let result = read_inline_rows_global_stats_batch_chunk(&kv, catalog, chunk)?;
        out.extend_from_slice(&result);
    }
    Ok(out)
}

#[cfg(feature = "foundationdb")]
fn read_inline_rows_global_stats_batch_chunk(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payloads: &[ReadInlineRowsPayload],
) -> CatalogResult<Vec<u8>> {
    if payloads.is_empty() {
        return Ok(Vec::new());
    }
    if payloads.len() == 1 {
        return read_inline_rows_payload_with_stats_request_and_mode(
            kv,
            catalog,
            payloads[0].clone(),
            InlineStatsRequest::Global,
            InlineStatsMode::ExactVisible,
        );
    }

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(payloads.len());
        for payload in payloads.iter().cloned() {
            let kv = kv.clone();
            handles.push(scope.spawn(move || {
                read_inline_rows_payload_with_stats_request_and_mode(
                    &kv,
                    catalog,
                    payload,
                    InlineStatsRequest::Global,
                    InlineStatsMode::ExactVisible,
                )
            }));
        }

        let mut out = Vec::new();
        for handle in handles {
            let chunk = handle.join().map_err(|_| {
                crate::CatalogError::Backend(
                    "inline global stats worker thread panicked".to_owned(),
                )
            })??;
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    })
}

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

#[derive(Clone, Copy)]
enum InlineStatsMode {
    Conservative,
    ExactVisible,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
enum InlineStatsRequest {
    Conservative,
    Exact,
    Global,
}

#[derive(Clone)]
struct InlineTableSnapshotContext {
    snapshot: SnapshotRow,
    table: TableRow,
    schema_id: SchemaId,
    schema_table: TableRow,
}

impl InlineTableSnapshotContext {
    fn load_for_table_name(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_name: &str,
        snapshot: SnapshotRow,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = InlineTableSnapshotContextCacheKey {
                namespace: kv.catalog_cache_namespace(),
                catalog,
                table_name: table_name.to_owned(),
                snapshot_order: snapshot.order,
            };
            let cache = inline_table_snapshot_context_cache();
            if let Some(context) = cache.get_ref(&key) {
                return Ok(context);
            }
            let context = Self::load_uncached(kv, catalog, table_name, snapshot)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load_uncached(kv, catalog, table_name, snapshot)
        }
    }

    fn load_uncached(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_name: &str,
        snapshot: SnapshotRow,
    ) -> CatalogResult<Self> {
        let inline_table = InlineTableName::parse(table_name);
        let (table, schema_id) = load_inlined_table(kv, catalog, snapshot.order, inline_table)?;
        let schema_table = table_row_for_inline_schema(kv, catalog, table.table_id, schema_id.0)?;
        Ok(Self {
            snapshot,
            table,
            schema_id,
            schema_table,
        })
    }

    fn payloads(
        &self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        include_flushed: bool,
    ) -> CatalogResult<Vec<InlineTablePayloadRow>> {
        if include_flushed {
            return crate::list_inline_table_payloads_at(
                kv,
                catalog,
                self.table.table_id,
                self.schema_id,
                self.snapshot.order,
            );
        }
        list_unflushed_inline_table_payloads_at(
            kv,
            catalog,
            self.table.table_id,
            self.schema_id,
            self.snapshot.order,
        )
    }

    fn render_context(
        &self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<InlineReadRenderContext> {
        load_inline_read_render_context(
            kv,
            catalog,
            self.table.table_id,
            self.schema_id,
            self.snapshot.order,
        )
    }
}

#[cfg(not(test))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InlineTableSnapshotContextCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_name: String,
    snapshot_order: crate::CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_TABLE_SNAPSHOT_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineTableSnapshotContextCacheKey, InlineTableSnapshotContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_table_snapshot_context_cache()
-> &'static BoundedCache<InlineTableSnapshotContextCacheKey, InlineTableSnapshotContext> {
    static_bounded_cache(&INLINE_TABLE_SNAPSHOT_CONTEXT_CACHE, 256)
}

#[cfg(not(test))]
pub(crate) fn invalidate_inline_read_context(catalog: CatalogId) {
    if let Some(cache) = INLINE_TABLE_SNAPSHOT_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = INLINE_READ_RENDER_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_inline_read_context(_catalog: CatalogId) {}

mod change_feed;
mod flush_deletions;
mod flush_partition;
mod flush_sort;
mod geometry_stats;
mod read_payload;
mod stats_accumulator;
mod stats_values;
mod table_resolution;

pub(crate) use change_feed::*;
pub(crate) use flush_deletions::*;
use flush_partition::*;
use flush_sort::*;
use geometry_stats::*;
use read_payload::*;
use stats_accumulator::*;
use stats_values::*;
use table_resolution::*;

#[cfg(test)]
#[path = "runtime_inline_rows_tests.rs"]
mod runtime_inline_rows_tests;
