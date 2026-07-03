#[cfg(not(test))]
use std::sync::OnceLock;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    ops::Bound,
};

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_protocol::RuntimeCatalogBackend;
use crate::{
    AttachedDataFile, CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId,
    InlineRowChangeKind, OrderedCatalogKv, SchemaId, SnapshotRow, TableId, TableRow,
    inline_change_feed::data_file_covers_inline_begin,
    inline_change_feed::list_inline_deleted_row_changes_for_schema,
    inline_column_types::inline_columns_payload,
    inline_data::{InlineTablePayloadRow, list_unflushed_inline_table_payloads_at},
    inline_read_schema::table_row_for_inline_schema,
    latest_snapshot, list_current_data_files_with_deletes, list_data_files_with_deletes_at,
    list_inline_row_payload_changes, list_snapshots, list_tables_at, load_table_at,
    public_snapshot_sequence_for_order,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot, ReadSnapshot},
    runtime_snapshots::{
        ducklake_snapshot_order_span as cached_ducklake_snapshot_order_span,
        public_snapshot_order_span, public_snapshot_sequences_by_order,
    },
    runtime_tabular_payload::parse_u64_field,
    snapshot_by_ducklake_sequence, snapshot_by_public_sequence,
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
            return Ok(context);
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
    catalog: CatalogId,
    table_name: String,
    snapshot_order: CatalogOrderId,
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

fn read_inline_rows_payload_with_stats_request(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
    stats_request: InlineStatsRequest,
) -> CatalogResult<Vec<u8>> {
    read_inline_rows_payload_with_stats_request_and_mode(
        kv,
        catalog,
        payload,
        stats_request,
        inline_stats_mode_for_request(stats_request),
    )
}

fn read_inline_rows_payload_with_stats_request_and_mode(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
    stats_request: InlineStatsRequest,
    stats_mode: InlineStatsMode,
) -> CatalogResult<Vec<u8>> {
    let metric_prefix = inline_read_metric_prefix(stats_request);
    let started = RuntimeMetricStage::start();
    let snapshot = inline_read_snapshot(kv, catalog, payload.snapshot, stats_request)?
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
        Err(crate::CatalogError::NotFound(_))
            if matches!(stats_request, InlineStatsRequest::Global) =>
        {
            return Ok(empty_global_inline_stats_payload());
        }
        Err(error) => return Err(error),
    };
    record_inline_stage(metric_prefix, "Schema", started);
    let started = RuntimeMetricStage::start();
    let mut payloads = context.payloads(kv, catalog, payload.include_flushed)?;
    record_inline_stage(metric_prefix, "Payloads", started);
    if payloads.is_empty() {
        let started = RuntimeMetricStage::start();
        let mut out = inline_columns_payload(&context.schema_table)?;
        out.push_str("inline_payload_count=0\n");
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table)
            .append_to(&mut out)?;
        record_inline_stage(metric_prefix, "Render", started);
        return Ok(out.into_bytes());
    }
    if matches!(stats_request, InlineStatsRequest::Global) {
        return render_global_inline_stats_payload(
            kv,
            catalog,
            &context,
            &mut payloads,
            &payload,
            stats_mode,
        );
    }
    let started = RuntimeMetricStage::start();
    let render_context = context.render_context(kv, catalog)?;
    record_inline_stage(metric_prefix, "Context", started);
    let started = RuntimeMetricStage::start();
    if !payload.include_flushed {
        retain_unmaterialized_inline_payloads(&render_context.materialized_files, &mut payloads);
    }
    record_inline_stage(metric_prefix, "Retain", started);
    let started = RuntimeMetricStage::start();
    let mut out = inline_columns_payload(&context.schema_table)?;
    out.push_str(&format!("inline_payload_count={}\n", payloads.len()));
    let mut catalog_stats =
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table);
    for inline_payload in payloads {
        append_snapshot_aware_rows(
            kv,
            catalog,
            inline_payload.begin_order,
            &inline_payload.payload,
            payload.include_deleted,
            stats_mode,
            &render_context,
            &mut catalog_stats,
            &mut out,
        )?;
    }
    catalog_stats.append_to(&mut out)?;
    record_inline_stage(metric_prefix, "Render", started);
    Ok(out.into_bytes())
}

fn empty_global_inline_stats_payload() -> Vec<u8> {
    b"inline_payload_count=0\ninline_table_stats\t0\t0\n".to_vec()
}

fn render_global_inline_stats_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    context: &InlineTableSnapshotContext,
    payloads: &mut Vec<InlineTablePayloadRow>,
    payload: &ReadInlineRowsPayload,
    stats_mode: InlineStatsMode,
) -> CatalogResult<Vec<u8>> {
    let metric_prefix = "ReadInlineRowsForGlobalStats";
    let started = RuntimeMetricStage::start();
    let materialized_files =
        InlineMaterializedFiles::load(kv, catalog, context.table.table_id, context.snapshot.order)?;
    let deleted_rows =
        if !payload.include_deleted && matches!(stats_mode, InlineStatsMode::ExactVisible) {
            Some(RawInlineDeletionIndex::load(
                kv,
                catalog,
                context.table.table_id,
                context.schema_id,
                context.snapshot.order,
            )?)
        } else {
            None
        };
    record_inline_stage(metric_prefix, "Context", started);
    let started = RuntimeMetricStage::start();
    record_inline_stage(metric_prefix, "Retain", started);
    let started = RuntimeMetricStage::start();
    let mut out = inline_columns_payload(&context.schema_table)?;
    out.push_str(&format!("inline_payload_count={}\n", payloads.len()));
    let mut catalog_stats =
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table);
    for inline_payload in payloads {
        accumulate_global_inline_stats(
            &inline_payload.payload,
            inline_payload.begin_order,
            payload.include_deleted,
            payload.include_flushed,
            stats_mode,
            &materialized_files,
            deleted_rows.as_ref(),
            &mut catalog_stats,
        )?;
    }
    if deleted_rows
        .as_ref()
        .is_some_and(|deleted_rows| deleted_rows.has_deletions())
    {
        catalog_stats.clear_min_max();
    }
    catalog_stats.append_to(&mut out)?;
    record_inline_stage(metric_prefix, "Render", started);
    Ok(out.into_bytes())
}

fn render_aggregate_inline_stats_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    context: &InlineTableSnapshotContext,
    payloads: &mut Vec<InlineTablePayloadRow>,
    payload: &ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let metric_prefix = "ReadInlineRowsForAggregateStats";
    let started = RuntimeMetricStage::start();
    let materialized_files =
        InlineMaterializedFiles::load(kv, catalog, context.table.table_id, context.snapshot.order)?;
    let stats_mode = inline_stats_mode_for_request(InlineStatsRequest::Global);
    let deleted_rows =
        if !payload.include_deleted && matches!(stats_mode, InlineStatsMode::ExactVisible) {
            Some(RawInlineDeletionIndex::load(
                kv,
                catalog,
                context.table.table_id,
                context.schema_id,
                context.snapshot.order,
            )?)
        } else {
            None
        };
    record_inline_stage(metric_prefix, "Context", started);
    let started = RuntimeMetricStage::start();
    let mut catalog_stats =
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table);
    for inline_payload in payloads {
        accumulate_global_inline_stats(
            &inline_payload.payload,
            inline_payload.begin_order,
            payload.include_deleted,
            payload.include_flushed,
            stats_mode,
            &materialized_files,
            deleted_rows.as_ref(),
            &mut catalog_stats,
        )?;
    }
    let mut out = String::new();
    writeln!(
        out,
        "inline_active_delete_count\t{}",
        usize::from(
            deleted_rows
                .as_ref()
                .is_some_and(|deleted_rows| deleted_rows.has_deletions())
        )
    )
    .map_err(|error| {
        crate::CatalogError::Decode(format!(
            "failed to render inline active delete count: {error}"
        ))
    })?;
    catalog_stats.append_aggregate_to(&mut out)?;
    record_inline_stage(metric_prefix, "Render", started);
    Ok(out.into_bytes())
}

fn inline_read_metric_prefix(stats_request: InlineStatsRequest) -> &'static str {
    match stats_request {
        InlineStatsRequest::Conservative | InlineStatsRequest::Exact => "ReadInlineRows",
        InlineStatsRequest::Global => "ReadInlineRowsForGlobalStats",
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_inline_stage(prefix: &str, stage: &str, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!("{prefix}Stage{stage}"),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_inline_stage(_prefix: &str, _stage: &str, _started: RuntimeMetricStage) {}

fn inline_read_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: Option<ReadSnapshot>,
    stats_request: InlineStatsRequest,
) -> CatalogResult<Option<crate::SnapshotRow>> {
    let Some(snapshot) = snapshot else {
        return latest_snapshot(kv, catalog);
    };
    let snapshot_id = snapshot.public_id();
    let latest = latest_snapshot(kv, catalog)?;
    if let Some(snapshot) = latest.clone() {
        let latest_sequence = DuckLakeSnapshotId(snapshot.sequence.0);
        if snapshot_id >= latest_sequence {
            return Ok(Some(snapshot));
        }
    }
    if matches!(stats_request, InlineStatsRequest::Global)
        && let Some(snapshot) = snapshot_by_public_sequence(kv, catalog, snapshot_id)?
    {
        return Ok(Some(snapshot));
    }
    snapshot_by_ducklake_sequence(kv, catalog, snapshot_id).map(|snapshot| snapshot.or(latest))
}

fn inline_stats_mode_for_request(stats_request: InlineStatsRequest) -> InlineStatsMode {
    match stats_request {
        InlineStatsRequest::Conservative => InlineStatsMode::Conservative,
        InlineStatsRequest::Exact => InlineStatsMode::ExactVisible,
        InlineStatsRequest::Global => InlineStatsMode::ExactVisible,
    }
}

pub(crate) fn inline_flush_delete_positions_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: InlineFlushDeletePositionsPayload,
) -> CatalogResult<Vec<u8>> {
    let snapshot = inline_read_snapshot(
        kv,
        catalog,
        Some(payload.snapshot),
        InlineStatsRequest::Conservative,
    )?
    .ok_or_else(|| missing_snapshot(payload.snapshot.public_id()))?;
    let context = InlineTableSnapshotContext::load_for_table_name(
        kv,
        catalog,
        &payload.table_name,
        snapshot,
    )?;
    let payloads = context.payloads(kv, catalog, false)?;
    let render_context = context.render_context(kv, catalog)?;
    let mut rows = Vec::new();
    for inline_payload in payloads {
        let begin_snapshot = render_context
            .snapshot_sequence(inline_payload.begin_order)
            .ok_or_else(|| missing_snapshot_order(inline_payload.begin_order))?;
        collect_inline_flush_rows(
            &inline_payload.payload,
            begin_snapshot,
            &render_context.deleted_rows,
            &mut rows,
        )?;
    }
    let filter = InlineFlushPartitionFilter::parse(
        payload.partition_filter.as_deref(),
        &context.schema_table,
    )?;
    rows.retain(|row| filter.matches(row));
    let file_order =
        InlineFlushFileOrder::parse(payload.file_order.as_deref(), &context.schema_table)?;
    rows.sort_by(|left, right| file_order.compare(left, right));
    let position_start = payload.position_start.unwrap_or(0);
    let position_end = payload.position_end.unwrap_or(u64::MAX);
    let delete_count = rows
        .iter()
        .enumerate()
        .filter(|(position, row)| {
            row.end_snapshot.is_some()
                && position_in_flush_window(*position, position_start, position_end)
        })
        .count();
    if delete_count == 0 {
        return Ok(b"delete_position_count=0\n".to_vec());
    }

    let mut out = format!("delete_position_count={delete_count}\n");
    for (position, row) in rows.iter().enumerate() {
        if !position_in_flush_window(position, position_start, position_end) {
            continue;
        }
        if let Some(end_snapshot) = row.end_snapshot {
            writeln!(out, "delete_position\t{end_snapshot}\t{position}").map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render inline delete position: {error}"
                ))
            })?;
        }
    }
    Ok(out.into_bytes())
}

fn position_in_flush_window(position: usize, start: u64, end: u64) -> bool {
    let Ok(position) = u64::try_from(position) else {
        return false;
    };
    start <= position && position < end
}

#[derive(Clone)]
struct InlineDeletionIndex {
    deleted_at: BTreeMap<u64, BTreeSet<u64>>,
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineDeletionIndexCacheKey {
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_DELETION_INDEX_CACHE: OnceLock<
    BoundedCache<InlineDeletionIndexCacheKey, InlineDeletionIndex>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_deletion_index_cache()
-> &'static BoundedCache<InlineDeletionIndexCacheKey, InlineDeletionIndex> {
    static_bounded_cache(&INLINE_DELETION_INDEX_CACHE, 512)
}

impl InlineDeletionIndex {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
        snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = InlineDeletionIndexCacheKey {
                catalog,
                table_id,
                schema_id,
                snapshot_order,
            };
            let cache = inline_deletion_index_cache();
            if let Some(index) = cache.get(key) {
                return Ok(index);
            }
            let index = Self::load_uncached(
                kv,
                catalog,
                table_id,
                schema_id,
                snapshot_order,
                snapshot_sequences,
            )?;
            cache.insert(key, index.clone());
            return Ok(index);
        }
        #[cfg(test)]
        {
            Self::load_uncached(
                kv,
                catalog,
                table_id,
                schema_id,
                snapshot_order,
                snapshot_sequences,
            )
        }
    }

    fn load_uncached(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
        snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    ) -> CatalogResult<Self> {
        let start_order =
            CatalogOrderId::from_bytes(snapshot_order.kind(), [0; CatalogOrderId::LEN]);
        let read_snapshot = snapshot_sequences
            .get(&snapshot_order)
            .copied()
            .ok_or_else(|| missing_snapshot_order(snapshot_order))?;
        let mut deleted_at = BTreeMap::<u64, BTreeSet<u64>>::new();
        for change in list_inline_deleted_row_changes_for_schema(
            kv,
            catalog,
            table_id,
            schema_id,
            start_order,
            snapshot_order,
        )? {
            let delete_snapshot = snapshot_sequences
                .get(&change.order)
                .copied()
                .ok_or_else(|| missing_snapshot_order(change.order))?;
            if delete_snapshot > read_snapshot {
                continue;
            }
            deleted_at
                .entry(change.row_id)
                .or_default()
                .insert(delete_snapshot);
        }
        Ok(Self { deleted_at })
    }

    fn hides_row_version(&self, row_id: u64, begin_snapshot: u64) -> bool {
        self.end_snapshot_for(row_id, begin_snapshot).is_some()
    }

    fn end_snapshot_for(&self, row_id: u64, begin_snapshot: u64) -> Option<u64> {
        self.deleted_at.get(&row_id).and_then(|delete_snapshots| {
            delete_snapshots
                .range((begin_snapshot + 1)..)
                .next()
                .copied()
        })
    }
}

struct RawInlineDeletionIndex {
    deleted_at: BTreeMap<u64, BTreeSet<CatalogOrderId>>,
}

impl RawInlineDeletionIndex {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let start_order =
            CatalogOrderId::from_bytes(snapshot_order.kind(), [0; CatalogOrderId::LEN]);
        let mut deleted_at = BTreeMap::<u64, BTreeSet<CatalogOrderId>>::new();
        for change in list_inline_deleted_row_changes_for_schema(
            kv,
            catalog,
            table_id,
            schema_id,
            start_order,
            snapshot_order,
        )? {
            deleted_at
                .entry(change.row_id)
                .or_default()
                .insert(change.order);
        }
        Ok(Self { deleted_at })
    }

    fn hides_row_version(&self, row_id: u64, begin_order: CatalogOrderId) -> bool {
        self.deleted_at.get(&row_id).is_some_and(|delete_orders| {
            delete_orders
                .range((Bound::Excluded(begin_order), Bound::Unbounded))
                .next()
                .is_some()
        })
    }

    fn has_deletions(&self) -> bool {
        !self.deleted_at.is_empty()
    }
}

fn accumulate_global_inline_stats(
    payload: &[u8],
    begin_order: CatalogOrderId,
    include_deleted: bool,
    include_materialized: bool,
    stats_mode: InlineStatsMode,
    materialized_files: &InlineMaterializedFiles,
    deleted_rows: Option<&RawInlineDeletionIndex>,
    catalog_stats: &mut InlineCatalogStats,
) -> CatalogResult<()> {
    let rows = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        catalog_stats.observe_row_id(row_id);
        if !include_materialized && materialized_files.materializes(row_id, begin_order) {
            continue;
        }
        if matches!(stats_mode, InlineStatsMode::ExactVisible) {
            if !include_deleted
                && deleted_rows.is_some_and(|index| index.hides_row_version(row_id, begin_order))
            {
                continue;
            }
        }
        catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
    }
    Ok(())
}

struct InlineFlushRow {
    row_id: u64,
    begin_snapshot: u64,
    end_snapshot: Option<u64>,
    values: Vec<String>,
}

struct InlineFlushPartitionFilter {
    predicates: Vec<InlineFlushPartitionPredicate>,
}

struct InlineFlushPartitionPredicate {
    column_index: usize,
    transform: InlineFlushPartitionTransform,
    value: Option<String>,
}

#[derive(Clone, Copy)]
enum InlineFlushPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
}

impl InlineFlushPartitionFilter {
    fn parse(filter: Option<&str>, schema_table: &TableRow) -> CatalogResult<Self> {
        let Some(filter) = filter.map(str::trim).filter(|filter| !filter.is_empty()) else {
            return Ok(Self {
                predicates: Vec::new(),
            });
        };
        let mut predicates = Vec::new();
        for clause in filter
            .split(" AND ")
            .map(str::trim)
            .filter(|clause| !clause.is_empty())
        {
            predicates.push(InlineFlushPartitionPredicate::parse(clause, schema_table)?);
        }
        Ok(Self { predicates })
    }

    fn matches(&self, row: &InlineFlushRow) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.matches(row))
    }
}

impl InlineFlushPartitionPredicate {
    fn parse(clause: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        if let Some(left) = clause.strip_suffix(" IS NULL") {
            let (column_index, transform) = parse_partition_left(left.trim(), schema_table)?;
            return Ok(Self {
                column_index,
                transform,
                value: None,
            });
        }
        let Some((left, right)) = clause.split_once(" = ") else {
            return Err(crate::CatalogError::Decode(format!(
                "unsupported inline flush partition filter: {clause}"
            )));
        };
        let (column_index, transform) = parse_partition_left(left.trim(), schema_table)?;
        Ok(Self {
            column_index,
            transform,
            value: Some(unquote_sql_partition_value(right.trim())),
        })
    }

    fn matches(&self, row: &InlineFlushRow) -> bool {
        let value = row
            .values
            .get(self.column_index)
            .and_then(|encoded| decode_inline_partition_value(encoded));
        let value = match (&self.transform, value) {
            (_, None) => None,
            (InlineFlushPartitionTransform::Identity, Some(value)) => Some(value),
            (InlineFlushPartitionTransform::Year, Some(value)) => date_part(&value, 0, 4),
            (InlineFlushPartitionTransform::Month, Some(value)) => date_part(&value, 5, 7),
            (InlineFlushPartitionTransform::Day, Some(value)) => date_part(&value, 8, 10),
            (InlineFlushPartitionTransform::Hour, Some(value)) => date_part(&value, 11, 13),
        };
        value == self.value
    }
}

fn parse_partition_left(
    left: &str,
    schema_table: &TableRow,
) -> CatalogResult<(usize, InlineFlushPartitionTransform)> {
    let left = unwrap_partition_cast(left.trim());
    for (prefix, transform) in [
        ("year(", InlineFlushPartitionTransform::Year),
        ("month(", InlineFlushPartitionTransform::Month),
        ("day(", InlineFlushPartitionTransform::Day),
        ("hour(", InlineFlushPartitionTransform::Hour),
    ] {
        if let Some(column) = left
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(')'))
        {
            return Ok((partition_column_index(schema_table, column)?, transform));
        }
    }
    Ok((
        partition_column_index(schema_table, left)?,
        InlineFlushPartitionTransform::Identity,
    ))
}

fn unwrap_partition_cast(left: &str) -> &str {
    let Some(inner) = left
        .strip_prefix("CAST(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return left;
    };
    inner
        .split_once(" AS ")
        .map(|(column, _)| column.trim())
        .unwrap_or(left)
}

fn partition_column_index(schema_table: &TableRow, column_name: &str) -> CatalogResult<usize> {
    let column_name = unquote_identifier(unwrap_partition_cast(column_name.trim()));
    schema_table
        .columns
        .iter()
        .filter(|column| column.parent_id.is_none())
        .position(|column| column.name.eq_ignore_ascii_case(&column_name))
        .ok_or_else(|| {
            crate::CatalogError::Decode(format!(
                "inline flush partition filter references unknown column {column_name}"
            ))
        })
}

fn unquote_identifier(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return value[1..value.len() - 1].replace("\"\"", "\"");
    }
    value.to_owned()
}

fn unquote_sql_partition_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return value[1..value.len() - 1].replace("''", "'");
    }
    value.to_owned()
}

fn date_part(value: &str, start: usize, end: usize) -> Option<String> {
    value.get(start..end).map(|part| part.to_owned())
}

fn decode_inline_partition_value(encoded: &str) -> Option<String> {
    if encoded == "n:" {
        return None;
    }
    if encoded == "b:1" {
        return Some("true".to_owned());
    }
    if encoded == "b:0" {
        return Some("false".to_owned());
    }
    if let Some(value) = encoded.strip_prefix("i:") {
        return Some(value.to_owned());
    }
    if let Some(value) = encoded
        .strip_prefix("s:")
        .or_else(|| encoded.strip_prefix("v:"))
    {
        return hex_decode(value).ok();
    }
    None
}

fn collect_inline_flush_rows(
    payload: &[u8],
    begin_snapshot: u64,
    deleted_rows: &InlineDeletionIndex,
    out: &mut Vec<InlineFlushRow>,
) -> CatalogResult<()> {
    let rows = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        out.push(InlineFlushRow {
            row_id,
            begin_snapshot,
            end_snapshot: deleted_rows.end_snapshot_for(row_id, begin_snapshot),
            values: fields[2..]
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        });
    }
    Ok(())
}

enum InlineFlushFileOrder {
    Default,
    Terms(Vec<InlineSortTerm>),
}

struct InlineSortTerm {
    key: InlineSortKey,
    descending: bool,
    nulls_first: bool,
}

enum InlineSortKey {
    RowId,
    BeginSnapshot,
    Column(usize),
    ColumnSquare(usize),
}

enum InlineSortValue {
    Null,
    Comparable(String),
}

impl InlineFlushFileOrder {
    fn parse(order: Option<&str>, schema_table: &TableRow) -> CatalogResult<Self> {
        let Some(order) = order.map(str::trim).filter(|order| !order.is_empty()) else {
            return Ok(Self::Default);
        };
        let mut terms = Vec::new();
        for term in split_inline_top_level(order) {
            terms.push(InlineSortTerm::parse(term, schema_table)?);
        }
        Ok(Self::Terms(terms))
    }

    fn compare(&self, left: &InlineFlushRow, right: &InlineFlushRow) -> Ordering {
        match self {
            Self::Default => compare_inline_flush_tie_breakers(left, right),
            Self::Terms(terms) => {
                for term in terms {
                    let ordering = term.compare(left, right);
                    if !ordering.is_eq() {
                        return ordering;
                    }
                }
                compare_inline_flush_tie_breakers(left, right)
            }
        }
    }
}

impl InlineSortTerm {
    fn parse(term: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        let (without_nulls, nulls_first) = strip_inline_order_nulls(term);
        let (expression, descending) = strip_inline_order_direction(without_nulls);
        let key = InlineSortKey::parse(expression, schema_table)?;
        Ok(Self {
            key,
            descending,
            nulls_first,
        })
    }

    fn compare(&self, left: &InlineFlushRow, right: &InlineFlushRow) -> Ordering {
        let ordering =
            compare_inline_sort_values(self.value(left), self.value(right), self.nulls_first);
        if self.descending {
            ordering.reverse()
        } else {
            ordering
        }
    }

    fn value(&self, row: &InlineFlushRow) -> InlineSortValue {
        match self.key {
            InlineSortKey::RowId => InlineSortValue::Comparable(row.row_id.to_string()),
            InlineSortKey::BeginSnapshot => {
                InlineSortValue::Comparable(row.begin_snapshot.to_string())
            }
            InlineSortKey::Column(index) => inline_sort_value(&row.values[index]),
            InlineSortKey::ColumnSquare(index) => inline_square_sort_value(&row.values[index]),
        }
    }
}

impl InlineSortKey {
    fn parse(expression: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        let normalized = normalize_inline_sort_expression(expression);
        if normalized.eq_ignore_ascii_case("row_id") {
            return Ok(Self::RowId);
        }
        if normalized.eq_ignore_ascii_case("begin_snapshot") {
            return Ok(Self::BeginSnapshot);
        }
        if let Some(column_name) = parse_inline_square_sort_expression(&normalized) {
            let Some((index, _)) = schema_table
                .columns
                .iter()
                .filter(|column| column.parent_id.is_none())
                .enumerate()
                .find(|(_, column)| column.name.eq_ignore_ascii_case(&column_name))
            else {
                return Err(crate::CatalogError::InvalidMutation(format!(
                    "unsupported inline flush file order term: {expression}"
                )));
            };
            return Ok(Self::ColumnSquare(index));
        }
        let Some((index, _)) = schema_table
            .columns
            .iter()
            .filter(|column| column.parent_id.is_none())
            .enumerate()
            .find(|(_, column)| column.name.eq_ignore_ascii_case(&normalized))
        else {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "unsupported inline flush file order term: {expression}"
            )));
        };
        Ok(Self::Column(index))
    }
}

fn parse_inline_square_sort_expression(expression: &str) -> Option<String> {
    let (left, right) = expression.split_once('*')?;
    let left = normalize_inline_sort_expression(left);
    let right = normalize_inline_sort_expression(right);
    if left.eq_ignore_ascii_case(&right) {
        Some(left)
    } else {
        None
    }
}

fn compare_inline_flush_tie_breakers(left: &InlineFlushRow, right: &InlineFlushRow) -> Ordering {
    left.row_id
        .cmp(&right.row_id)
        .then(left.begin_snapshot.cmp(&right.begin_snapshot))
}

fn compare_inline_sort_values(
    left: InlineSortValue,
    right: InlineSortValue,
    nulls_first: bool,
) -> Ordering {
    match (left, right) {
        (InlineSortValue::Null, InlineSortValue::Null) => Ordering::Equal,
        (InlineSortValue::Null, InlineSortValue::Comparable(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (InlineSortValue::Comparable(_), InlineSortValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (InlineSortValue::Comparable(left), InlineSortValue::Comparable(right)) => {
            stats_value_cmp(&left, &right)
        }
    }
}

fn inline_sort_value(encoded: &str) -> InlineSortValue {
    match decode_inline_stats_value(encoded) {
        Ok(InlineStatsValue::Null) => InlineSortValue::Null,
        Ok(InlineStatsValue::Comparable(value)) => InlineSortValue::Comparable(value),
        Ok(InlineStatsValue::Opaque) | Err(_) => InlineSortValue::Comparable(encoded.to_owned()),
    }
}

fn inline_square_sort_value(encoded: &str) -> InlineSortValue {
    match decode_inline_stats_value(encoded) {
        Ok(InlineStatsValue::Null) => InlineSortValue::Null,
        Ok(InlineStatsValue::Comparable(value)) => match value.parse::<i128>() {
            Ok(value) => InlineSortValue::Comparable(value.saturating_mul(value).to_string()),
            Err(_) => InlineSortValue::Comparable(value),
        },
        Ok(InlineStatsValue::Opaque) | Err(_) => InlineSortValue::Comparable(encoded.to_owned()),
    }
}

fn strip_inline_order_nulls(term: &str) -> (&str, bool) {
    let trimmed = term.trim();
    if let Some(value) = strip_ascii_suffix(trimmed, " NULLS FIRST") {
        return (value.trim(), true);
    }
    if let Some(value) = strip_ascii_suffix(trimmed, " NULLS LAST") {
        return (value.trim(), false);
    }
    (trimmed, false)
}

fn strip_inline_order_direction(term: &str) -> (&str, bool) {
    let trimmed = term.trim();
    if let Some(value) = strip_ascii_suffix(trimmed, " DESC") {
        return (value.trim(), true);
    }
    if let Some(value) = strip_ascii_suffix(trimmed, " ASC") {
        return (value.trim(), false);
    }
    (trimmed, false)
}

fn strip_ascii_suffix<'a>(value: &'a str, suffix: &str) -> Option<&'a str> {
    let start = value.len().checked_sub(suffix.len())?;
    if value[start..].eq_ignore_ascii_case(suffix) {
        Some(&value[..start])
    } else {
        None
    }
}

fn normalize_inline_sort_expression(expression: &str) -> String {
    let mut normalized = strip_balanced_parentheses(expression.trim())
        .trim()
        .to_owned();
    if let Some(prefix) = strip_ascii_suffix(&normalized, " + 0") {
        normalized = strip_balanced_parentheses(prefix.trim()).trim().to_owned();
    }
    unquote_inline_identifier(&normalized)
}

fn strip_balanced_parentheses(value: &str) -> &str {
    let mut current = value.trim();
    while current.starts_with('(')
        && current.ends_with(')')
        && parentheses_wrap_entire_value(current)
    {
        current = current[1..current.len() - 1].trim();
    }
    current
}

fn parentheses_wrap_entire_value(value: &str) -> bool {
    let mut depth = 0i32;
    let mut quote = false;
    for (index, ch) in value.char_indices() {
        match ch {
            '"' => quote = !quote,
            '(' if !quote => depth += 1,
            ')' if !quote => {
                depth -= 1;
                if depth == 0 && index != value.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

fn unquote_inline_identifier(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_owned()
    }
}

pub(crate) fn inline_row_changes_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: InlineRowChangesPayload,
    kind: InlineRowChangeKind,
) -> CatalogResult<Vec<u8>> {
    let start_order = change_feed_start_order(kv, catalog, payload.start_snapshot)?;
    let end_order = change_feed_end_order(kv, catalog, payload.end_snapshot)?;
    let start_snapshot_id = payload.start_snapshot.public_id().0;
    let end_snapshot_id = payload.end_snapshot.public_id().0;
    let inline_table = InlineTableName::parse(&payload.table_name);
    let (table, schema_id) = load_inlined_table(kv, catalog, end_order, inline_table)?;
    let schema_table = table_row_for_inline_schema(kv, catalog, table.table_id, schema_id.0)?;
    let changes = list_inline_row_payload_changes(
        kv,
        catalog,
        table.table_id,
        schema_id,
        start_order,
        end_order,
        kind,
    )?;
    let mut rendered_changes = Vec::new();
    for change in changes {
        let sequence = change_feed_snapshot_id_for_order(kv, catalog, change.change.order)?
            .ok_or_else(|| missing_snapshot_order(change.change.order))?;
        if sequence < start_snapshot_id || sequence > end_snapshot_id {
            continue;
        }
        let (begin_snapshot, end_snapshot) = match kind {
            InlineRowChangeKind::Inserted => (sequence.to_string(), String::new()),
            InlineRowChangeKind::Deleted => (String::new(), sequence.to_string()),
        };
        let line = String::from_utf8(change.payload).map_err(|error| {
            crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
        })?;
        let fields = inline_row_fields(&line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        if kind == InlineRowChangeKind::Inserted
            && inline_row_materialized_by_visible_file(
                kv,
                catalog,
                table.table_id,
                row_id,
                change.change.order,
                end_order,
            )?
        {
            continue;
        }
        rendered_changes.push(format!(
            "row_change\t{}\t{}\t{}\t{}\n",
            begin_snapshot,
            end_snapshot,
            fields[1],
            fields[2..].join("\t")
        ));
    }
    let mut out = inline_columns_payload(&schema_table)?;
    out.push_str(&format!(
        "inline_row_change_count={}\n",
        rendered_changes.len()
    ));
    for rendered in rendered_changes {
        out.push_str(&rendered);
    }
    Ok(out.into_bytes())
}

fn inline_row_materialized_by_visible_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    row_id: u64,
    inline_begin_order: CatalogOrderId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    InlineMaterializedFiles::load(kv, catalog, table_id, snapshot_order)
        .map(|files| files.materializes(row_id, inline_begin_order))
}

#[derive(Clone)]
struct InlineMaterializedFiles {
    visible_files: Vec<AttachedDataFile>,
}

impl InlineMaterializedFiles {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let visible_files = materialized_visible_files(kv, catalog, table_id, snapshot_order)?;
        Ok(Self { visible_files })
    }

    fn materializes(&self, row_id: u64, inline_begin_order: CatalogOrderId) -> bool {
        for attached in &self.visible_files {
            if attached
                .delete_file
                .as_ref()
                .is_some_and(|delete| delete.record_count >= attached.data_file.record_count)
            {
                continue;
            }
            let file = &attached.data_file;
            let Some(max_partial_order) = file.max_partial_order else {
                continue;
            };
            let row_id_end = file.row_id_start.saturating_add(file.record_count);
            if file.row_id_start <= row_id
                && row_id < row_id_end
                && file.validity.begin_order <= inline_begin_order
                && inline_begin_order <= max_partial_order
            {
                return true;
            }
        }
        false
    }
}

fn materialized_visible_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    if latest_snapshot(kv, catalog)?.is_some_and(|latest| latest.order == snapshot_order) {
        return list_current_data_files_with_deletes(kv, catalog, table_id);
    }
    list_data_files_with_deletes_at(kv, catalog, table_id, snapshot_order)
}

#[derive(Clone)]
struct InlineReadRenderContext {
    snapshot_sequences: BTreeMap<CatalogOrderId, u64>,
    materialized_files: InlineMaterializedFiles,
    deleted_rows: InlineDeletionIndex,
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineReadRenderContextCacheKey {
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_READ_RENDER_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineReadRenderContextCacheKey, InlineReadRenderContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_read_render_context_cache()
-> &'static BoundedCache<InlineReadRenderContextCacheKey, InlineReadRenderContext> {
    static_bounded_cache(&INLINE_READ_RENDER_CONTEXT_CACHE, 512)
}

fn load_inline_read_render_context(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<InlineReadRenderContext> {
    #[cfg(test)]
    {
        InlineReadRenderContext::load(kv, catalog, table_id, schema_id, snapshot_order)
    }
    #[cfg(not(test))]
    {
        let key = InlineReadRenderContextCacheKey {
            catalog,
            table_id,
            schema_id,
            snapshot_order,
        };
        let cache = inline_read_render_context_cache();
        if let Some(context) = cache.get(key) {
            return Ok(context.clone());
        }
        let context =
            InlineReadRenderContext::load(kv, catalog, table_id, schema_id, snapshot_order)?;
        cache.insert(key, context.clone());
        Ok(context)
    }
}

impl InlineReadRenderContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let snapshot_sequences = public_snapshot_sequences_by_order(kv, catalog)?;
        let materialized_files =
            InlineMaterializedFiles::load(kv, catalog, table_id, snapshot_order)?;
        let deleted_rows = InlineDeletionIndex::load(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            &snapshot_sequences,
        )?;
        Ok(Self {
            snapshot_sequences,
            materialized_files,
            deleted_rows,
        })
    }

    fn snapshot_sequence(&self, order: CatalogOrderId) -> Option<u64> {
        self.snapshot_sequences.get(&order).copied()
    }
}

fn retain_unmaterialized_inline_payloads(
    materialized_files: &InlineMaterializedFiles,
    payloads: &mut Vec<InlineTablePayloadRow>,
) {
    if payloads.is_empty() {
        return;
    }
    payloads.retain(|payload| {
        !materialized_files
            .visible_files
            .iter()
            .any(|attached| data_file_covers_inline_begin(&attached.data_file, payload.begin_order))
    });
}

fn change_feed_start_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedStartSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((start_order, _)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(start_order);
    }
    public_snapshot_order_span(kv, catalog, snapshot_id)?
        .map(|(start, _)| start)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

fn change_feed_end_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedEndSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((_, end_order)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(end_order);
    }
    snapshot_by_public_sequence(kv, catalog, snapshot_id)?
        .map(|snapshot| snapshot.order)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

fn ducklake_snapshot_order_span(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<(CatalogOrderId, CatalogOrderId)>> {
    cached_ducklake_snapshot_order_span(kv, catalog, snapshot_id)
}

fn change_feed_snapshot_id_for_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<Option<u64>> {
    if let Some(snapshot) = list_snapshots(kv, catalog)?
        .into_iter()
        .find(|snapshot| snapshot.order == order)
    {
        return Ok(Some(snapshot.sequence.0));
    }
    Ok(public_snapshot_sequence_for_order(kv, catalog, order)?.map(|snapshot| snapshot.0))
}

fn inline_row_fields(line: &str) -> CatalogResult<Vec<&str>> {
    let trimmed = line.trim_end_matches('\n');
    let fields = trimmed.split('\t').collect::<Vec<_>>();
    if fields.len() < 2 || fields[0] != "row" {
        return Err(crate::CatalogError::Decode(format!(
            "inline row payload has invalid shape: {line}"
        )));
    }
    Ok(fields)
}

fn append_snapshot_aware_rows(
    _kv: &impl OrderedCatalogKv,
    _catalog: CatalogId,
    begin_order: CatalogOrderId,
    payload: &[u8],
    include_deleted: bool,
    stats_mode: InlineStatsMode,
    render_context: &InlineReadRenderContext,
    catalog_stats: &mut InlineCatalogStats,
    out: &mut String,
) -> CatalogResult<()> {
    let begin_snapshot = render_context
        .snapshot_sequence(begin_order)
        .ok_or_else(|| missing_snapshot_order(begin_order))?;
    let rows = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        catalog_stats.observe_row_id(row_id);
        if matches!(stats_mode, InlineStatsMode::Conservative) {
            catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
        }
        if !include_deleted
            && render_context
                .deleted_rows
                .hides_row_version(row_id, begin_snapshot)
        {
            continue;
        }
        if render_context
            .materialized_files
            .materializes(row_id, begin_order)
        {
            continue;
        }
        if matches!(stats_mode, InlineStatsMode::ExactVisible) {
            catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
        }
        out.push_str(&format!(
            "row_change\t{}\t\t{}\t{}\n",
            begin_snapshot,
            fields[1],
            fields[2..].join("\t")
        ));
    }
    Ok(())
}

struct InlineCatalogStats {
    columns: Vec<InlineStatsColumn>,
    missing_current_column_ids: Vec<u64>,
    record_count: u64,
    next_row_id: u64,
    stats: BTreeMap<u64, LiveColumnStats>,
}

struct InlineStatsColumn {
    column_id: u64,
    column_type: String,
    children: Vec<InlineStatsChildColumn>,
}

struct InlineStatsChildColumn {
    column_id: u64,
    name: String,
}

impl InlineCatalogStats {
    fn for_inline_schema(schema_table: &TableRow, current_table: &TableRow) -> Self {
        let columns = schema_table
            .columns
            .iter()
            .filter(|column| column.parent_id.is_none())
            .map(|column| InlineStatsColumn {
                column_id: column.column_id.0,
                column_type: column.column_type.clone(),
                children: schema_table
                    .columns
                    .iter()
                    .filter(|child| child.parent_id == Some(column.column_id))
                    .map(|child| InlineStatsChildColumn {
                        column_id: child.column_id.0,
                        name: child.name.clone(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let schema_column_ids = schema_table
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<BTreeSet<_>>();
        let missing_current_column_ids = current_table
            .columns
            .iter()
            .filter(|column| !schema_column_ids.contains(&column.column_id))
            .map(|column| column.column_id.0)
            .collect::<Vec<_>>();
        Self {
            columns,
            missing_current_column_ids,
            record_count: 0,
            next_row_id: 0,
            stats: BTreeMap::new(),
        }
    }

    fn observe_row_id(&mut self, row_id: u64) {
        self.next_row_id = self.next_row_id.max(row_id + 1);
    }

    fn accumulate_visible_row(
        &mut self,
        row_id: u64,
        encoded_values: &[&str],
    ) -> CatalogResult<()> {
        if encoded_values.len() != self.columns.len() {
            return Err(crate::CatalogError::Decode(format!(
                "inline row has {} values for {} columns",
                encoded_values.len(),
                self.columns.len()
            )));
        }
        self.record_count += 1;
        self.observe_row_id(row_id);
        for (column, encoded_value) in self.columns.iter().zip(encoded_values.iter()) {
            self.stats
                .entry(column.column_id)
                .or_default()
                .accumulate_encoded(encoded_value, &column.column_type)?;
            accumulate_child_inline_stats(&mut self.stats, column, encoded_value)?;
        }
        Ok(())
    }

    fn clear_min_max(&mut self) {
        for stats in self.stats.values_mut() {
            stats.clear_min_max();
        }
    }

    fn append_to(&self, out: &mut String) -> CatalogResult<()> {
        writeln!(
            out,
            "inline_table_stats\t{}\t{}",
            self.record_count, self.next_row_id
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render inline table stats: {error}"))
        })?;
        for (column_id, stats) in &self.stats {
            writeln!(
                out,
                "inline_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                column_id,
                stats.has_contains_null,
                stats.contains_null,
                stats.min_value.is_some(),
                stats.min_value.as_deref().unwrap_or_default(),
                stats.max_value.is_some(),
                stats.max_value.as_deref().unwrap_or_default()
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!("failed to render inline stats: {error}"))
            })?;
            if let Some(extra_stats) = &stats.extra_stats {
                writeln!(
                    out,
                    "inline_column_extra_stats\t{}\t{}",
                    column_id, extra_stats
                )
                .map_err(|error| {
                    crate::CatalogError::Decode(format!(
                        "failed to render inline extra stats: {error}"
                    ))
                })?;
            }
        }
        if self.record_count > 0 {
            for column_id in &self.missing_current_column_ids {
                writeln!(
                    out,
                    "inline_column_stats\t{column_id}\ttrue\ttrue\tfalse\t\tfalse\t"
                )
                .map_err(|error| {
                    crate::CatalogError::Decode(format!(
                        "failed to render missing inline stats: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    fn append_aggregate_to(&self, out: &mut String) -> CatalogResult<()> {
        writeln!(out, "inline_aggregate_stats\t{}", self.record_count).map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render inline aggregate stats: {error}"))
        })?;
        for column in &self.columns {
            let stats = self.stats.get(&column.column_id);
            writeln!(
                out,
                "inline_aggregate_column_stats\t{}\t{}\t{}\t{}\t{}\t{}",
                column.column_id,
                stats.map(|stats| stats.non_null_count).unwrap_or(0),
                stats.and_then(|stats| stats.min_value.as_deref()).is_some(),
                stats
                    .and_then(|stats| stats.min_value.as_deref())
                    .unwrap_or_default(),
                stats.and_then(|stats| stats.max_value.as_deref()).is_some(),
                stats
                    .and_then(|stats| stats.max_value.as_deref())
                    .unwrap_or_default()
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render inline aggregate column stats: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct LiveColumnStats {
    non_null_count: u64,
    has_contains_null: bool,
    contains_null: bool,
    min_value: Option<String>,
    max_value: Option<String>,
    extra_stats: Option<InlineGeometryStats>,
}

impl LiveColumnStats {
    fn accumulate_encoded(&mut self, encoded: &str, column_type: &str) -> CatalogResult<()> {
        self.has_contains_null = true;
        let value = match decode_inline_stats_value(encoded)? {
            InlineStatsValue::Null => {
                self.accumulate_null();
                return Ok(());
            }
            InlineStatsValue::Opaque => {
                self.accumulate_opaque();
                return Ok(());
            }
            InlineStatsValue::Comparable(value) => value,
        };
        if column_type.eq_ignore_ascii_case("geometry") {
            self.accumulate_geometry(value);
            return Ok(());
        }
        self.accumulate_comparable(value);
        Ok(())
    }

    fn accumulate_null(&mut self) {
        self.has_contains_null = true;
        self.contains_null = true;
    }

    fn accumulate_opaque(&mut self) {
        self.has_contains_null = true;
        self.non_null_count += 1;
    }

    fn accumulate_comparable(&mut self, value: String) {
        self.has_contains_null = true;
        self.non_null_count += 1;
        if self
            .min_value
            .as_deref()
            .is_none_or(|current| stats_value_cmp(&value, current).is_lt())
        {
            self.min_value = Some(value.clone());
        }
        if self
            .max_value
            .as_deref()
            .is_none_or(|current| stats_value_cmp(&value, current).is_gt())
        {
            self.max_value = Some(value);
        }
    }

    fn accumulate_geometry(&mut self, value: String) {
        self.has_contains_null = true;
        self.non_null_count += 1;
        if let Some(stats) = InlineGeometryStats::parse_wkt(&value) {
            match &mut self.extra_stats {
                Some(current) => current.merge(stats),
                None => self.extra_stats = Some(stats),
            }
        }
    }

    fn clear_min_max(&mut self) {
        self.min_value = None;
        self.max_value = None;
    }
}

#[derive(Clone, Debug)]
struct InlineGeometryStats {
    xmin: Option<f64>,
    xmax: Option<f64>,
    ymin: Option<f64>,
    ymax: Option<f64>,
    zmin: Option<f64>,
    zmax: Option<f64>,
    mmin: Option<f64>,
    mmax: Option<f64>,
    types: BTreeSet<String>,
}

impl InlineGeometryStats {
    fn parse_wkt(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        let first_paren = trimmed.find('(')?;
        let header = trimmed[..first_paren].trim();
        let mut parts = header.split_whitespace();
        let geometry_type = parts.next()?.to_ascii_lowercase();
        let dimension = parts.next().unwrap_or("").to_ascii_lowercase();
        let coordinate_width = match dimension.as_str() {
            "z" | "m" => 3,
            "zm" => 4,
            _ => 2,
        };
        let mut stats = Self {
            xmin: None,
            xmax: None,
            ymin: None,
            ymax: None,
            zmin: None,
            zmax: None,
            mmin: None,
            mmax: None,
            types: [geometry_type_with_dimension(&geometry_type, &dimension)]
                .into_iter()
                .collect(),
        };
        let numbers = wkt_numbers(&trimmed[first_paren..]);
        if numbers.len() < coordinate_width {
            return None;
        }
        for coordinate in numbers.chunks(coordinate_width) {
            if coordinate.len() < coordinate_width {
                break;
            }
            stats.observe_xy(coordinate[0], coordinate[1]);
            match dimension.as_str() {
                "z" => stats.observe_z(coordinate[2]),
                "m" => stats.observe_m(coordinate[2]),
                "zm" => {
                    stats.observe_z(coordinate[2]);
                    stats.observe_m(coordinate[3]);
                }
                _ => {}
            }
        }
        Some(stats)
    }

    fn merge(&mut self, incoming: Self) {
        self.xmin = min_optional(self.xmin, incoming.xmin);
        self.xmax = max_optional(self.xmax, incoming.xmax);
        self.ymin = min_optional(self.ymin, incoming.ymin);
        self.ymax = max_optional(self.ymax, incoming.ymax);
        self.zmin = min_optional(self.zmin, incoming.zmin);
        self.zmax = max_optional(self.zmax, incoming.zmax);
        self.mmin = min_optional(self.mmin, incoming.mmin);
        self.mmax = max_optional(self.mmax, incoming.mmax);
        self.types.extend(incoming.types);
    }

    fn observe_xy(&mut self, x: f64, y: f64) {
        self.xmin = min_optional(self.xmin, Some(x));
        self.xmax = max_optional(self.xmax, Some(x));
        self.ymin = min_optional(self.ymin, Some(y));
        self.ymax = max_optional(self.ymax, Some(y));
    }

    fn observe_z(&mut self, z: f64) {
        self.zmin = min_optional(self.zmin, Some(z));
        self.zmax = max_optional(self.zmax, Some(z));
    }

    fn observe_m(&mut self, m: f64) {
        self.mmin = min_optional(self.mmin, Some(m));
        self.mmax = max_optional(self.mmax, Some(m));
    }

    fn to_json(&self) -> String {
        let mut out = String::from("{\"bbox\": {");
        write!(
            out,
            "\"xmin\": {}, \"xmax\": {}, \"ymin\": {}, \"ymax\": {}, \"zmin\": {}, \"zmax\": {}, \"mmin\": {}, \"mmax\": {}",
            json_number_or_null(self.xmin),
            json_number_or_null(self.xmax),
            json_number_or_null(self.ymin),
            json_number_or_null(self.ymax),
            json_number_or_null(self.zmin),
            json_number_or_null(self.zmax),
            json_number_or_null(self.mmin),
            json_number_or_null(self.mmax)
        )
        .expect("writing geometry stats JSON to string cannot fail");
        out.push_str("}, \"types\": [");
        for (index, geometry_type) in self.types.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push('"');
            out.push_str(geometry_type);
            out.push('"');
        }
        out.push_str("]}");
        out
    }
}

impl std::fmt::Display for InlineGeometryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_json())
    }
}

fn geometry_type_with_dimension(geometry_type: &str, dimension: &str) -> String {
    match dimension {
        "z" | "m" | "zm" => format!("{geometry_type}_{dimension}"),
        _ => geometry_type.to_owned(),
    }
}

fn wkt_numbers(value: &str) -> Vec<f64> {
    let mut numbers = Vec::new();
    let mut start = None;
    let mut previous = '\0';
    for (index, ch) in value.char_indices() {
        let exponent_sign = matches!(previous, 'e' | 'E') && matches!(ch, '-' | '+');
        if ch.is_ascii_digit() || matches!(ch, '-' | '+' | '.') || exponent_sign {
            start.get_or_insert(index);
            previous = ch;
            continue;
        }
        if matches!(ch, 'e' | 'E') && start.is_some() {
            previous = ch;
            continue;
        }
        if let Some(number_start) = start.take()
            && let Ok(number) = value[number_start..index].parse::<f64>()
        {
            numbers.push(number);
        }
        previous = ch;
    }
    if let Some(number_start) = start
        && let Ok(number) = value[number_start..].parse::<f64>()
    {
        numbers.push(number);
    }
    numbers
}

fn min_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn json_number_or_null(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

fn accumulate_child_inline_stats(
    stats: &mut BTreeMap<u64, LiveColumnStats>,
    column: &InlineStatsColumn,
    encoded: &str,
) -> CatalogResult<()> {
    if column.children.is_empty() {
        return Ok(());
    }
    let value = match decode_inline_stats_value(encoded)? {
        InlineStatsValue::Null => {
            for child in &column.children {
                stats.entry(child.column_id).or_default().accumulate_null();
            }
            return Ok(());
        }
        InlineStatsValue::Opaque => return Ok(()),
        InlineStatsValue::Comparable(value) => value,
    };
    let type_name = column.column_type.to_ascii_lowercase();
    if type_name == "list" || type_name.ends_with("[]") {
        if let Some(child) = column.children.first() {
            for item in parse_inline_list_values(&value) {
                accumulate_inline_text_value(stats, child.column_id, item);
            }
        }
        return Ok(());
    }
    if type_name == "struct" || type_name.starts_with("struct(") {
        let fields = parse_inline_struct_values(&value);
        for child in &column.children {
            match fields.iter().find(|(name, _)| name == &child.name) {
                Some((_, value)) => accumulate_inline_text_value(stats, child.column_id, value),
                None => stats.entry(child.column_id).or_default().accumulate_null(),
            }
        }
    }
    Ok(())
}

fn accumulate_inline_text_value(
    stats: &mut BTreeMap<u64, LiveColumnStats>,
    column_id: u64,
    value: &str,
) {
    let value = value.trim();
    if value.eq_ignore_ascii_case("NULL") {
        stats.entry(column_id).or_default().accumulate_null();
        return;
    }
    stats
        .entry(column_id)
        .or_default()
        .accumulate_comparable(unquote_inline_scalar(value));
}

fn parse_inline_list_values(value: &str) -> Vec<&str> {
    let trimmed = value.trim();
    let Some(inner) = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Vec::new();
    };
    split_inline_top_level(inner)
}

fn parse_inline_struct_values(value: &str) -> Vec<(String, &str)> {
    let trimmed = value.trim();
    let Some(inner) = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    else {
        return Vec::new();
    };
    split_inline_top_level(inner)
        .into_iter()
        .filter_map(|field| {
            let (name, value) = field.split_once(':')?;
            Some((unquote_inline_scalar(name.trim()), value.trim()))
        })
        .collect()
}

fn split_inline_top_level(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut depth = 0u32;
    for (index, ch) in value.char_indices() {
        match ch {
            '\'' => quote = !quote,
            '[' | '{' if !quote => depth += 1,
            ']' | '}' if !quote && depth > 0 => depth -= 1,
            ',' if !quote && depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = value[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

fn unquote_inline_scalar(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        trimmed[1..trimmed.len() - 1].replace("''", "'")
    } else {
        trimmed.to_owned()
    }
}

enum InlineStatsValue {
    Null,
    Opaque,
    Comparable(String),
}

fn decode_inline_stats_value(encoded: &str) -> CatalogResult<InlineStatsValue> {
    if encoded == "n:" {
        return Ok(InlineStatsValue::Null);
    }
    if let Some(value) = encoded.strip_prefix("i:") {
        return Ok(InlineStatsValue::Comparable(value.to_owned()));
    }
    if let Some(value) = encoded.strip_prefix("s:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if let Some(value) = encoded.strip_prefix("v:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if let Some(value) = encoded.strip_prefix("x:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if encoded.starts_with("d:") {
        return Ok(InlineStatsValue::Opaque);
    }
    match encoded {
        "b:0" => Ok(InlineStatsValue::Comparable("false".to_owned())),
        "b:1" => Ok(InlineStatsValue::Comparable("true".to_owned())),
        _ => Err(crate::CatalogError::Decode(format!(
            "inline stats payload has invalid value: {encoded}"
        ))),
    }
}

fn hex_decode(value: &str) -> CatalogResult<String> {
    if !value.len().is_multiple_of(2) {
        return Err(crate::CatalogError::Decode(
            "inline stats payload has odd-length hex".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut chars = value.as_bytes().chunks_exact(2);
    for chunk in &mut chars {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    String::from_utf8(bytes).map_err(|error| {
        crate::CatalogError::Decode(format!("inline stats payload is not utf8: {error}"))
    })
}

fn hex_nibble(byte: u8) -> CatalogResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crate::CatalogError::Decode(
            "inline stats payload has invalid hex".to_owned(),
        )),
    }
}

fn stats_value_cmp(left: &str, right: &str) -> Ordering {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InlineTableName<'a> {
    Generated {
        raw: &'a str,
        table_id: TableId,
        schema_id: SchemaId,
    },
    Legacy {
        raw: &'a str,
    },
}

impl<'a> InlineTableName<'a> {
    fn parse(raw: &'a str) -> Self {
        let Some(tail) = raw.strip_prefix("ducklake_inlined_data_") else {
            return Self::Legacy { raw };
        };
        let Some((table_id, schema_id)) = tail.split_once('_') else {
            return Self::Legacy { raw };
        };
        if table_id.is_empty() || schema_id.is_empty() || schema_id.contains('_') {
            return Self::Legacy { raw };
        }
        let Ok(table_id) = parse_u64_field("ReadInlineRows", table_id, "inline table id") else {
            return Self::Legacy { raw };
        };
        let Ok(schema_id) = parse_u64_field("ReadInlineRows", schema_id, "inline schema id") else {
            return Self::Legacy { raw };
        };
        Self::Generated {
            raw,
            table_id: TableId(table_id),
            schema_id: SchemaId(schema_id),
        }
    }
}

fn load_inlined_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    inline_table: InlineTableName<'_>,
) -> CatalogResult<(TableRow, SchemaId)> {
    let InlineTableName::Generated {
        raw,
        table_id,
        schema_id,
    } = inline_table
    else {
        let tables = list_tables_at(kv, catalog, snapshot_order)?;
        let (table, schema_id) = find_legacy_inlined_table(&tables, inline_table)?;
        return Ok((table.clone(), schema_id));
    };
    let table = load_table_at(kv, catalog, table_id, snapshot_order)?
        .ok_or(crate::CatalogError::NotFound("inlined table"))?;
    let has_inline_registration = table
        .inlined_data_tables
        .iter()
        .any(|inlined| inlined.table_name == raw && inlined.schema_version == schema_id.0);
    if has_inline_registration {
        return Ok((table, schema_id));
    }
    Err(crate::CatalogError::NotFound("inlined table registration"))
}

fn find_legacy_inlined_table<'a>(
    tables: &'a [TableRow],
    inline_table: InlineTableName<'_>,
) -> CatalogResult<(&'a TableRow, SchemaId)> {
    let InlineTableName::Legacy { raw } = inline_table else {
        return Err(crate::CatalogError::NotFound("inlined table"));
    };
    for table in tables {
        for inlined in &table.inlined_data_tables {
            if inlined.table_name == raw {
                return Ok((table, SchemaId(inlined.schema_version)));
            }
        }
    }
    Err(crate::CatalogError::NotFound("inlined table"))
}

fn missing_snapshot(snapshot_id: DuckLakeSnapshotId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot {snapshot_id} does not exist"))
}

fn missing_snapshot_order(order: CatalogOrderId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot order {order} does not exist"))
}

#[cfg(test)]
#[path = "runtime_inline_rows_tests.rs"]
mod runtime_inline_rows_tests;
