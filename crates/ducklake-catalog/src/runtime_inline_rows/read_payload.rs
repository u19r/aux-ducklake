use std::fmt::Write;

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_protocol::RuntimeCatalogBackend;
use crate::{
    CatalogId, CatalogResult, DuckLakeSnapshotId, OrderedCatalogKv,
    inline_column_types::inline_columns_payload, inline_data::InlineTablePayloadRow,
    latest_snapshot, runtime_snapshot_range::ReadSnapshot, snapshot_by_ducklake_sequence,
    snapshot_by_public_sequence,
};

use crate::runtime_inline_rows::*;

pub(super) fn read_inline_rows_payload_with_stats_request(
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

pub(super) fn read_inline_rows_payload_with_stats_request_and_mode(
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
            SnapshotAwareRowsInput {
                begin_order: inline_payload.begin_order,
                payload: &inline_payload.payload,
                include_deleted: payload.include_deleted,
                stats_mode,
                render_context: &render_context,
            },
            &mut catalog_stats,
            &mut out,
        )?;
    }
    catalog_stats.append_to(&mut out)?;
    record_inline_stage(metric_prefix, "Render", started);
    Ok(out.into_bytes())
}

pub(super) fn empty_global_inline_stats_payload() -> Vec<u8> {
    b"inline_payload_count=0\ninline_table_stats\t0\t0\n".to_vec()
}

pub(super) fn render_global_inline_stats_payload(
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
            GlobalInlineStatsInput {
                payload: &inline_payload.payload,
                begin_order: inline_payload.begin_order,
                include_deleted: payload.include_deleted,
                include_materialized: payload.include_flushed,
                stats_mode,
                materialized_files: &materialized_files,
                deleted_rows: deleted_rows.as_ref(),
            },
            &mut catalog_stats,
        )?;
    }
    if matches!(stats_mode, InlineStatsMode::ExactVisible)
        && deleted_rows
            .as_ref()
            .is_some_and(|deleted_rows| deleted_rows.has_deletions())
    {
        catalog_stats.clear_min_max();
    }
    catalog_stats.append_to(&mut out)?;
    record_inline_stage(metric_prefix, "Render", started);
    Ok(out.into_bytes())
}

pub(super) fn render_aggregate_inline_stats_payload(
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
            GlobalInlineStatsInput {
                payload: &inline_payload.payload,
                begin_order: inline_payload.begin_order,
                include_deleted: payload.include_deleted,
                include_materialized: payload.include_flushed,
                stats_mode,
                materialized_files: &materialized_files,
                deleted_rows: deleted_rows.as_ref(),
            },
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

pub(super) fn inline_read_metric_prefix(stats_request: InlineStatsRequest) -> &'static str {
    match stats_request {
        InlineStatsRequest::Conservative | InlineStatsRequest::Exact => "ReadInlineRows",
        InlineStatsRequest::Global => "ReadInlineRowsForGlobalStats",
    }
}

#[cfg(feature = "runtime-metrics")]
pub(super) fn record_inline_stage(prefix: &str, stage: &str, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!("{prefix}Stage{stage}"),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
pub(super) fn record_inline_stage(_prefix: &str, _stage: &str, _started: RuntimeMetricStage) {}

pub(super) fn inline_read_snapshot(
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

pub(super) fn inline_stats_mode_for_request(stats_request: InlineStatsRequest) -> InlineStatsMode {
    match stats_request {
        InlineStatsRequest::Conservative => InlineStatsMode::Conservative,
        InlineStatsRequest::Exact => InlineStatsMode::ExactVisible,
        InlineStatsRequest::Global => InlineStatsMode::ExactVisible,
    }
}
