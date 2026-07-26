use std::fmt::Write;

use crate::{
    CatalogId, CatalogResult, FdbOrderedCatalogKv,
    inline_column_types::inline_columns_payload,
    inline_data::{list_inline_current_rows, load_inline_next_row_id},
    latest_snapshot,
};

use crate::runtime_inline_rows::*;

enum CurrentInlineStats {
    Unavailable,
    MissingTable,
    Available {
        catalog_stats: InlineCatalogStats,
        row_count: usize,
        schema_table: Box<crate::TableRow>,
    },
}

pub(crate) fn read_foundationdb_inline_rows_global_stats_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    read_foundationdb_inline_rows_global_stats_payload_with_fallback(
        kv,
        catalog,
        payload,
        InlineStatsMode::Conservative,
    )
}

pub(crate) fn read_foundationdb_inline_rows_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    if let Some(output) = read_current_inline_rows_payload(kv, catalog, &payload)? {
        return Ok(output);
    }
    read_inline_rows_payload(kv, catalog, payload)
}

fn read_current_inline_rows_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: &ReadInlineRowsPayload,
) -> CatalogResult<Option<Vec<u8>>> {
    if payload.include_flushed || payload.include_deleted {
        return Ok(None);
    }
    let snapshot = inline_read_snapshot(
        kv,
        catalog,
        payload.snapshot,
        InlineStatsRequest::Conservative,
    )?
    .ok_or_else(|| crate::CatalogError::Decode("catalog snapshot does not exist".to_owned()))?;
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(None);
    };
    if snapshot.order != latest.order {
        return Ok(None);
    }
    let context = match InlineTableSnapshotContext::load_for_table_name(
        kv,
        catalog,
        &payload.table_name,
        snapshot,
    ) {
        Ok(context) => context,
        Err(crate::CatalogError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let current_rows =
        list_inline_current_rows(kv, catalog, context.table.table_id, context.schema_id)?;
    let next_row_id =
        load_inline_next_row_id(kv, catalog, context.table.table_id, context.schema_id)?;
    let mut out = inline_columns_payload(&context.schema_table)?;
    writeln!(out, "inline_payload_count={}", current_rows.len()).map_err(|error| {
        crate::CatalogError::Decode(format!(
            "failed to render current inline payload count: {error}"
        ))
    })?;
    let mut catalog_stats =
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table);
    catalog_stats.observe_next_row_id(next_row_id);
    for (row_id, row) in current_rows {
        catalog_stats.accumulate_current_row_payload(row_id, &row.payload)?;
        let fields = inline_row_fields(std::str::from_utf8(&row.payload).map_err(|error| {
            crate::CatalogError::Decode(format!("inline current-row payload is not utf8: {error}"))
        })?)?;
        writeln!(
            out,
            "row_change\t{}\t\t{}\t{}",
            row.begin_sequence.0,
            row_id,
            fields[2..].join("\t")
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render current inline row: {error}"))
        })?;
    }
    catalog_stats.append_to(&mut out)?;
    Ok(Some(out.into_bytes()))
}

pub(crate) fn read_foundationdb_inline_rows_global_stats_exact_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    read_foundationdb_inline_rows_global_stats_payload_with_fallback(
        kv,
        catalog,
        payload,
        InlineStatsMode::ExactVisible,
    )
}

fn read_foundationdb_inline_rows_global_stats_payload_with_fallback(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
    fallback_mode: InlineStatsMode,
) -> CatalogResult<Vec<u8>> {
    match load_current_inline_stats(kv, catalog, &payload)? {
        CurrentInlineStats::Unavailable => match fallback_mode {
            InlineStatsMode::Conservative => {
                read_inline_rows_global_stats_payload(kv, catalog, payload)
            }
            InlineStatsMode::ExactVisible => read_inline_rows_payload_with_stats_request_and_mode(
                kv,
                catalog,
                payload,
                InlineStatsRequest::Global,
                fallback_mode,
            ),
        },
        CurrentInlineStats::MissingTable => Ok(empty_global_inline_stats_payload()),
        CurrentInlineStats::Available {
            catalog_stats,
            row_count,
            schema_table,
        } => {
            let mut out = inline_columns_payload(&schema_table)?;
            writeln!(out, "inline_payload_count={row_count}").map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render current inline payload count: {error}"
                ))
            })?;
            catalog_stats.append_to(&mut out)?;
            Ok(out.into_bytes())
        }
    }
}

pub(crate) fn read_foundationdb_inline_rows_aggregate_stats_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    match load_current_inline_stats(kv, catalog, &payload)? {
        CurrentInlineStats::Unavailable => {
            read_inline_rows_aggregate_stats_payload(kv, catalog, payload)
        }
        CurrentInlineStats::MissingTable => Ok(b"inline_aggregate_stats\t0\n".to_vec()),
        CurrentInlineStats::Available { catalog_stats, .. } => {
            let mut out = "inline_active_delete_count\t0\n".to_owned();
            catalog_stats.append_aggregate_to(&mut out)?;
            Ok(out.into_bytes())
        }
    }
}

fn load_current_inline_stats(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: &ReadInlineRowsPayload,
) -> CatalogResult<CurrentInlineStats> {
    if payload.include_flushed || payload.include_deleted {
        return Ok(CurrentInlineStats::Unavailable);
    }
    let snapshot = inline_read_snapshot(kv, catalog, payload.snapshot, InlineStatsRequest::Global)?
        .ok_or_else(|| crate::CatalogError::Decode("catalog snapshot does not exist".to_owned()))?;
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(CurrentInlineStats::Unavailable);
    };
    if snapshot.order != latest.order {
        return Ok(CurrentInlineStats::Unavailable);
    }
    let context = match InlineTableSnapshotContext::load_for_table_name(
        kv,
        catalog,
        &payload.table_name,
        snapshot,
    ) {
        Ok(context) => context,
        Err(crate::CatalogError::NotFound(_)) => return Ok(CurrentInlineStats::MissingTable),
        Err(error) => return Err(error),
    };
    let started = RuntimeMetricStage::start();
    let current_rows =
        list_inline_current_rows(kv, catalog, context.table.table_id, context.schema_id)?;
    let next_row_id =
        load_inline_next_row_id(kv, catalog, context.table.table_id, context.schema_id)?;
    record_inline_stage("ReadInlineRowsForCurrentStats", "Index", started);
    let mut catalog_stats =
        InlineCatalogStats::for_inline_schema(&context.schema_table, &context.table);
    catalog_stats.observe_next_row_id(next_row_id);
    for (row_id, row) in &current_rows {
        catalog_stats.accumulate_current_row_payload(*row_id, &row.payload)?;
    }
    Ok(CurrentInlineStats::Available {
        catalog_stats,
        row_count: current_rows.len(),
        schema_table: Box::new(context.schema_table),
    })
}
