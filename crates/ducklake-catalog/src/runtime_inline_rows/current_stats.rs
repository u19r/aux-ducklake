use std::fmt::Write;

use crate::{
    CatalogId, CatalogResult, FdbOrderedCatalogKv,
    inline_column_types::inline_columns_payload,
    inline_data::{list_inline_current_rows, load_inline_next_row_id},
    latest_snapshot,
    runtime_inline_rows::*,
    runtime_inline_snapshot::capture::{
        captured_inline_current_rows, captured_inline_current_rows_or_latest,
    },
};

enum CurrentInlineStats {
    Unavailable,
    MissingTable,
    Available {
        current_rows: crate::runtime_inline_snapshot::capture::CapturedInlineRowsPayload,
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
    // Capture construction pins its own view. A completed capture needs no new GRV.
    if let Some(output) = read_current_inline_rows_payload(kv, catalog, &payload)? {
        return Ok(output);
    }
    let read_kv = crate::runtime_file_listing::runtime_read_snapshot_kv(kv)?;
    read_inline_rows_payload(&read_kv, catalog, payload)
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
    let current_rows = context.fetch_current_rows(kv, catalog)?;
    let Some(current_rows) = current_rows else {
        return Ok(None);
    };
    let mut out = inline_columns_payload(&context.schema_table)?;
    writeln!(out, "inline_payload_count={}", current_rows.rows.len()).map_err(|error| {
        crate::CatalogError::Decode(format!(
            "failed to render current inline payload count: {error}"
        ))
    })?;
    for (row_id, row) in &current_rows.rows {
        // Capture preparation already validated the row and accumulated its statistics.
        let values = std::str::from_utf8(&row.payload)
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "inline current-row payload is not utf8: {error}"
                ))
            })?
            .trim_end_matches('\n')
            .splitn(3, '\t')
            .nth(2)
            .unwrap_or_default();
        writeln!(
            out,
            "row_change\t{}\t\t{}\t{}",
            row.begin_sequence.0, row_id, values
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render current inline row: {error}"))
        })?;
    }
    out.push_str(&current_rows.global_stats);
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
            current_rows,
            schema_table,
        } => {
            let mut out = inline_columns_payload(&schema_table)?;
            writeln!(out, "inline_payload_count={}", current_rows.rows.len()).map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render current inline payload count: {error}"
                ))
            })?;
            out.push_str(&current_rows.global_stats);
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
        CurrentInlineStats::Available { current_rows, .. } => {
            Ok(current_rows.aggregate_stats.as_bytes().to_vec())
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
    let current_rows = context.fetch_current_rows(kv, catalog)?;
    let Some(current_rows) = current_rows else {
        return Ok(CurrentInlineStats::Unavailable);
    };
    record_inline_stage("ReadInlineRowsForCurrentStats", "Index", started);
    Ok(CurrentInlineStats::Available {
        current_rows,
        schema_table: Box::new(context.schema_table),
    })
}

impl InlineTableSnapshotContext {
    fn fetch_current_rows(
        &self,
        kv: &FdbOrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Option<crate::runtime_inline_snapshot::capture::CapturedInlineRowsPayload>>
    {
        let current_snapshot_capture_required =
            crate::runtime_inline_snapshot::capture::capture_inline_current_rows_at_read_version(
                kv,
                catalog,
                &self.table,
                self.snapshot.order,
            )?;
        captured_inline_current_rows_or_latest(
            captured_inline_current_rows(
                kv,
                catalog,
                self.table.table_id,
                self.schema_id,
                self.snapshot.order,
            ),
            current_snapshot_capture_required,
            || {
                let Some(latest) = latest_snapshot(kv, catalog)? else {
                    return Ok(None);
                };
                if self.snapshot.order != latest.order {
                    return Ok(None);
                }
                let mut rows = crate::runtime_inline_snapshot::capture::CapturedInlineRows {
                    rows: list_inline_current_rows(
                        kv,
                        catalog,
                        self.table.table_id,
                        self.schema_id,
                    )?,
                    next_row_id: load_inline_next_row_id(
                        kv,
                        catalog,
                        self.table.table_id,
                        self.schema_id,
                    )?,
                    ..Default::default()
                };
                rows.prepare_stats(&self.schema_table)?;
                Ok(Some(std::sync::Arc::new(rows)))
            },
        )
    }
}

impl crate::runtime_inline_snapshot::capture::CapturedInlineRows {
    pub(crate) fn prepare_stats(&mut self, schema: &crate::TableRow) -> CatalogResult<()> {
        let mut stats = InlineCatalogStats::for_inline_schema(schema);
        stats.observe_next_row_id(self.next_row_id);
        for (row_id, row) in &self.rows {
            stats.accumulate_current_row_payload(*row_id, &row.payload)?;
        }
        let mut global = String::new();
        stats.append_to(&mut global)?;
        let mut aggregate = "inline_active_delete_count\t0\n".to_owned();
        stats.append_aggregate_to(&mut aggregate)?;
        self.global_stats = global;
        self.aggregate_stats = aggregate;
        Ok(())
    }
}
