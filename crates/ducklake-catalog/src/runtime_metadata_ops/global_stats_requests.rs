#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "foundationdb")]
use super::file_stats_aggregate::{
    FileStatsAggregateInput, FileStatsReadStamp, StatsTableInputs, fetch_file_stats_aggregate,
};
#[cfg(feature = "foundationdb")]
use crate::file_stats::list_file_column_stats_for_data_file_ids;
#[cfg(feature = "foundationdb")]
use crate::runtime_file_listing::{
    ListDataFilesAtPayload, foundationdb_attached_data_files_at_order,
    foundationdb_data_files_at_payload,
};
#[cfg(feature = "foundationdb")]
use crate::runtime_metadata_ops::*;
use crate::{CatalogId, CatalogResult, OrderedCatalogKv, SnapshotRow};
#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, FdbOrderedCatalogKv, TableId,
    inline_data::list_inline_file_deletions_for_data_files_at,
    list_tables_at,
    runtime_inline_rows::{
        ReadInlineRowsPayload, read_foundationdb_inline_rows_aggregate_stats_payload,
        read_foundationdb_inline_rows_global_stats_payload,
    },
    runtime_snapshot_range::ReadSnapshot,
    snapshot_operations::{SnapshotOperationKind, snapshot_operation_table_ids_at},
    table_store::load_current_table_rows,
};

#[cfg(feature = "foundationdb")]
const MAX_GLOBAL_STATS_WORKERS: usize = 100;

#[cfg(feature = "foundationdb")]
pub(super) fn global_stats_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    requested_table_ids: &BTreeSet<TableId>,
) -> CatalogResult<Vec<u8>> {
    // Pin the complete stats operation. Worker threads receive this immutable
    // view, so file membership
    // and inline stats cannot straddle a concurrent flush.
    let read_kv = kv.with_read_version()?;
    let snapshot = stats_snapshot_for_request(&read_kv, catalog, snapshot_id)?;
    let StatsTableInputs {
        tables,
        stamps: mut read_stamps,
    } = StatsTableInputs::fetch(&read_kv, catalog, snapshot.order, requested_table_ids)?;
    let mut stats_by_table = BTreeMap::new();
    let rewrite_tables = snapshot_operation_table_ids_at(
        &read_kv,
        catalog,
        snapshot.order,
        SnapshotOperationKind::RewriteDelete,
    )?;
    let read_context_id = crate::store::active_runtime_read_context_id();
    let trace_parent = crate::runtime_trace::RuntimeTraceParent::capture();
    for chunk in tables.chunks(MAX_GLOBAL_STATS_WORKERS) {
        let results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for table in chunk {
                let kv = &read_kv;
                let rewrite = rewrite_tables.contains(&table.table_id);
                let source = read_stamps.remove(&table.table_id).unwrap_or_default();
                let request = TableStatsRequest {
                    snapshot: &snapshot,
                    table,
                    is_rewrite: rewrite,
                    source,
                };
                handles.push(scope.spawn(move || {
                    let span = trace_parent.enter("method.global_stats.Worker");
                    let _read_request = crate::store::begin_runtime_read_request(read_context_id);
                    let result = fetch_global_table_stats(kv, catalog, request);
                    span.finish(result.is_ok());
                    result
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        crate::CatalogError::Backend(
                            "global stats worker thread panicked".to_owned(),
                        )
                    })?
                })
                .collect::<CatalogResult<BTreeMap<_, _>>>()
        })?;
        for (table_id, stats) in results {
            stats_by_table.insert(table_id, stats);
        }
    }

    let mut out = format!("global_stats_snapshot={snapshot_id}\n");
    for table in &tables {
        if let Some(stats) = stats_by_table.remove(&table.table_id) {
            stats.append_to(&mut out)?;
        }
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
struct TableStatsRequest<'a> {
    snapshot: &'a SnapshotRow,
    table: &'a crate::TableRow,
    is_rewrite: bool,
    source: FileStatsReadStamp,
}

#[cfg(feature = "foundationdb")]
fn fetch_global_table_stats(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    request: TableStatsRequest<'_>,
) -> CatalogResult<(TableId, GlobalTableStats)> {
    let TableStatsRequest {
        snapshot,
        table,
        is_rewrite,
        source,
    } = request;
    let snapshot_order = snapshot.order;
    let resolved_snapshot_id = snapshot.sequence.0;
    let table_file_rows = fetch_stats_file_rows(kv, catalog, table.table_id, snapshot_order)?;
    let exact_inline_stats = can_recompute_exact_inline_stats(is_rewrite, &table_file_rows);
    let mut stats = fetch_file_stats_aggregate(
        kv,
        catalog,
        FileStatsAggregateInput {
            table,
            files: &table_file_rows,
            stamp: source,
        },
    )?;
    for inlined_table in &table.inlined_data_tables {
        let payload = ReadInlineRowsPayload {
            table_name: inlined_table.table_name.clone(),
            snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(resolved_snapshot_id))),
            include_flushed: false,
            include_deleted: false,
        };
        let inline_stats = if exact_inline_stats {
            read_foundationdb_inline_rows_aggregate_stats_payload(kv, catalog, payload)?
        } else {
            read_foundationdb_inline_rows_global_stats_payload(kv, catalog, payload)?
        };
        stats.accumulate_inline_payload(&inline_stats)?;
    }
    Ok((table.table_id, stats))
}

#[cfg(feature = "foundationdb")]
fn fetch_stats_file_rows(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: crate::CatalogOrderId,
) -> CatalogResult<Vec<GlobalStatsFileRow>> {
    let table_files =
        foundationdb_attached_data_files_at_order(kv, catalog, table_id, snapshot_order)?;
    let visible_file_ids = table_files
        .iter()
        .map(|attached| attached.data_file.data_file_id)
        .collect::<BTreeSet<_>>();
    let inline_deletions = list_inline_file_deletions_for_data_files_at(
        kv,
        catalog,
        table_id,
        snapshot_order,
        &visible_file_ids,
    )?;
    Ok(table_files
        .iter()
        .map(|attached| {
            let file = &attached.data_file;
            GlobalStatsFileRow {
                data_file_id: file.data_file_id,
                record_count: file.record_count,
                file_size_bytes: file.file_size_bytes,
                row_id_start: file.row_id_start_known.then_some(file.row_id_start),
                has_deletions: attached.delete_file.is_some()
                    || inline_deletions.contains_key(&file.data_file_id),
            }
        })
        .collect::<Vec<_>>())
}

#[cfg(feature = "foundationdb")]
pub(super) fn global_stats_inputs_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    include_inline_stats: bool,
    include_file_column_stats: bool,
    requested_table_ids: &BTreeSet<TableId>,
) -> CatalogResult<Vec<u8>> {
    // Keep file inputs, inline stats, and optional file-column stats at one
    // read version. This is also the first-owner path for callers that ask for
    // stats without listing files separately.
    let read_kv = crate::runtime_file_listing::runtime_read_snapshot_kv(kv)?;
    let snapshot = stats_snapshot_for_request(&read_kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = tables_for_global_stats(&read_kv, catalog, snapshot.order, requested_table_ids)?;
    let mut out = format!("global_stats_input_snapshot={snapshot_id}\n");
    let mut data_file_ids = BTreeSet::new();
    for table in &tables {
        let table_files = foundationdb_data_files_at_payload(
            &read_kv,
            catalog,
            ListDataFilesAtPayload {
                snapshot_id: resolved_snapshot_id,
                table_id: table.table_id,
            },
        )?;
        collect_data_file_ids_from_payload(&table_files, &mut data_file_ids)?;
        out.push_str(std::str::from_utf8(&table_files).map_err(|error| {
            crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
        })?);

        if include_inline_stats {
            for inlined_table in &table.inlined_data_tables {
                let inline_stats = read_foundationdb_inline_rows_global_stats_payload(
                    &read_kv,
                    catalog,
                    ReadInlineRowsPayload {
                        table_name: inlined_table.table_name.clone(),
                        snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(resolved_snapshot_id))),
                        include_flushed: false,
                        include_deleted: false,
                    },
                )?;
                append_table_inline_stats(&mut out, table.table_id, &inline_stats)?;
            }
        }
    }

    if include_file_column_stats {
        let file_stats = list_file_column_stats_for_data_file_ids(
            &read_kv,
            catalog,
            &data_file_ids.into_iter().collect::<Vec<_>>(),
        )?;
        out.push_str(
            std::str::from_utf8(&file_column_stats_payload(&file_stats)?).map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "global stats file-column payload is not utf-8: {error}"
                ))
            })?,
        );
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
fn tables_for_global_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: crate::CatalogOrderId,
    requested_table_ids: &BTreeSet<TableId>,
) -> CatalogResult<Vec<crate::TableRow>> {
    if requested_table_ids.is_empty() {
        return list_tables_at(kv, catalog, snapshot_order);
    }
    load_current_table_rows(kv, catalog, requested_table_ids)
}

pub(super) fn stats_snapshot_for_request(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
) -> CatalogResult<SnapshotRow> {
    let Some(latest) = crate::latest_snapshot(kv, catalog)? else {
        return Err(crate::CatalogError::Decode(format!(
            "snapshot {snapshot_id} does not exist"
        )));
    };
    if snapshot_id <= latest.sequence.0 {
        return Ok(latest);
    }
    Ok(latest)
}
