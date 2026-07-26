#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

use crate::{CatalogId, CatalogResult, OrderedCatalogKv, SnapshotRow};

#[cfg(feature = "foundationdb")]
use crate::runtime_file_listing::{
    ListDataFilesAtPayload, foundationdb_attached_data_files_at_order,
    foundationdb_data_files_at_payload,
};
#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, FdbOrderedCatalogKv, TableId,
    conflict_watermarks::load_table_next_row_ids,
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
use crate::runtime_metadata_ops::*;

#[cfg(feature = "foundationdb")]
const MAX_GLOBAL_STATS_WORKERS: usize = 100;

#[cfg(feature = "foundationdb")]
pub(super) fn global_stats_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    requested_table_ids: &BTreeSet<TableId>,
) -> CatalogResult<Vec<u8>> {
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = tables_for_global_stats(kv, catalog, snapshot.order, requested_table_ids)?;
    let table_ids = tables
        .iter()
        .map(|table| table.table_id)
        .collect::<Vec<_>>();
    let allocated_next_row_ids = load_table_next_row_ids(kv, catalog, &table_ids)?;
    let mut stats_by_table = BTreeMap::new();
    let columns_by_table = tables
        .iter()
        .map(|table| (table.table_id, leaf_column_ids(table)))
        .collect::<BTreeMap<_, _>>();
    let mut data_files = BTreeMap::new();
    let rewrite_tables = snapshot_operation_table_ids_at(
        kv,
        catalog,
        snapshot.order,
        SnapshotOperationKind::RewriteDelete,
    )?;
    let read_context_id = crate::store::active_runtime_read_context_id();
    for chunk in tables.chunks(MAX_GLOBAL_STATS_WORKERS) {
        let results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for table in chunk.iter().cloned() {
                let kv = kv.clone();
                let rewrite = rewrite_tables.contains(&table.table_id);
                let allocated_next_row_id = allocated_next_row_ids
                    .get(&table.table_id)
                    .copied()
                    .unwrap_or_default();
                handles.push(scope.spawn(move || {
                    let _read_request = crate::store::begin_runtime_read_request(read_context_id);
                    global_table_stats(
                        &kv,
                        catalog,
                        resolved_snapshot_id,
                        snapshot.order,
                        table,
                        rewrite,
                        allocated_next_row_id,
                    )
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
                .collect::<CatalogResult<Vec<_>>>()
        })?;
        for (table_id, stats, table_data_files) in results {
            stats_by_table.insert(table_id, stats);
            data_files.extend(
                table_data_files
                    .into_iter()
                    .map(|file| (file.data_file_id, file)),
            );
        }
    }

    let file_stats = crate::file_stats::list_file_column_stats_for_data_files(
        kv,
        catalog,
        &data_files.into_values().collect::<Vec<_>>(),
        &columns_by_table,
    )?;
    for row in file_stats {
        if let Some(stats) = stats_by_table.get_mut(&row.table_id) {
            stats.accumulate_file_column_stats(row);
        }
    }

    let mut out = format!("global_stats_snapshot={snapshot_id}\n");
    for table in &tables {
        if let Some(stats) = stats_by_table.get(&table.table_id) {
            stats.append_to(&mut out)?;
        }
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
fn global_table_stats(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    resolved_snapshot_id: u64,
    snapshot_order: crate::CatalogOrderId,
    table: crate::TableRow,
    rewrite: bool,
    allocated_next_row_id: u64,
) -> CatalogResult<(TableId, GlobalTableStats, Vec<crate::DataFileRow>)> {
    let table_files =
        foundationdb_attached_data_files_at_order(kv, catalog, table.table_id, snapshot_order)?;
    let visible_file_ids = table_files
        .iter()
        .map(|attached| attached.data_file.data_file_id)
        .collect::<BTreeSet<_>>();
    let inline_deletions = list_inline_file_deletions_for_data_files_at(
        kv,
        catalog,
        table.table_id,
        snapshot_order,
        &visible_file_ids,
    )?;
    let table_file_rows = table_files
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
        .collect::<Vec<_>>();
    let exact_inline_stats = can_recompute_exact_inline_stats(rewrite, &table_file_rows);
    let mut stats = GlobalTableStats::new(&table, allocated_next_row_id);
    for row in table_file_rows {
        stats.accumulate_file(&row);
    }
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
    Ok((
        table.table_id,
        stats,
        table_files
            .into_iter()
            .map(|attached| attached.data_file)
            .collect(),
    ))
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
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = tables_for_global_stats(kv, catalog, snapshot.order, requested_table_ids)?;
    let mut out = format!("global_stats_input_snapshot={snapshot_id}\n");
    let mut data_file_ids = BTreeSet::new();
    for table in &tables {
        let table_files = foundationdb_data_files_at_payload(
            kv,
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
                    kv,
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
            kv,
            catalog,
            &data_file_ids.into_iter().collect::<Vec<_>>(),
        )?;
        out.push_str(
            std::str::from_utf8(&file_column_stats_payload(file_stats)?).map_err(|error| {
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
