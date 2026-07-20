#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

use crate::{CatalogId, CatalogResult, OrderedCatalogKv, SnapshotRow, list_all_snapshots};

#[cfg(feature = "foundationdb")]
use crate::runtime_file_listing::{ListDataFilesAtPayload, foundationdb_data_files_at_payload};
#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, FdbOrderedCatalogKv, TableId, list_tables_at,
    runtime_inline_rows::{
        ReadInlineRowsPayload, read_inline_rows_aggregate_stats_payload,
        read_inline_rows_global_stats_payload,
    },
    runtime_snapshot_range::ReadSnapshot,
    snapshot_operations::{SnapshotOperationKind, snapshot_operation_table_ids_at},
};

#[cfg(feature = "foundationdb")]
use crate::runtime_metadata_ops::*;

#[cfg(feature = "foundationdb")]
pub(super) fn global_stats_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    requested_table_id: Option<TableId>,
) -> CatalogResult<Vec<u8>> {
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = list_tables_at(kv, catalog, snapshot.order)?
        .into_iter()
        .filter(|table| {
            requested_table_id
                .map(|table_id| table.table_id == table_id)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let mut stats_by_table = tables
        .iter()
        .map(|table| (table.table_id, GlobalTableStats::new(table)))
        .collect::<BTreeMap<_, _>>();
    let mut data_file_ids = BTreeSet::new();
    let rewrite_tables = snapshot_operation_table_ids_at(
        kv,
        catalog,
        snapshot.order,
        SnapshotOperationKind::RewriteDelete,
    )?;

    for table in &tables {
        let table_files = foundationdb_data_files_at_payload(
            kv,
            catalog,
            ListDataFilesAtPayload {
                snapshot_id: resolved_snapshot_id,
                table_id: table.table_id,
            },
        )?;
        let table_file_rows = global_stats_file_rows_from_payload(&table_files)?;
        let exact_inline_stats = can_recompute_exact_inline_stats(
            rewrite_tables.contains(&table.table_id),
            &table_file_rows,
        );
        for row in table_file_rows {
            data_file_ids.insert(row.data_file_id);
            if let Some(stats) = stats_by_table.get_mut(&row.table_id) {
                stats.accumulate_file(&row);
            }
        }

        for inlined_table in &table.inlined_data_tables {
            let payload = ReadInlineRowsPayload {
                table_name: inlined_table.table_name.clone(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(resolved_snapshot_id))),
                include_flushed: false,
                include_deleted: false,
            };
            let inline_stats = if exact_inline_stats {
                read_inline_rows_aggregate_stats_payload(kv, catalog, payload)?
            } else {
                read_inline_rows_global_stats_payload(kv, catalog, payload)?
            };
            if let Some(stats) = stats_by_table.get_mut(&table.table_id) {
                stats.accumulate_inline_payload(&inline_stats)?;
            }
        }
    }

    let file_stats = list_file_column_stats_for_data_file_ids(
        kv,
        catalog,
        &data_file_ids.into_iter().collect::<Vec<_>>(),
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
pub(super) fn global_stats_inputs_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    include_inline_stats: bool,
    include_file_column_stats: bool,
    requested_table_id: Option<TableId>,
) -> CatalogResult<Vec<u8>> {
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = list_tables_at(kv, catalog, snapshot.order)?
        .into_iter()
        .filter(|table| {
            requested_table_id
                .map(|table_id| table.table_id == table_id)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
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
                let inline_stats = read_inline_rows_global_stats_payload(
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
    if let Some(snapshot) = list_all_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence.0 == snapshot_id)
        .max_by_key(|snapshot| snapshot.order)
    {
        return Ok(snapshot);
    }
    Ok(latest)
}
