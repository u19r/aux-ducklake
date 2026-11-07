use std::{
    env,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, InlinedTableRow, PartitionKeyIndex, SchemaId,
    TableColumnRow, TableId, TablePartitionChange, TablePartitionFieldRow, TablePartitionRow,
    TableRow, TableSortFieldRow, TableSortRow, commit_change_table_partition, expire_snapshots,
    latest_snapshot, list_current_data_files,
    list_current_data_files_for_partition_scan_with_deletes,
    list_data_files_for_partition_scan_at_with_deletes, list_file_column_stats_for_table_column,
    list_inline_table_payloads_at, list_old_data_files_for_cleanup,
    register_file_column_stats_batch, remove_old_data_files,
};

use super::{
    args::Args,
    json_artifact::{Batch, BatchResult},
};

const HIGH_TABLE_BASE: u64 = 1_000;
const SHAPED_TABLE: TableId = TableId(500);
const INLINE_TABLE: TableId = TableId(501);
const CLEANUP_TABLE: TableId = TableId(502);
const PARTITION_HISTORY_TABLE: TableId = TableId(503);
const COMPACTION_TABLE: TableId = TableId(504);

pub(crate) fn production_shape_batches(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    args: &Args,
) -> Result<Vec<Batch>, Box<dyn std::error::Error>> {
    Ok(vec![
        high_table_count(kv, catalog, args)?,
        partition_sort_stats(kv, catalog)?,
        historical_partition_pruning(kv, catalog)?,
        inline_and_cleanup(kv, catalog)?,
        compaction_cleanup(kv, catalog)?,
    ])
}

fn high_table_count(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    args: &Args,
) -> Result<Batch, Box<dyn std::error::Error>> {
    timed("high_table_count", || {
        let mut transactions = 0_u64;
        for table_index in 0..args.high_tables {
            let table_id = TableId(HIGH_TABLE_BASE + table_index as u64);
            create_table(kv, catalog, table_id, &format!("many_{table_index}"))?;
            transactions += 1;
            let rows = (0..args.high_rows)
                .map(|row_index| {
                    data_file(
                        10_000_000 + (table_index as u64 * 10_000) + row_index as u64,
                        table_id,
                        format!("many-{table_index}-{row_index}.parquet"),
                        1,
                    )
                })
                .collect::<Vec<_>>();
            kv.append_data_files_versionstamped(catalog, rows)?;
            transactions += 1;
        }
        let opened = latest_snapshot(kv, catalog)?.ok_or("missing latest snapshot")?;
        Ok(BatchResult::new()
            .label(
                "fdb_high_table_count",
                format!("{},{}", args.high_tables, args.high_tables * args.high_rows),
            )
            .operation("fdb_transactions", transactions)
            .operation("created_tables", args.high_tables)
            .operation("appended_files", args.high_tables * args.high_rows)
            .estimate("catalog_open_latest_sequence", opened.sequence)
            .estimate("target_high_tables", args.high_tables)
            .estimate("target_rows_per_table", args.high_rows))
    })
}

fn partition_sort_stats(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
) -> Result<Batch, Box<dyn std::error::Error>> {
    timed("partition_sort_stats", || {
        let mut table = table_with_columns(SHAPED_TABLE, "shaped_probe");
        table.partition = Some(TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(2), "identity")],
        ));
        table.sort = Some(TableSortRow::new(
            1,
            vec![TableSortFieldRow::new(
                0,
                "id",
                "duckdb",
                "ASC",
                "NULLS FIRST",
            )],
        ));
        kv.create_table_versionstamped(catalog, table, None)?;
        let data_files = (0..40)
            .map(|index| data_file(20_000 + index, SHAPED_TABLE, format!("shaped-{index}"), 1))
            .collect::<Vec<_>>();
        let partition_values = data_files
            .iter()
            .map(|file| {
                FilePartitionValueRow::new(
                    file.data_file_id,
                    file.table_id,
                    PartitionKeyIndex(0),
                    if file.data_file_id.0 % 2 == 0 {
                        "even"
                    } else {
                        "odd"
                    },
                )
            })
            .collect::<Vec<_>>();
        let appended = kv
            .commit_data_mutation_versionstamped(
                catalog,
                None,
                ducklake_catalog::FdbDataMutation::new(
                    data_files,
                    Vec::new(),
                    Vec::new(),
                    partition_values,
                    Vec::new(),
                ),
            )?
            .data_files;
        let mut stats_rows = Vec::with_capacity(appended.len() * 2);
        for file in &appended {
            stats_rows.push(FileColumnStatsRow::new(
                file.data_file_id,
                file.table_id,
                ColumnId(1),
                0,
                Some(file.data_file_id.0.to_string()),
                Some(file.data_file_id.0.to_string()),
            ));
            stats_rows.push(FileColumnStatsRow::new(
                file.data_file_id,
                file.table_id,
                ColumnId(2),
                0,
                Some("even".to_owned()),
                Some("odd".to_owned()),
            ));
        }
        let stats_count = stats_rows.len();
        let mut stats_kv = mutable_handle(kv)?;
        register_file_column_stats_batch(&mut stats_kv, catalog, stats_rows)?;
        let even = list_current_data_files_for_partition_scan_with_deletes(
            kv,
            catalog,
            SHAPED_TABLE,
            PartitionKeyIndex(0),
            "even",
        )?;
        let id_stats =
            list_file_column_stats_for_table_column(kv, catalog, SHAPED_TABLE, ColumnId(1))?;
        let bucket_stats =
            list_file_column_stats_for_table_column(kv, catalog, SHAPED_TABLE, ColumnId(2))?;
        Ok(BatchResult::new()
            .label(
                "fdb_partition_stats",
                format!("{},{},{}", even.len(), id_stats.len(), bucket_stats.len()),
            )
            .operation("fdb_transactions", 4)
            .operation("partitioned_files", appended.len())
            .operation("stats_rows", stats_count)
            .estimate("partition_selectivity_even", even.len())
            .estimate("predicate_columns", 2))
    })
}

fn historical_partition_pruning(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
) -> Result<Batch, Box<dyn std::error::Error>> {
    timed("historical_partition_pruning", || {
        create_table(kv, catalog, PARTITION_HISTORY_TABLE, "partition_history")?;
        kv.append_data_files_versionstamped(
            catalog,
            (0..4)
                .map(|index| {
                    data_file(
                        40_000 + index,
                        PARTITION_HISTORY_TABLE,
                        format!("history-unpartitioned-{index}.parquet"),
                        1,
                    )
                })
                .collect(),
        )?;
        let before_partition = latest_snapshot(kv, catalog)?.ok_or("missing partition snapshot")?;
        let mut partition_kv = mutable_handle(kv)?;
        commit_change_table_partition(
            &mut partition_kv,
            catalog,
            &TablePartitionChange::new(
                PARTITION_HISTORY_TABLE,
                Some(TablePartitionRow::new(
                    1,
                    vec![TablePartitionFieldRow::new(0, ColumnId(2), "identity")],
                )),
            ),
            None,
        )?;
        let partitioned = (0..6)
            .map(|index| {
                data_file(
                    40_100 + index,
                    PARTITION_HISTORY_TABLE,
                    format!("history-partitioned-{index}.parquet"),
                    1,
                )
            })
            .collect::<Vec<_>>();
        let partition_values = partitioned
            .iter()
            .map(|file| {
                FilePartitionValueRow::new(
                    file.data_file_id,
                    file.table_id,
                    PartitionKeyIndex(0),
                    if file.data_file_id.0 % 2 == 0 {
                        "even"
                    } else {
                        "odd"
                    },
                )
            })
            .collect();
        kv.commit_data_mutation_versionstamped(
            catalog,
            None,
            ducklake_catalog::FdbDataMutation::new(
                partitioned,
                Vec::new(),
                Vec::new(),
                partition_values,
                Vec::new(),
            ),
        )?;
        let current_even = list_current_data_files_for_partition_scan_with_deletes(
            kv,
            catalog,
            PARTITION_HISTORY_TABLE,
            PartitionKeyIndex(0),
            "even",
        )?;
        let historical_even = list_data_files_for_partition_scan_at_with_deletes(
            kv,
            catalog,
            PARTITION_HISTORY_TABLE,
            PartitionKeyIndex(0),
            "even",
            before_partition.order,
        )?;
        Ok(BatchResult::new()
            .label(
                "fdb_historical_partition_pruning",
                format!("{},{}", current_even.len(), historical_even.len()),
            )
            .operation("fdb_transactions", 4)
            .operation(
                "unpartitioned_files_visible_historically",
                historical_even.len(),
            )
            .operation("current_partition_scan_files", current_even.len())
            .estimate("current_even_partitioned_files", 3)
            .estimate("pre_partition_unpartitioned_files", 4))
    })
}

fn inline_and_cleanup(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
) -> Result<Batch, Box<dyn std::error::Error>> {
    timed("inline_cleanup_churn", || {
        create_table(kv, catalog, INLINE_TABLE, "inline_bench")?;
        let mut inline_table = table_with_columns(INLINE_TABLE, "inline_bench");
        inline_table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_501_1", 1));
        kv.register_inline_table_payload_with_table_versionstamped(
            catalog,
            inline_table,
            SchemaId(1),
            b"row\t1\tone\nrow\t2\ttwo\n".to_vec(),
        )?;
        let inline_snapshot = latest_snapshot(kv, catalog)?.ok_or("missing inline snapshot")?;
        kv.commit_delete_inline_table_rows_versionstamped(
            catalog,
            INLINE_TABLE,
            SchemaId(1),
            &[1],
            None,
        )?;
        let delete_snapshot = latest_snapshot(kv, catalog)?.ok_or("missing delete snapshot")?;
        let payloads = list_inline_table_payloads_at(
            kv,
            catalog,
            INLINE_TABLE,
            SchemaId(1),
            delete_snapshot.order,
        )?;

        create_table(kv, catalog, CLEANUP_TABLE, "cleanup_bench")?;
        let [old_file] = kv
            .append_data_files_versionstamped(
                catalog,
                vec![data_file(30_000, CLEANUP_TABLE, "cleanup-old.parquet", 1)],
            )?
            .try_into()
            .map_err(|_| "cleanup append did not return one file")?;
        let append_snapshot = latest_snapshot(kv, catalog)?.ok_or("missing cleanup snapshot")?;
        kv.expire_data_file_versionstamped(catalog, old_file.data_file_id)?;
        kv.append_data_files_versionstamped(
            catalog,
            vec![data_file(30_001, CLEANUP_TABLE, "cleanup-new.parquet", 1)],
        )?;
        let mut cleanup_kv = mutable_handle(kv)?;
        expire_snapshots(
            &mut cleanup_kv,
            catalog,
            &[
                inline_snapshot.sequence,
                delete_snapshot.sequence,
                append_snapshot.sequence,
            ],
        )?;
        let cleanup = list_old_data_files_for_cleanup(kv, catalog)?;
        let removed = remove_old_data_files(&mut cleanup_kv, catalog, &[old_file.data_file_id])?;
        Ok(BatchResult::new()
            .label(
                "fdb_inline_cleanup",
                format!("{},{},{}", payloads.len(), cleanup.len(), removed.len()),
            )
            .operation("fdb_transactions", 8)
            .operation("inline_payloads_visible_after_delete", payloads.len())
            .operation("old_data_files_removed", removed.len())
            .estimate("expired_snapshots", 3)
            .estimate("inline_payload_bytes", 22))
    })
}

fn compaction_cleanup(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
) -> Result<Batch, Box<dyn std::error::Error>> {
    timed("compaction_cleanup", || {
        create_table(kv, catalog, COMPACTION_TABLE, "compaction_bench")?;
        let files = kv.append_data_files_versionstamped(
            catalog,
            (0..10)
                .map(|index| {
                    data_file(
                        50_000 + index,
                        COMPACTION_TABLE,
                        format!("compact-source-{index}.parquet"),
                        1,
                    )
                })
                .collect(),
        )?;
        let append_snapshot = latest_snapshot(kv, catalog)?.ok_or("missing compaction snapshot")?;
        let source_ids = files
            .iter()
            .take(4)
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>();
        kv.commit_data_mutation_versionstamped(
            catalog,
            None,
            ducklake_catalog::FdbDataMutation::new(
                vec![data_file(
                    50_999,
                    COMPACTION_TABLE,
                    "compact-replacement.parquet",
                    4,
                )],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                source_ids.clone(),
            ),
        )?;
        let current = list_current_data_files(kv, catalog, COMPACTION_TABLE)?;
        let mut compaction_kv = mutable_handle(kv)?;
        expire_snapshots(&mut compaction_kv, catalog, &[append_snapshot.sequence])?;
        let cleanup = list_old_data_files_for_cleanup(kv, catalog)?;
        let removable = cleanup
            .iter()
            .map(|row| row.data_file_id)
            .filter(|id| source_ids.contains(id))
            .collect::<Vec<_>>();
        let removed = remove_old_data_files(&mut compaction_kv, catalog, &removable)?;
        Ok(BatchResult::new()
            .label(
                "fdb_compaction_cleanup",
                format!("{},{},{}", current.len(), removable.len(), removed.len()),
            )
            .operation("fdb_transactions", 5)
            .operation("compacted_source_files", source_ids.len())
            .operation("old_data_files_removed", removed.len())
            .estimate("current_files_after_compaction", current.len()))
    })
}

fn timed(
    name: &str,
    run: impl FnOnce() -> Result<BatchResult, Box<dyn std::error::Error>>,
) -> Result<Batch, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let result = run()?;
    Ok(Batch {
        name: name.to_owned(),
        duration_ms: started.elapsed().as_secs_f64() * 1000.0,
        labels: result.labels,
        operation_counts: result.operation_counts,
        transaction_estimates: result.transaction_estimates,
    })
}

fn create_table(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    kv.create_table_versionstamped(catalog, table_with_columns(table_id, name), None)?;
    Ok(())
}

fn table_with_columns(table_id: TableId, name: &str) -> TableRow {
    TableRow::with_catalog_metadata(
        table_id,
        SchemaId(0),
        format!("benchmark-{name}-{}", now_micros()),
        name,
        format!("main/{name}"),
        vec![
            TableColumnRow::new(ColumnId(1), "id", "INTEGER", false, None),
            TableColumnRow::new(ColumnId(2), "bucket", "VARCHAR", true, None),
        ],
        CatalogOrderId::uuid_v7(0),
    )
}

fn data_file(id: u64, table: TableId, path: impl Into<String>, records: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        path,
        records,
        records.saturating_mul(10),
        CatalogOrderId::uuid_v7(0),
    )
}

fn mutable_handle(
    kv: &FdbOrderedCatalogKv,
) -> Result<FdbOrderedCatalogKv, Box<dyn std::error::Error>> {
    let cluster_file = env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
    Ok(FdbOrderedCatalogKv::open_with_prefix(
        cluster_file.as_deref(),
        kv.key_prefix().to_vec(),
    )?)
}

fn now_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0)
}
