use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    FilePartitionValueRow, MergeAdjacentCompaction, PartitionKeyIndex, SchemaId, TableColumnRow,
    TableId, TablePartitionChange, TablePartitionFieldRow, TablePartitionRow, TableRename,
    TableRow, commit_change_table_partition, commit_create_table_row,
    commit_data_mutation_with_file_partitions, commit_merge_adjacent_data_files,
    commit_rename_tables, initialize_catalog_if_absent, latest_snapshot,
    list_current_data_files_by_partition_value,
    list_current_data_files_for_partition_scan_with_deletes, list_file_partition_values,
    load_table_at,
};

#[test]
fn mirrors_bucket_partitioning_test_bucket_transform_and_values_are_stored() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/bucket_partitioning.test
    //
    // Storage contract:
    // - Bucket partitioning is stored as table partition metadata with transform `bucket(n)`.
    // - Flushed files carry the bucket partition values DuckLake calculated.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "bucketed_tbl",
        vec![
            column(1, "user_id", "varchar"),
            column(2, "value", "integer"),
        ],
    );
    set_partition(
        &mut kv,
        catalog,
        table,
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "bucket(4)")],
        ),
    );
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 2),
            data_file(2, table, 2, 2),
            data_file(3, table, 4, 1),
        ],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "1"),
            partition_value(2, table, 0, "2"),
            partition_value(3, table, 0, "3"),
        ],
    )
    .unwrap();

    let current = current_table(&kv, catalog, table);
    assert_eq!(current.partition.unwrap().fields[0].transform, "bucket(4)");
    assert_partition_files(&kv, catalog, table, "1", &[1]);
    assert_partition_files(&kv, catalog, table, "2", &[2]);
    assert_partition_files(&kv, catalog, table, "3", &[3]);
}

#[test]
fn mirrors_bucket_pruning_test_partition_scan_uses_bucket_partition_values() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/bucket_pruning.test
    //
    // Storage contract:
    // - DuckLake stores one file per bucket value.
    // - Partition scans can retrieve the exact file set for a bucket predicate.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "b1_no_stats",
        vec![column(1, "i", "integer")],
    );
    set_partition(
        &mut kv,
        catalog,
        table,
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "bucket(10)")],
        ),
    );
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        (0..10)
            .map(|bucket| data_file(bucket + 1, table, bucket * 100, 100))
            .collect(),
        Vec::new(),
        &[],
        (0..10)
            .map(|bucket| partition_value(bucket + 1, table, 0, &bucket.to_string()))
            .collect(),
    )
    .unwrap();

    assert_partition_files(&kv, catalog, table, "6", &[7]);
    assert_eq!(
        list_current_data_files_for_partition_scan_with_deletes(
            &kv,
            catalog,
            table,
            PartitionKeyIndex(0),
            "6",
        )
        .unwrap()[0]
            .data_file
            .data_file_id,
        DataFileId(7)
    );
}

#[test]
fn mirrors_merge_adjacent_null_partition_test_null_partition_values_survive_merge() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/merge_adjacent_null_partition.test
    //
    // Storage contract:
    // - SQL NULL partition values are stored consistently, including after add-files and merge.
    // - Multi-column partition replacements keep one partition value row per key.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "id", "integer"), column(2, "tag", "varchar")],
    );
    set_partition(
        &mut kv,
        catalog,
        table,
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(2), "identity")],
        ),
    );
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1), data_file(2, table, 1, 1)],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "__HIVE_DEFAULT_PARTITION__"),
            partition_value(2, table, 0, "__HIVE_DEFAULT_PARTITION__"),
        ],
    )
    .unwrap();
    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![data_file(3, table, 0, 2)],
            partition_values: vec![partition_value(3, table, 0, "__HIVE_DEFAULT_PARTITION__")],
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    assert_partition_files(&kv, catalog, table, "__HIVE_DEFAULT_PARTITION__", &[3]);
    assert_eq!(
        partition_values_for_file(&kv, catalog, DataFileId(3)),
        vec![partition_value(3, table, 0, "__HIVE_DEFAULT_PARTITION__")]
    );
}

#[test]
fn mirrors_multi_table_partition_test_partition_changes_are_independent_per_table() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/multi_table_partition.test
    //
    // Storage contract:
    // - Two partition changes in one DuckLake transaction become independent table partition
    //   metadata rows keyed by table id.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "partitioned_tbl",
        vec![column(1, "part_key", "integer")],
    );
    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "partitioned_tbl2",
        vec![column(1, "part_key", "integer")],
    );
    set_partition(
        &mut kv,
        catalog,
        TableId(1),
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ),
    );
    set_partition(
        &mut kv,
        catalog,
        TableId(2),
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ),
    );

    assert_eq!(
        current_table(&kv, catalog, TableId(1))
            .partition
            .unwrap()
            .fields[0]
            .transform,
        "identity"
    );
    assert_eq!(
        current_table(&kv, catalog, TableId(2))
            .partition
            .unwrap()
            .fields[0]
            .transform,
        "identity"
    );
}

#[test]
fn mirrors_partition_null_test_null_and_non_null_partition_values_are_separately_scannable() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/partition_null.test
    //
    // Storage contract:
    // - NULL, zero, and one partition values are stored as distinct file partition values.
    // - Partition scans can target each value separately.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "partitioned_tbl",
        vec![
            column(1, "part_key", "integer"),
            column(2, "values", "varchar"),
        ],
    );
    set_partition(
        &mut kv,
        catalog,
        table,
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ),
    );
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 3333),
            data_file(2, table, 3333, 3333),
            data_file(3, table, 6666, 3334),
        ],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "0"),
            partition_value(2, table, 0, "1"),
            partition_value(3, table, 0, "__HIVE_DEFAULT_PARTITION__"),
        ],
    )
    .unwrap();

    assert_partition_files(&kv, catalog, table, "0", &[1]);
    assert_partition_files(&kv, catalog, table, "1", &[2]);
    assert_partition_files(&kv, catalog, table, "__HIVE_DEFAULT_PARTITION__", &[3]);
}

#[test]
fn mirrors_partition_rename_in_transaction_test_renamed_table_keeps_partition_metadata() {
    // Mirrors: third_party/ducklake/test/sql/partitioning/partition_rename_in_transaction.test
    //
    // Storage contract:
    // - Renaming a partitioned table must preserve its partition metadata on the same table id.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, "t1", vec![column(1, "dt", "date")]);
    set_partition(
        &mut kv,
        catalog,
        table,
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ),
    );
    commit_rename_tables(
        &mut kv,
        catalog,
        &[TableRename::new(table, "auto_probe_after")],
    )
    .unwrap();

    let current = current_table(&kv, catalog, table);
    assert_eq!(current.name, "auto_probe_after");
    assert_eq!(
        current.partition.unwrap().fields,
        vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")]
    );
}

fn create_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    name: &str,
    columns: Vec<TableColumnRow>,
) -> TableRow {
    commit_create_table_row(
        kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(1),
            format!("table-{}", table.0),
            name,
            format!("main/{name}"),
            columns,
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap()
}

fn column(id: u64, name: &str, column_type: &str) -> TableColumnRow {
    TableColumnRow::new(ColumnId(id), name, column_type, true, None)
}

fn set_partition(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    partition: TablePartitionRow,
) {
    commit_change_table_partition(
        kv,
        catalog,
        &TablePartitionChange::new(table, Some(partition)),
        None,
    )
    .unwrap();
}

fn current_table(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> TableRow {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    load_table_at(kv, catalog, table, latest.order)
        .unwrap()
        .unwrap()
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/table-{}/file-{id}.parquet", table.0),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn partition_value(
    data_file_id: u64,
    table: TableId,
    key_index: u32,
    value: &str,
) -> FilePartitionValueRow {
    FilePartitionValueRow::new(
        DataFileId(data_file_id),
        table,
        PartitionKeyIndex(key_index),
        value,
    )
}

fn assert_partition_files(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    value: &str,
    expected: &[u64],
) {
    let ids =
        list_current_data_files_by_partition_value(kv, catalog, table, PartitionKeyIndex(0), value)
            .unwrap()
            .into_iter()
            .map(|file| file.data_file_id.0)
            .collect::<Vec<_>>();
    assert_eq!(ids, expected);
}

fn partition_values_for_file(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> Vec<FilePartitionValueRow> {
    list_file_partition_values(kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|row| row.data_file_id == data_file_id)
        .collect()
}
