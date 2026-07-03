use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    FilePartitionValueRow, InlineFileDeletionRow, PartitionKeyIndex, SchemaId, SchemaRow,
    TableColumnRow, TableId, TablePartitionFieldRow, TablePartitionRow, TableRow, ViewRow,
    commit_append_data_files, commit_create_schema_rows, commit_create_table_row,
    commit_create_view_row, commit_data_mutation_with_file_partitions_and_inline_deletes,
    commit_drop_schema_rows, commit_drop_tables, initialize_catalog_if_absent, latest_snapshot,
    list_current_data_files_for_partition_scan_with_deletes, list_data_files_with_deletes_at,
    list_inline_file_deletions_at, list_snapshots, public_snapshot_sequence_for_order,
    snapshot_by_public_sequence, snapshot_changes_made, snapshot_schema_version,
};

#[test]
fn mirrors_ducklake_snapshots_test_snapshot_listing_tracks_schema_and_data_changes() {
    // Mirrors: third_party/ducklake/test/sql/functions/ducklake_snapshots.test
    //
    // Storage contract:
    // - Schema/table/view/data operations create snapshots with increasing public sequence ids.
    // - Snapshot helpers must return schema-version and change summaries for those snapshots.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(101);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_create_schema_rows(&mut kv, catalog, vec![schema(1, "s1")]).unwrap();
    let create_schema = latest_snapshot(&kv, catalog).unwrap().unwrap();
    create_table(&mut kv, catalog, TableId(1), SchemaId(1), "tbl");
    let create_table = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, TableId(1), 0, 1)]).unwrap();
    let insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_tables(&mut kv, catalog, &[TableId(1)]).unwrap();
    let drop_table = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_schema_rows(&mut kv, catalog, &[SchemaId(1)]).unwrap();
    let drop_schema = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_create_view_row(&mut kv, catalog, view_row(TableId(100), "v1")).unwrap();
    let create_view = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_snapshots(&kv, catalog)
            .unwrap()
            .into_iter()
            .map(|snapshot| snapshot.sequence.0)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5, 6]
    );
    assert_eq!(
        snapshot_schema_version(&kv, catalog, create_schema.order).unwrap(),
        1
    );
    assert!(
        snapshot_changes_made(&kv, catalog, create_table.order)
            .unwrap()
            .contains("created_table:\"s1\".\"tbl\"")
    );
    assert!(
        snapshot_changes_made(&kv, catalog, insert.order)
            .unwrap()
            .contains("inserted_into_table:1")
    );
    assert!(
        snapshot_changes_made(&kv, catalog, drop_table.order)
            .unwrap()
            .contains("dropped_table:1")
    );
    assert!(
        snapshot_changes_made(&kv, catalog, drop_schema.order)
            .unwrap()
            .contains("dropped_schema:1")
    );
    assert!(
        snapshot_changes_made(&kv, catalog, create_view.order)
            .unwrap()
            .contains("created_view:\"main\".\"v1\"")
    );
}

#[test]
fn mirrors_issue_1074_test_mixed_inline_and_delete_file_snapshots_remain_time_travel_visible() {
    // Mirrors: third_party/ducklake/test/sql/issues/issue_1074.test
    //
    // Storage contract:
    // - A file can first receive an inline deletion, then replacement delete files in later
    //   snapshots.
    // - Historical reads at each snapshot attach only the delete metadata visible then.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(102);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1), "t");
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let v1 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        Vec::new(),
        &[],
        Vec::new(),
        vec![InlineFileDeletionRow::new(
            table,
            DataFileId(1),
            0,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let v2 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 2)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let v3 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(2, 1, 3)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let v4 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let append_public_sequence = public_snapshot_sequence_for_order(&kv, catalog, v1.order)
        .unwrap()
        .unwrap()
        .0;
    assert_public_snapshot_maps_to_order(&kv, catalog, append_public_sequence, v1.order);
    assert_public_snapshot_maps_to_order(&kv, catalog, append_public_sequence + 1, v2.order);
    assert_public_snapshot_maps_to_order(&kv, catalog, append_public_sequence + 2, v3.order);
    assert_public_snapshot_maps_to_order(&kv, catalog, append_public_sequence + 3, v4.order);
    assert_eq!(
        public_snapshot_sequence_for_order(&kv, catalog, v2.order).unwrap(),
        Some(ducklake_catalog::DuckLakeSnapshotId(
            append_public_sequence + 1
        ))
    );
    assert_eq!(
        public_snapshot_sequence_for_order(&kv, catalog, v3.order).unwrap(),
        Some(ducklake_catalog::DuckLakeSnapshotId(
            append_public_sequence + 2
        ))
    );
    assert!(
        list_data_files_with_deletes_at(&kv, catalog, table, v1.order).unwrap()[0]
            .delete_file
            .is_none()
    );
    assert!(
        list_data_files_with_deletes_at(&kv, catalog, table, v2.order).unwrap()[0]
            .delete_file
            .is_none()
    );
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, v2.order).unwrap(),
        std::collections::BTreeMap::from([(DataFileId(1), std::collections::BTreeSet::from([0]))])
    );
    assert_eq!(
        list_data_files_with_deletes_at(&kv, catalog, table, v3.order).unwrap()[0]
            .delete_file
            .as_ref()
            .unwrap()
            .record_count,
        2
    );
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, v3.order).unwrap(),
        std::collections::BTreeMap::new()
    );
    assert_eq!(
        list_data_files_with_deletes_at(&kv, catalog, table, v4.order).unwrap()[0]
            .delete_file
            .as_ref()
            .unwrap()
            .record_count,
        3
    );
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, v4.order).unwrap(),
        std::collections::BTreeMap::new()
    );
}

#[test]
fn mirrors_issue_865_update_wrong_result_test_file_listing_returns_physical_and_inline_deletes() {
    // Mirrors: third_party/ducklake/test/sql/issues/issue_865_update_wrong_result.test
    //
    // Storage contract:
    // - After one physical delete file hides ids >= 80 and a later inline file deletion hides ids
    //   75..79, a scan at the latest snapshot must receive both deletion sources for the same
    //   data file. Returning only the physical delete file makes DuckLake count 80 rows instead
    //   of the user-visible 75 rows.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(103);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1), "test");
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();

    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 20)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let physical_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        Vec::new(),
        &[],
        Vec::new(),
        (75..80)
            .map(|row_id| {
                InlineFileDeletionRow::new(table, DataFileId(1), row_id, CatalogOrderId::uuid_v7(0))
            })
            .collect(),
    )
    .unwrap();
    let inline_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let files = list_data_files_with_deletes_at(&kv, catalog, table, inline_delete.order).unwrap();
    assert_eq!(files.len(), 1);
    let attached = &files[0];
    assert_eq!(attached.data_file.data_file_id, DataFileId(1));
    assert_eq!(
        attached.delete_file.as_ref().map(|row| row.record_count),
        Some(20)
    );
    assert_eq!(
        attached
            .delete_file
            .as_ref()
            .map(|row| row.validity.begin_order),
        Some(physical_delete.order)
    );
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, inline_delete.order).unwrap(),
        std::collections::BTreeMap::from([(
            DataFileId(1),
            std::collections::BTreeSet::from([75, 76, 77, 78, 79])
        )])
    );
    let physical_delete_public_sequence =
        public_snapshot_sequence_for_order(&kv, catalog, physical_delete.order)
            .unwrap()
            .unwrap()
            .0;
    assert_public_snapshot_maps_to_order(
        &kv,
        catalog,
        physical_delete_public_sequence + 1,
        inline_delete.order,
    );
}

#[test]
fn mirrors_merge_timestamp_test_timestamp_partition_values_are_saved_for_inserted_file() {
    // Mirrors: third_party/ducklake/test/sql/merge/merge_timestamp.test
    //
    // Storage contract:
    // - A MERGE insert into a timestamp-partitioned table saves year/month partition values for
    //   the inserted data file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(104);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut row = table_row(table, SchemaId(1), "t");
    row.partition = Some(TablePartitionRow::new(
        1,
        vec![
            TablePartitionFieldRow::new(0, ColumnId(2), "year"),
            TablePartitionFieldRow::new(1, ColumnId(2), "month"),
        ],
    ));
    commit_create_table_row(&mut kv, catalog, row).unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1)],
        Vec::new(),
        &[],
        vec![
            FilePartitionValueRow::new(DataFileId(1), table, PartitionKeyIndex(0), "2026"),
            FilePartitionValueRow::new(DataFileId(1), table, PartitionKeyIndex(1), "6"),
        ],
        Vec::new(),
    )
    .unwrap();

    let partition_scan = list_current_data_files_for_partition_scan_with_deletes(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "2026",
    )
    .unwrap();
    assert_eq!(partition_scan.len(), 1);
    assert_eq!(partition_scan[0].data_file.data_file_id, DataFileId(1));
}

fn schema(id: u64, name: &str) -> SchemaRow {
    SchemaRow::new(
        SchemaId(id),
        format!("schema-{id}"),
        name,
        format!("main/{name}"),
        CatalogOrderId::uuid_v7(0),
    )
}

fn create_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    name: &str,
) -> TableRow {
    commit_create_table_row(kv, catalog, table_row(table, schema, name)).unwrap()
}

fn table_row(table: TableId, schema: SchemaId, name: &str) -> TableRow {
    TableRow::with_catalog_metadata(
        table,
        schema,
        format!("table-{}", table.0),
        name,
        format!("main/{name}"),
        vec![
            TableColumnRow::new(ColumnId(1), "id", "integer", true, None),
            TableColumnRow::new(ColumnId(2), "ts", "timestamptz", true, None),
        ],
        CatalogOrderId::uuid_v7(0),
    )
}

fn view_row(view: TableId, name: &str) -> ViewRow {
    ViewRow::new(
        view,
        SchemaId(0),
        format!("view-{}", view.0),
        name,
        "duckdb",
        "select 42",
        vec!["42".to_owned()],
        CatalogOrderId::uuid_v7(0),
    )
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

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> ducklake_catalog::DeleteFileRow {
    ducklake_catalog::DeleteFileRow::new(
        ducklake_catalog::DeleteFileId(id),
        DataFileId(data_file_id),
        format!("main/delete-{id}.parquet"),
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}

fn assert_public_snapshot_maps_to_order(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    public_sequence: u64,
    expected_order: CatalogOrderId,
) {
    let snapshot = snapshot_by_public_sequence(
        kv,
        catalog,
        ducklake_catalog::DuckLakeSnapshotId(public_sequence),
    )
    .unwrap()
    .unwrap_or_else(|| panic!("missing public snapshot {public_sequence}"));
    assert_eq!(snapshot.order, expected_order);
}
