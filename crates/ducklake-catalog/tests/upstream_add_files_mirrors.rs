use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    InlineTableFlush, MergeAdjacentCompaction, SchemaId, TableColumnRow, TableId, TableRow,
    commit_append_data_files, commit_append_table_columns, commit_create_table_row,
    commit_data_mutation_with_file_partitions, commit_drop_table_columns,
    commit_merge_adjacent_data_files, initialize_catalog_if_absent, insertion_files,
    latest_snapshot, list_data_files_at, list_old_data_files_for_cleanup,
    load_inline_table_payload_at, load_table_at, public_snapshot_sequence_for_order,
    register_inline_table_payload_with_table, remove_old_data_files, snapshot_by_public_sequence,
};

#[test]
fn mirrors_add_files_test_time_travel_after_added_file_and_schema_change() {
    // Mirrors: third_party/ducklake/test/sql/add_files/add_files.test
    //
    // Storage contract:
    // - DuckLake saves the initial flushed file, two externally added files, an ADD COLUMN, and a
    //   third externally added file.
    // - It later asks for the snapshot captured immediately after that third external add.
    // - Storage must return all four files and the schema that still contains col2 and col3, even
    //   after later DROP/ADD COLUMN work changes the current schema.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_create_table_row(&mut kv, catalog, add_files_table(table)).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1)]).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 1, 1)]).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, table, 2, 1)]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(3),
            "col3",
            "tinyint",
            false,
            None,
        )],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(4, table, 3, 1)]).unwrap();
    let v_file3 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_drop_table_columns(
        &mut kv,
        catalog,
        &[ducklake_catalog::ColumnDrop::new(table, ColumnId(2))],
    )
    .unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(4),
            "col2",
            "varchar",
            false,
            None,
        )],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(5, table, 4, 1)]).unwrap();

    let files_at_v_file3 = list_data_files_at(&kv, catalog, table, v_file3.order).unwrap();
    let table_at_v_file3 = load_table_at(&kv, catalog, table, v_file3.order)
        .unwrap()
        .unwrap();

    assert_eq!(
        files_at_v_file3
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(1), DataFileId(2), DataFileId(3), DataFileId(4)]
    );
    assert_eq!(
        table_at_v_file3
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["col1", "col2", "col3"]
    );
}

#[test]
fn mirrors_add_files_compaction_test_time_travel_cleanup_and_insertions() {
    // Mirrors: third_party/ducklake/test/sql/add_files/add_files_compaction.test
    //
    // Storage contract:
    // - DuckLake saves a user insert into `test`, an unrelated insert into `test2`, an inline
    //   flush for `test`, four manually added files, and then a merge-adjacent compaction.
    // - Cleanup may remove scheduled physical files, but must not remove metadata needed for
    //   time travel or insertion feeds.
    // - Public snapshot ids for s1/s_test2/s2/s3 must resolve back to storage snapshots that see
    //   the compacted replacement as the representation of the historical rows.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let test = TableId(1);
    let test2 = TableId(2);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_create_table_row(&mut kv, catalog, add_files_table(test)).unwrap();
    commit_create_table_row(&mut kv, catalog, add_files_table(test2)).unwrap();
    let mut test_row = add_files_table(test);
    test_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_1_0",
            0,
        ));
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        test_row,
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let s1 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_append_data_files(&mut kv, catalog, vec![data_file(100, test2, 0, 1)]).unwrap();
    let s_test2 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, test, 0, 1)],
        Vec::new(),
        &[InlineTableFlush::new(test, schema, s1.sequence)],
        Vec::new(),
    )
    .unwrap();
    for i in 2..=5 {
        commit_append_data_files(&mut kv, catalog, vec![data_file(i, test, i - 1, 1)]).unwrap();
    }
    let s2 = snapshot_by_nth_insert_public_id(&kv, catalog, test, 2);
    let s3 = snapshot_by_nth_insert_public_id(&kv, catalog, test, 3);
    let s_last_before_compaction = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: (1..=5).map(DataFileId).collect(),
            new_files: vec![data_file(6, test, 0, 5)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let cleanup = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(
        cleanup
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![
            DataFileId(1),
            DataFileId(2),
            DataFileId(3),
            DataFileId(4),
            DataFileId(5)
        ]
    );
    remove_old_data_files(
        &mut kv,
        catalog,
        &[
            DataFileId(1),
            DataFileId(2),
            DataFileId(3),
            DataFileId(4),
            DataFileId(5),
        ],
    )
    .unwrap();
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );

    for snapshot_order in [s1.order, s_test2.order] {
        let public_id = public_snapshot_sequence_for_order(&kv, catalog, snapshot_order)
            .unwrap()
            .unwrap();
        let resolved = snapshot_by_public_sequence(&kv, catalog, public_id)
            .unwrap()
            .unwrap();
        assert!(
            list_data_files_at(&kv, catalog, test, resolved.order)
                .unwrap()
                .is_empty(),
            "snapshot {public_id:?} is before flush and should not expose a data file"
        );
        assert_eq!(
            load_inline_table_payload_at(&kv, catalog, test, schema, resolved.order).unwrap(),
            Some(b"row\t0\ti:1\n".to_vec()),
            "snapshot {public_id:?} should still return the original inline row"
        );
    }

    for snapshot_order in [s2.order, s3.order, s_last_before_compaction.order] {
        let public_id = public_snapshot_sequence_for_order(&kv, catalog, snapshot_order)
            .unwrap()
            .unwrap();
        let resolved = snapshot_by_public_sequence(&kv, catalog, public_id)
            .unwrap()
            .unwrap();
        let files = list_data_files_at(&kv, catalog, test, resolved.order).unwrap();
        assert_eq!(
            files.len(),
            1,
            "snapshot {public_id:?} should use compacted replacement"
        );
        assert_eq!(files[0].data_file_id, DataFileId(6));
        assert_eq!(files[0].row_id_start, 0);
        assert_eq!(files[0].record_count, 5);
        assert_eq!(
            files[0].max_partial_order,
            Some(s_last_before_compaction.order)
        );
    }

    let feed_files = insertion_files(&kv, catalog, test, CatalogOrderId::uuid_v7(0), s3.order)
        .unwrap()
        .into_iter()
        .map(|file| file.data_file_id)
        .collect::<Vec<_>>();
    assert_eq!(feed_files, vec![DataFileId(6)]);
}

fn add_files_table(table: TableId) -> TableRow {
    let name = if table == TableId(1) { "test" } else { "test2" };
    TableRow::with_catalog_metadata(
        table,
        SchemaId(0),
        format!("{name}-uuid"),
        name,
        format!("main/{name}"),
        vec![
            TableColumnRow::new(ColumnId(1), "col1", "integer", true, None),
            TableColumnRow::new(ColumnId(2), "col2", "varchar", false, None),
        ],
        CatalogOrderId::uuid_v7(0),
    )
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/test/file-{id}.parquet"),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn snapshot_by_nth_insert_public_id(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    nth: usize,
) -> ducklake_catalog::SnapshotRow {
    let snapshots = ducklake_catalog::list_snapshots(kv, catalog).unwrap();
    let mut insert_snapshots = Vec::new();
    for snapshot in snapshots {
        if !insertion_files(kv, catalog, table, snapshot.order, snapshot.order)
            .unwrap()
            .is_empty()
        {
            insert_snapshots.push(snapshot);
        }
    }
    insert_snapshots[nth - 1].clone()
}
