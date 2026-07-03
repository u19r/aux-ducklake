use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnDrop, ColumnId, ColumnTypeChange, DataFileId, DataFileRow,
    DeleteFileId, DeleteFileRow, DuckLakeSnapshotId, FakeOrderedCatalogKv, FilePartitionValueRow,
    InlineFileDeletionRow, InlineRowChangeKind, InlineTableFlush, MergeAdjacentCompaction,
    PartitionKeyIndex, RewriteDeleteCompaction, SchemaId, TableColumnRow, TableId, TableRow,
    commit_append_data_files, commit_append_table_columns, commit_change_table_column_types,
    commit_create_table_row, commit_data_mutation_with_file_partitions,
    commit_data_mutation_with_file_partitions_and_inline_deletes,
    commit_delete_inline_table_rows_at_snapshot, commit_drop_table_columns,
    commit_inline_file_deletions, commit_merge_adjacent_data_files,
    commit_rewrite_delete_data_files, initialize_catalog_if_absent,
    list_current_data_files_for_partition_scan_with_deletes, list_current_data_files_with_deletes,
    list_data_files_at, list_inline_file_deletions_at, list_inline_row_changes,
    list_table_deletion_scan_files, load_inline_table_payload_at,
    register_inline_table_payload_with_table,
};

#[test]
fn mirrors_deletion_from_file_and_inserted_inlining_test_file_and_inline_row_delete_share_snapshot()
{
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_from_file_and_inserted_inlining.test
    //
    // Storage contract:
    // - DuckLake saves one physical data file for rows 0..9.
    // - It saves three newly inserted rows inline with row ids 10, 11, 12.
    // - One DELETE statement removes physical row id 5 and inline row id 11 in the same snapshot.
    // - Before flush, the physical delete is an inline file-deletion row and the inserted inline
    //   row is represented by an inline row deletion.
    // - Flush must turn those two deletes into delete-file metadata that begins at the DELETE
    //   snapshot, not at the later flush snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, schema);
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();

    let insert_payload = inline_payload(&[(10, 11), (11, 12), (12, 13)]);
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema),
        schema,
        insert_payload.clone(),
    )
    .unwrap();
    let inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let deleted = commit_file_and_inline_row_delete(
        &mut kv,
        catalog,
        table,
        schema,
        &[file_delete(table, 1, 5)],
        &[11],
    );

    assert_eq!(
        active_inline_file_deletes(&kv, catalog, table),
        vec![(DataFileId(1), 5, deleted.order)]
    );
    assert_deleted_inline_rows(&kv, catalog, table, schema, deleted.order, &[11]);
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, inserted.order).unwrap(),
        Some(insert_payload)
    );

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema,
        &inserted,
        vec![
            data_file(2, table, 10, 3),
            data_file(3, table, 13, 0).with_max_partial_order(Some(deleted.order)),
        ],
        vec![delete_file(1, 1, 1), delete_file(2, 2, 1)],
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(1), 1, deleted.order);
    assert_delete_attachment(&current, DataFileId(2), DeleteFileId(2), 1, deleted.order);
}

#[test]
fn mirrors_deletion_from_file_and_inserted_inlining_non_sequential_test_preserves_row_id_mapping() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_from_file_and_inserted_inlining_non_sequential.test
    //
    // Storage contract:
    // - Physical values are non-sequential, but row-id mapping is still positional.
    // - DELETE a=71 maps to physical row id 5 and DELETE a=500 maps to inline row id 11.
    // - Storage must save and return those row ids, independent of the SQL values.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, schema);
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();

    let insert_payload = inline_payload(&[(10, 999), (11, 500), (12, 777)]);
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema),
        schema,
        insert_payload,
    )
    .unwrap();
    let inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let deleted = commit_file_and_inline_row_delete(
        &mut kv,
        catalog,
        table,
        schema,
        &[file_delete(table, 1, 5)],
        &[11],
    );

    assert_eq!(
        active_inline_file_deletes(&kv, catalog, table),
        vec![(DataFileId(1), 5, deleted.order)]
    );
    assert_deleted_inline_rows(&kv, catalog, table, schema, deleted.order, &[11]);

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema,
        &inserted,
        vec![data_file(2, table, 10, 3)],
        vec![delete_file(1, 1, 1), delete_file(2, 2, 1)],
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(1), 1, deleted.order);
    assert_delete_attachment(&current, DataFileId(2), DeleteFileId(2), 1, deleted.order);
}

#[test]
fn mirrors_deletion_from_inlined_insertion_test_flush_delete_file_begins_at_inline_delete_snapshot()
{
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_from_inlined_insertion.test
    //
    // Storage contract:
    // - DuckLake saves three inserted rows inline.
    // - DELETE a=2 marks inline row id 1 deleted at snapshot 3.
    // - Time travel to the insertion snapshot must still return the original inline payload.
    // - Flush creates a physical data file plus a delete file whose begin snapshot remains the
    //   inline delete snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, schema);

    let insert_payload = inline_payload(&[(0, 1), (1, 2), (2, 3)]);
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema),
        schema,
        insert_payload.clone(),
    )
    .unwrap();
    let inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let deleted = commit_inline_row_delete(&mut kv, catalog, table, schema, &[1]);

    assert_deleted_inline_rows(&kv, catalog, table, schema, deleted.order, &[1]);
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, inserted.order).unwrap(),
        Some(insert_payload)
    );

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema,
        &inserted,
        vec![data_file(1, table, 0, 3)],
        vec![delete_file(2, 1, 1)],
    );

    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, inserted.order).unwrap(),
        Some(inline_payload(&[(0, 1), (1, 2), (2, 3)]))
    );
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(2), 1, deleted.order);
}

#[test]
fn mirrors_deletion_from_inlined_multiple_snapshots_test_flush_preserves_first_delete_snapshot() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_from_inlined_multiple_snapshots.test
    //
    // Storage contract:
    // - DuckLake saves eight inline rows.
    // - Four DELETE statements remove row ids 1/4, then 0/3, then 6, then 7.
    // - Flush writes one delete file for the physicalized inline data.
    // - That delete file contains all six deleted rows and begins at the first DELETE snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, schema);

    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema),
        schema,
        inline_payload(&[
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 1),
            (4, 2),
            (5, 3),
            (6, 5),
            (7, 6),
        ]),
    )
    .unwrap();
    let inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let delete_twos = commit_inline_row_delete(&mut kv, catalog, table, schema, &[1, 4]);
    let delete_ones = commit_inline_row_delete(&mut kv, catalog, table, schema, &[0, 3]);
    let delete_five = commit_inline_row_delete(&mut kv, catalog, table, schema, &[6]);
    let delete_six = commit_inline_row_delete(&mut kv, catalog, table, schema, &[7]);

    assert_deleted_inline_rows(&kv, catalog, table, schema, delete_twos.order, &[1, 4]);
    assert_deleted_inline_rows(&kv, catalog, table, schema, delete_ones.order, &[0, 3]);
    assert_deleted_inline_rows(&kv, catalog, table, schema, delete_five.order, &[6]);
    assert_deleted_inline_rows(&kv, catalog, table, schema, delete_six.order, &[7]);

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema,
        &inserted,
        vec![data_file(1, table, 0, 8)],
        vec![delete_file(2, 1, 6)],
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(2),
        6,
        delete_twos.order,
    );
}

#[test]
fn mirrors_deletion_inlining_test_repeated_inline_deletes_flush_and_later_cross_source_delete() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining.test
    //
    // Storage contract:
    // - Small physical-file deletes are accumulated as inline file deletions over snapshots.
    // - Flush materializes them as a cumulative delete file beginning at the first delete.
    // - Later inline deletes are added after the flush and the next flush replaces the delete file
    //   while preserving the first delete snapshot.
    // - A later DELETE spanning old physical data, newly inserted inline data, and a newer
    //   physical file must keep all three deletion sources separately attributable.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, schema);
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();

    let delete_lt_5 = commit_file_deletes(&mut kv, catalog, table, 1, 0..5);
    let delete_lt_9 = commit_file_deletes(&mut kv, catalog, table, 1, 5..9);
    let delete_15 = commit_file_deletes(&mut kv, catalog, table, 1, 15..16);

    assert_active_inline_file_deletes(
        &kv,
        catalog,
        table,
        &[
            (1, 0, delete_lt_5.order),
            (1, 1, delete_lt_5.order),
            (1, 2, delete_lt_5.order),
            (1, 3, delete_lt_5.order),
            (1, 4, delete_lt_5.order),
            (1, 5, delete_lt_9.order),
            (1, 6, delete_lt_9.order),
            (1, 7, delete_lt_9.order),
            (1, 8, delete_lt_9.order),
            (1, 15, delete_15.order),
        ],
    );

    flush_physical_delete_file(&mut kv, catalog, table, 1, 10);
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(1),
        10,
        delete_lt_5.order,
    );
    assert!(
        active_inline_file_deletes(&kv, catalog, table).is_empty(),
        "flush should remove active inline file deletions represented by the delete file"
    );

    let delete_lt_15 = commit_file_deletes(&mut kv, catalog, table, 1, 9..15);
    let delete_gt_45 = commit_file_deletes(&mut kv, catalog, table, 1, 46..50);
    assert_active_inline_file_deletes(
        &kv,
        catalog,
        table,
        &[
            (1, 9, delete_lt_15.order),
            (1, 10, delete_lt_15.order),
            (1, 11, delete_lt_15.order),
            (1, 12, delete_lt_15.order),
            (1, 13, delete_lt_15.order),
            (1, 14, delete_lt_15.order),
            (1, 46, delete_gt_45.order),
            (1, 47, delete_gt_45.order),
            (1, 48, delete_gt_45.order),
            (1, 49, delete_gt_45.order),
        ],
    );

    flush_physical_delete_file(&mut kv, catalog, table, 2, 20);
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(2),
        20,
        delete_lt_5.order,
    );

    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema),
        schema,
        inline_payload(&[(50, 51), (51, 52), (52, 53), (53, 54), (54, 55)]),
    )
    .unwrap();
    let inserted_inline = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(4, table, 56, 15)]).unwrap();
    let cross_source_delete = commit_file_and_inline_row_delete(
        &mut kv,
        catalog,
        table,
        schema,
        &[
            file_delete(table, 1, 40),
            file_delete(table, 4, 10),
            file_delete(table, 4, 11),
            file_delete(table, 4, 12),
            file_delete(table, 4, 13),
            file_delete(table, 4, 14),
        ],
        &[52],
    );

    assert_deleted_inline_rows(
        &kv,
        catalog,
        table,
        schema,
        cross_source_delete.order,
        &[52],
    );
    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema,
        &inserted_inline,
        vec![data_file(5, table, 50, 5)],
        vec![
            delete_file(3, 1, 21),
            delete_file(4, 5, 1),
            delete_file(5, 4, 5),
        ],
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(3),
        21,
        delete_lt_5.order,
    );
    assert_delete_attachment(
        &current,
        DataFileId(5),
        DeleteFileId(4),
        1,
        cross_source_delete.order,
    );
    assert_delete_attachment(
        &current,
        DataFileId(4),
        DeleteFileId(5),
        5,
        cross_source_delete.order,
    );
}

#[test]
fn mirrors_deletion_inlining_alter_test_schema_changes_do_not_lose_pending_inline_deletes() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_alter.test
    //
    // Storage contract:
    // - Inline file deletions created before ADD/DROP/ALTER COLUMN remain attached to the same
    //   data file.
    // - Inline row deletes for short-lived inserted inline payloads are flushed into delete files
    //   even when the table schema changes before and after those inserts.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema_v1 = SchemaId(1);
    let schema_v2 = SchemaId(2);
    let schema_v3 = SchemaId(3);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_two_column_table(&mut kv, catalog, table, schema_v1);
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();

    let first_delete = commit_file_deletes(&mut kv, catalog, table, 1, 0..5);
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(ColumnId(3), "k", "integer", true, None)],
    )
    .unwrap();
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema_v2),
        schema_v2,
        inline_payload(&[(50, 100)]),
    )
    .unwrap();
    let inserted_k = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    let delete_k = commit_inline_row_delete(&mut kv, catalog, table, schema_v2, &[50]);

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema_v2,
        &inserted_k,
        vec![data_file(2, table, 50, 1)],
        vec![delete_file(1, 1, 5), delete_file(2, 2, 1)],
    );
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(1),
        5,
        first_delete.order,
    );
    assert_delete_attachment(&current, DataFileId(2), DeleteFileId(2), 1, delete_k.order);

    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(3))]).unwrap();
    commit_file_deletes(&mut kv, catalog, table, 1, 45..50);
    commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(
            table,
            TableColumnRow::new(ColumnId(2), "j", "bigint", true, None),
        )],
    )
    .unwrap();
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(table, schema_v3),
        schema_v3,
        inline_payload(&[(51, 1000)]),
    )
    .unwrap();
    let inserted_bigint = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    let delete_bigint = commit_inline_row_delete(&mut kv, catalog, table, schema_v3, &[51]);

    flush_inline_data_with_delete_files(
        &mut kv,
        catalog,
        table,
        schema_v3,
        &inserted_bigint,
        vec![data_file(3, table, 51, 1)],
        vec![delete_file(3, 1, 10), delete_file(4, 3, 1)],
    );
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(3),
        10,
        first_delete.order,
    );
    assert_delete_attachment(
        &current,
        DataFileId(3),
        DeleteFileId(4),
        1,
        delete_bigint.order,
    );
}

#[test]
fn mirrors_deletion_inlining_compaction_test_merge_rewrite_and_time_travel_metadata() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_compaction.test
    //
    // Storage contract:
    // - A merge-adjacent compaction must not merge a file with active inline deletions.
    // - After the inline delete is flushed, rewrite-delete compaction replaces the source file but
    //   old snapshots still resolve to their original data-file visibility.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();
    let original = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    let delete_25 = commit_file_deletes(&mut kv, catalog, table, 1, 25..26);
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 51, 3)]).unwrap();

    let rejected = commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![data_file(3, table, 0, 52)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(
        rejected.is_err(),
        "merge-adjacent should not compact a source file with active inline deletions"
    );

    flush_physical_delete_file(&mut kv, catalog, table, 1, 1);
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(4, table, 0, 49)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let at_original = list_data_files_at(&kv, catalog, table, original.order).unwrap();
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(
        at_original
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(1)]
    );
    assert!(
        current
            .iter()
            .any(|file| file.data_file.data_file_id == DataFileId(4))
    );
    assert_eq!(
        active_inline_file_deletes(&kv, catalog, table),
        Vec::<(DataFileId, u64, CatalogOrderId)>::new()
    );
    assert_eq!(
        list_table_deletion_scan_files(&kv, catalog, table, original.order, delete_25.order)
            .unwrap()[0]
            .snapshot_order,
        delete_25.order
    );
}

#[test]
fn mirrors_deletion_inlining_large_test_many_inline_deletes_flush_to_one_cumulative_delete_file() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_large.test
    //
    // Storage contract:
    // - A large number of inline file-deletion rows across several snapshots remains queryable
    //   until flush.
    // - Flush replaces them with one cumulative delete file beginning at the first delete.
    // - A later large delete replaces that delete file while keeping the original begin snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10_000)]).unwrap();

    let d1 = commit_file_deletes_from_slice(&mut kv, catalog, table, 1, &[100, 500, 999]);
    commit_file_deletes_from_slice(&mut kv, catalog, table, 1, &[50, 2500, 5000, 7500]);
    commit_file_deletes(&mut kv, catalog, table, 1, 1000..1020);
    commit_file_deletes_from_slice(
        &mut kv,
        catalog,
        table,
        1,
        &(0..2400)
            .filter(|value| value % 100 == 77)
            .collect::<Vec<_>>(),
    );
    commit_file_deletes(&mut kv, catalog, table, 1, 3000..5900);

    assert_eq!(active_inline_file_deletes(&kv, catalog, table).len(), 2950);
    flush_physical_delete_file(&mut kv, catalog, table, 1, 2950);
    assert!(active_inline_file_deletes(&kv, catalog, table).is_empty());
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(1), 2950, d1.order);

    commit_file_deletes(&mut kv, catalog, table, 1, 9000..10_000);
    flush_physical_delete_file(&mut kv, catalog, table, 2, 3950);
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(2), 3950, d1.order);
}

#[test]
fn mirrors_deletion_inlining_partitions_test_inline_deletes_stay_partition_scoped_after_flush() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_partitions.test
    //
    // Storage contract:
    // - Three partition files receive small inline deletions in separate snapshots.
    // - Partition scans should return the same partitioned files with attached delete metadata
    //   after flush.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 50),
            data_file(2, table, 100, 50),
            data_file(3, table, 200, 50),
        ],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "2020"),
            partition_value(1, table, 1, "01"),
            partition_value(2, table, 0, "2020"),
            partition_value(2, table, 1, "02"),
            partition_value(3, table, 0, "2021"),
            partition_value(3, table, 1, "01"),
        ],
    )
    .unwrap();

    let d1 = commit_file_deletes(&mut kv, catalog, table, 1, 0..3);
    let d2 = commit_file_deletes(&mut kv, catalog, table, 2, 0..3);
    let d3 = commit_file_deletes(&mut kv, catalog, table, 3, 0..3);
    assert_eq!(active_inline_file_deletes(&kv, catalog, table).len(), 9);

    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![
            delete_file(1, 1, 3),
            delete_file(2, 2, 3),
            delete_file(3, 3, 3),
        ],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();

    let partition_2020 = list_current_data_files_for_partition_scan_with_deletes(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "2020",
    )
    .unwrap();
    assert_eq!(partition_2020.len(), 2);
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(&current, DataFileId(1), DeleteFileId(1), 3, d1.order);
    assert_delete_attachment(&current, DataFileId(2), DeleteFileId(2), 3, d2.order);
    assert_delete_attachment(&current, DataFileId(3), DeleteFileId(3), 3, d3.order);
    assert!(active_inline_file_deletes(&kv, catalog, table).is_empty());
}

#[test]
fn mirrors_deletion_inlining_stats_test_repeated_flush_keeps_delete_count_cumulative() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_stats.test
    //
    // Storage contract:
    // - The first flush writes a delete file for rows 0..4.
    // - A later inline delete for rows 45..49 replaces it with a cumulative delete file.
    // - The replacement keeps the first delete snapshot and reports the cumulative delete count.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();

    let first_delete = commit_file_deletes(&mut kv, catalog, table, 1, 0..5);
    flush_physical_delete_file(&mut kv, catalog, table, 1, 5);
    assert!(active_inline_file_deletes(&kv, catalog, table).is_empty());

    commit_file_deletes(&mut kv, catalog, table, 1, 45..50);
    flush_physical_delete_file(&mut kv, catalog, table, 2, 10);

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(2),
        10,
        first_delete.order,
    );
}

#[test]
fn mirrors_deletion_inlining_table_deletes_test_change_feed_returns_inline_deletes_by_snapshot() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_table_deletes.test
    //
    // Storage contract:
    // - Inline file deletions are the source of `ducklake_table_deletions`.
    // - Range scans over the deletion feed must return the file row ids grouped under the snapshot
    //   where each DELETE statement committed.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 20), data_file(2, table, 100, 15)],
    )
    .unwrap();
    let before_deletes = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let d1 = commit_file_deletes(&mut kv, catalog, table, 1, 0..3);
    let d2 = commit_file_deletes(&mut kv, catalog, table, 2, 0..2);
    let d3 = commit_inline_file_delete_rows(
        &mut kv,
        catalog,
        vec![
            file_delete(table, 1, 5),
            file_delete(table, 1, 10),
            file_delete(table, 2, 5),
        ],
    );

    assert!(
        list_table_deletion_scan_files(
            &kv,
            catalog,
            table,
            before_deletes.order,
            before_deletes.order
        )
        .unwrap()
        .is_empty()
    );
    assert_scan_inline_rows(
        &kv,
        catalog,
        table,
        d1.order,
        d1.order,
        &[(1, 0), (1, 1), (1, 2)],
    );
    assert_scan_inline_rows(&kv, catalog, table, d2.order, d2.order, &[(2, 0), (2, 1)]);
    assert_scan_inline_rows(
        &kv,
        catalog,
        table,
        d3.order,
        d3.order,
        &[(1, 5), (1, 10), (2, 5)],
    );
    assert_scan_inline_rows(
        &kv,
        catalog,
        table,
        d1.order,
        d3.order,
        &[
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (1, 5),
            (1, 10),
            (2, 5),
        ],
    );
}

#[test]
fn mirrors_deletion_inlining_transaction_test_committed_multi_delete_uses_one_begin_snapshot() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_inlining_transaction.test
    //
    // Storage contract:
    // - Rolled-back SQL transactions do not become storage writes.
    // - The committed transaction with three DELETE statements asks storage to save row ids 0..14
    //   as one committed metadata version.
    // - Flush removes those active inline deletes and materializes one delete file beginning at
    //   the committed transaction snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, table, SchemaId(1));
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();

    let committed_delete = commit_file_deletes(&mut kv, catalog, table, 1, 0..15);
    assert_active_inline_file_deletes(
        &kv,
        catalog,
        table,
        &(0..15)
            .map(|row_id| (1, row_id, committed_delete.order))
            .collect::<Vec<_>>(),
    );

    flush_physical_delete_file(&mut kv, catalog, table, 1, 15);
    assert!(active_inline_file_deletes(&kv, catalog, table).is_empty());
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_delete_attachment(
        &current,
        DataFileId(1),
        DeleteFileId(1),
        15,
        committed_delete.order,
    );
}

#[test]
fn mirrors_deletion_multiple_tables_test_flush_keeps_table_delete_snapshots_isolated() {
    // Mirrors: third_party/ducklake/test/sql/deletion_inlining/test_deletion_multiple_tables.test
    //
    // Storage contract:
    // - Two tables each have inline inserts and multiple inline deletes.
    // - Flush writes one delete file per table.
    // - Each table's delete file begins at that table's first delete snapshot, not at the other
    //   table's delete snapshot and not at the flush snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let t1 = TableId(1);
    let t2 = TableId(2);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(&mut kv, catalog, t1, schema);
    create_table(&mut kv, catalog, t2, schema);

    let payload = inline_payload(&[
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 1),
        (4, 2),
        (5, 3),
        (6, 5),
        (7, 6),
    ]);
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(t1, schema),
        schema,
        payload.clone(),
    )
    .unwrap();
    let t1_inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(t2, schema),
        schema,
        payload,
    )
    .unwrap();
    let t2_inserted = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let t1_first_delete = commit_inline_row_delete(&mut kv, catalog, t1, schema, &[1, 4]);
    commit_inline_row_delete(&mut kv, catalog, t1, schema, &[0, 3]);
    commit_inline_row_delete(&mut kv, catalog, t1, schema, &[6]);
    let t2_first_delete = commit_inline_row_delete(&mut kv, catalog, t2, schema, &[1, 4]);
    commit_inline_row_delete(&mut kv, catalog, t2, schema, &[0, 3]);
    commit_inline_row_delete(&mut kv, catalog, t2, schema, &[6]);

    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(1, t1, 0, 8), data_file(2, t2, 0, 8)],
        vec![delete_file(1, 1, 5), delete_file(2, 2, 5)],
        &[
            InlineTableFlush::new(t1, schema, t1_inserted.sequence),
            InlineTableFlush::new(t2, schema, t2_inserted.sequence),
        ],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();

    let t1_current = list_current_data_files_with_deletes(&kv, catalog, t1).unwrap();
    let t2_current = list_current_data_files_with_deletes(&kv, catalog, t2).unwrap();
    assert_delete_attachment(
        &t1_current,
        DataFileId(1),
        DeleteFileId(1),
        5,
        t1_first_delete.order,
    );
    assert_delete_attachment(
        &t2_current,
        DataFileId(2),
        DeleteFileId(2),
        5,
        t2_first_delete.order,
    );
}

fn create_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
) -> TableRow {
    commit_create_table_row(kv, catalog, base_table(table, schema)).unwrap()
}

fn create_two_column_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
) -> TableRow {
    let name = table_name(table);
    commit_create_table_row(
        kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            schema,
            format!("table-{}", table.0),
            &name,
            format!("main/{name}"),
            vec![
                TableColumnRow::new(ColumnId(1), "i", "integer", true, None),
                TableColumnRow::new(ColumnId(2), "j", "integer", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap()
}

fn base_table(table: TableId, schema: SchemaId) -> TableRow {
    let name = table_name(table);
    TableRow::with_catalog_metadata(
        table,
        schema,
        format!("table-{}", table.0),
        &name,
        format!("main/{name}"),
        vec![TableColumnRow::new(ColumnId(1), "a", "integer", true, None)],
        CatalogOrderId::uuid_v7(0),
    )
}

fn table_name(table: TableId) -> String {
    if table == TableId(1) {
        "t".to_owned()
    } else {
        format!("t{}", table.0)
    }
}

fn table_with_inline(table: TableId, schema: SchemaId) -> TableRow {
    let mut row = base_table(table, schema);
    row.inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            format!("ducklake_inlined_data_{}_{}", table.0, schema.0),
            schema.0,
        ));
    row
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/t/file-{id}.parquet"),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> DeleteFileRow {
    DeleteFileRow::new(
        DeleteFileId(id),
        DataFileId(data_file_id),
        format!("main/t/delete-{id}.parquet"),
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}

fn file_delete(table: TableId, data_file_id: u64, row_id: u64) -> InlineFileDeletionRow {
    InlineFileDeletionRow::new(
        table,
        DataFileId(data_file_id),
        row_id,
        CatalogOrderId::uuid_v7(0),
    )
}

fn inline_payload(rows: &[(u64, i64)]) -> Vec<u8> {
    rows.iter()
        .map(|(row_id, value)| format!("row\t{row_id}\ta:{value}\n"))
        .collect::<Vec<_>>()
        .join("")
        .into_bytes()
}

fn commit_file_and_inline_row_delete(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    file_deletions: &[InlineFileDeletionRow],
    inline_row_ids: &[u64],
) -> ducklake_catalog::SnapshotRow {
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        kv,
        catalog,
        Vec::new(),
        Vec::new(),
        &[],
        Vec::new(),
        file_deletions.to_vec(),
    )
    .unwrap();
    let deleted = ducklake_catalog::latest_snapshot(kv, catalog)
        .unwrap()
        .unwrap();
    commit_delete_inline_table_rows_at_snapshot(
        kv,
        catalog,
        table,
        schema,
        inline_row_ids,
        Some(DuckLakeSnapshotId(deleted.sequence.0)),
    )
    .unwrap();
    deleted
}

fn commit_inline_row_delete(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    inline_row_ids: &[u64],
) -> ducklake_catalog::SnapshotRow {
    commit_delete_inline_table_rows_at_snapshot(kv, catalog, table, schema, inline_row_ids, None)
        .unwrap();
    ducklake_catalog::latest_snapshot(kv, catalog)
        .unwrap()
        .unwrap()
}

fn commit_file_deletes(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    data_file_id: u64,
    row_ids: impl IntoIterator<Item = u64>,
) -> ducklake_catalog::SnapshotRow {
    commit_file_deletes_from_slice(
        kv,
        catalog,
        table,
        data_file_id,
        &row_ids.into_iter().collect::<Vec<_>>(),
    )
}

fn commit_file_deletes_from_slice(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    data_file_id: u64,
    row_ids: &[u64],
) -> ducklake_catalog::SnapshotRow {
    commit_inline_file_deletions(
        kv,
        catalog,
        row_ids
            .iter()
            .map(|row_id| file_delete(table, data_file_id, *row_id))
            .collect(),
    )
    .unwrap();
    ducklake_catalog::latest_snapshot(kv, catalog)
        .unwrap()
        .unwrap()
}

fn commit_inline_file_delete_rows(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    rows: Vec<InlineFileDeletionRow>,
) -> ducklake_catalog::SnapshotRow {
    commit_inline_file_deletions(kv, catalog, rows).unwrap();
    ducklake_catalog::latest_snapshot(kv, catalog)
        .unwrap()
        .unwrap()
}

fn flush_physical_delete_file(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    _table: TableId,
    delete_file_id: u64,
    record_count: u64,
) {
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        kv,
        catalog,
        Vec::new(),
        vec![delete_file(delete_file_id, 1, record_count)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
}

fn flush_inline_data_with_delete_files(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    inserted: &ducklake_catalog::SnapshotRow,
    data_files: Vec<DataFileRow>,
    delete_files: Vec<DeleteFileRow>,
) {
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        kv,
        catalog,
        data_files,
        delete_files,
        &[InlineTableFlush::new(table, schema, inserted.sequence)],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
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

fn active_inline_file_deletes(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
) -> Vec<(DataFileId, u64, CatalogOrderId)> {
    list_inline_file_deletions_at(
        kv,
        catalog,
        table,
        ducklake_catalog::latest_snapshot(kv, catalog)
            .unwrap()
            .unwrap()
            .order,
    )
    .unwrap()
    .into_iter()
    .flat_map(|(file_id, row_ids)| row_ids.into_iter().map(move |row_id| (file_id, row_id)))
    .map(|(file_id, row_id)| {
        let row = list_inline_row_file_delete(kv, catalog, table, file_id, row_id);
        (file_id, row_id, row.validity.begin_order)
    })
    .collect()
}

fn assert_active_inline_file_deletes(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    expected: &[(u64, u64, CatalogOrderId)],
) {
    let expected = expected
        .iter()
        .map(|(data_file_id, row_id, order)| (DataFileId(*data_file_id), *row_id, *order))
        .collect::<Vec<_>>();
    assert_eq!(active_inline_file_deletes(kv, catalog, table), expected);
}

fn list_inline_row_file_delete(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    data_file: DataFileId,
    row_id: u64,
) -> InlineFileDeletionRow {
    ducklake_catalog::list_inline_file_deletions_between(
        kv,
        catalog,
        table,
        CatalogOrderId::uuid_v7(0),
        ducklake_catalog::latest_snapshot(kv, catalog)
            .unwrap()
            .unwrap()
            .order,
    )
    .unwrap()
    .into_iter()
    .find(|row| row.data_file_id == data_file && row.row_id == row_id)
    .unwrap()
}

fn assert_deleted_inline_rows(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    order: CatalogOrderId,
    expected_row_ids: &[u64],
) {
    let rows = list_inline_row_changes(kv, catalog, table, order, order)
        .unwrap()
        .into_iter()
        .filter(|row| row.schema_id == schema && row.kind == InlineRowChangeKind::Deleted)
        .map(|row| row.row_id)
        .collect::<Vec<_>>();
    assert_eq!(rows, expected_row_ids);
}

fn assert_scan_inline_rows(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    start: CatalogOrderId,
    end: CatalogOrderId,
    expected: &[(u64, u64)],
) {
    let rows = list_table_deletion_scan_files(kv, catalog, table, start, end)
        .unwrap()
        .into_iter()
        .flat_map(|scan| {
            let data_file_id = scan.data_file.data_file_id.0;
            scan.inline_file_deletions
                .into_keys()
                .map(move |row_id| (data_file_id, row_id))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}

fn assert_delete_attachment(
    current: &[ducklake_catalog::AttachedDataFile],
    data_file_id: DataFileId,
    delete_file_id: DeleteFileId,
    record_count: u64,
    begin_order: CatalogOrderId,
) {
    let attached = current
        .iter()
        .find(|file| file.data_file.data_file_id == data_file_id)
        .unwrap_or_else(|| panic!("missing current data file {data_file_id:?}"));
    let delete = attached
        .delete_file
        .as_ref()
        .unwrap_or_else(|| panic!("missing delete file for {data_file_id:?}"));
    assert_eq!(delete.delete_file_id, delete_file_id);
    assert_eq!(delete.record_count, record_count);
    assert_eq!(delete.validity.begin_order, begin_order);
}
