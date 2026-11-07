use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FakeOrderedCatalogKv, FilePartitionValueRow, InlineFileDeletionRow, MergeAdjacentCompaction,
    PartitionKeyIndex, RewriteDeleteCompaction, SchemaId, TableColumnRow, TableId, TableRow,
    commit_append_data_files, commit_create_table_row,
    commit_data_mutation_with_file_partitions_and_inline_deletes, commit_inline_file_deletions,
    commit_merge_adjacent_data_files, commit_register_delete_files,
    commit_rewrite_delete_data_files, commit_rewrite_delete_data_files_with_conflict_check,
    expire_snapshots, initialize_catalog_if_absent, latest_snapshot,
    list_current_data_files_with_deletes, list_data_files_with_deletes_at,
    list_file_partition_values, list_old_data_files_for_cleanup, list_old_delete_files_for_cleanup,
    list_snapshots, public_snapshot_sequence_for_order, snapshot_changes_made,
};

#[test]
fn mirrors_insert_delete_loop_test_each_rewrite_replaces_deleted_file_with_live_rows() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/insert_delete_loop.test
    //
    // Storage contract:
    // - Every loop appends one file, saves a delete file against it, and rewrites that source file.
    // - Storage must end the source and delete file, keep one replacement per loop current, and
    //   never leak a current delete file after rewrite.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "tbl",
        vec![column(1, "i", "integer")],
    );

    for i in 1..=10 {
        let source = i;
        let replacement = 100 + i;
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(source, table, (i - 1) * 1000, 1000)],
        )
        .unwrap();
        commit_register_delete_files(&mut kv, catalog, vec![delete_file(i, source, 951)]).unwrap();
        commit_rewrite_delete_data_files(
            &mut kv,
            catalog,
            RewriteDeleteCompaction {
                source_file_ids: vec![DataFileId(source)],
                new_files: vec![data_file(replacement, table, (i - 1) * 1000 + 951, 49)],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
        assert_eq!(current.len(), i as usize);
        assert_eq!(current_row_count(&kv, catalog, table), i * 49);
        assert!(
            current.iter().all(|file| file.delete_file.is_none()),
            "rewrite should apply the delete and leave no current delete file"
        );
    }
}

#[test]
fn mirrors_last_snapshot_multiple_inserts_test_rewrite_keeps_historical_visibility_windows() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/last_snapshot_multiple_inserts.test
    //
    // Storage contract:
    // - Interleaved inserts and deletes produce several historical visibility windows.
    // - Rewrite-delete compaction replaces only the deleted source files in the current view.
    // - Reads at snapshots captured before the rewrite still resolve through the original rows.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "key", "integer"), column(2, "values", "varchar")],
    );

    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let after_first_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 50)]).unwrap();
    let after_first_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 100, 100)]).unwrap();
    let after_second_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(2, 1, 80)]).unwrap();
    let after_second_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(3, 2, 1)]).unwrap();
    let after_third_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, table, 200, 100)]).unwrap();
    let after_third_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(4, 3, 49)]).unwrap();
    let after_last_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();

    rewrite(
        &mut kv,
        catalog,
        &[1, 2, 3],
        &[data_file(10, table, 80, 120), data_file(11, table, 250, 50)],
    );

    assert_visible_count_at(&kv, catalog, table, after_first_insert.order, 100);
    assert_visible_count_at(&kv, catalog, table, after_first_delete.order, 50);
    assert_visible_count_at(&kv, catalog, table, after_second_insert.order, 150);
    assert_visible_count_at(&kv, catalog, table, after_second_delete.order, 120);
    assert_visible_count_at(&kv, catalog, table, after_third_delete.order, 119);
    assert_visible_count_at(&kv, catalog, table, after_third_insert.order, 219);
    assert_visible_count_at(&kv, catalog, table, after_last_delete.order, 170);
    assert_eq!(current_row_count(&kv, catalog, table), 170);
}

#[test]
fn mirrors_rewrite_deletion_vectors_test_rewrite_closes_multi_snapshot_delete_file() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/rewrite_deletion_vectors.test
    //
    // Storage contract:
    // - Replacement delete files may preserve the first delete begin snapshot while accumulating
    //   later deletes.
    // - Rewrite-delete compaction must end the active delete file and source data file at the
    //   rewrite snapshot, while preserving time-travel visibility before that rewrite.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1000)]).unwrap();
    let create = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let first_delete = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(
            1,
            1,
            "main/test/delete-1.puffin",
            100,
        )],
    )
    .unwrap();
    let first_delete_order = first_delete[0].validity.begin_order;
    let d1 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(
            2,
            1,
            "main/test/delete-2.puffin",
            200,
        )],
    )
    .unwrap();
    let d2 = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let current_delete = current_delete_file(&kv, catalog, table, 1);
    assert_eq!(current_delete.delete_file_id, DeleteFileId(2));
    assert_eq!(current_delete.validity.begin_order, first_delete_order);
    assert_eq!(visible_count_at(&kv, catalog, table, create.order), 1000);
    assert_eq!(visible_count_at(&kv, catalog, table, d1.order), 900);
    assert_eq!(visible_count_at(&kv, catalog, table, d2.order), 800);

    rewrite(&mut kv, catalog, &[1], &[data_file(3, table, 200, 800)]);
    assert_eq!(current_row_count(&kv, catalog, table), 800);
    assert!(
        list_current_data_files_with_deletes(&kv, catalog, table).unwrap()[0]
            .delete_file
            .is_none()
    );
    assert_eq!(visible_count_at(&kv, catalog, table, d2.order), 800);
}

#[test]
fn mirrors_last_snapshot_merge_rewrite_test_merge_and_rewrite_orders_both_preserve_history() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_last_snapshot_merge_rewrite.test
    //
    // Storage contract:
    // - DuckLake may rewrite deletes before merge-adjacent compaction, or merge unrelated files
    //   before rewriting deleted files.
    // - Both paths should leave the same current row count and preserve the captured historical
    //   visibility windows.
    for rewrite_first in [true, false] {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        create_table(
            &mut kv,
            catalog,
            table,
            "test",
            vec![column(1, "key", "integer")],
        );
        build_last_snapshot_rewrite_shape(&mut kv, catalog, table);
        let after_insert = nth_snapshot_order(&kv, catalog, 2);
        let after_first_delete = nth_snapshot_order(&kv, catalog, 3);
        let after_second_insert = nth_snapshot_order(&kv, catalog, 4);
        let after_second_delete = nth_snapshot_order(&kv, catalog, 5);

        if rewrite_first {
            rewrite(
                &mut kv,
                catalog,
                &[1, 2, 3],
                &[data_file(10, table, 80, 120), data_file(11, table, 250, 50)],
            );
            merge(
                &mut kv,
                catalog,
                &[10, 11],
                data_file(12, table, 80, 170),
                Vec::new(),
            );
        } else {
            let rejected = commit_merge_adjacent_data_files(
                &mut kv,
                catalog,
                MergeAdjacentCompaction {
                    source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3)],
                    new_files: vec![data_file(10, table, 80, 170)],
                    partition_values: Vec::new(),
                    file_column_stats: Vec::new(),
                },
            );
            assert!(
                rejected.is_err(),
                "merge-adjacent should not compact files that still have active delete files"
            );
            rewrite(
                &mut kv,
                catalog,
                &[1, 2, 3],
                &[data_file(11, table, 80, 170)],
            );
        }

        assert_eq!(current_row_count(&kv, catalog, table), 170);
        assert_visible_count_at(&kv, catalog, table, after_insert, 100);
        assert_visible_count_at(&kv, catalog, table, after_first_delete, 50);
        assert_visible_count_at(&kv, catalog, table, after_second_insert, 150);
        assert_visible_count_at(&kv, catalog, table, after_second_delete, 120);
    }
}

#[test]
fn mirrors_last_snapshot_rewrite_test_multiple_rewrites_close_old_metadata_but_keep_snapshots() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_last_snapshot_rewrite.test
    //
    // Storage contract:
    // - Repeated rewrite-delete commits should end exactly the source data/delete file metadata
    //   for that rewrite.
    // - Earlier snapshots still see the data files and delete-file windows that existed then.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "key", "integer"), column(2, "values", "varchar")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10_000)]).unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 1000)]).unwrap();
    let after_delete_1 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(2, 1, 1999)]).unwrap();
    let after_delete_2 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(3, 1, 5999)]).unwrap();
    let after_delete_3 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    rewrite(&mut kv, catalog, &[1], &[data_file(4, table, 1000, 4001)]);
    let first_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_register_delete_files(&mut kv, catalog, vec![delete_file(5, 4, 1000)]).unwrap();
    let after_delete_4 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    rewrite(&mut kv, catalog, &[4], &[data_file(6, table, 1000, 3001)]);
    let second_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_register_delete_files(&mut kv, catalog, vec![delete_file(7, 6, 1)]).unwrap();
    let after_delete_5 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    rewrite(&mut kv, catalog, &[6], &[data_file(8, table, 1000, 3000)]);

    assert_visible_count_at(&kv, catalog, table, after_delete_1.order, 9000);
    assert_visible_count_at(&kv, catalog, table, after_delete_2.order, 8001);
    assert_visible_count_at(&kv, catalog, table, after_delete_3.order, 4001);
    assert_visible_count_at(&kv, catalog, table, first_rewrite.order, 4001);
    assert_visible_count_at(&kv, catalog, table, after_delete_4.order, 3001);
    assert_visible_count_at(&kv, catalog, table, second_rewrite.order, 3001);
    assert_visible_count_at(&kv, catalog, table, after_delete_5.order, 3000);
    assert_eq!(current_row_count(&kv, catalog, table), 3000);
    // Each delete file has been superseded by a later cumulative delete or by a rewrite of
    // its source file, so all five are eligible once the replacement data files are scheduled.
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .len(),
        5
    );
    expire_all_but_current_snapshot(&mut kv, catalog);
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .len(),
        5
    );
}

#[test]
fn mirrors_rewrite_concurrency_test_stale_parallel_rewrite_is_rejected() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_concurrency.test
    //
    // Storage contract:
    // - Two concurrent rewrites planned from the same base snapshot target the same deleted file.
    // - The first commit succeeds; the second must see the rewrite/delete conflict and fail.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "key", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 97)]).unwrap();
    latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(2, table, 39, 3)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let after_first_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let stale = commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        base.order,
        after_first_rewrite.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(3, table, 39, 3)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );

    assert!(stale.is_err());
    assert_eq!(current_row_count(&kv, catalog, table), 3);
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog).unwrap().len(),
        1
    );
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .len(),
        1
    );
    expire_all_but_current_snapshot(&mut kv, catalog);
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog).unwrap().len(),
        1
    );
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn mirrors_rewrite_db_test_database_wide_rewrite_keeps_table_metadata_isolated() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_db.test
    //
    // Storage contract:
    // - A database-wide rewrite is a sequence of per-table rewrite commits.
    // - Rewriting one table must not close or mutate the other table's current data files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table_a = TableId(1);
    let table_b = TableId(2);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table_a,
        "test_0",
        vec![column(1, "key", "integer")],
    );
    create_table(
        &mut kv,
        catalog,
        table_b,
        "test_1",
        vec![column(1, "key", "integer")],
    );
    build_two_file_delete_shape(&mut kv, catalog, table_a, 1, 10, 20);
    build_two_file_delete_shape(&mut kv, catalog, table_b, 101, 110, 120);

    rewrite(&mut kv, catalog, &[1, 2], &[data_file(3, table_a, 80, 119)]);
    assert_eq!(current_file_ids(&kv, catalog, table_a), vec![3]);
    assert_eq!(current_file_ids(&kv, catalog, table_b), vec![101, 102]);

    rewrite(
        &mut kv,
        catalog,
        &[101, 102],
        &[data_file(103, table_b, 80, 119)],
    );
    assert_eq!(current_file_ids(&kv, catalog, table_a), vec![3]);
    assert_eq!(current_file_ids(&kv, catalog, table_b), vec![103]);
    assert_eq!(current_row_count(&kv, catalog, table_a), 119);
    assert_eq!(current_row_count(&kv, catalog, table_b), 119);
}

#[test]
fn mirrors_rewrite_inlined_file_deletes_test_inline_only_deletions_make_file_rewritable() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_inlined_file_deletes.test
    //
    // Storage contract:
    // - A source file with inline file deletions but no physical delete file is still eligible for
    //   rewrite-delete compaction.
    // - The replacement starts at the rewrite snapshot and becomes the only current data file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "a", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![InlineFileDeletionRow::new(
            table,
            DataFileId(1),
            25,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    rewrite(&mut kv, catalog, &[1], &[data_file(2, table, 0, 49)]);
    let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file.data_file_id, DataFileId(2));
    assert_eq!(
        current[0].data_file.validity.begin_order,
        rewrite_snapshot.order
    );
    assert!(current[0].delete_file.is_none());
}

#[test]
fn mirrors_rewrite_inlined_file_deletes_add_files_test_imported_file_with_inline_delete_rewrites() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_inlined_file_deletes_add_files.test
    //
    // Storage contract:
    // - Imported files are just data-file metadata once DuckLake saves them.
    // - Inline-only deletes on imported files should trigger the same rewrite path as ordinary
    //   table-created files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "a", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file_with_path(1, table, 0, 50, "../source.parquet")],
    )
    .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![InlineFileDeletionRow::new(
            table,
            DataFileId(1),
            25,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    rewrite(&mut kv, catalog, &[1], &[data_file(2, table, 0, 49)]);
    assert_eq!(current_file_ids(&kv, catalog, table), vec![2]);
    assert_eq!(current_row_count(&kv, catalog, table), 49);
}

#[test]
fn mirrors_rewrite_merge_adjacent_test_rewritten_file_can_later_merge_with_clean_files() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_merge_adjacent.test
    //
    // Storage contract:
    // - Rewrite-delete compaction applies deletes to one source file.
    // - The rewritten replacement is then a clean current data file and can participate in a
    //   merge-adjacent compaction with other current files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "a", "varchar"), column(2, "b", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1),
            data_file(2, table, 1, 1),
            data_file(3, table, 2, 100_000),
        ],
    )
    .unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 3, 99_899)]).unwrap();
    rewrite(&mut kv, catalog, &[3], &[data_file(4, table, 0, 101)]);
    merge(
        &mut kv,
        catalog,
        &[1, 2, 4],
        data_file(5, table, 0, 103),
        Vec::new(),
    );
    assert_eq!(current_file_ids(&kv, catalog, table), vec![5]);
    assert_eq!(current_row_count(&kv, catalog, table), 103);
}

#[test]
fn given_rewrite_replacement_uses_local_row_ids_when_source_starts_later_then_storage_normalizes_to_global_range()
 {
    // Mirrors the runtime request shape from:
    // third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_merge_adjacent.test
    //
    // Storage contract:
    // - DuckLake asks metadata storage to replace source file 3, whose global row-id range starts
    //   at 2.
    // - If the replacement payload uses file-local row ids starting at 0, storage must save the
    //   replacement in the same global row-id space as the source, otherwise later compaction sees
    //   a false overlap with unrelated files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "a", "varchar"), column(2, "b", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1),
            data_file(2, table, 1, 1),
            data_file(3, table, 2, 100_000),
        ],
    )
    .unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 3, 99_899)]).unwrap();

    let rewritten = commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(3)],
            new_files: vec![data_file(4, table, 0, 101)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    assert_eq!(rewritten.new_files[0].row_id_start, 2);
    merge(
        &mut kv,
        catalog,
        &[1, 2, 4],
        data_file(5, table, 0, 103),
        Vec::new(),
    );
    assert_eq!(current_file_ids(&kv, catalog, table), vec![5]);
}

#[test]
fn mirrors_rewrite_partitioning_test_rewrite_preserves_replacement_partition_values() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_partitioning.test
    //
    // Storage contract:
    // - DuckLake rewrites deleted files partition group by partition group.
    // - Replacement data files must keep the partition value rows supplied with the rewrite.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "partitioned",
        vec![column(1, "part_key", "integer"), column(2, "id", "integer")],
    );
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 2),
            data_file(2, table, 2, 2),
            data_file(3, table, 4, 1),
            data_file(4, table, 5, 1),
        ],
        vec![delete_file(1, 1, 1), delete_file(2, 2, 1)],
        &[],
        vec![
            partition_value(1, table, 0, "1"),
            partition_value(2, table, 0, "2"),
            partition_value(3, table, 0, "1"),
            partition_value(4, table, 0, "2"),
        ],
        Vec::new(),
    )
    .unwrap();

    rewrite_with_partitions(
        &mut kv,
        catalog,
        &[1],
        vec![data_file(5, table, 0, 2)],
        vec![partition_value(5, table, 0, "1")],
    );
    rewrite_with_partitions(
        &mut kv,
        catalog,
        &[2],
        vec![data_file(6, table, 2, 2)],
        vec![partition_value(6, table, 0, "2")],
    );
    merge(
        &mut kv,
        catalog,
        &[3, 5],
        data_file(7, table, 0, 3),
        vec![partition_value(7, table, 0, "1")],
    );
    merge(
        &mut kv,
        catalog,
        &[4, 6],
        data_file(8, table, 2, 3),
        vec![partition_value(8, table, 0, "2")],
    );

    assert_eq!(partition_values(&kv, catalog, 7), vec!["1"]);
    assert_eq!(partition_values(&kv, catalog, 8), vec!["2"]);
    assert_eq!(current_file_ids(&kv, catalog, table), vec![7, 8]);
}

#[test]
fn mirrors_rewrite_preserves_row_id_test_replacements_keep_original_row_id_ranges() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_preserves_row_id.test
    //
    // Storage contract:
    // - Rewriting a data file with deletes must keep row-id lineage in the replacement file.
    // - Later inserts use row-id ranges after the existing maximum, not after the compacted row
    //   count.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "a", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 5)]).unwrap();
    rewrite(&mut kv, catalog, &[1], &[data_file(2, table, 0, 5)]);
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, table, 10, 2)]).unwrap();

    let mut current = list_current_data_files_with_deletes(&kv, catalog, table)
        .unwrap()
        .into_iter()
        .map(|file| (file.data_file.data_file_id.0, file.data_file.row_id_start))
        .collect::<Vec<_>>();
    current.sort_unstable();
    assert_eq!(current, vec![(2, 0), (3, 10)]);
}

#[test]
fn mirrors_rewrite_target_file_size_test_storage_accepts_prefiltered_rewrite_groups() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_target_file_size.test
    //
    // Storage contract:
    // - Target-file-size planning is done by DuckLake before it calls storage.
    // - Storage must apply whatever source group and replacement files it is given, preserving
    //   current file count and closing delete files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "tbl",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        (1..=4)
            .map(|id| data_file(id, table, (id - 1) * 1000, 1000))
            .collect(),
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![
            delete_file(1, 1, 1),
            delete_file(2, 2, 1),
            delete_file(3, 3, 1),
        ],
    )
    .unwrap();

    rewrite(
        &mut kv,
        catalog,
        &[1, 2, 3],
        &[
            data_file(5, table, 1, 999),
            data_file(6, table, 1001, 999),
            data_file(7, table, 2001, 999),
        ],
    );
    assert_eq!(current_file_ids(&kv, catalog, table), vec![4, 5, 6, 7]);
    assert!(
        list_current_data_files_with_deletes(&kv, catalog, table)
            .unwrap()
            .iter()
            .all(|file| file.delete_file.is_none())
    );

    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![
            delete_file(4, 5, 998),
            delete_file(5, 6, 998),
            delete_file(6, 7, 998),
            delete_file(7, 4, 1),
        ],
    )
    .unwrap();
    rewrite(&mut kv, catalog, &[5, 6, 7], &[data_file(8, table, 1, 3)]);
    rewrite(&mut kv, catalog, &[4], &[data_file(9, table, 3001, 999)]);
    assert_eq!(current_row_count(&kv, catalog, table), 1002);
}

#[test]
fn mirrors_rewrite_transaction_conflict_test_delete_and_rewrite_conflict_but_insert_does_not() {
    // Mirrors: third_party/ducklake/test/sql/rewrite_data_files/test_rewrite_transaction_conflict.test
    //
    // Storage contract:
    // - Delete/rewrite and rewrite/rewrite races against the same table must be rejected.
    // - A concurrent insert after a rewrite base does not mutate the source files and should not
    //   conflict with the rewrite.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "i", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 10)]).unwrap();
    let through_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let stale_after_delete = commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        base.order,
        through_delete.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(2, table, 10, 90)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(stale_after_delete.is_err());

    let rewrite_base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        rewrite_base.order,
        rewrite_base.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(3, table, 10, 90)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let after_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let stale_rewrite = commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        rewrite_base.order,
        after_rewrite.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(4, table, 10, 90)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(stale_rewrite.is_err());

    commit_append_data_files(&mut kv, catalog, vec![data_file(5, table, 100, 1)]).unwrap();
    let after_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert!(
        commit_rewrite_delete_data_files_with_conflict_check(
            &mut kv,
            catalog,
            after_rewrite.order,
            after_insert.order,
            RewriteDeleteCompaction {
                source_file_ids: vec![DataFileId(3)],
                new_files: vec![data_file(6, table, 10, 90)],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .is_err(),
        "clean files cannot be rewrite-delete compacted without a delete source"
    );
}

#[test]
fn mirrors_row_id_test_storage_rejects_current_row_id_overlap_and_keeps_gaps_after_rewrite() {
    // Mirrors: third_party/ducklake/test/sql/rowid/ducklake_row_id.test
    //
    // Storage contract:
    // - Committed data-file row-id ranges are stable storage metadata.
    // - Rewrites preserve gaps by using the row-id start DuckLake supplies.
    // - New current files may not overlap current row-id ranges for the same table.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "i", "integer")],
    );

    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 3), data_file(2, table, 3, 2)],
    )
    .unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 1)]).unwrap();
    rewrite(&mut kv, catalog, &[1], &[data_file(3, table, 0, 2)]);
    commit_append_data_files(&mut kv, catalog, vec![data_file(4, table, 5, 5)]).unwrap();

    assert_eq!(
        current_row_ranges(&kv, catalog, table),
        vec![(0, 2), (3, 2), (5, 5)]
    );
    let overlap = commit_append_data_files(&mut kv, catalog, vec![data_file(5, table, 4, 10)]);
    assert!(overlap.is_err());
}

#[test]
fn mirrors_retry_commit_failure_test_storage_rejects_stale_commit_and_accepts_retried_base() {
    // Mirrors: third_party/ducklake/test/sql/retry/commit_failure.test
    //
    // Storage contract:
    // - The retry loop lives above storage, but storage must give deterministic conflict results.
    // - A stale rewrite attempt is rejected; the same logical work retried from the latest base is
    //   accepted when the source file is still current and deleted.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "tbl",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    let first_reader_base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 2)]).unwrap();
    let after_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let stale = commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        first_reader_base.order,
        after_delete.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(2, table, 2, 8)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(stale.is_err());

    commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        after_delete.order,
        after_delete.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(3, table, 2, 8)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    assert_eq!(current_file_ids(&kv, catalog, table), vec![3]);
}

#[test]
fn mirrors_max_retry_count_test_storage_conflict_signal_is_stable_for_retry_budgeting() {
    // Mirrors: third_party/ducklake/test/sql/settings/max_retry_count.test
    //
    // Storage contract:
    // - `ducklake_max_retry_count` is a DuckDB setting, but its useful behavior depends on storage
    //   reporting conflicts consistently.
    // - Repeated stale attempts against the same base should all fail without changing metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "retry_test",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    let stale_base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 1)]).unwrap();
    let through = latest_snapshot(&kv, catalog).unwrap().unwrap();

    for replacement in 2..=4 {
        let result = commit_rewrite_delete_data_files_with_conflict_check(
            &mut kv,
            catalog,
            stale_base.order,
            through.order,
            RewriteDeleteCompaction {
                source_file_ids: vec![DataFileId(1)],
                new_files: vec![data_file(replacement, table, 1, 9)],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        );
        assert!(result.is_err());
        assert_eq!(current_file_ids(&kv, catalog, table), vec![1]);
    }
}

#[test]
fn mirrors_current_commit_test_latest_snapshot_reports_only_committed_metadata() {
    // Mirrors: third_party/ducklake/test/sql/snapshot_info/ducklake_current_commit.test
    //
    // Storage contract:
    // - The current snapshot id is the latest committed catalog snapshot sequence.
    // - Uncommitted DuckDB transactions do not write catalog metadata, so storage should only
    //   advance after a commit reaches the catalog.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    assert_eq!(current_public_snapshot(&kv, catalog), 0);

    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "integer",
        vec![column(1, "i", "integer")],
    );
    assert_eq!(current_public_snapshot(&kv, catalog), 1);

    let observed_inside_uncommitted_transactions = current_public_snapshot(&kv, catalog);
    assert_eq!(observed_inside_uncommitted_transactions, 1);

    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "integers_2",
        vec![column(1, "i", "integer")],
    );
    assert_eq!(current_public_snapshot(&kv, catalog), 2);

    commit_append_data_files(&mut kv, catalog, vec![data_file(1, TableId(1), 0, 1)]).unwrap();
    assert_eq!(current_public_snapshot(&kv, catalog), 3);
}

fn build_last_snapshot_rewrite_shape(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
) {
    commit_append_data_files(kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    commit_register_delete_files(kv, catalog, vec![delete_file(1, 1, 50)]).unwrap();
    commit_append_data_files(kv, catalog, vec![data_file(2, table, 100, 100)]).unwrap();
    commit_register_delete_files(kv, catalog, vec![delete_file(2, 1, 80)]).unwrap();
    commit_register_delete_files(kv, catalog, vec![delete_file(3, 2, 1)]).unwrap();
    commit_append_data_files(kv, catalog, vec![data_file(3, table, 200, 100)]).unwrap();
    commit_register_delete_files(kv, catalog, vec![delete_file(4, 3, 49)]).unwrap();
}

fn build_two_file_delete_shape(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    first_file_id: u64,
    first_delete_id: u64,
    second_delete_id: u64,
) {
    commit_append_data_files(
        kv,
        catalog,
        vec![
            data_file(first_file_id, table, 0, 100),
            data_file(first_file_id + 1, table, 100, 100),
        ],
    )
    .unwrap();
    commit_register_delete_files(
        kv,
        catalog,
        vec![
            delete_file(first_delete_id, first_file_id, 80),
            delete_file(second_delete_id, first_file_id + 1, 1),
        ],
    )
    .unwrap();
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

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    data_file_with_path(
        id,
        table,
        row_id_start,
        record_count,
        &format!("main/table-{}/file-{id}.parquet", table.0),
    )
}

fn data_file_with_path(
    id: u64,
    table: TableId,
    row_id_start: u64,
    record_count: u64,
    path: &str,
) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        path,
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> DeleteFileRow {
    delete_file_with_path(
        id,
        data_file_id,
        &format!("main/delete-{id}.parquet"),
        record_count,
    )
}

fn delete_file_with_path(
    id: u64,
    data_file_id: u64,
    path: &str,
    record_count: u64,
) -> DeleteFileRow {
    DeleteFileRow::new(
        DeleteFileId(id),
        DataFileId(data_file_id),
        path,
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
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

fn rewrite(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    replacements: &[DataFileRow],
) {
    rewrite_with_partitions(kv, catalog, source_ids, replacements.to_vec(), Vec::new());
}

fn rewrite_with_partitions(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    replacements: Vec<DataFileRow>,
    partition_values: Vec<FilePartitionValueRow>,
) {
    commit_rewrite_delete_data_files(
        kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: source_ids.iter().copied().map(DataFileId).collect(),
            new_files: replacements,
            partition_values,
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
}

fn merge(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    replacement: DataFileRow,
    partition_values: Vec<FilePartitionValueRow>,
) {
    commit_merge_adjacent_data_files(
        kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: source_ids.iter().copied().map(DataFileId).collect(),
            new_files: vec![replacement],
            partition_values,
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
}

fn current_file_ids(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> Vec<u64> {
    let mut ids = list_current_data_files_with_deletes(kv, catalog, table)
        .unwrap()
        .into_iter()
        .map(|file| file.data_file.data_file_id.0)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn current_row_ranges(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
) -> Vec<(u64, u64)> {
    let mut ranges = list_current_data_files_with_deletes(kv, catalog, table)
        .unwrap()
        .into_iter()
        .map(|file| (file.data_file.row_id_start, file.data_file.record_count))
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    ranges
}

fn current_row_count(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> u64 {
    visible_count_at(
        kv,
        catalog,
        table,
        latest_snapshot(kv, catalog).unwrap().unwrap().order,
    )
}

fn visible_count_at(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    order: CatalogOrderId,
) -> u64 {
    list_data_files_with_deletes_at(kv, catalog, table, order)
        .unwrap()
        .into_iter()
        .map(|file| {
            file.data_file.record_count
                - file
                    .delete_file
                    .as_ref()
                    .map_or(0, |delete| delete.record_count)
        })
        .sum()
}

fn assert_visible_count_at(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    order: CatalogOrderId,
    expected: u64,
) {
    assert_eq!(visible_count_at(kv, catalog, table, order), expected);
}

fn current_delete_file(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    data_file_id: u64,
) -> DeleteFileRow {
    list_current_data_files_with_deletes(kv, catalog, table)
        .unwrap()
        .into_iter()
        .find(|file| file.data_file.data_file_id == DataFileId(data_file_id))
        .and_then(|file| file.delete_file)
        .unwrap()
}

fn partition_values(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: u64,
) -> Vec<String> {
    list_file_partition_values(kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|row| row.data_file_id == DataFileId(data_file_id))
        .map(|row| row.partition_value)
        .collect()
}

fn nth_snapshot_order(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    sequence: u64,
) -> CatalogOrderId {
    list_snapshots(kv, catalog)
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.sequence.0 == sequence)
        .unwrap()
        .order
}

fn current_public_snapshot(kv: &FakeOrderedCatalogKv, catalog: CatalogId) -> u64 {
    latest_snapshot(kv, catalog).unwrap().unwrap().sequence.0
}

fn expire_all_but_current_snapshot(kv: &mut FakeOrderedCatalogKv, catalog: CatalogId) {
    let Some(current) = latest_snapshot(kv, catalog).unwrap() else {
        return;
    };
    let expirable = list_snapshots(kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.sequence != current.sequence)
        .map(|snapshot| snapshot.sequence)
        .collect::<Vec<_>>();
    expire_snapshots(kv, catalog, &expirable).unwrap();
}

#[allow(dead_code)]
fn snapshot_change_text(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> String {
    snapshot_changes_made(kv, catalog, order).unwrap()
}

#[allow(dead_code)]
fn public_sequence_for_order(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> u64 {
    public_snapshot_sequence_for_order(kv, catalog, order)
        .unwrap()
        .unwrap()
        .0
}
