use ducklake_catalog::{
    CatalogDebugRow, CatalogId, CatalogOrderId, ColumnDrop, ColumnId, ColumnRename,
    ColumnTypeChange, DataFileId, DataFileRow, FakeOrderedCatalogKv, FilePartitionValueRow,
    MergeAdjacentCompaction, PartitionKeyIndex, RewriteDeleteCompaction, SchemaId, TableColumnRow,
    TableId, TableRow, commit_append_data_files, commit_append_table_columns,
    commit_change_table_column_types, commit_create_table_row, commit_data_mutation_with_details,
    commit_data_mutation_with_file_partitions,
    commit_data_mutation_with_file_partitions_and_inline_deletes, commit_drop_table_columns,
    commit_drop_tables, commit_merge_adjacent_data_files,
    commit_merge_adjacent_data_files_with_conflict_check, commit_rename_table_columns,
    commit_rewrite_delete_data_files, expire_snapshots, initialize_catalog_if_absent,
    latest_snapshot, list_catalog_debug_rows, list_current_data_files_by_partition_value,
    list_current_data_files_with_deletes, list_data_files_at, list_file_partition_values,
    list_old_data_files_for_cleanup, list_old_delete_files_for_cleanup, list_snapshots,
    remove_old_data_files, remove_old_delete_files,
};

#[test]
fn mirrors_cleanup_old_files_global_option_test_expired_dropped_table_file_enters_cleanup_queue() {
    // Mirrors: third_party/ducklake/test/sql/compaction/cleanup_old_files_global_option.test
    //
    // Storage contract:
    // - DuckLake inserts a file, drops the table, expires old snapshots, and then asks storage for
    //   old files eligible for cleanup.
    // - Storage must keep the old file discoverable until cleanup removes it.
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
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1000)]).unwrap();
    let insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_tables(&mut kv, catalog, &[table]).unwrap();
    expire_snapshots(&mut kv, catalog, &[insert.sequence]).unwrap();

    let old = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(
        old.iter().map(|file| file.data_file_id).collect::<Vec<_>>(),
        vec![DataFileId(1)]
    );
    remove_old_data_files(&mut kv, catalog, &[DataFileId(1)]).unwrap();
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn mirrors_compaction_alter_table_test_merge_groups_follow_schema_boundaries() {
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_alter_table.test
    //
    // Storage contract:
    // - DuckLake groups merge-adjacent requests by compatible schema boundaries.
    // - Replacement files must cover the historical visibility windows of their source files.
    // - Current reads return the three replacement files, while time-travel reads before each
    //   schema change still resolve the relevant replacement group.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "id", "integer"), column(2, "i", "integer")],
    );
    append_one_file(&mut kv, catalog, table, 1, 0);
    append_one_file(&mut kv, catalog, table, 2, 1);
    let before_add = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(&mut kv, catalog, table, vec![column(3, "j", "integer")]).unwrap();
    append_one_file(&mut kv, catalog, table, 3, 2);
    append_one_file(&mut kv, catalog, table, 4, 3);
    let before_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(2))]).unwrap();
    append_one_file(&mut kv, catalog, table, 5, 4);
    append_one_file(&mut kv, catalog, table, 6, 5);
    let before_add_i = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(&mut kv, catalog, table, vec![column(4, "i", "varchar")]).unwrap();
    append_one_file(&mut kv, catalog, table, 7, 6);
    append_one_file(&mut kv, catalog, table, 8, 7);

    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file(9, table, 0, 2),
        Vec::new(),
    );
    merge(
        &mut kv,
        catalog,
        &[3, 4],
        data_file(10, table, 2, 2),
        Vec::new(),
    );
    merge(
        &mut kv,
        catalog,
        &[5, 6, 7, 8],
        data_file(11, table, 4, 4),
        Vec::new(),
    );

    assert_current_file_ids(&kv, catalog, table, &[9, 10, 11]);
    assert_file_ids_at(&kv, catalog, table, before_add.order, &[9]);
    assert_file_ids_at(&kv, catalog, table, before_drop.order, &[9, 10]);
    assert_file_ids_at(&kv, catalog, table, before_add_i.order, &[9, 10, 11]);
}

#[test]
fn mirrors_compaction_cleanup_global_test_old_file_queue_survives_until_cleanup() {
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_cleanup_global.test
    //
    // Storage contract:
    // - Deleted and dropped table files remain in the cleanup queue after snapshot expiration.
    // - Cleanup removes queued old data files and delete files together.
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
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 10)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    commit_drop_tables(&mut kv, catalog, &[table]).unwrap();

    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    let old_delete = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert!(old_data.is_empty());
    assert!(old_delete.is_empty());

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let expirable = list_snapshots(&kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.order < latest.order)
        .map(|snapshot| snapshot.sequence)
        .collect::<Vec<_>>();
    expire_snapshots(&mut kv, catalog, &expirable).unwrap();

    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    let old_delete = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(old_data.len(), 1);
    assert_eq!(old_delete.len(), 1);
    remove_old_data_files(&mut kv, catalog, &[DataFileId(1)]).unwrap();
    remove_old_delete_files(&mut kv, catalog, &[ducklake_catalog::DeleteFileId(1)]).unwrap();
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn mirrors_compaction_delete_conflict_test_stale_merge_rewrite_is_rejected() {
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_delete_conflict.test
    //
    // Storage contract:
    // - A compaction planned at an older base snapshot must fail when a delete/rewrite conflict is
    //   committed before the compaction commit.
    // - Inserts alone should not conflict with a compaction planned over existing source files.
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
        vec![data_file(1, table, 0, 1), data_file(2, table, 1, 1)],
    )
    .unwrap();
    let base = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 1)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let through = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let rejected = commit_merge_adjacent_data_files_with_conflict_check(
        &mut kv,
        catalog,
        base.order,
        through.order,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![data_file(3, table, 0, 2)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(rejected.is_err());

    commit_append_data_files(&mut kv, catalog, vec![data_file(4, table, 2, 1)]).unwrap();
    let after_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let accepted = commit_merge_adjacent_data_files_with_conflict_check(
        &mut kv,
        catalog,
        through.order,
        after_insert.order,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(2), DataFileId(4)],
            new_files: vec![data_file(5, table, 1, 2)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(accepted.is_ok());

    let stale_drop = commit_data_mutation_with_details(
        &mut kv,
        catalog,
        ducklake_catalog::DataMutationInput {
            data_files: Vec::new(),
            delete_files: Vec::new(),
            inline_flushes: [].to_vec(),
            partition_values: Vec::new(),
            inline_file_deletions: Vec::new(),
            dropped_data_file_ids: vec![DataFileId(2), DataFileId(4)],
            ..ducklake_catalog::DataMutationInput::default()
        },
    );
    assert!(stale_drop.is_err());
}

#[test]
fn mirrors_compaction_partitioned_table_test_replacement_files_keep_partition_values() {
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_partitioned_table.test
    //
    // Storage contract:
    // - DuckLake compacts files within each partition and supplies partition values for each
    //   replacement file.
    // - Current partition scans should return exactly the replacement file for that partition.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "partitioned",
        vec![
            column(1, "part_key", "integer"),
            column(2, "value", "integer"),
        ],
    );
    append_partitioned_files(
        &mut kv,
        catalog,
        table,
        &[(1, "1"), (2, "1"), (3, "2"), (4, "2")],
    );
    let first_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();

    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file(5, table, 0, 2),
        vec![partition_value(5, table, 0, "1")],
    );
    merge(
        &mut kv,
        catalog,
        &[3, 4],
        data_file(6, table, 2, 2),
        vec![partition_value(6, table, 0, "2")],
    );

    assert_current_file_ids(&kv, catalog, table, &[5, 6]);
    assert_partition_files(&kv, catalog, table, "1", &[5]);
    assert_partition_files(&kv, catalog, table, "2", &[6]);
    assert_file_ids_at(&kv, catalog, table, first_insert.order, &[1, 2, 3, 4]);
}

#[test]
fn mirrors_compaction_partitioned_non_adjacent_test_same_partition_files_merge_across_insert_order()
{
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_partitioned_non_adjacent.test
    //
    // Storage contract:
    // - DuckLake may send non-adjacent file ids from the same partition as one merge group.
    // - Replacement partition metadata must still point at the partition value, not the original
    //   insertion order.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "partitioned",
        vec![
            column(1, "part_key", "integer"),
            column(2, "value", "integer"),
        ],
    );
    append_partitioned_files(
        &mut kv,
        catalog,
        table,
        &[(1, "1"), (2, "2"), (3, "1"), (4, "2")],
    );

    merge(
        &mut kv,
        catalog,
        &[1, 3],
        data_file(5, table, 0, 2),
        vec![partition_value(5, table, 0, "1")],
    );
    merge(
        &mut kv,
        catalog,
        &[2, 4],
        data_file(6, table, 1, 2),
        vec![partition_value(6, table, 0, "2")],
    );

    assert_current_file_ids(&kv, catalog, table, &[5, 6]);
    assert_partition_files(&kv, catalog, table, "1", &[5]);
    assert_partition_files(&kv, catalog, table, "2", &[6]);
}

#[test]
fn mirrors_compaction_per_thread_output_test_merge_then_rewrite_keeps_current_replacement() {
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_per_thread_output.test
    //
    // Storage contract:
    // - Per-thread output changes how DuckLake writes files, but storage receives ordinary merge
    //   and rewrite requests.
    // - After merge and delete rewrite, current reads should expose only the final replacement.
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
        vec![
            data_file(1, table, 0, 1),
            data_file(2, table, 1, 1),
            data_file(3, table, 2, 1),
        ],
    )
    .unwrap();
    merge(
        &mut kv,
        catalog,
        &[1, 2, 3],
        data_file(4, table, 0, 3),
        Vec::new(),
    );
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 4, 1)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(4)],
            new_files: vec![data_file(5, table, 0, 2)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    assert_current_file_ids(&kv, catalog, table, &[5]);
}

#[test]
fn mirrors_merge_adjacent_file_size_filter_test_only_selected_sources_are_replaced() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_file_size_filter.test
    //
    // Storage contract:
    // - DuckLake applies min/max size filters before calling storage.
    // - Storage replaces only the source file ids it is given and leaves non-selected files current.
    let catalog = CatalogId(1);
    let table = TableId(1);
    let mut kv = create_file_size_filter_table(catalog, table);

    merge(
        &mut kv,
        catalog,
        &[3, 4],
        data_file_with_size(5, table, 100, 200, 3_600),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[1, 2, 5]);

    let mut kv = create_file_size_filter_table(catalog, table);
    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file_with_size(5, table, 0, 20, 800),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[3, 4, 5]);
}

#[test]
fn mirrors_merge_adjacent_options_test_table_selection_compacts_only_requested_table_ids() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_options.test
    //
    // Storage contract:
    // - DuckLake resolves table/schema options before storage is called.
    // - A merge for one table id should not expire files for similarly named tables in another
    //   schema.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    for table_id in 1..=4 {
        create_table_in_schema(
            &mut kv,
            catalog,
            TableId(table_id),
            SchemaId(table_id),
            "example",
            vec![column(1, "key", "varchar")],
        );
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(table_id * 10, TableId(table_id), 0, 1),
                data_file(table_id * 10 + 1, TableId(table_id), 1, 1),
            ],
        )
        .unwrap();
    }

    merge(
        &mut kv,
        catalog,
        &[20, 21],
        data_file(99, TableId(2), 0, 2),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, TableId(1), &[10, 11]);
    assert_current_file_ids(&kv, catalog, TableId(2), &[99]);
    assert_current_file_ids(&kv, catalog, TableId(3), &[30, 31]);
    assert_current_file_ids(&kv, catalog, TableId(4), &[40, 41]);
}

#[test]
fn mirrors_merge_files_expired_snapshots_test_current_files_can_merge_after_old_snapshots_expire() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_files_expired_snapshots.test
    //
    // Storage contract:
    // - Expiring old snapshots must not hide files that are still current.
    // - Those current files remain eligible for merge-adjacent compaction.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "example",
        vec![column(1, "key", "varchar")],
    );
    let create = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1), data_file(2, table, 1, 1)],
    )
    .unwrap();
    expire_snapshots(&mut kv, catalog, &[create.sequence]).unwrap();

    assert_current_file_ids(&kv, catalog, table, &[1, 2]);
    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file(3, table, 0, 2),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[3]);
}

#[test]
fn mirrors_expire_snapshots_drop_table_test_cleanup_removes_table_scoped_file_metadata() {
    // Mirrors: third_party/ducklake/test/sql/compaction/expire_snapshots_drop_table.test
    //
    // Storage contract:
    // - After a table is dropped and its historical snapshots are expired, cleanup should remove
    //   table-scoped data-file and delete-file metadata.
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
    let create = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1000)]).unwrap();
    let insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 250)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let first_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(2, 1, 500)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let second_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_tables(&mut kv, catalog, &[table]).unwrap();

    expire_snapshots(
        &mut kv,
        catalog,
        &[
            create.sequence,
            insert.sequence,
            first_delete.sequence,
            second_delete.sequence,
        ],
    )
    .unwrap();
    drain_cleanup(&mut kv, catalog);

    assert!(
        list_catalog_debug_rows(&kv, catalog, usize::MAX)
            .unwrap()
            .into_iter()
            .all(|row| !matches!(
                row,
                CatalogDebugRow::Table(ref table_row) if table_row.table_id == table
            ) && !matches!(
                row,
                CatalogDebugRow::DataFile(ref data_file) if data_file.table_id == table
            ) && !matches!(row, CatalogDebugRow::DeleteFile(_))),
        "dropped table's table/data/delete metadata should be gone after expiration and cleanup"
    );
}

#[test]
fn mirrors_expire_snapshots_metadata_cleanup_test_table_debug_rows_are_removed_after_drop_cleanup()
{
    // Mirrors: third_party/ducklake/test/sql/compaction/expire_snapshots_metadata_cleanup.test
    //
    // Storage contract:
    // - Dropping a table and expiring its history should remove table-scoped debug metadata,
    //   including columns and file rows.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "tbl",
        vec![column(1, "i", "integer"), column(2, "v", "variant")],
    );
    let create = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1), data_file(2, table, 1, 1)],
    )
    .unwrap();
    let insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(&mut kv, catalog, table, vec![column(3, "j", "integer")]).unwrap();
    let alter = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_tables(&mut kv, catalog, &[table]).unwrap();

    expire_snapshots(
        &mut kv,
        catalog,
        &[create.sequence, insert.sequence, alter.sequence],
    )
    .unwrap();
    drain_cleanup(&mut kv, catalog);

    assert!(
        list_catalog_debug_rows(&kv, catalog, usize::MAX)
            .unwrap()
            .into_iter()
            .all(|row| match row {
                CatalogDebugRow::Table(row) => row.table_id != table,
                CatalogDebugRow::Column { table_id, .. } => table_id != table,
                CatalogDebugRow::DataFile(row) => row.table_id != table,
                CatalogDebugRow::DeleteFile(_) | CatalogDebugRow::Snapshot(_) => true,
            })
    );
}

#[test]
fn mirrors_merge_adjacent_after_add_files_schema_evolution_test_replacements_do_not_duplicate_external_rows()
 {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_after_add_files_schema_evolution.test
    //
    // Storage contract:
    // - Externally added files, rewritten files, later inserts, and schema changes all become
    //   ordinary data-file metadata.
    // - A merge-adjacent replacement should expire only its source files and leave one current
    //   representation of each row-id span.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![
            column(1, "id", "integer"),
            column(2, "name", "varchar"),
            column(3, "score", "double"),
        ],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file_with_path(1, table, 0, 50, "../external1.parquet"),
            data_file_with_path(2, table, 50, 50, "../external2.parquet"),
        ],
    )
    .unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(3, table, 0, 20)],
        vec![delete_file(1, 1, 20), delete_file(2, 2, 10)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(4, table, 200, 30)]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(4, "category", "varchar")],
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file_without_row_ids(5, table, 120)],
    )
    .unwrap();

    merge(
        &mut kv,
        catalog,
        &[3, 4, 5],
        data_file(6, table, 0, 120),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[1, 2, 6]);
    let current_row_count = list_current_data_files_with_deletes(&kv, catalog, table)
        .unwrap()
        .into_iter()
        .map(|file| file.data_file.record_count)
        .sum::<u64>();
    assert_eq!(current_row_count, 220);
}

#[test]
fn mirrors_compaction_schema_version_per_table_test_other_table_schema_changes_do_not_block_merge()
{
    // Mirrors: third_party/ducklake/test/sql/compaction/compaction_schema_version_per_table.test
    //
    // Storage contract:
    // - Schema changes on one table must not prevent merge-adjacent compaction for another table.
    // - Each table id owns its data-file visibility independently.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let compact = TableId(1);
    let other = TableId(2);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        compact,
        "table_to_compact",
        vec![column(1, "a", "integer")],
    );
    create_table(
        &mut kv,
        catalog,
        other,
        "another_table",
        vec![column(1, "x", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, compact, 0, 5),
            data_file(2, compact, 5, 2),
            data_file(3, other, 0, 2),
            data_file(4, other, 2, 2),
        ],
    )
    .unwrap();
    commit_append_table_columns(&mut kv, catalog, other, vec![column(2, "y", "varchar")]).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(5, compact, 7, 2), data_file(6, other, 4, 2)],
    )
    .unwrap();

    merge(
        &mut kv,
        catalog,
        &[1, 2, 5],
        data_file(7, compact, 0, 9),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, compact, &[7]);
    assert_current_file_ids(&kv, catalog, other, &[3, 4, 6]);
}

#[test]
fn mirrors_merge_adjacent_cross_schema_isolation_test_incompatible_boundaries_keep_files_separate()
{
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_cross_schema_isolation.test
    //
    // Storage contract:
    // - DuckLake must only send merge groups that are compatible across schema/partition
    //   boundaries.
    // - Storage then expires exactly those source files and leaves incompatible files current.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let rename_table = TableId(1);
    let drop_table = TableId(2);
    let type_table = TableId(3);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    create_table(
        &mut kv,
        catalog,
        rename_table,
        "t_rename",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, rename_table, 0, 3),
            data_file(2, rename_table, 3, 2),
        ],
    )
    .unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            rename_table,
            column(1, "ident", "integer"),
        )],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, rename_table, 5, 2)]).unwrap();
    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file(4, rename_table, 0, 5),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, rename_table, &[3, 4]);

    create_table(
        &mut kv,
        catalog,
        drop_table,
        "t_drop",
        vec![column(1, "id", "integer"), column(2, "extra", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(10, drop_table, 0, 3),
            data_file(11, drop_table, 3, 2),
        ],
    )
    .unwrap();
    commit_drop_table_columns(
        &mut kv,
        catalog,
        &[ColumnDrop::new(drop_table, ColumnId(2))],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(12, drop_table, 5, 2)]).unwrap();
    merge(
        &mut kv,
        catalog,
        &[10, 11],
        data_file(13, drop_table, 0, 5),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, drop_table, &[12, 13]);

    create_table(
        &mut kv,
        catalog,
        type_table,
        "t_type",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(20, type_table, 0, 3),
            data_file(21, type_table, 3, 2),
        ],
    )
    .unwrap();
    commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(type_table, column(1, "id", "bigint"))],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(22, type_table, 5, 2)]).unwrap();
    merge(
        &mut kv,
        catalog,
        &[20, 21],
        data_file(23, type_table, 0, 5),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, type_table, &[22, 23]);
}

#[test]
fn mirrors_merge_adjacent_external_hive_paths_test_replacement_path_and_null_partition_are_stored()
{
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_external_hive_paths.test
    //
    // Storage contract:
    // - External files can be compacted into a replacement under the table's canonical
    //   hive-partitioned path.
    // - The replacement file keeps the partition value metadata, including the null hive marker.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "id", "integer"), column(2, "source", "varchar")],
    );
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file_with_path(1, table, 0, 1, "../external/source=audio/a.parquet"),
            data_file_with_path(2, table, 1, 1, "../external/source=audio/b.parquet"),
            data_file_with_path(
                3,
                table,
                2,
                1,
                "../external/source=__HIVE_DEFAULT_PARTITION__/a.parquet",
            ),
            data_file_with_path(
                4,
                table,
                3,
                1,
                "../external/source=__HIVE_DEFAULT_PARTITION__/b.parquet",
            ),
        ],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "audio"),
            partition_value(2, table, 0, "audio"),
            partition_value(3, table, 0, "__HIVE_DEFAULT_PARTITION__"),
            partition_value(4, table, 0, "__HIVE_DEFAULT_PARTITION__"),
        ],
    )
    .unwrap();

    merge(
        &mut kv,
        catalog,
        &[1, 2],
        data_file_with_path(5, table, 0, 2, "source=audio/ducklake-1.parquet"),
        vec![partition_value(5, table, 0, "audio")],
    );
    merge(
        &mut kv,
        catalog,
        &[3, 4],
        data_file_with_path(
            6,
            table,
            2,
            2,
            "source=__HIVE_DEFAULT_PARTITION__/ducklake-2.parquet",
        ),
        vec![partition_value(6, table, 0, "__HIVE_DEFAULT_PARTITION__")],
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file.path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "source=audio/ducklake-1.parquet",
            "source=__HIVE_DEFAULT_PARTITION__/ducklake-2.parquet"
        ]
    );
    assert_partition_files(&kv, catalog, table, "audio", &[5]);
    assert_partition_files(&kv, catalog, table, "__HIVE_DEFAULT_PARTITION__", &[6]);
}

#[test]
fn mirrors_merge_adjacent_max_files_test_storage_accepts_prefiltered_limited_merge_batches() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_max_files.test
    //
    // Storage contract:
    // - DuckLake enforces max_compacted_files before issuing storage writes.
    // - Each storage call replaces exactly the source batch it is given.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "example",
        vec![column(1, "key", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        (1..=20).map(|id| data_file(id, table, id - 1, 1)).collect(),
    )
    .unwrap();

    merge(
        &mut kv,
        catalog,
        &(1..=20).collect::<Vec<_>>(),
        data_file(21, table, 0, 20),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[21]);
}

#[test]
fn mirrors_merge_adjacent_max_files_partitioned_test_global_limit_compacts_one_partition_group() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_adjacent_max_files_partitioned.test
    //
    // Storage contract:
    // - With a global max operation limit, DuckLake sends only one partition group to storage.
    // - Storage compacts that group and leaves the other partition group current.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "v", "integer"), column(2, "p", "integer")],
    );
    append_partitioned_files(
        &mut kv,
        catalog,
        table,
        &[(1, "0"), (2, "1"), (3, "0"), (4, "1")],
    );
    merge(
        &mut kv,
        catalog,
        &[1, 3],
        data_file(5, table, 0, 2),
        vec![partition_value(5, table, 0, "0")],
    );
    assert_current_file_ids(&kv, catalog, table, &[2, 4, 5]);
}

#[test]
fn mirrors_merge_rewrite_partial_file_info_test_partial_max_survives_merge_then_rewrite() {
    // Mirrors: third_party/ducklake/test/sql/compaction/merge_rewrite_partial_file_info.test
    //
    // Storage contract:
    // - Merge replacements that cover only a partial snapshot range carry max_partial_order.
    // - Later rewrite-delete and merge operations must keep current visibility correct for both
    //   partial and non-partial files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file_with_size(1, table, 0, 5000, 20_000)],
    )
    .unwrap();
    append_one_file(&mut kv, catalog, table, 2, 5000);
    let b = latest_snapshot(&kv, catalog).unwrap().unwrap();
    append_one_file(&mut kv, catalog, table, 3, 5100);
    let c = latest_snapshot(&kv, catalog).unwrap().unwrap();
    merge(
        &mut kv,
        catalog,
        &[2, 3],
        data_file(4, table, 5000, 200).with_max_partial_order(Some(c.order)),
        Vec::new(),
    );
    append_one_file(&mut kv, catalog, table, 5, 5200);
    append_one_file(&mut kv, catalog, table, 6, 5210);
    let e = latest_snapshot(&kv, catalog).unwrap().unwrap();
    merge(
        &mut kv,
        catalog,
        &[5, 6],
        data_file(7, table, 5200, 20).with_max_partial_order(Some(e.order)),
        Vec::new(),
    );

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file.max_partial_order)
            .collect::<Vec<_>>(),
        vec![None, Some(c.order), Some(e.order)]
    );
    assert!(b.order < c.order);
}

#[test]
fn mirrors_mix_large_small_insertions_test_only_small_insert_files_are_merged() {
    // Mirrors: third_party/ducklake/test/sql/compaction/mix_large_small_insertions.test
    //
    // Storage contract:
    // - DuckLake may merge only the small inserted files and leave large files separate.
    // - The merged small-file replacement keeps the row-id range supplied by DuckLake.
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
        vec![
            data_file(1, table, 0, 1),
            data_file_with_size(2, table, 1, 10_000, 40_000),
            data_file(3, table, 10_001, 1),
            data_file(4, table, 10_002, 1),
            data_file_with_size(5, table, 10_003, 10_000, 40_000),
            data_file(6, table, 20_003, 1),
            data_file(7, table, 20_004, 1),
        ],
    )
    .unwrap();
    let small_merge_partial = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
    merge(
        &mut kv,
        catalog,
        &[1, 3, 4, 6, 7],
        data_file(8, table, 0, 5).with_max_partial_order(Some(small_merge_partial)),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, table, &[2, 5, 8]);
}

#[test]
fn mirrors_repro_merge_adjacent_zero_output_test_empty_source_files_can_be_expired_without_replacement()
 {
    // Mirrors: third_party/ducklake/test/sql/compaction/repro_merge_adjacent_zero_output.test
    //
    // Storage contract:
    // - DuckLake may compact several empty files into zero output files.
    // - Storage should be able to expire the source files without requiring a replacement file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        (1..=4).map(|id| data_file(id, table, id - 1, 0)).collect(),
    )
    .unwrap();

    let result = commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3), DataFileId(4)],
            new_files: Vec::new(),
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );
    assert!(
        result.is_ok(),
        "empty-source compaction should allow zero output files"
    );
}

#[test]
fn mirrors_rewrite_deletes_full_file_delete_after_flush_test_full_delete_rewrite_can_create_no_files()
 {
    // Mirrors: third_party/ducklake/test/sql/compaction/rewrite_deletes_full_file_delete_after_flush.test
    //
    // Storage contract:
    // - Flush can materialize a fully deleted inline insertion as one data file plus one delete
    //   file covering every row.
    // - Rewrite-delete compaction may replace that pair with no current data file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "id", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 20)]).unwrap();
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

    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: Vec::new(),
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    assert_current_file_ids(&kv, catalog, table, &[]);
}

#[test]
fn mirrors_small_insert_compaction_test_unrelated_table_insert_does_not_break_time_travel() {
    // Mirrors: third_party/ducklake/test/sql/compaction/small_insert_compaction.test
    //
    // Storage contract:
    // - Compaction of one table's small inserts should ignore unrelated table writes between those
    //   inserts.
    // - Historical reads for snapshots before compaction resolve through the replacement file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let test = TableId(1);
    let test2 = TableId(2);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        test,
        "test",
        vec![column(1, "i", "integer")],
    );
    create_table(
        &mut kv,
        catalog,
        test2,
        "test2",
        vec![column(1, "i", "integer")],
    );
    append_one_file(&mut kv, catalog, test, 1, 0);
    let s1 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    append_one_file(&mut kv, catalog, test2, 20, 0);
    append_one_file(&mut kv, catalog, test, 2, 1);
    let s2 = latest_snapshot(&kv, catalog).unwrap().unwrap();
    append_one_file(&mut kv, catalog, test, 3, 2);
    append_one_file(&mut kv, catalog, test, 4, 3);
    append_one_file(&mut kv, catalog, test, 5, 4);

    merge(
        &mut kv,
        catalog,
        &[1, 2, 3, 4, 5],
        data_file(6, test, 0, 5),
        Vec::new(),
    );
    assert_current_file_ids(&kv, catalog, test, &[6]);
    assert_file_ids_at(&kv, catalog, test, s1.order, &[6]);
    assert_file_ids_at(&kv, catalog, test, s2.order, &[6]);
    assert_current_file_ids(&kv, catalog, test2, &[20]);
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog).unwrap().len(),
        5
    );

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let expirable = list_snapshots(&kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.order < latest.order)
        .map(|snapshot| snapshot.sequence)
        .collect::<Vec<_>>();
    expire_snapshots(&mut kv, catalog, &expirable).unwrap();

    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog).unwrap().len(),
        5
    );
}

fn create_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    name: &str,
    columns: Vec<TableColumnRow>,
) -> TableRow {
    create_table_in_schema(kv, catalog, table, SchemaId(1), name, columns)
}

fn create_table_in_schema(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    name: &str,
    columns: Vec<TableColumnRow>,
) -> TableRow {
    commit_create_table_row(
        kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            schema,
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

fn append_one_file(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    id: u64,
    row_id_start: u64,
) {
    commit_append_data_files(kv, catalog, vec![data_file(id, table, row_id_start, 1)]).unwrap();
}

fn append_partitioned_files(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    files: &[(u64, &str)],
) {
    commit_data_mutation_with_file_partitions(
        kv,
        catalog,
        files
            .iter()
            .enumerate()
            .map(|(index, (id, _))| data_file(*id, table, index as u64, 1))
            .collect(),
        Vec::new(),
        &[],
        files
            .iter()
            .map(|(id, value)| partition_value(*id, table, 0, value))
            .collect(),
    )
    .unwrap();
}

fn create_file_size_filter_table(catalog: CatalogId, table: TableId) -> FakeOrderedCatalogKv {
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "example",
        vec![column(1, "key", "integer")],
    );
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file_with_size(1, table, 0, 10, 400),
            data_file_with_size(2, table, 10, 10, 400),
            data_file_with_size(3, table, 100, 100, 1_800),
            data_file_with_size(4, table, 200, 100, 1_800),
        ],
    )
    .unwrap();
    kv
}

fn merge(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    new_file: DataFileRow,
    partition_values: Vec<FilePartitionValueRow>,
) {
    commit_merge_adjacent_data_files(
        kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: source_ids.iter().copied().map(DataFileId).collect(),
            new_files: vec![new_file],
            partition_values,
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    data_file_with_size(id, table, row_id_start, record_count, 1024)
}

fn data_file_without_row_ids(id: u64, table: TableId, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/table-{}/file-{id}.parquet", table.0),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
}

fn data_file_with_size(
    id: u64,
    table: TableId,
    row_id_start: u64,
    record_count: u64,
    size: u64,
) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/table-{}/file-{id}.parquet", table.0),
        record_count,
        size,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
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

fn assert_current_file_ids(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    expected: &[u64],
) {
    let ids = list_current_data_files_with_deletes(kv, catalog, table)
        .unwrap()
        .into_iter()
        .map(|file| file.data_file.data_file_id.0)
        .collect::<Vec<_>>();
    assert_eq!(ids, expected);
}

fn assert_file_ids_at(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    order: CatalogOrderId,
    expected: &[u64],
) {
    let ids = list_data_files_at(kv, catalog, table, order)
        .unwrap()
        .into_iter()
        .map(|file| file.data_file_id.0)
        .collect::<Vec<_>>();
    assert_eq!(ids, expected);
}

fn assert_partition_files(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    partition_value: &str,
    expected: &[u64],
) {
    let ids = list_current_data_files_by_partition_value(
        kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        partition_value,
    )
    .unwrap()
    .into_iter()
    .map(|file| file.data_file_id.0)
    .collect::<Vec<_>>();
    assert_eq!(ids, expected);
    let stored_partition_values = expected
        .iter()
        .flat_map(|id| partition_values_for_file(kv, catalog, DataFileId(*id)))
        .map(|row| row.partition_value)
        .collect::<Vec<_>>();
    assert_eq!(
        stored_partition_values,
        vec![partition_value; expected.len()]
    );
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

fn drain_cleanup(kv: &mut FakeOrderedCatalogKv, catalog: CatalogId) {
    let old_data = list_old_data_files_for_cleanup(kv, catalog).unwrap();
    let old_delete = list_old_delete_files_for_cleanup(kv, catalog).unwrap();
    remove_old_data_files(
        kv,
        catalog,
        &old_data
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    remove_old_delete_files(
        kv,
        catalog,
        &old_delete
            .iter()
            .map(|row| row.delete_file.delete_file_id)
            .collect::<Vec<_>>(),
    )
    .unwrap();
}
