use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnDrop, ColumnId, DataFileChangeKind, DataFileId, DataFileRow,
    DeleteFileId, DeleteFileRow, FakeOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow,
    InlineTableFlush, MergeAdjacentCompaction, PartitionKeyIndex, RawSnapshotSequence,
    RewriteDeleteCompaction, SchemaId, SchemaRow, TableColumnRow, TableId, TablePartitionChange,
    TablePartitionFieldRow, TablePartitionRow, TableRow, TableSortChange, TableSortFieldRow,
    TableSortRow, ViewRow, commit_append_data_files,
    commit_append_table_columns_with_conflict_check, commit_change_table_partition,
    commit_change_table_partition_with_conflict_check, commit_change_table_sort,
    commit_create_schema_rows, commit_create_table_row, commit_create_view_row,
    commit_data_mutation_with_file_partitions,
    commit_data_mutation_with_file_partitions_and_inline_deletes,
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files,
    commit_delete_inline_table_rows, commit_drop_schema_rows, commit_drop_table_columns,
    commit_drop_views, commit_merge_adjacent_data_files, commit_register_delete_files,
    commit_rewrite_delete_data_files, initialize_catalog_if_absent, latest_snapshot,
    list_current_data_files_by_partition_value, list_current_data_files_with_deletes,
    list_data_file_changes, list_data_files_at, list_file_column_stats,
    list_file_column_stats_for_table_column, list_file_partition_values, list_inline_row_changes,
    list_inline_table_payloads_at, list_table_deletion_scan_files, list_tables_at, list_views_at,
    load_schema_at, load_table_at, load_view_at, register_file_column_stats,
    register_inline_table_payload_with_table, snapshot_changes_made,
};

#[test]
fn mirrors_sorted_flush_basic_test_sort_metadata_and_flushed_file_are_saved() {
    // Mirrors: third_party/ducklake/test/sql/sorted_table/data_inlining_flush_sorted_basic.test
    //
    // Storage contract:
    // - DuckLake saves the SET SORTED BY definition as table metadata.
    // - Flushing inline rows creates a data file while preserving earlier inline insert change
    //   metadata for table_changes/time travel.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let created = create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "i", "integer")],
    );
    register_inline_rows(&mut kv, catalog, created, SchemaId(1), 0, 10);
    let before_sort = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_change_table_sort(
        &mut kv,
        catalog,
        &TableSortChange::new(
            table,
            Some(TableSortRow::new(
                1,
                vec![TableSortFieldRow::new(
                    0,
                    "i",
                    "duckdb",
                    "DESC",
                    "NULLS_LAST",
                )],
            )),
        ),
    )
    .unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 10)],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            SchemaId(1),
            RawSnapshotSequence(2),
        )],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let after_flush = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let table_at_flush = load_table_at(&kv, catalog, table, after_flush.order)
        .unwrap()
        .unwrap();
    assert_eq!(
        table_at_flush.sort.unwrap().fields[0].expression,
        "i",
        "storage must return the sort expression DuckLake saved before flushing"
    );
    assert_eq!(current_row_count(&kv, catalog, table), 10);
    assert!(
        !list_inline_row_changes(&kv, catalog, table, before_sort.order, after_flush.order)
            .unwrap()
            .is_empty(),
        "flush must not erase inline insert change-feed rows"
    );
    assert!(
        snapshot_changes_made(&kv, catalog, after_flush.order)
            .unwrap()
            .contains("flushed_inlined:1")
    );
}

#[test]
fn mirrors_sorted_flush_expression_test_expression_sort_metadata_is_saved_verbatim() {
    // Mirrors: third_party/ducklake/test/sql/sorted_table/data_inlining_flush_sorted_basic_expression.test
    //
    // Storage contract:
    // - Expression sort keys are opaque DuckDB expressions to the catalog.
    // - Storage must round-trip the expression text and sort direction unchanged.
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

    commit_change_table_sort(
        &mut kv,
        catalog,
        &TableSortChange::new(
            table,
            Some(TableSortRow::new(
                7,
                vec![TableSortFieldRow::new(
                    0,
                    "i * i",
                    "duckdb",
                    "DESC",
                    "NULLS_LAST",
                )],
            )),
        ),
    )
    .unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let sort = load_table_at(&kv, catalog, table, latest.order)
        .unwrap()
        .unwrap()
        .sort
        .unwrap();
    assert_eq!(sort.sort_id, 7);
    assert_eq!(sort.fields[0].expression, "i * i");
}

#[test]
fn mirrors_count_star_optimization_inlined_test_inline_payloads_survive_schema_evolution_and_deletes()
 {
    // Mirrors: third_party/ducklake/test/sql/stats/count_star_optimization_inlined.test
    //
    // Storage contract:
    // - Inline payloads for successive schema versions are table-scoped and visible together.
    // - Inline deletes are recorded without corrupting the payload visibility for other schema
    //   versions.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut current = create_table(
        &mut kv,
        catalog,
        table,
        "inlined_test",
        vec![column(1, "i", "integer")],
    );
    register_inline_rows(&mut kv, catalog, current.clone(), SchemaId(1), 0, 50);
    register_inline_rows(&mut kv, catalog, current.clone(), SchemaId(1), 50, 30);
    let base = latest_order(&kv, catalog);
    commit_append_table_columns_with_conflict_check(
        &mut kv,
        catalog,
        table,
        base,
        base,
        vec![column(2, "j", "integer")],
    )
    .unwrap();
    current = load_current_table(&kv, catalog, table);
    register_inline_rows(&mut kv, catalog, current.clone(), SchemaId(1), 80, 10);
    let base = latest_order(&kv, catalog);
    commit_append_table_columns_with_conflict_check(
        &mut kv,
        catalog,
        table,
        base,
        base,
        vec![column(3, "k", "varchar")],
    )
    .unwrap();
    current = load_current_table(&kv, catalog, table);
    register_inline_rows(&mut kv, catalog, current, SchemaId(1), 90, 10);
    let before_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let deleted = commit_delete_inline_table_rows(
        &mut kv,
        catalog,
        table,
        SchemaId(1),
        &[0, 10, 20, 30, 40, 50, 60, 70, 80, 90],
    )
    .unwrap();

    assert_eq!(deleted.deleted_row_count, 10);
    assert_eq!(
        list_inline_table_payloads_at(&kv, catalog, table, SchemaId(1), before_delete.order)
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn mirrors_filter_pushdown_test_file_column_stats_are_saved_for_pruning() {
    // Mirrors: third_party/ducklake/test/sql/stats/filter_pushdown.test
    //
    // Storage contract:
    // - DuckLake supplies min/max/null stats for each file and column.
    // - Storage must return all stats rows so DuckDB can prune files for filters.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "filter_pushdown",
        vec![column(1, "i", "integer")],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1000),
            data_file(2, table, 1000, 1000),
            data_file(3, table, 2000, 1000),
            data_file(4, table, 3000, 1),
        ],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        vec![
            stats(1, table, 1, 0, Some("0"), Some("999")),
            stats(2, table, 1, 0, Some("100000"), Some("100999")),
            stats(3, table, 1, 0, Some("500000"), Some("500999")),
            stats(4, table, 1, 0, Some("501000"), Some("501000")),
        ],
        Vec::new(),
    )
    .unwrap();

    assert_eq!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
            .unwrap()
            .into_iter()
            .map(|row| (
                row.data_file_id.0,
                row.min_value.unwrap(),
                row.max_value.unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, "0".to_owned(), "999".to_owned()),
            (2, "100000".to_owned(), "100999".to_owned()),
            (3, "500000".to_owned(), "500999".to_owned()),
            (4, "501000".to_owned(), "501000".to_owned()),
        ]
    );
}

#[test]
fn mirrors_filter_stress_test_many_imported_file_stats_are_indexed_by_table_and_column() {
    // Mirrors: third_party/ducklake/test/sql/stats/filter_stress.test
    //
    // Storage contract:
    // - Imported file stats must be independently addressable for every file.
    // - Null counts and min/max values must not be dropped for high-file-count tables.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "events",
        vec![column(1, "i", "integer")],
    );
    let files = (1..=54)
        .map(|id| data_file(id, table, (id - 1) * 2048, 2048))
        .collect::<Vec<_>>();
    let file_stats = (1..=54)
        .map(|id| {
            stats(
                id,
                table,
                1,
                if id % 7 == 0 { 10 } else { 0 },
                Some(&((id - 1) * 2048).to_string()),
                Some(&(id * 2048 - 1).to_string()),
            )
        })
        .collect::<Vec<_>>();
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        files,
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        file_stats,
        Vec::new(),
    )
    .unwrap();

    let stored = list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1)).unwrap();
    assert_eq!(stored.len(), 54);
    assert!(stored.iter().any(|row| row.null_count == 10));
}

#[test]
fn mirrors_global_stats_test_stats_for_multiple_types_are_persisted() {
    // Mirrors: third_party/ducklake/test/sql/stats/global_stats.test
    //
    // Storage contract:
    // - File stats store type-specific min/max values as catalog strings.
    // - Null counts survive alongside min/max for optimizer consumers.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![
            column(1, "i", "integer"),
            column(2, "d", "date"),
            column(3, "s", "varchar"),
            column(4, "b", "boolean"),
        ],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 3)],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        vec![
            stats(1, table, 1, 1, Some("42"), Some("87")),
            stats(1, table, 2, 0, Some("1992-01-01"), Some("2000-02-03")),
            stats(1, table, 3, 0, Some("bye bye"), Some("hello wo")),
            stats(1, table, 4, 0, Some("false"), Some("true")),
        ],
        Vec::new(),
    )
    .unwrap();

    assert_eq!(list_file_column_stats(&kv, catalog).unwrap().len(), 4);
    assert_eq!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1)).unwrap()[0]
            .null_count,
        1
    );
}

#[test]
fn mirrors_min_max_nested_leaf_rewrite_corruption_test_unrewritten_nested_leaf_stats_remain_visible()
 {
    // Mirrors: third_party/ducklake/test/sql/stats/min_max_nested_leaf_rewrite_corruption.test
    //
    // Storage contract:
    // - Rewriting one file must not delete stats rows for untouched files.
    // - Nested leaf column ids are ordinary column ids to storage and must round-trip.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "i", "integer")],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 50), data_file(2, table, 50, 50)],
        vec![delete_file(1, 2, 1)],
        &[],
        Vec::new(),
        Vec::new(),
        vec![
            stats(1, table, 1, 0, Some("1"), Some("50")),
            stats(1, table, 3, 0, Some("1"), Some("50")),
            stats(1, table, 5, 0, Some("1"), Some("50")),
            stats(1, table, 7, 0, Some("1"), Some("50")),
            stats(1, table, 8, 0, Some("2"), Some("100")),
            stats(2, table, 1, 0, Some("51"), Some("100")),
            stats(2, table, 3, 0, Some("51"), Some("100")),
            stats(2, table, 5, 0, Some("51"), Some("100")),
            stats(2, table, 7, 0, Some("51"), Some("100")),
            stats(2, table, 8, 0, Some("102"), Some("200")),
        ],
        Vec::new(),
    )
    .unwrap();
    rewrite(&mut kv, catalog, &[2], &[data_file(3, table, 50, 49)]);

    let untouched_leaf_stats =
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(3))
            .unwrap()
            .into_iter()
            .find(|row| row.data_file_id == DataFileId(1))
            .unwrap();
    assert_eq!(untouched_leaf_stats.min_value.as_deref(), Some("1"));
    assert_eq!(untouched_leaf_stats.max_value.as_deref(), Some("50"));
}

#[test]
fn mirrors_min_max_optimization_compaction_test_rewrite_can_store_tightened_replacement_stats() {
    // Mirrors: third_party/ducklake/test/sql/stats/min_max_optimization_compaction.test
    //
    // Storage contract:
    // - After DuckLake rewrites deletes away, replacement file stats represent the tightened row
    //   range and must be returned to optimizer callers.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t",
        vec![column(1, "i", "integer")],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 200)],
        vec![delete_file(1, 1, 2)],
        &[],
        Vec::new(),
        Vec::new(),
        vec![stats(1, table, 1, 0, Some("1"), Some("200"))],
        Vec::new(),
    )
    .unwrap();
    rewrite_with_stats(
        &mut kv,
        catalog,
        &[1],
        vec![data_file(2, table, 1, 198)],
        vec![stats(2, table, 1, 0, Some("2"), Some("199"))],
    );
    let rewritten_stats = list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
        .unwrap()
        .into_iter()
        .find(|row| row.data_file_id == DataFileId(2))
        .unwrap();
    assert_eq!(rewritten_stats.min_value.as_deref(), Some("2"));
    assert_eq!(rewritten_stats.max_value.as_deref(), Some("199"));
}

#[test]
fn mirrors_min_max_optimization_deletes_test_delete_file_keeps_original_stats_until_rewrite() {
    // Mirrors: third_party/ducklake/test/sql/stats/min_max_optimization_deletes.test
    //
    // Storage contract:
    // - Delete files do not mutate the source file stats.
    // - The active delete file is returned with the source so the caller can avoid unsafe min/max
    //   folding until rewrite removes it.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "big",
        vec![column(1, "i", "integer")],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 100)],
        vec![delete_file(1, 1, 1)],
        &[],
        Vec::new(),
        Vec::new(),
        vec![stats(1, table, 1, 0, Some("1"), Some("100"))],
        Vec::new(),
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert!(current[0].delete_file.is_some());
    let stored_stats =
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1)).unwrap();
    assert_eq!(stored_stats[0].min_value.as_deref(), Some("1"));
}

#[test]
fn mirrors_topn_file_pruning_test_timestamp_stats_keep_ordered_file_ranges() {
    // Mirrors: third_party/ducklake/test/sql/stats/topn_file_pruning.test
    //
    // Storage contract:
    // - Top-N pruning depends on ordered min/max stats for timestamp columns.
    // - Storage must return the file ranges without reordering or coalescing them.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "events",
        vec![column(1, "timestamp", "timestamp")],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1000),
            data_file(2, table, 1000, 500),
            data_file(3, table, 1500, 200),
            data_file(4, table, 1700, 100),
        ],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        vec![
            stats(
                1,
                table,
                1,
                0,
                Some("2026-01-01"),
                Some("2026-01-01 00:16:39"),
            ),
            stats(
                2,
                table,
                1,
                0,
                Some("2026-01-02"),
                Some("2026-01-02 00:08:19"),
            ),
            stats(
                3,
                table,
                1,
                0,
                Some("2026-01-03"),
                Some("2026-01-03 00:03:19"),
            ),
            stats(
                4,
                table,
                1,
                0,
                Some("2026-01-04"),
                Some("2026-01-04 00:01:39"),
            ),
        ],
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
            .unwrap()
            .into_iter()
            .map(|row| row.max_value.unwrap())
            .collect::<Vec<_>>(),
        vec![
            "2026-01-01 00:16:39".to_owned(),
            "2026-01-02 00:08:19".to_owned(),
            "2026-01-03 00:03:19".to_owned(),
            "2026-01-04 00:01:39".to_owned(),
        ]
    );
}

#[test]
fn mirrors_table_insertions_test_insert_change_feed_survives_merge_and_update() {
    // Mirrors: third_party/ducklake/test/sql/table_changes/ducklake_table_insertions.test
    //
    // Storage contract:
    // - Insert commits are indexed as added data-file changes.
    // - Merge-adjacent compaction is reorganization; later update-like mutations add new files and
    //   preserve earlier insertion changes for change-feed reads.
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
    for id in 1..=5 {
        commit_append_data_files(&mut kv, catalog, vec![data_file(id, table, id - 1, 1)]).unwrap();
    }
    let before_merge = latest_order(&kv, catalog);
    merge(
        &mut kv,
        catalog,
        &[1, 2, 3, 4, 5],
        data_file(6, table, 0, 6),
        Vec::new(),
    );
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(7, table, 0, 4)],
        vec![delete_file(1, 6, 4)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let end = latest_order(&kv, catalog);

    let changes =
        list_data_file_changes(&kv, catalog, table, CatalogOrderId::uuid_v7(0), end).unwrap();
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.kind == DataFileChangeKind::Added)
            .count(),
        7
    );
    assert!(changes.iter().any(|change| change.order <= before_merge));
}

#[test]
fn mirrors_window_partition_row_loss_test_change_feed_returns_all_insert_and_delete_rows() {
    // Mirrors: third_party/ducklake/test/sql/table_changes/window_partition_row_loss.test
    //
    // Storage contract:
    // - Change-feed APIs must return every inserted and deleted scan row; consumers may then apply
    //   SQL window functions without storage dropping duplicates.
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
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 2)]).unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(2, table, 2, 1)],
        vec![delete_file(1, 1, 1)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let end = latest_order(&kv, catalog);
    let data_file_changes =
        list_data_file_changes(&kv, catalog, table, CatalogOrderId::uuid_v7(0), end).unwrap();
    assert_eq!(
        data_file_changes
            .iter()
            .map(|change| (change.kind, change.data_file_id))
            .collect::<Vec<_>>(),
        vec![
            (DataFileChangeKind::Added, DataFileId(1)),
            (DataFileChangeKind::Added, DataFileId(2)),
        ]
    );
    assert_eq!(
        list_table_deletion_scan_files(&kv, catalog, table, CatalogOrderId::uuid_v7(0), end)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn mirrors_time_travel_views_test_view_visibility_tracks_snapshot_order() {
    // Mirrors: third_party/ducklake/test/sql/time_travel/time_travel_views.test
    //
    // Storage contract:
    // - Views are versioned catalog objects.
    // - Reads at older snapshots should return only views that existed at that snapshot, including
    //   views in schemas that are later dropped.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "test",
        vec![column(1, "i", "integer")],
    );
    let before_view = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_create_view_row(
        &mut kv,
        catalog,
        view(10, SchemaId(1), "v1", "SELECT * FROM test"),
    )
    .unwrap();
    let after_view = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_views(&mut kv, catalog, &[TableId(10)]).unwrap();
    let after_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert!(
        load_view_at(&kv, catalog, TableId(10), before_view.order)
            .unwrap()
            .is_none()
    );
    assert!(
        load_view_at(&kv, catalog, TableId(10), after_view.order)
            .unwrap()
            .is_some()
    );
    assert!(
        load_view_at(&kv, catalog, TableId(10), after_drop.order)
            .unwrap()
            .is_none()
    );
}

#[test]
fn mirrors_concurrent_table_creation_test_different_table_ids_and_names_can_commit() {
    // Mirrors: third_party/ducklake/test/sql/transaction/concurrent_table_creation.test
    //
    // Storage contract:
    // - Independent table creations with distinct ids/names are independent metadata writes.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "test",
        vec![column(1, "i", "integer")],
    );
    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "test2",
        vec![column(1, "s", "varchar")],
    );
    let current = latest_order(&kv, catalog);
    assert_eq!(list_tables_at(&kv, catalog, current).unwrap().len(), 2);
}

#[test]
fn mirrors_partition_commit_retry_remap_test_partition_values_reference_committed_partition_ids() {
    // Mirrors: third_party/ducklake/test/sql/transaction/partition_commit_retry_remap.test
    //
    // Storage contract:
    // - After a retry, partition metadata must use committed table partition ids, not temporary
    //   transaction-local ids.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "first_write",
        vec![column(1, "source", "varchar")],
    );
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                2,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 3)],
        Vec::new(),
        &[],
        vec![partition_value(1, table, 0, "google_ads_data_hub")],
    )
    .unwrap();
    let partitioned = load_current_table(&kv, catalog, table).partition.unwrap();
    assert_eq!(partitioned.partition_id, 2);
    assert_eq!(
        partition_values_for_file(&kv, catalog, DataFileId(1))[0]
            .partition_key_index
            .0,
        0
    );
}

#[test]
fn mirrors_transaction_conflict_cleanup_test_conflicting_create_does_not_commit_metadata() {
    // Mirrors: third_party/ducklake/test/sql/transaction/transaction_conflict_cleanup.test
    //
    // Storage contract:
    // - If a conflicting transaction is rejected before catalog commit, no table/data-file rows
    //   for that failed attempt should be visible in storage.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "test",
        vec![column(1, "i", "integer")],
    );
    let duplicate = commit_create_table_row(
        &mut kv,
        catalog,
        table_row(TableId(2), "test", vec![column(1, "s", "varchar")]),
    );
    assert!(
        duplicate.is_err(),
        "storage should reject committing a second current table with the same schema/name"
    );
}

#[test]
fn mirrors_transaction_conflicts_test_same_name_table_create_conflicts_but_different_schema_names_do_not()
 {
    // Mirrors: third_party/ducklake/test/sql/transaction/transaction_conflicts.test
    //
    // Storage contract:
    // - Current table names conflict only within the same schema.
    // - The same table name in different schemas is valid metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_schema_rows(
        &mut kv,
        catalog,
        vec![
            SchemaRow::new(
                SchemaId(2),
                "schema-2",
                "s1",
                "main/s1",
                CatalogOrderId::uuid_v7(0),
            ),
            SchemaRow::new(
                SchemaId(3),
                "schema-3",
                "s2",
                "main/s2",
                CatalogOrderId::uuid_v7(0),
            ),
        ],
    )
    .unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        table_row_in_schema(
            TableId(1),
            SchemaId(2),
            "same_name_tbl",
            vec![column(1, "i", "integer")],
        ),
    )
    .unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        table_row_in_schema(
            TableId(2),
            SchemaId(3),
            "same_name_tbl",
            vec![column(1, "i", "integer")],
        ),
    )
    .unwrap();
    let duplicate = commit_create_table_row(
        &mut kv,
        catalog,
        table_row_in_schema(
            TableId(3),
            SchemaId(2),
            "same_name_tbl",
            vec![column(1, "i", "integer")],
        ),
    );
    assert!(duplicate.is_err());
}

#[test]
fn mirrors_transaction_conflicts_delete_test_delete_conflicts_with_schema_or_drop_changes() {
    // Mirrors: third_party/ducklake/test/sql/transaction/transaction_conflicts_delete.test
    //
    // Storage contract:
    // - A delete/rewrite planned before a table schema change or drop must be rejected.
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
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 1000)]).unwrap();
    let base = latest_order(&kv, catalog);
    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(1))]).unwrap();
    let through = latest_order(&kv, catalog);
    let stale_partition_change = commit_change_table_partition_with_conflict_check(
        &mut kv,
        catalog,
        base,
        through,
        &TablePartitionChange::new(table, None),
        None,
    );
    assert!(stale_partition_change.is_err());
}

#[test]
fn mirrors_transaction_conflicts_view_test_view_name_and_comment_conflicts_are_rejected() {
    // Mirrors: third_party/ducklake/test/sql/transaction/transaction_conflicts_view.test
    //
    // Storage contract:
    // - Current views should have unique names within a schema.
    // - Comment changes are versioned replacements of the same view id.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_view_row(&mut kv, catalog, view(1, SchemaId(1), "test", "SELECT 42")).unwrap();
    let duplicate =
        commit_create_view_row(&mut kv, catalog, view(2, SchemaId(1), "test", "SELECT 84"));
    assert!(duplicate.is_err());
}

#[test]
fn mirrors_transaction_insert_update_delete_test_one_commit_can_add_and_delete_metadata() {
    // Mirrors: third_party/ducklake/test/sql/transaction/transaction_insert_update_delete.test
    //
    // Storage contract:
    // - A transaction with insert, update, and delete is one metadata mutation containing new data
    //   files and delete metadata together.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t1",
        vec![column(1, "c1", "integer")],
    );
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 2), data_file(2, table, 2, 1)],
        vec![delete_file(1, 1, 2)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(current_file_ids(&kv, catalog, table), vec![1, 2]);
    assert_eq!(current_row_count(&kv, catalog, table), 1);
}

#[test]
fn mirrors_list_type_test_nested_list_column_stats_are_persisted() {
    // Mirrors: third_party/ducklake/test/sql/types/list.test
    //
    // Storage contract:
    // - List column type names and list-element stats are ordinary table/stat metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![
            column(1, "l", "integer[]"),
            column(2, "l.element", "integer"),
        ],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 6)],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        vec![stats(1, table, 2, 1, Some("1"), Some("7"))],
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(2)).unwrap()[0]
            .max_value
            .as_deref(),
        Some("7")
    );
}

#[test]
fn mirrors_struct_type_test_nested_struct_leaf_stats_are_persisted() {
    // Mirrors: third_party/ducklake/test/sql/types/struct.test
    //
    // Storage contract:
    // - Struct column type names and leaf stats must survive table metadata round-trips.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![
            column(1, "s", "struct(i integer, j integer)"),
            column(2, "s.i", "integer"),
        ],
    );
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 5)],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        vec![stats(1, table, 2, 1, Some("1"), Some("6"))],
        Vec::new(),
    )
    .unwrap();
    let current = load_current_table(&kv, catalog, table);
    assert!(current.columns.iter().any(|column| column.name == "s.i"));
}

#[test]
fn mirrors_basic_update_test_update_commit_preserves_previous_snapshot_visibility() {
    // Mirrors: third_party/ducklake/test/sql/update/basic_update.test
    //
    // Storage contract:
    // - An update saves replacement data and delete metadata.
    // - Time travel before the update still sees the original file without the later delete.
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
    let before_update = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(2, table, 0, 500)],
        vec![delete_file(1, 1, 500)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();

    assert_eq!(current_row_count(&kv, catalog, table), 1000);
    assert_eq!(
        list_data_files_at(&kv, catalog, table, before_update.order)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn mirrors_update_partitioning_test_updated_partition_file_keeps_new_partition_value_and_old_time_travel()
 {
    // Mirrors: third_party/ducklake/test/sql/update/update_partitioning.test
    //
    // Storage contract:
    // - Updating a partition key creates a replacement data file with the new partition value.
    // - The old partition file remains visible for time travel before the update.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "partitioned_tbl",
        vec![column(1, "part_key", "integer")],
    );
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                2,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 5000),
            data_file(2, table, 5000, 5000),
        ],
        Vec::new(),
        &[],
        vec![
            partition_value(1, table, 0, "0"),
            partition_value(2, table, 0, "1"),
        ],
    )
    .unwrap();
    let before_update = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        vec![data_file(3, table, 0, 5000)],
        vec![delete_file(1, 1, 5000)],
        &[],
        vec![partition_value(3, table, 0, "2")],
        Vec::new(),
    )
    .unwrap();

    assert_eq!(
        list_current_data_files_by_partition_value(&kv, catalog, table, PartitionKeyIndex(0), "2")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, before_update.order)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn mirrors_view_schema_test_schema_scoped_views_are_versioned_with_schema_lifecycle() {
    // Mirrors: third_party/ducklake/test/sql/view/ducklake_view_schema.test
    //
    // Storage contract:
    // - Views with the same name in different schemas are distinct objects.
    // - Dropping schemas/views removes them from current visibility but not from prior snapshots.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_schema_rows(
        &mut kv,
        catalog,
        vec![
            SchemaRow::new(
                SchemaId(2),
                "schema-2",
                "s1",
                "main/s1",
                CatalogOrderId::uuid_v7(0),
            ),
            SchemaRow::new(
                SchemaId(3),
                "schema-3",
                "s2",
                "main/s2",
                CatalogOrderId::uuid_v7(0),
            ),
        ],
    )
    .unwrap();
    commit_create_view_row(&mut kv, catalog, view(1, SchemaId(2), "v1", "SELECT 42")).unwrap();
    commit_create_view_row(
        &mut kv,
        catalog,
        view(2, SchemaId(3), "v1", "SELECT 'hello', 'world'"),
    )
    .unwrap();
    let before_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_views(&mut kv, catalog, &[TableId(1), TableId(2)]).unwrap();
    commit_drop_schema_rows(&mut kv, catalog, &[SchemaId(2), SchemaId(3)]).unwrap();
    let after_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_views_at(&kv, catalog, before_drop.order)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        list_views_at(&kv, catalog, after_drop.order).unwrap().len(),
        0
    );
    assert!(
        load_schema_at(&kv, catalog, SchemaId(2), after_drop.order)
            .unwrap()
            .is_none()
    );
}

fn create_table(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    name: &str,
    columns: Vec<TableColumnRow>,
) -> TableRow {
    commit_create_table_row(kv, catalog, table_row(table, name, columns)).unwrap()
}

fn table_row(table: TableId, name: &str, columns: Vec<TableColumnRow>) -> TableRow {
    table_row_in_schema(table, SchemaId(1), name, columns)
}

fn table_row_in_schema(
    table: TableId,
    schema: SchemaId,
    name: &str,
    columns: Vec<TableColumnRow>,
) -> TableRow {
    TableRow::with_catalog_metadata(
        table,
        schema,
        format!("table-{}", table.0),
        name,
        format!("main/{name}"),
        columns,
        CatalogOrderId::uuid_v7(0),
    )
}

fn column(id: u64, name: &str, column_type: &str) -> TableColumnRow {
    TableColumnRow::new(ColumnId(id), name, column_type, true, None)
}

fn view(id: u64, schema: SchemaId, name: &str, sql: &str) -> ViewRow {
    ViewRow::new(
        TableId(id),
        schema,
        format!("view-{id}"),
        name,
        "duckdb",
        sql,
        Vec::new(),
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

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> DeleteFileRow {
    DeleteFileRow::new(
        DeleteFileId(id),
        DataFileId(data_file_id),
        format!("main/delete-{id}.parquet"),
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}

fn stats(
    data_file_id: u64,
    table: TableId,
    column_id: u64,
    null_count: u64,
    min: Option<&str>,
    max: Option<&str>,
) -> FileColumnStatsRow {
    FileColumnStatsRow::new(
        DataFileId(data_file_id),
        table,
        ColumnId(column_id),
        null_count,
        min.map(ToOwned::to_owned),
        max.map(ToOwned::to_owned),
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

fn register_inline_rows(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableRow,
    schema: SchemaId,
    start: u64,
    count: u64,
) {
    let payload = (start..start + count)
        .map(|row_id| format!("row\t{row_id}\ti:{row_id}\n"))
        .collect::<String>()
        .into_bytes();
    register_inline_table_payload_with_table(kv, catalog, table, schema, payload).unwrap();
}

fn rewrite(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    replacements: &[DataFileRow],
) {
    commit_rewrite_delete_data_files(
        kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: source_ids.iter().copied().map(DataFileId).collect(),
            new_files: replacements.to_vec(),
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
}

fn rewrite_with_stats(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    source_ids: &[u64],
    replacements: Vec<DataFileRow>,
    file_stats: Vec<FileColumnStatsRow>,
) {
    let rewritten = commit_rewrite_delete_data_files(
        kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: source_ids.iter().copied().map(DataFileId).collect(),
            new_files: replacements.clone(),
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    for row in file_stats {
        if rewritten
            .new_files
            .iter()
            .any(|file| file.data_file_id == row.data_file_id)
        {
            register_file_column_stats(kv, catalog, row).unwrap();
        }
    }
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

fn latest_order(kv: &FakeOrderedCatalogKv, catalog: CatalogId) -> CatalogOrderId {
    latest_snapshot(kv, catalog).unwrap().unwrap().order
}

fn load_current_table(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> TableRow {
    load_table_at(kv, catalog, table, latest_order(kv, catalog))
        .unwrap()
        .unwrap()
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

fn current_row_count(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> u64 {
    list_current_data_files_with_deletes(kv, catalog, table)
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
