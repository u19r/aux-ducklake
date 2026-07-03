use ducklake_catalog::{
    CatalogDebugRow, CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId,
    DeleteFileRow, DuckLakeSnapshotId, FakeOrderedCatalogKv, FileColumnStatsRow,
    FilePartitionValueRow, INLINE_PAYLOAD_LIMIT_BYTES, InlineFileDeletionRow, InlineTableFlush,
    InlineTablePayloadCommit, MacroId, MacroImplementationRow, MacroParameterRow, MacroRow,
    MergeAdjacentCompaction, OrderedCatalogKv, PartitionKeyIndex, RangeDirection,
    RawSnapshotSequence, RewriteDeleteCompaction, SchemaId, TableColumnRow, TableId,
    TablePartitionChange, TablePartitionFieldRow, TablePartitionRow, TableRow, TableSortChange,
    TableSortFieldRow, TableSortRow, append_data_file, commit_append_data_files,
    commit_append_table_columns, commit_change_table_partition, commit_change_table_sort,
    commit_create_macro_rows, commit_create_table_row, commit_data_mutation_with_file_partitions,
    commit_delete_inline_table_rows, commit_inline_file_deletions,
    commit_merge_adjacent_data_files, commit_register_delete_files,
    commit_rewrite_delete_data_files, expire_snapshots, initialize_catalog_if_absent,
    insertion_files,
    keys::{KeyFamily, family_prefix, inline_table_end_key},
    latest_snapshot, list_catalog_debug_rows, list_current_data_files,
    list_current_data_files_by_partition_value, list_data_file_changes, list_data_files_at,
    list_file_column_stats_for_table_column, list_file_partition_values,
    list_inline_row_payload_changes, list_macros_at, list_old_data_files_for_cleanup,
    list_snapshots, list_table_deletion_scan_files, load_inline_table_payload_at, load_table_at,
    public_snapshot_sequence_for_order, register_file_column_stats, register_file_partition_value,
    register_inline_table_payload, register_inline_table_payload_with_table, remove_old_data_files,
    route_inline_table_payload_or_data_file, snapshot_by_public_sequence,
};

// This file is a fast Rust translation queue for upstream SQLLogic failures. Each test names the
// storage contract directly: DuckLake asks the metadata storage to save X, then later asks it to
// return Y. Some tests intentionally fail until the Rust storage model owns the missing concept.

#[test]
fn given_external_file_footer_size_when_saving_data_file_then_footer_size_is_returnable() {
    // Upstream: test/sql/add_files/add_file_footer_size.test
    // Storage request: persist data-file metadata for an externally added Parquet file.
    // Required return: ducklake_list_files can return data_file_footer_size > 0.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/test/data.parquet",
                1,
                4096,
                CatalogOrderId::uuid_v7(0),
            )
            .with_footer_size(Some(512)),
        ],
    )
    .unwrap();

    let files = list_current_data_files(&kv, catalog, table).unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].footer_size, Some(512));
}

#[test]
fn given_hidden_metadata_debug_rows_when_stats_are_saved_then_stats_are_returned() {
    // Upstream: hidden metadata table access failures such as add_column_default_stats and stats/*.
    // Storage request: save table, column, data-file, and column-stats rows.
    // Required return: a metadata reader can see the stats row, not only the base file row.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 3)]).unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            DataFileId(1),
            table,
            ColumnId(2),
            0,
            Some("42".to_owned()),
            Some("42".to_owned()),
        )
        .with_value_count(Some(3)),
    )
    .unwrap();

    let debug_rows = list_catalog_debug_rows(&kv, catalog, 100).unwrap();
    let stats_rows =
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(2)).unwrap();

    assert_eq!(stats_rows.len(), 1);
    assert!(
        debug_rows.len() >= 5,
        "debug/hidden metadata rows omit saved file-column stats"
    );
}

#[test]
fn given_oversized_inline_rows_when_registering_inline_payload_then_storage_routes_away_from_fdb_values()
 {
    // Upstream: test/sql/data_inlining/data_inlining_partitions.test
    // Storage request: save many inline rows whose total encoded payload is over FDB's safe item
    // size, while each encoded row remains below the per-value limit.
    // Required return: metadata commit succeeds by chunking the payload; object/file fallback is
    // only required when one encoded row is too large to inline safely.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();

    let payload = small_inline_rows_payload(INLINE_PAYLOAD_LIMIT_BYTES + 1);
    let rows = register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        payload.clone(),
    )
    .expect("many small inline rows over 90KB total should be chunked, not rejected");

    assert!(rows.len() > 1);
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, rows[0].validity.begin_order)
            .unwrap(),
        Some(payload),
        "storage should return the chunked inline payload it was asked to save"
    );
}

#[test]
fn given_single_inline_row_over_fdb_limit_when_routing_then_storage_uses_file_backed_metadata() {
    // Upstream requirement: FoundationDB has a 100KB item limit.
    // Storage request: save one encoded inline row that exceeds the FDB-safe row ceiling.
    // Required return: direct inline storage rejects it, and the routing API writes supplied
    // file-backed metadata without leaving inline chunks.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let payload = oversized_inline_row_payload();
    let direct_error =
        register_inline_table_payload(&mut kv, catalog, table, schema, payload.clone())
            .unwrap_err();
    assert!(
        direct_error
            .to_string()
            .contains(&format!("over {INLINE_PAYLOAD_LIMIT_BYTES} byte limit")),
        "direct inline registration should reject a single oversized row before writing chunks"
    );
    let fallback = DataFileRow::new(
        DataFileId(501),
        table,
        "main/orders/inline-fallback-0001.parquet",
        20,
        (INLINE_PAYLOAD_LIMIT_BYTES + 1) as u64,
        CatalogOrderId::uuid_v7(0),
    );
    let routed =
        route_inline_table_payload_or_data_file(&mut kv, catalog, table, schema, payload, fallback)
            .expect("single oversized inline row should route through file-backed metadata");

    let InlineTablePayloadCommit::FileBacked(files) = routed else {
        panic!("single oversized inline row should route to file-backed metadata");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].data_file_id, DataFileId(501));
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        files,
        "storage should return the fallback file metadata after routing"
    );
    assert!(
        kv.scan_prefix(
            &family_prefix(catalog, KeyFamily::InlineTable),
            RangeDirection::Forward,
            usize::MAX,
        )
        .is_empty(),
        "oversized payload must not leave inline FDB chunks behind"
    );
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, latest.order).unwrap(),
        None
    );
}

#[test]
fn given_compacted_sources_when_listing_hidden_current_data_files_then_only_replacement_is_returned()
 {
    // Upstream: test/sql/compaction/merge_adjacent_max_files.test
    // Storage request: replace many source files with one compacted file.
    // Required return: current metadata readers should not report expired source rows as current.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
    for id in 0..20 {
        append_data_file(&mut kv, catalog, data_file(id, table, id * 10, 1)).unwrap();
    }

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: (0..20).map(DataFileId).collect(),
            new_files: vec![data_file(20, table, 0, 20)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let current = list_current_data_files(&kv, catalog, table).unwrap();
    let debug_data_files = list_catalog_debug_rows(&kv, catalog, 100)
        .unwrap()
        .into_iter()
        .filter(|row| matches!(row, CatalogDebugRow::DataFile(_)))
        .count();

    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(20)]
    );
    assert_eq!(
        debug_data_files, 1,
        "hidden current data-file metadata should not expose expired compacted sources"
    );
}

#[test]
fn given_compacted_sources_when_cleanup_all_runs_then_scheduled_files_are_returned_and_metadata_remains()
 {
    // Upstream: test/sql/add_files/add_files_compaction.test
    // Storage request: replace compacted source files and schedule their physical paths for cleanup.
    // Required return: cleanup_all lists the scheduled files immediately, then removing cleanup records
    // clears only the cleanup markers and preserves historical data-file metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
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
    let before_compaction = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3)],
            new_files: vec![data_file(4, table, 0, 3)],
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
        vec![DataFileId(1), DataFileId(2), DataFileId(3)]
    );

    assert_eq!(
        remove_old_data_files(
            &mut kv,
            catalog,
            &[DataFileId(1), DataFileId(2), DataFileId(3)]
        )
        .unwrap()
        .len(),
        3
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, before_compaction.order)
            .unwrap()
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(1), DataFileId(2), DataFileId(3)],
        "draining scheduled physical cleanup must preserve time-travel source metadata"
    );
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_add_column_default_after_existing_rows_when_stats_are_saved_then_default_contributes_to_min_max()
 {
    // Upstream: test/sql/alter/add_column_default_stats.test
    // Storage request: after ADD COLUMN DEFAULT, save stats for values materialized by that default.
    // Required return: min/max for the new column reflect both default and explicit values.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 4)]).unwrap();

    let changed = commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![
            TableColumnRow::new(ColumnId(3), "new_default", "int32", true, None)
                .with_default_metadata(None::<String>, Some("99"), "literal"),
        ],
    )
    .expect("ADD COLUMN DEFAULT should be expressible as a storage metadata update");
    assert_eq!(changed.columns.len(), 3);

    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            DataFileId(1),
            table,
            ColumnId(3),
            0,
            Some("99".to_owned()),
            Some("200".to_owned()),
        )
        .with_value_count(Some(4)),
    )
    .unwrap();

    let stats = list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(3)).unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let table_row = load_table_at(&kv, catalog, table, latest.order)
        .unwrap()
        .unwrap();

    assert_eq!(table_row.columns[2].name, "new_default");
    assert_eq!(table_row.columns[2].default_value.as_deref(), Some("99"));
    assert_eq!(stats[0].min_value.as_deref(), Some("99"));
    assert_eq!(stats[0].max_value.as_deref(), Some("200"));
}

#[test]
fn given_partition_values_for_new_files_when_data_mutation_commits_then_partition_pruning_reads_them()
 {
    // Upstream: add_files_hive_partition_cast, partitioning/*, compaction partition failures.
    // Storage request: save files and partition values in one metadata mutation.
    // Required return: partition pruning returns exactly the matching current files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 10), data_file(2, table, 10, 10)],
        Vec::new(),
        &[],
        vec![
            FilePartitionValueRow::new(DataFileId(1), table, PartitionKeyIndex(0), "2020"),
            FilePartitionValueRow::new(DataFileId(2), table, PartitionKeyIndex(0), "2021"),
        ],
    )
    .unwrap();

    let files = list_current_data_files_by_partition_value(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "2021",
    )
    .unwrap();

    assert_eq!(
        files
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(2)]
    );
}

#[test]
fn given_partition_values_for_compaction_output_when_sources_replaced_then_pruning_reads_replacement()
 {
    // Upstream: compaction_partitioned_table, merge_adjacent_null_partition, multi_key_merge.
    // Storage request: expire partitioned source files and save replacement file partition values.
    // Required return: partition pruning returns the replacement file and not expired sources.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 5), data_file(2, table, 5, 5)],
        Vec::new(),
        &[],
        vec![
            FilePartitionValueRow::new(DataFileId(1), table, PartitionKeyIndex(0), "north"),
            FilePartitionValueRow::new(DataFileId(2), table, PartitionKeyIndex(0), "north"),
        ],
    )
    .unwrap();

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![data_file(3, table, 0, 10)],
            partition_values: vec![FilePartitionValueRow::new(
                DataFileId(3),
                table,
                PartitionKeyIndex(0),
                "north",
            )],
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let files = list_current_data_files_by_partition_value(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "north",
    )
    .unwrap();

    assert_eq!(
        files
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(3)],
        "partition pruning should return compacted replacement metadata only"
    );
}

#[test]
fn given_null_partition_value_when_saved_then_null_and_empty_string_are_distinct() {
    // Upstream: test/sql/partitioning/partition_null.test and merge_adjacent_null_partition.test
    // Storage request: save partition metadata for SQL NULL and empty string.
    // Required return: pruning can distinguish IS NULL from = ''.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    append_data_file(&mut kv, catalog, data_file(1, table, 0, 1)).unwrap();
    append_data_file(&mut kv, catalog, data_file(2, table, 1, 1)).unwrap();

    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(DataFileId(1), table, PartitionKeyIndex(0), ""),
    )
    .unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(
            DataFileId(2),
            table,
            PartitionKeyIndex(0),
            "__ducklake_null__",
        ),
    )
    .unwrap();

    let all_partition_values = list_file_partition_values(&kv, catalog).unwrap();
    assert!(
        all_partition_values
            .iter()
            .any(|row| row.partition_value == "__ducklake_null__"
                && row.data_file_id == DataFileId(2)),
        "partition value model has no typed SQL NULL representation"
    );
    assert_ne!(
        all_partition_values[0].partition_value,
        all_partition_values[1].partition_value
    );
}

#[test]
fn given_sort_definition_when_saved_and_reset_then_current_and_history_are_isolated() {
    // Upstream: sorted_table/* and sorted flush tests.
    // Storage request: save a table sort definition, then reset it.
    // Required return: current table has no sort while historical snapshots keep the saved sort.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_change_table_sort(
        &mut kv,
        catalog,
        &TableSortChange::new(
            table,
            Some(TableSortRow::new(
                7,
                vec![TableSortFieldRow::new(
                    0,
                    "amount",
                    "duckdb",
                    "ASC",
                    "NULLS_LAST",
                )],
            )),
        ),
    )
    .unwrap()
    .unwrap();
    let sorted_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_change_table_sort(&mut kv, catalog, &TableSortChange::new(table, None))
        .unwrap()
        .unwrap();
    let reset_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        load_table_at(&kv, catalog, table, create_snapshot.order)
            .unwrap()
            .unwrap()
            .sort,
        None
    );
    assert_eq!(
        load_table_at(&kv, catalog, table, sorted_snapshot.order)
            .unwrap()
            .unwrap()
            .sort
            .unwrap()
            .fields[0]
            .expression,
        "amount"
    );
    assert_eq!(
        load_table_at(&kv, catalog, table, reset_snapshot.order)
            .unwrap()
            .unwrap()
            .sort,
        None
    );
}

#[test]
fn given_partition_definition_when_saved_and_reset_then_current_and_history_are_isolated() {
    // Upstream: partitioning_alter, drop_partition_column, partition_rename_in_transaction.
    // Storage request: save a table partition definition, then reset it.
    // Required return: current table has no partition while historical snapshots keep the saved one.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(table)).unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                8,
                vec![TablePartitionFieldRow::new(0, ColumnId(2), "identity")],
            )),
        ),
        None,
    )
    .unwrap()
    .unwrap();
    let partitioned_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(table, None),
        None,
    )
    .unwrap()
    .unwrap();
    let reset_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        load_table_at(&kv, catalog, table, create_snapshot.order)
            .unwrap()
            .unwrap()
            .partition,
        None
    );
    assert_eq!(
        load_table_at(&kv, catalog, table, partitioned_snapshot.order)
            .unwrap()
            .unwrap()
            .partition
            .unwrap()
            .fields[0]
            .column_id,
        ColumnId(2)
    );
    assert_eq!(
        load_table_at(&kv, catalog, table, reset_snapshot.order)
            .unwrap()
            .unwrap()
            .partition,
        None
    );
}

#[test]
fn given_inline_file_deletes_across_schema_changes_when_listing_deletions_then_deleted_rows_are_returned()
 {
    // Upstream: test/sql/deletion_inlining/test_deletion_inlining_alter.test
    // Storage request: save inline file deletion rows before/after table schema changes.
    // Required return: deletion scan planning still returns the deleted row ids for the data file.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let appended =
        commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();
    let base = appended[0].validity.begin_order;
    let deletion = commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, DataFileId(1), 1, CatalogOrderId::uuid_v7(0)),
            InlineFileDeletionRow::new(table, DataFileId(1), 2, CatalogOrderId::uuid_v7(0)),
        ],
    )
    .unwrap();
    let end = deletion[0].validity.begin_order;

    let scans = list_table_deletion_scan_files(&kv, catalog, table, base, end).unwrap();

    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].inline_file_deletions.len(), 2);
}

#[test]
fn given_physical_delete_file_when_registered_then_deletion_scan_returns_previous_delete_context() {
    // Upstream: delete/deletion_vector and rewrite_data_files families.
    // Storage request: save multiple delete files for one data file.
    // Required return: later delete scan includes the previous delete file so rewrites can compose correctly.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let appended =
        commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();
    let start = appended[0].validity.begin_order;
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(1),
            DataFileId(1),
            "main/orders/delete-1.parquet",
            2,
            100,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(2),
            DataFileId(1),
            "main/orders/delete-2.parquet",
            2,
            100,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let second_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let scans =
        list_table_deletion_scan_files(&kv, catalog, table, start, second_snapshot.order).unwrap();

    assert_eq!(scans.len(), 2);
    assert_eq!(
        scans[0].delete_file.as_ref().unwrap().delete_file_id,
        DeleteFileId(1)
    );
    assert!(
        scans[0].previous_delete_file.is_none(),
        "first delete file should not have previous delete context"
    );
    assert_eq!(
        scans[1].delete_file.as_ref().unwrap().delete_file_id,
        DeleteFileId(2)
    );
    assert_eq!(
        scans[1]
            .previous_delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        DeleteFileId(1)
    );
}

#[test]
fn given_delete_file_then_rewrite_when_listing_deletions_then_full_delete_keeps_prior_delete_context()
 {
    // Upstream: rewrite_data_files/* and deletion_vector_multi_snapshot.
    // Storage request: save a partial delete file, then rewrite the data file.
    // Required return: delete planning sees both the partial delete and later full-file deletion.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let appended =
        commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 50)]).unwrap();
    let append_order = appended[0].validity.begin_order;
    let delete = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(1),
            DataFileId(1),
            "main/orders/delete-1.parquet",
            2,
            100,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(2, table, 0, 48)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let rewrite_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;

    let scans =
        list_table_deletion_scan_files(&kv, catalog, table, append_order, rewrite_order).unwrap();

    assert_eq!(scans.len(), 2);
    assert_eq!(
        scans[0].delete_file.as_ref().unwrap().delete_file_id,
        delete[0].delete_file_id
    );
    assert!(
        scans[1].full_file_delete,
        "rewrite must expose the source file as a full-file deletion"
    );
    assert_eq!(
        scans[1]
            .previous_delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        delete[0].delete_file_id,
        "full-file rewrite should retain prior delete context"
    );
}

#[test]
fn given_macro_metadata_when_saved_then_current_snapshot_lists_macro_rows() {
    // Upstream: macros/* and hidden ducklake_macro access.
    // Storage request: save macro metadata.
    // Required return: metadata readers can enumerate the saved macro row.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(&mut kv, catalog, orders_table(TableId(10))).unwrap();
    commit_create_macro_rows(
        &mut kv,
        catalog,
        vec![MacroRow::new(
            MacroId(1),
            SchemaId(0),
            "plus_one",
            vec![MacroImplementationRow {
                dialect: "duckdb".to_owned(),
                sql: "x + 1".to_owned(),
                macro_type: "scalar".to_owned(),
                parameters: vec![MacroParameterRow {
                    parameter_name: "x".to_owned(),
                    parameter_type: "int32".to_owned(),
                    default_value: "NULL".to_owned(),
                    default_value_type: "unknown".to_owned(),
                }],
            }],
            CatalogOrderId::uuid_v7(0),
        )],
        None,
    )
    .unwrap();

    let current_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let macros = list_macros_at(&kv, catalog, current_snapshot.order).unwrap();

    assert_eq!(macros.len(), 1);
    assert_eq!(macros[0].macro_id, MacroId(1));
    assert_eq!(macros[0].name, "plus_one");
    assert_eq!(macros[0].implementations[0].sql, "x + 1");
}

#[test]
fn given_many_column_stats_when_saved_then_all_stats_are_returnable_by_column() {
    // Upstream: stats/*, default/*_stats, geo stats, and nested stats families.
    // Storage request: save file-level column stats for multiple files and columns.
    // Required return: each table/column lookup returns all saved stats without cross-column bleed.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 5), data_file(2, table, 5, 5)],
    )
    .unwrap();
    for (file_id, min, max) in [
        (DataFileId(1), "10".to_owned(), "20".to_owned()),
        (DataFileId(2), "21".to_owned(), "30".to_owned()),
    ] {
        register_file_column_stats(
            &mut kv,
            catalog,
            FileColumnStatsRow::new(file_id, table, ColumnId(2), 0, Some(min), Some(max))
                .with_value_count(Some(5)),
        )
        .unwrap();
        register_file_column_stats(
            &mut kv,
            catalog,
            FileColumnStatsRow::new(
                file_id,
                table,
                ColumnId(1),
                0,
                Some("row-id".to_owned()),
                Some("row-id".to_owned()),
            )
            .with_value_count(Some(5)),
        )
        .unwrap();
    }

    let amount_stats =
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(2)).unwrap();

    assert_eq!(amount_stats.len(), 2);
    assert_eq!(amount_stats[0].min_value.as_deref(), Some("10"));
    assert_eq!(amount_stats[1].max_value.as_deref(), Some("30"));
    assert!(
        amount_stats.iter().all(|row| row.column_id == ColumnId(2)),
        "stats lookup should not return rows saved for other columns"
    );
}

#[test]
fn given_many_files_in_one_transaction_when_checkpoint_reads_table_then_all_files_share_one_snapshot()
 {
    // Upstream: test/sql/checkpoint/many_inserts_transaction.test
    // Storage request: one DuckLake transaction can save many file additions.
    // Required return: checkpoint/current read sees all rows from the one committed snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    let committed = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 10),
            data_file(2, table, 10, 1),
            data_file(3, table, 11, 1),
            data_file(4, table, 12, 1),
        ],
    )
    .unwrap();

    let files = list_current_data_files(&kv, catalog, table).unwrap();

    assert_eq!(files.len(), 4);
    assert!(
        files
            .iter()
            .all(|file| file.validity.begin_order == committed[0].validity.begin_order)
    );
    assert_eq!(files.iter().map(|file| file.record_count).sum::<u64>(), 13);
}

#[test]
fn given_expired_snapshot_when_current_files_are_read_then_current_state_survives_and_old_snapshot_disappears()
 {
    // Upstream: expire_snapshots*, cleanup_old_files*, and time-travel expiry failures.
    // Storage request: expire an older public snapshot.
    // Required return: lookup for the expired snapshot fails closed while current file metadata
    // remains readable.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 10, 10)]).unwrap();
    assert_eq!(list_snapshots(&kv, catalog).unwrap().len(), 3);

    let expired = expire_snapshots(&mut kv, catalog, &[RawSnapshotSequence(1)]).unwrap();

    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].sequence, RawSnapshotSequence(1));
    assert!(
        !list_snapshots(&kv, catalog)
            .unwrap()
            .iter()
            .any(|snapshot| snapshot.sequence == RawSnapshotSequence(1))
    );
    assert_eq!(
        list_data_files_at(
            &kv,
            catalog,
            table,
            latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn given_inline_rows_deleted_then_payload_read_at_old_snapshot_still_returns_original_rows() {
    // Upstream: data_inlining update/delete and time-travel families.
    // Storage request: save inline rows, then save row tombstones.
    // Required return: historical reads still return the original inline payload.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();
    let chunks = register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        b"row\t0\ti:1\ts:61\nrow\t1\ti:2\ts:62\n".to_vec(),
    )
    .unwrap();
    let original_order = chunks[0].validity.begin_order;

    commit_delete_inline_table_rows(&mut kv, catalog, table, schema, &[0]).unwrap();

    let historical =
        load_inline_table_payload_at(&kv, catalog, table, schema, original_order).unwrap();

    assert_eq!(
        historical,
        Some(b"row\t0\ti:1\ts:61\nrow\t1\ti:2\ts:62\n".to_vec())
    );
}

#[test]
fn given_inline_rows_flushed_to_file_when_listing_insertions_then_flush_file_is_not_user_insert() {
    // Upstream: data_inlining_flush.test table_changes('test', 12, 12).
    // Storage request: save inline rows, then save a physical file that materializes those rows.
    // Required return: reads see the file, but insertion change feeds do not report the flush as new data.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();
    for snapshot in 2..=11 {
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_row.clone(),
            schema,
            format!("row\t{}\ti:{}\n", snapshot - 2, snapshot - 2).into_bytes(),
        )
        .unwrap();
        assert_eq!(
            latest_snapshot(&kv, catalog).unwrap().unwrap().sequence.0,
            snapshot
        );
    }
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 10)
                .with_begin_order(flush_snapshot.order)
                .with_max_partial_order(Some(flush_snapshot.order)),
        ],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            schema,
            flush_snapshot.sequence,
        )],
        Vec::new(),
    )
    .unwrap();
    let flush_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;

    assert_eq!(
        list_data_files_at(&kv, catalog, table, flush_order)
            .unwrap()
            .len(),
        1
    );
    assert!(
        insertion_files(&kv, catalog, table, flush_order, flush_order)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_unrelated_commit_between_inline_insert_and_flush_when_reading_table_then_inline_row_is_not_duplicated()
 {
    // Upstream: test/sql/add_files/add_files_compaction.test before merge_adjacent_files.
    // Storage request: save inline rows for one table, save an unrelated commit for another table,
    // then save a physical file that materializes the first table's inline rows.
    // Required return: current reads see the materialized file and no still-live inline payload.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let other_table = TableId(11);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        named_orders_table(other_table, "other_orders"),
    )
    .unwrap();
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let inline_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
    commit_append_data_files(&mut kv, catalog, vec![data_file(100, other_table, 0, 1)]).unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1)
                .with_begin_order(flush_snapshot.order)
                .with_max_partial_order(Some(flush_snapshot.order)),
        ],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            schema,
            flush_snapshot.sequence,
        )],
        Vec::new(),
    )
    .unwrap();
    let flush_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;

    assert_eq!(
        list_data_files_at(&kv, catalog, table, flush_order)
            .unwrap()
            .len(),
        1
    );
    assert!(
        OrderedCatalogKv::get(
            &kv,
            &inline_table_end_key(catalog, table, flush_order, schema, inline_order)
        )
        .unwrap()
        .is_some(),
        "flush snapshot must end the inline payload beginning at {inline_order:?}"
    );
}

#[test]
fn given_inline_rows_flushed_before_feed_end_when_listing_insertions_then_original_inline_rows_are_hidden()
 {
    // Upstream: test/sql/add_files/add_files_compaction.test ducklake_table_insertions(..., s3).
    // Storage request: save inline rows, flush them to a physical data file, then save later files.
    // Required return: an insertion feed through the later files returns the physical-file rows,
    // not the old inline rows that have already been materialized by the flush.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();
    let feed_start = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1)
                .with_begin_order(flush_snapshot.order)
                .with_max_partial_order(Some(flush_snapshot.order)),
        ],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            schema,
            flush_snapshot.sequence,
        )],
        Vec::new(),
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(2, table, 1, 1), data_file(3, table, 2, 1)],
    )
    .unwrap();
    let feed_end = latest_snapshot(&kv, catalog).unwrap().unwrap().order;

    let inline_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        feed_start,
        feed_end,
        ducklake_catalog::InlineRowChangeKind::Inserted,
    )
    .unwrap();

    assert!(inline_insertions.is_empty());
}

#[test]
fn given_two_inline_inserts_flushed_when_listing_insertions_to_second_public_snapshot_then_both_files_return()
 {
    // Upstream: test/sql/table_changes/ducklake_table_insertions.test, ti(0, s2).
    // Storage request: save two inline inserts, each later materialized by a flush file, then
    // read insertions through the second public insert snapshot.
    // Required return: both materialized files are returned and the old inline rows are hidden.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();

    let mut second_flush_order = None;
    for row_id in 0..=1 {
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_row.clone(),
            schema,
            format!("row\t{row_id}\ti:{}\n", row_id + 1).into_bytes(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_data_mutation_with_file_partitions(
            &mut kv,
            catalog,
            vec![
                data_file(row_id + 1, table, row_id, 1)
                    .with_begin_order(inline_snapshot.order)
                    .with_max_partial_order(Some(inline_snapshot.order)),
            ],
            Vec::new(),
            &[InlineTableFlush::new(
                table,
                schema,
                inline_snapshot.sequence,
            )],
            Vec::new(),
        )
        .unwrap();
        second_flush_order = Some(latest_snapshot(&kv, catalog).unwrap().unwrap().order);
    }

    let start = snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(0))
        .unwrap()
        .unwrap();
    let end = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(Some(end.order), second_flush_order);
    let files = insertion_files(&kv, catalog, table, start.order, end.order).unwrap();
    assert_eq!(
        files
            .iter()
            .map(|file| file.row_id_start)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let inline_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        start.order,
        end.order,
        ducklake_catalog::InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert!(inline_insertions.is_empty());
}

#[test]
fn given_inline_file_materialized_inside_change_feed_window_when_listing_data_file_changes_then_file_is_returned()
 {
    // Upstream: test/sql/table_changes/ducklake_table_insertions.test, ti(s3, s4).
    // Storage request: save an inline insert, then save the physical file that materializes it,
    // then save a later inline insert in the same table.
    // Required return: a data-file change feed starting at the materialization snapshot returns
    // the materialized file, even though the row's logical visibility began at the inline insert.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();

    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row.clone(),
        schema,
        b"row\t2\ti:3\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            data_file(3, table, 2, 1)
                .with_begin_order(inline_snapshot.order)
                .with_max_partial_order(Some(inline_snapshot.order)),
        ],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            schema,
            inline_snapshot.sequence,
        )],
        Vec::new(),
    )
    .unwrap();
    let materialized_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let pre_flush_inline_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        CatalogOrderId::uuid_v7(0),
        inline_snapshot.order,
        ducklake_catalog::InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert!(
        pre_flush_inline_insertions.is_empty(),
        "materialized insertions should be served from the data file, not the old inline row"
    );

    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        b"row\t3\tn:\n".to_vec(),
    )
    .unwrap();
    let end_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let post_flush_inline_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        CatalogOrderId::uuid_v7(0),
        materialized_snapshot.order,
        ducklake_catalog::InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert!(post_flush_inline_insertions.is_empty());

    let changes = list_data_file_changes(
        &kv,
        catalog,
        table,
        materialized_snapshot.order,
        end_snapshot.order,
    )
    .unwrap();

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].order, materialized_snapshot.order);
    assert_eq!(changes[0].data_file_id, DataFileId(3));
}

#[test]
fn given_inline_flush_before_add_file_when_public_snapshot_is_used_then_time_travel_reads_add_file_snapshot()
 {
    // Upstream: test/sql/add_files/add_files_compaction.test time travel to s2 after compaction.
    // Storage request: publish snapshot ids, then resolve a public snapshot id back to a storage
    // snapshot for a table read.
    // Required return: the public id for the first add-file commit resolves to that add-file
    // snapshot, not to the preceding inline flush or unrelated table commit.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let other_table = TableId(11);
    let schema = SchemaId(0);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table_row = orders_table(table);
    table_row
        .inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            0,
        ));
    commit_create_table_row(&mut kv, catalog, table_row.clone()).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        named_orders_table(other_table, "other_orders"),
    )
    .unwrap();
    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_row,
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(100, other_table, 0, 1)]).unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1)],
        Vec::new(),
        &[InlineTableFlush::new(
            table,
            schema,
            flush_snapshot.sequence,
        )],
        Vec::new(),
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 1, 1)]).unwrap();
    let first_add_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, table, 2, 1)]).unwrap();
    let last_add_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3)],
            new_files: vec![data_file(4, table, 0, 3)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let first_add_public_id =
        public_snapshot_sequence_for_order(&kv, catalog, first_add_snapshot.order)
            .unwrap()
            .unwrap();
    let resolved = snapshot_by_public_sequence(&kv, catalog, first_add_public_id)
        .unwrap()
        .unwrap();
    let files = list_data_files_at(&kv, catalog, table, resolved.order).unwrap();

    assert_eq!(resolved.order, first_add_snapshot.order);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].data_file_id, DataFileId(4));
    assert_eq!(files[0].row_id_start, 0);
    assert_eq!(files[0].record_count, 3);
    assert_eq!(files[0].max_partial_order, Some(last_add_snapshot.order));
}

fn orders_table(table: TableId) -> TableRow {
    named_orders_table(table, "orders")
}

fn named_orders_table(table: TableId, name: &str) -> TableRow {
    TableRow::with_catalog_metadata(
        table,
        SchemaId(0),
        format!("table-{}", table.0),
        name,
        format!("main/{name}"),
        vec![
            TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
            TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
        ],
        CatalogOrderId::uuid_v7(0),
    )
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/orders/file-{id}.parquet"),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn small_inline_rows_payload(min_len: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut row_id = 0_u64;
    while payload.len() <= min_len {
        payload.extend_from_slice(format!("row\t{row_id}\ti:{row_id}\n").as_bytes());
        row_id += 1;
    }
    payload
}

fn oversized_inline_row_payload() -> Vec<u8> {
    let mut payload = b"row\t1\ts:".to_vec();
    payload.extend(std::iter::repeat_n(b'x', INLINE_PAYLOAD_LIMIT_BYTES));
    payload.push(b'\n');
    payload
}
