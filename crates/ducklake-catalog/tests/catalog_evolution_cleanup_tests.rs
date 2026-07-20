use ducklake_catalog::{
    CatalogError, CatalogId, CatalogOrderId, ColumnCommentChange, ColumnDefaultChange, ColumnDrop,
    ColumnId, ColumnRename, ColumnTypeChange, CommitAttemptId, DataFileId, DataFileRow,
    DeleteFileId, DeleteFileRow, FakeOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow,
    InlineFileDeletionRow, InlineRowChangeKind, InlineTableFlush, MacroId, MacroImplementationRow,
    MacroParameterRow, MacroRow, MergeAdjacentCompaction, PartitionKeyIndex, RawSnapshotSequence,
    RewriteDeleteCompaction, SchemaId, TableColumnRow, TableCommentChange, TableId,
    TablePartitionChange, TablePartitionFieldRow, TablePartitionRow, TableRename, TableRow,
    TableSortChange, TableSortFieldRow, TableSortRow, ViewCommentChange, ViewRename, ViewRow,
    commit_append_data_file, commit_append_data_files,
    commit_append_data_files_with_inline_flushes, commit_append_table_columns,
    commit_append_table_columns_with_conflict_check, commit_change_table_column_defaults,
    commit_change_table_column_defaults_with_conflict_check, commit_change_table_column_types,
    commit_change_table_column_types_with_conflict_check, commit_change_table_comments,
    commit_change_table_partition, commit_change_table_partition_with_conflict_check,
    commit_change_table_sort, commit_change_table_sort_with_conflict_check,
    commit_change_view_comment, commit_create_macro_rows, commit_create_table_row,
    commit_create_view_row, commit_data_mutation, commit_data_mutation_with_file_partitions,
    commit_data_mutation_with_file_partitions_and_inline_deletes, commit_delete_inline_table_rows,
    commit_drop_macros, commit_drop_table_columns, commit_drop_table_columns_with_conflict_check,
    commit_drop_tables, commit_drop_views, commit_merge_adjacent_data_files,
    commit_merge_adjacent_data_files_with_conflict_check, commit_register_delete_files,
    commit_rename_table_columns, commit_rename_table_columns_with_conflict_check,
    commit_rename_tables, commit_rename_tables_with_conflict_check, commit_rename_views,
    commit_rewrite_delete_data_files, commit_rewrite_delete_data_files_with_conflict_check,
    expire_snapshots, initialize_catalog_if_absent, insertion_files,
    keys::{snapshot_key, snapshot_timestamp_key, table_delete_file_change_key},
    latest_snapshot, list_catalog_debug_rows, list_current_data_files,
    list_current_data_files_by_partition_value, list_data_file_changes, list_data_files_at,
    list_file_column_stats_for_table_column, list_inline_file_deletions_at,
    list_inline_row_payload_changes, list_inline_table_payloads_at,
    list_known_files_for_orphan_cleanup, list_macros_at, list_old_data_files_for_cleanup,
    list_old_delete_files_for_cleanup, list_old_inline_table_payloads_for_cleanup, list_snapshots,
    list_snapshots_older_than, list_table_deletion_scan_files, load_commit_attempt, load_macro_at,
    load_table_at, load_view_at, register_file_column_stats, register_inline_table_payload,
    remove_old_data_files, remove_old_delete_files, remove_old_inline_table_payloads,
    snapshot_by_raw_sequence,
};

#[test]
fn given_catalog_rows_when_listing_debug_rows_then_primary_metadata_families_are_decoded() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", false, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            DataFileId(1),
            "main/orders/file-0001-delete.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    let rows = list_catalog_debug_rows(&kv, catalog, 100).unwrap();
    assert!(
        rows.iter()
            .any(|row| matches!(row, ducklake_catalog::CatalogDebugRow::Snapshot(snapshot) if snapshot.sequence == ducklake_catalog::RawSnapshotSequence(0)))
    );
    assert!(
        rows.iter()
            .any(|row| matches!(row, ducklake_catalog::CatalogDebugRow::Table(table) if table.name == "orders"))
    );
    assert!(
        rows.iter()
            .any(|row| matches!(row, ducklake_catalog::CatalogDebugRow::Column { column, .. } if column.name == "amount"))
    );
    assert!(
        rows.iter()
            .any(|row| matches!(row, ducklake_catalog::CatalogDebugRow::DataFile(file) if file.data_file_id == DataFileId(1)))
    );
    assert!(
        rows.iter()
            .any(|row| matches!(row, ducklake_catalog::CatalogDebugRow::DeleteFile(file) if file.delete_file_id == DeleteFileId(11)))
    );
}

#[test]
fn given_table_renamed_then_current_uses_new_name_and_history_preserves_old_name() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let renamed = commit_rename_tables(
        &mut kv,
        catalog,
        &[TableRename::new(table, "orders_archive")],
    )
    .unwrap();
    let rename_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(renamed.len(), 1);
    assert_eq!(renamed[0].previous.name, "orders");
    assert_eq!(renamed[0].renamed.name, "orders_archive");

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, rename_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(historical.name, "orders");
    assert_eq!(current.name, "orders_archive");
}

#[test]
fn given_table_rename_to_existing_name_then_commit_is_rejected() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            TableId(10),
            SchemaId(0),
            "orders-uuid",
            "orders",
            "main/orders",
            Vec::new(),
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            TableId(11),
            SchemaId(0),
            "archive-uuid",
            "archive",
            "main/archive",
            Vec::new(),
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();

    let error = commit_rename_tables(
        &mut kv,
        catalog,
        &[TableRename::new(TableId(10), "archive")],
    )
    .unwrap_err();

    assert!(
        matches!(error, CatalogError::InvalidMutation(message) if message.contains("already exists"))
    );
}

#[test]
fn given_column_renamed_then_current_uses_new_name_and_history_preserves_old_name() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let renamed = commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            table,
            TableColumnRow::new(ColumnId(2), "total", "int32", true, None),
        )],
    )
    .unwrap();
    let rename_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(renamed.len(), 1);
    assert_eq!(renamed[0].previous.name, "amount");
    assert_eq!(renamed[0].renamed.name, "total");

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, rename_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(
        historical
            .columns
            .iter()
            .any(|column| column.name == "amount")
    );
    assert!(
        historical
            .columns
            .iter()
            .all(|column| column.name != "total")
    );
    assert!(current.columns.iter().any(|column| column.name == "total"));
    assert!(current.columns.iter().all(|column| column.name != "amount"));
}

#[test]
fn given_nested_leaf_column_renamed_then_current_uses_new_child_name_and_history_preserves_old_name()
 {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(3), "a", "int32", true, Some(ColumnId(2))),
                TableColumnRow::new(ColumnId(4), "b", "int32", true, Some(ColumnId(2))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let renamed = commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            table,
            TableColumnRow::new(ColumnId(4), "c", "int32", true, Some(ColumnId(2))),
        )],
    )
    .unwrap();
    let rename_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(renamed.len(), 1);
    assert_eq!(renamed[0].previous.name, "b");
    assert_eq!(renamed[0].renamed.name, "c");
    assert_eq!(renamed[0].renamed.parent_id, Some(ColumnId(2)));

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, rename_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(
        historical
            .columns
            .iter()
            .any(|column| column.name == "b" && column.parent_id == Some(ColumnId(2)))
    );
    assert!(
        current
            .columns
            .iter()
            .any(|column| column.name == "c" && column.parent_id == Some(ColumnId(2)))
    );
    assert!(current.columns.iter().all(|column| column.name != "b"));
}

#[test]
fn given_column_replacement_changes_type_then_rename_commit_is_rejected() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(
                ColumnId(2),
                "amount",
                "int32",
                true,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();

    let error = commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            table,
            TableColumnRow::new(ColumnId(2), "total", "int64", true, None),
        )],
    )
    .unwrap_err();

    assert!(
        matches!(error, CatalogError::InvalidMutation(message) if message.contains("more than its name"))
    );
}

#[test]
fn given_column_dropped_then_current_removes_column_and_history_preserves_it() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let dropped =
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(2))])
            .unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].column.name, "amount");

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, drop_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(
        historical
            .columns
            .iter()
            .any(|column| column.name == "amount")
    );
    assert!(current.columns.iter().all(|column| column.name != "amount"));
    assert!(current.columns.iter().any(|column| column.name == "id"));
}

#[test]
fn given_nested_leaf_column_drop_then_current_removes_child_and_history_preserves_it() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, Some(ColumnId(1))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let dropped =
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(2))])
            .unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].column.name, "amount");
    assert_eq!(dropped[0].column.parent_id, Some(ColumnId(1)));

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, drop_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(
        historical
            .columns
            .iter()
            .any(|column| column.name == "amount" && column.parent_id == Some(ColumnId(1)))
    );
    assert!(current.columns.iter().all(|column| column.name != "amount"));
    assert!(
        current
            .columns
            .iter()
            .any(|column| column.name == "payload" && column.parent_id.is_none())
    );
}

#[test]
fn given_parent_column_drop_when_committed_then_descendant_columns_are_removed_together() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, Some(ColumnId(1))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();

    let dropped =
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(1))])
            .unwrap();
    let current_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current = load_table_at(&kv, catalog, table, current_snapshot.order)
        .unwrap()
        .unwrap();

    assert_eq!(dropped.len(), 2);
    assert!(
        dropped
            .iter()
            .any(|drop| drop.column.column_id == ColumnId(1))
    );
    assert!(
        dropped
            .iter()
            .any(|drop| drop.column.column_id == ColumnId(2))
    );
    assert!(current.columns.is_empty());
}

#[test]
fn given_column_type_changed_then_current_uses_new_type_and_history_preserves_old_type() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(
            table,
            TableColumnRow::new(ColumnId(2), "amount", "int64", true, None),
        )],
    )
    .unwrap();
    let change_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].previous.column_type, "int32");
    assert_eq!(changed[0].changed.column_type, "int64");

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, change_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(
        historical
            .columns
            .iter()
            .any(|column| column.name == "amount" && column.column_type == "int32")
    );
    assert!(
        current
            .columns
            .iter()
            .any(|column| column.name == "amount" && column.column_type == "int64")
    );
}

#[test]
fn given_nested_leaf_column_type_changed_then_current_uses_new_type_and_history_preserves_old_type()
{
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(3), "a", "int32", true, Some(ColumnId(2))),
                TableColumnRow::new(ColumnId(4), "b", "int32", true, Some(ColumnId(2))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(
            table,
            TableColumnRow::new(ColumnId(4), "b", "int64", true, Some(ColumnId(2))),
        )],
    )
    .unwrap();
    let change_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].previous.column_type, "int32");
    assert_eq!(changed[0].changed.column_type, "int64");
    assert_eq!(changed[0].changed.parent_id, Some(ColumnId(2)));

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, change_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(historical.columns.iter().any(|column| {
        column.name == "b" && column.column_type == "int32" && column.parent_id == Some(ColumnId(2))
    }));
    assert!(current.columns.iter().any(|column| {
        column.name == "b" && column.column_type == "int64" && column.parent_id == Some(ColumnId(2))
    }));
}

#[test]
fn given_parent_column_type_changed_then_commit_is_rejected_until_tree_evolution_is_owned() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, Some(ColumnId(1))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();

    let error = commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(
            table,
            TableColumnRow::new(ColumnId(1), "payload", "struct", true, None),
        )],
    )
    .unwrap_err();

    assert!(
        matches!(error, CatalogError::InvalidMutation(message) if message.contains("parent column type change"))
    );
}

#[test]
fn given_column_default_changed_then_current_uses_new_default_and_history_preserves_old_default() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None)
                    .with_default_metadata(None::<String>, Some("5"), "literal"),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed =
        commit_change_table_column_defaults(
            &mut kv,
            catalog,
            &[ColumnDefaultChange::new(
                table,
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None)
                    .with_default_metadata(None::<String>, Some("42"), "literal"),
            )],
        )
        .unwrap();
    let set_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].previous.default_value.as_deref(), Some("5"));
    assert_eq!(changed[0].changed.default_value.as_deref(), Some("42"));

    let dropped =
        commit_change_table_column_defaults(
            &mut kv,
            catalog,
            &[ColumnDefaultChange::new(
                table,
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None)
                    .with_default_metadata(None::<String>, None::<String>, "literal"),
            )],
        )
        .unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped[0].previous.default_value.as_deref(), Some("42"));
    assert_eq!(dropped[0].changed.default_value, None);

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_set = load_table_at(&kv, catalog, table, set_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, drop_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(historical.columns[1].default_value.as_deref(), Some("5"));
    assert_eq!(after_set.columns[1].default_value.as_deref(), Some("42"));
    assert_eq!(current.columns[1].default_value, None);
}

#[test]
fn given_table_sort_changed_then_current_uses_new_sort_and_history_preserves_old_sort() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_table_sort(
        &mut kv,
        catalog,
        &TableSortChange::new(
            table,
            Some(TableSortRow::new(
                7,
                vec![TableSortFieldRow::new(
                    0,
                    "id",
                    "duckdb",
                    "ASC",
                    "NULLS_LAST",
                )],
            )),
        ),
    )
    .unwrap()
    .unwrap();
    let set_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.previous, None);
    assert_eq!(changed.changed.as_ref().unwrap().fields[0].expression, "id");

    let reset = commit_change_table_sort(&mut kv, catalog, &TableSortChange::new(table, None))
        .unwrap()
        .unwrap();
    let reset_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(reset.previous.as_ref().unwrap().fields.len(), 1);
    assert_eq!(reset.changed, None);

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_set = load_table_at(&kv, catalog, table, set_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, reset_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(historical.sort, None);
    assert_eq!(after_set.sort.as_ref().unwrap().sort_id, 7);
    assert_eq!(after_set.sort.as_ref().unwrap().fields[0].expression, "id");
    assert_eq!(current.sort, None);
}

#[test]
fn given_table_partition_changed_then_current_uses_new_partition_and_history_preserves_old_partition()
 {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "region", "varchar", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                8,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap()
    .unwrap();
    let set_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.previous, None);
    assert_eq!(
        changed.changed.as_ref().unwrap().fields[0].column_id,
        ColumnId(1)
    );

    let reset = commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(table, None),
        None,
    )
    .unwrap()
    .unwrap();
    let reset_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(reset.previous.as_ref().unwrap().fields.len(), 1);
    assert_eq!(reset.changed, None);

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_set = load_table_at(&kv, catalog, table, set_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, reset_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(historical.partition, None);
    assert_eq!(after_set.partition.as_ref().unwrap().partition_id, 8);
    assert_eq!(
        after_set.partition.as_ref().unwrap().fields[0].transform,
        "identity"
    );
    assert_eq!(current.partition, None);
}

#[test]
fn given_table_and_column_comments_changed_then_history_preserves_previous_comments() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_table_comments(
        &mut kv,
        catalog,
        &[TableCommentChange::new(table, Some("orders table"))],
        &[ColumnCommentChange::new(
            table,
            ColumnId(2),
            Some("order amount"),
        )],
    )
    .unwrap()
    .unwrap();
    let comment_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.table_id, table);
    assert_eq!(
        changed.table_comment.as_ref().unwrap().comment.as_deref(),
        Some("orders table")
    );
    assert_eq!(changed.column_comments.len(), 1);

    let dropped = commit_change_table_comments(
        &mut kv,
        catalog,
        &[TableCommentChange::new(table, None::<String>)],
        &[ColumnCommentChange::new(table, ColumnId(2), None::<String>)],
    )
    .unwrap()
    .unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.table_comment.as_ref().unwrap().comment, None);
    assert_eq!(dropped.column_comments[0].comment, None);

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_set = load_table_at(&kv, catalog, table, comment_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, drop_snapshot.order)
        .unwrap()
        .unwrap();

    assert_eq!(historical.comment, None);
    assert_eq!(historical.columns[1].comment, None);
    assert_eq!(after_set.comment.as_deref(), Some("orders table"));
    assert_eq!(
        after_set.columns[1].comment.as_deref(),
        Some("order amount")
    );
    assert_eq!(current.comment, None);
    assert_eq!(current.columns[1].comment, None);
}

#[test]
fn given_view_created_and_commented_then_history_preserves_comment_versions() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let view = TableId(20);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_view_row(
        &mut kv,
        catalog,
        ViewRow::new(
            view,
            SchemaId(0),
            ducklake_catalog::ViewDefinition::new(
                "view-uuid",
                "orders_view",
                "duckdb",
                "SELECT 1 AS id",
                vec!["id".to_owned()],
            ),
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changed = commit_change_view_comment(
        &mut kv,
        catalog,
        &ViewCommentChange::new(view, Some("view comment")),
    )
    .unwrap();
    let comment_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(changed.previous, None);
    assert_eq!(changed.changed.as_deref(), Some("view comment"));

    let dropped = commit_change_view_comment(
        &mut kv,
        catalog,
        &ViewCommentChange::new(view, None::<String>),
    )
    .unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.previous.as_deref(), Some("view comment"));
    assert_eq!(dropped.changed, None);

    let historical = load_view_at(&kv, catalog, view, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_set = load_view_at(&kv, catalog, view, comment_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_view_at(&kv, catalog, view, drop_snapshot.order)
        .unwrap()
        .unwrap();

    assert_eq!(historical.comment, None);
    assert_eq!(after_set.comment.as_deref(), Some("view comment"));
    assert_eq!(current.comment, None);
    assert_eq!(current.sql, "SELECT 1 AS id");
    assert_eq!(current.column_aliases, vec!["id"]);
}

#[test]
fn given_view_renamed_and_dropped_then_history_preserves_versions() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let view = TableId(20);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_view_row(
        &mut kv,
        catalog,
        ViewRow::new(
            view,
            SchemaId(0),
            ducklake_catalog::ViewDefinition::new(
                "view-uuid",
                "orders_view",
                "duckdb",
                "SELECT 1 AS id",
                vec!["id".to_owned()],
            ),
            CatalogOrderId::uuid_v7(0),
        )
        .with_comment(Some("view comment")),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let renamed = commit_rename_views(
        &mut kv,
        catalog,
        &[ViewRename::new(view, "renamed_orders_view")],
    )
    .unwrap();
    let rename_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(renamed.len(), 1);
    assert_eq!(renamed[0].previous.name, "orders_view");
    assert_eq!(renamed[0].renamed.name, "renamed_orders_view");
    assert_eq!(renamed[0].renamed.comment.as_deref(), Some("view comment"));

    let dropped = commit_drop_views(&mut kv, catalog, &[view]).unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].view.name, "renamed_orders_view");

    let historical = load_view_at(&kv, catalog, view, create_snapshot.order)
        .unwrap()
        .unwrap();
    let after_rename = load_view_at(&kv, catalog, view, rename_snapshot.order)
        .unwrap()
        .unwrap();
    let after_drop = load_view_at(&kv, catalog, view, drop_snapshot.order).unwrap();

    assert_eq!(historical.name, "orders_view");
    assert_eq!(after_rename.name, "renamed_orders_view");
    assert_eq!(after_rename.sql, "SELECT 1 AS id");
    assert_eq!(after_rename.column_aliases, vec!["id"]);
    assert_eq!(after_rename.comment.as_deref(), Some("view comment"));
    assert!(after_drop.is_none());
}

#[test]
fn given_macro_created_then_current_snapshot_lists_implementation_and_parameters() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let macro_id = MacroId(30);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let created = commit_create_macro_rows(
        &mut kv,
        catalog,
        vec![MacroRow::new(
            macro_id,
            SchemaId(0),
            "add_tax",
            vec![MacroImplementationRow {
                dialect: "duckdb".to_owned(),
                sql: "(amount + 1)".to_owned(),
                macro_type: "scalar".to_owned(),
                parameters: vec![MacroParameterRow {
                    parameter_name: "amount".to_owned(),
                    parameter_type: "bigint".to_owned(),
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

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "add_tax");
    assert_eq!(created[0].validity.begin_order, current_snapshot.order);

    let loaded = load_macro_at(&kv, catalog, macro_id, current_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.name, "add_tax");
    assert_eq!(loaded.implementations[0].macro_type, "scalar");
    assert_eq!(
        loaded.implementations[0].parameters[0].parameter_name,
        "amount"
    );

    let listed = list_macros_at(&kv, catalog, current_snapshot.order).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].macro_id, macro_id);
}

#[test]
fn given_macro_dropped_then_history_preserves_created_snapshot() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let macro_id = MacroId(30);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_macro_rows(
        &mut kv,
        catalog,
        vec![MacroRow::new(
            macro_id,
            SchemaId(0),
            "add_tax",
            vec![MacroImplementationRow {
                dialect: "duckdb".to_owned(),
                sql: "(amount + 1)".to_owned(),
                macro_type: "scalar".to_owned(),
                parameters: vec![MacroParameterRow {
                    parameter_name: "amount".to_owned(),
                    parameter_type: "bigint".to_owned(),
                    default_value: "NULL".to_owned(),
                    default_value_type: "unknown".to_owned(),
                }],
            }],
            CatalogOrderId::uuid_v7(0),
        )],
        None,
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let dropped = commit_drop_macros(&mut kv, catalog, &[macro_id], None).unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].macro_row.macro_id, macro_id);
    assert_eq!(
        dropped[0].macro_row.validity.end_order,
        Some(drop_snapshot.order)
    );
    assert!(
        load_macro_at(&kv, catalog, macro_id, drop_snapshot.order)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        load_macro_at(&kv, catalog, macro_id, create_snapshot.order)
            .unwrap()
            .unwrap()
            .name,
        "add_tax"
    );
}

#[test]
fn given_column_replacement_changes_name_then_type_change_commit_is_rejected() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(
                ColumnId(2),
                "amount",
                "int32",
                true,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();

    let error = commit_change_table_column_types(
        &mut kv,
        catalog,
        &[ColumnTypeChange::new(
            table,
            TableColumnRow::new(ColumnId(2), "total", "int64", true, None),
        )],
    )
    .unwrap_err();

    assert!(
        matches!(error, CatalogError::InvalidMutation(message) if message.contains("changes identity metadata during type change"))
    );
}

#[test]
fn given_append_and_delete_in_one_mutation_when_listing_cdf_then_both_use_one_snapshot_order() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let original = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap()
    .remove(0);
    let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let mutation = commit_data_mutation(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/file-0002.parquet",
                1,
                512,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(9),
        ],
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            original.data_file_id,
            "main/orders/file-0001-delete.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
        &[],
    )
    .unwrap();
    let update_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        mutation.data_files[0].validity.begin_order,
        update_snapshot.order
    );
    assert_eq!(
        mutation.delete_files[0].validity.begin_order,
        update_snapshot.order
    );

    let data_changes = list_data_file_changes(
        &kv,
        catalog,
        table,
        append_snapshot.order,
        update_snapshot.order,
    )
    .unwrap();
    let replacement_change = data_changes
        .iter()
        .find(|change| change.data_file_id == DataFileId(2))
        .unwrap();
    assert_eq!(replacement_change.order, update_snapshot.order);

    let delete_scans = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        append_snapshot.order,
        update_snapshot.order,
    )
    .unwrap();
    assert_eq!(delete_scans.len(), 1);
    assert_eq!(delete_scans[0].snapshot_order, update_snapshot.order);
}

#[test]
fn given_append_mutation_with_partition_values_then_pruning_reads_same_snapshot_commit() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let partition_key = PartitionKeyIndex(0);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mutation = commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/region=eu/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            ),
            DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/region=us/file-0002.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            ),
        ],
        Vec::new(),
        &[],
        vec![
            FilePartitionValueRow::new(DataFileId(1), table, partition_key, "eu"),
            FilePartitionValueRow::new(DataFileId(2), table, partition_key, "us"),
        ],
    )
    .unwrap();
    let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(mutation.partition_value_count, 2);
    assert_eq!(mutation.data_files[0].validity.begin_order, snapshot.order);

    let eu_files =
        list_current_data_files_by_partition_value(&kv, catalog, table, partition_key, "eu")
            .unwrap();
    assert_eq!(eu_files.len(), 1);
    assert_eq!(eu_files[0].data_file_id, DataFileId(1));
    assert_eq!(eu_files[0].validity.begin_order, snapshot.order);
}

#[test]
fn given_inline_file_deletions_when_rewrite_delete_runs_then_source_file_is_replaced() {
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
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();

    let mutation = commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        Vec::new(),
        &[],
        Vec::new(),
        vec![InlineFileDeletionRow::new(
            table,
            DataFileId(1),
            3,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(mutation.inline_file_deletion_count, 1);
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, delete_snapshot.order)
            .unwrap()
            .get(&DataFileId(1))
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![3]
    );

    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    9,
                    1900,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file_id, DataFileId(2));
    let historical = list_data_files_at(&kv, catalog, table, delete_snapshot.order).unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].data_file_id, DataFileId(1));
}

#[test]
fn given_rewrite_delete_without_physical_or_inline_deletes_then_source_file_is_rejected() {
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
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();

    let error = commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    10,
                    2048,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("data file 1 has no delete file or inline deletions to rewrite")
    );
}

#[test]
fn given_append_mutation_partition_value_for_other_table_then_commit_is_rejected() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let error = commit_data_mutation_with_file_partitions(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(1),
            TableId(10),
            "main/orders/region=eu/file-0001.parquet",
            10,
            2048,
            CatalogOrderId::uuid_v7(0),
        )],
        Vec::new(),
        &[],
        vec![FilePartitionValueRow::new(
            DataFileId(1),
            TableId(11),
            PartitionKeyIndex(0),
            "eu",
        )],
    )
    .unwrap_err();

    assert!(
        matches!(error, CatalogError::InvalidMutation(message) if message.contains("does not match appended data file table"))
    );
}

#[test]
fn given_delete_files_and_rewrite_when_listing_table_deletions_then_partial_and_full_delete_scans_are_returned()
 {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let data_file = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap()
    .remove(0);
    let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let first_delete = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            data_file.data_file_id,
            "main/orders/file-0001-delete-a.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap()
    .remove(0);
    let first_delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let second_delete = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(12),
            data_file.data_file_id,
            "main/orders/file-0001-delete-b.parquet",
            2,
            256,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap()
    .remove(0);
    let second_delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![data_file.data_file_id],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    8,
                    1024,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let first_partial_scans = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        first_delete_snapshot.order,
        first_delete_snapshot.order,
    )
    .unwrap();
    assert_eq!(first_partial_scans.len(), 1);
    assert_eq!(
        first_partial_scans[0]
            .delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        first_delete.delete_file_id
    );
    assert!(first_partial_scans[0].previous_delete_file.is_none());

    let partial_scans = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        append_snapshot.order,
        second_delete_snapshot.order,
    )
    .unwrap();
    assert_eq!(partial_scans.len(), 2);
    assert_eq!(
        partial_scans[0]
            .delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        first_delete.delete_file_id
    );
    assert!(partial_scans[0].previous_delete_file.is_none());
    assert_eq!(
        partial_scans[1]
            .delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        second_delete.delete_file_id
    );
    assert_eq!(
        partial_scans[1]
            .previous_delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        first_delete.delete_file_id
    );

    let full_scans = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        rewrite_snapshot.order,
        rewrite_snapshot.order,
    )
    .unwrap();
    assert_eq!(full_scans.len(), 1);
    assert!(full_scans[0].full_file_delete);
    assert_eq!(full_scans[0].data_file.data_file_id, data_file.data_file_id);
    assert!(full_scans[0].delete_file.is_none());
    assert_eq!(
        full_scans[0]
            .previous_delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        second_delete.delete_file_id
    );

    let reloaded_first_delete = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        first_delete_snapshot.order,
        first_delete_snapshot.order,
    )
    .unwrap()
    .remove(0);
    assert_eq!(
        reloaded_first_delete
            .delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        first_delete.delete_file_id
    );
    assert!(reloaded_first_delete.previous_delete_file.is_none());
}

#[test]
fn given_table_drop_when_listing_current_and_historical_state_then_visibility_windows_are_closed() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            DataFileId(1),
            table,
            ColumnId(1),
            0,
            Some("1".into()),
            Some("10".into()),
        ),
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            DataFileId(1),
            "main/orders/file-0001-delete.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let dropped = commit_drop_tables(&mut kv, catalog, &[table]).unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].table.table_id, table);
    assert_eq!(dropped[0].expired_data_file_count, 1);
    assert_eq!(dropped[0].expired_delete_file_count, 1);
    assert!(
        load_table_at(&kv, catalog, table, drop_snapshot.order)
            .unwrap()
            .is_none()
    );
    assert!(
        load_table_at(&kv, catalog, table, append_snapshot.order)
            .unwrap()
            .is_some()
    );
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, append_snapshot.order)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
            .unwrap()
            .len(),
        1
    );

    let changes = list_data_file_changes(
        &kv,
        catalog,
        table,
        delete_snapshot.order,
        drop_snapshot.order,
    )
    .unwrap();
    assert!(
        changes
            .iter()
            .any(|change| change.data_file_id == DataFileId(1)
                && change.kind == ducklake_catalog::DataFileChangeKind::Removed
                && change.order == drop_snapshot.order)
    );

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
    expire_snapshots(
        &mut kv,
        catalog,
        &[
            ducklake_catalog::RawSnapshotSequence(1),
            append_snapshot.sequence,
            delete_snapshot.sequence,
        ],
    )
    .unwrap();
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
    let removed_delete_files =
        remove_old_delete_files(&mut kv, catalog, &[DeleteFileId(11)]).unwrap();
    assert_eq!(removed_delete_files.len(), 1);
    let removed_data_files = remove_old_data_files(&mut kv, catalog, &[DataFileId(1)]).unwrap();
    assert_eq!(removed_data_files.len(), 1);
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
    assert!(
        list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
            .unwrap()
            .is_empty()
    );
    assert!(
        load_table_at(&kv, catalog, table, drop_snapshot.order)
            .unwrap()
            .is_none()
    );
}

#[test]
fn given_empty_table_drop_after_append_base_when_append_retries_then_table_conflict_blocks_publish()
{
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let attempt = CommitAttemptId(900);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "empty-table-uuid",
            "empty_orders",
            "main/empty_orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_drop_tables(&mut kv, catalog, &[table]).unwrap();
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = commit_append_data_file(
        &mut kv,
        catalog,
        attempt,
        base_snapshot.order,
        drop_snapshot.order,
        DataFileRow::new(
            DataFileId(77),
            table,
            "main/empty_orders/blocked.parquet",
            1,
            256,
            CatalogOrderId::uuid_v7(10_000),
        ),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableLogicalConflict {
            table_id,
            dropped_at
        } if table_id == table && dropped_at == drop_snapshot.order
    ));
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
    assert_eq!(load_commit_attempt(&kv, catalog, attempt).unwrap(), None);
}

#[test]
fn given_add_column_after_concurrent_add_column_when_checked_then_schema_conflict_blocks_publish() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(2),
            "amount",
            "int32",
            true,
            None,
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = commit_append_table_columns_with_conflict_check(
        &mut kv,
        catalog,
        table,
        base_snapshot.order,
        concurrent_snapshot.order,
        vec![TableColumnRow::new(
            ColumnId(3),
            "status",
            "varchar",
            true,
            None,
        )],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableSchemaConflict {
            table_id,
            changed_at
        } if table_id == table && changed_at == concurrent_snapshot.order
    ));
    let current = load_table_at(&kv, catalog, table, concurrent_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(current.columns.len(), 2);
    assert!(current.columns.iter().any(|column| column.name == "amount"));
    assert!(!current.columns.iter().any(|column| column.name == "status"));
}

#[test]
fn given_remaining_ddl_after_concurrent_schema_change_when_checked_then_schema_conflict_blocks_publish()
 {
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_rename_tables_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &[TableRename::new(table, "orders_archive")],
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_rename_table_columns_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &[ColumnRename::new(
                table,
                TableColumnRow::new(ColumnId(2), "total", "int32", true, None),
            )],
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_drop_table_columns_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &[ColumnDrop::new(table, ColumnId(2))],
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_change_table_column_types_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &[ColumnTypeChange::new(
                table,
                TableColumnRow::new(ColumnId(2), "amount", "int64", true, None),
            )],
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_change_table_column_defaults_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &[ColumnDefaultChange::new(
                table,
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None)
                    .with_default_metadata(None::<String>, Some("42"), "literal"),
            )],
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_change_table_sort_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &TableSortChange::new(
                table,
                Some(TableSortRow::new(
                    7,
                    vec![TableSortFieldRow::new(
                        0,
                        "id",
                        "duckdb",
                        "ASC",
                        "NULLS_LAST",
                    )],
                )),
            ),
        )
    });
    assert_stale_schema_change_conflicts(|kv, catalog, table, base, through| {
        commit_change_table_partition_with_conflict_check(
            kv,
            catalog,
            base,
            through,
            &TablePartitionChange::new(
                table,
                Some(TablePartitionRow::new(
                    8,
                    vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
                )),
            ),
            None,
        )
    });
}

fn assert_stale_schema_change_conflicts<T: std::fmt::Debug>(
    stale_commit: impl FnOnce(
        &mut FakeOrderedCatalogKv,
        CatalogId,
        TableId,
        CatalogOrderId,
        CatalogOrderId,
    ) -> Result<T, CatalogError>,
) {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "amount", "int32", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(3),
            "status",
            "varchar",
            true,
            None,
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = stale_commit(
        &mut kv,
        catalog,
        table,
        base_snapshot.order,
        concurrent_snapshot.order,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableSchemaConflict {
            table_id,
            changed_at
        } if table_id == table && changed_at == concurrent_snapshot.order
    ));
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        concurrent_snapshot.order
    );
}

#[test]
fn given_nested_column_added_then_current_has_child_and_history_preserves_parent_only() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-table-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "int32", true, None),
                TableColumnRow::new(ColumnId(2), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(3), "a", "int32", true, Some(ColumnId(2))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(4),
            "b",
            "int32",
            true,
            Some(ColumnId(2)),
        )],
    )
    .unwrap();
    let add_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, add_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(historical.columns.iter().all(|column| column.name != "b"));
    assert!(current.columns.iter().any(|column| column.name == "b"
        && column.parent_id == Some(ColumnId(2))
        && column.column_type == "int32"));
}

#[test]
fn given_append_after_concurrent_add_column_when_checked_then_schema_conflict_blocks_publish() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let attempt = CommitAttemptId(901);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(2),
            "amount",
            "int32",
            true,
            None,
        )],
    )
    .unwrap();
    let schema_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = commit_append_data_file(
        &mut kv,
        catalog,
        attempt,
        base_snapshot.order,
        schema_snapshot.order,
        DataFileRow::new(
            DataFileId(77),
            table,
            "main/orders/blocked.parquet",
            1,
            256,
            CatalogOrderId::uuid_v7(10_000),
        ),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableSchemaConflict {
            table_id,
            changed_at
        } if table_id == table && changed_at == schema_snapshot.order
    ));
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
    assert_eq!(load_commit_attempt(&kv, catalog, attempt).unwrap(), None);
}

#[test]
fn given_merge_compaction_after_unrelated_concurrent_append_when_checked_then_replaces_only_source()
{
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
                "main/orders/base.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(2),
            table,
            "main/orders/concurrent.parquet",
            5,
            1024,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_merge_adjacent_data_files_with_conflict_check(
        &mut kv,
        catalog,
        base_snapshot.order,
        concurrent_snapshot.order,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(3),
                    table,
                    "main/orders/replacement.parquet",
                    15,
                    3072,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(2))
    );
    assert!(current.iter().any(|file| file.data_file_id == DataFileId(3)
        && file.validity.begin_order > concurrent_snapshot.order));
    assert!(
        !current
            .iter()
            .any(|file| file.data_file_id == DataFileId(1))
    );
}

#[test]
fn given_compacted_sources_are_cleaned_when_listing_insertions_then_replacement_preserves_history()
{
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
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    let first_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/file-0002.parquet",
                11,
                4096,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10),
        ],
    )
    .unwrap();
    let second_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(3),
                    table,
                    "main/orders/compacted.parquet",
                    21,
                    6144,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let compacted_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let insertions_before_cleanup = insertion_files(
        &kv,
        catalog,
        table,
        first_snapshot.order,
        compacted_snapshot.order,
    )
    .unwrap();
    assert_eq!(insertions_before_cleanup.len(), 1);
    assert_eq!(insertions_before_cleanup[0].data_file_id, DataFileId(3));

    expire_snapshots(
        &mut kv,
        catalog,
        &[
            ducklake_catalog::RawSnapshotSequence(1),
            ducklake_catalog::RawSnapshotSequence(2),
        ],
    )
    .unwrap();

    assert_eq!(
        remove_old_data_files(&mut kv, catalog, &[DataFileId(1), DataFileId(2)])
            .unwrap()
            .len(),
        2
    );

    let insertions = insertion_files(
        &kv,
        catalog,
        table,
        first_snapshot.order,
        compacted_snapshot.order,
    )
    .unwrap();

    assert_eq!(insertions.len(), 1);
    assert_eq!(insertions[0].data_file_id, DataFileId(3));
    assert_eq!(insertions[0].validity.begin_order, first_snapshot.order);
    assert_eq!(insertions[0].max_partial_order, Some(second_snapshot.order));
}

#[test]
fn given_merge_replacement_has_explicit_visibility_when_committed_then_it_is_not_overwritten() {
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
                "main/orders/first.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    let first_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/second.parquet",
                11,
                4096,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10),
        ],
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(3),
                table,
                "main/orders/third.parquet",
                12,
                8192,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(21),
        ],
    )
    .unwrap();
    let third_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(2), DataFileId(3)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(4),
                    table,
                    "main/orders/explicit.parquet",
                    21,
                    6144,
                    first_snapshot.order,
                )
                .with_row_id_start(10)
                .with_max_partial_order(Some(third_snapshot.order)),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let files = list_current_data_files(&kv, catalog, table).unwrap();
    let replacement = files
        .iter()
        .find(|file| file.data_file_id == DataFileId(4))
        .unwrap();
    assert_eq!(replacement.validity.begin_order, first_snapshot.order);
    assert_eq!(replacement.max_partial_order, Some(third_snapshot.order));
}

#[test]
fn given_rewrite_delete_compaction_after_unrelated_concurrent_append_when_checked_then_replaces_only_source()
 {
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
                "main/orders/base.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            DataFileId(1),
            "main/orders/base-delete.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(2),
            table,
            "main/orders/concurrent.parquet",
            5,
            1024,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_rewrite_delete_data_files_with_conflict_check(
        &mut kv,
        catalog,
        base_snapshot.order,
        concurrent_snapshot.order,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(3),
                    table,
                    "main/orders/rewrite.parquet",
                    9,
                    1920,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(2))
    );
    assert!(current.iter().any(|file| file.data_file_id == DataFileId(3)
        && file.validity.begin_order > concurrent_snapshot.order));
    assert!(
        !current
            .iter()
            .any(|file| file.data_file_id == DataFileId(1))
    );
}

#[test]
fn given_old_delete_file_cleanup_when_removed_then_delete_change_index_is_removed() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let data_file = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap()
    .remove(0);
    let first_delete = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            data_file.data_file_id,
            "main/orders/file-0001-delete-a.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap()
    .remove(0);
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(12),
            data_file.data_file_id,
            "main/orders/file-0001-delete-b.parquet",
            2,
            256,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    expire_snapshots(
        &mut kv,
        catalog,
        &[ducklake_catalog::RawSnapshotSequence(2)],
    )
    .unwrap();

    let removed =
        remove_old_delete_files(&mut kv, catalog, &[first_delete.delete_file_id]).unwrap();

    assert_eq!(removed.len(), 1);
    assert!(
        ducklake_catalog::OrderedCatalogKv::get(
            &kv,
            &table_delete_file_change_key(
                catalog,
                table,
                first_delete.validity.begin_order,
                first_delete.delete_file_id,
            )
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn given_current_and_historical_files_when_listing_known_orphan_cleanup_files_then_all_catalog_files_are_known()
 {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let original = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap()
    .remove(0);
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(11),
            original.data_file_id,
            "main/orders/file-0001-delete.parquet",
            2,
            128,
            delete_snapshot.order,
        )],
    )
    .unwrap();
    let compaction_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![original.data_file_id],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    8,
                    1024,
                    compaction_snapshot.order,
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let rendered = list_known_files_for_orphan_cleanup(&kv, catalog)
        .unwrap()
        .into_iter()
        .map(|row| match row {
            ducklake_catalog::KnownCleanupFileRow::Data(file) => {
                format!("data:{}:{}", file.data_file_id.0, file.path)
            }
            ducklake_catalog::KnownCleanupFileRow::Delete {
                delete_file,
                table_id,
            } => format!(
                "delete:{}:{}:{}",
                delete_file.delete_file_id.0, table_id.0, delete_file.path
            ),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rendered,
        vec![
            "data:1:main/orders/file-0001.parquet",
            "data:2:main/orders/file-0002.parquet",
            "delete:11:10:main/orders/file-0001-delete.parquet",
        ]
    );
}

#[test]
fn given_old_snapshot_when_expired_then_time_travel_fails_closed_and_current_read_survives() {
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
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(2),
            table,
            "main/orders/file-0002.parquet",
            11,
            4096,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    let before = list_snapshots(&kv, catalog).unwrap();
    assert_eq!(
        before
            .iter()
            .map(|snapshot| snapshot.sequence)
            .collect::<Vec<_>>(),
        vec![
            ducklake_catalog::RawSnapshotSequence(0),
            ducklake_catalog::RawSnapshotSequence(1),
            ducklake_catalog::RawSnapshotSequence(2)
        ]
    );

    let expired = expire_snapshots(
        &mut kv,
        catalog,
        &[ducklake_catalog::RawSnapshotSequence(1)],
    )
    .unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(
        expired[0].sequence,
        ducklake_catalog::RawSnapshotSequence(1)
    );

    assert!(
        list_snapshots(&kv, catalog)
            .unwrap()
            .iter()
            .all(|snapshot| snapshot.sequence != RawSnapshotSequence(1))
    );
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().sequence,
        ducklake_catalog::RawSnapshotSequence(2)
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap().len(),
        2
    );
}

#[test]
fn given_multiple_internal_orders_for_public_snapshot_when_lookup_then_latest_order_is_returned() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let first = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(10),
        ducklake_catalog::RawSnapshotSequence(7),
        100,
    );
    let second = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(20),
        ducklake_catalog::RawSnapshotSequence(7),
        200,
    );
    let latest = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(30),
        ducklake_catalog::RawSnapshotSequence(8),
        300,
    );
    let mut batch = ducklake_catalog::KvBatch::new();
    for snapshot in [&first, &second, &latest] {
        batch.put(snapshot_key(catalog, snapshot.order), snapshot.encode());
        batch.put(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot.sequence.to_be_bytes().to_vec(),
        );
    }
    kv.commit(batch).unwrap();

    assert_eq!(
        snapshot_by_raw_sequence(&kv, catalog, RawSnapshotSequence(7))
            .unwrap()
            .unwrap()
            .order,
        second.order
    );

    let expired = expire_snapshots(
        &mut kv,
        catalog,
        &[ducklake_catalog::RawSnapshotSequence(7)],
    )
    .unwrap();
    assert_eq!(expired.len(), 2);
    assert!(
        list_snapshots(&kv, catalog)
            .unwrap()
            .iter()
            .all(|snapshot| snapshot.sequence != RawSnapshotSequence(7))
    );
}

#[test]
fn given_latest_snapshot_when_expired_then_mutation_is_rejected() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    let error = expire_snapshots(
        &mut kv,
        catalog,
        &[ducklake_catalog::RawSnapshotSequence(0)],
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot expire latest snapshot 0")
    );
    assert!(
        snapshot_by_raw_sequence(&kv, catalog, RawSnapshotSequence(0))
            .unwrap()
            .is_some()
    );
}

#[test]
fn given_snapshot_timestamp_index_when_listing_older_then_scan_returns_only_old_snapshots() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let old = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(10),
        ducklake_catalog::RawSnapshotSequence(1),
        100,
    );
    let boundary = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(20),
        ducklake_catalog::RawSnapshotSequence(2),
        200,
    );
    let latest = ducklake_catalog::SnapshotRow::with_created_at_micros(
        CatalogOrderId::uuid_v7(30),
        ducklake_catalog::RawSnapshotSequence(3),
        300,
    );
    let mut batch = ducklake_catalog::KvBatch::new();
    for snapshot in [&old, &boundary, &latest] {
        batch.put(snapshot_key(catalog, snapshot.order), snapshot.encode());
        batch.put(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot.sequence.to_be_bytes().to_vec(),
        );
    }
    kv.commit(batch).unwrap();

    let older = list_snapshots_older_than(&kv, catalog, 200).unwrap();

    assert_eq!(older, vec![old]);
}

#[test]
fn given_compacted_file_when_referenced_snapshot_expires_then_cleanup_removes_metadata() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let amount = ColumnId(2);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(1),
            table,
            "main/orders/file-0001.parquet",
            10,
            2048,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            DataFileId(1),
            table,
            amount,
            0,
            Some("1".into()),
            Some("10".into()),
        ),
    )
    .unwrap();
    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    10,
                    2048,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let cleanup_before_expiry = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup_before_expiry.len(), 1);
    assert_eq!(cleanup_before_expiry[0].data_file_id, DataFileId(1));

    expire_snapshots(
        &mut kv,
        catalog,
        &[ducklake_catalog::RawSnapshotSequence(1)],
    )
    .unwrap();

    let cleanup = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].data_file_id, DataFileId(1));

    let removed = remove_old_data_files(&mut kv, catalog, &[DataFileId(1)]).unwrap();
    assert_eq!(removed.len(), 1);
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap().len(),
        1
    );
    assert!(
        list_file_column_stats_for_table_column(&kv, catalog, table, amount)
            .unwrap()
            .iter()
            .all(|row| row.data_file_id != DataFileId(1))
    );
}

#[test]
fn given_rewritten_delete_file_when_snapshots_expire_then_delete_cleanup_removes_metadata() {
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
                "main/orders/file-0001.parquet",
                10,
                2048,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(10),
            DataFileId(1),
            "main/orders/file-0001-delete.parquet",
            2,
            512,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/orders/file-0002.parquet",
                    8,
                    2048,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let cleanup_before_expiry = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup_before_expiry.len(), 1);
    assert_eq!(
        cleanup_before_expiry[0].delete_file.delete_file_id,
        DeleteFileId(10)
    );

    expire_snapshots(
        &mut kv,
        catalog,
        &[
            ducklake_catalog::RawSnapshotSequence(1),
            ducklake_catalog::RawSnapshotSequence(2),
        ],
    )
    .unwrap();

    let cleanup = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].delete_file.delete_file_id, DeleteFileId(10));
    assert_eq!(cleanup[0].table_id, table);

    let removed = remove_old_delete_files(&mut kv, catalog, &[DeleteFileId(10)]).unwrap();
    assert_eq!(removed.len(), 1);
    assert!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog).unwrap().len(),
        1
    );
}

#[test]
fn given_inline_payload_flushed_then_cleanup_waits_until_old_snapshot_expires() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t1\tone\n".to_vec())
        .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        inline_snapshot.sequence,
        ducklake_catalog::RawSnapshotSequence(1)
    );

    commit_append_data_files_with_inline_flushes(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(20),
            table,
            "main/inline/file-0001.parquet",
            1,
            1024,
            CatalogOrderId::uuid_v7(0),
        )],
        &[InlineTableFlush::new(
            table,
            schema,
            inline_snapshot.sequence,
        )],
    )
    .unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let old_payloads =
        list_inline_table_payloads_at(&kv, catalog, table, schema, inline_snapshot.order).unwrap();
    assert_eq!(old_payloads.len(), 1);
    assert_eq!(old_payloads[0].payload, b"row\t1\tone\n");
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, latest.order)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );

    expire_snapshots(&mut kv, catalog, &[inline_snapshot.sequence]).unwrap();

    let cleanup = list_old_inline_table_payloads_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].id.table_id, table);
    assert_eq!(cleanup[0].id.schema_id, schema);
    assert_eq!(cleanup[0].id.begin_order, inline_snapshot.order);
    assert_eq!(cleanup[0].chunk_count, 1);

    let removed = remove_old_inline_table_payloads(&mut kv, catalog, &[cleanup[0].id]).unwrap();
    assert_eq!(removed.len(), 1);
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_inline_rows_deleted_then_history_is_preserved_and_delete_change_is_recorded() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    register_inline_table_payload(
        &mut kv,
        catalog,
        table,
        schema,
        b"row\t1\ti:1\ts:6f6e65\nrow\t2\ti:2\ts:74776f\nrow\t3\ti:3\ts:7468726565\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let commit = commit_delete_inline_table_rows(&mut kv, catalog, table, schema, &[2]).unwrap();
    assert_eq!(commit.deleted_row_count, 1);
    assert_eq!(commit.rewritten_payload_count, 0);
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let historical =
        list_inline_table_payloads_at(&kv, catalog, table, schema, inline_snapshot.order).unwrap();
    assert_eq!(historical.len(), 1);
    assert!(
        historical[0]
            .payload
            .windows(b"row\t2".len())
            .any(|window| window == b"row\t2")
    );

    let deletions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        inline_snapshot.order,
        delete_snapshot.order,
        InlineRowChangeKind::Deleted,
    )
    .unwrap();
    assert_eq!(deletions.len(), 1);
    assert_eq!(deletions[0].change.row_id, 2);
}

#[test]
fn given_inline_insert_and_delete_when_listing_inline_cdf_then_only_changed_rows_are_returned() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let initial_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    register_inline_table_payload(
        &mut kv,
        catalog,
        table,
        schema,
        b"row\t1\ti:1\ts:6f6e65\nrow\t2\ti:2\ts:74776f\nrow\t3\ti:3\ts:7468726565\n".to_vec(),
    )
    .unwrap();
    let insert_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        initial_snapshot.order,
        insert_snapshot.order,
        InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert_eq!(insertions.len(), 3);
    assert_eq!(insertions[0].change.row_id, 1);
    assert_eq!(insertions[1].change.row_id, 2);
    assert_eq!(insertions[2].change.row_id, 3);

    commit_delete_inline_table_rows(&mut kv, catalog, table, schema, &[2]).unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let delete_window_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        insert_snapshot.order,
        delete_snapshot.order,
        InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert!(
        delete_window_insertions
            .iter()
            .all(|change| change.change.order != delete_snapshot.order),
        "delete rewrite must not report surviving rows as inserts"
    );
    let deletions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table,
        schema,
        insert_snapshot.order,
        delete_snapshot.order,
        InlineRowChangeKind::Deleted,
    )
    .unwrap();
    assert_eq!(deletions.len(), 1);
    assert_eq!(deletions[0].change.row_id, 2);
    assert_eq!(
        std::str::from_utf8(&deletions[0].payload).unwrap(),
        "row\t2\ti:2\ts:74776f\n"
    );
}

#[test]
fn given_multiple_inline_payloads_flushed_then_cleanup_waits_until_all_visible_snapshots_expire() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t1\tone\n".to_vec())
        .unwrap();
    let first_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t2\ttwo\n".to_vec())
        .unwrap();
    let second_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    commit_append_data_files_with_inline_flushes(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(20),
            table,
            "main/inline/file-0001.parquet",
            2,
            1024,
            CatalogOrderId::uuid_v7(0),
        )],
        &[InlineTableFlush::new(
            table,
            schema,
            second_inline_snapshot.sequence,
        )],
    )
    .unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, first_inline_snapshot.order)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, second_inline_snapshot.order)
            .unwrap()
            .len(),
        2
    );
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, flush_snapshot.order)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );

    expire_snapshots(&mut kv, catalog, &[first_inline_snapshot.sequence]).unwrap();
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );

    expire_snapshots(&mut kv, catalog, &[second_inline_snapshot.sequence]).unwrap();
    let cleanup = list_old_inline_table_payloads_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup.len(), 2);
    assert!(cleanup.iter().all(|row| row.chunk_count == 1));

    let cleanup_ids: Vec<_> = cleanup.iter().map(|row| row.id).collect();
    let removed = remove_old_inline_table_payloads(&mut kv, catalog, &cleanup_ids).unwrap();
    assert_eq!(removed.len(), 2);
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, flush_snapshot.order)
            .unwrap()
            .is_empty()
    );
}
