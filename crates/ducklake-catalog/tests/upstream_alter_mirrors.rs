use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnDefaultChange, ColumnDrop, ColumnId, ColumnRename,
    ColumnTypeChange, DataFileId, DataFileRow, FakeOrderedCatalogKv, FileColumnStatsRow, SchemaId,
    TableColumnRow, TableId, TableRename, TableRow, commit_append_data_files,
    commit_append_table_columns, commit_change_table_column_defaults,
    commit_change_table_column_types, commit_create_table_row,
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files,
    commit_drop_table_columns, commit_drop_tables, commit_rename_table_columns,
    commit_rename_tables, initialize_catalog_if_absent, latest_snapshot,
    list_current_data_files_with_deletes, list_file_column_stats_for_table_column, list_tables_at,
    load_table_at,
};

#[test]
fn mirrors_add_column_default_stats_test_default_and_explicit_stats_are_saved() {
    // Mirrors: third_party/ducklake/test/sql/alter/add_column_default_stats.test
    //
    // Storage contract:
    // - When DuckLake adds a column with a default while rows are being written, it asks storage
    //   to persist column stats for the added column.
    // - Those stats must reflect default values and later explicit values for the same column.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "t1",
        vec![column(1, "a", "integer")],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        TableId(1),
        vec![column(2, "b", "integer").with_default_metadata(Some("42"), Some("42"), "literal")],
    )
    .unwrap();
    append_file_with_stats(
        &mut kv,
        catalog,
        data_file(1, TableId(1), 0, 3),
        vec![stats(1, TableId(1), 2, 0, "42", "42")],
    );
    assert_column_stats(&kv, catalog, TableId(1), ColumnId(2), &[("42", "42")]);

    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "t2",
        vec![column(1, "a", "integer")],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        TableId(2),
        vec![column(2, "b", "integer").with_default_metadata(Some("99"), Some("99"), "literal")],
    )
    .unwrap();
    append_file_with_stats(
        &mut kv,
        catalog,
        data_file(2, TableId(2), 0, 4),
        vec![stats(2, TableId(2), 2, 0, "99", "200")],
    );
    append_file_with_stats(
        &mut kv,
        catalog,
        data_file(3, TableId(2), 4, 1),
        vec![stats(3, TableId(2), 2, 0, "20", "20")],
    );
    assert_column_stats(
        &kv,
        catalog,
        TableId(2),
        ColumnId(2),
        &[("99", "200"), ("20", "20")],
    );

    create_table(
        &mut kv,
        catalog,
        TableId(3),
        "t_del",
        vec![column(1, "a", "integer")],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        TableId(3),
        vec![column(2, "b", "integer").with_default_metadata(Some("999"), Some("999"), "literal")],
    )
    .unwrap();
    append_file_with_stats(
        &mut kv,
        catalog,
        data_file(4, TableId(3), 0, 1),
        vec![stats(4, TableId(3), 2, 0, "5", "5")],
    );
    assert_column_stats(&kv, catalog, TableId(3), ColumnId(2), &[("5", "5")]);
}

#[test]
fn mirrors_add_column_nested_test_nested_columns_are_appended_in_order() {
    // Mirrors: third_party/ducklake/test/sql/alter/add_column_nested.test
    //
    // Storage contract:
    // - DuckLake asks storage to append top-level nested columns after existing data.
    // - Current table metadata must return the original struct column followed by both new
    //   columns in their committed order.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(1, "col1", "struct(i int, j int)")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1)]).unwrap();

    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(2, "new_col2", "int[]")],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 1, 1)]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(3, "new_col3", "struct(k int, v int)")],
    )
    .unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, table, 2, 1)]).unwrap();

    assert_current_columns(
        &kv,
        catalog,
        table,
        &[
            ("col1", "struct(i int, j int)"),
            ("new_col2", "int[]"),
            ("new_col3", "struct(k int, v int)"),
        ],
    );
}

#[test]
fn mirrors_drop_column_nested_test_dropped_nested_columns_do_not_reappear_after_adds() {
    // Mirrors: third_party/ducklake/test/sql/alter/drop_column_nested.test
    //
    // Storage contract:
    // - Dropped columns must be absent from current table metadata.
    // - New columns with similar names are new column ids, not resurrected dropped columns.
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
            column(1, "col1", "struct(i int, j int)"),
            column(2, "col2", "struct(k int, v int)"),
            column(3, "col3", "int[]"),
        ],
    );

    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(2))]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(4, "new_col2", "int[]")],
    )
    .unwrap();
    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(3))]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(5, "new_col3", "struct(k int, v int)")],
    )
    .unwrap();

    assert_current_columns(
        &kv,
        catalog,
        table,
        &[
            ("col1", "struct(i int, j int)"),
            ("new_col2", "int[]"),
            ("new_col3", "struct(k int, v int)"),
        ],
    );
}

#[test]
fn mirrors_expire_snapshot_bug_test_renamed_table_keeps_files_until_final_drop() {
    // Mirrors: third_party/ducklake/test/sql/alter/expire_snapshot_bug.test
    //
    // Storage contract:
    // - Table rename changes table metadata but not table id ownership of data files.
    // - Data files remain visible through multiple renames.
    // - Dropping the final table expires the current files for that table id.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "a",
        vec![column(1, "i", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1)]).unwrap();
    commit_rename_tables(&mut kv, catalog, &[TableRename::new(table, "b")]).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 1, 1)]).unwrap();

    assert_current_table_name(&kv, catalog, table, "b");
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, table)
            .unwrap()
            .len(),
        2
    );
    commit_rename_tables(&mut kv, catalog, &[TableRename::new(table, "c")]).unwrap();
    assert_current_table_name(&kv, catalog, table, "c");
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, table)
            .unwrap()
            .len(),
        2
    );

    let dropped = commit_drop_tables(&mut kv, catalog, &[table]).unwrap();
    assert_eq!(dropped[0].expired_data_file_count, 2);
    assert!(
        list_current_data_files_with_deletes(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn mirrors_mixed_alter2_test_default_column_drop_keeps_remaining_column_order() {
    // Mirrors: third_party/ducklake/test/sql/alter/mixed_alter2.test
    //
    // Storage contract:
    // - Consecutive ADD COLUMN operations append nullable/default/nested columns.
    // - Dropping the default column removes only that column version and leaves the surrounding
    //   columns in order.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "tbl",
        vec![column(1, "col1", "integer")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1)]).unwrap();
    commit_append_table_columns(&mut kv, catalog, table, vec![column(2, "col2", "varchar")])
        .unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(3, "new_column", "varchar").with_default_metadata(
            Some("my_default"),
            Some("my_default"),
            "literal",
        )],
    )
    .unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(4, "nested_column", "struct(i integer)")],
    )
    .unwrap();
    assert_current_columns(
        &kv,
        catalog,
        table,
        &[
            ("col1", "integer"),
            ("col2", "varchar"),
            ("new_column", "varchar"),
            ("nested_column", "struct(i integer)"),
        ],
    );

    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(3))]).unwrap();
    assert_current_columns(
        &kv,
        catalog,
        table,
        &[
            ("col1", "integer"),
            ("col2", "varchar"),
            ("nested_column", "struct(i integer)"),
        ],
    );
}

#[test]
fn mirrors_multi_alter_same_column_transaction_test_final_column_identity_is_saved() {
    // Mirrors: third_party/ducklake/test/sql/alter/multi_alter_same_column_transaction.test
    //
    // Storage contract:
    // - Multiple operations on the same column in one SQL transaction arrive as final table
    //   metadata: default metadata and final column name must both be persisted.
    // - ADD+RENAME+DROP leaves no trace of the temporary column in current metadata.
    // - Two added columns renamed in one transaction must keep their final names and ids.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "t",
        vec![column(1, "id", "integer"), column(2, "col1", "integer")],
    );
    commit_change_table_column_defaults(
        &mut kv,
        catalog,
        &[ColumnDefaultChange::new(
            TableId(1),
            column(2, "col1", "integer").with_default_metadata(Some("42"), Some("42"), "literal"),
        )],
    )
    .unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            TableId(1),
            column(2, "col1_final", "integer"),
        )],
    )
    .unwrap();
    let table = current_table(&kv, catalog, TableId(1));
    assert_eq!(table.columns[1].name, "col1_final");
    assert_eq!(table.columns[1].default_value.as_deref(), Some("42"));

    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "add_rename_drop_test",
        vec![column(1, "id", "integer"), column(2, "val", "varchar")],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        TableId(2),
        vec![column(3, "tmp_col", "integer")],
    )
    .unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            TableId(2),
            column(3, "renamed_col", "integer"),
        )],
    )
    .unwrap();
    commit_drop_table_columns(
        &mut kv,
        catalog,
        &[ColumnDrop::new(TableId(2), ColumnId(3))],
    )
    .unwrap();
    assert_current_columns(
        &kv,
        catalog,
        TableId(2),
        &[("id", "integer"), ("val", "varchar")],
    );

    create_table(
        &mut kv,
        catalog,
        TableId(3),
        "add_rename_two_cols_test",
        vec![column(1, "id", "integer")],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        TableId(3),
        vec![column(2, "col_a", "varchar"), column(3, "col_b", "integer")],
    )
    .unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[
            ColumnRename::new(TableId(3), column(2, "col_a_renamed", "varchar")),
            ColumnRename::new(TableId(3), column(3, "col_b_renamed", "integer")),
        ],
    )
    .unwrap();
    assert_current_columns(
        &kv,
        catalog,
        TableId(3),
        &[
            ("id", "integer"),
            ("col_a_renamed", "varchar"),
            ("col_b_renamed", "integer"),
        ],
    );
}

#[test]
fn mirrors_rename_table_dbt_workload_test_swapping_names_in_one_commit_preserves_files() {
    // Mirrors: third_party/ducklake/test/sql/alter/rename_table_dbt_workload.test
    //
    // Storage contract:
    // - In one transaction DuckLake creates a temp table, renames the original to backup, and
    //   renames the temp table to the original name.
    // - Current table listing must contain only the final names, with data files attached to the
    //   original table ids.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(1),
        "my_table",
        vec![column(1, "range", "bigint")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, TableId(1), 0, 42)]).unwrap();
    create_table(
        &mut kv,
        catalog,
        TableId(2),
        "my_table_tmp",
        vec![column(1, "range", "bigint")],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(2, TableId(2), 0, 84)]).unwrap();

    commit_rename_tables(
        &mut kv,
        catalog,
        &[
            TableRename::new(TableId(1), "my_table_backup"),
            TableRename::new(TableId(2), "my_table"),
        ],
    )
    .unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let names = list_tables_at(&kv, catalog, latest.order)
        .unwrap()
        .into_iter()
        .map(|table| table.name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["my_table_backup", "my_table"]);
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, TableId(1)).unwrap()[0]
            .data_file
            .record_count,
        42
    );
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, TableId(2)).unwrap()[0]
            .data_file
            .record_count,
        84
    );
}

#[test]
fn mirrors_struct_evolution_nested_test_type_changes_preserve_current_nested_shape() {
    // Mirrors: third_party/ducklake/test/sql/alter/struct_evolution_nested.test
    //
    // Storage contract:
    // - Repeated SET DATA TYPE operations replace the column type in table metadata.
    // - Adding a new deeply nested column after the type evolution appends it without changing
    //   the final evolved type of the original column.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![column(
            1,
            "col1",
            "struct(i int, j struct(c1 tinyint, c2 int[]), k int)",
        )],
    );
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1)]).unwrap();
    let initial = latest_snapshot(&kv, catalog).unwrap().unwrap();

    for (file_id, column_type) in [
        (
            2,
            "struct(i int, j struct(c1 int, c2 int[], c3 tinyint), k int)",
        ),
        (3, "struct(j struct(c2 int[]), k int)"),
        (
            4,
            "struct(j struct(c2 int[], x struct(a int, b int, c int)), k int)",
        ),
        (5, "struct(k int)"),
    ] {
        commit_change_table_column_types(
            &mut kv,
            catalog,
            &[ColumnTypeChange::new(table, column(1, "col1", column_type))],
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(file_id, table, file_id, 1)],
        )
        .unwrap();
    }
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(
            2,
            "col2",
            "struct(i int, j struct(c1 tinyint, c2 int[]), k int)",
        )],
    )
    .unwrap();

    assert_current_columns(
        &kv,
        catalog,
        table,
        &[
            ("col1", "struct(k int)"),
            (
                "col2",
                "struct(i int, j struct(c1 tinyint, c2 int[]), k int)",
            ),
        ],
    );
    let historical = load_table_at(&kv, catalog, table, initial.order)
        .unwrap()
        .unwrap();
    assert_eq!(
        historical.columns[0].column_type,
        "struct(i int, j struct(c1 tinyint, c2 int[]), k int)"
    );
}

#[test]
fn mirrors_struct_evolution_nested_alter_test_nested_column_add_drop_updates_leaf_metadata() {
    // Mirrors: third_party/ducklake/test/sql/alter/struct_evolution_nested_alter.test
    //
    // Storage contract:
    // - Nested ADD COLUMN appends leaf metadata under the correct parent id.
    // - Nested DROP COLUMN removes only the requested leaf/subtree metadata.
    // - Adding a separate top-level nested column after those changes does not resurrect dropped
    //   leaf columns under col1.
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
            column(1, "col1", "struct"),
            child_column(2, "i", "integer", 1),
            child_column(3, "j", "struct", 1),
            child_column(4, "c1", "integer", 3),
            child_column(5, "c2", "int[]", 3),
            child_column(6, "k", "integer", 1),
        ],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![child_column(7, "c3", "tinyint", 3)],
    )
    .unwrap();
    commit_drop_table_columns(
        &mut kv,
        catalog,
        &[
            ColumnDrop::new(table, ColumnId(2)),
            ColumnDrop::new(table, ColumnId(4)),
            ColumnDrop::new(table, ColumnId(7)),
        ],
    )
    .unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![
            child_column(8, "x", "struct", 3),
            child_column(9, "a", "integer", 8),
            child_column(10, "b", "integer", 8),
            child_column(11, "c", "integer", 8),
        ],
    )
    .unwrap();
    commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(3))]).unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![column(
            12,
            "col2",
            "struct(i int, j struct(c1 tinyint, c2 int[]), k int)",
        )],
    )
    .unwrap();

    let table = current_table(&kv, catalog, table);
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str(), column.parent_id))
            .collect::<Vec<_>>(),
        vec![
            (ColumnId(1), "col1", None),
            (ColumnId(6), "k", Some(ColumnId(1))),
            (ColumnId(12), "col2", None),
        ]
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

fn child_column(id: u64, name: &str, column_type: &str, parent_id: u64) -> TableColumnRow {
    TableColumnRow::new(
        ColumnId(id),
        name,
        column_type,
        true,
        Some(ColumnId(parent_id)),
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

fn stats(
    file_id: u64,
    table: TableId,
    column_id: u64,
    null_count: u64,
    min: &str,
    max: &str,
) -> FileColumnStatsRow {
    FileColumnStatsRow::new(
        DataFileId(file_id),
        table,
        ColumnId(column_id),
        null_count,
        Some(min.to_owned()),
        Some(max.to_owned()),
    )
}

fn append_file_with_stats(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    data_file: DataFileRow,
    stats: Vec<FileColumnStatsRow>,
) {
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files(
        kv,
        catalog,
        vec![data_file],
        Vec::new(),
        &[],
        Vec::new(),
        Vec::new(),
        stats,
        Vec::new(),
    )
    .unwrap();
}

fn current_table(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> TableRow {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    load_table_at(kv, catalog, table, latest.order)
        .unwrap()
        .unwrap()
}

fn assert_current_columns(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    expected: &[(&str, &str)],
) {
    let table = current_table(kv, catalog, table);
    let columns = table
        .columns
        .iter()
        .filter(|column| column.parent_id.is_none())
        .map(|column| (column.name.as_str(), column.column_type.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(columns, expected);
}

fn assert_current_table_name(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    expected: &str,
) {
    assert_eq!(current_table(kv, catalog, table).name, expected);
}

fn assert_column_stats(
    kv: &FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    column: ColumnId,
    expected: &[(&str, &str)],
) {
    let stats = list_file_column_stats_for_table_column(kv, catalog, table, column)
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.min_value.unwrap_or_default(),
                row.max_value.unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(min, max)| ((*min).to_owned(), (*max).to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(stats, expected);
}
