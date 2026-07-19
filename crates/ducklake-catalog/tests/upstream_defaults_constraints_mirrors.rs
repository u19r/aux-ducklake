use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnDefaultChange, ColumnId, DataFileId, DataFileRow,
    FakeOrderedCatalogKv, FileColumnStatsRow, InlineRowChangeKind, InlineTableFlush,
    OrderedCatalogKv, SchemaId, TableColumnRow, TableId, TableRow, commit_append_table_columns,
    commit_change_table_column_defaults, commit_create_table_row, commit_data_mutation,
    commit_data_mutation_with_details, initialize_catalog_if_absent, keys::inline_table_end_key,
    latest_snapshot, list_file_column_stats_for_table_column, list_inline_row_changes,
    load_table_at, register_inline_table_payload_with_table,
};

#[test]
fn mirrors_not_null_test_column_nullability_is_persisted_in_table_metadata() {
    // Mirrors: third_party/ducklake/test/sql/constraints/not_null.test
    //
    // Storage contract:
    // - DuckLake stores NOT NULL as `nulls_allowed = false` in table-column metadata.
    // - A nullable sibling column remains independently nullable.
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
            TableColumnRow::new(ColumnId(1), "i", "integer", false, None),
            TableColumnRow::new(ColumnId(2), "j", "integer", true, None),
        ],
    );

    let current = current_table(&kv, catalog, table);
    assert_eq!(
        current
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.nulls_allowed))
            .collect::<Vec<_>>(),
        vec![("i", false), ("j", true)]
    );
}

#[test]
fn mirrors_data_inlining_flush_test_flush_ends_payload_but_preserves_insert_change_feed() {
    // Mirrors: third_party/ducklake/test/sql/data_inlining/data_inlining_flush.test
    //
    // Storage contract:
    // - DuckLake saves ten one-row inline payloads across ten snapshots.
    // - Flush writes a data file and ends the inline payloads.
    // - The inline insertion change feed remains queryable across the original insert snapshots.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![TableColumnRow::new(ColumnId(1), "i", "integer", true, None)],
    );

    let mut first_insert = None;
    let mut insert_orders = Vec::new();
    let mut last_insert = None;
    for row_id in 0..10 {
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_with_inline(table, schema),
            schema,
            inline_payload(&[(row_id, row_id as i64)]),
        )
        .unwrap();
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        first_insert.get_or_insert(snapshot.clone());
        insert_orders.push(snapshot.order);
        last_insert = Some(snapshot);
    }
    let first_insert = first_insert.unwrap();
    let last_insert = last_insert.unwrap();

    commit_data_mutation(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 10)],
        Vec::new(),
        &[InlineTableFlush::new(table, schema, last_insert.sequence)],
    )
    .unwrap();
    let flush = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_inline_row_changes(&kv, catalog, table, first_insert.order, last_insert.order)
            .unwrap()
            .into_iter()
            .filter(|change| change.kind == InlineRowChangeKind::Inserted)
            .map(|change| change.row_id)
            .collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
    for begin_order in insert_orders {
        assert!(
            OrderedCatalogKv::get(
                &kv,
                &inline_table_end_key(catalog, table, flush.order, schema, begin_order)
            )
            .unwrap()
            .is_some(),
            "flush snapshot must end inline payload beginning at {begin_order:?}"
        );
    }
}

#[test]
fn mirrors_add_column_with_default_test_initial_and_current_defaults_are_versioned() {
    // Mirrors: third_party/ducklake/test/sql/default/add_column_with_default.test
    //
    // Storage contract:
    // - ADD COLUMN with DEFAULT records both the initial default and current default.
    // - ALTER SET/DROP DEFAULT creates later column versions without changing the initial default.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "test",
        vec![TableColumnRow::new(ColumnId(1), "i", "integer", true, None)],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![
            TableColumnRow::new(ColumnId(2), "j", "integer", true, None).with_default_metadata(
                Some("42"),
                Some("42"),
                "literal",
            ),
        ],
    )
    .unwrap();
    commit_change_table_column_defaults(
        &mut kv,
        catalog,
        &[
            ColumnDefaultChange::new(
                table,
                TableColumnRow::new(ColumnId(1), "i", "integer", true, None).with_default_metadata(
                    None::<String>,
                    Some("1000"),
                    "literal",
                ),
            ),
            ColumnDefaultChange::new(
                table,
                TableColumnRow::new(ColumnId(2), "j", "integer", true, None).with_default_metadata(
                    Some("42"),
                    None::<String>,
                    "literal",
                ),
            ),
        ],
    )
    .unwrap();

    let current = current_table(&kv, catalog, table);
    assert_eq!(current.columns[0].default_value.as_deref(), Some("1000"));
    assert_eq!(current.columns[1].initial_default.as_deref(), Some("42"));
    assert_eq!(current.columns[1].default_value, None);
}

#[test]
fn mirrors_all_types_column_default_stats_test_default_stats_are_saved_for_added_columns() {
    // Mirrors: third_party/ducklake/test/sql/default/all_types_column_default_stats.test
    //
    // Storage contract:
    // - DuckLake calculates min/max/null stats for added default columns and sends them to storage.
    // - Storage must persist those stats verbatim for every column id.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table(
        &mut kv,
        catalog,
        table,
        "t_all",
        vec![
            TableColumnRow::new(ColumnId(1), "c_bool", "boolean", true, None),
            TableColumnRow::new(ColumnId(2), "c_int", "integer", true, None),
            TableColumnRow::new(ColumnId(3), "c_varchar", "varchar", true, None),
        ],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![
            TableColumnRow::new(ColumnId(4), "n_bool", "boolean", true, None)
                .with_default_metadata(Some("1"), Some("1"), "literal"),
            TableColumnRow::new(ColumnId(5), "n_int", "integer", true, None).with_default_metadata(
                Some("-5"),
                Some("-5"),
                "literal",
            ),
            TableColumnRow::new(ColumnId(6), "n_varchar", "varchar", true, None)
                .with_default_metadata(Some("hello"), Some("hello"), "literal"),
        ],
    )
    .unwrap();
    append_file_with_stats(
        &mut kv,
        catalog,
        data_file(1, table, 0, 3),
        vec![
            stats(1, table, 4, 0, "true", "true"),
            stats(1, table, 5, 0, "-5", "-5"),
            stats(1, table, 6, 0, "hello", "hello"),
        ],
    );

    assert_column_stats(&kv, catalog, table, ColumnId(4), &[("true", "true")]);
    assert_column_stats(&kv, catalog, table, ColumnId(5), &[("-5", "-5")]);
    assert_column_stats(&kv, catalog, table, ColumnId(6), &[("hello", "hello")]);
}

#[test]
fn mirrors_default_expressions_test_literal_and_expression_defaults_keep_type_metadata() {
    // Mirrors: third_party/ducklake/test/sql/default/default_expressions.test
    //
    // Storage contract:
    // - Default expressions and literal strings are distinguished by default_value_type.
    // - The raw default text is stored exactly as DuckLake passes it.
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
            TableColumnRow::new(ColumnId(1), "id", "integer", true, None),
            TableColumnRow::new(ColumnId(2), "created_at", "timestamp", true, None)
                .with_default_metadata(None::<String>, Some("now()"), "expression"),
        ],
    );
    commit_change_table_column_defaults(
        &mut kv,
        catalog,
        &[ColumnDefaultChange::new(
            table,
            TableColumnRow::new(ColumnId(2), "created_at", "timestamp", true, None)
                .with_default_metadata(None::<String>, Some("random()"), "literal"),
        )],
    )
    .unwrap();

    let column = &current_table(&kv, catalog, table).columns[1];
    assert_eq!(column.default_value.as_deref(), Some("random()"));
    assert_eq!(column.default_value_type, "literal");
}

#[test]
fn mirrors_struct_field_default_test_nested_default_metadata_is_attached_to_child_column() {
    // Mirrors: third_party/ducklake/test/sql/default/struct_field_default.test
    //
    // Storage contract:
    // - Adding a nested struct field with a default stores the child column under the parent id.
    // - The child keeps its initial default metadata.
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
            TableColumnRow::new(ColumnId(1), "col1", "struct", true, None),
            TableColumnRow::new(ColumnId(2), "i", "integer", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(3), "j", "integer", true, Some(ColumnId(1))),
        ],
    );
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![
            TableColumnRow::new(ColumnId(4), "k", "integer", true, Some(ColumnId(1)))
                .with_default_metadata(Some("42"), Some("42"), "literal"),
        ],
    )
    .unwrap();

    let current = current_table(&kv, catalog, table);
    let child = current
        .columns
        .iter()
        .find(|column| column.column_id == ColumnId(4))
        .unwrap();
    assert_eq!(child.parent_id, Some(ColumnId(1)));
    assert_eq!(child.initial_default.as_deref(), Some("42"));
    assert_eq!(child.default_value.as_deref(), Some("42"));
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

fn table_with_inline(table: TableId, schema: SchemaId) -> TableRow {
    let mut row = TableRow::with_catalog_metadata(
        table,
        schema,
        format!("table-{}", table.0),
        "test",
        "main/test",
        vec![TableColumnRow::new(ColumnId(1), "i", "integer", true, None)],
        CatalogOrderId::uuid_v7(0),
    );
    row.inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            format!("ducklake_inlined_data_{}_{}", table.0, schema.0),
            schema.0,
        ));
    row
}

fn current_table(kv: &FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> TableRow {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    load_table_at(kv, catalog, table, latest.order)
        .unwrap()
        .unwrap()
}

fn inline_payload(rows: &[(u64, i64)]) -> Vec<u8> {
    rows.iter()
        .map(|(row_id, value)| format!("row\t{row_id}\ti:{value}\n"))
        .collect::<Vec<_>>()
        .join("")
        .into_bytes()
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
    commit_data_mutation_with_details(
        kv,
        catalog,
        ducklake_catalog::DataMutationInput {
            data_files: vec![data_file],
            delete_files: Vec::new(),
            inline_flushes: [].to_vec(),
            partition_values: Vec::new(),
            inline_file_deletions: Vec::new(),
            file_column_stats: stats,
            dropped_data_file_ids: Vec::new(),
        },
    )
    .unwrap();
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
