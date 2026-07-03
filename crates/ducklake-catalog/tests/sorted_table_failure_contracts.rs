use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnDefaultChange, ColumnId, ColumnRename, DataFileId,
    DataFileRow, FakeOrderedCatalogKv, InlineTableFlush, MutableCatalogKv, RawSnapshotSequence,
    SchemaId, TableColumnRow, TableId, TableRow, TableSortChange, TableSortFieldRow, TableSortRow,
    TableVersionReplacement, commit_append_table_columns, commit_change_table_column_defaults,
    commit_change_table_sort, commit_create_table_row, commit_data_mutation_with_file_partitions,
    commit_rename_table_columns, initialize_catalog_if_absent, insertion_files, latest_snapshot,
    list_current_data_files, load_table_at, register_inline_table_payload_with_table,
};

#[test]
fn given_data_inlining_flush_sorted_alter_table_when_storage_saves_flush_then_sort_metadata_is_still_visible()
 {
    let mut fixture = SortedFixture::new("test2");
    fixture.insert_inline_rows(10);
    fixture.set_sort("i", "DESC", "NULLS_FIRST");
    fixture.add_column(ColumnId(2), "new_column");

    let flush_order = fixture.flush_inline_rows_to_file(DataFileId(20));

    fixture.assert_current_file(DataFileId(20), 10);
    fixture.assert_no_user_insert_at(flush_order);
    fixture.assert_current_sort("i", "DESC", "NULLS_FIRST");
}

#[test]
fn given_data_inlining_flush_sorted_basic_when_storage_saves_flush_then_sort_metadata_is_available_to_writer()
 {
    let mut fixture = SortedFixture::new("test");
    fixture.insert_inline_rows(10);
    fixture.set_sort("i", "DESC", "NULLS_FIRST");

    let flush_order = fixture.flush_inline_rows_to_file(DataFileId(20));

    fixture.assert_current_file(DataFileId(20), 10);
    fixture.assert_no_user_insert_at(flush_order);
    fixture.assert_current_sort("i", "DESC", "NULLS_FIRST");
}

#[test]
fn given_data_inlining_flush_sorted_basic_expression_when_storage_saves_flush_then_expression_sort_round_trips()
 {
    let mut fixture = SortedFixture::new("test");
    fixture.insert_inline_rows(10);
    fixture.set_sort("i * i", "DESC", "NULLS_FIRST");

    let flush_order = fixture.flush_inline_rows_to_file(DataFileId(20));

    fixture.assert_current_file(DataFileId(20), 10);
    fixture.assert_no_user_insert_at(flush_order);
    fixture.assert_current_sort("i * i", "DESC", "NULLS_FIRST");
}

#[test]
fn given_macro_expression_sort_inside_rolled_back_transaction_when_storage_is_not_asked_to_commit_then_table_has_no_sort()
 {
    let fixture = SortedFixture::new("macro_sort_test");

    fixture.assert_current_has_no_sort();
}

#[test]
fn given_data_inlining_flush_sorted_renamed_when_columns_are_renamed_then_current_table_returns_renamed_columns()
 {
    let mut fixture = SortedFixture::new_with_columns(
        "renamed_columns_test",
        vec![
            TableColumnRow::new(ColumnId(1), "unique_id", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(2), "sort_key_1", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(3), "sort_key_2", "VARCHAR", true, None),
        ],
    );
    fixture.set_sort("sort_key_1, sort_key_2", "ASC", "NULLS_LAST");
    fixture.rename_column(ColumnId(2), "sort_key_1_changed");
    fixture.rename_column(ColumnId(3), "sort_key_2_changed");

    fixture.assert_current_columns(&["unique_id", "sort_key_1_changed", "sort_key_2_changed"]);
}

#[test]
fn given_data_inlining_flush_sorted_transaction_renamed_when_one_commit_changes_sort_and_column_then_storage_returns_one_table_version()
 {
    let mut fixture = SortedFixture::new_with_columns(
        "renamed_columns_test",
        vec![
            TableColumnRow::new(ColumnId(1), "unique_id", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(2), "sort_key_1", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(3), "sort_key_2", "VARCHAR", true, None),
        ],
    );

    fixture.replace_current_table(|table| {
        table.sort = Some(sort_row("sort_key_1, sort_key_2", "ASC", "NULLS_LAST"));
        table.columns[1].name = "sort_key_1_renamed".to_owned();
    });

    fixture.assert_current_columns(&["unique_id", "sort_key_1_renamed", "sort_key_2"]);
    fixture.assert_current_sort("sort_key_1, sort_key_2", "ASC", "NULLS_LAST");
}

#[test]
fn given_insert_sorted_default_direction_when_table_and_sort_are_created_together_then_current_table_returns_sort()
 {
    let mut fixture = SortedFixture::new_with_columns(
        "data",
        vec![
            TableColumnRow::new(ColumnId(1), "date", "DATE", true, None),
            TableColumnRow::new(ColumnId(2), "value", "FLOAT", true, None),
        ],
    );

    fixture.replace_current_table(|table| {
        table.sort = Some(sort_row("date", "ASC", "NULLS_LAST"));
    });

    fixture.assert_current_sort("date", "ASC", "NULLS_LAST");
}

#[test]
fn given_insert_sorted_transaction_when_create_add_column_and_sort_are_one_commit_then_storage_returns_final_schema()
 {
    let mut fixture = SortedFixture::new_with_columns(
        "test_add_sort_insert",
        vec![TableColumnRow::new(ColumnId(1), "a", "INTEGER", true, None)],
    );

    fixture.replace_current_table(|table| {
        table
            .columns
            .push(TableColumnRow::new(ColumnId(2), "b", "INTEGER", true, None));
        table.sort = Some(sort_row("b", "ASC", "NULLS_LAST"));
    });

    fixture.assert_current_columns(&["a", "b"]);
    fixture.assert_current_sort("b", "ASC", "NULLS_LAST");
}

#[test]
fn given_set_default_preserves_sort_key_when_default_and_rename_commit_then_sort_survives_without_duplicate_columns()
 {
    let mut fixture = SortedFixture::new_with_columns(
        "t",
        vec![
            TableColumnRow::new(ColumnId(1), "a", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(2), "b", "INTEGER", true, None),
        ],
    );
    fixture.set_sort("a", "ASC", "NULLS_LAST");

    commit_change_table_column_defaults(
        &mut fixture.kv,
        fixture.catalog,
        &[ColumnDefaultChange::new(
            fixture.table,
            TableColumnRow::new(ColumnId(2), "b", "INTEGER", true, None).with_default_metadata(
                None::<String>,
                Some("42"),
                "literal",
            ),
        )],
    )
    .unwrap();
    fixture.rename_column(ColumnId(1), "a_renamed");

    fixture.assert_current_columns(&["a_renamed", "b"]);
    fixture.assert_current_sort("a", "ASC", "NULLS_LAST");
    assert_eq!(
        fixture.current_table().columns[1].default_value.as_deref(),
        Some("42")
    );
}

struct SortedFixture {
    kv: FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
}

impl SortedFixture {
    fn new(name: &str) -> Self {
        Self::new_with_columns(
            name,
            vec![TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None)],
        )
    }

    fn new_with_columns(name: &str, columns: Vec<TableColumnRow>) -> Self {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let schema = SchemaId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table,
                schema,
                format!("{name}-uuid"),
                name,
                format!("main/{name}"),
                columns,
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        Self {
            kv,
            catalog,
            table,
            schema,
        }
    }

    fn insert_inline_rows(&mut self, count: u64) {
        let table = self.current_table();
        let payload = (0..count)
            .map(|row_id| format!("row\t{row_id}\ti:{row_id}\n"))
            .collect::<String>()
            .into_bytes();
        register_inline_table_payload_with_table(
            &mut self.kv,
            self.catalog,
            table,
            self.schema,
            payload,
        )
        .unwrap();
    }

    fn set_sort(&mut self, expression: &str, direction: &str, null_order: &str) {
        commit_change_table_sort(
            &mut self.kv,
            self.catalog,
            &TableSortChange::new(
                self.table,
                Some(sort_row(expression, direction, null_order)),
            ),
        )
        .unwrap()
        .unwrap();
    }

    fn add_column(&mut self, column_id: ColumnId, name: &str) {
        commit_append_table_columns(
            &mut self.kv,
            self.catalog,
            self.table,
            vec![TableColumnRow::new(column_id, name, "INTEGER", true, None)],
        )
        .unwrap();
    }

    fn rename_column(&mut self, column_id: ColumnId, name: &str) {
        let mut column = self
            .current_table()
            .columns
            .into_iter()
            .find(|column| column.column_id == column_id)
            .unwrap();
        column.name = name.to_owned();
        commit_rename_table_columns(
            &mut self.kv,
            self.catalog,
            &[ColumnRename::new(self.table, column)],
        )
        .unwrap();
    }

    fn replace_current_table(&mut self, change: impl FnOnce(&mut TableRow)) {
        let latest = latest_snapshot(&self.kv, self.catalog).unwrap().unwrap();
        let previous = load_table_at(&self.kv, self.catalog, self.table, latest.order)
            .unwrap()
            .unwrap();
        let mut next = previous.clone();
        change(&mut next);
        self.kv
            .commit_table_replacements(
                self.catalog,
                latest.sequence,
                vec![TableVersionReplacement::new(self.table, previous, next)],
            )
            .unwrap();
    }

    fn flush_inline_rows_to_file(&mut self, data_file_id: DataFileId) -> CatalogOrderId {
        let before_flush = latest_snapshot(&self.kv, self.catalog).unwrap().unwrap();
        commit_data_mutation_with_file_partitions(
            &mut self.kv,
            self.catalog,
            vec![DataFileRow::new(
                data_file_id,
                self.table,
                format!("main/table-{}/flush.parquet", self.table.0),
                10,
                1024,
                CatalogOrderId::uuid_v7(0),
            )],
            Vec::new(),
            &[InlineTableFlush::new(
                self.table,
                self.schema,
                RawSnapshotSequence(before_flush.sequence.0),
            )],
            Vec::new(),
        )
        .unwrap();
        latest_snapshot(&self.kv, self.catalog)
            .unwrap()
            .unwrap()
            .order
    }

    fn assert_current_file(&self, data_file_id: DataFileId, record_count: u64) {
        let files = list_current_data_files(&self.kv, self.catalog, self.table).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].data_file_id, data_file_id);
        assert_eq!(files[0].record_count, record_count);
    }

    fn assert_no_user_insert_at(&self, order: CatalogOrderId) {
        assert!(
            insertion_files(&self.kv, self.catalog, self.table, order, order)
                .unwrap()
                .is_empty()
        );
    }

    fn assert_current_sort(&self, expression: &str, direction: &str, null_order: &str) {
        let sort = self.current_table().sort.unwrap();
        assert_eq!(sort.fields[0].expression, expression);
        assert_eq!(sort.fields[0].sort_direction, direction);
        assert_eq!(sort.fields[0].null_order, null_order);
    }

    fn assert_current_has_no_sort(&self) {
        assert_eq!(self.current_table().sort, None);
    }

    fn assert_current_columns(&self, names: &[&str]) {
        let table = self.current_table();
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            names
        );
    }

    fn current_table(&self) -> TableRow {
        let latest = latest_snapshot(&self.kv, self.catalog).unwrap().unwrap();
        load_table_at(&self.kv, self.catalog, self.table, latest.order)
            .unwrap()
            .unwrap()
    }
}

fn sort_row(expression: &str, direction: &str, null_order: &str) -> TableSortRow {
    TableSortRow::new(
        1,
        vec![TableSortFieldRow::new(
            0, expression, "duckdb", direction, null_order,
        )],
    )
}
