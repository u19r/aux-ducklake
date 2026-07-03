#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, ColumnRename, FakeOrderedCatalogKv, InlinedTableRow,
        MutableCatalogKv, SchemaId, TableColumnRow, TableId, TableRow, TableVersionReplacement,
        commit_create_table_row, commit_rename_table_columns, initialize_catalog_if_absent,
        latest_snapshot,
    };

    use super::super::table_row_for_inline_schema;

    #[test]
    fn given_inlined_payload_registered_before_rename_when_loading_schema_then_original_column_names_are_used()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "table-uuid",
                "orders",
                "main/orders",
                vec![
                    TableColumnRow::new(ColumnId(1), "col1", "INTEGER", true, None),
                    TableColumnRow::new(ColumnId(2), "col2", "VARCHAR", true, None),
                ],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut registered = created.clone();
        registered
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        kv.commit_table_replacements(
            catalog,
            create_snapshot.sequence,
            vec![TableVersionReplacement::new(table_id, created, registered)],
        )
        .unwrap();

        commit_rename_table_columns(
            &mut kv,
            catalog,
            &[ColumnRename::new(
                table_id,
                TableColumnRow::new(ColumnId(1), "new_col", "INTEGER", true, None),
            )],
        )
        .unwrap();

        let schema_table = table_row_for_inline_schema(&kv, catalog, table_id, 1).unwrap();

        assert_eq!(schema_table.columns[0].name, "col1");
        assert_eq!(schema_table.columns[1].name, "col2");
    }
}
