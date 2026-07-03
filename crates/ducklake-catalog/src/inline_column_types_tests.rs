#[cfg(test)]
mod tests {
    use crate::{
        CatalogOrderId, ColumnId, SchemaId, TableColumnRow, TableId, TableRow,
        inline_column_types::inline_columns_payload,
    };

    #[test]
    fn given_struct_column_when_rendering_inline_columns_then_children_are_folded_into_root_type() {
        let table = table_with_columns(vec![
            TableColumnRow::new(ColumnId(1), "s", "struct", true, None),
            TableColumnRow::new(ColumnId(2), "i", "INTEGER", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(3), "j", "INTEGER", true, Some(ColumnId(1))),
        ]);

        let payload = inline_columns_payload(&table).unwrap();

        assert_eq!(
            payload,
            "column\t1\ts\tSTRUCT(i INTEGER, j INTEGER)\ttrue\n"
        );
    }

    #[test]
    fn given_list_column_when_rendering_inline_columns_then_child_is_folded_into_root_type() {
        let table = table_with_columns(vec![
            TableColumnRow::new(ColumnId(1), "l", "list", true, None),
            TableColumnRow::new(ColumnId(2), "element", "INTEGER", true, Some(ColumnId(1))),
        ]);

        let payload = inline_columns_payload(&table).unwrap();

        assert_eq!(payload, "column\t1\tl\tINTEGER[]\ttrue\n");
    }

    #[test]
    fn given_map_column_when_rendering_inline_columns_then_children_are_folded_into_root_type() {
        let table = table_with_columns(vec![
            TableColumnRow::new(ColumnId(1), "m", "map", true, None),
            TableColumnRow::new(ColumnId(2), "key", "VARCHAR", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(3), "value", "INTEGER", true, Some(ColumnId(1))),
        ]);

        let payload = inline_columns_payload(&table).unwrap();

        assert_eq!(payload, "column\t1\tm\tMAP(VARCHAR, INTEGER)\ttrue\n");
    }

    #[test]
    fn given_struct_child_name_needs_quoting_when_rendering_inline_columns_then_name_is_escaped() {
        let table = table_with_columns(vec![
            TableColumnRow::new(ColumnId(1), "s", "struct", true, None),
            TableColumnRow::new(ColumnId(2), "a b", "INTEGER", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(3), "quote\"me", "VARCHAR", true, Some(ColumnId(1))),
        ]);

        let payload = inline_columns_payload(&table).unwrap();

        assert_eq!(
            payload,
            "column\t1\ts\tSTRUCT(\"a b\" INTEGER, \"quote\"\"me\" VARCHAR)\ttrue\n"
        );
    }

    #[test]
    fn given_ducklake_leaf_aliases_when_rendering_inline_columns_then_duckdb_type_names_are_used() {
        let table = table_with_columns(vec![
            TableColumnRow::new(ColumnId(1), "s", "struct", true, None),
            TableColumnRow::new(ColumnId(2), "lat", "float64", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(3), "speed", "int32", true, Some(ColumnId(1))),
            TableColumnRow::new(ColumnId(4), "ok", "boolean", true, Some(ColumnId(1))),
        ]);

        let payload = inline_columns_payload(&table).unwrap();

        assert_eq!(
            payload,
            "column\t1\ts\tSTRUCT(lat DOUBLE, speed INTEGER, ok BOOLEAN)\ttrue\n"
        );
    }

    fn table_with_columns(columns: Vec<TableColumnRow>) -> TableRow {
        let mut table = TableRow::new(TableId(42), "inline_test", CatalogOrderId::from_u128(1));
        table.schema_id = SchemaId(0);
        table.columns = columns;
        table
    }
}
