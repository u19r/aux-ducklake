#[cfg(test)]
mod tests {
    use super::super::{create_tables_payload, next_column_id};
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, FakeOrderedCatalogKv, MutableCatalogKv, SchemaId,
        TableColumnRow, TableId, TableRow, TableVersionReplacement, commit_create_table_row,
        initialize_catalog_if_absent, latest_snapshot, load_table_at,
    };

    #[test]
    fn create_tables_payload_returns_persisted_table_identity() {
        let mut table = TableRow::with_catalog_metadata(
            TableId(17),
            SchemaId(0),
            "runtime-table-uuid",
            "events",
            "main/events/",
            Vec::new(),
            CatalogOrderId::uuid_v7(0),
        );
        table.comment = Some("hot path".to_owned());

        let payload =
            String::from_utf8(create_tables_payload(&[TableId(18)], vec![table])).unwrap();

        assert!(payload.contains("created_table_count=1\n"));
        assert!(payload.contains("created_table\t18\t17\t0\tevents\n"));
    }

    #[test]
    fn next_column_id_uses_historical_maximum_when_current_schema_dropped_column() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "table-uuid",
                "events",
                "main/events/",
                vec![
                    TableColumnRow::new(ColumnId(1), "root", "struct", true, None),
                    TableColumnRow::new(ColumnId(8), "old_nested", "int8", true, Some(ColumnId(1))),
                ],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let previous = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        let mut current = previous.clone();
        current
            .columns
            .retain(|column| column.column_id != ColumnId(8));
        kv.commit_table_replacements(
            catalog,
            latest.sequence,
            vec![TableVersionReplacement::new(table_id, previous, current)],
        )
        .unwrap();

        assert_eq!(next_column_id(&kv, catalog, table_id).unwrap(), 9);
    }
}
