#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, ColumnId, FakeOrderedCatalogKv, SchemaId, TableColumnRow, TableId, TableRow,
        commit_create_table_row, ids::CatalogOrderId, store::latest_snapshot,
        table_store::load_table_at,
    };

    use super::super::{ColumnTypeChange, commit_change_table_column_types};

    #[test]
    fn given_type_change_with_composed_default_when_committed_then_replaces_full_column_version() {
        let mut kv = FakeOrderedCatalogKv::default();
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let column_id = ColumnId(2);
        let table = TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            "table-uuid",
            "message",
            "message/",
            vec![TableColumnRow::new(
                column_id, "user_id", "INTEGER", false, None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        commit_create_table_row(&mut kv, catalog, table).unwrap();

        let replacement = TableColumnRow::new(column_id, "user_id", "BIGINT", false, None)
            .with_default_metadata(None::<String>, Some("123"), "literal");
        let changed = commit_change_table_column_types(
            &mut kv,
            catalog,
            &[ColumnTypeChange::new(table_id, replacement)],
        )
        .unwrap();

        assert_eq!(changed.len(), 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let current = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        let column = current
            .columns
            .iter()
            .find(|column| column.column_id == column_id)
            .unwrap();
        assert_eq!(column.column_type, "BIGINT");
        assert_eq!(column.default_value.as_deref(), Some("123"));
        assert!(column.created_with_table);
    }

    #[test]
    fn given_nested_child_type_change_when_nullability_metadata_changes_then_commit_succeeds() {
        let mut kv = FakeOrderedCatalogKv::default();
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let parent_id = ColumnId(2);
        let child_id = ColumnId(4);
        let table = TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            "table-uuid",
            "message",
            "message/",
            vec![
                TableColumnRow::new(parent_id, "payload", "struct", false, None),
                TableColumnRow::new(child_id, "k", "TINYINT", false, Some(parent_id)),
            ],
            CatalogOrderId::uuid_v7(0),
        );
        commit_create_table_row(&mut kv, catalog, table).unwrap();

        let replacement = TableColumnRow::new(child_id, "k", "INTEGER", true, Some(parent_id));
        let changed = commit_change_table_column_types(
            &mut kv,
            catalog,
            &[ColumnTypeChange::new(table_id, replacement)],
        )
        .unwrap();

        assert_eq!(changed.len(), 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let current = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        let child = current
            .columns
            .iter()
            .find(|column| column.column_id == child_id)
            .unwrap();
        assert_eq!(child.column_type, "INTEGER");
        assert!(child.nulls_allowed);
    }
}
