#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, ColumnId, ColumnRename, DuckLakeSnapshotId, FakeOrderedCatalogKv, SchemaId,
        TableColumnRow, TableId, TableRow, commit_append_table_columns, commit_create_table_row,
        initialize_catalog_if_absent, latest_snapshot, load_table_at,
    };

    use super::super::{DdlPayload, ddl_base_snapshot};

    #[test]
    fn given_ddl_commit_snapshot_already_exists_when_loading_base_then_current_public_snapshot_is_used()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();
        let changed = commit_append_table_columns(
            &mut kv,
            catalog,
            table_id,
            vec![
                TableColumnRow::new(ColumnId(2), "k", "INTEGER", true, None)
                    .with_created_with_table(false),
            ],
        )
        .unwrap();

        let ddl = DdlPayload {
            commit_snapshot_id: DuckLakeSnapshotId(2),
            read_snapshot_id: None,
            rows: Vec::new(),
        };

        let base = ddl_base_snapshot(&kv, catalog, &ddl).unwrap();
        assert_eq!(base.order, changed.validity.begin_order);
    }

    #[test]
    fn given_ddl_read_snapshot_when_loading_base_then_read_snapshot_wins_over_existing_commit_snapshot()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();
        commit_append_table_columns(
            &mut kv,
            catalog,
            table_id,
            vec![
                TableColumnRow::new(ColumnId(2), "k", "INTEGER", true, None)
                    .with_created_with_table(false),
            ],
        )
        .unwrap();

        let ddl = DdlPayload {
            commit_snapshot_id: DuckLakeSnapshotId(2),
            read_snapshot_id: Some(DuckLakeSnapshotId(1)),
            rows: Vec::new(),
        };

        let base = ddl_base_snapshot(&kv, catalog, &ddl).unwrap();
        assert_eq!(base.order, created.validity.begin_order);
    }

    #[test]
    fn given_add_columns_payload_for_existing_column_default_when_routed_then_default_is_changed() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let (append, defaults, renames) = super::super::split_add_columns_by_current_table(
            &kv,
            catalog,
            table_id,
            vec![(
                table_id,
                TableColumnRow::new(ColumnId(1), "j", "INTEGER", true, None)
                    .with_default_metadata(None::<String>, Some("42"), "literal")
                    .with_created_with_table(false),
            )],
        )
        .unwrap();
        assert!(append.is_empty());
        assert_eq!(defaults.len(), 1);
        assert!(renames.is_empty());

        crate::commit_change_table_column_defaults(&mut kv, catalog, &defaults).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns[1].default_value.as_deref(), Some("42"));
    }

    #[test]
    fn given_add_columns_payload_for_existing_column_name_when_routed_then_column_is_renamed() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let (append, defaults, renames) = super::super::split_add_columns_by_current_table(
            &kv,
            catalog,
            table_id,
            vec![(
                table_id,
                TableColumnRow::new(ColumnId(1), "renamed_j", "INTEGER", true, None)
                    .with_created_with_table(false),
            )],
        )
        .unwrap();

        assert!(append.is_empty());
        assert!(defaults.is_empty());
        assert_eq!(renames.len(), 1);
        crate::commit_rename_table_columns(&mut kv, catalog, &renames).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns[1].name, "renamed_j");
    }

    #[test]
    fn given_nested_rename_payload_when_normalized_then_current_shape_is_preserved() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, nested_table(table_id)).unwrap();

        let renames = vec![ColumnRename::new(
            table_id,
            TableColumnRow::new(ColumnId(4), "c", "INTEGER", false, None),
        )];

        let normalized =
            crate::normalize_column_renames_to_current_shape(&kv, catalog, &renames).unwrap();

        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].column.name, "c");
        assert_eq!(normalized[0].column.parent_id, Some(ColumnId(3)));
        assert!(normalized[0].column.nulls_allowed);
    }

    fn table(table_id: TableId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            "table-uuid",
            "test",
            "test",
            vec![
                TableColumnRow::new(ColumnId(0), "i", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(1), "j", "INTEGER", true, None),
            ],
            crate::CatalogOrderId::uuid_v7(0),
        )
    }

    fn nested_table(table_id: TableId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            "table-uuid",
            "test",
            "test",
            vec![
                TableColumnRow::new(ColumnId(1), "id", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(3), "payload", "struct", true, None),
                TableColumnRow::new(ColumnId(4), "b", "INTEGER", true, Some(ColumnId(3))),
            ],
            crate::CatalogOrderId::uuid_v7(0),
        )
    }
}
