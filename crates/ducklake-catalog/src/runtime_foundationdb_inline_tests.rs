#[cfg(test)]
mod tests {
    use crate::{
        CatalogOrderId, ColumnId, InlinedTableRow, TableColumnRow, TableId, TablePartitionFieldRow,
        TablePartitionRow, TableRow, ValidityWindow,
    };

    #[cfg(feature = "foundationdb")]
    use super::super::prepare_foundationdb_inline_mutations;
    use super::super::same_user_visible_table_for_inline_insert;

    #[test]
    fn given_only_inline_table_registration_changed_when_checking_inline_insert_then_table_is_same()
    {
        let read_table = table_with_column(CatalogOrderId::from_u128(10));
        let mut current_table = table_with_column(CatalogOrderId::from_u128(11));
        current_table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_7_1", 1));

        assert!(same_user_visible_table_for_inline_insert(
            &read_table,
            &current_table
        ));
    }

    #[test]
    fn given_partition_metadata_changed_when_checking_inline_insert_then_table_is_different() {
        let read_table = table_with_column(CatalogOrderId::from_u128(10));
        let mut current_table = table_with_column(CatalogOrderId::from_u128(11));
        current_table.partition = Some(TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ));

        assert!(!same_user_visible_table_for_inline_insert(
            &read_table,
            &current_table
        ));
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn fdb_live_given_inline_table_is_already_registered_when_preparing_rows_then_table_history_is_not_rewritten()
     {
        if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
            return;
        }
        let catalog = crate::CatalogId(7302);
        let table_id = TableId(901);
        let schema_id = crate::SchemaId(1);
        let prefix = format!(
            "aux-ducklake-test/inline-registration-history/{}/{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        kv.create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "inline-history-uuid",
                "inline_history",
                "main/inline_history/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
            None,
        )
        .unwrap();
        let latest = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut registered = crate::load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        registered
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_901_1", 1));
        kv.register_inline_table_payload_with_table_versionstamped(
            catalog,
            registered,
            schema_id,
            b"row\t1\tone\n".to_vec(),
        )
        .unwrap();

        let (tables, payloads, deletes) = prepare_foundationdb_inline_mutations(
            &kv,
            catalog,
            vec![crate::runtime_inline_ops::RuntimeInlineRows {
                read_snapshot: None,
                commit_snapshot: None,
                commit_metadata: crate::SnapshotCommitMetadata::default(),
                table_id,
                schema_version: 1,
                table_name: "ducklake_inlined_data_901_1".to_owned(),
                payload: "row\t2\ttwo\n".to_owned(),
            }],
            Vec::new(),
            None,
        )
        .unwrap();

        assert!(tables.is_empty());
        assert_eq!(payloads.len(), 1);
        assert!(deletes.is_empty());
    }

    fn table_with_column(order: CatalogOrderId) -> TableRow {
        let mut table = TableRow::new(TableId(7), "test", order);
        table.uuid = "table-uuid".to_owned();
        table.path = "main/test/".to_owned();
        table.validity = ValidityWindow::new(order, None);
        table
            .columns
            .push(TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None));
        table
    }
}
