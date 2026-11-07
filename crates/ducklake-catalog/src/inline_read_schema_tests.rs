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
    #[test]
    fn given_expired_schema_snapshots_when_loading_unregistered_schema_then_it_is_not_found() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(10),
                SchemaId(0),
                "old",
                "orders",
                "main/orders",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "original",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let original = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_rename_table_columns(
            &mut kv,
            catalog,
            &[ColumnRename::new(
                TableId(10),
                TableColumnRow::new(ColumnId(1), "renamed", "INTEGER", true, None),
            )],
        )
        .unwrap();
        assert_eq!(
            table_row_for_inline_schema(&kv, catalog, TableId(10), 1)
                .unwrap()
                .columns[0]
                .name,
            "original"
        );
        crate::expire_snapshots(&mut kv, catalog, &[original.sequence]).unwrap();
        assert!(matches!(
            table_row_for_inline_schema(&kv, catalog, TableId(10), 1),
            Err(crate::CatalogError::NotFound(_))
        ));
        assert_eq!(
            table_row_for_inline_schema(&kv, catalog, TableId(10), 2)
                .unwrap()
                .columns[0]
                .name,
            "renamed"
        );
    }

    fn build_legacy_schema_fixture() -> FakeOrderedCatalogKv {
        use crate::{KvBatch, SnapshotRow, store::stage_snapshot};
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, CatalogId(1)).unwrap();
        commit_create_table_row(
            &mut kv,
            CatalogId(1),
            TableRow::with_catalog_metadata(
                TableId(10),
                SchemaId(0),
                "old",
                "orders",
                "main/orders",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "original",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let mut batch = KvBatch::new();
        for value in 10..=1_010_u128 {
            stage_snapshot(
                &mut batch,
                CatalogId(1),
                &SnapshotRow::new(
                    CatalogOrderId::uuid_v7(value),
                    crate::RawSnapshotSequence(value as u64),
                ),
            );
        }
        kv.commit(batch).unwrap();
        kv
    }

    fn measure_legacy_schema(kv: &impl crate::OrderedCatalogKv, repetitions: usize) {
        let started = std::time::Instant::now();
        for _ in 0..repetitions {
            let table = table_row_for_inline_schema(kv, CatalogId(1), TableId(10), 1).unwrap();
            assert_eq!(table.columns[0].name, "original");
            assert!(table.inlined_data_tables.is_empty());
            std::hint::black_box(table);
        }
        eprintln!(
            "legacy-schema {repetitions} requests {:?}",
            started.elapsed()
        );
        assert!(started.elapsed().as_secs_f64() < 20.0);
    }

    #[test]
    fn given_unregistered_historical_schema_when_loading_then_original_columns_are_returned() {
        let kv = build_legacy_schema_fixture();
        measure_legacy_schema(&kv, 20);
        assert!(matches!(
            table_row_for_inline_schema(&kv, CatalogId(1), TableId(10), 999),
            Err(crate::CatalogError::NotFound(_))
        ));
        assert!(matches!(
            table_row_for_inline_schema(&kv, CatalogId(2), TableId(10), 1),
            Err(crate::CatalogError::NotFound(_))
        ));
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    #[ignore = "requires a task-owned FoundationDB prefix and live cluster"]
    fn given_live_unregistered_historical_schema_when_loading_then_original_columns_are_returned() {
        use crate::{FdbOrderedCatalogKv, KvBatch, RangeDirection};
        let cluster = std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").unwrap();
        let prefix = std::env::var("AUX_DUCKLAKE_FDB_PREFIX").unwrap();
        assert!(prefix.contains("plan-completion/"));
        let mut kv =
            FdbOrderedCatalogKv::open_with_prefix(Some(&cluster), prefix.into_bytes()).unwrap();
        let fixture = build_legacy_schema_fixture();
        let mut batch = KvBatch::new();
        let items = fixture.scan_prefix(
            &crate::keys::catalog_prefix(CatalogId(1)),
            RangeDirection::Forward,
            usize::MAX,
        );
        assert!(items.len() > 3_000);
        for item in items {
            batch.put(item.key, item.value);
        }
        kv.commit(batch).unwrap();
        measure_legacy_schema(&kv, 5);
    }
}
