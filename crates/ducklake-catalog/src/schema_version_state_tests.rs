#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv, InlinedTableRow,
        KvBatch, MutableCatalogKv, RawSnapshotSequence, SnapshotRow, TableId, TableRow,
        TableVersionReplacement, append_data_file, commit_create_table_row,
        keys::{catalog_snapshot_version_key, current_schema_version_key},
        latest_snapshot,
        schema_version_state::{
            load_catalog_snapshot_version, load_schema_version, load_schema_version_begin_snapshot,
            load_schema_versions_at, stage_next_schema_version,
        },
    };
    use std::{cell::Cell, collections::BTreeSet};

    #[test]
    fn given_schema_version_key_missing_when_staging_next_version_then_version_becomes_one() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();

        let snapshot = SnapshotRow::new(CatalogOrderId::uuid_v7(1), RawSnapshotSequence(7));
        stage_next_schema_version(&kv, &mut batch, catalog, &snapshot).unwrap();
        kv.commit(batch).unwrap();

        assert_eq!(
            load_schema_version(&kv, &current_schema_version_key(catalog)).unwrap(),
            Some(1)
        );
        assert_eq!(
            load_catalog_snapshot_version(&kv, catalog).unwrap(),
            Some(1)
        );
        assert_eq!(
            load_schema_version_begin_snapshot(&kv, catalog, 1).unwrap(),
            Some(crate::DuckLakeSnapshotId(7))
        );
    }

    #[test]
    fn given_schema_version_key_when_staging_next_version_then_version_increments() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut first = KvBatch::new();
        let first_snapshot = SnapshotRow::new(CatalogOrderId::uuid_v7(1), RawSnapshotSequence(7));
        stage_next_schema_version(&kv, &mut first, catalog, &first_snapshot).unwrap();
        kv.commit(first).unwrap();

        let mut second = KvBatch::new();
        let second_snapshot = SnapshotRow::new(CatalogOrderId::uuid_v7(2), RawSnapshotSequence(9));
        stage_next_schema_version(&kv, &mut second, catalog, &second_snapshot).unwrap();
        kv.commit(second).unwrap();

        assert_eq!(
            load_schema_version(&kv, &current_schema_version_key(catalog)).unwrap(),
            Some(2)
        );
        assert_eq!(
            load_catalog_snapshot_version(&kv, catalog).unwrap(),
            Some(2)
        );
    }

    #[test]
    fn given_multiple_schema_changes_when_loading_requested_orders_then_each_order_uses_preceding_version()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        for value in [1, 3, 7] {
            let mut batch = KvBatch::new();
            let snapshot = SnapshotRow::new(
                CatalogOrderId::uuid_v7(value),
                RawSnapshotSequence(value as u64),
            );
            stage_next_schema_version(&kv, &mut batch, catalog, &snapshot).unwrap();
            kv.commit(batch).unwrap();
        }
        let requested = [0, 1, 2, 3, 6, 7, 9]
            .into_iter()
            .map(CatalogOrderId::uuid_v7)
            .collect::<BTreeSet<_>>();

        let versions = load_schema_versions_at(&kv, catalog, &requested).unwrap();

        for (order, expected_version) in [(0, 0), (1, 1), (2, 1), (3, 2), (6, 2), (7, 3), (9, 3)] {
            assert_eq!(
                versions.get(&CatalogOrderId::uuid_v7(order)),
                Some(&expected_version)
            );
        }
    }

    #[test]
    fn given_long_schema_history_when_reading_recent_orders_then_versions_match_point_reads() {
        use crate::keys::schema_version_history_key;
        use crate::schema_version_state::load_schema_version_at;
        use std::time::Instant;
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();
        for value in 1..=20_000_u128 {
            batch.put(
                schema_version_history_key(catalog, CatalogOrderId::uuid_v7(value * 2)),
                (value as u64).to_be_bytes().to_vec(),
            );
        }
        kv.commit(batch).unwrap();
        for values in [
            vec![],
            vec![0],
            vec![39_990],
            vec![39_989, 39_990, 39_993, 40_002],
            vec![0, 40_002],
        ] {
            let orders = values
                .into_iter()
                .map(CatalogOrderId::uuid_v7)
                .collect::<BTreeSet<_>>();
            let expected = orders
                .iter()
                .map(|order| {
                    (
                        *order,
                        load_schema_version_at(&kv, catalog, *order).unwrap(),
                    )
                })
                .collect();
            assert_eq!(
                load_schema_versions_at(&kv, catalog, &orders).unwrap(),
                expected
            );
        }
        let recent = [39_989, 39_990, 39_993, 40_002]
            .into_iter()
            .map(CatalogOrderId::uuid_v7)
            .collect();
        let measured = SchemaHistoryCountingKv {
            inner: &kv,
            rows: Cell::new(0),
            scans: Cell::new(0),
        };
        load_schema_versions_at(&measured, catalog, &recent).unwrap();
        assert_eq!(
            measured.rows.get(),
            7,
            "one predecessor and six interval rows, not all 20,000 history entries"
        );
        assert_eq!(measured.scans.get(), 2);
        let full = [0, 40_002]
            .into_iter()
            .map(CatalogOrderId::uuid_v7)
            .collect();
        let singleton = BTreeSet::from([CatalogOrderId::uuid_v7(39_993)]);
        for (name, orders) in [("recent", recent), ("full", full), ("singleton", singleton)] {
            let started = Instant::now();
            for _ in 0..100 {
                std::hint::black_box(load_schema_versions_at(&kv, catalog, &orders).unwrap());
            }
            eprintln!("schema-history {name} 100 requests {:?}", started.elapsed());
        }
        assert!(
            load_schema_versions_at(
                &kv,
                CatalogId(2),
                &BTreeSet::from([CatalogOrderId::uuid_v7(40_002)])
            )
            .unwrap()
            .values()
            .all(|value| *value == 0)
        );
    }

    #[test]
    fn given_malformed_selected_schema_version_when_reading_then_error_is_preserved() {
        use crate::keys::schema_version_history_key;
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let order = CatalogOrderId::uuid_v7(7);
        let mut batch = KvBatch::new();
        batch.put(schema_version_history_key(catalog, order), vec![1]);
        kv.commit(batch).unwrap();
        for orders in [
            BTreeSet::from([order]),
            BTreeSet::from([CatalogOrderId::uuid_v7(0), order]),
        ] {
            assert!(matches!(
                load_schema_versions_at(&kv, catalog, &orders),
                Err(crate::CatalogError::InvalidKey(_))
            ));
        }
        assert!(
            load_schema_versions_at(&kv, catalog, &BTreeSet::new())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn given_data_commit_when_schema_version_key_exists_then_version_does_not_change() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::new(TableId(10), "items", crate::CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let version_before = kv.get(&current_schema_version_key(catalog));
        let catalog_snapshot_version_before = kv.get(&catalog_snapshot_version_key(catalog));

        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(100),
                table.table_id,
                "file-100.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();

        assert_eq!(kv.get(&current_schema_version_key(catalog)), version_before);
        assert_eq!(
            kv.get(&catalog_snapshot_version_key(catalog)),
            catalog_snapshot_version_before
        );
    }

    #[test]
    fn given_table_render_facts_change_without_user_schema_change_when_committed_then_only_catalog_snapshot_version_changes()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::new(TableId(10), "items", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let schema_version_before = kv.get(&current_schema_version_key(catalog));
        let catalog_snapshot_version_before = load_catalog_snapshot_version(&kv, catalog)
            .unwrap()
            .unwrap();
        let mut registered = created.clone();
        registered
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

        kv.commit_table_replacements(
            catalog,
            latest.sequence,
            vec![TableVersionReplacement::new(
                created.table_id,
                created,
                registered,
            )],
        )
        .unwrap();

        assert_eq!(
            kv.get(&current_schema_version_key(catalog)),
            schema_version_before
        );
        assert_eq!(
            load_catalog_snapshot_version(&kv, catalog).unwrap(),
            Some(catalog_snapshot_version_before + 1)
        );
    }
    struct SchemaHistoryCountingKv<'a> {
        inner: &'a FakeOrderedCatalogKv,
        rows: Cell<usize>,
        scans: Cell<usize>,
    }

    impl crate::OrderedCatalogKv for SchemaHistoryCountingKv<'_> {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.get(key))
        }
        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            Ok(self.inner.scan_prefix(prefix, direction, limit))
        }
        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            let rows = self.inner.scan_range(start, end, direction, limit);
            self.rows.set(self.rows.get() + rows.len());
            self.scans.set(self.scans.get() + 1);
            Ok(rows)
        }
        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.read_conflict_fence(key))
        }
    }
}
