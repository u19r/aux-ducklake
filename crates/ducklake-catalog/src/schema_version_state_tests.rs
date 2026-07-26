#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv, InlinedTableRow,
        KvBatch, MutableCatalogKv, RawSnapshotSequence, SnapshotRow, TableId, TableRow,
        TableVersionReplacement, append_data_file, commit_create_table_row,
        keys::{catalog_snapshot_version_key, current_schema_version_key},
        latest_snapshot,
        schema_version_state::{
            load_catalog_snapshot_version, load_current_schema_version,
            load_schema_version_begin_snapshot, load_schema_versions_at, stage_next_schema_version,
        },
    };
    use std::collections::BTreeSet;

    #[test]
    fn given_schema_version_key_missing_when_staging_next_version_then_version_becomes_one() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();

        let snapshot = SnapshotRow::new(CatalogOrderId::uuid_v7(1), RawSnapshotSequence(7));
        stage_next_schema_version(&kv, &mut batch, catalog, &snapshot).unwrap();
        kv.commit(batch).unwrap();

        assert_eq!(load_current_schema_version(&kv, catalog).unwrap(), Some(1));
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

        assert_eq!(load_current_schema_version(&kv, catalog).unwrap(), Some(2));
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
}
