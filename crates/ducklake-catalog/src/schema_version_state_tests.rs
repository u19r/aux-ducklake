#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv, InlinedTableRow,
        KvBatch, MutableCatalogKv, TableId, TableRow, TableVersionReplacement, append_data_file,
        commit_create_table_row,
        keys::{catalog_snapshot_version_key, current_schema_version_key},
        latest_snapshot,
        schema_version_state::{
            load_catalog_snapshot_version, load_current_schema_version, stage_next_schema_version,
        },
    };

    #[test]
    fn given_schema_version_key_missing_when_staging_next_version_then_version_becomes_one() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();

        stage_next_schema_version(&kv, &mut batch, catalog).unwrap();
        kv.commit(batch).unwrap();

        assert_eq!(load_current_schema_version(&kv, catalog).unwrap(), Some(1));
        assert_eq!(
            load_catalog_snapshot_version(&kv, catalog).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn given_schema_version_key_when_staging_next_version_then_version_increments() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut first = KvBatch::new();
        stage_next_schema_version(&kv, &mut first, catalog).unwrap();
        kv.commit(first).unwrap();

        let mut second = KvBatch::new();
        stage_next_schema_version(&kv, &mut second, catalog).unwrap();
        kv.commit(second).unwrap();

        assert_eq!(load_current_schema_version(&kv, catalog).unwrap(), Some(2));
        assert_eq!(
            load_catalog_snapshot_version(&kv, catalog).unwrap(),
            Some(2)
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
}
