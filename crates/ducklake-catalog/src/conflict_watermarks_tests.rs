#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, DataFileId, DataFileRow, FakeOrderedCatalogKv, KvBatch, TableId,
        conflict_watermarks::{
            decode_table_next_row_ids, load_conflict_watermarks, load_max_catalog_id_watermark,
            load_max_file_id_watermark, stage_conflict_watermarks_initialized,
            stage_max_catalog_id_watermark, stage_max_file_id_watermark, stage_table_next_row_id,
        },
        data_file_store::append_data_file,
        initialize_empty_catalog,
        keys::{
            conflict_max_catalog_id_key, conflict_max_file_id_key,
            conflict_watermarks_initialized_key, decode_key,
        },
        runtime_catalog_snapshot::snapshot_watermarks,
    };

    #[test]
    fn given_conflict_watermarks_marker_key_when_decoded_then_metadata_family_label_is_preserved() {
        let label = decode_key(&conflict_watermarks_initialized_key(CatalogId(1))).unwrap();

        assert!(label.starts_with("catalog=1/family=metadata-version/tail="));
    }

    #[test]
    fn given_new_catalog_when_initialized_then_main_schema_catalog_id_is_reserved() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();

        initialize_empty_catalog(&mut kv, catalog).unwrap();

        assert_eq!(
            kv.get(&conflict_watermarks_initialized_key(catalog)),
            Some(vec![1])
        );
        assert_eq!(
            kv.get(&conflict_max_catalog_id_key(catalog)),
            Some(0_u64.to_be_bytes().to_vec())
        );
        assert_eq!(kv.get(&conflict_max_file_id_key(catalog)), None);
    }

    #[test]
    fn given_missing_conflict_watermarks_marker_when_loading_snapshot_then_it_fails_closed() {
        let catalog = CatalogId(1);
        let kv = FakeOrderedCatalogKv::new();

        let result = snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10));

        assert!(result.is_err());
    }

    #[test]
    fn given_marker_with_partial_numeric_keys_when_loading_snapshot_then_optional_maxima_are_accepted()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_conflict_watermarks_initialized(&mut batch, catalog);
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 123).unwrap();
        kv.commit(batch).unwrap();

        let watermarks =
            snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10)).unwrap();

        assert_eq!(watermarks.next_catalog_id, 1);
        assert_eq!(watermarks.next_file_id, 124);
    }

    #[test]
    fn given_malformed_conflict_watermarks_marker_when_loading_snapshot_then_it_fails_closed() {
        let catalog = CatalogId(1);
        for value in [&[2_u8][..], &[1, 0][..], &[][..]] {
            let mut kv = FakeOrderedCatalogKv::new();
            let mut batch = KvBatch::new();
            batch.put(conflict_watermarks_initialized_key(catalog), value.to_vec());
            kv.commit(batch).unwrap();

            let result = snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10));

            assert!(result.is_err(), "marker value {value:?} should be rejected");
        }
    }

    #[test]
    fn given_marker_without_numeric_keys_when_loading_snapshot_then_next_ids_remain_zero() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(conflict_watermarks_initialized_key(catalog), vec![1]);
        kv.commit(batch).unwrap();

        let watermarks =
            snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10)).unwrap();

        assert_eq!(watermarks.next_catalog_id, 0);
        assert_eq!(watermarks.next_file_id, 0);
    }

    #[test]
    fn given_direct_append_when_loading_snapshot_then_max_file_id_advances_next_file_id() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_empty_catalog(&mut kv, catalog).unwrap();

        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                TableId(1),
                "file.parquet",
                1,
                64,
                crate::CatalogOrderId::uuid_v7(1),
            ),
        )
        .unwrap();

        assert_eq!(load_max_file_id_watermark(&kv, catalog).unwrap(), Some(7));
        let watermarks =
            snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10)).unwrap();
        assert_eq!(watermarks.next_file_id, 8);
    }

    #[test]
    fn given_multiple_file_watermark_updates_in_one_batch_when_committed_then_highest_survives() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();

        stage_max_file_id_watermark(&kv, &mut batch, catalog, 200).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 12).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 450).unwrap();
        kv.commit(batch).unwrap();

        assert_eq!(load_max_file_id_watermark(&kv, catalog).unwrap(), Some(450));
    }

    #[test]
    fn given_existing_higher_catalog_watermark_when_lower_value_is_staged_then_value_does_not_regress()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut first = KvBatch::new();
        stage_max_catalog_id_watermark(&kv, &mut first, catalog, 99).unwrap();
        kv.commit(first).unwrap();

        let mut second = KvBatch::new();
        stage_max_catalog_id_watermark(&kv, &mut second, catalog, 3).unwrap();
        kv.commit(second).unwrap();

        assert_eq!(
            load_max_catalog_id_watermark(&kv, catalog).unwrap(),
            Some(99)
        );
    }

    #[test]
    fn given_conflict_watermark_keys_when_loading_conflict_watermarks_then_next_ids_use_keys() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_conflict_watermarks_initialized(&mut batch, catalog);
        stage_max_catalog_id_watermark(&kv, &mut batch, catalog, 41).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 123).unwrap();
        kv.commit(batch).unwrap();

        let watermarks =
            snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10)).unwrap();

        assert_eq!(watermarks.next_catalog_id, 42);
        assert_eq!(watermarks.next_file_id, 124);
    }

    #[test]
    fn given_conflict_watermark_keys_when_batch_loaded_then_both_values_return() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_conflict_watermarks_initialized(&mut batch, catalog);
        stage_max_catalog_id_watermark(&kv, &mut batch, catalog, 41).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 123).unwrap();
        kv.commit(batch).unwrap();

        let watermarks = load_conflict_watermarks(&kv, catalog).unwrap();

        assert_eq!(watermarks.max_catalog_id, Some(41));
        assert_eq!(watermarks.max_file_id, Some(123));
    }

    #[test]
    fn given_table_row_id_watermarks_when_batch_loaded_then_missing_tables_default_to_zero() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_table_next_row_id(&kv, &mut batch, catalog, TableId(2), 21).unwrap();
        stage_table_next_row_id(&kv, &mut batch, catalog, TableId(3), 34).unwrap();
        kv.commit(batch).unwrap();

        let watermarks = decode_table_next_row_ids(
            catalog,
            &[TableId(1), TableId(2), TableId(3)],
            crate::OrderedCatalogKv::batch_get(
                &kv,
                &[TableId(1), TableId(2), TableId(3)]
                    .map(|table| crate::keys::table_next_row_id_key(catalog, table)),
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(watermarks.get(&TableId(1)), Some(&0));
        assert_eq!(watermarks.get(&TableId(2)), Some(&21));
        assert_eq!(watermarks.get(&TableId(3)), Some(&34));
    }
}
