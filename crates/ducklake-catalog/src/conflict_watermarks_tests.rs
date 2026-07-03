#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, FakeOrderedCatalogKv, KvBatch,
        conflict_watermarks::{
            load_conflict_watermarks, load_max_catalog_id_watermark, load_max_file_id_watermark,
            stage_max_catalog_id_watermark, stage_max_file_id_watermark,
        },
        runtime_catalog_snapshot::conflict_snapshot_watermarks,
    };

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
        stage_max_catalog_id_watermark(&kv, &mut batch, catalog, 41).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 123).unwrap();
        kv.commit(batch).unwrap();

        let watermarks =
            conflict_snapshot_watermarks(&kv, catalog, crate::CatalogOrderId::uuid_v7(10)).unwrap();

        assert_eq!(watermarks.next_catalog_id, 42);
        assert_eq!(watermarks.next_file_id, 124);
    }

    #[test]
    fn given_conflict_watermark_keys_when_batch_loaded_then_both_values_return() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_max_catalog_id_watermark(&kv, &mut batch, catalog, 41).unwrap();
        stage_max_file_id_watermark(&kv, &mut batch, catalog, 123).unwrap();
        kv.commit(batch).unwrap();

        let watermarks = load_conflict_watermarks(&kv, catalog).unwrap();

        assert_eq!(watermarks.max_catalog_id, Some(41));
        assert_eq!(watermarks.max_file_id, Some(123));
    }
}
