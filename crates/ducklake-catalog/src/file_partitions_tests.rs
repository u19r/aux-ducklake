#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv, KvBatch,
        OrderedCatalogKv, PartitionKeyIndex, RangeDirection, RangeItem, TableId,
        commit_append_data_files, list_current_data_files_by_partition_value,
        register_file_partition_value,
    };

    use super::super::{
        FilePartitionValueRow, encode_partition_lookup_value,
        list_file_partition_values_for_data_files, partition_lookup_key,
        stage_file_partition_value,
    };

    #[test]
    fn given_lookup_has_file_row_when_listing_exact_value_then_ignores_unmatched_current_files() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        let [matched, unmatched] = commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(11), table),
                data_file(DataFileId(12), table),
            ],
        )
        .unwrap()
        .try_into()
        .unwrap();
        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(matched.data_file_id, table, PartitionKeyIndex(0), "eu"),
        )
        .unwrap();
        let mut corrupt_unmatched = KvBatch::new();
        corrupt_unmatched.put(
            crate::keys::current_data_file_key(catalog, table, unmatched.data_file_id),
            b"not a data file row".to_vec(),
        );
        kv.commit(corrupt_unmatched).unwrap();

        let rows = list_current_data_files_by_partition_value(
            &kv,
            catalog,
            table,
            PartitionKeyIndex(0),
            "eu",
        )
        .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![matched.data_file_id]
        );
    }

    #[test]
    fn given_lookup_value_contains_data_file_when_decoding_then_current_read_uses_lookup() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let data_file = data_file(DataFileId(11), table);
        let partition =
            FilePartitionValueRow::new(data_file.data_file_id, table, PartitionKeyIndex(0), "eu");
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        stage_file_partition_value(&mut batch, catalog, &partition, &data_file);
        kv.commit(batch).unwrap();

        let lookup_value = kv
            .get(&partition_lookup_key(catalog, &partition))
            .expect("partition lookup value exists");
        assert_eq!(
            lookup_value,
            encode_partition_lookup_value(&partition, &data_file)
        );

        let rows = list_current_data_files_by_partition_value(
            &kv,
            catalog,
            table,
            PartitionKeyIndex(0),
            "eu",
        )
        .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![data_file.data_file_id]
        );
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_lookup_values_for_partition_key_when_listing_key_values_then_returns_all_values() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        for (data_file_id, partition_key_index, partition_value) in [
            (DataFileId(11), PartitionKeyIndex(0), "apac"),
            (DataFileId(12), PartitionKeyIndex(0), "eu"),
            (DataFileId(13), PartitionKeyIndex(1), "1"),
        ] {
            let data_file = data_file(data_file_id, table);
            let partition = FilePartitionValueRow::new(
                data_file_id,
                table,
                partition_key_index,
                partition_value,
            );
            let mut batch = KvBatch::new();
            stage_file_partition_value(&mut batch, catalog, &partition, &data_file);
            kv.commit(batch).unwrap();
        }

        let rows = super::super::list_partition_lookup_values_for_key(
            &kv,
            catalog,
            table,
            PartitionKeyIndex(0),
        )
        .unwrap();

        assert_eq!(
            rows.iter()
                .map(|row| row.partition_value.as_str())
                .collect::<Vec<_>>(),
            vec!["apac", "eu"]
        );
    }

    #[test]
    fn given_partition_values_for_file_set_when_loaded_twice_then_second_load_uses_cache() {
        let catalog = CatalogId(123);
        let table = TableId(7);
        let data_file_ids = [DataFileId(11), DataFileId(12)];
        let mut inner = FakeOrderedCatalogKv::new();
        for data_file_id in data_file_ids {
            let data_file = data_file(data_file_id, table);
            let partition =
                FilePartitionValueRow::new(data_file_id, table, PartitionKeyIndex(0), "eu");
            let mut batch = KvBatch::new();
            stage_file_partition_value(&mut batch, catalog, &partition, &data_file);
            inner.commit(batch).unwrap();
        }
        let kv = PartitionValueScanCountingKv::new(inner, catalog);
        let data_file_ids = data_file_ids.into_iter().collect();

        let first =
            list_file_partition_values_for_data_files(&kv, catalog, &data_file_ids).unwrap();
        let second =
            list_file_partition_values_for_data_files(&kv, catalog, &data_file_ids).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(kv.file_partition_value_prefix_scans(), 0);
        assert_eq!(kv.file_partition_value_range_scans(), 1);
    }

    #[test]
    fn given_sparse_partition_file_set_when_loaded_then_uses_exact_prefix_scans() {
        let catalog = CatalogId(125);
        let table = TableId(7);
        let data_file_ids = [DataFileId(11), DataFileId(10_000)];
        let mut inner = FakeOrderedCatalogKv::new();
        for data_file_id in data_file_ids {
            let data_file = data_file(data_file_id, table);
            let partition =
                FilePartitionValueRow::new(data_file_id, table, PartitionKeyIndex(0), "eu");
            let mut batch = KvBatch::new();
            stage_file_partition_value(&mut batch, catalog, &partition, &data_file);
            inner.commit(batch).unwrap();
        }
        let kv = PartitionValueScanCountingKv::new(inner, catalog);
        let data_file_ids = data_file_ids.into_iter().collect();

        let rows = list_file_partition_values_for_data_files(&kv, catalog, &data_file_ids).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(kv.file_partition_value_prefix_scans(), 2);
        assert_eq!(kv.file_partition_value_range_scans(), 0);
    }

    #[test]
    fn given_empty_partition_cache_when_partition_is_registered_then_next_load_sees_new_row() {
        let catalog = CatalogId(124);
        let table = TableId(7);
        let data_file_id = DataFileId(11);
        let mut kv = FakeOrderedCatalogKv::new();
        commit_append_data_files(&mut kv, catalog, vec![data_file(data_file_id, table)]).unwrap();
        let data_file_ids = [data_file_id].into_iter().collect();

        assert!(
            list_file_partition_values_for_data_files(&kv, catalog, &data_file_ids)
                .unwrap()
                .is_empty()
        );

        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(data_file_id, table, PartitionKeyIndex(0), "eu"),
        )
        .unwrap();

        let rows = list_file_partition_values_for_data_files(&kv, catalog, &data_file_ids).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_file_id, data_file_id);
        assert_eq!(rows[0].partition_value, "eu");
    }

    struct PartitionValueScanCountingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        file_partition_value_prefix_scans: Cell<usize>,
        file_partition_value_range_scans: Cell<usize>,
    }

    impl PartitionValueScanCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog,
                file_partition_value_prefix_scans: Cell::new(0),
                file_partition_value_range_scans: Cell::new(0),
            }
        }

        fn file_partition_value_prefix_scans(&self) -> usize {
            self.file_partition_value_prefix_scans.get()
        }

        fn file_partition_value_range_scans(&self) -> usize {
            self.file_partition_value_range_scans.get()
        }
    }

    impl OrderedCatalogKv for PartitionValueScanCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix.starts_with(&crate::keys::family_prefix(
                self.catalog,
                crate::keys::KeyFamily::FilePartitionValue,
            )) {
                self.file_partition_value_prefix_scans.set(
                    self.file_partition_value_prefix_scans
                        .get()
                        .saturating_add(1),
                );
            }
            OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if start.starts_with(&crate::keys::family_prefix(
                self.catalog,
                crate::keys::KeyFamily::FilePartitionValue,
            )) {
                self.file_partition_value_range_scans.set(
                    self.file_partition_value_range_scans
                        .get()
                        .saturating_add(1),
                );
            }
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    fn data_file(data_file_id: DataFileId, table_id: TableId) -> DataFileRow {
        DataFileRow::new(
            data_file_id,
            table_id,
            format!("file-{}.parquet", data_file_id.0),
            5,
            128,
            CatalogOrderId::uuid_v7(0),
        )
    }
}
