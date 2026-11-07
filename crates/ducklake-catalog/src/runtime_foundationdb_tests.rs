#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        FakeOrderedCatalogKv, FilePartitionValueRow, InlineTableFlush, OrderedCatalogKv,
        PartitionKeyIndex, RangeDirection, RangeItem, RawSnapshotSequence, SchemaId,
        TableColumnRow, TableId, TablePartitionFieldRow, TablePartitionRow, TableRow,
        append_data_file, commit_create_table_row, initialize_catalog_if_absent,
        keys::data_file_key, latest_snapshot, runtime_data_mutation_ops::RuntimeDataMutation,
        runtime_data_mutation_ops::RuntimeFilePartitionSet,
    };

    #[test]
    fn given_fdb_prefix_env_is_unset_when_loading_prefix_then_default_is_dl_prefix() {
        assert_eq!(super::super::foundationdb_key_prefix(None), "dl/");
    }

    #[test]
    fn given_fdb_prefix_env_is_set_when_loading_prefix_then_configured_value_wins() {
        assert_eq!(
            super::super::foundationdb_key_prefix(Some("custom/catalog/".to_owned())),
            "custom/catalog/"
        );
    }

    #[test]
    fn given_delete_targets_new_file_when_checking_staleness_then_committed_lookup_is_not_required()
    {
        let kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        let read_order = CatalogOrderId::uuid_v7(1);
        let data_files = vec![DataFileRow::new(
            DataFileId(10),
            table,
            "main/new-file.parquet",
            100,
            1_024,
            read_order,
        )];
        let delete_files = vec![DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "main/delete-new-file.parquet",
            10,
            512,
            read_order,
        )];

        super::super::reject_delete_targets_changed_after_read(
            &kv,
            catalog,
            read_order,
            Some(read_order),
            &data_files,
            &delete_files,
            &[],
        )
        .unwrap();
    }

    #[test]
    fn given_multiple_current_delete_targets_when_checking_staleness_then_data_files_are_loaded_once()
     {
        let mut inner = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(
            &mut inner,
            catalog,
            TableRow::new(table, "events", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        for data_file_id in [DataFileId(10), DataFileId(11)] {
            append_data_file(
                &mut inner,
                catalog,
                DataFileRow::new(
                    data_file_id,
                    table,
                    format!("main/{}.parquet", data_file_id.0),
                    100,
                    1_024,
                    CatalogOrderId::uuid_v7(0),
                ),
            )
            .unwrap();
        }
        let read_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = DataFileBatchRecordingKv::new(
            inner,
            [
                data_file_key(catalog, DataFileId(10)),
                data_file_key(catalog, DataFileId(11)),
            ],
        );
        let delete_files = vec![
            DeleteFileRow::new(
                DeleteFileId(20),
                DataFileId(10),
                "main/delete-10.parquet",
                10,
                512,
                read_order,
            ),
            DeleteFileRow::new(
                DeleteFileId(21),
                DataFileId(11),
                "main/delete-11.parquet",
                10,
                512,
                read_order,
            ),
        ];

        let loaded = super::super::reject_delete_targets_changed_after_read(
            &kv,
            catalog,
            read_order,
            Some(read_order),
            &[],
            &delete_files,
            &[],
        )
        .unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(kv.data_file_batch_gets(), 1);
        assert_eq!(kv.data_file_keys_loaded(), 2);
        assert_eq!(kv.data_file_gets(), 0);
        assert_eq!(kv.scan_count(), 0);
    }

    #[test]
    fn given_inline_flush_read_snapshot_is_stale_when_committing_then_conflict_is_returned() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let mut mutation = RuntimeDataMutation::default();
        mutation.inline_flushes.push(InlineTableFlush::new(
            TableId(1),
            SchemaId(1),
            RawSnapshotSequence(0),
        ));

        let error = super::super::reject_stale_data_mutation(&kv, catalog, &mutation).unwrap_err();

        assert!(error.to_string().contains("conflict flushing inline data"));
    }

    #[test]
    fn given_inline_flush_follows_metadata_from_same_commit_when_committing_then_it_is_accepted() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let mut mutation = RuntimeDataMutation {
            proposed_commit_snapshot: Some(
                crate::runtime_snapshot_range::ProposedCommitSnapshot::new(crate::CommitAttemptId(
                    1,
                )),
            ),
            ..RuntimeDataMutation::default()
        };
        mutation.inline_flushes.push(InlineTableFlush::new(
            TableId(1),
            SchemaId(1),
            RawSnapshotSequence(0),
        ));

        super::super::reject_stale_data_mutation(&kv, catalog, &mutation).unwrap();
    }

    #[test]
    fn given_partition_changed_after_read_when_append_matches_current_partition_then_staleness_check_passes()
     {
        let table_id = TableId(1);
        let read_order = CatalogOrderId::uuid_v7(1);
        let mut source_partitioned = TableRow::with_catalog_metadata(
            table_id,
            crate::SchemaId(0),
            "table-uuid",
            "first_write",
            "main/first_write",
            vec![
                TableColumnRow::new(crate::ColumnId(1), "source", "VARCHAR", true, None),
                TableColumnRow::new(crate::ColumnId(2), "id", "INTEGER", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        );
        source_partitioned.partition = Some(TablePartitionRow::new(
            10,
            vec![TablePartitionFieldRow::new(
                0,
                crate::ColumnId(1),
                "identity",
            )],
        ));
        let mut id_partitioned = source_partitioned.clone();
        id_partitioned.partition = Some(TablePartitionRow::new(
            11,
            vec![TablePartitionFieldRow::new(
                0,
                crate::ColumnId(2),
                "identity",
            )],
        ));
        let data_file_id = DataFileId(42);
        let data_files = vec![DataFileRow::new(
            data_file_id,
            table_id,
            "main/first_write/id=1/file.parquet",
            3,
            128,
            read_order,
        )];
        let partition_sets = vec![RuntimeFilePartitionSet {
            data_file_id,
            table_id,
            partition_id: 11,
        }];
        let partition_values = vec![FilePartitionValueRow::new(
            data_file_id,
            table_id,
            PartitionKeyIndex(0),
            "1",
        )];

        let [expectation] = super::super::append_partition_expectations(
            &data_files,
            &partition_values,
            &partition_sets,
        )
        .try_into()
        .unwrap();

        assert!(expectation.matches_current_table(&id_partitioned));
        assert!(!expectation.matches_current_table(&source_partitioned));
    }

    struct DataFileBatchRecordingKv {
        inner: FakeOrderedCatalogKv,
        data_file_keys: [Vec<u8>; 2],
        data_file_gets: Cell<usize>,
        data_file_batch_gets: Cell<usize>,
        data_file_keys_loaded: Cell<usize>,
        scan_count: Cell<usize>,
    }

    impl DataFileBatchRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, data_file_keys: [Vec<u8>; 2]) -> Self {
            Self {
                inner,
                data_file_keys,
                data_file_gets: Cell::new(0),
                data_file_batch_gets: Cell::new(0),
                data_file_keys_loaded: Cell::new(0),
                scan_count: Cell::new(0),
            }
        }

        fn data_file_gets(&self) -> usize {
            self.data_file_gets.get()
        }

        fn data_file_batch_gets(&self) -> usize {
            self.data_file_batch_gets.get()
        }

        fn data_file_keys_loaded(&self) -> usize {
            self.data_file_keys_loaded.get()
        }

        fn scan_count(&self) -> usize {
            self.scan_count.get()
        }

        fn is_data_file_key(&self, key: &[u8]) -> bool {
            self.data_file_keys.iter().any(|candidate| candidate == key)
        }
    }

    impl OrderedCatalogKv for DataFileBatchRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if self.is_data_file_key(key) {
                self.data_file_gets
                    .set(self.data_file_gets.get().saturating_add(1));
            }
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            let data_file_key_count = keys.iter().filter(|key| self.is_data_file_key(key)).count();
            if data_file_key_count > 0 {
                self.data_file_batch_gets
                    .set(self.data_file_batch_gets.get().saturating_add(1));
                self.data_file_keys_loaded.set(
                    self.data_file_keys_loaded
                        .get()
                        .saturating_add(data_file_key_count),
                );
            }
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            self.scan_count.set(self.scan_count.get().saturating_add(1));
            OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            self.scan_count.set(self.scan_count.get().saturating_add(1));
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
