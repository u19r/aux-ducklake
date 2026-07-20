#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::super::*;
    use crate::{
        CatalogError, CatalogOrderId, DeleteFileId, FakeOrderedCatalogKv, FilePartitionValueRow,
        PartitionKeyIndex, RangeDirection, RangeItem, initialize_catalog_if_absent,
        keys::{KeyFamily, family_prefix, latest_snapshot_row_key, order_delete_file_change_key},
        register_file_partition_value,
    };

    #[test]
    fn given_encrypted_file_rows_when_encoded_then_both_keys_round_trip() {
        let order = CatalogOrderId::uuid_v7(1);
        let data_file = DataFileRow::new(
            DataFileId(3),
            TableId(7),
            "encrypted.parquet",
            10,
            512,
            order,
        )
        .with_encryption_key("AQIDBA==");
        let delete_file = DeleteFileRow::new(
            DeleteFileId(4),
            DataFileId(3),
            "encrypted-delete.parquet",
            2,
            128,
            order,
        )
        .with_encryption_key("BQYHCA==");

        assert_eq!(DataFileRow::decode(&data_file.encode()).unwrap(), data_file);
        assert_eq!(
            DeleteFileRow::decode(&delete_file.encode()).unwrap(),
            delete_file
        );
    }

    #[test]
    fn given_later_cumulative_delete_exists_when_listing_before_its_change_then_original_delete_is_attached()
     {
        let catalog = CatalogId(88);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let [data_file] = commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(DataFileId(3), table, 0, 100)],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let [first_delete] = commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(4),
                data_file.data_file_id,
                "delete-1.parquet",
                50,
                100,
                data_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let historical_snapshot_order = first_delete.validity.begin_order;
        let [mut cumulative_delete] = commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(5),
                data_file.data_file_id,
                "delete-2.parquet",
                80,
                100,
                first_delete.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let cumulative_delete_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
        cumulative_delete.max_partial_order = Some(cumulative_delete.validity.begin_order);
        let mut batch = KvBatch::new();
        batch.put(
            delete_file_key(catalog, cumulative_delete.delete_file_id),
            cumulative_delete.encode(),
        );
        batch.put(
            delete_file_timeline_key(
                catalog,
                cumulative_delete.data_file_id,
                cumulative_delete_order,
                cumulative_delete.delete_file_id,
            ),
            cumulative_delete.encode(),
        );
        kv.commit(batch).unwrap();

        let files = list_data_files_with_deletes_at(&kv, catalog, table, historical_snapshot_order)
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].delete_file.as_ref().map(|row| row.delete_file_id),
            Some(first_delete.delete_file_id)
        );
        assert_eq!(
            cumulative_delete.validity.begin_order,
            first_delete.validity.begin_order
        );

        let files_after_cumulative_change =
            list_data_files_with_deletes_at(&kv, catalog, table, cumulative_delete_order).unwrap();
        assert_eq!(files_after_cumulative_change.len(), 1);
        assert_eq!(
            files_after_cumulative_change[0]
                .delete_file
                .as_ref()
                .map(|row| row.delete_file_id),
            Some(cumulative_delete.delete_file_id)
        );
    }

    #[test]
    fn given_replacement_overlaps_only_dropped_files_when_validating_then_accepts_mutation() {
        let catalog = CatalogId(88);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(3), table, 0, 4),
                data_file(DataFileId(6), table, 4, 4),
            ],
        )
        .unwrap();

        let replacement = data_file(DataFileId(18), table, 0, 8);
        let allowed_current_file_ids = BTreeSet::from([DataFileId(3), DataFileId(6)]);

        reject_current_data_file_row_id_overlaps_except(
            &kv,
            catalog,
            &[replacement],
            &allowed_current_file_ids,
        )
        .unwrap();
    }

    #[test]
    fn given_replacement_overlaps_unrelated_live_file_when_validating_then_rejects_mutation() {
        let catalog = CatalogId(88);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(3), table, 0, 4),
                data_file(DataFileId(6), table, 4, 4),
                data_file(DataFileId(9), table, 8, 4),
            ],
        )
        .unwrap();

        let replacement = data_file(DataFileId(18), table, 0, 12);
        let allowed_current_file_ids = BTreeSet::from([DataFileId(3), DataFileId(6)]);
        let error = reject_current_data_file_row_id_overlaps_except(
            &kv,
            catalog,
            &[replacement],
            &allowed_current_file_ids,
        )
        .unwrap_err();

        assert_eq!(
            error,
            CatalogError::InvalidMutation(
                "conflict committing data mutation: data file 18 row ids [0..12) overlap data file 9 row ids [8..12)".to_owned()
            )
        );
    }

    #[test]
    fn given_multiple_new_files_for_one_table_when_validating_row_ids_then_current_files_are_scanned_once()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(3), table, 0, 4),
                data_file(DataFileId(6), table, 10, 4),
            ],
        )
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);
        let proposed = vec![
            data_file(DataFileId(18), table, 20, 2),
            data_file(DataFileId(19), table, 22, 2),
            data_file(DataFileId(20), table, 24, 2),
        ];

        reject_current_data_file_row_id_overlaps_except(&kv, catalog, &proposed, &BTreeSet::new())
            .unwrap();

        assert_eq!(kv.current_data_file_prefix_scans(), 1);
    }

    #[test]
    fn given_new_files_without_overlap_capable_row_ids_when_validating_then_current_files_are_not_scanned()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(3), table, 0, 4)],
        )
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);
        let unknown_row_ids = DataFileRow::new(
            DataFileId(18),
            table,
            "unknown-row-ids.parquet",
            4,
            128,
            CatalogOrderId::uuid_v7(0),
        );
        let zero_rows = data_file(DataFileId(19), table, 20, 0);
        let proposed = vec![unknown_row_ids, zero_rows];

        reject_current_data_file_row_id_overlaps_except(&kv, catalog, &proposed, &BTreeSet::new())
            .unwrap();

        assert_eq!(kv.current_data_file_prefix_scans(), 0);
    }

    #[test]
    fn given_table_current_files_loaded_twice_when_context_is_cached_then_table_prefix_is_scanned_once()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(3), table, 0, 4),
                data_file(DataFileId(4), TableId(8), 0, 4),
            ],
        )
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);

        let first = TableCurrentDataFilesContext::for_table(&kv, catalog, table).unwrap();
        let second = TableCurrentDataFilesContext::for_table(&kv, catalog, table).unwrap();

        assert_eq!(first.rows.len(), 1);
        assert_eq!(second.rows.len(), 1);
        assert_eq!(kv.current_data_file_prefix_scans(), 1);
    }

    #[test]
    fn given_table_current_files_loaded_with_known_latest_order_when_context_is_cached_then_latest_snapshot_is_not_read()
     {
        let catalog = CatalogId(1);
        let table = TableId(107);
        let mut inner = FakeOrderedCatalogKv::new();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(3), table, 0, 4),
                data_file(DataFileId(4), TableId(108), 0, 4),
            ],
        )
        .unwrap();
        let latest_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = CountingPartitionScanKv::new(inner);

        let first =
            TableCurrentDataFilesContext::for_table_at_order(&kv, catalog, table, latest_order)
                .unwrap();
        let second =
            TableCurrentDataFilesContext::for_table_at_order(&kv, catalog, table, latest_order)
                .unwrap();

        assert_eq!(first.rows.len(), 1);
        assert_eq!(second.rows.len(), 1);
        assert_eq!(kv.current_data_file_prefix_scans(), 1);
        assert_eq!(kv.latest_snapshot_gets(), 0);
    }

    #[test]
    fn given_partial_replacement_has_unknown_row_ids_when_listing_then_it_does_not_hide_current_file()
     {
        let table = TableId(7);
        let unrelated = data_file(DataFileId(2), table, 2, 1);
        let mut replacement = DataFileRow::new(
            DataFileId(4),
            table,
            "sparse-replacement.parquet",
            2,
            128,
            CatalogOrderId::uuid_v7(10),
        )
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));
        replacement.row_id_start_known = false;
        replacement.row_id_start = 0;

        let kv = FakeOrderedCatalogKv::new();
        let rows =
            without_backfilled_source_duplicates(&kv, CatalogId(1), vec![replacement, unrelated])
                .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(2)]
        );
    }

    #[test]
    fn given_partial_replacement_and_later_same_partition_file_when_listing_then_later_file_survives()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(4), table, 0, 2),
                data_file(DataFileId(9), table, 4, 1),
            ],
        )
        .unwrap();
        for file_id in [DataFileId(4), DataFileId(9)] {
            register_file_partition_value(
                &mut kv,
                catalog,
                FilePartitionValueRow::new(file_id, table, PartitionKeyIndex(0), "1"),
            )
            .unwrap();
        }

        let mut replacement = DataFileRow::new(
            DataFileId(4),
            table,
            "replacement.parquet",
            2,
            128,
            CatalogOrderId::uuid_v7(10),
        )
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));
        replacement.row_id_start_known = false;
        replacement.row_id_start = 0;
        let mut later = data_file(DataFileId(9), table, 4, 1);
        later.validity.begin_order = CatalogOrderId::uuid_v7(30);

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, later]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
    }

    #[test]
    fn given_row_ids_cover_backfilled_source_when_listing_then_partition_values_are_not_scanned() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let replacement = data_file(DataFileId(4), table, 0, 10)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));
        let source = data_file(DataFileId(9), table, 4, 1);
        let kv = CountingPartitionScanKv::new(FakeOrderedCatalogKv::new());

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, source]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4)]
        );
        assert_eq!(kv.partition_prefix_scans(), 0);
    }

    #[test]
    fn given_row_ids_overlap_in_different_partition_paths_when_listing_then_source_survives() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut replacement = data_file(DataFileId(4), table, 0, 2)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));
        replacement.path = "main/t/year=2021/replacement.parquet".to_owned();
        let mut source = data_file(DataFileId(9), table, 0, 2);
        source.path = "main/t/year=2020/source.parquet".to_owned();
        let kv = CountingPartitionScanKv::new(FakeOrderedCatalogKv::new());

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, source]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
        assert_eq!(kv.partition_prefix_scans(), 0);
    }

    #[test]
    fn given_known_row_ids_do_not_cover_same_partition_source_when_listing_then_source_survives() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let replacement = data_file(DataFileId(4), table, 100, 21)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));
        let source = data_file(DataFileId(9), table, 0, 20);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![replacement.clone(), source.clone()])
            .unwrap();
        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, source]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
    }

    #[test]
    fn given_unknown_row_ids_and_older_same_partition_source_when_listing_then_source_survives() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut replacement = data_file(DataFileId(4), table, 0, 21)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(30)));
        replacement.row_id_start_known = false;
        replacement.validity.begin_order = CatalogOrderId::uuid_v7(20);
        let mut source = data_file(DataFileId(9), table, 0, 20);
        source.validity.begin_order = CatalogOrderId::uuid_v7(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![replacement.clone(), source.clone()])
            .unwrap();
        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        register_file_partition_value(
            &mut kv,
            catalog,
            FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, source]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
    }

    #[test]
    fn given_current_backfilled_candidates_when_listing_then_partition_values_are_loaded_once() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut replacement = data_file(DataFileId(4), table, 0, 21)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(30)));
        replacement.row_id_start_known = false;
        replacement.validity.begin_order = CatalogOrderId::uuid_v7(20);
        let mut source = data_file(DataFileId(9), table, 0, 20);
        source.row_id_start_known = false;
        source.validity.begin_order = CatalogOrderId::uuid_v7(20);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![replacement.clone(), source.clone()],
        )
        .unwrap();
        register_file_partition_value(
            &mut inner,
            catalog,
            FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        register_file_partition_value(
            &mut inner,
            catalog,
            FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);

        let rows =
            without_backfilled_source_duplicates(&kv, catalog, vec![replacement, source]).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4)]
        );
        assert!(
            kv.partition_prefix_scans() <= 1,
            "duplicate-suppression partition values should be loaded at most once"
        );
    }

    #[test]
    fn partition_key_lookup_uses_linear_strategy_for_tiny_comparison_sets() {
        let table = TableId(7);
        let lookup = PartitionKeyLookup::new(
            vec![
                FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(2), "z"),
                FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "a"),
                FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "a"),
                FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(2), "z"),
            ],
            1,
        );

        assert!(matches!(lookup, PartitionKeyLookup::Linear(_)));
        assert!(lookup.same_non_empty_key(DataFileId(4), DataFileId(9)));
        assert!(!lookup.same_non_empty_key(DataFileId(4), DataFileId(99)));
    }

    #[test]
    fn partition_key_lookup_sorts_values_once_per_file_for_large_comparison_sets() {
        let table = TableId(7);
        let lookup = PartitionKeyLookup::new(
            vec![
                FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(2), "z"),
                FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "a"),
                FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "a"),
                FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(2), "z"),
                FilePartitionValueRow::new(DataFileId(12), table, PartitionKeyIndex(0), "b"),
            ],
            100,
        );

        assert!(matches!(lookup, PartitionKeyLookup::Indexed(_)));
        assert!(lookup.same_non_empty_key(DataFileId(4), DataFileId(9)));
        assert!(!lookup.same_non_empty_key(DataFileId(4), DataFileId(12)));
        assert!(!lookup.same_non_empty_key(DataFileId(4), DataFileId(99)));
    }

    #[test]
    fn given_table_has_no_delete_changes_when_attaching_deletes_then_timelines_are_not_scanned() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let snapshot_order = CatalogOrderId::uuid_v7(20);
        let data_files = vec![
            data_file(DataFileId(4), table, 0, 10),
            data_file(DataFileId(9), table, 10, 10),
        ];
        let kv = CountingPartitionScanKv::new(FakeOrderedCatalogKv::new());

        let attached = attach_delete_files_at(&kv, catalog, data_files, snapshot_order).unwrap();

        assert_eq!(attached.len(), 2);
        assert!(attached.iter().all(|file| file.delete_file.is_none()));
        assert_eq!(kv.delete_timeline_scans(), 0);
    }

    #[test]
    fn given_only_other_table_has_delete_changes_when_attaching_deletes_then_timelines_are_not_scanned()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let other_table = TableId(8);
        let snapshot_order = CatalogOrderId::uuid_v7(20);
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            order_delete_file_change_key(
                catalog,
                CatalogOrderId::uuid_v7(10),
                other_table,
                DeleteFileId(30),
            ),
            Vec::new(),
        );
        inner.commit(batch).unwrap();
        let data_files = vec![
            data_file(DataFileId(4), table, 0, 10),
            data_file(DataFileId(9), table, 10, 10),
        ];
        let kv = CountingPartitionScanKv::new(inner);

        let attached = attach_delete_files_at(&kv, catalog, data_files, snapshot_order).unwrap();

        assert_eq!(attached.len(), 2);
        assert!(attached.iter().all(|file| file.delete_file.is_none()));
        assert_eq!(kv.order_delete_change_range_scans(), 1);
        assert_eq!(kv.delete_timeline_scans(), 0);
    }

    #[test]
    fn given_table_has_no_delete_changes_when_attaching_current_deletes_then_current_delete_pointers_are_not_read()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let data_files = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(4), table, 0, 10),
                data_file(DataFileId(9), table, 10, 10),
            ],
        )
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);

        let attached = attach_current_delete_files(&kv, catalog, data_files).unwrap();

        assert_eq!(attached.len(), 2);
        assert!(attached.iter().all(|file| file.delete_file.is_none()));
        assert_eq!(kv.order_delete_change_range_scans(), 1);
        assert_eq!(kv.current_delete_file_gets(), 0);
    }

    #[test]
    fn given_current_delete_exists_when_attaching_current_deletes_then_current_delete_is_attached()
    {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let [data_file] = commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(44), table, 0, 10)],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let [delete_file] = commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(45),
                data_file.data_file_id,
                "delete-45.parquet",
                3,
                128,
                data_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let kv = CountingPartitionScanKv::new(inner);

        let attached = attach_current_delete_files(&kv, catalog, vec![data_file]).unwrap();

        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0]
                .delete_file
                .as_ref()
                .map(|row| row.delete_file_id),
            Some(delete_file.delete_file_id)
        );
        assert_eq!(kv.order_delete_change_range_scans(), 1);
        assert_eq!(kv.current_delete_file_gets(), 1);
    }

    #[test]
    fn given_later_delete_exists_when_attaching_historical_deletes_then_timeline_scan_is_bounded() {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let [data_file] = commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(4), table, 0, 10)],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let [first_delete] = commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(30),
                data_file.data_file_id,
                "delete-1.parquet",
                4,
                128,
                data_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let historical_order = first_delete.validity.begin_order;
        let [later_delete] = commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(31),
                data_file.data_file_id,
                "delete-2.parquet",
                6,
                128,
                first_delete.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let later_timeline_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        assert_eq!(later_delete.validity.begin_order, historical_order);
        assert!(later_timeline_order > historical_order);
        let kv = CountingPartitionScanKv::new(inner);

        let attached =
            attach_delete_files_at(&kv, catalog, vec![data_file], historical_order).unwrap();

        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0]
                .delete_file
                .as_ref()
                .map(|row| row.delete_file_id),
            Some(first_delete.delete_file_id)
        );
        assert_eq!(kv.delete_timeline_scans(), 1);
        assert_eq!(
            kv.delete_timeline_items(),
            1,
            "later cumulative delete timeline rows should be excluded by the range end"
        );
    }

    #[test]
    fn given_multiple_delete_timeline_rows_when_attaching_deletes_then_only_latest_candidate_is_read()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let [data_file] = commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(40), table, 0, 10)],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let [first_delete] = commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(41),
                data_file.data_file_id,
                "delete-41.parquet",
                4,
                128,
                data_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let [second_delete] = commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(42),
                data_file.data_file_id,
                "delete-42.parquet",
                6,
                128,
                first_delete.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
        let snapshot_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = CountingPartitionScanKv::new(inner);

        let attached =
            attach_delete_files_at(&kv, catalog, vec![data_file], snapshot_order).unwrap();

        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0]
                .delete_file
                .as_ref()
                .map(|row| row.delete_file_id),
            Some(second_delete.delete_file_id)
        );
        assert_eq!(kv.delete_timeline_scans(), 1);
        assert_eq!(
            kv.delete_timeline_items(),
            1,
            "reverse timeline lookup should stop after the newest row at or before the requested snapshot"
        );
    }

    #[test]
    fn given_inline_materialization_file_when_listing_then_it_is_not_treated_as_backfilled_replacement()
     {
        let table = TableId(7);
        let begin = CatalogOrderId::uuid_v7(10);
        let source = data_file(DataFileId(2), table, 4, 1);
        let mut materialized =
            data_file(DataFileId(4), table, 4, 1).with_max_partial_order(Some(begin));
        materialized.validity.begin_order = begin;

        let kv = FakeOrderedCatalogKv::new();
        let rows =
            without_backfilled_source_duplicates(&kv, CatalogId(1), vec![materialized, source])
                .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(2)]
        );
    }

    #[test]
    fn given_current_file_is_missing_begin_index_when_listing_then_visible_current_file_is_returned()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let snapshot_order = CatalogOrderId::uuid_v7(10);
        let row = data_file(DataFileId(4), table, 4, 1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(data_file_key(catalog, row.data_file_id), row.encode());
        batch.put(
            current_data_file_key(catalog, table, row.data_file_id),
            row.data_file_id.0.to_be_bytes().to_vec(),
        );
        kv.commit(batch).unwrap();

        let rows = list_data_files_at(&kv, catalog, table, snapshot_order).unwrap();

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4)]
        );
    }

    #[test]
    fn given_table_snapshot_context_is_reused_when_listing_visible_files_then_begin_range_is_scanned_once()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let appended = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(4), table, 0, 10),
                data_file(DataFileId(9), table, 10, 10),
            ],
        )
        .unwrap();
        let snapshot_order = appended[0].validity.begin_order;
        let kv = CountingPartitionScanKv::new(inner);

        let context = TableDataFilesAtContext::load(&kv, catalog, table, snapshot_order).unwrap();
        let first = context.visible_rows();
        let second = context.visible_rows();

        assert_eq!(
            first.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
        assert_eq!(
            second
                .iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(4), DataFileId(9)]
        );
        assert_eq!(kv.data_file_begin_range_scans(), 1);
        assert_eq!(kv.current_data_file_prefix_scans(), 1);
    }

    #[test]
    fn given_backfilled_sources_when_table_snapshot_context_is_reused_then_partition_values_are_loaded_once()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut replacement = data_file(DataFileId(4), table, 0, 21)
            .with_max_partial_order(Some(CatalogOrderId::uuid_v7(30)));
        replacement.row_id_start_known = false;
        replacement.validity.begin_order = CatalogOrderId::uuid_v7(20);
        let mut source = data_file(DataFileId(9), table, 0, 20);
        source.row_id_start_known = false;
        source.validity.begin_order = CatalogOrderId::uuid_v7(20);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![replacement.clone(), source.clone()],
        )
        .unwrap();
        register_file_partition_value(
            &mut inner,
            catalog,
            FilePartitionValueRow::new(DataFileId(4), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        register_file_partition_value(
            &mut inner,
            catalog,
            FilePartitionValueRow::new(DataFileId(9), table, PartitionKeyIndex(0), "b"),
        )
        .unwrap();
        let snapshot_order = source.validity.begin_order;
        let kv = CountingPartitionScanKv::new(inner);

        let context = TableDataFilesAtContext::load(&kv, catalog, table, snapshot_order).unwrap();
        let first = context.visible_rows();
        let second = context.visible_rows();

        assert_eq!(
            first.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(4)]
        );
        assert_eq!(
            second
                .iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(4)]
        );
        assert!(
            kv.partition_prefix_scans() <= 1,
            "cached table/snapshot contexts should not reload duplicate-suppression partition values per visible_rows call"
        );
    }

    fn data_file(
        data_file_id: DataFileId,
        table_id: TableId,
        row_id_start: u64,
        record_count: u64,
    ) -> DataFileRow {
        DataFileRow::new(
            data_file_id,
            table_id,
            format!("file-{}.parquet", data_file_id.0),
            record_count,
            128,
            CatalogOrderId::uuid_v7(0),
        )
        .with_row_id_start(row_id_start)
    }

    struct CountingPartitionScanKv {
        inner: FakeOrderedCatalogKv,
        partition_prefix_scans: Cell<usize>,
        order_delete_change_range_scans: Cell<usize>,
        data_file_begin_range_scans: Cell<usize>,
        current_data_file_prefix_scans: Cell<usize>,
        current_delete_file_gets: Cell<usize>,
        delete_timeline_scans: Cell<usize>,
        delete_timeline_items: Cell<usize>,
        latest_snapshot_gets: Cell<usize>,
    }

    impl CountingPartitionScanKv {
        fn new(inner: FakeOrderedCatalogKv) -> Self {
            Self {
                inner,
                partition_prefix_scans: Cell::new(0),
                order_delete_change_range_scans: Cell::new(0),
                data_file_begin_range_scans: Cell::new(0),
                current_data_file_prefix_scans: Cell::new(0),
                current_delete_file_gets: Cell::new(0),
                delete_timeline_scans: Cell::new(0),
                delete_timeline_items: Cell::new(0),
                latest_snapshot_gets: Cell::new(0),
            }
        }

        fn partition_prefix_scans(&self) -> usize {
            self.partition_prefix_scans.get()
        }

        fn delete_timeline_scans(&self) -> usize {
            self.delete_timeline_scans.get()
        }

        fn delete_timeline_items(&self) -> usize {
            self.delete_timeline_items.get()
        }

        fn order_delete_change_range_scans(&self) -> usize {
            self.order_delete_change_range_scans.get()
        }

        fn data_file_begin_range_scans(&self) -> usize {
            self.data_file_begin_range_scans.get()
        }

        fn current_data_file_prefix_scans(&self) -> usize {
            self.current_data_file_prefix_scans.get()
        }

        fn current_delete_file_gets(&self) -> usize {
            self.current_delete_file_gets.get()
        }

        fn latest_snapshot_gets(&self) -> usize {
            self.latest_snapshot_gets.get()
        }
    }

    impl OrderedCatalogKv for CountingPartitionScanKv {
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            if key == latest_snapshot_row_key(CatalogId(1)).as_slice() {
                self.latest_snapshot_gets
                    .set(self.latest_snapshot_gets.get().saturating_add(1));
            }
            if key.starts_with(&family_prefix(CatalogId(1), KeyFamily::CurrentDeleteFile)) {
                self.current_delete_file_gets
                    .set(self.current_delete_file_gets.get().saturating_add(1));
            }
            Ok(self.inner.get(key))
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            if prefix.starts_with(&family_prefix(CatalogId(1), KeyFamily::FilePartitionValue)) {
                self.partition_prefix_scans
                    .set(self.partition_prefix_scans.get().saturating_add(1));
            }
            if prefix.starts_with(&family_prefix(CatalogId(1), KeyFamily::DeleteFileTimeline)) {
                self.delete_timeline_scans
                    .set(self.delete_timeline_scans.get().saturating_add(1));
            }
            if prefix.starts_with(&family_prefix(CatalogId(1), KeyFamily::CurrentDataFile)) {
                self.current_data_file_prefix_scans
                    .set(self.current_data_file_prefix_scans.get().saturating_add(1));
            }
            Ok(self.inner.scan_prefix(prefix, direction, limit))
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            if start.starts_with(&family_prefix(CatalogId(1), KeyFamily::DeleteFileTimeline)) {
                self.delete_timeline_scans
                    .set(self.delete_timeline_scans.get().saturating_add(1));
            }
            if start.starts_with(&family_prefix(
                CatalogId(1),
                KeyFamily::DeleteFileChangeByOrder,
            )) {
                self.order_delete_change_range_scans
                    .set(self.order_delete_change_range_scans.get().saturating_add(1));
            }
            if start.starts_with(&family_prefix(CatalogId(1), KeyFamily::DataFileBegin)) {
                self.data_file_begin_range_scans
                    .set(self.data_file_begin_range_scans.get().saturating_add(1));
            }
            let items = self.inner.scan_range(start, end, direction, limit);
            if start.starts_with(&family_prefix(CatalogId(1), KeyFamily::DeleteFileTimeline)) {
                self.delete_timeline_items
                    .set(self.delete_timeline_items.get().saturating_add(items.len()));
            }
            Ok(items)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.read_conflict_fence(key))
        }
    }
}
