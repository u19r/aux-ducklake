#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::delete_change_feed::list_table_deletion_scan_files;
    use crate::keys::delete_file_timeline_key;
    use crate::keys::{
        order_delete_file_change_key, order_delete_file_change_prefix,
        table_delete_file_change_key, table_delete_file_change_prefix,
    };
    use crate::{
        CatalogId, CatalogOrderId, CatalogResult, ColumnId, DataFileId, DataFileRow, DeleteFileId,
        DeleteFileRow, DuckLakeSnapshotId, FakeOrderedCatalogKv, InlineFileDeletionRow,
        InlinedTableRow, KvBatch, MergeAdjacentCompaction, OrderedCatalogKv, RangeDirection,
        RangeItem, SchemaId, TableColumnRow, TableId, TableRow, append_data_file,
        commit_append_data_files, commit_create_table_row, commit_data_mutation,
        commit_inline_file_deletions, commit_merge_adjacent_data_files,
        commit_register_delete_files, initialize_catalog_if_absent, latest_snapshot,
        register_inline_table_payload_with_table,
    };

    use super::super::{ChangeFeedPayload, data_file_changes_payload, table_deletions_payload};
    use crate::runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot};

    #[test]
    fn given_unflushed_inline_payload_when_listing_insertions_then_payload_returns_materialized_file()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let schema = SchemaId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut row = table_row(table);
        row.inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_0", 0));
        commit_create_table_row(&mut kv, catalog, row.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            row,
            schema,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let inline = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            data_file(1, table, 0)
                .with_begin_order(inline.order)
                .with_max_partial_order(Some(inline.order)),
        )
        .unwrap();

        let payload = String::from_utf8(
            data_file_changes_payload(
                &kv,
                catalog,
                change_feed_payload(table, 0, inline.sequence.0),
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("change_count=1\n"));
        assert!(payload.contains("change_file\tadded\t2\t1\t10\tfile-1.parquet\t1\t100\t0\t\t2"));
    }

    #[test]
    fn given_compacted_sources_when_listing_insertions_before_compaction_then_payload_returns_replacement()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table_row(table)).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0)]).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 1)]).unwrap();
        let end = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_merge_adjacent_data_files(
            &mut kv,
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(1), DataFileId(2)],
                new_files: vec![data_file_with_count(3, table, 0, 2)],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        let payload = String::from_utf8(
            data_file_changes_payload(&kv, catalog, change_feed_payload(table, 0, end.sequence.0))
                .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("change_count=1\n"));
        assert!(payload.contains("change_file\tadded\t2\t3\t10\tfile-3.parquet\t2\t100\t0"));
        assert!(!payload.contains("file-1.parquet"));
        assert!(!payload.contains("file-2.parquet"));
    }

    #[test]
    fn given_five_insert_files_then_merge_when_listing_insertions_through_early_snapshots_then_replacement_is_bounded()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table_row(table)).unwrap();

        let mut insert_sequences = Vec::new();
        for (data_file_id, row_id_start) in [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)] {
            commit_append_data_files(
                &mut kv,
                catalog,
                vec![data_file(data_file_id, table, row_id_start)],
            )
            .unwrap();
            insert_sequences.push(latest_snapshot(&kv, catalog).unwrap().unwrap().sequence.0);
        }
        commit_merge_adjacent_data_files(
            &mut kv,
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![
                    DataFileId(1),
                    DataFileId(2),
                    DataFileId(3),
                    DataFileId(4),
                    DataFileId(5),
                ],
                new_files: vec![data_file_with_count(6, table, 0, 5)],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        let through_first = String::from_utf8(
            data_file_changes_payload(
                &kv,
                catalog,
                change_feed_payload(table, 0, insert_sequences[0]),
            )
            .unwrap(),
        )
        .unwrap();
        let through_second = String::from_utf8(
            data_file_changes_payload(
                &kv,
                catalog,
                change_feed_payload(table, 0, insert_sequences[1]),
            )
            .unwrap(),
        )
        .unwrap();

        let expected_replacement = format!(
            "change_file\tadded\t{}\t6\t10\tfile-6.parquet\t5\t100\t0\t\t{}",
            insert_sequences[0], insert_sequences[4]
        );
        let expected_replacement_with_filter =
            format!("{expected_replacement}\t\t{}", insert_sequences[0]);
        assert!(through_first.contains("change_count=1\n"));
        assert!(through_first.contains(&expected_replacement_with_filter));
        assert!(!through_first.contains("file-1.parquet"));
        let expected_second_replacement_with_filter =
            format!("{expected_replacement}\t\t{}", insert_sequences[1]);
        assert!(through_second.contains("change_count=1\n"));
        assert!(through_second.contains(&expected_second_replacement_with_filter));
    }

    #[test]
    fn given_inline_file_deletes_after_requested_range_when_listing_table_deletions_then_returns_no_scans()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table_row(table)).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file_with_count(1, table, 0, 20)],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![
                InlineFileDeletionRow::new(
                    table,
                    DataFileId(1),
                    0,
                    crate::CatalogOrderId::uuid_v7(0),
                ),
                InlineFileDeletionRow::new(
                    table,
                    DataFileId(1),
                    1,
                    crate::CatalogOrderId::uuid_v7(0),
                ),
            ],
        )
        .unwrap();

        let payload = String::from_utf8(
            table_deletions_payload(&kv, catalog, change_feed_payload(table, 1, 2)).unwrap(),
        )
        .unwrap();

        assert_eq!(payload, "deletion_scan_count=0\n");
    }

    #[test]
    fn given_delete_file_is_committed_when_listing_deletions_then_order_index_is_written() {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table_row(table)).unwrap();
        let [data_file] = commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0)])
            .unwrap()
            .try_into()
            .unwrap();

        let [delete_file] = commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(7),
                data_file.data_file_id,
                "delete-7.parquet",
                1,
                899,
                data_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();

        assert!(
            kv.get(&table_delete_file_change_key(
                catalog,
                table,
                delete_file.validity.begin_order,
                delete_file.delete_file_id,
            ))
            .is_some()
        );
        assert!(
            kv.get(&order_delete_file_change_key(
                catalog,
                delete_file.validity.begin_order,
                table,
                delete_file.delete_file_id,
            ))
            .is_some()
        );
    }

    #[test]
    fn given_no_delete_change_in_requested_order_range_when_listing_deletions_then_table_change_range_is_not_scanned()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut inner = FakeOrderedCatalogKv::new();
        let legacy_change_order = CatalogOrderId::uuid_v7(100);
        let requested_start = CatalogOrderId::uuid_v7(200);
        let requested_end = CatalogOrderId::uuid_v7(300);
        let mut batch = KvBatch::new();
        batch.put(
            table_delete_file_change_key(catalog, table, legacy_change_order, DeleteFileId(7)),
            Vec::new(),
        );
        inner.commit(batch).unwrap();
        let kv = DeleteChangeScanRecordingKv::new(inner, catalog, table);

        let scans =
            list_table_deletion_scan_files(&kv, catalog, table, requested_start, requested_end)
                .unwrap();

        assert!(scans.is_empty());
        assert_eq!(kv.order_delete_change_range_scans(), 1);
        assert_eq!(kv.table_delete_change_range_scans(), 0);
    }

    #[test]
    fn given_only_other_tables_have_delete_changes_in_order_range_when_listing_deletions_then_table_change_range_is_not_scanned()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let other_table = TableId(11);
        let mut inner = FakeOrderedCatalogKv::new();
        let requested_start = CatalogOrderId::uuid_v7(200);
        let requested_end = CatalogOrderId::uuid_v7(300);
        let change_order = CatalogOrderId::uuid_v7(250);
        let mut batch = KvBatch::new();
        batch.put(
            order_delete_file_change_key(catalog, change_order, other_table, DeleteFileId(7)),
            Vec::new(),
        );
        inner.commit(batch).unwrap();
        let kv = DeleteChangeScanRecordingKv::new(inner, catalog, table);

        let scans =
            list_table_deletion_scan_files(&kv, catalog, table, requested_start, requested_end)
                .unwrap();

        assert!(scans.is_empty());
        assert_eq!(kv.order_delete_change_range_scans(), 1);
        assert_eq!(kv.table_delete_change_range_scans(), 0);
    }

    #[test]
    fn given_cumulative_delete_file_when_listing_table_deletions_then_previous_file_uses_change_order()
     {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        let initial = initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table_row(table)).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![data_file_with_count(1, table, 0, 3)])
            .unwrap();
        let insert_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        commit_data_mutation(
            &mut kv,
            catalog,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(1),
                DataFileId(1),
                "delete-1.parquet",
                1,
                899,
                initial.order,
            )],
            &[],
        )
        .unwrap();
        let first_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();

        commit_data_mutation(
            &mut kv,
            catalog,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(2),
                DataFileId(1),
                "delete-2.parquet",
                2,
                1103,
                first_delete.order,
            )],
            &[],
        )
        .unwrap();
        let second_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut batch = KvBatch::new();
        batch.put(
            delete_file_timeline_key(catalog, DataFileId(1), first_delete.order, DeleteFileId(2)),
            DeleteFileId(2).0.to_be_bytes().to_vec(),
        );
        kv.commit(batch).unwrap();

        let output = table_deletions_payload(
            &kv,
            catalog,
            change_feed_payload(table, insert_snapshot.sequence.0, second_delete.sequence.0),
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("deletion_scan_count=2\n"), "{output}");
        assert!(
            output.contains(&format!(
                "delete_scan\tpartial\t{}\t1\t10\tfile-1.parquet\t3\t100\t0\t\t1\tdelete-1.parquet\t1\t899\t\t\t\t\t\n",
                first_delete.sequence.0
            )),
            "{output}"
        );
        assert!(
            output.contains(&format!(
                "delete_scan\tpartial\t{}\t1\t10\tfile-1.parquet\t3\t100\t0\t\t2\tdelete-2.parquet\t2\t1103\t1\tdelete-1.parquet\t1\t899\t\n",
                second_delete.sequence.0
            )),
            "{output}"
        );
    }

    fn table_row(table: TableId) -> TableRow {
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            format!("table-{}", table.0),
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "i", "integer", true, None)],
            crate::CatalogOrderId::uuid_v7(0),
        )
    }

    fn change_feed_payload(
        table_id: TableId,
        start_snapshot_id: u64,
        end_snapshot_id: u64,
    ) -> ChangeFeedPayload {
        ChangeFeedPayload {
            table_id,
            start_snapshot: ChangeFeedStartSnapshot::new(DuckLakeSnapshotId(start_snapshot_id)),
            end_snapshot: ChangeFeedEndSnapshot::new(DuckLakeSnapshotId(end_snapshot_id)),
        }
    }

    fn data_file(id: u64, table: TableId, row_id_start: u64) -> DataFileRow {
        data_file_with_count(id, table, row_id_start, 1)
    }

    fn data_file_with_count(
        id: u64,
        table: TableId,
        row_id_start: u64,
        record_count: u64,
    ) -> DataFileRow {
        DataFileRow::new(
            DataFileId(id),
            table,
            format!("file-{id}.parquet"),
            record_count,
            100,
            crate::CatalogOrderId::uuid_v7(0),
        )
        .with_row_id_start(row_id_start)
    }

    struct DeleteChangeScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        table: TableId,
        order_delete_change_range_scans: Cell<usize>,
        table_delete_change_range_scans: Cell<usize>,
    }

    impl DeleteChangeScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> Self {
            Self {
                inner,
                catalog,
                table,
                order_delete_change_range_scans: Cell::new(0),
                table_delete_change_range_scans: Cell::new(0),
            }
        }

        fn order_delete_change_range_scans(&self) -> usize {
            self.order_delete_change_range_scans.get()
        }

        fn table_delete_change_range_scans(&self) -> usize {
            self.table_delete_change_range_scans.get()
        }
    }

    impl OrderedCatalogKv for DeleteChangeScanRecordingKv {
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.get(key))
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            Ok(self.inner.scan_prefix(prefix, direction, limit))
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            if start.starts_with(&order_delete_file_change_prefix(self.catalog)) {
                self.order_delete_change_range_scans
                    .set(self.order_delete_change_range_scans.get().saturating_add(1));
            }
            if start.starts_with(&table_delete_file_change_prefix(self.catalog, self.table)) {
                self.table_delete_change_range_scans
                    .set(self.table_delete_change_range_scans.get().saturating_add(1));
            }
            Ok(self.inner.scan_range(start, end, direction, limit))
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.read_conflict_fence(key))
        }
    }
}
