#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        FakeOrderedCatalogKv, KvBatch, MergeAdjacentCompaction, RawSnapshotSequence, SnapshotRow,
        TableId, ValidityWindow, commit_append_data_files, commit_merge_adjacent_data_files,
        initialize_catalog_if_absent,
        keys::{
            data_file_key, delete_file_key, delete_file_timeline_key,
            scheduled_data_file_cleanup_key,
        },
        maintenance::{
            DeleteFilePhysicalCleanupDecision, delete_file_is_safe_for_physical_cleanup,
            delete_file_physical_cleanup_decision, stage_scheduled_compacted_data_file_cleanup,
            stage_scheduled_delete_file_cleanup,
        },
        remove_old_data_files,
        store::stage_snapshot,
    };

    use super::super::{OldFilesCleanupRequest, old_files_cleanup_payload};

    #[test]
    fn given_cleanup_filter_threshold_when_listing_old_files_then_only_older_unreachable_scheduled_files_return()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();
        stage_unreachable_scheduled_file(&mut batch, catalog, 10, 100, "older.parquet");
        stage_unreachable_scheduled_file(&mut batch, catalog, 11, 200, "newer.parquet");
        kv.commit(batch).unwrap();

        let payload = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: false,
                schedule_before_micros: Some(150),
            },
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("cleanup_file_count=1"));
        assert!(text.contains("cleanup_file\tdata\t10\t1\tolder.parquet"));
        assert!(!text.contains("newer.parquet"));
    }

    #[test]
    fn given_cleanup_all_when_listing_old_files_then_all_unreachable_scheduled_files_return() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();
        stage_unreachable_scheduled_file(&mut batch, catalog, 10, 100, "older.parquet");
        stage_unreachable_scheduled_file(&mut batch, catalog, 11, 200, "newer.parquet");
        kv.commit(batch).unwrap();

        let payload = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: true,
                schedule_before_micros: None,
            },
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("cleanup_file_count=2"));
        assert!(text.contains("older.parquet"));
        assert!(text.contains("newer.parquet"));
    }

    #[test]
    fn given_cleanup_all_when_scheduled_file_is_visible_to_retained_snapshot_then_file_is_not_returned()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut batch = KvBatch::new();
        stage_retained_snapshot(&mut batch, catalog, 2);
        stage_scheduled_file(
            &mut batch,
            catalog,
            10,
            100,
            "still-needed.parquet",
            Some(CatalogOrderId::uuid_v7(3)),
        );
        kv.commit(batch).unwrap();

        let payload = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: true,
                schedule_before_micros: None,
            },
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("cleanup_file_count=0"), "{text}");
        assert!(!text.contains("still-needed.parquet"));
    }

    #[test]
    fn given_sparse_compaction_sources_when_listing_old_files_then_scheduled_sources_are_returned()
    {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(10, table, 0, "source-1.parquet")],
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(11, table, 10_000, "large-file.parquet")],
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(12, table, 10_001, "source-2.parquet")],
        )
        .unwrap();
        commit_merge_adjacent_data_files(
            &mut kv,
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(10), DataFileId(12)],
                new_files: vec![DataFileRow::new(
                    DataFileId(13),
                    table,
                    "merged.parquet",
                    2,
                    100,
                    CatalogOrderId::uuid_v7(0),
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        let payload = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: true,
                schedule_before_micros: None,
            },
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("cleanup_file_count=2"), "{text}");
        assert!(text.contains("cleanup_file\tdata\t10\t1\tsource-1.parquet"));
        assert!(text.contains("cleanup_file\tdata\t12\t1\tsource-2.parquet"));
        assert!(!text.contains("large-file.parquet"));

        remove_old_data_files(&mut kv, catalog, &[DataFileId(10), DataFileId(12)]).unwrap();
        let after_remove = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: true,
                schedule_before_micros: None,
            },
        )
        .unwrap();
        let text = String::from_utf8(after_remove).unwrap();
        assert!(text.contains("cleanup_file_count=0"), "{text}");
    }

    #[test]
    fn given_retained_snapshot_needs_delete_file_when_source_replacement_is_scheduled_then_delete_file_is_not_safe_for_cleanup()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        let mut batch = KvBatch::new();
        stage_retained_snapshot(&mut batch, catalog, 3);
        let mut source = data_file(10, table, 0, "source.parquet");
        source.validity =
            ValidityWindow::new(CatalogOrderId::uuid_v7(1), Some(CatalogOrderId::uuid_v7(5)));
        let mut replacement = data_file(11, table, 0, "replacement.parquet");
        replacement.validity = ValidityWindow::new(CatalogOrderId::uuid_v7(5), None);
        batch.put(data_file_key(catalog, source.data_file_id), source.encode());
        batch.put(
            data_file_key(catalog, replacement.data_file_id),
            replacement.encode(),
        );
        stage_scheduled_compacted_data_file_cleanup(&mut batch, catalog, source.data_file_id);
        kv.commit(batch).unwrap();
        let mut delete_file = DeleteFileRow::new(
            DeleteFileId(20),
            source.data_file_id,
            "source-delete.parquet",
            50,
            100,
            CatalogOrderId::uuid_v7(2),
        );
        delete_file.validity.end_order = Some(CatalogOrderId::uuid_v7(5));
        let snapshots = vec![SnapshotRow::new(
            CatalogOrderId::uuid_v7(3),
            RawSnapshotSequence(3),
        )];

        assert!(
            !delete_file_is_safe_for_physical_cleanup(&kv, catalog, &delete_file, &snapshots)
                .unwrap()
        );
        assert_eq!(
            delete_file_physical_cleanup_decision(&kv, catalog, &delete_file, &snapshots).unwrap(),
            DeleteFilePhysicalCleanupDecision::CleanupCandidateStillNeededByRetainedSnapshot
        );
    }

    #[test]
    fn given_current_delete_file_when_classifying_physical_cleanup_then_it_is_not_a_cleanup_candidate()
     {
        let kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let delete_file = DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "current-delete.parquet",
            50,
            100,
            CatalogOrderId::uuid_v7(2),
        );

        assert_eq!(
            delete_file_physical_cleanup_decision(&kv, catalog, &delete_file, &[]).unwrap(),
            DeleteFilePhysicalCleanupDecision::NotCleanupCandidate
        );
    }

    #[test]
    fn given_ended_delete_file_without_retained_snapshot_when_classifying_physical_cleanup_then_it_is_safe_to_remove()
     {
        let kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let mut delete_file = DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "expired-delete.parquet",
            50,
            100,
            CatalogOrderId::uuid_v7(2),
        );
        delete_file.validity.end_order = Some(CatalogOrderId::uuid_v7(5));

        assert_eq!(
            delete_file_physical_cleanup_decision(&kv, catalog, &delete_file, &[]).unwrap(),
            DeleteFilePhysicalCleanupDecision::SafeToRemove
        );
    }

    #[test]
    fn given_retained_snapshot_can_see_older_delete_file_when_cleanup_lists_old_files_then_delete_file_is_not_returned()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        let mut batch = KvBatch::new();
        stage_retained_snapshot(&mut batch, catalog, 3);
        let source = data_file(10, table, 0, "source.parquet");
        batch.put(data_file_key(catalog, source.data_file_id), source.encode());
        let mut superseded_delete = DeleteFileRow::new(
            DeleteFileId(20),
            source.data_file_id,
            "source-delete-1.parquet",
            50,
            100,
            CatalogOrderId::uuid_v7(2),
        );
        superseded_delete.validity.end_order = Some(CatalogOrderId::uuid_v7(4));
        let cumulative_delete = DeleteFileRow::new(
            DeleteFileId(21),
            source.data_file_id,
            "source-delete-2.parquet",
            80,
            100,
            CatalogOrderId::uuid_v7(2),
        )
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(4)));
        batch.put(
            delete_file_key(catalog, superseded_delete.delete_file_id),
            superseded_delete.encode(),
        );
        batch.put(
            delete_file_key(catalog, cumulative_delete.delete_file_id),
            cumulative_delete.encode(),
        );
        batch.put(
            delete_file_timeline_key(
                catalog,
                source.data_file_id,
                CatalogOrderId::uuid_v7(2),
                superseded_delete.delete_file_id,
            ),
            superseded_delete.encode(),
        );
        batch.put(
            delete_file_timeline_key(
                catalog,
                source.data_file_id,
                CatalogOrderId::uuid_v7(4),
                cumulative_delete.delete_file_id,
            ),
            cumulative_delete.encode(),
        );
        stage_scheduled_delete_file_cleanup(
            &mut batch,
            catalog,
            table,
            superseded_delete.delete_file_id,
        );
        kv.commit(batch).unwrap();
        let snapshots = vec![SnapshotRow::new(
            CatalogOrderId::uuid_v7(3),
            RawSnapshotSequence(3),
        )];

        assert!(
            !delete_file_is_safe_for_physical_cleanup(&kv, catalog, &superseded_delete, &snapshots)
                .unwrap()
        );
        assert_eq!(
            delete_file_physical_cleanup_decision(&kv, catalog, &superseded_delete, &snapshots)
                .unwrap(),
            DeleteFilePhysicalCleanupDecision::CleanupCandidateStillNeededByRetainedSnapshot
        );

        let payload = old_files_cleanup_payload(
            &kv,
            catalog,
            OldFilesCleanupRequest {
                cleanup_all: true,
                schedule_before_micros: None,
            },
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("cleanup_file_count=0"), "{text}");
        assert!(!text.contains("source-delete-1.parquet"), "{text}");
    }

    fn data_file(file_id: u64, table: TableId, row_id_start: u64, path: &str) -> DataFileRow {
        DataFileRow::new(
            DataFileId(file_id),
            table,
            path,
            1,
            100,
            CatalogOrderId::uuid_v7(0),
        )
        .with_row_id_start(row_id_start)
    }

    fn stage_unreachable_scheduled_file(
        batch: &mut KvBatch,
        catalog: CatalogId,
        file_id: u64,
        schedule_start_micros: i64,
        path: &str,
    ) {
        stage_scheduled_file(
            batch,
            catalog,
            file_id,
            schedule_start_micros,
            path,
            Some(CatalogOrderId::uuid_v7(2)),
        );
    }

    fn stage_scheduled_file(
        batch: &mut KvBatch,
        catalog: CatalogId,
        file_id: u64,
        schedule_start_micros: i64,
        path: &str,
        end_order: Option<CatalogOrderId>,
    ) {
        let mut data_file = DataFileRow::new(
            DataFileId(file_id),
            TableId(1),
            path,
            10,
            100,
            CatalogOrderId::uuid_v7(1),
        );
        data_file.validity.end_order = end_order;
        batch.put(
            data_file_key(catalog, data_file.data_file_id),
            data_file.encode(),
        );
        batch.put(
            scheduled_data_file_cleanup_key(catalog, data_file.data_file_id),
            schedule_start_micros.to_be_bytes().to_vec(),
        );
    }

    fn stage_retained_snapshot(batch: &mut KvBatch, catalog: CatalogId, order: u128) {
        let snapshot = SnapshotRow::new(
            CatalogOrderId::uuid_v7(order),
            RawSnapshotSequence(order as u64),
        );
        stage_snapshot(batch, catalog, &snapshot);
    }
}
