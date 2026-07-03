#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, ColumnDrop, ColumnId, DataFileId, DataFileRow, DeleteFileId,
        DeleteFileRow, DuckLakeSnapshotId, FakeOrderedCatalogKv, InlineFileDeletionRow,
        InlineRowChangeKind, InlineTableFlush, InlinedTableRow, KvBatch, MacroId,
        MacroImplementationRow, MacroRow, RawSnapshotSequence, SchemaId, SchemaRow, SnapshotRow,
        SnapshotTimestampBound, TableColumnRow, TableId, TablePartitionChange,
        TablePartitionFieldRow, TablePartitionRow, TableRow, ValidityWindow,
        commit_append_data_files, commit_append_data_files_with_inline_flushes,
        commit_append_table_columns, commit_change_table_partition, commit_create_macro_rows,
        commit_create_schema_rows, commit_create_table_row, commit_drop_table_columns,
        commit_inline_file_deletions, commit_register_delete_files,
        conflict::write_data_file_change,
        data_file_store::stage_append_data_file,
        expire_snapshots, initialize_catalog_if_absent,
        inline_change_feed::stage_inline_row_change,
        keys::{
            order_delete_file_change_key, schema_object_key, table_delete_file_change_key,
            table_object_key,
        },
        latest_snapshot, public_snapshot_sequence_for_order,
        register_inline_table_payload_with_table,
        runtime_catalog_snapshot::{conflict_snapshot_payload, public_snapshot_payload},
        snapshot_operations::{SnapshotOperationKind, stage_snapshot_operation},
        store::stage_snapshot,
    };

    use super::super::{
        ListSnapshotsPayload, SnapshotReadContext, list_snapshots_payload,
        snapshot_by_ducklake_sequence, snapshot_by_public_sequence, snapshot_changes_after_payload,
        snapshot_changes_made, snapshot_schema_version, snapshot_schema_versions_by_order,
    };

    #[test]
    fn given_snapshot_context_when_selecting_by_timestamp_then_bounds_use_loaded_snapshots() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        for (order_number, sequence, created_at_micros) in
            [(10, 1, 1_000), (20, 2, 2_000), (30, 3, 3_000)]
        {
            stage_snapshot(
                &mut batch,
                catalog,
                &SnapshotRow::with_created_at_micros(
                    CatalogOrderId::uuid_v7(order_number),
                    RawSnapshotSequence(sequence),
                    created_at_micros,
                ),
            );
        }
        kv.commit(batch).unwrap();

        let context = SnapshotReadContext::for_current_catalog(&kv, catalog).unwrap();

        let lower = context
            .snapshot_at_timestamp(1_500, SnapshotTimestampBound::Lower)
            .unwrap();
        let upper = context
            .snapshot_at_timestamp(1_500, SnapshotTimestampBound::Upper)
            .unwrap();
        assert_eq!(lower.sequence, RawSnapshotSequence(2));
        assert_eq!(upper.sequence, RawSnapshotSequence(1));
    }

    #[test]
    fn given_macro_created_when_getting_snapshot_then_public_schema_version_advances() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let created = commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![MacroRow::new(
                MacroId(7),
                SchemaId(0),
                "simple",
                vec![MacroImplementationRow {
                    dialect: "duckdb".to_owned(),
                    sql: "a".to_owned(),
                    macro_type: "scalar".to_owned(),
                    parameters: Vec::new(),
                }],
                CatalogOrderId::uuid_v7(0),
            )],
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();

        let public_sequence =
            public_snapshot_sequence_for_order(&kv, catalog, created[0].validity.begin_order)
                .unwrap()
                .unwrap();
        assert_eq!(public_sequence, DuckLakeSnapshotId(1));
        assert_eq!(
            snapshot_schema_version(&kv, catalog, created[0].validity.begin_order).unwrap(),
            1
        );
        assert_eq!(
            snapshot_changes_made(&kv, catalog, created[0].validity.begin_order).unwrap(),
            "created_scalar_macro:\"main\".\"simple\""
        );
    }

    #[test]
    fn given_catalog_schema_changes_when_indexing_schema_versions_then_matches_per_snapshot_lookup()
    {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let schema = commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(9),
                "schema-9",
                "extra",
                "extra",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let table = commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(20),
                schema[0].schema_id,
                "table-20",
                "t",
                "t",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "BIGINT",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![MacroRow::new(
                MacroId(30),
                schema[0].schema_id,
                "m",
                vec![MacroImplementationRow {
                    dialect: "duckdb".to_owned(),
                    sql: "1".to_owned(),
                    macro_type: "scalar".to_owned(),
                    parameters: Vec::new(),
                }],
                CatalogOrderId::uuid_v7(0),
            )],
            Some(RawSnapshotSequence(3)),
        )
        .unwrap();

        let indexed = snapshot_schema_versions_by_order(&kv, catalog).unwrap();
        for snapshot in [schema[0].validity.begin_order, table.validity.begin_order] {
            assert_eq!(
                indexed.get(&snapshot).copied(),
                Some(snapshot_schema_version(&kv, catalog, snapshot).unwrap())
            );
        }
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(
            indexed.get(&latest.order).copied(),
            Some(snapshot_schema_version(&kv, catalog, latest.order).unwrap())
        );
    }

    #[test]
    fn given_added_and_removed_files_without_delete_evidence_when_getting_snapshot_changes_then_reports_merge_adjacent()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let order = CatalogOrderId::uuid_v7(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut batch = KvBatch::new();
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Removed,
            DataFileId(1),
        );
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Added,
            DataFileId(2),
        );
        kv.commit(batch).unwrap();

        let changes = snapshot_changes_made(&kv, catalog, order).unwrap();

        assert_eq!(changes, "merge_adjacent:10");
    }

    #[test]
    fn given_added_and_removed_files_with_delete_evidence_when_getting_snapshot_changes_then_reports_rewrite_delete()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let order = CatalogOrderId::uuid_v7(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut batch = KvBatch::new();
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Removed,
            DataFileId(1),
        );
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Added,
            DataFileId(2),
        );
        batch.put(
            table_delete_file_change_key(catalog, table_id, order, DeleteFileId(20)),
            Vec::new(),
        );
        batch.put(
            order_delete_file_change_key(catalog, order, table_id, DeleteFileId(20)),
            Vec::new(),
        );
        kv.commit(batch).unwrap();

        let changes = snapshot_changes_made(&kv, catalog, order).unwrap();

        assert_eq!(changes, "rewrite_delete:10");
    }

    #[test]
    fn given_rewrite_delete_operation_when_getting_snapshot_changes_then_reports_rewrite_delete() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let order = CatalogOrderId::uuid_v7(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut batch = KvBatch::new();
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Removed,
            DataFileId(1),
        );
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            order,
            crate::DataFileChangeKind::Added,
            DataFileId(2),
        );
        stage_snapshot_operation(
            &mut batch,
            catalog,
            order,
            SnapshotOperationKind::RewriteDelete,
            table_id,
        );
        kv.commit(batch).unwrap();

        let changes = snapshot_changes_made(&kv, catalog, order).unwrap();

        assert_eq!(changes, "rewrite_delete:10");
    }

    #[test]
    fn given_rewrite_removes_file_with_prior_delete_evidence_when_getting_snapshot_changes_then_reports_rewrite_delete()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "main/table/source.parquet",
                10,
                100,
                CatalogOrderId::uuid_v7(1),
            )],
        )
        .unwrap();
        commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(20),
                DataFileId(1),
                "main/table/source-delete.parquet",
                3,
                50,
                CatalogOrderId::uuid_v7(2),
            )],
        )
        .unwrap();

        let rewrite_order = CatalogOrderId::uuid_v7(7);
        let mut batch = KvBatch::new();
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            rewrite_order,
            crate::DataFileChangeKind::Removed,
            DataFileId(1),
        );
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            rewrite_order,
            crate::DataFileChangeKind::Added,
            DataFileId(2),
        );
        kv.commit(batch).unwrap();

        let changes = snapshot_changes_made(&kv, catalog, rewrite_order).unwrap();

        assert_eq!(changes, "rewrite_delete:10");
    }

    #[test]
    fn given_inline_table_registration_when_getting_snapshot_then_raw_schema_version_does_not_advance()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created_table = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let create_schema_version =
            snapshot_schema_version(&kv, catalog, create_snapshot.order).unwrap();

        let mut table_with_inline = created_table;
        table_with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new("orders_inlined_1", 1));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_with_inline,
            SchemaId(1),
            b"row\t1\ti:7\n".to_vec(),
        )
        .unwrap();

        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(
            snapshot_schema_version(&kv, catalog, latest.order).unwrap(),
            create_schema_version
        );
    }

    #[test]
    fn given_inline_rows_are_flushed_then_snapshot_changes_report_inline_categories() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created_table = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let mut table_with_inline = created_table;
        table_with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(
                "ducklake_inlined_data_10_1",
                schema_id.0,
            ));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_with_inline,
            schema_id,
            b"row\t1\ti:7\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        assert_eq!(
            snapshot_changes_made(&kv, catalog, inline_snapshot.order).unwrap(),
            "inlined_insert:10"
        );

        commit_append_data_files_with_inline_flushes(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(20),
                table_id,
                "main/orders/file.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
            &[InlineTableFlush::new(
                table_id,
                schema_id,
                inline_snapshot.sequence,
            )],
        )
        .unwrap();
        let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        assert_eq!(
            snapshot_changes_made(&kv, catalog, flush_snapshot.order).unwrap(),
            "flushed_inlined:10"
        );
    }

    #[test]
    fn given_table_created_and_partitioned_for_same_ducklake_commit_then_public_snapshots_are_coalesced()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        commit_change_table_partition(
            &mut kv,
            catalog,
            &TablePartitionChange::new(table_id, Some(identity_partition(ColumnId(1)))),
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=2"));
        assert!(payload.contains("snapshot\t0\t"));
        assert!(payload.contains("snapshot\t1\t"));
        assert!(payload.contains("created_table:\"main\".\"orders\",altered_table:10"));
        assert_eq!(public_snapshot_schema_version(&payload, 1), Some(1));
        for snapshot_row in payload
            .lines()
            .filter(|line| line.starts_with("snapshot\t"))
        {
            assert_eq!(snapshot_row.split('\t').count(), 8);
        }
    }

    #[test]
    fn given_transaction_schema_and_table_then_later_partition_alter_public_schema_version_counts_transaction_once()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_schema_snapshot(&mut kv, catalog, 10, SchemaId(1), "s1", 1);
        stage_table_snapshot_in_schema(&mut kv, catalog, 20, table_id, SchemaId(1), "orders", 1);

        let alter_order = CatalogOrderId::uuid_v7(30);
        let mut previous = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(20));
        previous.schema_id = SchemaId(1);
        previous.validity = ValidityWindow::new(CatalogOrderId::uuid_v7(20), Some(alter_order));
        let mut next = previous.clone();
        next.validity = ValidityWindow::new(alter_order, None);
        next.partition = Some(identity_partition(ColumnId(1)));
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(alter_order, crate::RawSnapshotSequence(2)),
        );
        batch.put(
            table_object_key(catalog, table_id, previous.validity.begin_order),
            previous.encode(),
        );
        batch.put(
            table_object_key(catalog, table_id, alter_order),
            next.encode(),
        );
        kv.commit(batch).unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"), "{payload}");
        assert_eq!(public_snapshot_schema_version(&payload, 1), Some(1));
        assert_eq!(public_snapshot_schema_version(&payload, 2), Some(2));
        assert!(payload.contains("created_schema:\"s1\""), "{payload}");
        assert!(
            payload.contains("created_table:\"s1\".\"orders\""),
            "{payload}"
        );
        assert!(payload.contains("altered_table:10"), "{payload}");
    }

    #[test]
    fn given_implicit_main_schema_created_before_first_table_then_public_snapshot_is_one_commit() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=2"));
        assert!(payload.contains("snapshot\t1\t"));
        assert!(payload.contains("created_schema:\"main\",created_table:\"main\".\"orders\""));
    }

    #[test]
    fn given_conflict_check_then_storage_returns_only_changes_after_base_public_snapshot() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(TableId(10), "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();

        let payload = String::from_utf8(
            snapshot_changes_after_payload(&kv, catalog, DuckLakeSnapshotId(0)).unwrap(),
        )
        .unwrap();

        assert_eq!(
            payload,
            "changes_made=created_schema:\"main\",created_table:\"main\".\"orders\"\n"
        );
    }

    #[test]
    fn given_snapshot_ids_shift_after_expiry_when_requested_by_ducklake_id_then_stable_ids_are_returned()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, TableId(10), "orders", 1);
        stage_data_snapshot(&mut kv, catalog, 20, 2, DataFileId(20), TableId(10));
        stage_delete_file_snapshot(&mut kv, catalog, 30, 3, DeleteFileId(30), TableId(10));
        stage_delete_file_snapshot(&mut kv, catalog, 40, 4, DeleteFileId(40), TableId(10));
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(CatalogOrderId::uuid_v7(50), RawSnapshotSequence(5)),
        );
        kv.commit(batch).unwrap();
        expire_snapshots(&mut kv, catalog, &[RawSnapshotSequence(3)]).unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: Some(vec![
                        DuckLakeSnapshotId(1),
                        DuckLakeSnapshotId(2),
                        DuckLakeSnapshotId(4),
                    ]),
                    protect_latest: true,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"), "{payload}");
        assert!(payload.contains("snapshot\t1\t"));
        assert!(payload.contains("snapshot\t2\t"));
        assert!(payload.contains("snapshot\t4\t"));
        assert!(!payload.contains("snapshot\t3\t"));
        assert!(!payload.contains("snapshot\t5\t"));
    }

    #[test]
    fn given_snapshot_expired_when_resolving_public_snapshot_then_raw_history_remains_internal_only()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let expired = SnapshotRow::new(CatalogOrderId::uuid_v7(10), RawSnapshotSequence(1));
        let current = SnapshotRow::new(CatalogOrderId::uuid_v7(20), RawSnapshotSequence(2));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &expired);
        stage_snapshot(&mut batch, catalog, &current);
        kv.commit(batch).unwrap();

        expire_snapshots(&mut kv, catalog, &[expired.sequence]).unwrap();

        assert_eq!(
            snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(expired.sequence.0))
                .unwrap(),
            None
        );
        let payload =
            public_snapshot_payload(&kv, catalog, DuckLakeSnapshotId(expired.sequence.0)).unwrap();
        assert_eq!(
            String::from_utf8(payload).unwrap(),
            "catalog_snapshot_exists=false\n"
        );
        assert!(
            snapshot_by_ducklake_sequence(&kv, catalog, DuckLakeSnapshotId(expired.sequence.0))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn given_multiple_rows_share_ducklake_id_when_requested_then_storage_returns_one_snapshot() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_schema_snapshot(&mut kv, catalog, 10, SchemaId(1), "s1", 1);
        stage_table_snapshot_in_schema(&mut kv, catalog, 20, TableId(10), SchemaId(1), "orders", 1);
        stage_data_snapshot(&mut kv, catalog, 30, 2, DataFileId(20), TableId(10));
        stage_data_snapshot(&mut kv, catalog, 40, 3, DataFileId(21), TableId(10));

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: Some(vec![
                        DuckLakeSnapshotId(1),
                        DuckLakeSnapshotId(2),
                    ]),
                    protect_latest: true,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=2"), "{payload}");
        assert_eq!(payload.matches("snapshot\t1\t").count(), 1, "{payload}");
        assert_eq!(payload.matches("snapshot\t2\t").count(), 1, "{payload}");
        assert!(!payload.contains("snapshot\t3\t"), "{payload}");
    }

    #[test]
    fn given_expired_schema_change_snapshot_when_table_still_current_then_schema_version_survives()
    {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, TableId(10), "orders", 1);
        let first_file_order = CatalogOrderId::uuid_v7(20);
        stage_data_snapshot(&mut kv, catalog, 20, 2, DataFileId(20), TableId(10));
        stage_data_snapshot(&mut kv, catalog, 30, 3, DataFileId(21), TableId(10));

        expire_snapshots(
            &mut kv,
            catalog,
            &[RawSnapshotSequence(1), RawSnapshotSequence(2)],
        )
        .unwrap();
        let current = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let payload = String::from_utf8(conflict_snapshot_payload(&kv, catalog).unwrap()).unwrap();

        assert_eq!(current.sequence, RawSnapshotSequence(3));
        assert!(payload.contains("ducklake_schema_version=1\n"), "{payload}");
        assert_eq!(
            snapshot_schema_version(&kv, catalog, current.order).unwrap(),
            1
        );
        assert_eq!(
            public_snapshot_sequence_for_order(&kv, catalog, first_file_order).unwrap(),
            None
        );
    }

    #[test]
    fn given_expired_add_column_snapshot_when_listing_public_snapshots_then_schema_version_survives()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        commit_append_table_columns(
            &mut kv,
            catalog,
            table_id,
            vec![TableColumnRow::new(
                ColumnId(2),
                "status",
                "VARCHAR",
                false,
                None,
            )],
        )
        .unwrap();
        let add_column = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table_id, ColumnId(1))])
            .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        expire_snapshots(&mut kv, catalog, &[add_column.sequence]).unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let latest_line = payload
            .lines()
            .filter(|line| line.starts_with("snapshot\t"))
            .next_back()
            .unwrap();
        let fields = latest_line.split('\t').collect::<Vec<_>>();

        assert_eq!(latest.sequence, RawSnapshotSequence(3));
        assert_eq!(fields[3], "3", "{payload}");
    }

    #[test]
    fn given_conflict_check_then_latest_snapshot_metadata_avoids_public_snapshot_listing() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(TableId(10), "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();

        let payload = String::from_utf8(conflict_snapshot_payload(&kv, catalog).unwrap()).unwrap();

        assert!(payload.contains("ducklake_snapshot_id=2\n"));
        assert!(payload.contains("ducklake_schema_version=2\n"));
        assert!(payload.contains("ducklake_next_catalog_id=11\n"));
        assert!(payload.contains("ducklake_next_file_id=0\n"));
    }

    #[test]
    fn given_data_commit_after_schema_changes_when_getting_conflict_snapshot_then_schema_version_stays_current()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let table = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();

        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(20),
                table.table_id,
                "file-20.parquet",
                10,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let payload = String::from_utf8(conflict_snapshot_payload(&kv, catalog).unwrap()).unwrap();

        assert!(payload.contains("ducklake_snapshot_id=3\n"));
        assert!(payload.contains("ducklake_schema_version=2\n"));
    }

    #[test]
    fn given_explicit_schema_created_before_table_then_public_snapshots_stay_separate() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(1),
                "s1-uuid",
                "s1",
                "s1/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let mut table = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0));
        table.schema_id = SchemaId(1);
        table.path = "s1/orders".to_owned();
        commit_create_table_row(&mut kv, catalog, table).unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"));
        assert!(payload.contains("snapshot\t1\t"));
        assert!(payload.contains("created_schema:\"s1\""));
        assert!(payload.contains("snapshot\t2\t"));
        assert!(payload.contains("created_table:\"s1\".\"orders\""));
    }

    #[test]
    fn given_explicit_transaction_creates_schema_table_and_data_then_public_snapshot_merges_them() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_schema_snapshot(&mut kv, catalog, 10, SchemaId(1), "s1", 1);
        stage_table_snapshot_in_schema(&mut kv, catalog, 20, table_id, SchemaId(1), "orders", 1);
        stage_data_snapshot(&mut kv, catalog, 30, 1, DataFileId(20), table_id);

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=2"));
        assert!(payload.contains("snapshot\t1\t"));
        assert!(payload.contains("created_schema:\"s1\""));
        assert!(payload.contains("created_table:\"s1\".\"orders\""));
        assert!(payload.contains("inserted_into_table:10"));
    }

    #[test]
    fn given_explicit_transaction_creates_table_and_inline_rows_then_public_snapshot_merges_them() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_schema_snapshot(&mut kv, catalog, 10, SchemaId(1), "s1", 1);
        stage_table_snapshot_in_schema(&mut kv, catalog, 20, table_id, SchemaId(1), "orders", 1);
        stage_inline_insert_snapshot(&mut kv, catalog, 30, 1, table_id, SchemaId(1));

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=2"), "{payload}");
        assert_eq!(payload.matches("snapshot\t1\t").count(), 1, "{payload}");
        assert!(payload.contains("created_schema:\"s1\""), "{payload}");
        assert!(
            payload.contains("created_table:\"s1\".\"orders\""),
            "{payload}"
        );
        assert!(payload.contains("inlined_insert:10"), "{payload}");
    }

    #[test]
    fn given_table_renamed_when_listing_snapshots_then_new_table_name_is_findable() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, table_id, "a", 1);
        stage_table_rename_snapshot(&mut kv, catalog, 10, 20, table_id, "a", "b", 2);

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"), "{payload}");
        assert!(
            payload.contains("created_table:\"main\".\"b\""),
            "{payload}"
        );
    }

    #[test]
    fn given_public_snapshot_id_after_helper_commit_then_it_maps_to_last_raw_snapshot_in_group() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        commit_change_table_partition(
            &mut kv,
            catalog,
            &TablePartitionChange::new(table_id, Some(identity_partition(ColumnId(1)))),
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let public = snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(1))
            .unwrap()
            .unwrap();

        assert_eq!(public.order, latest.order);
        assert_eq!(public.sequence, crate::RawSnapshotSequence(1));
    }

    #[test]
    fn given_inline_file_delete_after_append_when_mapping_public_snapshots_then_delete_stays_separate()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let [file] = commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table_id,
                    "main/orders/file.parquet",
                    20,
                    256,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
        )
        .unwrap()
        .try_into()
        .unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![InlineFileDeletionRow::new(
                table_id,
                file.data_file_id,
                0,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let append_public = snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(2))
            .unwrap()
            .unwrap();
        let delete_public = snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(3))
            .unwrap()
            .unwrap();

        assert_eq!(append_public.order, file.validity.begin_order);
        assert_eq!(delete_public.order, delete_snapshot.order);
        assert_ne!(append_public.order, delete_public.order);
    }

    #[test]
    fn given_data_change_after_schema_change_then_public_snapshots_do_not_merge_data_commit() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        commit_change_table_partition(
            &mut kv,
            catalog,
            &TablePartitionChange::new(table_id, Some(identity_partition(ColumnId(1)))),
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "main/orders/file.parquet",
                3,
                1024,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"));
        assert!(payload.contains("inserted_into_table:10"));
    }

    #[test]
    fn given_delete_file_change_then_snapshot_changes_report_table_delete() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, table_id, "orders", 1);
        stage_delete_file_snapshot(&mut kv, catalog, 20, 2, DeleteFileId(5), table_id);

        let payload = String::from_utf8(
            list_snapshots_payload(
                &kv,
                catalog,
                ListSnapshotsPayload {
                    older_than_micros: None,
                    requested_ducklake_ids: None,
                    protect_latest: false,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("snapshot_count=3"));
        assert!(payload.contains("deleted_from_table:10"));
    }

    #[test]
    fn given_concurrent_data_commits_reuse_proposed_snapshot_then_public_snapshots_stay_separate() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, TableId(1), "orders", 1);
        stage_table_snapshot(&mut kv, catalog, 20, TableId(2), "returns", 2);
        stage_data_snapshot(&mut kv, catalog, 30, 2, DataFileId(20), TableId(2));
        stage_data_snapshot(&mut kv, catalog, 40, 2, DataFileId(21), TableId(1));

        let current = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let current_public =
            public_snapshot_sequence_for_order(&kv, catalog, current.order).unwrap();

        assert_eq!(current_public, Some(DuckLakeSnapshotId(2)));
    }

    #[test]
    fn given_multiple_public_groups_share_snapshot_id_when_resolving_snapshot_then_latest_group_is_used()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, TableId(1), "orders", 1);
        stage_data_snapshot(&mut kv, catalog, 20, 1, DataFileId(20), TableId(1));

        let public = snapshot_by_public_sequence(&kv, catalog, DuckLakeSnapshotId(1))
            .unwrap()
            .unwrap();

        assert_eq!(public.order, CatalogOrderId::uuid_v7(20));
    }

    #[test]
    fn given_first_insert_reuses_helper_snapshot_id_then_it_gets_next_public_snapshot() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        stage_table_snapshot(&mut kv, catalog, 10, TableId(1), "orders", 1);
        stage_table_replacement_snapshot(&mut kv, catalog, 10, 20, TableId(1), "orders", 2);
        stage_data_snapshot(&mut kv, catalog, 30, 2, DataFileId(20), TableId(1));

        let file_snapshot =
            public_snapshot_sequence_for_order(&kv, catalog, CatalogOrderId::uuid_v7(30)).unwrap();

        assert_eq!(file_snapshot, Some(DuckLakeSnapshotId(2)));
    }

    fn table_with_columns(table_id: TableId, name: &str, begin_order: CatalogOrderId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                false,
                None,
            )],
            begin_order,
        )
    }

    fn identity_partition(column_id: ColumnId) -> TablePartitionRow {
        TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, column_id, "identity")],
        )
    }

    fn stage_table_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        table_id: TableId,
        name: &str,
        sequence: u64,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let mut table = table_with_columns(table_id, name, order);
        table.validity = ValidityWindow::new(order, None);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(table_object_key(catalog, table_id, order), table.encode());
        kv.commit(batch).unwrap();
    }

    fn stage_schema_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        schema_id: SchemaId,
        name: &str,
        sequence: u64,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let schema = SchemaRow::new(
            schema_id,
            format!("{name}-uuid"),
            name,
            format!("{name}/"),
            order,
        );
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(
            schema_object_key(catalog, schema_id, order),
            schema.encode(),
        );
        kv.commit(batch).unwrap();
    }

    fn stage_table_snapshot_in_schema(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        table_id: TableId,
        schema_id: SchemaId,
        name: &str,
        sequence: u64,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let mut table = table_with_columns(table_id, name, order);
        table.schema_id = schema_id;
        table.path = format!("{}/{}", schema_id.0, name);
        table.validity = ValidityWindow::new(order, None);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(table_object_key(catalog, table_id, order), table.encode());
        kv.commit(batch).unwrap();
    }

    fn stage_data_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        sequence: u64,
        data_file_id: DataFileId,
        table_id: TableId,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let mut file = DataFileRow::new(
            data_file_id,
            table_id,
            format!("main/table-{}/file-{}.parquet", table_id.0, data_file_id.0),
            1,
            128,
            order,
        );
        file.validity = ValidityWindow::new(order, None);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        stage_append_data_file(kv, &mut batch, catalog, &file).unwrap();
        kv.commit(batch).unwrap();
    }

    fn stage_inline_insert_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        sequence: u64,
        table_id: TableId,
        schema_id: SchemaId,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        stage_inline_row_change(
            &mut batch,
            catalog,
            table_id,
            schema_id,
            order,
            InlineRowChangeKind::Inserted,
            0,
        );
        kv.commit(batch).unwrap();
    }

    fn stage_delete_file_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        order_number: u128,
        sequence: u64,
        delete_file_id: DeleteFileId,
        table_id: TableId,
    ) {
        let order = CatalogOrderId::uuid_v7(order_number);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(
            table_delete_file_change_key(catalog, table_id, order, delete_file_id),
            Vec::new(),
        );
        batch.put(
            order_delete_file_change_key(catalog, order, table_id, delete_file_id),
            Vec::new(),
        );
        kv.commit(batch).unwrap();
    }

    fn stage_table_rename_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        previous_order_number: u128,
        next_order_number: u128,
        table_id: TableId,
        previous_name: &str,
        next_name: &str,
        sequence: u64,
    ) {
        let previous_order = CatalogOrderId::uuid_v7(previous_order_number);
        let next_order = CatalogOrderId::uuid_v7(next_order_number);
        let mut previous = table_with_columns(table_id, previous_name, previous_order);
        previous.validity = ValidityWindow::new(previous_order, Some(next_order));
        let mut next = table_with_columns(table_id, next_name, next_order);
        next.validity = ValidityWindow::new(next_order, None);
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(next_order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(
            table_object_key(catalog, table_id, previous_order),
            previous.encode(),
        );
        batch.put(
            table_object_key(catalog, table_id, next_order),
            next.encode(),
        );
        kv.commit(batch).unwrap();
    }

    fn stage_table_replacement_snapshot(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        previous_order_number: u128,
        next_order_number: u128,
        table_id: TableId,
        name: &str,
        sequence: u64,
    ) {
        let previous_order = CatalogOrderId::uuid_v7(previous_order_number);
        let next_order = CatalogOrderId::uuid_v7(next_order_number);
        let mut previous = table_with_columns(table_id, name, previous_order);
        previous.validity = ValidityWindow::new(previous_order, Some(next_order));
        let mut next = table_with_columns(table_id, name, next_order);
        next.validity = ValidityWindow::new(next_order, None);
        next.inlined_data_tables
            .push(InlinedTableRow::new("orders_inlined_1", 1));
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(next_order, crate::RawSnapshotSequence(sequence)),
        );
        batch.put(
            table_object_key(catalog, table_id, previous_order),
            previous.encode(),
        );
        batch.put(
            table_object_key(catalog, table_id, next_order),
            next.encode(),
        );
        kv.commit(batch).unwrap();
    }

    fn public_snapshot_schema_version(payload: &str, snapshot_id: u64) -> Option<u64> {
        payload
            .lines()
            .filter(|line| line.starts_with("snapshot\t"))
            .find_map(|line| {
                let fields = line.split('\t').collect::<Vec<_>>();
                if fields.get(1).and_then(|value| value.parse::<u64>().ok()) == Some(snapshot_id) {
                    return fields.get(3).and_then(|value| value.parse::<u64>().ok());
                }
                None
            })
    }
}
