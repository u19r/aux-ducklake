#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
    };

    #[cfg(feature = "foundationdb")]
    use crate::file_partitions::stage_file_partition_value;
    use crate::{
        AttachedDataFile, CatalogId, CatalogOrderId, ColumnDrop, ColumnId, DataFileId, DataFileRow,
        FakeOrderedCatalogKv, InlineFileDeletionRow, InlineTableFlush, InlinedTableRow,
        OrderedCatalogKv, RangeDirection, RangeItem, RawSnapshotSequence, SchemaId, TableColumnRow,
        TableId, TablePartitionChange, TablePartitionFieldRow, TablePartitionRow, TableRow,
        append_data_file, commit_append_data_files, commit_append_table_columns,
        commit_change_table_partition, commit_create_table_row, commit_data_mutation,
        commit_drop_table_columns, commit_inline_file_deletions, expire_snapshots,
        initialize_catalog_if_absent,
        keys::{
            family_prefix, inline_file_deletion_file_prefix, inline_file_deletion_table_prefix,
            snapshot_timestamp_prefix, table_object_scan_prefix,
        },
        latest_snapshot, register_inline_table_payload_with_table,
    };
    #[cfg(feature = "foundationdb")]
    use crate::{FilePartitionValueRow, KvBatch, PartitionKeyIndex};

    use super::super::{
        FileListingRequestContext, FileListingRole, InlineSuppressionContext,
        ListDataFilesAtPayload, data_files_at_payload, file_listing_role,
        partition_files_payload_with_context,
        should_suppress_unflushed_inline_materialization_file,
    };
    #[cfg(feature = "foundationdb")]
    use super::super::{PartitionPruneComparison, matching_partition_values};

    #[test]
    fn given_latest_file_listing_when_resolving_snapshot_then_snapshot_history_is_not_scanned() {
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
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/latest.parquet",
                1,
                1024,
                snapshot.order,
            ),
        )
        .unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(kv, catalog);

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("main/orders/latest.parquet"), "{payload}");
        assert_eq!(
            kv.snapshot_timestamp_scan_count(),
            1,
            "latest snapshot resolution should use the latest row; the one remaining scan renders public snapshot ids"
        );
    }

    #[test]
    fn given_non_snapshot_schema_orders_when_rendering_files_then_schema_versions_are_reused() {
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
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let schema_order = CatalogOrderId::uuid_v7(1);
        for data_file_id in [DataFileId(7), DataFileId(8)] {
            append_data_file(
                &mut kv,
                catalog,
                DataFileRow::new(
                    data_file_id,
                    table_id,
                    format!("main/orders/{}.parquet", data_file_id.0),
                    1,
                    1024,
                    snapshot.order,
                )
                .with_max_partial_order(Some(schema_order)),
            )
            .unwrap();
        }
        let kv = TableObjectScanRecordingKv::new(kv, catalog);

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=2"), "{payload}");
        assert_eq!(
            kv.table_object_scan_count(),
            1,
            "schema-version rendering should reuse the request map for non-snapshot file schema orders"
        );
    }

    #[test]
    fn given_current_partition_render_context_when_rendering_files_then_schema_versions_are_reused()
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
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let schema_order = CatalogOrderId::uuid_v7(1);
        let files = [DataFileId(7), DataFileId(8)]
            .into_iter()
            .map(|data_file_id| {
                AttachedDataFile::new(
                    DataFileRow::new(
                        data_file_id,
                        table_id,
                        format!("main/orders/{}.parquet", data_file_id.0),
                        1,
                        1024,
                        snapshot.order,
                    )
                    .with_max_partial_order(Some(schema_order)),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let kv = TableObjectScanRecordingKv::new(kv, catalog);
        let mut request = FileListingRequestContext::from_latest(Some(snapshot.clone()));

        let payload = String::from_utf8(
            partition_files_payload_with_context(
                &kv,
                catalog,
                snapshot.order,
                "a".to_owned(),
                files,
                Some(&mut request),
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=2"), "{payload}");
        assert_eq!(
            kv.table_object_scan_count(),
            1,
            "current partition file rendering should reuse the request map for non-snapshot file schema orders"
        );
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_partition_lookup_values_when_matching_range_then_returns_candidate_values() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        for (data_file_id, partition_key_index, partition_value) in [
            (DataFileId(7), PartitionKeyIndex(0), "apac"),
            (DataFileId(8), PartitionKeyIndex(0), "eu"),
            (DataFileId(9), PartitionKeyIndex(0), "us"),
            (DataFileId(10), PartitionKeyIndex(1), "1"),
            (DataFileId(11), PartitionKeyIndex(1), "2"),
            (DataFileId(12), PartitionKeyIndex(1), "10"),
        ] {
            let data_file = DataFileRow::new(
                data_file_id,
                table_id,
                format!("main/orders/{partition_value}.parquet"),
                1,
                1024,
                CatalogOrderId::uuid_v7(0),
            );
            let partition = FilePartitionValueRow::new(
                data_file_id,
                table_id,
                partition_key_index,
                partition_value,
            );
            let mut batch = KvBatch::new();
            stage_file_partition_value(&mut batch, catalog, &partition, &data_file);
            kv.commit(batch).unwrap();
        }

        let regions = matching_partition_values(
            &kv,
            catalog,
            table_id,
            PartitionKeyIndex(0),
            "VARCHAR",
            PartitionPruneComparison::GreaterThanOrEqual,
            "eu",
        )
        .unwrap();
        let buckets = matching_partition_values(
            &kv,
            catalog,
            table_id,
            PartitionKeyIndex(1),
            "INTEGER",
            PartitionPruneComparison::GreaterThanOrEqual,
            "2",
        )
        .unwrap();

        assert_eq!(regions, vec!["eu", "us"]);
        assert_eq!(
            buckets.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from(["2".to_owned(), "10".to_owned()])
        );
    }

    #[test]
    fn given_sparse_backfilled_compaction_replacement_when_inline_begin_is_unmaterialized_then_file_is_not_suppressed()
     {
        let begin_order = CatalogOrderId::uuid_v7(3);
        let mut replacement = DataFileRow::new(
            DataFileId(11),
            TableId(1),
            "main/partitioned/part_key=1/replacement.parquet",
            2,
            677,
            begin_order,
        )
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(7)));
        replacement.row_id_start_known = false;

        let context = InlineSuppressionContext {
            unmaterialized_inline_begin_orders: vec![begin_order],
        };

        assert_eq!(
            file_listing_role(&replacement),
            FileListingRole::BackfilledCompactionReplacement
        );
        assert!(
            !should_suppress_unflushed_inline_materialization_file(&replacement, &context),
            "backfilled compaction replacements are already file-backed source-of-truth rows"
        );
    }

    #[test]
    fn given_sparse_same_snapshot_inline_materialization_when_inline_begin_is_unmaterialized_then_file_is_suppressed()
     {
        let begin_order = CatalogOrderId::uuid_v7(3);
        let mut materialization = DataFileRow::new(
            DataFileId(4),
            TableId(1),
            "main/partitioned/part_key=1/flush.parquet",
            1,
            653,
            begin_order,
        )
        .with_max_partial_order(Some(begin_order));
        materialization.row_id_start_known = false;

        let context = InlineSuppressionContext {
            unmaterialized_inline_begin_orders: vec![begin_order],
        };

        assert_eq!(
            file_listing_role(&materialization),
            FileListingRole::InlineMaterializationFile { begin_order }
        );
        assert!(
            should_suppress_unflushed_inline_materialization_file(&materialization, &context),
            "same-snapshot materializations without row-id anchoring must not double-count inline rows"
        );
    }

    #[test]
    fn given_future_commit_snapshot_when_listing_files_then_latest_committed_files_are_returned() {
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
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/current.parquet",
                1,
                1024,
                snapshot.order,
            ),
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: snapshot.sequence.0 + 8,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("main/orders/current.parquet"), "{payload}");
    }

    #[test]
    fn given_empty_latest_file_listing_when_rendering_then_snapshot_history_is_not_scanned() {
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
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(kv, catalog);

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(payload, "file_count=0\n");
        assert_eq!(kv.snapshot_timestamp_scan_count(), 0);
    }

    #[test]
    fn given_historical_file_listing_when_rendering_then_snapshot_context_is_reused() {
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
        let first_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/later.parquet",
                1,
                1024,
                first_snapshot.order,
            ),
        )
        .unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(kv, catalog);

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: first_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("main/orders/later.parquet"), "{payload}");
        assert_eq!(
            kv.snapshot_timestamp_scan_count(),
            1,
            "historical resolution and render should share one snapshot context"
        );
    }

    #[test]
    fn given_file_begin_snapshot_is_expired_when_listing_current_files_then_begin_snapshot_is_clamped()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        let initial = initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(7),
                    table_id,
                    "main/orders/expired-begin.parquet",
                    1,
                    1024,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
        )
        .unwrap();
        let file_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
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
        let current_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        expire_snapshots(
            &mut kv,
            catalog,
            &[
                initial.sequence,
                create_snapshot.sequence,
                file_snapshot.sequence,
            ],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: current_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=1"), "{payload}");
        assert!(
            payload.contains("main/orders/expired-begin.parquet"),
            "{payload}"
        );
    }

    #[test]
    fn given_backfilled_inline_file_when_listing_at_unflushed_inline_snapshot_then_file_is_suppressed()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut table = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        commit_create_table_row(&mut kv, catalog, table.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table,
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/materialized-inline.parquet",
                1,
                1024,
                inline_snapshot.order,
            )
            .with_max_partial_order(Some(inline_snapshot.order)),
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: inline_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(payload, "file_count=0\n");
    }

    #[test]
    fn given_inline_suppression_when_loading_unflushed_payloads_then_end_orders_are_scanned_once() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let mut table = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        commit_create_table_row(&mut inner, catalog, table.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut inner,
            catalog,
            table,
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        append_data_file(
            &mut inner,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/materialized-inline.parquet",
                1,
                1024,
                inline_snapshot.order,
            )
            .with_max_partial_order(Some(inline_snapshot.order)),
        )
        .unwrap();
        let kv = InlineEndOrderScanRecordingKv::new(inner, catalog, table_id);

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: inline_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(payload, "file_count=0\n");
        assert_eq!(kv.end_order_scan_count(), 1);
    }

    #[test]
    fn given_merge_replacement_spans_unflushed_inline_snapshot_when_listing_then_file_is_returned()
    {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut table = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        commit_create_table_row(&mut kv, catalog, table.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table.clone(),
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let first_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table,
            schema_id,
            b"row\t1\ti:2\n".to_vec(),
        )
        .unwrap();
        let second_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/merge-replacement.parquet",
                2,
                1024,
                first_inline_snapshot.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(second_inline_snapshot.order)),
        )
        .unwrap();

        let listed =
            crate::list_data_files_at(&kv, catalog, table_id, second_inline_snapshot.order)
                .unwrap();
        assert_eq!(listed.len(), 1, "{listed:?}");

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: first_inline_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=1"), "{payload}");
        assert!(
            payload.contains("main/orders/merge-replacement.parquet"),
            "{payload}"
        );
    }

    #[test]
    fn given_flushed_inline_file_begins_and_ends_at_same_snapshot_when_listing_later_version_then_file_is_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut table = table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        commit_create_table_row(&mut kv, catalog, table.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table.clone(),
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let first_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table,
            schema_id,
            b"row\t1\ti:2\n".to_vec(),
        )
        .unwrap();
        let second_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_data_mutation(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(7),
                    table_id,
                    "main/orders/flushed-january.parquet",
                    1,
                    1024,
                    first_inline_snapshot.order,
                )
                .with_row_id_start(0)
                .with_max_partial_order(Some(first_inline_snapshot.order)),
            ],
            vec![],
            &[InlineTableFlush::new(
                table_id,
                schema_id,
                RawSnapshotSequence(second_inline_snapshot.sequence.0),
            )],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: second_inline_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=1"), "{payload}");
        assert!(
            payload.contains("main/orders/flushed-january.parquet"),
            "{payload}"
        );
    }

    #[test]
    fn given_cross_schema_merge_replacement_when_listing_then_schema_version_uses_max_partial_snapshot()
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
        let first_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
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
        let second_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/merged-cross-schema.parquet",
                9,
                581,
                first_snapshot.order,
            )
            .with_row_id_start(0)
            .with_mapping_id(Some(538))
            .with_max_partial_order(Some(second_snapshot.order)),
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: second_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let file_line = payload
            .lines()
            .find(|line| line.starts_with("file\t"))
            .unwrap();
        let fields: Vec<_> = file_line.split('\t').collect();

        assert_eq!(fields[1], "7");
        assert_eq!(fields[7], "538");
        assert_eq!(fields[14], first_snapshot.sequence.0.to_string());
        assert_eq!(fields[15], second_snapshot.sequence.0.to_string());
        assert_eq!(fields[17], "2");
        assert_eq!(fields[19], "");

        let bounded_payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: first_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let bounded_file_line = bounded_payload
            .lines()
            .find(|line| line.starts_with("file\t"))
            .unwrap();
        let bounded_fields: Vec<_> = bounded_file_line.split('\t').collect();

        assert_eq!(bounded_fields[15], second_snapshot.sequence.0.to_string());
        assert_eq!(bounded_fields[19], first_snapshot.sequence.0.to_string());
    }

    #[test]
    fn given_file_schema_snapshot_expired_when_listing_then_schema_version_uses_raw_file_order() {
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
        let schema_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(7),
                    table_id,
                    "main/orders/after-schema-change.parquet",
                    1,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
        )
        .unwrap();
        let data_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        expire_snapshots(&mut kv, catalog, &[schema_snapshot.sequence]).unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: data_snapshot.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let file_line = payload
            .lines()
            .find(|line| line.starts_with("file\t"))
            .unwrap();
        let fields: Vec<_> = file_line.split('\t').collect();

        assert_eq!(fields[1], "7");
        assert_eq!(fields[17], "2", "{payload}");
    }

    #[test]
    fn given_expired_schema_interval_when_listing_then_each_file_keeps_raw_schema_bucket() {
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
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/v1.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
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
        let add_column_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(8),
                table_id,
                "main/orders/v2.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let add_column_data_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table_id, ColumnId(1))])
            .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(9),
                table_id,
                "main/orders/v3.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        expire_snapshots(
            &mut kv,
            catalog,
            &[
                add_column_snapshot.sequence,
                add_column_data_snapshot.sequence,
            ],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: latest.sequence.0,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let schema_versions_by_file = payload
            .lines()
            .filter(|line| line.starts_with("file\t"))
            .map(|line| {
                let fields: Vec<_> = line.split('\t').collect();
                (fields[1].to_string(), fields[17].to_string())
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            schema_versions_by_file.get("7").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            schema_versions_by_file.get("8").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            schema_versions_by_file.get("9").map(String::as_str),
            Some("3")
        );
    }

    #[test]
    fn given_helper_schema_snapshot_before_insert_then_file_listing_exposes_public_begin_snapshot()
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
            &TablePartitionChange::new(
                table_id,
                Some(TablePartitionRow::new(
                    1,
                    vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
                )),
            ),
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/file.parquet",
                3,
                1024,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: 2,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=1"));
        assert!(payload.contains("\tmain/orders/file.parquet\t3\t1024\t\t\t\t\t\t\t\t\t2\t"));
    }

    #[test]
    fn given_file_with_physical_row_ids_when_listing_files_then_row_id_start_is_empty() {
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
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(7),
                table_id,
                "main/orders/rewrite.parquet",
                49,
                760,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: 2,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let file_line = payload
            .lines()
            .find(|line| line.starts_with("file\t"))
            .unwrap();
        let fields: Vec<_> = file_line.split('\t').collect();

        assert_eq!(fields[1], "7");
        assert_eq!(fields[6], "");
    }

    #[test]
    fn given_file_footer_size_when_listing_files_then_payload_keeps_file_tag_plus_eighteen_values()
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
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(7),
                    table_id,
                    "main/orders/file.parquet",
                    3,
                    1024,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_footer_size(Some(151)),
            ],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: 2,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let file_line = payload
            .lines()
            .find(|line| line.starts_with("file\t"))
            .unwrap();
        let fields: Vec<_> = file_line.split('\t').collect();

        assert_eq!(fields.len(), 20);
        assert_eq!(fields[16], "151");
        assert_eq!(fields[17], "1");
        assert_eq!(fields[18], "");
        assert_eq!(fields[19], "");
    }

    #[test]
    fn given_inline_deletion_context_for_one_table_when_loaded_then_only_table_prefix_is_scanned() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let other_table_id = TableId(11);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![
                InlineFileDeletionRow::new(table_id, DataFileId(7), 1, CatalogOrderId::uuid_v7(0)),
                InlineFileDeletionRow::new(
                    other_table_id,
                    DataFileId(8),
                    2,
                    CatalogOrderId::uuid_v7(0),
                ),
            ],
        )
        .unwrap();
        let kv = InlineDeletionScanRecordingKv::new(kv, catalog, table_id, DataFileId(7));

        let rows = crate::runtime_read_context::InlineDeletionReadContext::for_table(
            &kv, catalog, table_id,
        )
        .unwrap()
        .rows_at(CatalogOrderId::uuid_v7(99));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_file_id, DataFileId(7));
        assert_eq!(kv.table_scan_count(), 1);
        assert_eq!(
            kv.catalog_scan_count(),
            0,
            "table-scoped inline deletion loads must not scan the catalog-wide family"
        );
    }

    #[test]
    fn given_visible_file_set_when_listing_inline_deletions_then_only_file_prefixes_are_scanned() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let visible_file_id = DataFileId(7);
        let hidden_file_id = DataFileId(8);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![
                InlineFileDeletionRow::new(
                    table_id,
                    visible_file_id,
                    1,
                    CatalogOrderId::uuid_v7(0),
                ),
                InlineFileDeletionRow::new(table_id, hidden_file_id, 2, CatalogOrderId::uuid_v7(0)),
            ],
        )
        .unwrap();
        let kv = InlineDeletionScanRecordingKv::new(kv, catalog, table_id, visible_file_id);
        let file_ids = BTreeSet::from([visible_file_id]);

        let grouped = crate::inline_data::list_inline_file_deletions_for_data_files_at(
            &kv,
            catalog,
            table_id,
            CatalogOrderId::uuid_v7(99),
            &file_ids,
        )
        .unwrap();

        assert_eq!(grouped.len(), 1);
        assert_eq!(
            grouped.get(&visible_file_id).cloned().unwrap_or_default(),
            BTreeSet::from([1])
        );
        assert_eq!(kv.file_scan_count(), 1);
        assert_eq!(
            kv.table_scan_count(),
            0,
            "small visible-file lookups should not scan the table-wide inline deletion prefix"
        );
    }

    #[test]
    fn given_large_file_set_when_listing_inline_deletions_then_table_prefix_is_scanned_once() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let file_ids = (1..=9).map(DataFileId).collect::<BTreeSet<_>>();
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            file_ids
                .iter()
                .map(|file_id| {
                    InlineFileDeletionRow::new(
                        table_id,
                        *file_id,
                        file_id.0,
                        CatalogOrderId::uuid_v7(0),
                    )
                })
                .collect(),
        )
        .unwrap();
        let kv = InlineDeletionScanRecordingKv::new(kv, catalog, table_id, DataFileId(1));

        let grouped = crate::inline_data::list_inline_file_deletions_for_data_files_at(
            &kv,
            catalog,
            table_id,
            CatalogOrderId::uuid_v7(99),
            &file_ids,
        )
        .unwrap();

        assert_eq!(grouped.len(), file_ids.len());
        assert_eq!(kv.file_scan_count(), 0);
        assert_eq!(
            kv.table_scan_count(),
            1,
            "large visible-file lookups should fall back to one table-wide inline deletion scan"
        );
    }

    #[test]
    fn given_public_snapshot_after_two_appends_when_listing_files_then_both_files_are_returned() {
        let catalog = CatalogId(1);
        let table_id = TableId(11);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_columns(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(10),
                table_id,
                "main/orders/first.parquet",
                1000,
                1024,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(11),
                table_id,
                "main/orders/second.parquet",
                1000,
                1024,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();

        let payload = String::from_utf8(
            data_files_at_payload(
                &kv,
                catalog,
                ListDataFilesAtPayload {
                    snapshot_id: 3,
                    table_id,
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("file_count=2"), "{payload}");
        assert!(payload.contains("main/orders/first.parquet"), "{payload}");
        assert!(payload.contains("main/orders/second.parquet"), "{payload}");
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

    struct SnapshotTimestampScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        snapshot_timestamp_prefix: Vec<u8>,
        snapshot_timestamp_scans: Rc<RefCell<usize>>,
    }

    impl SnapshotTimestampScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                snapshot_timestamp_prefix: snapshot_timestamp_prefix(catalog),
                snapshot_timestamp_scans: Rc::new(RefCell::new(0)),
            }
        }

        fn snapshot_timestamp_scan_count(&self) -> usize {
            *self.snapshot_timestamp_scans.borrow()
        }
    }

    impl OrderedCatalogKv for SnapshotTimestampScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix == self.snapshot_timestamp_prefix.as_slice() {
                *self.snapshot_timestamp_scans.borrow_mut() += 1;
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
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    struct TableObjectScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        table_object_prefix: Vec<u8>,
        table_object_scans: Rc<RefCell<usize>>,
    }

    impl TableObjectScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                table_object_prefix: table_object_scan_prefix(catalog),
                table_object_scans: Rc::new(RefCell::new(0)),
            }
        }

        fn table_object_scan_count(&self) -> usize {
            *self.table_object_scans.borrow()
        }
    }

    impl OrderedCatalogKv for TableObjectScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix == self.table_object_prefix.as_slice() {
                *self.table_object_scans.borrow_mut() += 1;
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
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    struct InlineDeletionScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        catalog_inline_deletion_prefix: Vec<u8>,
        table_inline_deletion_prefix: Vec<u8>,
        file_inline_deletion_prefix: Vec<u8>,
        catalog_scans: Rc<RefCell<usize>>,
        table_scans: Rc<RefCell<usize>>,
        file_scans: Rc<RefCell<usize>>,
    }

    impl InlineDeletionScanRecordingKv {
        fn new(
            inner: FakeOrderedCatalogKv,
            catalog: CatalogId,
            table_id: TableId,
            data_file_id: DataFileId,
        ) -> Self {
            Self {
                inner,
                catalog_inline_deletion_prefix: family_prefix(
                    catalog,
                    crate::keys::KeyFamily::InlineFileDeletion,
                ),
                table_inline_deletion_prefix: inline_file_deletion_table_prefix(catalog, table_id),
                file_inline_deletion_prefix: inline_file_deletion_file_prefix(
                    catalog,
                    table_id,
                    data_file_id,
                ),
                catalog_scans: Rc::new(RefCell::new(0)),
                table_scans: Rc::new(RefCell::new(0)),
                file_scans: Rc::new(RefCell::new(0)),
            }
        }

        fn catalog_scan_count(&self) -> usize {
            *self.catalog_scans.borrow()
        }

        fn table_scan_count(&self) -> usize {
            *self.table_scans.borrow()
        }

        fn file_scan_count(&self) -> usize {
            *self.file_scans.borrow()
        }
    }

    struct InlineEndOrderScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        end_order_prefix: Vec<u8>,
        end_order_scans: Rc<RefCell<usize>>,
    }

    impl InlineEndOrderScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId, table_id: TableId) -> Self {
            let mut end_order_prefix = family_prefix(catalog, crate::keys::KeyFamily::EndOrder);
            end_order_prefix.extend_from_slice(&table_id.0.to_be_bytes());
            end_order_prefix.push(b'/');
            Self {
                inner,
                end_order_prefix,
                end_order_scans: Rc::new(RefCell::new(0)),
            }
        }

        fn end_order_scan_count(&self) -> usize {
            *self.end_order_scans.borrow()
        }
    }

    impl OrderedCatalogKv for InlineEndOrderScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix == self.end_order_prefix.as_slice() {
                *self.end_order_scans.borrow_mut() += 1;
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
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    impl OrderedCatalogKv for InlineDeletionScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix == self.catalog_inline_deletion_prefix.as_slice() {
                *self.catalog_scans.borrow_mut() += 1;
            }
            if prefix == self.table_inline_deletion_prefix.as_slice() {
                *self.table_scans.borrow_mut() += 1;
            }
            if prefix == self.file_inline_deletion_prefix.as_slice() {
                *self.file_scans.borrow_mut() += 1;
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
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
