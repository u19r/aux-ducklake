#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeSet};

    use crate::{
        CatalogId, CatalogOrderId, ColumnId, ColumnTypeChange, DataFileId, DataFileRow,
        DeleteFileId, DeleteFileRow, FakeOrderedCatalogKv, FileColumnStatsRow,
        FilePartitionValueRow, InlinedTableRow, KvBatch, MutableCatalogKv, PartitionKeyIndex,
        RangeDirection, RangeItem, SnapshotRow, TableColumnRow, TableId, TableRow,
        TableVersionReplacement, commit_append_data_files, commit_change_table_column_types,
        commit_create_table_row, commit_register_delete_files, expire_data_file, expire_snapshots,
        initialize_catalog_if_absent, latest_snapshot, list_data_files, list_file_column_stats,
        list_file_partition_values, list_snapshots, load_table_at,
        public_snapshot_sequence_for_order, register_file_column_stats,
        register_file_partition_value, store::stage_snapshot,
    };

    use super::super::{
        GlobalColumnStats, append_table_inline_stats, begin_snapshot_for_schema_version,
        collect_data_file_ids_from_payload, current_metadata_data_file_rows,
        current_metadata_file_column_stats, current_metadata_file_partition_values,
        data_file_mirror_sql, data_file_rows_payload, delete_file_mirror_sql,
        delete_file_rows_payload, file_column_stats_mirror_sql, file_partition_values_mirror_sql,
        initialization_metadata_settings, json_number_or_null,
        list_current_data_files_for_data_file_ids, list_delete_files_for_delete_file_ids,
        list_file_column_stats_for_data_file_ids, optional_metadata_payload_bool,
        scheduled_cleanup_mirror_sql, semantic_delete_begin_orders_for_rows,
        stats_snapshot_for_request,
    };

    #[cfg(feature = "foundationdb")]
    use super::super::{can_recompute_exact_inline_stats, global_stats_file_rows_from_payload};

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_rewrite_snapshot_when_selecting_exact_inline_stats_then_requires_delete_free_files() {
        let delete_free = global_stats_file_rows_from_payload(
            b"file\t1\t2\tmain/t.parquet\t10\t100\t0\t\t\t\t\t\t\t\t1\t\t\t\t\t\t\t\t\n",
        )
        .unwrap();
        let with_delete = global_stats_file_rows_from_payload(
            b"file\t1\t2\tmain/t.parquet\t10\t100\t0\t\t9\tmain/d.parquet\t1\t10\t2\t\t1\t\t\t\t\t\t\t\t\n",
        )
        .unwrap();

        assert!(can_recompute_exact_inline_stats(true, &delete_free));
        assert!(!can_recompute_exact_inline_stats(false, &delete_free));
        assert!(!can_recompute_exact_inline_stats(true, &with_delete));
    }

    #[test]
    fn given_inline_schema_version_when_lookup_begin_snapshot_then_returns_raw_ducklake_snapshot_id()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::new(table_id, "orders", CatalogOrderId::uuid_v7(0)),
        )
        .unwrap();

        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut registered = created.clone();
        registered
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_99", 99));
        kv.commit_table_replacements(
            catalog,
            latest.sequence,
            vec![TableVersionReplacement::new(table_id, created, registered)],
        )
        .unwrap();

        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let expected = crate::DuckLakeSnapshotId(inline_snapshot.sequence.0);

        assert_eq!(
            begin_snapshot_for_schema_version(&kv, catalog, table_id, 99).unwrap(),
            expected
        );
        assert_ne!(expected.0, 99);
    }

    #[test]
    fn given_inline_column_without_bounds_when_merging_global_stats_then_file_min_max_are_cleared_but_extra_stats_survive()
     {
        let mut stats = GlobalColumnStats {
            has_min: true,
            min_value: "POINT(0 0)".to_owned(),
            has_max: true,
            max_value: "POINT(0 0)".to_owned(),
            has_extra_stats: true,
            extra_stats: "{\"extent\":{\"x_min\":0}}".to_owned(),
            ..GlobalColumnStats::default()
        };

        stats.merge_inline(true, false, false, "", false, "");

        assert!(!stats.has_min);
        assert!(stats.min_value.is_empty());
        assert!(!stats.has_max);
        assert!(stats.max_value.is_empty());
        assert!(stats.has_extra_stats);
        assert_eq!(stats.extra_stats, "{\"extent\":{\"x_min\":0}}");
    }

    #[test]
    fn given_integral_geometry_bound_when_rendering_json_then_number_remains_real() {
        assert_eq!(json_number_or_null(Some(-2.0)), "-2.0");
    }

    #[test]
    fn given_optional_metadata_bool_payload_when_parsing_then_defaults_and_rejects_invalid_values()
    {
        assert!(optional_metadata_payload_bool(b"", "include_file_column_stats", true).unwrap());
        assert!(!optional_metadata_payload_bool(b"", "include_file_column_stats", false).unwrap());
        assert!(
            optional_metadata_payload_bool(
                b"include_file_column_stats=true\n",
                "include_file_column_stats",
                false
            )
            .unwrap()
        );
        assert!(
            !optional_metadata_payload_bool(
                b"include_file_column_stats=false\n",
                "include_file_column_stats",
                true
            )
            .unwrap()
        );
        assert!(
            optional_metadata_payload_bool(
                b"include_file_column_stats=0\n",
                "include_file_column_stats",
                true
            )
            .is_err()
        );
    }

    #[test]
    fn given_future_stats_snapshot_when_resolving_then_latest_committed_snapshot_is_used() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        create_orders_table(&mut kv, catalog, table_id);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let resolved =
            stats_snapshot_for_request(&kv, catalog, latest.sequence.0.saturating_add(1)).unwrap();

        assert_eq!(resolved.order, latest.order);
        assert_eq!(resolved.sequence, latest.sequence);
    }

    #[test]
    fn given_exact_older_stats_snapshot_when_resolving_then_latest_committed_snapshot_is_used() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        create_orders_table(&mut kv, catalog, table_id);
        let first = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "main/orders/one.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(1),
            )],
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let resolved = stats_snapshot_for_request(&kv, catalog, first.sequence.0).unwrap();

        assert_eq!(resolved.order, latest.order);
        assert_eq!(resolved.sequence, latest.sequence);
    }

    #[test]
    fn given_lower_missing_stats_snapshot_when_resolving_then_latest_committed_snapshot_is_used() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(CatalogOrderId::uuid_v7(10), crate::RawSnapshotSequence(10)),
        );
        kv.commit(batch).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let resolved = stats_snapshot_for_request(&kv, catalog, 5).unwrap();

        assert_eq!(resolved.order, latest.order);
        assert_eq!(resolved.sequence, latest.sequence);
    }

    #[test]
    fn given_mirror_rows_when_rendering_sql_then_escapes_strings_and_preserves_nulls() {
        let begin_order = CatalogOrderId::uuid_v7(1);
        let end_order = CatalogOrderId::uuid_v7(2);
        let partial_order = CatalogOrderId::uuid_v7(3);
        let snapshots = vec![
            SnapshotRow::new(begin_order, crate::RawSnapshotSequence(10)),
            SnapshotRow::new(end_order, crate::RawSnapshotSequence(11)),
            SnapshotRow::new(partial_order, crate::RawSnapshotSequence(12)),
        ];

        let mut data_file = DataFileRow::new(
            DataFileId(7),
            TableId(4),
            "main/table/a'b.parquet",
            12,
            4096,
            begin_order,
        )
        .with_encryption_key("AQIDBA==");
        data_file.validity.end_order = Some(end_order);
        data_file.max_partial_order = Some(partial_order);
        data_file.footer_size = Some(128);
        data_file.mapping_id = Some(99);
        data_file.row_id_start = 42;
        data_file.row_id_start_known = true;
        let data_sql = data_file_mirror_sql(&[data_file], &snapshots);
        assert!(data_sql.contains("'main/table/a''b.parquet'"), "{data_sql}");
        assert!(data_sql.contains(", 10, 11, 7,"), "{data_sql}");
        assert!(data_sql.contains(", 128, 42,"), "{data_sql}");
        assert!(data_sql.contains(", 'AQIDBA==', 99,"), "{data_sql}");
        assert!(data_sql.contains(", 99, 12);"), "{data_sql}");

        let partition_sql = file_partition_values_mirror_sql(vec![
            FilePartitionValueRow::new(
                DataFileId(7),
                TableId(4),
                PartitionKeyIndex(0),
                "north'west",
            ),
            FilePartitionValueRow::new(
                DataFileId(8),
                TableId(4),
                PartitionKeyIndex(1),
                "__HIVE_DEFAULT_PARTITION__",
            ),
        ]);
        assert!(partition_sql.contains("'north''west'"), "{partition_sql}");
        assert!(
            partition_sql.contains("VALUES (8, 4, 1, NULL);"),
            "{partition_sql}"
        );

        let stats_sql = file_column_stats_mirror_sql(vec![FileColumnStatsRow {
            data_file_id: DataFileId(7),
            table_id: TableId(4),
            column_id: ColumnId(2),
            value_count: None,
            null_count: 3,
            min_value: Some("a'b".to_owned()),
            max_value: None,
            extra_stats: Some("x'y".to_owned()),
        }]);
        assert!(
            stats_sql.contains("VALUES (7, 4, 2, NULL, NULL, 3, 'a''b', NULL, NULL, 'x''y');"),
            "{stats_sql}"
        );

        let mut delete_file = DeleteFileRow::new(
            DeleteFileId(5),
            DataFileId(7),
            "delete'a.parquet",
            2,
            512,
            begin_order,
        )
        .with_encryption_key("BQYHCA==");
        delete_file.validity.end_order = Some(end_order);
        let delete_rows = vec![delete_file];
        let delete_sql = delete_file_mirror_sql(&delete_rows, &snapshots);
        assert!(delete_sql.contains("'delete''a.parquet'"), "{delete_sql}");
        assert!(delete_sql.contains(", 10, 11, 7,"), "{delete_sql}");
        assert!(
            delete_sql.contains(", 0, 'BQYHCA==', NULL);"),
            "{delete_sql}"
        );

        let cleanup_sql = scheduled_cleanup_mirror_sql(&delete_rows, &snapshots);
        assert!(
            cleanup_sql.contains("VALUES (5, 'delete''a.parquet', false, NOW());"),
            "{cleanup_sql}"
        );
    }

    #[test]
    fn given_encrypted_initialization_when_parsed_then_required_global_settings_are_preserved() {
        let rows = initialization_metadata_settings(
            b"version=1.0\ncreated_by=DuckDB test\ndata_path=/lake/data/\nencrypted=true\n",
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                crate::MetadataSettingRow::global("version", "1.0"),
                crate::MetadataSettingRow::global("created_by", "DuckDB test"),
                crate::MetadataSettingRow::global("data_path", "/lake/data/"),
                crate::MetadataSettingRow::global("encrypted", "true"),
            ]
        );
    }

    #[test]
    fn given_table_schema_version_when_lookup_begin_snapshot_then_returns_schema_change_snapshot() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "order_id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_change_table_column_types(
            &mut kv,
            catalog,
            &[ColumnTypeChange::new(
                table_id,
                TableColumnRow::new(ColumnId(1), "order_id", "BIGINT", false, None),
            )],
        )
        .unwrap();

        let changed = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let expected = public_snapshot_sequence_for_order(&kv, catalog, changed.order)
            .unwrap()
            .unwrap();

        assert_eq!(
            begin_snapshot_for_schema_version(&kv, catalog, table_id, 2).unwrap(),
            expected
        );
    }

    #[test]
    fn given_schema_begin_snapshot_expired_when_lookup_begin_snapshot_then_returns_raw_schema_boundary()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "order_id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "main/orders/one.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let first_data_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(2),
                table_id,
                "main/orders/two.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        latest_snapshot(&kv, catalog).unwrap().unwrap();
        expire_snapshots(
            &mut kv,
            catalog,
            &[create_snapshot.sequence, first_data_snapshot.sequence],
        )
        .unwrap();

        assert_eq!(
            begin_snapshot_for_schema_version(&kv, catalog, table_id, 1).unwrap(),
            crate::DuckLakeSnapshotId(create_snapshot.sequence.0)
        );
    }

    #[test]
    fn given_expired_file_metadata_is_retained_when_listing_current_mirror_rows_then_old_stats_and_partitions_are_filtered()
     {
        let catalog = CatalogId(1);
        let table = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(10), table, 0),
                data_file(DataFileId(11), table, 1),
            ],
        )
        .unwrap();
        for file_id in [DataFileId(10), DataFileId(11)] {
            register_file_partition_value(
                &mut kv,
                catalog,
                FilePartitionValueRow::new(
                    file_id,
                    table,
                    PartitionKeyIndex(0),
                    file_id.0.to_string(),
                ),
            )
            .unwrap();
            register_file_column_stats(
                &mut kv,
                catalog,
                FileColumnStatsRow::new(
                    file_id,
                    table,
                    ColumnId(1),
                    0,
                    Some(file_id.0.to_string()),
                    Some(file_id.0.to_string()),
                ),
            )
            .unwrap();
        }
        expire_data_file(
            &mut kv,
            catalog,
            DataFileId(10),
            CatalogOrderId::uuid_v7(99),
        )
        .unwrap();

        assert_eq!(list_file_partition_values(&kv, catalog).unwrap().len(), 2);
        assert_eq!(list_file_column_stats(&kv, catalog).unwrap().len(), 2);

        assert_eq!(
            current_metadata_file_partition_values(&kv, catalog)
                .unwrap()
                .into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(11)]
        );
        assert_eq!(
            current_metadata_file_column_stats(&kv, catalog)
                .unwrap()
                .into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(11)]
        );

        let kv = PartitionValueScanRecordingKv::new(kv, catalog);
        assert_eq!(
            current_metadata_file_partition_values(&kv, catalog)
                .unwrap()
                .into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(11)]
        );
        assert_eq!(
            kv.broad_file_partition_value_scans(),
            0,
            "current metadata partition mirror rows should not scan the full FilePartitionValue family"
        );
    }

    #[test]
    fn given_file_payload_when_collecting_global_stats_inputs_then_data_file_ids_are_deduplicated()
    {
        let payload = [
            "global_stats_input_snapshot=7",
            "file\t10\t1\t1\t\tmain/a.parquet\tfalse\tparquet\t10\t128\t0\t0\t\t\t\t",
            "file\t11\t1\t2\t\tmain/b.parquet\tfalse\tparquet\t10\t128\t0\t10\t\t\t\t",
            "file_column_stats\t10\t1\t1\t\t10\t0\t1\t9\t\t",
            "file\t10\t1\t3\t\tmain/a.parquet\tfalse\tparquet\t10\t128\t0\t0\t\t\t\t",
        ]
        .join("\n");
        let mut ids = BTreeSet::new();

        collect_data_file_ids_from_payload(payload.as_bytes(), &mut ids).unwrap();

        assert_eq!(
            ids.into_iter().collect::<Vec<_>>(),
            vec![DataFileId(10), DataFileId(11)]
        );
    }

    #[test]
    fn given_inline_stats_payload_when_appending_global_inputs_then_rows_are_tagged_with_table_id()
    {
        let mut out = String::new();
        let payload =
            b"inline_table_stats\t2\t5\ninline_column_stats\t1\ttrue\tfalse\ttrue\t10\ttrue\t20\n";

        append_table_inline_stats(&mut out, TableId(42), payload).unwrap();

        assert_eq!(
            out,
            "table_inline_stats\t42\t2\t5\ntable_inline_column_stats\t42\t1\ttrue\tfalse\ttrue\t10\ttrue\t20\n"
        );
    }

    #[test]
    #[ignore = "internal policy test: expired public snapshots are not retained only to resolve inline schema markers"]
    fn given_inline_marker_matches_public_schema_version_when_lookup_begin_snapshot_then_schema_version_wins()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "order_id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_change_table_column_types(
            &mut kv,
            catalog,
            &[ColumnTypeChange::new(
                table_id,
                TableColumnRow::new(ColumnId(1), "order_id", "BIGINT", false, None),
            )],
        )
        .unwrap();
        let schema_change = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let expected = public_snapshot_sequence_for_order(&kv, catalog, schema_change.order)
            .unwrap()
            .unwrap();

        let mut table = load_table_at(&kv, catalog, table_id, schema_change.order)
            .unwrap()
            .unwrap();
        let previous = table.clone();
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_2", 2));
        kv.commit_table_replacements(
            catalog,
            schema_change.sequence,
            vec![TableVersionReplacement::new(table_id, previous, table)],
        )
        .unwrap();
        expire_snapshots(&mut kv, catalog, &[schema_change.sequence]).unwrap();

        assert_eq!(
            begin_snapshot_for_schema_version(&kv, catalog, table_id, 2).unwrap(),
            expected
        );
    }

    #[test]
    fn given_ended_file_loses_public_end_snapshot_when_listing_metadata_files_then_file_is_not_current()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "old-uuid",
                "example",
                "main/example/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "key",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "old.parquet",
                20,
                100,
                CatalogOrderId::uuid_v7(1),
            )],
        )
        .unwrap();
        let expired_order = kv.generated_order_id();
        expire_data_file(&mut kv, catalog, DataFileId(1), expired_order).unwrap();
        let mut batch = KvBatch::new();
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(expired_order, crate::RawSnapshotSequence(3)),
        );
        stage_snapshot(
            &mut batch,
            catalog,
            &SnapshotRow::new(kv.generated_order_id(), crate::RawSnapshotSequence(4)),
        );
        kv.commit(batch).unwrap();
        expire_snapshots(&mut kv, catalog, &[crate::RawSnapshotSequence(3)]).unwrap();

        let payload = data_file_rows_payload(
            &list_data_files(&kv, catalog).unwrap(),
            &list_snapshots(&kv, catalog).unwrap(),
        );
        let old_file = payload
            .lines()
            .find(|line| line.starts_with("data_file\t1\t1\told.parquet\t"))
            .unwrap();
        let fields = old_file.split('\t').collect::<Vec<_>>();
        assert_eq!(fields[9], "0", "{payload}");
    }

    #[test]
    fn given_compacted_sources_when_exporting_current_metadata_files_then_only_live_replacements_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "uuid",
                "example",
                "main/example/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "key",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table_id,
                    "source-a.parquet",
                    10,
                    100,
                    CatalogOrderId::uuid_v7(1),
                ),
                DataFileRow::new(
                    DataFileId(2),
                    table_id,
                    "source-b.parquet",
                    10,
                    100,
                    CatalogOrderId::uuid_v7(2),
                ),
            ],
        )
        .unwrap();

        let compaction_order = kv.generated_order_id();
        expire_data_file(&mut kv, catalog, DataFileId(1), compaction_order).unwrap();
        expire_data_file(&mut kv, catalog, DataFileId(2), compaction_order).unwrap();
        let mut replacement = DataFileRow::new(
            DataFileId(3),
            table_id,
            "replacement.parquet",
            20,
            180,
            CatalogOrderId::uuid_v7(3),
        );
        replacement.validity.begin_order = compaction_order;
        commit_append_data_files(&mut kv, catalog, vec![replacement]).unwrap();

        let all_rows = list_data_files(&kv, catalog).unwrap();
        assert_eq!(all_rows.len(), 3);
        let current_rows = current_metadata_data_file_rows(all_rows);
        assert_eq!(current_rows.len(), 1);
        assert_eq!(current_rows[0].data_file_id, DataFileId(3));

        let payload = data_file_rows_payload(&current_rows, &list_snapshots(&kv, catalog).unwrap());
        assert!(payload.contains("data_file_count=1\n"), "{payload}");
        assert!(!payload.contains("source-a.parquet"), "{payload}");
        assert!(!payload.contains("source-b.parquet"), "{payload}");
        assert!(payload.contains("replacement.parquet"), "{payload}");
    }

    #[test]
    fn given_changed_data_file_ids_when_listing_bounded_current_metadata_files_then_only_requested_live_files_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(1), table_id, 0),
                data_file(DataFileId(2), table_id, 1),
                data_file(DataFileId(3), table_id, 2),
            ],
        )
        .unwrap();
        expire_data_file(&mut kv, catalog, DataFileId(2), CatalogOrderId::uuid_v7(99)).unwrap();

        let rows = list_current_data_files_for_data_file_ids(
            &kv,
            catalog,
            &[DataFileId(1), DataFileId(2)],
        )
        .unwrap();

        assert_eq!(
            rows.into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(1)]
        );
    }

    #[test]
    fn given_duplicate_changed_data_file_ids_when_listing_bounded_current_metadata_files_then_each_file_is_read_once()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                data_file(DataFileId(1), table_id, 0),
                data_file(DataFileId(2), table_id, 1),
            ],
        )
        .unwrap();
        let kv = BatchGetCountingKv::new(inner);

        let rows = list_current_data_files_for_data_file_ids(
            &kv,
            catalog,
            &[DataFileId(2), DataFileId(1), DataFileId(2), DataFileId(1)],
        )
        .unwrap();

        assert_eq!(
            rows.into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(1), DataFileId(2)]
        );
        assert_eq!(kv.batch_get_key_count(), 2);
    }

    #[test]
    fn given_changed_data_file_ids_when_listing_bounded_file_column_stats_then_only_requested_file_stats_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(1), table_id, 0),
                data_file(DataFileId(2), table_id, 1),
            ],
        )
        .unwrap();
        for data_file_id in [DataFileId(1), DataFileId(2)] {
            register_file_column_stats(
                &mut kv,
                catalog,
                FileColumnStatsRow::new(
                    data_file_id,
                    table_id,
                    ColumnId(1),
                    0,
                    Some(data_file_id.0.to_string()),
                    Some(data_file_id.0.to_string()),
                ),
            )
            .unwrap();
        }

        let rows =
            list_file_column_stats_for_data_file_ids(&kv, catalog, &[DataFileId(2)]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_file_id, DataFileId(2));
    }

    #[test]
    fn given_changed_delete_file_ids_when_listing_bounded_delete_files_then_only_requested_delete_files_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                data_file(DataFileId(1), table_id, 0),
                data_file(DataFileId(2), table_id, 1),
            ],
        )
        .unwrap();
        commit_register_delete_files(
            &mut kv,
            catalog,
            vec![
                delete_file(DeleteFileId(10), DataFileId(1)),
                delete_file(DeleteFileId(11), DataFileId(2)),
            ],
        )
        .unwrap();

        let rows =
            list_delete_files_for_delete_file_ids(&kv, catalog, &[DeleteFileId(11)]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].delete_file_id, DeleteFileId(11));
        assert_eq!(rows[0].data_file_id, DataFileId(2));
    }

    #[test]
    fn given_duplicate_delete_file_ids_when_listing_bounded_delete_files_then_each_file_is_read_once()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(1), table_id, 0)],
        )
        .unwrap();
        commit_register_delete_files(
            &mut inner,
            catalog,
            vec![
                delete_file(DeleteFileId(10), DataFileId(1)),
                delete_file(DeleteFileId(11), DataFileId(1)),
            ],
        )
        .unwrap();
        let kv = BatchGetCountingKv::new(inner);

        let rows = list_delete_files_for_delete_file_ids(
            &kv,
            catalog,
            &[
                DeleteFileId(11),
                DeleteFileId(10),
                DeleteFileId(11),
                DeleteFileId(10),
            ],
        )
        .unwrap();

        assert_eq!(
            rows.into_iter()
                .map(|row| row.delete_file_id)
                .collect::<Vec<_>>(),
            vec![DeleteFileId(10), DeleteFileId(11)]
        );
        assert_eq!(kv.batch_get_key_count(), 2);
    }

    #[test]
    fn given_multiple_delete_files_for_one_data_file_when_rendering_delete_rows_then_begin_snapshot_is_semantic_begin()
     {
        let rows = vec![
            DeleteFileRow::new(
                DeleteFileId(10),
                DataFileId(1),
                "delete-10.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(10),
            ),
            DeleteFileRow::new(
                DeleteFileId(11),
                DataFileId(1),
                "delete-11.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(20),
            ),
        ];
        let snapshots = vec![
            SnapshotRow::new(CatalogOrderId::uuid_v7(10), crate::RawSnapshotSequence(3)),
            SnapshotRow::new(CatalogOrderId::uuid_v7(20), crate::RawSnapshotSequence(4)),
        ];

        let payload = delete_file_rows_payload(&rows, &snapshots);
        let delete_rows = payload
            .lines()
            .filter(|line| line.starts_with("delete_file\t"))
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();

        assert_eq!(delete_rows.len(), 2, "{payload}");
        assert_eq!(delete_rows[0][6], "3", "{payload}");
        assert_eq!(delete_rows[1][6], "3", "{payload}");
    }

    #[test]
    fn given_bounded_delete_file_subset_when_resolving_semantic_begin_then_sibling_timeline_is_used()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(DataFileId(1), table_id, 0)],
        )
        .unwrap();
        let first_delete = commit_register_delete_files(
            &mut kv,
            catalog,
            vec![delete_file(DeleteFileId(10), DataFileId(1))],
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let later_delete = commit_register_delete_files(
            &mut kv,
            catalog,
            vec![delete_file(DeleteFileId(11), DataFileId(1))],
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let begin_orders =
            semantic_delete_begin_orders_for_rows(&kv, catalog, &[later_delete]).unwrap();

        assert_eq!(
            begin_orders.get(&DataFileId(1)).copied(),
            Some(first_delete.validity.begin_order)
        );
    }

    #[test]
    fn given_file_with_physical_row_ids_when_listing_metadata_files_then_row_id_start_is_empty() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "uuid",
                "example",
                "main/example/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "key",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table_id,
                "rewrite.parquet",
                49,
                760,
                CatalogOrderId::uuid_v7(1),
            )],
        )
        .unwrap();

        let payload = data_file_rows_payload(
            &list_data_files(&kv, catalog).unwrap(),
            &list_snapshots(&kv, catalog).unwrap(),
        );
        let file = payload
            .lines()
            .find(|line| line.starts_with("data_file\t1\t1\trewrite.parquet\t"))
            .unwrap();
        let fields = file.split('\t').collect::<Vec<_>>();

        assert_eq!(fields[6], "", "{payload}");
    }

    fn data_file(data_file_id: DataFileId, table_id: TableId, row_id_start: u64) -> DataFileRow {
        DataFileRow::new(
            data_file_id,
            table_id,
            format!("file-{}.parquet", data_file_id.0),
            1,
            128,
            CatalogOrderId::uuid_v7(1),
        )
        .with_row_id_start(row_id_start)
    }

    fn create_orders_table(kv: &mut FakeOrderedCatalogKv, catalog: CatalogId, table_id: TableId) {
        commit_create_table_row(
            kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                crate::SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "order_id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
    }

    fn delete_file(delete_file_id: DeleteFileId, data_file_id: DataFileId) -> DeleteFileRow {
        DeleteFileRow::new(
            delete_file_id,
            data_file_id,
            format!("delete-{}.parquet", delete_file_id.0),
            1,
            128,
            CatalogOrderId::uuid_v7(2),
        )
    }

    struct BatchGetCountingKv {
        inner: FakeOrderedCatalogKv,
        batch_get_key_count: Cell<usize>,
    }

    impl BatchGetCountingKv {
        fn new(inner: FakeOrderedCatalogKv) -> Self {
            Self {
                inner,
                batch_get_key_count: Cell::new(0),
            }
        }

        fn batch_get_key_count(&self) -> usize {
            self.batch_get_key_count.get()
        }
    }

    impl crate::OrderedCatalogKv for BatchGetCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            self.batch_get_key_count
                .set(self.batch_get_key_count.get().saturating_add(keys.len()));
            crate::OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            crate::OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            crate::OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    struct PartitionValueScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        broad_file_partition_value_scans: Cell<usize>,
    }

    impl PartitionValueScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog,
                broad_file_partition_value_scans: Cell::new(0),
            }
        }

        fn broad_file_partition_value_scans(&self) -> usize {
            self.broad_file_partition_value_scans.get()
        }
    }

    impl crate::OrderedCatalogKv for PartitionValueScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            crate::OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if prefix
                == crate::keys::family_prefix(
                    self.catalog,
                    crate::keys::KeyFamily::FilePartitionValue,
                )
            {
                self.broad_file_partition_value_scans.set(
                    self.broad_file_partition_value_scans
                        .get()
                        .saturating_add(1),
                );
            }
            crate::OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            crate::OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
