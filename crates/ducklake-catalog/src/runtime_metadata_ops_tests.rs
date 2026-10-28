#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::{BTreeMap, BTreeSet},
    };

    use super::super::{
        GlobalColumnStats, InlineStatsMerge, append_data_file_mirror_inserts,
        append_delete_file_mirror_inserts, append_file_column_stats_mirror_inserts,
        append_file_partition_values_mirror_inserts, append_table_inline_stats,
        begin_snapshot_for_schema_version, bounded_delete_file_mirror_sql,
        collect_data_file_ids_from_payload, current_metadata_data_file_rows,
        current_metadata_file_column_stats, data_file_rows_payload, delete_file_rows_payload,
        file_stats::{bounded_partition_info_mirror_sql, recompute_table_column_stats},
        initialization_metadata_settings, json_number_or_null,
        list_current_data_files_for_data_file_ids, list_data_files_for_data_file_ids,
        list_delete_files_for_data_file_ids, list_delete_files_for_delete_file_ids,
        mirror_refresh::bounded_append_mirror_sql_for_catalog,
        optional_metadata_payload_bool, partition_value_request, payload_u64_values,
        render_mutation_mirror_sql_for_catalog, scheduled_cleanup_delta_sql,
        semantic_delete_begin_orders_for_rows, stats_snapshot_for_request,
    };
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, ColumnTypeChange, DataFileId, DataFileRow,
        DeleteFileId, DeleteFileRow, FakeOrderedCatalogKv, FileColumnStatsRow,
        FilePartitionValueRow, InlinedTableRow, KvBatch, MutableCatalogKv, PartitionKeyIndex,
        RangeDirection, RangeItem, SnapshotRow, TableColumnRow, TableId, TablePartitionRow,
        TableRow, TableVersionReplacement, commit_append_data_files,
        commit_change_table_column_types, commit_create_table_row, commit_register_delete_files,
        expire_data_file, expire_snapshots, file_stats::list_file_column_stats_for_data_file_ids,
        initialize_catalog_if_absent, latest_snapshot, list_data_files, list_file_column_stats,
        list_file_partition_values, list_snapshots, load_table_at,
        public_snapshot_sequence_for_order, register_file_column_stats,
        register_file_partition_value, store::stage_snapshot,
        typed_scalar::normalize_catalog_scalar_type,
    };

    fn cleanup_mirror_payload(ids: &[DataFileId]) -> Vec<u8> {
        ids.iter()
            .map(|id| format!("file\t{}\t4\n", id.0))
            .collect::<String>()
            .into_bytes()
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_contiguous_and_sparse_statistics_when_aggregating_then_reads_stay_within_requested_keys()
     {
        use crate::runtime_metadata_ops::file_stats_aggregate::fetch_aggregate_source_rows;
        let catalog = CatalogId(987_064);
        let files = [DataFileId(1), DataFileId(2), DataFileId(3)];
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        for file in &files[..2] {
            for column in [0, 1, 2, 3, 4, 100_000] {
                if *file == DataFileId(2) && column == 2 {
                    continue;
                }
                let row = FileColumnStatsRow::new(
                    *file,
                    TableId(7),
                    ColumnId(column),
                    0,
                    Some("2".into()),
                    Some("10".into()),
                );
                crate::file_stats::stage_file_column_stats(&mut batch, catalog, &row);
            }
        }
        inner.commit(batch).unwrap();
        let kv = BatchGetCountingKv::new(inner);
        let contiguous = BTreeSet::from([ColumnId(1), ColumnId(2), ColumnId(3)]);
        let rows = fetch_aggregate_source_rows(&kv, catalog, &files, &contiguous).unwrap();
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|row| contiguous.contains(&row.column_id)));
        assert_eq!(kv.batch_get_key_count(), 0);
        assert_eq!(kv.range_read_count.get(), 3);
        assert_eq!(kv.range_row_count.get(), 5);
        let sparse = BTreeSet::from([ColumnId(1), ColumnId(3)]);
        let rows = fetch_aggregate_source_rows(&kv, catalog, &files, &sparse).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(kv.batch_get_key_count(), 6);
        assert_eq!(kv.range_read_count.get(), 3);
        assert_eq!(kv.range_row_count.get(), 5);
        let rows =
            fetch_aggregate_source_rows(&kv, catalog, &files, &BTreeSet::from([ColumnId(1)]))
                .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(kv.batch_get_key_count(), 9);
        let mut inner = kv.inner;
        for (file, column) in [(2, 2), (1, 100_000), (1, 3)] {
            let mut batch = KvBatch::new();
            let wrong = FileColumnStatsRow::new(
                DataFileId(file),
                TableId(7),
                ColumnId(column),
                0,
                None,
                None,
            );
            batch.put(
                crate::keys::file_column_stats_key(catalog, DataFileId(1), ColumnId(2)),
                wrong.encode(),
            );
            inner.commit(batch).unwrap();
            assert!(fetch_aggregate_source_rows(&inner, catalog, &files, &contiguous).is_err());
        }
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_scoped_stats_inputs_when_table_is_missing_or_misidentified_then_results_preserve_identity()
     {
        use crate::runtime_metadata_ops::file_stats_aggregate::StatsTableInputs;
        let catalog = CatalogId(987_062);
        let id = TableId(7);
        let table = TableRow::new(id, "stats", CatalogOrderId::uuid_v7(1));
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        crate::table_store::stage_current_table_row(&mut batch, catalog, &table);
        batch.put(
            crate::keys::table_next_row_id_key(catalog, id),
            42_u64.to_be_bytes().to_vec(),
        );
        kv.commit(batch).unwrap();
        let ids = std::collections::BTreeSet::from([id, TableId(8)]);
        let inputs =
            StatsTableInputs::fetch(&kv, catalog, table.validity.begin_order, &ids).unwrap();
        assert_eq!(inputs.tables, vec![table.clone()]);
        assert_eq!(inputs.stamps[&id].allocated_next_row_id, 42);
        let mut wrong_table = table;
        wrong_table.table_id = TableId(9);
        let mut batch = KvBatch::new();
        batch.put(
            crate::keys::current_table_row_key(catalog, id),
            wrong_table.encode(),
        );
        kv.commit(batch).unwrap();
        assert!(matches!(
            StatsTableInputs::fetch(&kv, catalog, wrong_table.validity.begin_order, &ids),
            Err(crate::CatalogError::Decode(_))
        ));
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_versioned_file_stats_when_sources_change_then_aggregate_tracks_version_membership_and_type()
     {
        use crate::runtime_metadata_ops::file_stats_aggregate::{
            FileStatsAggregateInput, fetch_file_stats_aggregate, fetch_file_stats_read_stamps,
        };
        let catalog = CatalogId(987_061);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut table = TableRow::new(TableId(7), "aggregate", CatalogOrderId::uuid_v7(1));
        table.columns.push(TableColumnRow::new(
            ColumnId(1),
            "value",
            "INTEGER",
            false,
            None,
        ));
        let files = [
            GlobalStatsFileRow {
                data_file_id: DataFileId(1),
                record_count: 2,
                file_size_bytes: 10,
                row_id_start: Some(0),
                has_deletions: false,
            },
            GlobalStatsFileRow {
                data_file_id: DataFileId(2),
                record_count: 2,
                file_size_bytes: 10,
                row_id_start: Some(2),
                has_deletions: false,
            },
        ];
        let render = |kv: &FakeOrderedCatalogKv, table: &TableRow, files: &[GlobalStatsFileRow]| {
            let stamp = fetch_file_stats_read_stamps(kv, catalog, std::slice::from_ref(table))
                .unwrap()
                .remove(&table.table_id)
                .unwrap();
            let stats = fetch_file_stats_aggregate(
                kv,
                catalog,
                FileStatsAggregateInput {
                    table,
                    files,
                    stamp,
                },
            )
            .unwrap();
            let mut out = String::new();
            stats.append_to(&mut out).unwrap();
            out
        };
        let mut batch = KvBatch::new();
        for (file, min, max) in [(1, "2", "10"), (2, "-3", "9")] {
            let row = FileColumnStatsRow::new(
                DataFileId(file),
                table.table_id,
                ColumnId(1),
                0,
                Some(min.to_owned()),
                Some(max.to_owned()),
            )
            .with_value_count(Some(2));
            crate::file_stats::stage_file_column_stats(&mut batch, catalog, &row);
        }
        batch.put(
            crate::keys::table_file_stats_version_key(catalog, table.table_id),
            vec![1],
        );
        kv.commit(batch).unwrap();
        let first = render(&kv, &table, &files);
        assert_eq!(
            first,
            "global_table_stats\t7\t4\t4\t20\nglobal_table_column_stats\t7\t1\ttrue\tfalse\ttrue\t-3\ttrue\t10\tfalse\t\n"
        );
        assert_eq!(render(&kv, &table, &files), first);
        let mut allocation = KvBatch::new();
        crate::conflict_watermarks::stage_table_next_row_id(
            &kv,
            &mut allocation,
            catalog,
            table.table_id,
            100,
        )
        .unwrap();
        kv.commit(allocation).unwrap();
        table.name = "renamed-with-same-stats".to_owned();
        let allocated = render(&kv, &table, &files);
        assert_eq!(allocated, first.replace("\t4\t4\t20", "\t4\t100\t20"));
        let mut unknown_files = files;
        unknown_files[0].row_id_start = None;
        let unknown = render(&kv, &table, &unknown_files);
        // A known file after an unknown one cannot undo the allocator offset.
        assert!(unknown.starts_with("global_table_stats\t7\t4\t102\t20\n"));
        let mut allocation = KvBatch::new();
        crate::conflict_watermarks::stage_table_next_row_id(
            &kv,
            &mut allocation,
            catalog,
            table.table_id,
            u64::MAX,
        )
        .unwrap();
        kv.commit(allocation).unwrap();
        assert!(
            render(&kv, &table, &unknown_files)
                .starts_with(&format!("global_table_stats\t7\t4\t{}\t20\n", u64::MAX))
        );

        table.columns[0].column_type = "VARCHAR".to_owned();
        assert!(
            render(&kv, &table, &files).contains(
                "global_table_column_stats\t7\t1\ttrue\tfalse\tfalse\t\tfalse\t\tfalse\t\n"
            )
        );
        table.columns[0].column_type = "INTEGER".to_owned();
        assert!(render(&kv, &table, &files[..1]).contains("\ttrue\t2\ttrue\t10\tfalse"));
        let mut batch = KvBatch::new();
        let replacement = FileColumnStatsRow::new(
            DataFileId(2),
            table.table_id,
            ColumnId(1),
            0,
            Some("20".to_owned()),
            Some("30".to_owned()),
        )
        .with_value_count(Some(2));
        crate::file_stats::stage_file_column_stats(&mut batch, catalog, &replacement);
        batch.put(
            crate::keys::table_file_stats_version_key(catalog, table.table_id),
            vec![2],
        );
        kv.commit(batch).unwrap();
        assert!(render(&kv, &table, &files).contains("\ttrue\t2\ttrue\t30\tfalse"));
    }

    #[test]
    fn given_committed_schema_index_when_fetching_descriptor_then_the_mapping_can_be_reused() {
        let catalog = CatalogId(45);
        let mut kv = FakeOrderedCatalogKv::default();
        let mut batch = KvBatch::new();
        batch.put(
            crate::keys::schema_version_begin_snapshot_key(catalog, 3),
            7_u64.to_be_bytes().to_vec(),
        );
        kv.commit(batch).unwrap();

        let output =
            super::super::render_schema_begin_descriptor(&kv, catalog, TableId(10), 3).unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "begin_snapshot_id=7\nstable_schema_begin=true\n"
        );
    }

    #[test]
    fn given_file_statistics_columns_when_parsing_then_identifiers_are_validated() {
        use super::super::file_statistics_request::requested_file_statistics;
        let (files, columns) = requested_file_statistics(
            b"data_file_id\t2\ncolumn_id\t1\ncolumn_id\t1\ncolumn_id\t9\n",
        )
        .unwrap();
        assert_eq!(files, vec![DataFileId(2)]);
        assert_eq!(columns, BTreeSet::from([ColumnId(1), ColumnId(9)]));
        for invalid in [
            "column_id\tnope\n",
            "column_id\t1\textra\n",
            "unknown\t1\n",
            "data_file_id\n",
        ] {
            assert!(
                requested_file_statistics(invalid.as_bytes()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn given_decimal_global_bounds_when_comparing_then_order_is_numeric() {
        let mut stats = GlobalColumnStats::with_type(normalize_catalog_scalar_type("DECIMAL"));
        stats.merge_inline(InlineStatsMerge {
            has_contains_null: true,
            contains_null: false,
            has_non_null_values: true,
            min_value: Some("9.9"),
            max_value: Some("9.9"),
        });
        stats.merge_inline(InlineStatsMerge {
            has_contains_null: true,
            contains_null: false,
            has_non_null_values: true,
            min_value: Some("10.0"),
            max_value: Some("10.0"),
        });

        assert_eq!(stats.min_value, "9.9");
        assert_eq!(stats.max_value, "10.0");
    }

    #[test]
    fn given_malformed_or_nonfinite_global_bounds_when_merged_then_both_extrema_clear() {
        for (declaration, invalid) in [("DECIMAL", "not-a-number"), ("DOUBLE", "NaN")] {
            let mut stats =
                GlobalColumnStats::with_type(normalize_catalog_scalar_type(declaration));
            stats.merge_inline(InlineStatsMerge {
                has_contains_null: true,
                contains_null: false,
                has_non_null_values: true,
                min_value: Some("1"),
                max_value: Some("2"),
            });
            stats.merge_inline(InlineStatsMerge {
                has_contains_null: true,
                contains_null: false,
                has_non_null_values: true,
                min_value: Some(invalid),
                max_value: Some("2"),
            });

            assert!(!stats.has_min, "{declaration}");
            assert!(!stats.has_max, "{declaration}");
            assert!(stats.min_value.is_empty(), "{declaration}");
            assert!(stats.max_value.is_empty(), "{declaration}");
        }
    }

    #[test]
    fn given_inverted_inline_global_bounds_when_merged_then_both_extrema_clear() {
        let mut stats = GlobalColumnStats::with_type(normalize_catalog_scalar_type("DECIMAL"));
        stats.merge_inline(InlineStatsMerge {
            has_contains_null: true,
            contains_null: false,
            has_non_null_values: true,
            min_value: Some("10.0"),
            max_value: Some("9.9"),
        });

        assert!(!stats.has_min);
        assert!(!stats.has_max);
        assert!(stats.min_value.is_empty());
        assert!(stats.max_value.is_empty());
    }

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_inline_and_file_decimal_stats_when_merged_then_global_extrema_are_exact() {
        let table = TableRow::with_catalog_metadata(
            TableId(8),
            crate::SchemaId(0),
            "decimal-uuid",
            "decimal",
            "main/decimal",
            vec![TableColumnRow::new(
                ColumnId(1),
                "value",
                "DECIMAL(38, 17)",
                false,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        let mut stats = GlobalTableStats::new(&table, 0);
        stats.accumulate_file(&GlobalStatsFileRow {
            data_file_id: DataFileId(1),
            record_count: 1,
            file_size_bytes: 10,
            row_id_start: Some(0),
            has_deletions: false,
        });
        stats.accumulate_file_column_stats(
            FileColumnStatsRow::new(
                DataFileId(1),
                TableId(8),
                ColumnId(1),
                0,
                Some("10.0".to_owned()),
                Some("10.0".to_owned()),
            )
            .with_value_count(Some(1)),
        );
        stats
            .accumulate_inline_payload(
                b"inline_aggregate_stats\t1\ninline_aggregate_next_row_id\t1\ninline_aggregate_column_stats\t1\t1\ttrue\t9.9\ttrue\t9.9\n",
            )
            .unwrap();

        let mut output = String::new();
        stats.append_to(&mut output).unwrap();

        assert!(
            output.contains("global_table_column_stats\t8\t1\ttrue\tfalse\ttrue\t9.9\ttrue\t10.0"),
            "{output}"
        );
    }

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_old_inline_layout_when_new_column_default_is_missing_then_global_bounds_include_the_default()
     {
        let mut table = TableRow::new(TableId(8), "defaults", CatalogOrderId::uuid_v7(1));
        let mut column = TableColumnRow::new(ColumnId(2), "added", "BIGINT", true, None);
        column.initial_default = Some("9".to_owned());
        column.created_with_table = false;
        table.columns.push(column);
        let mut stats = GlobalTableStats::new(&table, 0);
        stats
            .accumulate_inline_payload(b"inline_aggregate_stats\t2\n")
            .unwrap();
        stats.accumulate_inline_payload(b"inline_aggregate_stats\t1\ninline_aggregate_column_stats\t2\t1\ttrue\t10\ttrue\t10\n").unwrap();
        let mut output = String::new();
        stats.append_to(&mut output).unwrap();
        assert!(
            output.contains("global_table_column_stats\t8\t2\ttrue\tfalse\ttrue\t9\ttrue\t10"),
            "{output}"
        );
    }

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_missing_inline_column_when_default_cannot_be_proven_then_bounds_stay_unknown() {
        for (column_type, is_original, parent) in [
            ("BOOLEAN", false, None),
            ("BIGINT", true, None),
            ("BIGINT", false, Some(ColumnId(1))),
        ] {
            let mut table = TableRow::new(TableId(8), "defaults", CatalogOrderId::uuid_v7(1));
            let mut column = TableColumnRow::new(ColumnId(2), "added", column_type, true, parent);
            column.created_with_table = is_original;
            column.initial_default = Some("9".to_owned());
            table.columns.push(column);
            let mut stats = GlobalTableStats::new(&table, 0);
            stats
                .accumulate_inline_payload(b"inline_aggregate_stats\t2\n")
                .unwrap();
            let mut output = String::new();
            stats.append_to(&mut output).unwrap();
            assert!(
                output.contains("global_table_column_stats\t8\t2\ttrue\t"),
                "{output}"
            );
            assert!(!output.contains("\ttrue\t9\t"), "{output}");
        }
    }

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_missing_file_column_with_nonnull_default_when_stats_merge_then_bounds_are_unknown() {
        let mut table = TableRow::new(TableId(8), "defaults", CatalogOrderId::uuid_v7(1));
        let mut column = TableColumnRow::new(ColumnId(2), "added", "BIGINT", true, None);
        column.created_with_table = false;
        column.initial_default = Some("9".to_owned());
        table.columns.push(column);
        let mut stats = GlobalTableStats::new(&table, 0);
        stats.accumulate_file(&GlobalStatsFileRow {
            data_file_id: DataFileId(1),
            record_count: 2,
            file_size_bytes: 100,
            row_id_start: Some(0),
            has_deletions: false,
        });
        stats.accumulate_inline_payload(b"inline_aggregate_stats\t1\ninline_aggregate_column_stats\t2\t1\ttrue\t10\ttrue\t10\n").unwrap();
        let mut output = String::new();
        stats.append_to(&mut output).unwrap();
        assert!(
            output.contains("global_table_column_stats\t8\t2\ttrue\ttrue\tfalse\t\tfalse\t"),
            "{output}"
        );
    }

    #[cfg(feature = "foundationdb")]
    use super::super::{GlobalStatsFileRow, GlobalTableStats, can_recompute_exact_inline_stats};

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_rewrite_snapshot_when_selecting_exact_inline_stats_then_requires_delete_free_files() {
        let delete_free = vec![GlobalStatsFileRow {
            data_file_id: DataFileId(1),
            record_count: 10,
            file_size_bytes: 100,
            row_id_start: Some(0),
            has_deletions: false,
        }];
        let with_delete = vec![GlobalStatsFileRow {
            has_deletions: true,
            ..delete_free[0]
        }];

        assert!(can_recompute_exact_inline_stats(true, &delete_free));
        assert!(!can_recompute_exact_inline_stats(false, &delete_free));
        assert!(!can_recompute_exact_inline_stats(true, &with_delete));
    }

    #[test]
    #[cfg(feature = "foundationdb")]
    fn given_compaction_removes_high_row_ids_when_rendering_stats_then_allocator_does_not_regress()
    {
        let table = TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(1));
        let mut stats = GlobalTableStats::new(&table, 21);
        stats.accumulate_file(&GlobalStatsFileRow {
            data_file_id: DataFileId(1),
            record_count: 15,
            file_size_bytes: 1_024,
            row_id_start: Some(2),
            has_deletions: false,
        });
        let mut output = String::new();

        stats.append_to(&mut output).unwrap();

        assert_eq!(output, "global_table_stats\t1\t15\t21\t1024\n");
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

        let descriptor = String::from_utf8(
            super::super::render_schema_begin_descriptor(&kv, catalog, table_id, 99).unwrap(),
        )
        .unwrap();
        assert!(
            descriptor.contains("stable_schema_begin=false"),
            "{descriptor}"
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

        stats.merge_inline(InlineStatsMerge {
            has_contains_null: true,
            contains_null: false,
            has_non_null_values: false,
            min_value: None,
            max_value: None,
        });

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
    fn given_partition_value_request_when_parsed_then_file_ids_and_keys_are_typed_and_deduplicated()
    {
        let (files, keys) = partition_value_request(
            b"data_file_id\t9\npartition_key_index\t1\ndata_file_id\t9\npartition_key_index\t1\npartition_key_index\t0\n",
        )
        .unwrap();

        assert_eq!(files, [DataFileId(9)].into_iter().collect());
        assert_eq!(
            keys,
            [PartitionKeyIndex(0), PartitionKeyIndex(1)]
                .into_iter()
                .collect()
        );
        assert!(partition_value_request(b"unknown\t1\n").is_err());
        assert!(partition_value_request(b"partition_key_index\t4294967296\n").is_err());
    }

    #[test]
    fn given_repeated_u64_fields_when_parsing_then_all_values_are_retained() {
        assert_eq!(
            payload_u64_values(b"table_id=7\ntable_id=11\ntable_id=13\n", "table_id").unwrap(),
            vec![7, 11, 13]
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
        let mut data_sql = String::new();
        append_data_file_mirror_inserts(
            &mut data_sql,
            &[data_file.clone()],
            &snapshots,
            &BTreeMap::from([(DataFileId(7), 17)]),
        );
        assert!(!data_sql.contains("SELECT partition_id"), "{data_sql}");
        assert!(
            data_sql.contains("unnest([42]), unnest([17]), "),
            "{data_sql}"
        );
        let mut unpartitioned_sql = String::new();
        append_data_file_mirror_inserts(
            &mut unpartitioned_sql,
            &[data_file],
            &snapshots,
            &BTreeMap::new(),
        );
        assert!(
            unpartitioned_sql.contains("unnest([42]), unnest([NULL]), "),
            "{unpartitioned_sql}"
        );
        assert!(data_sql.contains("'main/table/a''b.parquet'"), "{data_sql}");
        assert!(
            data_sql.contains("unnest([10]), unnest([11]), unnest([7]),"),
            "{data_sql}"
        );
        assert!(
            data_sql.contains("unnest([128]), unnest([42]),"),
            "{data_sql}"
        );
        assert!(
            data_sql.contains("unnest(['AQIDBA==']), unnest([99]),"),
            "{data_sql}"
        );
        assert!(
            data_sql.contains("unnest([99]), unnest([12]), unnest([NULL]);"),
            "{data_sql}"
        );

        let mut partition_sql = String::new();
        append_file_partition_values_mirror_inserts(
            &mut partition_sql,
            vec![
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
            ],
        );
        assert_eq!(
            partition_sql,
            "INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value SELECT unnest([7, 8]), \
             unnest([4, 4]), unnest([0, 1]), unnest(['north''west', NULL]);\n"
        );

        let mut stats_sql = String::new();
        append_file_column_stats_mirror_inserts(
            &mut stats_sql,
            vec![FileColumnStatsRow {
                data_file_id: DataFileId(7),
                table_id: TableId(4),
                column_id: ColumnId(2),
                value_count: None,
                null_count: 3,
                min_value: Some("a'b".to_owned()),
                max_value: None,
                extra_stats: Some("x'y".to_owned()),
            }],
        );
        assert!(
            stats_sql.contains(
                "SELECT unnest([7]), unnest([4]), unnest([2]), unnest([NULL]), unnest([NULL]), \
                 unnest([3]), unnest(['a''b']), unnest([NULL]), unnest([NULL]), unnest(['x''y']);"
            ),
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
        let mut delete_sql = String::new();
        append_delete_file_mirror_inserts(
            &mut delete_sql,
            &delete_rows,
            &super::super::semantic_delete_begin_orders_from_rows(&delete_rows),
            &snapshots,
        );
        assert!(!delete_sql.contains("SELECT table_id"), "{delete_sql}");
        assert_eq!(delete_sql.matches("LEFT JOIN").count(), 1);
        assert!(delete_sql.contains("'delete''a.parquet'"), "{delete_sql}");
        assert!(
            delete_sql.contains("unnest([10]), unnest([11]), unnest([7]),"),
            "{delete_sql}"
        );
        assert!(
            delete_sql
                .contains("unnest([0]), unnest(['BQYHCA==']), unnest([NULL]), unnest([NULL]))"),
            "{delete_sql}"
        );

        let cleanup_sql = scheduled_cleanup_delta_sql(
            &[DataFileId(6)],
            &[DeleteFileId(5)],
            &[crate::maintenance::ScheduledDataFileCleanupRow {
                data_file: DataFileRow::new(
                    DataFileId(6),
                    TableId(1),
                    "data'a.parquet",
                    1,
                    256,
                    begin_order,
                ),
                schedule_start_micros: 1_234_567,
                cleanup_kind:
                    crate::maintenance::ScheduledDataFileCleanupKind::CompactionReplacement,
            }],
            &[crate::maintenance::ScheduledDeleteFileCleanupRow {
                delete_file: delete_rows[0].clone(),
                table_id: TableId(1),
                schedule_start_micros: 2_345_678,
            }],
        );
        assert_eq!(
            cleanup_sql,
            "DELETE FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion WHERE \
             data_file_id IN (5, 6);\nINSERT INTO \
             {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion SELECT unnest([6, 5]), \
             unnest(['data''a.parquet', 'delete''a.parquet']), unnest([false, false]), \
             unnest([make_timestamptz(1234567), make_timestamptz(2345678)]);\n"
        );
    }

    #[test]
    fn bounded_partition_mirror_replaces_only_requested_tables() {
        let mut partitioned = TableRow::new(TableId(4), "partitioned", CatalogOrderId::uuid_v7(1));
        partitioned.partition = Some(TablePartitionRow::new(17, Vec::new()));

        let sql = bounded_partition_info_mirror_sql(&[TableId(4), TableId(9)], &[partitioned]);

        assert!(
            sql.starts_with(
                "DELETE FROM {METADATA_CATALOG}.ducklake_partition_info WHERE table_id IN (4, 9);"
            ),
            "{sql}"
        );
        assert!(
            sql.contains(
                "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info VALUES (17, 4, NULL, \
                 NULL);"
            ),
            "{sql}"
        );
        assert!(!sql.contains("VALUES (17, 9"), "{sql}");
        assert!(!sql.contains("DELETE FROM {METADATA_CATALOG}.ducklake_partition_info;"));
    }

    #[test]
    fn given_scheduled_cleanup_when_rendering_bounded_mirror_then_only_requested_history_is_read() {
        let catalog = CatalogId(1);
        let begin_order = CatalogOrderId::uuid_v7(1);
        let data_file = DataFileRow::new(
            DataFileId(6),
            TableId(1),
            "data.parquet",
            1,
            256,
            begin_order,
        );
        let delete_file = DeleteFileRow::new(
            DeleteFileId(5),
            data_file.data_file_id,
            "delete.parquet",
            1,
            128,
            begin_order,
        );
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            crate::keys::data_file_key(catalog, data_file.data_file_id),
            data_file.encode(),
        );
        batch.put(
            crate::keys::delete_file_key(catalog, delete_file.delete_file_id),
            delete_file.encode(),
        );
        batch.put(
            crate::keys::scheduled_data_file_cleanup_key(catalog, data_file.data_file_id),
            crate::maintenance::encode_scheduled_data_cleanup_value(
                crate::maintenance::ScheduledDataFileCleanupKind::CompactionReplacement,
                1_234_567,
            ),
        );
        batch.put(
            crate::keys::scheduled_delete_file_cleanup_key(catalog, delete_file.delete_file_id),
            crate::maintenance::encode_scheduled_delete_cleanup_value(TableId(1), 2_345_678),
        );
        batch.put(
            crate::keys::delete_file_timeline_key(
                catalog,
                data_file.data_file_id,
                begin_order,
                delete_file.delete_file_id,
            ),
            delete_file.encode(),
        );
        inner.commit(batch).unwrap();
        let kv = CleanupMirrorScanRecordingKv::new(inner, catalog);

        let payload = cleanup_mirror_payload(&[data_file.data_file_id]);
        let sql = String::from_utf8(
            render_mutation_mirror_sql_for_catalog(&kv, catalog, &payload).unwrap(),
        )
        .unwrap();

        assert!(sql.contains("'data.parquet'"), "{sql}");
        assert!(sql.contains("'delete.parquet'"), "{sql}");
        assert_eq!(kv.history_scans(), 0);
        assert_eq!(kv.batch_gets(), 2);
        assert_eq!(kv.direct_gets(), 0);
    }

    #[test]
    fn given_changed_file_when_rendering_cleanup_delta_then_pending_queue_is_not_scanned() {
        let catalog = CatalogId(1);
        let begin_order = CatalogOrderId::uuid_v7(1);
        let data_file = DataFileRow::new(
            DataFileId(6),
            TableId(1),
            "data.parquet",
            1,
            256,
            begin_order,
        );
        let delete_file = DeleteFileRow::new(
            DeleteFileId(5),
            data_file.data_file_id,
            "delete.parquet",
            1,
            128,
            begin_order,
        );
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            crate::keys::data_file_key(catalog, data_file.data_file_id),
            data_file.encode(),
        );
        batch.put(
            crate::keys::delete_file_key(catalog, delete_file.delete_file_id),
            delete_file.encode(),
        );
        batch.put(
            crate::keys::scheduled_data_file_cleanup_key(catalog, data_file.data_file_id),
            crate::maintenance::encode_scheduled_data_cleanup_value(
                crate::maintenance::ScheduledDataFileCleanupKind::CompactionReplacement,
                1_234_567,
            ),
        );
        batch.put(
            crate::keys::scheduled_delete_file_cleanup_key(catalog, delete_file.delete_file_id),
            crate::maintenance::encode_scheduled_delete_cleanup_value(TableId(1), 2_345_678),
        );
        batch.put(
            crate::keys::delete_file_timeline_key(
                catalog,
                data_file.data_file_id,
                begin_order,
                delete_file.delete_file_id,
            ),
            delete_file.encode(),
        );
        inner.commit(batch).unwrap();
        let kv = CleanupMirrorScanRecordingKv::new(inner, catalog);

        let payload = cleanup_mirror_payload(&[data_file.data_file_id]);
        let sql = String::from_utf8(
            render_mutation_mirror_sql_for_catalog(&kv, catalog, &payload).unwrap(),
        )
        .unwrap();

        assert!(sql.starts_with(
            "DELETE FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion WHERE \
             data_file_id IN (5, 6);"
        ));
        assert!(sql.contains("'data.parquet'"), "{sql}");
        assert!(sql.contains("'delete.parquet'"), "{sql}");
        assert_eq!(kv.scheduled_cleanup_scans(), 0);
        assert_eq!(kv.batch_gets(), 2);
        assert_eq!(kv.direct_gets(), 0);
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
    fn given_maintained_schema_begin_when_lookup_repeats_then_snapshot_history_is_not_scanned() {
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
        let expected = latest_snapshot(&kv, catalog).unwrap().unwrap().sequence.0;
        let kv = CleanupMirrorScanRecordingKv::new(kv, catalog);

        for _ in 0..3 {
            assert_eq!(
                begin_snapshot_for_schema_version(&kv, catalog, table_id, 1)
                    .unwrap()
                    .0,
                expected
            );
        }

        assert_eq!(kv.history_scans(), 0);
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
            current_metadata_file_column_stats(&kv, catalog)
                .unwrap()
                .into_iter()
                .map(|row| row.data_file_id)
                .collect::<Vec<_>>(),
            vec![DataFileId(11)]
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
    #[ignore = "internal policy test: expired public snapshots are not retained only to resolve \
                inline schema markers"]
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
        register_file_column_stats(
            &mut kv,
            catalog,
            FileColumnStatsRow::new(DataFileId(2), table_id, ColumnId(2), 4, None, None),
        )
        .unwrap();
        let selected = crate::file_stats::list_file_column_stats_for_data_file_columns(
            &kv,
            catalog,
            &[DataFileId(2), DataFileId(2)],
            &BTreeSet::from([ColumnId(1), ColumnId(99)]),
        )
        .unwrap();
        assert_eq!(selected, rows);
        let unknown = crate::file_stats::list_file_column_stats_for_data_file_columns(
            &kv,
            catalog,
            &[DataFileId(2)],
            &BTreeSet::from([ColumnId(2)]),
        )
        .unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].null_count, 4);
        assert_eq!(unknown[0].min_value, None);
        assert_eq!(unknown[0].max_value, None);
    }

    #[test]
    fn given_expired_changed_file_when_listing_bounded_metadata_then_table_identity_is_retained() {
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(DataFileId(1), table_id, 0)],
        )
        .unwrap();
        expire_data_file(&mut kv, catalog, DataFileId(1), CatalogOrderId::uuid_v7(99)).unwrap();

        let rows = list_data_files_for_data_file_ids(&kv, catalog, &[DataFileId(1)]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table_id, table_id);
        assert!(rows[0].validity.end_order.is_some());
    }

    #[test]
    fn given_remapped_table_when_rendering_append_delta_then_partition_and_stats_use_committed_owner()
     {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        for table in 1..=100 {
            let mut row = TableRow::new(
                TableId(table),
                format!("table_{table}"),
                CatalogOrderId::uuid_v7(0),
            );
            if table == 7 {
                row.partition = Some(TablePartitionRow::new(17, Vec::new()));
            }
            row.columns.push(TableColumnRow::new(
                ColumnId(1),
                "value",
                "INTEGER",
                false,
                None,
            ));
            commit_create_table_row(&mut inner, catalog, row).unwrap();
            commit_append_data_files(
                &mut inner,
                catalog,
                vec![data_file(DataFileId(table), TableId(table), 0)],
            )
            .unwrap();
            register_file_column_stats(
                &mut inner,
                catalog,
                FileColumnStatsRow::new(
                    DataFileId(table),
                    TableId(table),
                    ColumnId(1),
                    0,
                    Some("1".to_owned()),
                    Some("9".to_owned()),
                )
                .with_value_count(Some(1)),
            )
            .unwrap();
        }
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![data_file(DataFileId(1007), TableId(7), 1)],
        )
        .unwrap();
        register_file_column_stats(
            &mut inner,
            catalog,
            FileColumnStatsRow::new(
                DataFileId(1007),
                TableId(7),
                ColumnId(1),
                0,
                Some("-7".to_owned()),
                Some("12".to_owned()),
            )
            .with_value_count(Some(1)),
        )
        .unwrap();
        let partition =
            FilePartitionValueRow::new(DataFileId(7), TableId(7), PartitionKeyIndex(0), "eu");
        register_file_partition_value(&mut inner, catalog, partition.clone()).unwrap();
        let mut partition = partition;
        partition.table_id = TableId(1); // The C++ plan retains the requested ID after commit remaps it.
        let mut kv = MirrorScanCountingKv::new(inner, catalog);
        let sql = String::from_utf8(
            bounded_append_mirror_sql_for_catalog(
                &kv,
                catalog,
                &[DataFileId(7), DataFileId(7)],
                vec![partition],
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(kv.broad_history_scans(), 0);
        assert!(!sql.contains("SELECT partition_id"), "{sql}");
        assert!(
            sql.contains(
                "unnest([0]), unnest([17]), unnest([NULL]), unnest([NULL]), unnest([NULL]), \
                 unnest([NULL])"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("SELECT unnest([7]), unnest([7]), unnest([0]), unnest(['eu'])"),
            "{sql}"
        );

        assert!(
            sql.contains("ducklake_table_column_stats WHERE table_id IN (7);"),
            "{sql}"
        );
        assert!(!sql.contains("stats.table_id IN (7)"), "{sql}");
        assert!(
            !sql.contains("DELETE FROM {METADATA_CATALOG}.ducklake_table_column_stats;\n"),
            "{sql}"
        );
        assert!(
            sql.contains(
                "INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats (table_id, column_id, \
                 contains_null, contains_nan, min_value, max_value, extra_stats) SELECT unnest([7]), \
                 unnest([1]), unnest([false]), unnest([NULL]), unnest(['-7']), unnest(['12']), unnest([NULL]);"
            ),
            "{sql}"
        );
        assert!(!sql.contains("min(stats.min_value)"), "{sql}");
        assert!(!sql.contains("max(stats.max_value)"), "{sql}");
        for id in [7, 1007] {
            expire_data_file(
                &mut kv.inner,
                catalog,
                DataFileId(id),
                CatalogOrderId::uuid_v7(999),
            )
            .unwrap();
        }
        let expired = String::from_utf8(
            bounded_append_mirror_sql_for_catalog(&kv, catalog, &[DataFileId(1007)], Vec::new())
                .unwrap(),
        )
        .unwrap();
        assert!(expired.contains("ducklake_table_column_stats WHERE table_id IN (7);"));
        assert!(!expired.contains("INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats"));
        assert_eq!(kv.broad_history_scans(), 0);
    }

    #[test]
    fn given_decimal_file_stats_when_recomputing_bounded_table_stats_then_extrema_are_typed() {
        let table = TableRow::with_catalog_metadata(
            TableId(7),
            crate::SchemaId(0),
            "decimal-uuid",
            "decimal",
            "main/decimal/",
            vec![TableColumnRow::new(
                ColumnId(1),
                "value",
                "DECIMAL(38, 17)",
                false,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        let files = vec![
            data_file(DataFileId(1), TableId(7), 0),
            data_file(DataFileId(2), TableId(7), 1),
        ];
        let file_stats = vec![
            FileColumnStatsRow::new(
                DataFileId(1),
                TableId(7),
                ColumnId(1),
                0,
                Some("9.9".to_owned()),
                Some("9.9".to_owned()),
            )
            .with_value_count(Some(1)),
            FileColumnStatsRow::new(
                DataFileId(2),
                TableId(7),
                ColumnId(1),
                0,
                Some("10.0".to_owned()),
                Some("10.0".to_owned()),
            )
            .with_value_count(Some(1)),
        ];

        let rows = recompute_table_column_stats(&[table], &files, &file_stats);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].min_value.as_deref(), Some("9.9"));
        assert_eq!(rows[0].max_value.as_deref(), Some("10.0"));
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
    fn given_combined_mirror_roles_when_rendering_then_each_requested_projection_is_replaced() {
        let kv = FakeOrderedCatalogKv::new();
        for roles in 1..=7 {
            let payload = format!("file\t7\t{roles}\nfile\t7\t{roles}\n");
            let sql = String::from_utf8(
                render_mutation_mirror_sql_for_catalog(&kv, CatalogId(1), payload.as_bytes())
                    .unwrap(),
            )
            .unwrap();
            for (role, table) in [
                (1, "ducklake_data_file"),
                (2, "ducklake_delete_file"),
                (4, "ducklake_files_scheduled_for_deletion"),
            ] {
                assert_eq!(
                    sql.contains(&format!("DELETE FROM {{METADATA_CATALOG}}.{table} WHERE")),
                    roles & role != 0,
                    "{sql}"
                );
            }
            assert!(!sql.contains("(7, 7)"), "{sql}");
        }
    }

    #[test]
    fn given_invalid_mirror_targets_when_rendering_then_request_is_rejected() {
        let kv = FakeOrderedCatalogKv::new();
        for payload in [
            "file\t7\t0\n",
            "file\t7\t8\n",
            "file\tnope\t1\n",
            "file\t7\n",
            "unknown\t7\n",
            "file\t7\t4\nfile_partition\t7\t1\t0\tvalue\n",
            "file\t7\t1\nfile_partition\t7\t1\t4294967296\tvalue\n",
        ] {
            assert!(
                render_mutation_mirror_sql_for_catalog(&kv, CatalogId(1), payload.as_bytes())
                    .is_err(),
                "{payload}"
            );
        }
        assert!(
            render_mutation_mirror_sql_for_catalog(&kv, CatalogId(1), b"")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn given_duplicate_mirror_targets_when_rendering_then_targets_are_typed_and_deduplicated() {
        let kv = FakeOrderedCatalogKv::new();
        let sql = String::from_utf8(
            render_mutation_mirror_sql_for_catalog(
                &kv,
                CatalogId(1),
                b"file\t2\t2\ndelete_file_id\t11\nfile\t1\t2\ndelete_file_id\t11\n",
            )
            .unwrap(),
        )
        .unwrap();
        assert!(
            sql.contains("WHERE delete_file_id IN (11) OR data_file_id IN (1, 2);"),
            "{sql}"
        );
        assert_eq!(sql.matches("DELETE FROM").count(), 1, "{sql}");
    }

    #[test]
    fn given_one_affected_data_file_when_listing_delete_mirror_delta_then_unrelated_histories_are_not_scanned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            (1..=100)
                .map(|id| data_file(DataFileId(id), table_id, id))
                .collect(),
        )
        .unwrap();
        commit_register_delete_files(
            &mut inner,
            catalog,
            (1..=100)
                .map(|id| delete_file(DeleteFileId(id + 100), DataFileId(id)))
                .collect(),
        )
        .unwrap();
        let kv = MirrorScanCountingKv::new(inner, catalog);

        let rows = list_delete_files_for_data_file_ids(&kv, catalog, &[DataFileId(42)]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].delete_file_id, DeleteFileId(142));
        assert_eq!(kv.timeline_scans(), 1);
        assert_eq!(kv.broad_history_scans(), 0);
    }

    #[test]
    fn given_stamped_timeline_key_when_listing_mirror_rows_then_commit_orders_are_resolved() {
        let catalog = CatalogId(1);
        let file_id = DataFileId(42);
        let committed = CatalogOrderId::fdb_versionstamp([1; 10], 0);
        let placeholder = crate::ids::incomplete_fdb_order();
        let row = DeleteFileRow::new(
            DeleteFileId(7),
            file_id,
            "delete.parquet",
            1,
            128,
            placeholder,
        )
        .with_max_partial_order(Some(placeholder));
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            crate::keys::delete_file_timeline_key(catalog, file_id, committed, row.delete_file_id),
            row.encode(),
        );
        kv.commit(batch).unwrap();

        let rows = list_delete_files_for_data_file_ids(&kv, catalog, &[file_id]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].validity.begin_order, committed);
        assert_eq!(rows[0].max_partial_order, Some(committed));
    }

    #[test]
    fn given_affected_data_file_when_rendering_delete_mirror_delta_then_all_siblings_are_replaced()
    {
        let rows = vec![
            delete_file(DeleteFileId(10), DataFileId(1)),
            delete_file(DeleteFileId(11), DataFileId(1)),
        ];
        let semantic_begin_orders = super::super::semantic_delete_begin_orders_from_rows(&rows);
        let snapshots = vec![SnapshotRow::new(
            CatalogOrderId::uuid_v7(2),
            crate::RawSnapshotSequence(3),
        )];

        let sql = bounded_delete_file_mirror_sql(
            &[DeleteFileId(10), DeleteFileId(11)],
            &[DataFileId(1)],
            &rows,
            &semantic_begin_orders,
            &snapshots,
        );

        assert!(
            sql.contains("WHERE delete_file_id IN (10, 11) OR data_file_id IN (1);"),
            "{sql}"
        );
        assert_eq!(sql.matches("INSERT INTO").count(), 1, "{sql}");
        assert!(sql.contains("unnest([10, 11])"), "{sql}");
        assert!(sql.contains("unnest([1, 1])"), "{sql}");
        assert!(sql.contains("'delete-10.parquet'"), "{sql}");
        assert!(sql.contains("'delete-11.parquet'"), "{sql}");
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
        range_read_count: Cell<usize>,
        range_row_count: Cell<usize>,
    }

    struct MirrorScanCountingKv {
        inner: FakeOrderedCatalogKv,
        delete_file_family_prefix: Vec<u8>,
        delete_file_timeline_family_prefix: Vec<u8>,
        broad_file_prefixes: [Vec<u8>; 2],
        timeline_scans: Cell<usize>,
        broad_history_scans: Cell<usize>,
    }

    impl MirrorScanCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                delete_file_family_prefix: crate::keys::family_prefix(
                    catalog,
                    crate::keys::KeyFamily::DeleteFile,
                ),
                delete_file_timeline_family_prefix: crate::keys::family_prefix(
                    catalog,
                    crate::keys::KeyFamily::DeleteFileTimeline,
                ),
                broad_file_prefixes: [
                    crate::keys::KeyFamily::DataFile,
                    crate::keys::KeyFamily::FileColumnStats,
                ]
                .map(|family| crate::keys::family_prefix(catalog, family)),
                timeline_scans: Cell::new(0),
                broad_history_scans: Cell::new(0),
            }
        }

        fn timeline_scans(&self) -> usize {
            self.timeline_scans.get()
        }

        fn broad_history_scans(&self) -> usize {
            self.broad_history_scans.get()
        }
    }

    impl crate::OrderedCatalogKv for MirrorScanCountingKv {
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
            if prefix == self.delete_file_family_prefix
                || prefix == self.delete_file_timeline_family_prefix
                || self
                    .broad_file_prefixes
                    .iter()
                    .any(|family| prefix == family)
            {
                self.broad_history_scans
                    .set(self.broad_history_scans.get().saturating_add(1));
            } else if prefix.starts_with(&self.delete_file_timeline_family_prefix) {
                self.timeline_scans
                    .set(self.timeline_scans.get().saturating_add(1));
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

    impl BatchGetCountingKv {
        fn new(inner: FakeOrderedCatalogKv) -> Self {
            Self {
                inner,
                batch_get_key_count: Cell::new(0),
                range_read_count: Cell::new(0),
                range_row_count: Cell::new(0),
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
            self.range_read_count.set(self.range_read_count.get() + 1);
            let rows =
                crate::OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)?;
            self.range_row_count
                .set(self.range_row_count.get() + rows.len());
            Ok(rows)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    struct CleanupMirrorScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        history_prefixes: Vec<Vec<u8>>,
        scheduled_cleanup_prefix: Vec<u8>,
        history_scans: Cell<usize>,
        scheduled_cleanup_scans: Cell<usize>,
        direct_gets: Cell<usize>,
        batch_gets: Cell<usize>,
    }

    impl CleanupMirrorScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                history_prefixes: [
                    crate::keys::KeyFamily::DeleteFile,
                    crate::keys::KeyFamily::Snapshot,
                    crate::keys::KeyFamily::SnapshotByTimestamp,
                ]
                .map(|family| crate::keys::family_prefix(catalog, family))
                .to_vec(),
                scheduled_cleanup_prefix: crate::keys::family_prefix(
                    catalog,
                    crate::keys::KeyFamily::ScheduledCleanup,
                ),
                history_scans: Cell::new(0),
                scheduled_cleanup_scans: Cell::new(0),
                direct_gets: Cell::new(0),
                batch_gets: Cell::new(0),
            }
        }

        fn history_scans(&self) -> usize {
            self.history_scans.get()
        }

        fn scheduled_cleanup_scans(&self) -> usize {
            self.scheduled_cleanup_scans.get()
        }

        fn direct_gets(&self) -> usize {
            self.direct_gets.get()
        }

        fn batch_gets(&self) -> usize {
            self.batch_gets.get()
        }
    }

    impl crate::OrderedCatalogKv for CleanupMirrorScanRecordingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            self.direct_gets
                .set(self.direct_gets.get().saturating_add(1));
            crate::OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            self.batch_gets.set(self.batch_gets.get().saturating_add(1));
            crate::OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if self
                .history_prefixes
                .iter()
                .any(|history_prefix| history_prefix == prefix)
            {
                self.history_scans
                    .set(self.history_scans.get().saturating_add(1));
            }
            if prefix.starts_with(&self.scheduled_cleanup_prefix) {
                self.scheduled_cleanup_scans
                    .set(self.scheduled_cleanup_scans.get().saturating_add(1));
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
