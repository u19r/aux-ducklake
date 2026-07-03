#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeSet};

    use crate::{
        CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        FakeOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow,
        MergeAdjacentCompaction, OrderedCatalogKv, PartitionKeyIndex, RangeDirection, RangeItem,
        TableId, commit_append_data_files, commit_inline_file_deletions,
        commit_register_delete_files,
        keys::{
            KeyFamily, current_delete_file_key, data_file_key, delete_file_timeline_prefix,
            family_prefix, file_column_stats_data_file_prefix, inline_file_deletion_file_prefix,
            inline_file_deletion_table_prefix, latest_snapshot_row_key,
            order_delete_file_change_prefix,
        },
        latest_snapshot, register_file_column_stats, register_file_partition_value,
    };

    use super::super::{
        MergeSourceContext, RewriteSourceContext, derive_merge_replacement_stats,
        list_partition_values_for_source_file_ids, merge_stats_for_replacement,
        normalize_merge_replacements, normalize_rewrite_replacement_row_ids_from_sources,
        reject_rewrite_source_delete_conflicts_since_base, reject_source_delete_files,
        rewrite_source_deletions,
    };

    #[test]
    fn given_source_file_deleted_after_read_snapshot_when_rewrite_checks_conflicts_then_rejects() {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let source = DataFileId(4);
        let mut kv = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let appended = commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    source,
                    table,
                    "main/test/source.parquet",
                    90,
                    1098,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(10),
            ],
        )
        .unwrap();
        let read_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
        commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(7),
                source,
                "main/test/source-delete.parquet",
                30,
                1381,
                appended[0].validity.begin_order,
            )],
        )
        .unwrap();
        let through_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;

        let error = reject_rewrite_source_delete_conflicts_since_base(
            &kv,
            catalog,
            table,
            read_order,
            through_order,
            &[source],
        )
        .unwrap_err();

        assert!(matches!(error, crate::CatalogError::LogicalConflict { .. }));
    }

    #[test]
    fn given_unrelated_delete_changes_when_rewrite_checks_sources_then_uses_source_scoped_scans() {
        let catalog = CatalogId(93);
        let table = TableId(1);
        let source = DataFileId(4);
        let unrelated = DataFileId(5);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let appended = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    source,
                    table,
                    "main/test/source.parquet",
                    90,
                    1098,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(10),
                DataFileRow::new(
                    unrelated,
                    table,
                    "main/test/unrelated.parquet",
                    90,
                    1098,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(100),
            ],
        )
        .unwrap();
        let read_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(7),
                unrelated,
                "main/test/unrelated-delete.parquet",
                30,
                1381,
                appended[1].validity.begin_order,
            )],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut inner,
            catalog,
            vec![InlineFileDeletionRow::new(
                table,
                unrelated,
                101,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let through_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = SourceDeleteConflictScanCountingKv::new(inner, catalog, table, source);

        reject_rewrite_source_delete_conflicts_since_base(
            &kv,
            catalog,
            table,
            read_order,
            through_order,
            &[source],
        )
        .unwrap();

        assert_eq!(kv.table_delete_change_range_scans(), 0);
        assert_eq!(kv.table_inline_prefix_scans(), 0);
        assert_eq!(kv.source_delete_timeline_range_scans(), 1);
        assert_eq!(kv.source_inline_range_scans(), 1);
    }

    #[test]
    fn given_cross_schema_merge_sources_when_new_column_stats_missing_then_missing_rows_count_as_nulls()
     {
        let table = TableId(3);
        let sources = vec![
            DataFileRow::new(
                DataFileId(18),
                table,
                "main/t/source-18.parquet",
                5,
                674,
                CatalogOrderId::uuid_v7(17),
            ),
            DataFileRow::new(
                DataFileId(20),
                table,
                "main/t/source-20.parquet",
                2,
                585,
                CatalogOrderId::uuid_v7(19),
            ),
            DataFileRow::new(
                DataFileId(23),
                table,
                "main/t/source-23.parquet",
                2,
                710,
                CatalogOrderId::uuid_v7(22),
            ),
        ];
        let stats = vec![
            FileColumnStatsRow::new(
                DataFileId(18),
                table,
                ColumnId(1),
                0,
                Some("0".to_owned()),
                Some("4".to_owned()),
            ),
            FileColumnStatsRow::new(
                DataFileId(20),
                table,
                ColumnId(1),
                0,
                Some("5".to_owned()),
                Some("6".to_owned()),
            ),
            FileColumnStatsRow::new(
                DataFileId(23),
                table,
                ColumnId(1),
                0,
                Some("7".to_owned()),
                Some("8".to_owned()),
            ),
            FileColumnStatsRow::new(
                DataFileId(23),
                table,
                ColumnId(2),
                0,
                Some("eight".to_owned()),
                Some("seven".to_owned()),
            ),
        ];
        let source_refs = sources.iter().collect::<Vec<_>>();

        let merged = merge_stats_for_replacement(
            &stats,
            &source_refs,
            &DataFileRow::new(
                DataFileId(24),
                table,
                "main/t/merged.parquet",
                9,
                581,
                CatalogOrderId::uuid_v7(17),
            ),
        );

        let label_stats = merged
            .iter()
            .find(|row| row.column_id == ColumnId(2))
            .unwrap();
        assert_eq!(label_stats.value_count, Some(9));
        assert_eq!(label_stats.null_count, 7);
        assert_eq!(label_stats.min_value.as_deref(), Some("eight"));
        assert_eq!(label_stats.max_value.as_deref(), Some("seven"));
    }

    #[test]
    fn given_partitioned_merge_replaces_multiple_groups_when_normalized_then_each_replacement_uses_partition_visibility()
     {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let file_1 = append_file_with_partition(&mut kv, catalog, table, DataFileId(1), 0, "1");
        let file_2 = append_file_with_partition(&mut kv, catalog, table, DataFileId(2), 1, "2");
        let file_3 = append_file_with_partition(&mut kv, catalog, table, DataFileId(3), 2, "1");
        let file_4 = append_file_with_partition(&mut kv, catalog, table, DataFileId(4), 3, "2");

        let mut compaction = MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3), DataFileId(4)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(5),
                    table,
                    "main/table-1/merged-part-1.parquet",
                    2,
                    512,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0)
                .with_begin_order(file_3),
                DataFileRow::new(
                    DataFileId(6),
                    table,
                    "main/table-1/merged-part-2.parquet",
                    2,
                    512,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(1)
                .with_begin_order(file_4),
            ],
            partition_values: vec![
                partition_value(DataFileId(5), table, "1"),
                partition_value(DataFileId(6), table, "2"),
            ],
            file_column_stats: Vec::new(),
        };

        let source_context = MergeSourceContext::load(&kv, catalog, &compaction).unwrap();
        normalize_merge_replacements(&source_context, &mut compaction).unwrap();

        assert_eq!(compaction.new_files[0].validity.begin_order, file_1);
        assert_eq!(compaction.new_files[0].max_partial_order, Some(file_3));
        assert_eq!(compaction.new_files[1].validity.begin_order, file_2);
        assert_eq!(compaction.new_files[1].max_partial_order, Some(file_4));
    }

    #[test]
    fn given_single_partitioned_merge_replacement_when_normalized_then_uses_scoped_sources() {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let first_source_order =
            append_file_with_partition(&mut kv, catalog, table, DataFileId(1), 2, "2023-07-02");
        let second_source_order =
            append_file_with_partition(&mut kv, catalog, table, DataFileId(2), 4, "2023-07-02");

        let mut compaction = MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(3),
                    table,
                    "main/sales/sale_date=2023-07-02/merged.parquet",
                    4,
                    813,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(2)
                .with_begin_order(second_source_order),
            ],
            partition_values: vec![FilePartitionValueRow::new(
                DataFileId(3),
                table,
                PartitionKeyIndex(0),
                "2023-07-02".to_owned(),
            )],
            file_column_stats: Vec::new(),
        };

        let source_context = MergeSourceContext::load(&kv, catalog, &compaction).unwrap();
        normalize_merge_replacements(&source_context, &mut compaction).unwrap();

        assert_eq!(
            compaction.new_files[0].validity.begin_order,
            first_source_order
        );
        assert_eq!(
            compaction.new_files[0].max_partial_order,
            Some(second_source_order)
        );
    }

    #[test]
    fn given_source_file_partition_values_when_loaded_then_catalog_wide_partition_scan_is_not_used()
    {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();

        append_file_with_partition(&mut inner, catalog, table, DataFileId(1), 0, "source");
        append_file_with_partition(&mut inner, catalog, table, DataFileId(2), 1, "source");
        append_file_with_partition(&mut inner, catalog, table, DataFileId(99), 2, "unrelated");
        let kv = PartitionScanRejectingKv::new(inner, catalog);

        let rows = list_partition_values_for_source_file_ids(
            &kv,
            catalog,
            &[DataFileId(1), DataFileId(2)],
        )
        .unwrap();
        let loaded_ids = rows
            .iter()
            .map(|row| row.data_file_id)
            .collect::<BTreeSet<_>>();

        assert_eq!(loaded_ids, BTreeSet::from([DataFileId(1), DataFileId(2)]));
        assert_eq!(kv.partition_prefix_scans(), 0);
    }

    #[test]
    fn given_merge_stats_when_derived_then_catalog_wide_stats_scan_is_not_used() {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        append_file_with_partition(&mut inner, catalog, table, DataFileId(1), 0, "source");
        append_file_with_partition(&mut inner, catalog, table, DataFileId(2), 1, "source");
        append_file_with_partition(&mut inner, catalog, table, DataFileId(99), 2, "unrelated");
        for data_file_id in [DataFileId(1), DataFileId(2), DataFileId(99)] {
            register_file_column_stats(
                &mut inner,
                catalog,
                FileColumnStatsRow::new(
                    data_file_id,
                    table,
                    ColumnId(1),
                    0,
                    Some(format!("{}", data_file_id.0)),
                    Some(format!("{}", data_file_id.0)),
                ),
            )
            .unwrap();
        }
        let kv = StatsScanRejectingKv::new(inner, catalog);
        let compaction = MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![DataFileRow::new(
                DataFileId(3),
                table,
                "main/table-1/merged.parquet",
                2,
                512,
                CatalogOrderId::uuid_v7(0),
            )],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        };
        let source_context = MergeSourceContext::load(&kv, catalog, &compaction).unwrap();

        let stats = derive_merge_replacement_stats(
            &kv,
            catalog,
            &source_context,
            &compaction.new_files,
            &compaction.partition_values,
        )
        .unwrap();

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].data_file_id, DataFileId(3));
        assert_eq!(kv.catalog_stats_prefix_scans(), 0);
        assert_eq!(kv.source_stats_prefix_scans(), 0);
        assert_eq!(kv.source_stats_range_scans(), 1);
    }

    #[test]
    fn given_merge_sources_when_rejecting_delete_files_then_current_delete_reads_are_batched() {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let appended = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    1,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    1,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(1),
            ],
        )
        .unwrap();
        commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(7),
                DataFileId(2),
                "main/table-1/source-2-delete.parquet",
                1,
                64,
                appended[1].validity.begin_order,
            )],
        )
        .unwrap();
        let kv = CurrentDeleteReadCountingKv::new(inner, catalog);

        let error =
            reject_source_delete_files(&kv, catalog, &[DataFileId(1), DataFileId(2)]).unwrap_err();

        assert!(matches!(error, crate::CatalogError::InvalidMutation(_)));
        assert_eq!(kv.current_delete_gets(), 0);
        assert_eq!(kv.current_delete_batch_gets(), 1);
    }

    #[test]
    fn given_rewrite_sources_with_inline_deletions_when_loading_then_uses_file_scoped_scans() {
        let catalog = CatalogId(90);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let sources = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(3),
                DataFileRow::new(
                    DataFileId(99),
                    table,
                    "main/table-1/unrelated.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(99),
            ],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut inner,
            catalog,
            vec![
                InlineFileDeletionRow::new(table, DataFileId(1), 1, CatalogOrderId::uuid_v7(0)),
                InlineFileDeletionRow::new(table, DataFileId(2), 4, CatalogOrderId::uuid_v7(0)),
                InlineFileDeletionRow::new(table, DataFileId(99), 99, CatalogOrderId::uuid_v7(0)),
            ],
        )
        .unwrap();
        let kv = RewriteInlineDeletionScanCountingKv::new(
            inner,
            catalog,
            table,
            BTreeSet::from([DataFileId(1), DataFileId(2)]),
        );

        let deletions = rewrite_source_deletions(&kv, catalog, table, &sources[..2]).unwrap();

        assert_eq!(deletions.inline_file_deletions.len(), 2);
        assert_eq!(kv.inline_deletion_table_scans(), 0);
        assert_eq!(kv.inline_deletion_file_scans(), 2);
    }

    #[test]
    fn given_rewrite_source_with_materialized_delete_when_loading_inline_deletions_then_skips_that_file()
     {
        let catalog = CatalogId(91);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let sources = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(3),
            ],
        )
        .unwrap();
        commit_register_delete_files(
            &mut inner,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(10),
                DataFileId(1),
                "main/table-1/source-1-delete.parquet",
                1,
                64,
                sources[0].validity.begin_order,
            )],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut inner,
            catalog,
            vec![
                InlineFileDeletionRow::new(table, DataFileId(1), 1, CatalogOrderId::uuid_v7(0)),
                InlineFileDeletionRow::new(table, DataFileId(2), 4, CatalogOrderId::uuid_v7(0)),
            ],
        )
        .unwrap();
        let kv = RewriteInlineDeletionScanCountingKv::new(
            inner,
            catalog,
            table,
            BTreeSet::from([DataFileId(1), DataFileId(2)]),
        );

        let deletions = rewrite_source_deletions(&kv, catalog, table, &sources).unwrap();

        assert_eq!(deletions.expired_delete_files.len(), 1);
        assert_eq!(deletions.inline_file_deletions.len(), 1);
        assert_eq!(kv.inline_deletion_table_scans(), 0);
        assert_eq!(kv.inline_deletion_file_scans(), 1);
    }

    #[test]
    fn given_all_rewrite_sources_have_materialized_deletes_when_loading_then_skips_inline_discovery()
     {
        let catalog = CatalogId(92);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let sources = commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(3),
            ],
        )
        .unwrap();
        commit_register_delete_files(
            &mut inner,
            catalog,
            vec![
                DeleteFileRow::new(
                    DeleteFileId(10),
                    DataFileId(1),
                    "main/table-1/source-1-delete.parquet",
                    1,
                    64,
                    sources[0].validity.begin_order,
                ),
                DeleteFileRow::new(
                    DeleteFileId(11),
                    DataFileId(2),
                    "main/table-1/source-2-delete.parquet",
                    1,
                    64,
                    sources[1].validity.begin_order,
                ),
            ],
        )
        .unwrap();
        let kv = RewriteInlineDeletionScanCountingKv::new(
            inner,
            catalog,
            table,
            BTreeSet::from([DataFileId(1), DataFileId(2)]),
        );

        let deletions = rewrite_source_deletions(&kv, catalog, table, &sources).unwrap();

        assert_eq!(deletions.expired_delete_files.len(), 2);
        assert!(deletions.inline_file_deletions.is_empty());
        assert_eq!(kv.latest_snapshot_gets(), 0);
        assert_eq!(kv.inline_deletion_table_scans(), 0);
        assert_eq!(kv.inline_deletion_file_scans(), 0);
    }

    #[test]
    fn given_rewrite_sources_loaded_when_normalizing_replacements_then_source_rows_are_reused() {
        let catalog = CatalogId(88);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(100),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    2,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(103),
            ],
        )
        .unwrap();
        let kv = SourceDataFileReadCountingKv::new(inner, catalog);
        let mut replacements = vec![DataFileRow::new(
            DataFileId(10),
            table,
            "main/table-1/replacement.parquet",
            2,
            128,
            CatalogOrderId::uuid_v7(0),
        )];

        let source_context = RewriteSourceContext::load(
            &kv,
            catalog,
            &[DataFileId(1), DataFileId(2)],
            &replacements,
        )
        .unwrap();
        normalize_rewrite_replacement_row_ids_from_sources(
            source_context.sources(),
            &mut replacements,
        )
        .unwrap();

        assert_eq!(source_context.table_id(), table);
        assert_eq!(replacements[0].row_id_start, 103);
        assert!(replacements[0].row_id_start_known);
        assert_eq!(kv.source_data_file_gets(), 0);
        assert_eq!(kv.source_data_file_batch_gets(), 1);
    }

    #[test]
    fn given_merge_sources_loaded_when_normalizing_replacements_then_source_rows_are_reused() {
        let catalog = CatalogId(89);
        let table = TableId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(1),
                    table,
                    "main/table-1/source-1.parquet",
                    3,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(100),
                DataFileRow::new(
                    DataFileId(2),
                    table,
                    "main/table-1/source-2.parquet",
                    2,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(103),
            ],
        )
        .unwrap();
        let kv = SourceDataFileReadCountingKv::new(inner, catalog);
        let mut compaction = MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![DataFileRow::new(
                DataFileId(10),
                table,
                "main/table-1/replacement.parquet",
                5,
                256,
                CatalogOrderId::uuid_v7(0),
            )],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        };

        let source_context = MergeSourceContext::load(&kv, catalog, &compaction).unwrap();
        normalize_merge_replacements(&source_context, &mut compaction).unwrap();

        assert_eq!(source_context.table_id(), table);
        assert_eq!(compaction.new_files[0].row_id_start, 100);
        assert!(compaction.new_files[0].row_id_start_known);
        assert_eq!(kv.source_data_file_gets(), 0);
        assert_eq!(kv.source_data_file_batch_gets(), 1);
    }

    fn append_file_with_partition(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        table: TableId,
        data_file_id: DataFileId,
        row_id_start: u64,
        partition: &str,
    ) -> CatalogOrderId {
        let file = commit_append_data_files(
            kv,
            catalog,
            vec![
                DataFileRow::new(
                    data_file_id,
                    table,
                    format!("main/table-{}/file-{}.parquet", table.0, data_file_id.0),
                    1,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(row_id_start),
            ],
        )
        .unwrap()
        .remove(0);
        register_file_partition_value(kv, catalog, partition_value(data_file_id, table, partition))
            .unwrap();
        file.validity.begin_order
    }

    fn partition_value(
        data_file_id: DataFileId,
        table: TableId,
        value: &str,
    ) -> FilePartitionValueRow {
        FilePartitionValueRow::new(data_file_id, table, PartitionKeyIndex(0), value.to_owned())
    }

    struct PartitionScanRejectingKv {
        inner: FakeOrderedCatalogKv,
        catalog_partition_family_prefix: Vec<u8>,
        partition_prefix_scans: Cell<usize>,
    }

    struct CurrentDeleteReadCountingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        current_delete_gets: Cell<usize>,
        current_delete_batch_gets: Cell<usize>,
    }

    struct SourceDataFileReadCountingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        source_data_file_gets: Cell<usize>,
        source_data_file_batch_gets: Cell<usize>,
    }

    struct RewriteInlineDeletionScanCountingKv {
        inner: FakeOrderedCatalogKv,
        latest_snapshot_key: Vec<u8>,
        table_prefix: Vec<u8>,
        file_prefixes: BTreeSet<Vec<u8>>,
        latest_snapshot_gets: Cell<usize>,
        inline_deletion_table_scans: Cell<usize>,
        inline_deletion_file_scans: Cell<usize>,
    }

    struct SourceDeleteConflictScanCountingKv {
        inner: FakeOrderedCatalogKv,
        table_delete_change_prefix: Vec<u8>,
        table_inline_prefix: Vec<u8>,
        source_delete_timeline_prefix: Vec<u8>,
        source_inline_prefix: Vec<u8>,
        table_delete_change_range_scans: Cell<usize>,
        table_inline_prefix_scans: Cell<usize>,
        source_delete_timeline_range_scans: Cell<usize>,
        source_inline_range_scans: Cell<usize>,
    }

    struct StatsScanRejectingKv {
        inner: FakeOrderedCatalogKv,
        catalog_stats_family_prefix: Vec<u8>,
        catalog: CatalogId,
        catalog_stats_prefix_scans: Cell<usize>,
        source_stats_prefix_scans: Cell<usize>,
        source_stats_range_scans: Cell<usize>,
    }

    impl CurrentDeleteReadCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog,
                current_delete_gets: Cell::new(0),
                current_delete_batch_gets: Cell::new(0),
            }
        }

        fn current_delete_gets(&self) -> usize {
            self.current_delete_gets.get()
        }

        fn current_delete_batch_gets(&self) -> usize {
            self.current_delete_batch_gets.get()
        }

        fn is_current_delete_key(&self, key: &[u8]) -> bool {
            key == current_delete_file_key(self.catalog, DataFileId(1))
                || key == current_delete_file_key(self.catalog, DataFileId(2))
        }
    }

    impl OrderedCatalogKv for CurrentDeleteReadCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if self.is_current_delete_key(key) {
                self.current_delete_gets
                    .set(self.current_delete_gets.get().saturating_add(1));
            }
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            if keys.iter().any(|key| self.is_current_delete_key(key)) {
                self.current_delete_batch_gets
                    .set(self.current_delete_batch_gets.get().saturating_add(1));
            }
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
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

    impl SourceDataFileReadCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog,
                source_data_file_gets: Cell::new(0),
                source_data_file_batch_gets: Cell::new(0),
            }
        }

        fn source_data_file_gets(&self) -> usize {
            self.source_data_file_gets.get()
        }

        fn source_data_file_batch_gets(&self) -> usize {
            self.source_data_file_batch_gets.get()
        }

        fn is_source_data_file_key(&self, key: &[u8]) -> bool {
            key == data_file_key(self.catalog, DataFileId(1))
                || key == data_file_key(self.catalog, DataFileId(2))
        }
    }

    impl RewriteInlineDeletionScanCountingKv {
        fn new(
            inner: FakeOrderedCatalogKv,
            catalog: CatalogId,
            table: TableId,
            data_file_ids: BTreeSet<DataFileId>,
        ) -> Self {
            Self {
                inner,
                latest_snapshot_key: latest_snapshot_row_key(catalog),
                table_prefix: inline_file_deletion_table_prefix(catalog, table),
                file_prefixes: data_file_ids
                    .into_iter()
                    .map(|data_file_id| {
                        inline_file_deletion_file_prefix(catalog, table, data_file_id)
                    })
                    .collect(),
                latest_snapshot_gets: Cell::new(0),
                inline_deletion_table_scans: Cell::new(0),
                inline_deletion_file_scans: Cell::new(0),
            }
        }

        fn latest_snapshot_gets(&self) -> usize {
            self.latest_snapshot_gets.get()
        }

        fn inline_deletion_table_scans(&self) -> usize {
            self.inline_deletion_table_scans.get()
        }

        fn inline_deletion_file_scans(&self) -> usize {
            self.inline_deletion_file_scans.get()
        }
    }

    impl OrderedCatalogKv for RewriteInlineDeletionScanCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if key == self.latest_snapshot_key {
                self.latest_snapshot_gets
                    .set(self.latest_snapshot_gets.get().saturating_add(1));
            }
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
            if prefix == self.table_prefix {
                self.inline_deletion_table_scans
                    .set(self.inline_deletion_table_scans.get().saturating_add(1));
            }
            if self.file_prefixes.contains(prefix) {
                self.inline_deletion_file_scans
                    .set(self.inline_deletion_file_scans.get().saturating_add(1));
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

    impl SourceDeleteConflictScanCountingKv {
        fn new(
            inner: FakeOrderedCatalogKv,
            catalog: CatalogId,
            table: TableId,
            source: DataFileId,
        ) -> Self {
            Self {
                inner,
                table_delete_change_prefix: order_delete_file_change_prefix(catalog),
                table_inline_prefix: inline_file_deletion_table_prefix(catalog, table),
                source_delete_timeline_prefix: delete_file_timeline_prefix(catalog, source),
                source_inline_prefix: inline_file_deletion_file_prefix(catalog, table, source),
                table_delete_change_range_scans: Cell::new(0),
                table_inline_prefix_scans: Cell::new(0),
                source_delete_timeline_range_scans: Cell::new(0),
                source_inline_range_scans: Cell::new(0),
            }
        }

        fn table_delete_change_range_scans(&self) -> usize {
            self.table_delete_change_range_scans.get()
        }

        fn table_inline_prefix_scans(&self) -> usize {
            self.table_inline_prefix_scans.get()
        }

        fn source_delete_timeline_range_scans(&self) -> usize {
            self.source_delete_timeline_range_scans.get()
        }

        fn source_inline_range_scans(&self) -> usize {
            self.source_inline_range_scans.get()
        }
    }

    impl OrderedCatalogKv for SourceDeleteConflictScanCountingKv {
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
            if prefix == self.table_inline_prefix {
                self.table_inline_prefix_scans
                    .set(self.table_inline_prefix_scans.get().saturating_add(1));
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
            if start.starts_with(&self.table_delete_change_prefix) {
                self.table_delete_change_range_scans
                    .set(self.table_delete_change_range_scans.get().saturating_add(1));
            }
            if start.starts_with(&self.source_delete_timeline_prefix) {
                self.source_delete_timeline_range_scans.set(
                    self.source_delete_timeline_range_scans
                        .get()
                        .saturating_add(1),
                );
            }
            if start.starts_with(&self.source_inline_prefix) {
                self.source_inline_range_scans
                    .set(self.source_inline_range_scans.get().saturating_add(1));
            }
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    impl OrderedCatalogKv for SourceDataFileReadCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if self.is_source_data_file_key(key) {
                self.source_data_file_gets
                    .set(self.source_data_file_gets.get().saturating_add(1));
            }
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            if keys.iter().any(|key| self.is_source_data_file_key(key)) {
                self.source_data_file_batch_gets
                    .set(self.source_data_file_batch_gets.get().saturating_add(1));
            }
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
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

    impl StatsScanRejectingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog_stats_family_prefix: family_prefix(catalog, KeyFamily::FileColumnStats),
                catalog,
                catalog_stats_prefix_scans: Cell::new(0),
                source_stats_prefix_scans: Cell::new(0),
                source_stats_range_scans: Cell::new(0),
            }
        }

        fn catalog_stats_prefix_scans(&self) -> usize {
            self.catalog_stats_prefix_scans.get()
        }

        fn source_stats_prefix_scans(&self) -> usize {
            self.source_stats_prefix_scans.get()
        }

        fn source_stats_range_scans(&self) -> usize {
            self.source_stats_range_scans.get()
        }

        fn is_source_stats_prefix(&self, prefix: &[u8]) -> bool {
            prefix == file_column_stats_data_file_prefix(self.catalog, DataFileId(1))
                || prefix == file_column_stats_data_file_prefix(self.catalog, DataFileId(2))
        }
    }

    impl OrderedCatalogKv for StatsScanRejectingKv {
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
            if prefix == self.catalog_stats_family_prefix {
                self.catalog_stats_prefix_scans
                    .set(self.catalog_stats_prefix_scans.get().saturating_add(1));
            }
            if self.is_source_stats_prefix(prefix) {
                self.source_stats_prefix_scans
                    .set(self.source_stats_prefix_scans.get().saturating_add(1));
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
            if self.is_source_stats_prefix(start) {
                self.source_stats_range_scans
                    .set(self.source_stats_range_scans.get().saturating_add(1));
            }
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    impl PartitionScanRejectingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog_partition_family_prefix: family_prefix(
                    catalog,
                    KeyFamily::FilePartitionValue,
                ),
                partition_prefix_scans: Cell::new(0),
            }
        }

        fn partition_prefix_scans(&self) -> usize {
            self.partition_prefix_scans.get()
        }
    }

    impl OrderedCatalogKv for PartitionScanRejectingKv {
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
            assert_ne!(
                prefix, self.catalog_partition_family_prefix,
                "merge-adjacent compaction should not scan all catalog partition values"
            );
            if prefix.starts_with(&self.catalog_partition_family_prefix) {
                self.partition_prefix_scans
                    .set(self.partition_prefix_scans.get().saturating_add(1));
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
