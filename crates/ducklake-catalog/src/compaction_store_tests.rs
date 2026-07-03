#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        FakeOrderedCatalogKv, FileColumnStatsRow, MergeAdjacentCompaction, RewriteDeleteCompaction,
        TableId, commit_append_data_files, commit_merge_adjacent_data_files,
        commit_register_delete_files, commit_rewrite_delete_data_files, initialize_empty_catalog,
        list_current_data_files, list_file_column_stats_for_table_column,
        register_file_column_stats,
    };

    #[test]
    fn given_merge_replacement_without_row_id_when_sources_have_row_ids_then_storage_derives_start()
    {
        let catalog = CatalogId(0);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_empty_catalog(&mut kv, catalog).unwrap();

        commit_append_data_files(
            &mut kv,
            catalog,
            vec![data_file(1, table, 0, 1), data_file(2, table, 1, 1)],
        )
        .unwrap();

        let merged = DataFileRow::new(
            DataFileId(3),
            table,
            "main/table/merged.parquet",
            2,
            512,
            CatalogOrderId::uuid_v7(0),
        );
        let committed = commit_merge_adjacent_data_files(
            &mut kv,
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(1), DataFileId(2)],
                new_files: vec![merged],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        assert!(committed.new_files[0].row_id_start_known);
        assert_eq!(committed.new_files[0].row_id_start, 0);
        let live_files = list_current_data_files(&kv, catalog, table).unwrap();
        assert_eq!(live_files.len(), 1);
        assert_eq!(live_files[0].data_file_id, DataFileId(3));
        assert!(live_files[0].row_id_start_known);
        assert_eq!(live_files[0].row_id_start, 0);
    }

    #[test]
    fn given_rewrite_replacement_without_row_id_when_source_has_deleted_prefix_then_storage_derives_upper_range()
     {
        let catalog = CatalogId(0);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_empty_catalog(&mut kv, catalog).unwrap();
        commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 1000)]).unwrap();
        commit_register_delete_files(
            &mut kv,
            catalog,
            vec![DeleteFileRow::new(
                DeleteFileId(2),
                DataFileId(1),
                "main/table/1-delete.parquet",
                951,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        register_file_column_stats(
            &mut kv,
            catalog,
            FileColumnStatsRow::new(
                DataFileId(1),
                table,
                ColumnId(1),
                0,
                Some("1000".to_owned()),
                Some("1999".to_owned()),
            )
            .with_value_count(Some(1000)),
        )
        .unwrap();

        let rewritten = commit_rewrite_delete_data_files(
            &mut kv,
            catalog,
            RewriteDeleteCompaction {
                source_file_ids: vec![DataFileId(1)],
                new_files: vec![DataFileRow::new(
                    DataFileId(3),
                    table,
                    "main/table/rewrite.parquet",
                    49,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

        assert!(rewritten.new_files[0].row_id_start_known);
        assert_eq!(rewritten.new_files[0].row_id_start, 951);
        let live_files = list_current_data_files(&kv, catalog, table).unwrap();
        assert_eq!(live_files[0].row_id_start, 951);
        let stats = list_file_column_stats_for_table_column(&kv, catalog, table, ColumnId(1))
            .unwrap()
            .into_iter()
            .find(|row| row.data_file_id == DataFileId(3))
            .unwrap();
        assert_eq!(stats.value_count, Some(49));
        assert_eq!(stats.min_value.as_deref(), Some("1000"));
        assert_eq!(stats.max_value.as_deref(), Some("1999"));
    }

    fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
        DataFileRow::new(
            DataFileId(id),
            table,
            format!("main/table/{id}.parquet"),
            record_count,
            128,
            CatalogOrderId::uuid_v7(0),
        )
        .with_row_id_start(row_id_start)
    }
}
