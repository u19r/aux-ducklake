#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        FakeOrderedCatalogKv, FilePartitionValueRow, MutableCatalogKv, PartitionKeyIndex,
        TableColumnRow, TableId, TablePartitionFieldRow, TablePartitionRow, TableRow,
        TableVersionReplacement, commit_create_table_row, initialize_catalog_if_absent,
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
            &data_files,
            &delete_files,
            &[],
        )
        .unwrap();
    }

    #[test]
    fn given_partition_changed_after_read_when_append_matches_current_partition_then_staleness_check_passes()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let read_order = CatalogOrderId::uuid_v7(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
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
        let created = commit_create_table_row(&mut kv, catalog, source_partitioned).unwrap();
        let created_snapshot = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut id_partitioned = created.clone();
        id_partitioned.partition = Some(TablePartitionRow::new(
            11,
            vec![TablePartitionFieldRow::new(
                0,
                crate::ColumnId(2),
                "identity",
            )],
        ));
        kv.commit_table_replacements(
            catalog,
            created_snapshot.sequence,
            vec![TableVersionReplacement::new(
                table_id,
                created,
                id_partitioned,
            )],
        )
        .unwrap();
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

        super::super::reject_append_files_incompatible_with_current_tables(
            &kv,
            catalog,
            read_order,
            &data_files,
            &partition_values,
            &partition_sets,
        )
        .unwrap();
    }
}
