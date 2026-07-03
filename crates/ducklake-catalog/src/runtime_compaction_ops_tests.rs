#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
        SchemaId, TableColumnRow, TableId, TableRow, commit_append_data_files,
        commit_create_table_row, initialize_catalog_if_absent, latest_snapshot,
    };

    use super::super::{
        MERGE_ADJACENT_FILES, RewriteDeleteOperation, compaction_payload_values,
        merge_adjacent_compactions_from_payload, resolve_compaction_file_visibility,
        resolve_compaction_file_visibility_orders,
    };

    #[test]
    fn given_compaction_file_has_empty_row_id_when_parsed_then_row_id_is_unknown() {
        let parsed = compaction_payload_values(
            MERGE_ADJACENT_FILES,
            b"source_file\t10\t1\nfile\t3\t10\tmain/table/merged.parquet\t2\t512\t\n",
        )
        .unwrap();

        assert_eq!(parsed.source_file_ids.len(), 1);
        assert_eq!(parsed.new_files.len(), 1);
        assert!(!parsed.new_files[0].row_id_start_known);
    }

    #[test]
    fn given_rewrite_delete_payload_spans_tables_when_routed_then_each_table_gets_its_own_compaction()
     {
        let parsed = compaction_payload_values(
            super::super::REWRITE_DELETE_FILES,
            b"source_file\t10\t1\nsource_file\t20\t2\nfile\t3\t10\tmain/a/rewrite.parquet\t9\t512\t\nfile\t4\t20\tmain/b/rewrite.parquet\t8\t512\t\nfile_column_stats\t3\t10\t1\t9\t0\t2\t10\t\nfile_column_stats\t4\t20\t1\t8\t0\t3\t10\t\n",
        )
        .unwrap();

        let operation = RewriteDeleteOperation::from_payload(parsed).unwrap();
        let compactions = operation.compactions;

        assert_eq!(compactions.len(), 2);
        assert_eq!(compactions[0].source_file_ids, vec![DataFileId(1)]);
        assert_eq!(compactions[0].new_files[0].table_id, TableId(10));
        assert_eq!(compactions[0].file_column_stats[0].table_id, TableId(10));
        assert_eq!(compactions[1].source_file_ids, vec![DataFileId(2)]);
        assert_eq!(compactions[1].new_files[0].table_id, TableId(20));
        assert_eq!(compactions[1].file_column_stats[0].table_id, TableId(20));
    }

    #[test]
    fn given_merge_adjacent_payload_spans_tables_when_routed_then_each_table_gets_its_own_compaction()
     {
        let parsed = compaction_payload_values(
            MERGE_ADJACENT_FILES,
            b"source_file\t10\t1\nsource_file\t20\t2\nfile\t3\t10\tmain/a/merged.parquet\t9\t512\t0\nfile\t4\t20\tmain/b/merged.parquet\t8\t512\t9\nfile_column_stats\t3\t10\t1\t9\t0\t2\t10\t\nfile_column_stats\t4\t20\t1\t8\t0\t3\t10\t\n",
        )
        .unwrap();

        let compactions = merge_adjacent_compactions_from_payload(parsed).unwrap();

        assert_eq!(compactions.len(), 2);
        assert_eq!(
            compactions[0].compaction.source_file_ids,
            vec![DataFileId(1)]
        );
        assert_eq!(compactions[0].compaction.new_files[0].table_id, TableId(10));
        assert_eq!(
            compactions[0].compaction.file_column_stats[0].table_id,
            TableId(10)
        );
        assert_eq!(
            compactions[1].compaction.source_file_ids,
            vec![DataFileId(2)]
        );
        assert_eq!(compactions[1].compaction.new_files[0].table_id, TableId(20));
        assert_eq!(
            compactions[1].compaction.file_column_stats[0].table_id,
            TableId(20)
        );
    }

    #[test]
    fn given_compaction_payload_has_public_visibility_when_resolved_then_file_uses_catalog_orders()
    {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table,
                SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders/",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
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
                table,
                "main/orders/source.parquet",
                1,
                100,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let parsed = compaction_payload_values(
            MERGE_ADJACENT_FILES,
            format!(
                "source_file\t10\t1\nfile\t3\t10\tmain/orders/merged.parquet\t2\t512\t0\t\t{}\t{}\n",
                create_snapshot.sequence.0, append_snapshot.sequence.0
        )
        .as_bytes(),
        )
        .unwrap();
        let mut intents = merge_adjacent_compactions_from_payload(parsed).unwrap();
        let intent = intents.pop().unwrap();
        let visibility = intent.file_visibility;
        let mut compaction = intent.compaction;

        let resolved =
            resolve_compaction_file_visibility_orders(&kv, catalog, visibility[0]).unwrap();
        resolve_compaction_file_visibility(&kv, catalog, &mut compaction, &visibility).unwrap();

        assert_eq!(resolved.data_file_id, DataFileId(3));
        assert_eq!(resolved.begin_order, create_snapshot.order);
        assert_eq!(resolved.max_partial_order, Some(append_snapshot.order));
        assert_eq!(
            compaction.new_files[0].validity.begin_order,
            create_snapshot.order
        );
        assert_eq!(
            compaction.new_files[0].max_partial_order,
            Some(append_snapshot.order)
        );
    }
}
