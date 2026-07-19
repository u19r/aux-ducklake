#[cfg(test)]
mod storage_contract_tests {
    use super::super::{
        InlineMaterializationFile, InlineMaterializationRequest, delete_inline_rows_with_catalog,
        inline_delete_payload, register_inline_tables_with_catalog,
    };
    use crate::{
        CatalogError, CatalogId, CatalogOrderId, FakeOrderedCatalogKv, InlinedTableRow, SchemaId,
        TableColumnRow, TableId, TableRow, commit_create_table_row, initialize_catalog_if_absent,
        register_inline_table_payload_with_table,
    };

    #[test]
    fn register_inline_tables_requires_the_requested_table_id_to_exist() {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(7),
                SchemaId(0),
                "events-table-uuid",
                "events",
                "main/events/",
                vec![TableColumnRow::new(
                    crate::ColumnId(1),
                    "id",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();

        let error = register_inline_tables_with_catalog(
            &mut kv,
            catalog,
            InlineMaterializationRequest {
                commit_snapshot: None,
                materialization_files: vec![InlineMaterializationFile {
                    table_id: TableId(8),
                    schema_version: 2,
                    table_name: "ducklake_inlined_data_8_2".to_owned(),
                }],
            },
        )
        .unwrap_err();

        assert!(matches!(error, CatalogError::NotFound("inline table")));
    }

    #[test]
    fn given_delete_inline_rows_spans_schema_versions_when_parsed_then_targets_are_grouped_by_inlined_table()
     {
        let request = inline_delete_payload(
            b"commit_snapshot\t7\n\
              delete\t10\tducklake_inlined_data_10_1\t0\n\
              delete\t10\tducklake_inlined_data_10_2\t80\n\
              delete\t10\tducklake_inlined_data_10_1\t10\n",
        )
        .unwrap();

        assert_eq!(request.commit_snapshot, Some(crate::DuckLakeSnapshotId(7)));
        assert_eq!(request.targets.len(), 2);
        assert_eq!(request.targets[0].table_id, TableId(10));
        assert_eq!(request.targets[0].table_name, "ducklake_inlined_data_10_1");
        assert_eq!(request.targets[0].row_ids, vec![0, 10]);
        assert_eq!(request.targets[1].table_id, TableId(10));
        assert_eq!(request.targets[1].table_name, "ducklake_inlined_data_10_2");
        assert_eq!(request.targets[1].row_ids, vec![80]);
    }

    #[test]
    fn given_delete_inline_rows_spans_schema_versions_when_committed_then_all_matching_rows_are_deleted()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut table = TableRow::new(table_id, "inlined_test", CatalogOrderId::uuid_v7(0));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_2", 2));
        commit_create_table_row(&mut kv, catalog, table.clone()).unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table.clone(),
            SchemaId(1),
            b"row\t0\ti:0\nrow\t10\ti:10\nrow\t11\ti:11\n".to_vec(),
        )
        .unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table,
            SchemaId(2),
            b"row\t80\ti:80\nrow\t81\ti:81\n".to_vec(),
        )
        .unwrap();
        let request = inline_delete_payload(
            b"delete\t10\tducklake_inlined_data_10_1\t0\n\
              delete\t10\tducklake_inlined_data_10_2\t80\n\
              delete\t10\tducklake_inlined_data_10_1\t10\n",
        )
        .unwrap();

        let commit = delete_inline_rows_with_catalog(&mut kv, catalog, request).unwrap();

        assert_eq!(commit.deleted_row_count, 3);
        assert_eq!(commit.rewritten_payload_count, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        inline_file_deletions_for_flush_payload, inline_file_deletions_payload,
        inline_flush_delete_positions_payload_values, inline_rows_payload, inline_rows_payloads,
        read_inline_rows_for_flush_payload_values,
        read_inline_rows_for_global_stats_batch_payload_values, register_inlined_table,
    };
    use crate::{
        CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
        DuckLakeSnapshotId, FakeOrderedCatalogKv, InlineFileDeletionRow, InlinedTableRow, TableId,
        TableRow, commit_append_data_files, commit_inline_file_deletions,
        commit_register_delete_files, initialize_catalog_if_absent, latest_snapshot,
    };

    #[test]
    fn given_flush_inline_read_payload_when_parsed_then_historical_rows_are_included() {
        let payload = read_inline_rows_for_flush_payload_values(
            b"inlined_table_name=ducklake_inlined_data_1_1\nsnapshot_id=3\ninclude_deleted=false\n",
        )
        .unwrap();

        assert_eq!(payload.table_name, "ducklake_inlined_data_1_1");
        assert_eq!(payload.snapshot.unwrap().public_id(), DuckLakeSnapshotId(3));
        assert!(payload.include_flushed);
        assert!(payload.include_deleted);
    }

    #[test]
    fn given_global_stats_batch_payload_when_parsed_then_all_inline_tables_share_snapshot() {
        let payload = read_inline_rows_for_global_stats_batch_payload_values(
            b"snapshot_id=9\ninlined_table_name=ducklake_inlined_data_1_1\ninlined_table_name=ducklake_inlined_data_1_2\n",
        )
        .unwrap();

        assert_eq!(payload.len(), 2);
        assert_eq!(payload[0].table_name, "ducklake_inlined_data_1_1");
        assert_eq!(payload[1].table_name, "ducklake_inlined_data_1_2");
        assert_eq!(
            payload[0].snapshot.unwrap().public_id(),
            DuckLakeSnapshotId(9)
        );
        assert_eq!(
            payload[1].snapshot.unwrap().public_id(),
            DuckLakeSnapshotId(9)
        );
    }

    #[test]
    fn given_current_flush_delete_positions_payload_when_parsed_then_snapshot_and_table_are_preserved()
     {
        let payload = inline_flush_delete_positions_payload_values(
            b"inlined_table_name=ducklake_inlined_data_1_1\nsnapshot_id=3\n",
            "ListCurrentInlineFlushDeletePositions",
        )
        .unwrap();

        assert_eq!(payload.table_name, "ducklake_inlined_data_1_1");
        assert_eq!(payload.snapshot.public_id(), DuckLakeSnapshotId(3));
    }

    #[test]
    fn given_inline_file_deletions_when_listing_flush_rows_then_file_path_and_delete_snapshot_are_returned()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(7);
        let initial = initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(42),
                    table,
                    "main/t/file.parquet",
                    3,
                    300,
                    initial.order,
                )
                .with_row_id_start(10),
            ],
        )
        .unwrap();
        commit_register_delete_files(
            &mut kv,
            catalog,
            vec![
                DeleteFileRow::new(
                    DeleteFileId(9),
                    DataFileId(42),
                    "main/t/delete.parquet",
                    1,
                    100,
                    initial.order,
                )
                .with_encryption_key("base64-key"),
            ],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![
                InlineFileDeletionRow::new(table, DataFileId(42), 10, initial.order),
                InlineFileDeletionRow::new(table, DataFileId(42), 11, initial.order),
            ],
        )
        .unwrap();
        let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let payload = inline_file_deletions_for_flush_payload(
            &kv,
            catalog,
            table,
            delete_snapshot.sequence.0,
        )
        .unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains("inline_file_deletion_count=2\n"), "{text}");
        assert!(
            text.contains(
                "inline_file_delete\t42\tmain/t/file.parquet\tfalse\t10\t3\t9\tmain/t/delete.parquet\tfalse\t2\tbase64-key\tPARQUET\n"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "inline_file_delete\t42\tmain/t/file.parquet\tfalse\t11\t3\t9\tmain/t/delete.parquet\tfalse\t2\tbase64-key\tPARQUET\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn given_inline_file_deletions_when_listing_rows_at_snapshot_then_file_id_and_row_id_are_returned()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(7);
        let initial = initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_append_data_files(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(42),
                    table,
                    "main/t/file.parquet",
                    3,
                    300,
                    initial.order,
                )
                .with_row_id_start(10),
            ],
        )
        .unwrap();
        commit_inline_file_deletions(
            &mut kv,
            catalog,
            vec![
                InlineFileDeletionRow::new(table, DataFileId(42), 10, initial.order),
                InlineFileDeletionRow::new(table, DataFileId(42), 11, initial.order),
            ],
        )
        .unwrap();
        let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let payload =
            inline_file_deletions_payload(&kv, catalog, table, delete_snapshot.sequence.0).unwrap();
        let text = String::from_utf8(payload).unwrap();

        assert_eq!(
            text,
            "inline_file_deletion_count=2\ninline_file_delete\t42\t10\ninline_file_delete\t42\t11\n"
        );
    }

    #[test]
    fn given_preserved_inline_rows_when_parsing_register_payload_then_only_current_commit_rows_are_staged()
     {
        let request = inline_rows_payload(
            b"commit_snapshot\t2\n\
              table\t14\t1\tducklake_inlined_data_14_1\n\
              row_begin\t1\t1\ti:10\n\
              row_begin\t2\t2\ti:20\n\
              row\t3\ti:30\n",
        )
        .unwrap();

        assert_eq!(request.payload, "row\t2\ti:20\nrow\t3\ti:30\n");
    }

    #[test]
    fn given_retried_inline_rows_begin_after_read_snapshot_when_parsing_register_payload_then_rows_are_staged()
     {
        let request = inline_rows_payload(
            b"read_snapshot\t1\n\
              commit_snapshot\t3\n\
              table\t14\t1\tducklake_inlined_data_14_1\n\
              row_begin\t1\t1\ti:10\n\
              row_begin\t2\t2\ti:20\n\
              row\t3\ti:30\n",
        )
        .unwrap();

        assert_eq!(request.payload, "row\t2\ti:20\nrow\t3\ti:30\n");
    }

    #[test]
    fn given_inline_rows_with_commit_metadata_when_parsed_then_metadata_is_typed() {
        let request = inline_rows_payload(
            b"commit_snapshot\t3\n\
              commit_author\tPedro\n\
              commit_message\t\n\
              commit_extra_info\t{\"query_id\":\"audit-1\"}\n\
              table\t1\t1\tducklake_inlined_data_1_1\n\
              row\t1\ti:1\ts:706564726f\n",
        )
        .unwrap();

        assert_eq!(request.commit_snapshot.unwrap().0, 3);
        assert_eq!(request.commit_metadata.author.as_deref(), Some("Pedro"));
        assert_eq!(request.commit_metadata.commit_message.as_deref(), Some(""));
        assert_eq!(
            request.commit_metadata.commit_extra_info.as_deref(),
            Some("{\"query_id\":\"audit-1\"}")
        );
        assert_eq!(request.payload, "row\t1\ti:1\ts:706564726f\n");
    }

    #[test]
    fn given_inline_rows_for_multiple_tables_when_parsed_then_each_table_gets_its_own_payload() {
        let requests = inline_rows_payloads(
            b"commit_snapshot\t10\n\
              read_snapshot\t9\n\
              table\t4\t9\tducklake_inlined_data_4_9\n\
              row\t1\ti:42\n\
              table\t3\t9\tducklake_inlined_data_3_9\n\
              row\t1\ts:68656c6c6f\ts:776f726c64\n",
        )
        .unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].table_id, TableId(4));
        assert_eq!(requests[0].schema_version, 9);
        assert_eq!(requests[0].payload, "row\t1\ti:42\n");
        assert_eq!(requests[1].table_id, TableId(3));
        assert_eq!(requests[1].schema_version, 9);
        assert_eq!(requests[1].payload, "row\t1\ts:68656c6c6f\ts:776f726c64\n");
    }

    #[test]
    fn given_superseded_inline_table_when_registering_new_inline_table_then_previous_inline_tables_are_retained()
     {
        let mut table = TableRow::new(TableId(14), "orders", CatalogOrderId::uuid_v7(1));
        table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_14_1", 1));

        let changed =
            register_inlined_table(&mut table, "ducklake_inlined_data_14_2".to_owned(), 2);

        assert!(changed);
        assert_eq!(table.inlined_data_tables.len(), 2);
        assert_eq!(
            table.inlined_data_tables[1].table_name,
            "ducklake_inlined_data_14_2"
        );
        assert_eq!(table.inlined_data_tables[1].schema_version, 2);
    }
}
