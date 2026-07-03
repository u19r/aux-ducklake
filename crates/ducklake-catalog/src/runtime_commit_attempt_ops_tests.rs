#[cfg(test)]
mod tests {
    use std::time::Instant;

    use alloc_counter::{AllocationGuard, emit_report};

    use super::super::*;
    #[cfg(feature = "foundationdb")]
    use crate::runtime_inline_rows::{
        ReadInlineRowsPayload, read_inline_rows_global_stats_payload,
    };
    use crate::runtime_snapshot_range::ProposedCommitSnapshot;
    #[cfg(feature = "foundationdb")]
    use crate::runtime_snapshot_range::ReadSnapshot;
    use crate::{
        CatalogId, ColumnId, FakeOrderedCatalogKv, SchemaId, SchemaRow, TableColumnRow, TableId,
        TablePartitionFieldRow, TablePartitionRow, TableRow, TableSortFieldRow, TableSortRow,
        ViewRow, commit_change_view_comment, commit_create_schema_rows, commit_create_table_row,
        commit_create_view_row, commit_drop_schema_rows, initialize_catalog_if_absent,
        latest_snapshot, list_schemas_at, load_table_at, load_view_at,
    };

    #[test]
    fn given_commit_attempt_payload_when_parsed_then_snapshots_and_intents_are_typed() {
        let intent = commit_attempt_intent(
            b"read_snapshot\t7\ncommit_snapshot\t8\nmetadata\tAddColumns\ncolumn\t1\t2\tb\tINTEGER\ttrue\t\t\t\tliteral\nmetadata\tChangeSortKeys\nsort\t1\t4\nsort_field\t1\t4\t0\tb\tduckdb\tASC\tNULLS_LAST\ninline\tRegisterInlineRows\ntable\t1\t0\tducklake_inlined_data_1_0\nrow\t0\ncompaction\tRewriteDeleteFiles\nrewrite\t1\t2\ti:1\ndata_mutation\nfile\t9\t1\tmain/t/file.parquet\t1\t128\t0\n",
        )
        .unwrap();

        assert_eq!(intent.read_snapshot, Some(DuckLakeSnapshotId(7)));
        assert_eq!(intent.proposed_commit_snapshot, CommitAttemptId(8));
        assert_eq!(intent.metadata_intents.len(), 2);
        assert_eq!(
            intent.metadata_intents[0].operation,
            RuntimeMetadataOperation::AddColumns
        );
        assert_eq!(intent.inline_payloads.len(), 1);
        assert_eq!(
            intent.inline_payloads[0].operation,
            RuntimeInlineOperation::RegisterInlineRows
        );
        assert_eq!(intent.compaction_intents.len(), 1);
        assert_eq!(
            intent.compaction_intents[0].operation,
            RuntimeCompactionOperation::RewriteDeleteFiles
        );
        assert!(
            std::str::from_utf8(&intent.data_mutation_payload)
                .unwrap()
                .contains("file\t9\t1")
        );
    }

    #[test]
    fn given_commit_attempt_data_mutation_flush_after_proposed_snapshot_when_parsed_then_rust_moves_commit_snapshot_past_flush()
     {
        let intent = commit_attempt_intent(
            b"read_snapshot\t6\ncommit_snapshot\t7\ndata_mutation\ninline_table\tducklake_inlined_data_10_3\t9\n",
        )
        .unwrap();

        assert_eq!(intent.proposed_commit_snapshot, CommitAttemptId(10));
        assert_eq!(intent.read_snapshot, Some(DuckLakeSnapshotId(6)));
    }

    fn large_data_mutation_payload(rows: usize) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"commit_snapshot\t99\nread_snapshot\t98\n");
        for index in 0..rows {
            payload.extend_from_slice(
                format!("file\t{index}\t1\tmain/t/file-{index}.parquet\t1\t128\t{index}\n")
                    .as_bytes(),
            );
        }
        payload
    }

    #[test]
    fn perf_loop_commit_header_rewrite_reports_allocations() {
        let payload = large_data_mutation_payload(2_000);
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(98)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(99)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: Vec::new(),
            compaction_intents: Vec::new(),
            data_mutation_payload: payload.clone(),
            inline_payloads: Vec::new(),
        };
        let guard = AllocationGuard::start(
            module_path!(),
            "perf_loop_commit_header_rewrite_reports_allocations",
            file!(),
            line!(),
            Some("commit_header_rewrite_2k_rows"),
        );

        let output =
            payload_with_commit_header(&intent, "CommitDataMutation", &payload, true, true)
                .unwrap();

        assert!(output.starts_with(b"commit_snapshot\t99\nread_snapshot\t98\n"));
        assert!(output.ends_with(b"file\t1999\t1\tmain/t/file-1999.parquet\t1\t128\t1999\n"));
        let report = guard.finish();
        emit_report(&report);
    }

    #[test]
    #[ignore]
    fn perf_loop_commit_header_rewrite_reports_cpu() {
        let payload = large_data_mutation_payload(2_000);
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(98)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(99)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: Vec::new(),
            compaction_intents: Vec::new(),
            data_mutation_payload: payload.clone(),
            inline_payloads: Vec::new(),
        };
        let started = Instant::now();
        let output =
            payload_with_commit_header(&intent, "CommitDataMutation", &payload, true, true)
                .unwrap();
        assert!(output.ends_with(b"file\t1999\t1\tmain/t/file-1999.parquet\t1\t128\t1999\n"));
        println!(
            "commit_header_rewrite_2k_rows_ns={}",
            started.elapsed().as_nanos()
        );
    }

    #[test]
    fn given_commit_attempt_payload_with_commit_metadata_when_parsed_then_metadata_is_typed() {
        let intent = commit_attempt_intent(
            b"commit_snapshot\t8\ncommit_author\tducklake-user\ncommit_message\t\ncommit_extra_info\t{\"query_id\":\"audit-1\"}\nmetadata\tCreateTables\ntable\t7\t0\ttable-uuid\tcreated\tmain/created/\t\ncolumn\t7\t0\ta\tINTEGER\ttrue\t\t\t\tNULL\tliteral\n",
        )
        .unwrap();

        assert_eq!(intent.proposed_commit_snapshot, CommitAttemptId(8));
        assert_eq!(
            intent.commit_metadata.author.as_deref(),
            Some("ducklake-user")
        );
        assert_eq!(intent.commit_metadata.commit_message.as_deref(), Some(""));
        assert_eq!(
            intent.commit_metadata.commit_extra_info.as_deref(),
            Some("{\"query_id\":\"audit-1\"}")
        );
    }

    #[test]
    fn given_replace_table_and_schema_intents_when_committed_then_recreated_table_is_visible() {
        let catalog = CatalogId(1);
        let old_schema_id = SchemaId(2);
        let new_schema_id = SchemaId(5);
        let old_table_id = TableId(4);
        let new_table_id = TableId(6);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                old_schema_id,
                "old-schema-uuid",
                "s1",
                "main/s1/",
                crate::CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                old_table_id,
                old_schema_id,
                "old-table-uuid",
                "tbl",
                "main/s1/tbl/",
                vec![TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None)],
                crate::CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();

        let intent = commit_attempt_intent(
            b"commit_snapshot\t11\nread_snapshot\t10\nmetadata\tCreateSchemas\nschema\t5\tnew-schema-uuid\ts1\tmain/s1/\nmetadata\tDropSchemas\nschema\t2\nmetadata\tReplaceTables\ndrop_table\t4\ntable\t6\t5\tnew-table-uuid\ttbl\tmain/s1/tbl/\t\ncolumn\t6\t1\ta\tdate\ttrue\t\t\t\tNULL\tliteral\n",
        )
        .unwrap();

        let result = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let schemas = list_schemas_at(&kv, catalog, latest.order).unwrap();
        let recreated = load_table_at(&kv, catalog, new_table_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(latest.sequence, RawSnapshotSequence(11));
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].schema_id, new_schema_id);
        assert!(
            load_table_at(&kv, catalog, old_table_id, latest.order)
                .unwrap()
                .is_none()
        );
        assert_eq!(recreated.schema_id, new_schema_id);
        assert_eq!(recreated.columns[0].column_type, "date");
        assert_eq!(result.created_tables.len(), 1);
        assert_eq!(result.created_tables[0].persisted.table_id, new_table_id);
    }

    #[test]
    fn given_schema_dropped_after_read_snapshot_when_create_table_commits_then_storage_rejects_it()
    {
        let catalog = CatalogId(1);
        let schema_id = SchemaId(2);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                schema_id,
                "schema-uuid",
                "s1",
                "main/s1/",
                crate::CatalogOrderId::from_u128(0),
            )],
        )
        .unwrap();
        commit_drop_schema_rows(&mut kv, catalog, &[schema_id]).unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t3\nread_snapshot\t1\nmetadata\tCreateTables\ntable\t7\t2\ttable-uuid\ttbl\tmain/s1/tbl/\t\ncolumn\t7\t1\ti\tINT\ttrue\t\t\t\tNULL\tliteral\n",
        )
        .unwrap();

        let error = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap_err();

        assert!(error.to_string().contains("schema 2 no longer exists"));
    }

    #[test]
    fn given_table_created_after_read_snapshot_when_drop_schema_commits_then_storage_rejects_it() {
        let catalog = CatalogId(1);
        let schema_id = SchemaId(2);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                schema_id,
                "schema-uuid",
                "s1",
                "main/s1/",
                crate::CatalogOrderId::from_u128(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(7),
                schema_id,
                "table-uuid",
                "test",
                "main/s1/test/",
                vec![TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None)],
                crate::CatalogOrderId::from_u128(0),
            ),
        )
        .unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t3\nread_snapshot\t1\nmetadata\tDropSchemas\nschema\t2\n",
        )
        .unwrap();

        let error = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap_err();

        assert!(error.to_string().contains("conflict dropping schema 2"));
    }

    #[test]
    fn given_view_comment_changed_after_read_snapshot_when_comment_commits_then_storage_rejects_it()
    {
        let catalog = CatalogId(1);
        let view_id = TableId(31);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_view_row(
            &mut kv,
            catalog,
            ViewRow::new(
                view_id,
                SchemaId(0),
                "view-uuid",
                "comment_view",
                "duckdb",
                "SELECT 42",
                vec!["42".to_owned()],
                crate::CatalogOrderId::from_u128(0),
            ),
        )
        .unwrap();
        commit_change_view_comment(
            &mut kv,
            catalog,
            &crate::ViewCommentChange::new(view_id, Some("con1 comment")),
        )
        .unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t3\nread_snapshot\t1\nmetadata\tChangeViewComment\nview_comment\t31\tvalue\tcon2 comment\n",
        )
        .unwrap();

        let error = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap_err();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let view = load_view_at(&kv, catalog, view_id, latest.order)
            .unwrap()
            .unwrap();

        assert!(
            error
                .to_string()
                .contains("another transaction has altered it")
        );
        assert_eq!(view.comment.as_deref(), Some("con1 comment"));
    }

    #[test]
    fn given_view_created_after_read_snapshot_when_comment_commits_then_storage_saves_comment() {
        let catalog = CatalogId(1);
        let view_id = TableId(31);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_view_row(
            &mut kv,
            catalog,
            ViewRow::new(
                view_id,
                SchemaId(0),
                "view-uuid",
                "renamed_view",
                "duckdb",
                "SELECT 42",
                vec!["42".to_owned()],
                crate::CatalogOrderId::from_u128(0),
            ),
        )
        .unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t3\nread_snapshot\t1\nmetadata\tChangeViewComment\nview_comment\t31\tvalue\tview comment\n",
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let view = load_view_at(&kv, catalog, view_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(view.name, "renamed_view");
        assert_eq!(view.comment.as_deref(), Some("view comment"));
    }

    #[test]
    fn given_view_created_commented_and_renamed_in_one_commit_then_final_view_keeps_comment() {
        let catalog = CatalogId(1);
        let view_id = TableId(31);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t2\nread_snapshot\t1\nmetadata\tCreateViews\nview\t31\t0\tview-uuid\tv\tduckdb\tSELECT 42\t42\t\nmetadata\tChangeViewComment\nview_comment\t31\tvalue\tview comment\nmetadata\tRenameViews\nview\t31\tv2\n",
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let view = load_view_at(&kv, catalog, view_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(view.name, "v2");
        assert_eq!(view.comment.as_deref(), Some("view comment"));
    }

    #[test]
    fn given_new_table_comment_skips_missing_read_snapshot() {
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t2\nread_snapshot\t1\nmetadata\tCreateTables\ntable\t7\t0\ttable-uuid\tcreated\tmain/created/\t\ncolumn\t7\t1\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral\nmetadata\tChangeComments\ntable_comment\t7\tvalue\ttable comment\ncolumn_comment\t7\t1\tvalue\tcolumn comment\n",
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(latest.sequence, RawSnapshotSequence(2));
        assert_eq!(table.comment.as_deref(), Some("table comment"));
        assert_eq!(table.columns[0].comment.as_deref(), Some("column comment"));
    }

    #[test]
    fn given_existing_table_comment_accepts_next_public_snapshot() {
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "table-uuid",
                "created",
                "main/created/",
                vec![TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None)],
                crate::CatalogOrderId::from_u128(0),
            ),
        )
        .unwrap();
        let latest_before = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let next_public_snapshot = latest_before.sequence.0 + 1;
        let intent = commit_attempt_intent(
            format!(
                "commit_snapshot\t{}\nread_snapshot\t{}\nmetadata\tChangeComments\ntable_comment\t7\tvalue\ttable comment\n",
                next_public_snapshot + 1,
                next_public_snapshot
            )
            .as_bytes(),
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(table.comment.as_deref(), Some("table comment"));
    }

    #[test]
    fn given_nested_add_columns_updates_existing_field_when_committed_then_type_is_promoted() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "table-uuid",
                "test",
                "main/test/",
                vec![
                    TableColumnRow::new(ColumnId(1), "col1", "struct", true, None),
                    TableColumnRow::new(ColumnId(3), "j", "struct", true, Some(ColumnId(1))),
                    TableColumnRow::new(ColumnId(4), "c1", "int8", true, Some(ColumnId(3))),
                    TableColumnRow::new(ColumnId(7), "k", "int32", true, Some(ColumnId(1))),
                ],
                crate::CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t2\nread_snapshot\t1\nmetadata\tAddColumns\ncolumn\t1\t4\tc1\tint32\tfalse\t3\t\tNULL\tliteral\ncolumn\t1\t8\tx\tstruct\ttrue\t3\t\tNULL\tliteral\ncolumn\t1\t9\ta\tint32\ttrue\t8\t\tNULL\tliteral\ncolumn\t1\t10\tb\tint32\ttrue\t8\t\tNULL\tliteral\ncolumn\t1\t11\tc\tint32\ttrue\t8\t\tNULL\tliteral\ncolumn\t1\t12\tk\tint32\ttrue\t8\t\tNULL\tliteral\n",
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();

        let c1 = table
            .columns
            .iter()
            .find(|column| column.column_id == ColumnId(4))
            .unwrap();
        assert_eq!(c1.column_type, "int32");
        assert!(!c1.nulls_allowed);
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.column_id == ColumnId(8) && column.name == "x")
        );
        assert_eq!(
            table
                .columns
                .iter()
                .filter(|column| column.parent_id == Some(ColumnId(8)))
                .count(),
            4
        );
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.column_id == ColumnId(7)
                    && column.name == "k"
                    && column.parent_id == Some(ColumnId(1)))
        );
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.column_id == ColumnId(12)
                    && column.name == "k"
                    && column.parent_id == Some(ColumnId(8)))
        );
    }

    #[test]
    fn given_rename_column_with_default_when_committed_then_default_is_preserved_on_new_name() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "table-uuid",
                "t",
                "main/t/",
                vec![TableColumnRow::new(
                    ColumnId(2),
                    "col1",
                    "int32",
                    true,
                    None,
                )],
                crate::CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let intent = commit_attempt_intent(
            b"commit_snapshot\t2\nread_snapshot\t1\nmetadata\tRenameColumns\ncolumn\t1\t2\tcol1_final\tint32\ttrue\t\t\t42\tliteral\n",
        )
        .unwrap();

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        let column = table
            .columns
            .iter()
            .find(|column| column.column_id == ColumnId(2))
            .unwrap();

        assert_eq!(column.name, "col1_final");
        assert_eq!(column.default_value.as_deref(), Some("42"));
        assert_eq!(column.default_value_type, "literal");
    }

    #[test]
    fn given_commit_attempt_payload_with_two_data_mutations_when_parsed_then_rejects_it() {
        let error = commit_attempt_intent(
            b"commit_snapshot\t8\ndata_mutation\nfile\t9\t1\tmain/t/file-a.parquet\t1\t128\t0\ndata_mutation\nfile\t10\t1\tmain/t/file-b.parquet\t1\t128\t1\n",
        )
        .unwrap_err();

        assert!(error.to_string().contains("multiple data_mutation"));
    }

    #[test]
    fn given_data_mutation_section_when_header_is_added_then_commit_snapshot_is_owned_by_attempt() {
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(7)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(8)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: Vec::new(),
            compaction_intents: Vec::new(),
            data_mutation_payload: b"file\t9\t1\tmain/t/file.parquet\t1\t128\t0".to_vec(),
            inline_payloads: Vec::new(),
        };

        let payload = payload_with_commit_header(
            &intent,
            "CommitDataMutation",
            &intent.data_mutation_payload,
            include_read_snapshot_for_storage_intents(&intent),
            true,
        )
        .unwrap();
        let text = std::str::from_utf8(&payload).unwrap();

        assert!(text.starts_with("commit_snapshot\t8\nread_snapshot\t7\n"));
        assert!(text.contains("file\t9\t1\tmain/t/file.parquet\t1\t128\t0"));
    }

    #[test]
    fn given_commit_metadata_when_header_is_added_then_storage_intent_receives_metadata() {
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(7)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(8)),
            commit_metadata: SnapshotCommitMetadata {
                author: Some("ducklake-user".to_owned()),
                commit_message: Some(String::new()),
                commit_extra_info: Some("{\"query_id\":\"audit-1\"}".to_owned()),
            },
            metadata_intents: Vec::new(),
            compaction_intents: Vec::new(),
            data_mutation_payload: b"file\t9\t1\tmain/t/file.parquet\t1\t128\t0".to_vec(),
            inline_payloads: Vec::new(),
        };

        let payload = payload_with_commit_header(
            &intent,
            "CommitDataMutation",
            &intent.data_mutation_payload,
            include_read_snapshot_for_storage_intents(&intent),
            true,
        )
        .unwrap();
        let text = std::str::from_utf8(&payload).unwrap();

        assert!(text.contains("commit_author\tducklake-user\n"));
        assert!(text.contains("commit_message\t\n"));
        assert!(text.contains("commit_extra_info\t{\"query_id\":\"audit-1\"}\n"));
    }

    #[test]
    fn given_metadata_and_data_mutation_when_header_is_added_then_read_snapshot_stays_on_attempt() {
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(7)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(8)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::RenameTables,
                payload: b"table\t1\trenamed".to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: b"file\t9\t1\tmain/t/file.parquet\t1\t128\t0".to_vec(),
            inline_payloads: Vec::new(),
        };

        let payload = payload_with_commit_header(
            &intent,
            "CommitDataMutation",
            &intent.data_mutation_payload,
            include_read_snapshot_for_storage_intents(&intent),
            true,
        )
        .unwrap();
        let text = std::str::from_utf8(&payload).unwrap();

        assert!(text.starts_with("commit_snapshot\t8\n"));
        assert!(!text.contains("read_snapshot\t7\n"));
    }

    #[test]
    fn given_unknown_inline_intent_when_parsed_then_storage_rejects_attempt() {
        let error = RuntimeInlineOperation::parse("UnknownInline").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CommitAttempt does not support inline operation")
        );
    }

    #[test]
    fn given_unknown_compaction_intent_when_parsed_then_storage_rejects_attempt() {
        let error = RuntimeCompactionOperation::parse("UnknownCompaction").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CommitAttempt does not support compaction operation")
        );
    }

    #[test]
    fn given_unknown_metadata_intent_when_parsed_then_storage_rejects_attempt() {
        let error = RuntimeMetadataOperation::parse("UnknownMetadata").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CommitAttempt does not support metadata operation")
        );
    }

    #[test]
    fn given_metadata_commit_with_commit_metadata_when_committed_then_snapshot_returns_metadata() {
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(0)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(1)),
            commit_metadata: SnapshotCommitMetadata {
                author: Some("ducklake-user".to_owned()),
                commit_message: Some(String::new()),
                commit_extra_info: Some("{\"query_id\":\"audit-1\"}".to_owned()),
            },
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::CreateTables,
                payload: b"table\t7\t0\ttable-uuid\tcreated\tmain/created/\t\ncolumn\t7\t0\ta\tINTEGER\ttrue\t\t\t\tNULL\tliteral"
                    .to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(latest.sequence, RawSnapshotSequence(1));
        assert_eq!(latest.created_by, "ducklake-user");
        assert_eq!(latest.commit_message.as_deref(), Some(""));
        assert_eq!(
            latest.commit_extra_info.as_deref(),
            Some("{\"query_id\":\"audit-1\"}")
        );
        assert!(
            load_table_at(&kv, catalog, table_id, latest.order)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn given_schema_and_table_create_intents_when_committed_then_table_is_visible_in_new_schema() {
        let catalog = CatalogId(1);
        let schema_id = SchemaId(10);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(0)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(1)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateSchemas,
                    payload: b"schema\t10\tschema-uuid\ts1\ts1".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateTables,
                    payload: b"table\t7\t10\ttable-uuid\ttbl\ts1/tbl/\t\ncolumn\t7\t0\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 2);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(latest.sequence, RawSnapshotSequence(1));
        let schemas = list_schemas_at(&kv, catalog, latest.order).unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].schema_id, schema_id);
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.schema_id, schema_id);
        assert_eq!(table.name, "tbl");
    }

    #[test]
    fn given_rename_default_and_sort_intents_when_committed_then_storage_writes_one_table_version()
    {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::AddColumns,
                    payload: b"column\t1\t1\tj\tINTEGER\ttrue\t\t\t42\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::RenameColumns,
                    payload: b"column\t1\t0\trenamed_i\tINTEGER\ttrue\t\t\tNULL\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload: b"sort\t1\t9\nsort_field\t1\t9\t0\trenamed_i\tduckdb\tASC\tNULLS_LAST"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(latest.sequence, RawSnapshotSequence(2));
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns[0].name, "renamed_i");
        assert_eq!(table.columns[1].default_value.as_deref(), Some("42"));
        assert_eq!(table.sort.unwrap().sort_id, 9);
    }

    #[test]
    fn given_add_column_and_sort_intents_when_committed_then_current_table_has_final_schema() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::AddColumns,
                    payload: b"column\t1\t2\tk\tINTEGER\ttrue\t\t\tNULL\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload: b"sort\t1\t4\nsort_field\t1\t4\t0\tk\tduckdb\tASC\tNULLS_LAST"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.columns[2].name, "k");
        assert_eq!(table.sort.unwrap().fields[0].expression, "k");
    }

    #[test]
    fn given_type_change_and_sort_intents_when_committed_then_table_returns_one_final_image() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeColumnTypes,
                    payload: b"column\t1\t0\ti\tBIGINT\ttrue\t\t\tNULL\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload: b"sort\t1\t4\nsort_field\t1\t4\t0\ti\tduckdb\tDESC\tNULLS_LAST"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns[0].column_type, "BIGINT");
        assert_eq!(table.sort.unwrap().fields[0].sort_direction, "DESC");
    }

    #[test]
    fn given_table_rename_comment_and_sort_intents_when_committed_then_table_returns_one_final_image()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::RenameTables,
                    payload: b"table\t1\trenamed_table".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeComments,
                    payload: b"table_comment\t1\tvalue\timportant table\ncolumn_comment\t1\t0\tvalue\tprimary value"
                        .to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload:
                        b"sort\t1\t4\nsort_field\t1\t4\t0\ti\tduckdb\tDESC\tNULLS_LAST"
                            .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.name, "renamed_table");
        assert_eq!(table.comment.as_deref(), Some("important table"));
        assert_eq!(table.columns[0].comment.as_deref(), Some("primary value"));
        assert_eq!(table.sort.unwrap().fields[0].sort_direction, "DESC");
    }

    #[test]
    fn given_table_rename_collides_with_existing_table_when_committed_then_storage_rejects_attempt()
    {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(TableId(1))).unwrap();
        let mut other = table(TableId(2));
        other.name = "existing".to_owned();
        other.uuid = "other-uuid".to_owned();
        other.path = "other".to_owned();
        commit_create_table_row(&mut kv, catalog, other).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(2)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(3)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::RenameTables,
                payload: b"table\t1\texisting".to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let error = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap_err();

        assert!(error.to_string().contains("already exists in schema"));
    }

    #[test]
    fn given_create_table_add_column_and_sort_intents_when_committed_then_created_table_has_final_schema()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(7);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(0)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(1)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateTables,
                    payload: b"table\t7\t0\ttable-uuid\tcreated\tmain/created/\t\ncolumn\t7\t0\ta\tINTEGER\ttrue\t\t\t\tNULL\tliteral"
                        .to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::AddColumns,
                    payload: b"column\t7\t1\tb\tINTEGER\ttrue\t\t\tNULL\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload:
                        b"sort\t7\t4\nsort_field\t7\t4\t0\tb\tduckdb\tASC\tNULLS_LAST"
                            .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 1);
        assert_eq!(changed.created_tables.len(), 1);
        assert_eq!(changed.created_tables[0].persisted.table_id, table_id);
        assert_eq!(changed.created_tables[0].persisted.name, "created");
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(latest.sequence, RawSnapshotSequence(1));
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[1].name, "b");
        assert_eq!(table.sort.unwrap().fields[0].expression, "b");
    }

    #[test]
    fn given_created_table_renamed_into_existing_table_name_when_committed_then_storage_returns_final_table_identities()
     {
        let catalog = CatalogId(1);
        let original_table_id = TableId(1);
        let replacement_table_id = TableId(2);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut original = table(original_table_id);
        original.name = "my_table".to_owned();
        original.uuid = "original-table-uuid".to_owned();
        original.path = "main/my_table/".to_owned();
        commit_create_table_row(&mut kv, catalog, original).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateTables,
                    payload: b"table\t2\t0\treplacement-table-uuid\tmy_table_tmp\tmain/my_table_tmp/\t\ncolumn\t2\t0\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral"
                        .to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::RenameTables,
                    payload: b"table\t1\tmy_table_backup\ntable\t2\tmy_table".to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 2);
        assert_eq!(changed.created_tables.len(), 1);
        assert_eq!(
            changed.created_tables[0].persisted.table_id,
            replacement_table_id
        );
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let current_tables = list_tables_at(&kv, catalog, latest.order).unwrap();
        assert_eq!(current_tables.len(), 2);
        assert!(
            current_tables
                .iter()
                .any(|table| table.table_id == original_table_id && table.name == "my_table_backup")
        );
        assert!(
            current_tables
                .iter()
                .any(|table| table.table_id == replacement_table_id && table.name == "my_table")
        );
    }

    #[test]
    fn given_created_table_id_collides_when_committed_then_storage_allocates_persisted_table_id() {
        let catalog = CatalogId(1);
        let requested_table_id = TableId(1);
        let persisted_table_id = TableId(2);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(requested_table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(0)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(1)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::CreateTables,
                payload: b"table\t1\t0\tsecond-table-uuid\ttest2\tmain/test2/\t\ncolumn\t1\t1\ts\tVARCHAR\ttrue\t\t\t\tNULL\tliteral"
                    .to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.created_tables.len(), 1);
        assert_eq!(
            changed.created_tables[0].requested_table_id,
            requested_table_id
        );
        assert_eq!(
            changed.created_tables[0].persisted.table_id,
            persisted_table_id
        );
        assert_eq!(
            changed.table_id_remaps(),
            BTreeMap::from([(requested_table_id, persisted_table_id)])
        );
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let current_tables = list_tables_at(&kv, catalog, latest.order).unwrap();
        assert_eq!(current_tables.len(), 2);
        assert!(
            current_tables
                .iter()
                .any(|table| table.table_id == requested_table_id && table.name == "test")
        );
        assert!(
            current_tables
                .iter()
                .any(|table| table.table_id == persisted_table_id && table.name == "test2")
        );
    }

    #[test]
    fn given_created_table_id_remap_when_payloads_are_rewritten_then_storage_intents_use_persisted_id()
     {
        let remaps = BTreeMap::from([(TableId(1), TableId(2))]);

        let inline = remap_inline_payload(
            RuntimeInlineOperation::RegisterInlineRows,
            b"table\t1\t1\tducklake_inlined_data_1_1\nrow\t0\ts:68656c6c6f\n",
            &remaps,
        )
        .unwrap();
        let compaction = remap_compaction_payload(
            RuntimeCompactionOperation::RewriteDeleteFiles,
            b"source_file\t1\t10\nfile\t0\t1\tmain/test2/rewrite.parquet\t1\t128\t0\nfile_partition\t0\t1\t0\tus\nfile_column_stats\t0\t1\t1\t1\t0\ta\tz\t\n",
            &remaps,
        )
        .unwrap();
        let data = remap_data_mutation_payload(
            b"file\t0\t1\tmain/test2/file.parquet\t1\t128\t0\nfile_partition_set\t0\t1\t3\nfile_partition\t0\t1\t0\tus\nfile_column_stats\t0\t1\t1\t1\t0\ta\tz\t\ninline_table\tducklake_inlined_data_1_1\t1\n",
            &remaps,
        )
        .unwrap();

        let inline = String::from_utf8(inline).unwrap();
        let compaction = String::from_utf8(compaction).unwrap();
        let data = String::from_utf8(data).unwrap();
        assert!(inline.contains("table\t2\t1\tducklake_inlined_data_2_1"));
        assert!(compaction.contains("source_file\t2\t10"));
        assert!(compaction.contains("file\t0\t2\tmain/test2/rewrite.parquet"));
        assert!(compaction.contains("file_partition\t0\t2\t0\tus"));
        assert!(compaction.contains("file_column_stats\t0\t2\t1\t1\t0\ta\tz\t"));
        assert!(data.contains("file\t0\t2\tmain/test2/file.parquet"));
        assert!(data.contains("file_partition_set\t0\t2\t3"));
        assert!(data.contains("file_partition\t0\t2\t0\tus"));
        assert!(data.contains("file_column_stats\t0\t2\t1\t1\t0\ta\tz\t"));
        assert!(data.contains("inline_table\tducklake_inlined_data_2_1\t1"));
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn fdb_live_given_stale_created_table_id_remapped_when_inline_rows_committed_then_remapped_inline_table_is_readable()
     {
        if live_fdb_disabled() {
            return;
        }

        let catalog = CatalogId(902);
        let prefix = unique_prefix("commit-attempt-inline-remap");
        set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
        let first_commit = b"commit_snapshot\t1\nread_snapshot\t0\nmetadata\tCreateTables\ntable\t1\t0\tfirst-table-uuid\ttest\tmain/test/\t\ncolumn\t1\t1\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral\ninline\tRegisterInlineTables\ntable\t1\t1\tducklake_inlined_data_1_1\ninline\tRegisterInlineRows\ntable\t1\t1\tducklake_inlined_data_1_1\nrow\t0\ti:42\n";
        let second_commit = b"commit_snapshot\t1\nread_snapshot\t0\nmetadata\tCreateTables\ntable\t1\t0\tsecond-table-uuid\ttest2\tmain/test2/\t\ncolumn\t1\t1\ts\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\ninline\tRegisterInlineTables\ntable\t1\t1\tducklake_inlined_data_1_1\ninline\tRegisterInlineRows\ntable\t1\t1\tducklake_inlined_data_1_1\nrow\t0\ts:68656c6c6f\n";

        let result = (|| -> CatalogResult<()> {
            let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes())?;
            kv.initialize_catalog_if_absent_versionstamped(catalog)?;
            commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog, first_commit)?;
            let output =
                commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog, second_commit)?;
            let output = String::from_utf8(output)
                .map_err(|error| CatalogError::Decode(error.to_string()))?;
            assert!(output.contains("created_table\t1\t2\t0\ttest2"));

            let latest =
                latest_snapshot(&kv, catalog)?.ok_or(CatalogError::NotFound("snapshot"))?;
            let current_tables = list_tables_at(&kv, catalog, latest.order)?;
            let test2 = current_tables
                .iter()
                .find(|table| table.table_id == TableId(2) && table.name == "test2")
                .ok_or(CatalogError::NotFound("remapped table"))?;
            assert!(
                test2
                    .inlined_data_tables
                    .iter()
                    .any(|inline| inline.table_name == "ducklake_inlined_data_2_1"),
                "remapped table did not persist its remapped inline table: {test2:?}"
            );
            let inline_rows = read_inline_rows_global_stats_payload(
                &kv,
                catalog,
                ReadInlineRowsPayload {
                    table_name: "ducklake_inlined_data_2_1".to_owned(),
                    snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(1))),
                    include_flushed: false,
                    include_deleted: false,
                },
            )?;
            let inline_rows = String::from_utf8(inline_rows)
                .map_err(|error| CatalogError::Decode(error.to_string()))?;
            assert!(inline_rows.contains("row_change\t1\t\t0\ts:68656c6c6f"));
            Ok(())
        })();
        remove_env("AUX_DUCKLAKE_FDB_PREFIX");
        result.unwrap();
    }

    #[test]
    fn given_two_partition_changes_when_committed_then_both_table_partition_facts_are_stored() {
        let catalog = CatalogId(1);
        let first_table_id = TableId(1);
        let second_table_id = TableId(2);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(first_table_id)).unwrap();
        let mut second = table(second_table_id);
        second.name = "test2".to_owned();
        second.uuid = "test2-uuid".to_owned();
        second.path = "test2".to_owned();
        commit_create_table_row(&mut kv, catalog, second).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(2)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(3)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::ChangePartitionKeys,
                payload: b"partition\t1\t11\npartition_field\t1\t11\t0\t0\tidentity\npartition\t2\t22\npartition_field\t2\t22\t0\t0\tidentity"
                    .to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 2);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let first = load_table_at(&kv, catalog, first_table_id, latest.order)
            .unwrap()
            .unwrap();
        let second = load_table_at(&kv, catalog, second_table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(
            first.partition,
            Some(TablePartitionRow::new(
                11,
                vec![TablePartitionFieldRow::new(0, ColumnId(0), "identity")]
            ))
        );
        assert_eq!(
            second.partition,
            Some(TablePartitionRow::new(
                22,
                vec![TablePartitionFieldRow::new(0, ColumnId(0), "identity")]
            ))
        );
    }

    #[test]
    fn given_partition_reset_when_committed_then_table_partition_is_cleared() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut partitioned = table(table_id);
        partitioned.partition = Some(TablePartitionRow::new(
            11,
            vec![TablePartitionFieldRow::new(0, ColumnId(0), "identity")],
        ));
        commit_create_table_row(&mut kv, catalog, partitioned).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::ChangePartitionKeys,
                payload: b"partition\t1\t11".to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(table.partition, None);
    }

    #[test]
    fn given_partition_changed_after_read_snapshot_when_partition_change_commits_then_storage_rejects_it()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();
        let first = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::ChangePartitionKeys,
                payload: b"partition\t1\t11\npartition_field\t1\t11\t0\t0\tidentity".to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };
        commit_metadata_intents_with_kv(&mut kv, catalog, &first).unwrap();
        let second = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(3)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::ChangePartitionKeys,
                payload: b"partition\t1\t12\npartition_field\t1\t12\t0\t1\tidentity".to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let error = commit_metadata_intents_with_kv(&mut kv, catalog, &second).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("table metadata changed after read snapshot")
        );
    }

    #[test]
    fn given_created_table_id_is_remapped_when_partition_change_uses_requested_id_then_storage_commits()
     {
        let catalog = CatalogId(1);
        let requested_table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let mut existing = table(requested_table_id);
        existing.name = "unrelated_table".to_owned();
        existing.uuid = "unrelated-table-uuid".to_owned();
        commit_create_table_row(&mut kv, catalog, existing).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateTables,
                    payload: b"table\t1\t0\tfirst-write-uuid\tfirst_write\tmain/first_write/\t\ncolumn\t1\t0\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral\ncolumn\t1\t1\tsource\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\n"
                        .to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangePartitionKeys,
                    payload: b"partition\t1\t11\npartition_field\t1\t11\t0\t1\tidentity"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let result = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let created = load_table_at(&kv, catalog, TableId(2), latest.order)
            .unwrap()
            .unwrap();

        assert_eq!(result.created_tables.len(), 1);
        assert_eq!(
            result.created_tables[0].requested_table_id,
            requested_table_id
        );
        assert_eq!(result.created_tables[0].persisted.table_id, TableId(2));
        assert_eq!(
            created.partition,
            Some(TablePartitionRow::new(
                11,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")]
            ))
        );
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn fdb_live_given_created_table_renamed_into_existing_table_name_when_committed_then_storage_returns_final_table_identities()
     {
        if live_fdb_disabled() {
            return;
        }

        let catalog = CatalogId(901);
        let original_table_id = TableId(1);
        let replacement_table_id = TableId(2);
        let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(
            unique_prefix("commit-attempt-mixed-create-rename").into_bytes(),
        )
        .unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        kv.create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                original_table_id,
                SchemaId(0),
                "original-table-uuid",
                "my_table",
                "main/my_table/",
                vec![TableColumnRow::new(ColumnId(0), "i", "INTEGER", true, None)],
                crate::CatalogOrderId::uuid_v7(0),
            ),
            Some(RawSnapshotSequence(1)),
        )
        .unwrap();
        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::CreateTables,
                    payload: b"table\t2\t0\treplacement-table-uuid\tmy_table_tmp\tmain/my_table_tmp/\t\ncolumn\t2\t0\ti\tINTEGER\ttrue\t\t\t\tNULL\tliteral"
                        .to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::RenameTables,
                    payload: b"table\t1\tmy_table_backup\ntable\t2\tmy_table".to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };
        let mut runtime_kv = RuntimeMutableCatalog::FoundationDb(kv.clone());

        let changed = commit_metadata_intents_with_kv(&mut runtime_kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 2);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(latest.sequence, RawSnapshotSequence(2));
        let current_tables = list_tables_at(&kv, catalog, latest.order).unwrap();
        assert_eq!(current_tables.len(), 2);
        let backup = load_table_at(&kv, catalog, original_table_id, latest.order)
            .unwrap()
            .unwrap();
        let replacement = load_table_at(&kv, catalog, replacement_table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(backup.name, "my_table_backup");
        assert_eq!(replacement.name, "my_table");
        assert_eq!(backup.validity.begin_order, latest.order);
        assert_eq!(replacement.validity.begin_order, latest.order);
    }

    #[test]
    fn given_drop_add_and_sort_intents_when_committed_then_readded_column_is_not_duplicated() {
        let catalog = CatalogId(1);
        let table_id = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, table(table_id)).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(1)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(2)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::DropColumns,
                    payload: b"column\t1\t1".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::AddColumns,
                    payload: b"column\t1\t2\tb\tINTEGER\ttrue\t\t\tNULL\tliteral".to_vec(),
                },
                RuntimeMetadataIntent {
                    operation: RuntimeMetadataOperation::ChangeSortKeys,
                    payload: b"sort\t1\t4\nsort_field\t1\t4\t0\tb\tduckdb\tDESC\tNULLS_LAST"
                        .to_vec(),
                },
            ],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        assert_eq!(
            table
                .columns
                .iter()
                .filter(|column| column.name == "b")
                .count(),
            1
        );
        assert_eq!(table.sort.unwrap().fields[0].expression, "b");
    }

    #[test]
    fn given_nested_list_columns_when_each_list_has_element_child_then_table_creation_succeeds() {
        let catalog = CatalogId(1);
        let table_id = TableId(9);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        let intent = RuntimeCommitAttemptIntent {
            read_snapshot: Some(DuckLakeSnapshotId(0)),
            proposed_commit_snapshot: ProposedCommitSnapshot::new(CommitAttemptId(1)),
            commit_metadata: SnapshotCommitMetadata::default(),
            metadata_intents: vec![RuntimeMetadataIntent {
                operation: RuntimeMetadataOperation::CreateTables,
                payload: b"table\t9\t0\tnested-types\tdata_types\tmain/data_types/\t\ncolumn\t9\t1\tdate_ar\tDATE[]\ttrue\t\t\t\tNULL\tliteral\ncolumn\t9\t2\telement\tDATE\ttrue\t1\t\t\tNULL\tliteral\ncolumn\t9\t3\tdou_ar\tDOUBLE[]\ttrue\t\t\t\tNULL\tliteral\ncolumn\t9\t4\telement\tDOUBLE\ttrue\t3\t\t\tNULL\tliteral"
                    .to_vec(),
            }],
            compaction_intents: Vec::new(),
            data_mutation_payload: Vec::new(),
            inline_payloads: Vec::new(),
        };

        let changed = commit_metadata_intents_with_kv(&mut kv, catalog, &intent).unwrap();

        assert_eq!(changed.changed_table_count, 1);
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let table = load_table_at(&kv, catalog, table_id, latest.order)
            .unwrap()
            .unwrap();
        let element_columns = table
            .columns
            .iter()
            .filter(|column| column.name == "element")
            .collect::<Vec<_>>();
        assert_eq!(element_columns.len(), 2);
        assert_eq!(element_columns[0].parent_id, Some(ColumnId(1)));
        assert_eq!(element_columns[1].parent_id, Some(ColumnId(3)));
    }

    fn table(table_id: TableId) -> TableRow {
        let mut table = TableRow::with_catalog_metadata(
            table_id,
            SchemaId(0),
            "table-uuid",
            "test",
            "test",
            vec![
                TableColumnRow::new(ColumnId(0), "i", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(1), "j", "INTEGER", true, None),
            ],
            crate::CatalogOrderId::from_u128(0),
        );
        table.sort = Some(TableSortRow::new(
            8,
            vec![TableSortFieldRow::new(
                0,
                "i",
                "duckdb",
                "ASC",
                "NULLS_LAST",
            )],
        ));
        table
    }

    #[cfg(feature = "foundationdb")]
    fn live_fdb_disabled() -> bool {
        if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() == Ok("1") {
            return false;
        }
        eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
        true
    }

    #[cfg(feature = "foundationdb")]
    fn unique_prefix(test_name: &str) -> String {
        format!(
            "aux-ducklake-test/{}/{}/{}",
            test_name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[cfg(feature = "foundationdb")]
    fn set_env(key: &str, value: &str) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[cfg(feature = "foundationdb")]
    fn remove_env(key: &str) {
        unsafe {
            std::env::remove_var(key);
        }
    }
}
