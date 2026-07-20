#[cfg(test)]
mod tests {
    use super::super::{
        ResolvedRuntimeVisibility, affected_table_ids,
        complete_inline_flushes_from_materialized_files, data_mutation_payload_values,
        parse_inlined_table_name, resolve_data_file_visibility,
        resolve_data_file_visibility_orders, resolve_delete_file_visibility_orders,
    };
    use crate::{
        CatalogId, CatalogOrderId, ColumnId, DataFileId, DataMutationInput, DeleteFileId,
        FakeOrderedCatalogKv, InlinedTableRow, SchemaId, SnapshotRow, TableColumnRow, TableId,
        TableRow, commit_create_table_row, commit_data_mutation_with_details,
        initialize_catalog_if_absent, latest_snapshot, register_inline_table_payload_with_table,
        snapshot_changes_made,
    };

    #[test]
    fn given_inline_table_flush_intent_when_parsed_then_rust_derives_typed_ids() {
        let mutation =
            data_mutation_payload_values(b"inline_table\tducklake_inlined_data_10_3\t7\n").unwrap();

        assert_eq!(mutation.inline_flushes.len(), 1);
        assert_eq!(mutation.inline_flushes[0].table_id.0, 10);
        assert_eq!(mutation.inline_flushes[0].schema_id.0, 3);
        assert_eq!(mutation.inline_flushes[0].flush_snapshot_sequence.0, 7);
    }

    #[test]
    fn given_multiple_inline_table_flush_intents_when_parsed_then_each_flush_is_preserved() {
        let mutation = data_mutation_payload_values(
            b"inline_table\tducklake_inlined_data_2_1\t7\n\
              inline_table\tducklake_inlined_data_3_1\t8\n",
        )
        .unwrap();

        assert_eq!(mutation.inline_flushes.len(), 2);
        assert_eq!(mutation.inline_flushes[0].table_id.0, 2);
        assert_eq!(mutation.inline_flushes[0].schema_id.0, 1);
        assert_eq!(mutation.inline_flushes[0].flush_snapshot_sequence.0, 7);
        assert_eq!(mutation.inline_flushes[1].table_id.0, 3);
        assert_eq!(mutation.inline_flushes[1].schema_id.0, 1);
        assert_eq!(mutation.inline_flushes[1].flush_snapshot_sequence.0, 8);
    }

    #[test]
    fn given_inline_flush_after_proposed_snapshot_when_resolving_then_commit_snapshot_moves_past_flush()
     {
        let mut mutation = data_mutation_payload_values(
            b"commit_snapshot\t7\ninline_table\tducklake_inlined_data_10_3\t9\n",
        )
        .unwrap();

        mutation.resolve_proposed_commit_snapshot_from_inline_flushes();

        assert_eq!(
            mutation
                .proposed_commit_snapshot
                .unwrap()
                .commit_attempt_id()
                .0,
            10
        );
    }

    #[test]
    fn given_inline_flush_before_proposed_snapshot_when_resolving_then_commit_snapshot_is_preserved()
     {
        let mut mutation = data_mutation_payload_values(
            b"commit_snapshot\t12\ninline_table\tducklake_inlined_data_10_3\t9\n",
        )
        .unwrap();

        mutation.resolve_proposed_commit_snapshot_from_inline_flushes();

        assert_eq!(
            mutation
                .proposed_commit_snapshot
                .unwrap()
                .commit_attempt_id()
                .0,
            12
        );
    }

    #[test]
    fn given_no_inline_flush_when_resolving_then_commit_snapshot_is_preserved() {
        let mut mutation = data_mutation_payload_values(
            b"commit_snapshot\t8\nfile\t9\t1\tmain/t/file.parquet\t1\t128\t0\n",
        )
        .unwrap();

        mutation.resolve_proposed_commit_snapshot_from_inline_flushes();

        assert_eq!(
            mutation
                .proposed_commit_snapshot
                .unwrap()
                .commit_attempt_id()
                .0,
            8
        );
    }

    #[test]
    fn given_file_visibility_intent_when_parsed_then_rust_keeps_public_snapshot_ids() {
        let mutation = data_mutation_payload_values(
            b"file\t9\t10\tmain/orders/flushed.parquet\t3\t1024\t0\t\t151\t4\t6\n",
        )
        .unwrap();

        assert_eq!(mutation.data_files.len(), 1);
        assert_eq!(mutation.data_files[0].data_file_id.0, 9);
        assert_eq!(mutation.data_files[0].footer_size, Some(151));
        assert_eq!(mutation.data_file_visibility.len(), 1);
        assert_eq!(mutation.data_file_visibility[0].data_file_id.0, 9);
        assert_eq!(mutation.data_file_visibility[0].begin_snapshot.0, 4);
        assert_eq!(
            mutation.data_file_visibility[0]
                .max_partial_snapshot
                .unwrap()
                .0,
            6
        );
    }

    #[test]
    fn given_encrypted_data_file_when_parsed_then_encryption_key_is_preserved() {
        let mutation = data_mutation_payload_values(
            b"file\t9\t10\tmain/orders/encrypted.parquet\t3\t1024\t0\t\t151\t4\t6\tAQIDBA==\n",
        )
        .unwrap();

        assert!(
            format!("{:?}", mutation.data_files[0]).contains("AQIDBA=="),
            "parsed data-file metadata must retain the per-file encryption key"
        );
    }

    #[test]
    fn given_encrypted_delete_file_when_parsed_then_encryption_key_is_preserved() {
        let mutation = data_mutation_payload_values(
            b"delete_file\t30\t10\t9\tmain/orders/delete.parquet\t1\t64\t4\t6\tBQYHCA==\n",
        )
        .unwrap();

        assert!(
            format!("{:?}", mutation.materialized_delete_files[0].row()).contains("BQYHCA=="),
            "parsed delete-file metadata must retain the per-file encryption key"
        );
    }

    #[test]
    fn given_data_file_visibility_snapshot_ids_when_resolved_then_catalog_orders_are_typed() {
        let catalog = CatalogId(1);
        let (kv, snapshot) = catalog_with_single_table_snapshot(catalog, TableId(10));
        let payload = format!(
            "file\t9\t10\tmain/orders/flushed.parquet\t3\t1024\t0\t\t151\t{}\t{}\n",
            snapshot.sequence.0, snapshot.sequence.0
        );
        let mutation = data_mutation_payload_values(payload.as_bytes()).unwrap();

        let resolved = resolve_data_file_visibility_orders(
            &kv,
            catalog,
            mutation.data_file_visibility[0],
            None,
        )
        .unwrap();

        assert_eq!(
            resolved,
            ResolvedRuntimeVisibility {
                begin_order: snapshot.order,
                max_partial_order: Some(snapshot.order),
            }
        );
    }

    #[test]
    fn given_delete_file_visibility_snapshot_ids_when_resolved_then_catalog_orders_are_typed() {
        let catalog = CatalogId(1);
        let (kv, snapshot) = catalog_with_single_table_snapshot(catalog, TableId(10));
        let payload = format!(
            "delete_file\t30\t10\t9\tmain/orders/delete.parquet\t1\t64\t{}\t{}\n",
            snapshot.sequence.0, snapshot.sequence.0
        );
        let mutation = data_mutation_payload_values(payload.as_bytes()).unwrap();

        assert_eq!(
            mutation.delete_file_visibility[0]
                .begin_snapshot
                .public_id()
                .0,
            snapshot.sequence.0
        );
        let resolved = resolve_delete_file_visibility_orders(
            &kv,
            catalog,
            mutation.delete_file_visibility[0],
            None,
        )
        .unwrap();

        assert_eq!(
            resolved,
            ResolvedRuntimeVisibility {
                begin_order: snapshot.order,
                max_partial_order: Some(snapshot.order),
            }
        );
    }

    #[test]
    fn given_delete_max_partial_snapshot_is_current_commit_when_resolved_then_order_is_versionstamped()
     {
        let catalog = CatalogId(1);
        let (kv, snapshot) = catalog_with_single_table_snapshot(catalog, TableId(10));
        let proposed_sequence = snapshot.sequence.0 + 1;
        let payload = format!(
            "commit_snapshot\t{proposed_sequence}\ndelete_file\t30\t10\t9\tmain/orders/delete.parquet\t1\t64\t{}\t{proposed_sequence}\n",
            snapshot.sequence.0
        );
        let mutation = data_mutation_payload_values(payload.as_bytes()).unwrap();

        let resolved = resolve_delete_file_visibility_orders(
            &kv,
            catalog,
            mutation.delete_file_visibility[0],
            mutation.proposed_commit_snapshot,
        )
        .unwrap();

        assert_eq!(resolved.begin_order, snapshot.order);
        assert_eq!(
            resolved.max_partial_order,
            Some(crate::ids::incomplete_fdb_order())
        );
    }

    #[test]
    fn given_delete_file_without_visibility_when_parsed_then_no_historical_begin_is_invented() {
        let mutation = data_mutation_payload_values(
            b"commit_snapshot\t3\nread_snapshot\t2\ndelete_file\t3\t1\t0\tmain/t/delete.parquet\t1\t778\t\n",
        )
        .unwrap();

        assert_eq!(mutation.materialized_delete_files.len(), 1);
        assert_eq!(
            mutation.materialized_delete_files[0]
                .row()
                .validity
                .begin_order,
            CatalogOrderId::uuid_v7(0)
        );
        assert!(mutation.materialized_delete_files[0].is_historical_delete_file());
        assert!(mutation.delete_file_visibility.is_empty());
    }

    #[test]
    fn given_materialized_inline_file_without_flush_marker_when_committing_then_storage_records_inline_flush()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();

        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                schema_id,
                "orders-uuid",
                "orders",
                "main/orders",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();

        let mut table_with_inline = TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            "orders-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                true,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        table_with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_with_inline,
            schema_id,
            b"row\t1\ti:1\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        let payload = format!(
            "file\t20\t10\tmain/orders/flushed.parquet\t1\t128\t0\t\t\t{}\t{}\n",
            inline_snapshot.sequence.0, inline_snapshot.sequence.0
        );
        let mut mutation = data_mutation_payload_values(payload.as_bytes()).unwrap();

        resolve_data_file_visibility(&kv, catalog, &mut mutation).unwrap();
        complete_inline_flushes_from_materialized_files(&kv, catalog, &mut mutation).unwrap();

        assert_eq!(mutation.inline_flushes.len(), 1);
        assert_eq!(mutation.inline_flushes[0].table_id, table_id);
        assert_eq!(mutation.inline_flushes[0].schema_id, schema_id);
        assert_eq!(
            mutation.inline_flushes[0].flush_snapshot_sequence,
            inline_snapshot.sequence
        );

        commit_data_mutation_with_details(
            &mut kv,
            catalog,
            DataMutationInput {
                data_files: mutation.data_files,
                inline_flushes: mutation.inline_flushes,
                ..DataMutationInput::default()
            },
        )
        .unwrap();
        let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        assert_eq!(
            snapshot_changes_made(&kv, catalog, flush_snapshot.order).unwrap(),
            "flushed_inlined:10"
        );
    }

    #[test]
    fn given_materialized_inline_file_has_delete_file_when_completing_flush_then_delete_file_role_is_typed()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(1);
        let data_file_id = DataFileId(20);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                schema_id,
                "orders-uuid",
                "orders",
                "main/orders",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let mut table_with_inline = TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            "orders-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                true,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        table_with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            table_with_inline,
            schema_id,
            b"row\t1\ti:1\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let payload = format!(
            "file\t{}\t10\tmain/orders/flushed.parquet\t1\t128\t0\t\t\t{}\t{}\n\
             delete_file\t30\t10\t{}\tmain/orders/flushed-delete.parquet\t1\t64\t\t\n",
            data_file_id.0, inline_snapshot.sequence.0, inline_snapshot.sequence.0, data_file_id.0
        );
        let mut mutation = data_mutation_payload_values(payload.as_bytes()).unwrap();

        resolve_data_file_visibility(&kv, catalog, &mut mutation).unwrap();
        complete_inline_flushes_from_materialized_files(&kv, catalog, &mut mutation).unwrap();

        assert!(mutation.materialized_delete_files[0].materializes_inline_deletes());
        assert_eq!(
            mutation.materialized_delete_files[0].row().delete_file_id,
            DeleteFileId(30)
        );
    }

    #[test]
    fn given_direct_data_mutation_with_commit_metadata_when_parsed_then_metadata_is_typed() {
        let mutation = data_mutation_payload_values(
            b"commit_snapshot\t3\ncommit_author\tPedro\ncommit_message\t\ncommit_extra_info\t{\"query_id\":\"audit-1\"}\nfile\t9\t10\tmain/orders/file.parquet\t3\t1024\t0\n",
        )
        .unwrap();

        assert_eq!(
            mutation
                .proposed_commit_snapshot
                .unwrap()
                .commit_attempt_id()
                .0,
            3
        );
        assert_eq!(mutation.commit_metadata.author.as_deref(), Some("Pedro"));
        assert_eq!(mutation.commit_metadata.commit_message.as_deref(), Some(""));
        assert_eq!(
            mutation.commit_metadata.commit_extra_info.as_deref(),
            Some("{\"query_id\":\"audit-1\"}")
        );
        assert_eq!(mutation.data_files.len(), 1);
    }

    #[test]
    fn given_mixed_data_mutation_when_collecting_affected_tables_then_only_plain_appends_are_reported()
     {
        let table = TableId(10);
        let mutation = data_mutation_payload_values(
            b"file\t1\t10\tmain/orders/plain.parquet\t10\t512\t0\n\
              file\t2\t11\tmain/orders/flushed.parquet\t10\t512\t0\t\t\t4\t6\n\
              delete_file\t3\t10\t1\tmain/orders/delete.parquet\t10\t512\t\n\
              dropped_data_file\t1\n",
        )
        .unwrap();

        assert_eq!(affected_table_ids(&mutation).unwrap(), vec![table]);
    }

    #[test]
    fn given_malformed_inline_table_name_when_parsed_then_intent_is_rejected() {
        let error = parse_inlined_table_name("ducklake_inlined_data_10_3_extra").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CommitDataMutation inline table name is invalid")
        );
    }

    fn catalog_with_single_table_snapshot(
        catalog: CatalogId,
        table_id: TableId,
    ) -> (FakeOrderedCatalogKv, SnapshotRow) {
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(1),
                "orders-uuid",
                "orders",
                "main/orders",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        (kv, snapshot)
    }
}
