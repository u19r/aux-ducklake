#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        CatalogError, CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId,
        DeleteFileRow, DuckLakeSnapshotId, FakeOrderedCatalogKv, InlineTableFlush, InlinedTableRow,
        MutableCatalogKv, OrderedCatalogKv, RangeDirection, RangeItem, RawSnapshotSequence,
        SchemaId, TableColumnRow, TableId, TableRow, TableVersionReplacement, append_data_file,
        commit_create_table_row, commit_data_mutation, commit_delete_inline_table_rows,
        commit_delete_inline_table_rows_at_snapshot, initialize_catalog_if_absent,
        keys::snapshot_timestamp_prefix,
        latest_snapshot, public_snapshot_sequence_for_order, register_delete_file,
        register_inline_table_payload_with_table,
        runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot, ReadSnapshot},
    };

    use super::super::{
        InlineFlushDeletePositionsPayload, InlineFlushPartitionFilter, InlineFlushRow,
        InlineGeometryStats, InlineRowChangesPayload, InlineStatsMode, InlineStatsRequest,
        InlineTableName, LiveColumnStats, ReadInlineRowsPayload,
        inline_flush_delete_positions_payload, inline_read_snapshot, inline_row_changes_payload,
        load_inlined_table, read_inline_rows_aggregate_stats_payload,
        read_inline_rows_global_stats_payload, read_inline_rows_payload,
        read_inline_rows_payload_with_stats_request_and_mode,
    };

    #[test]
    fn given_future_global_stats_snapshot_when_resolving_then_latest_snapshot_is_returned_without_history_scan()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let schema_id = SchemaId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "orders", schema_id),
        )
        .unwrap();
        let mut with_inline = created;
        with_inline.inlined_data_tables.push(InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            schema_id.0,
        ));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(kv, catalog);

        let resolved = inline_read_snapshot(
            &kv,
            catalog,
            Some(ReadSnapshot::new(DuckLakeSnapshotId(latest.sequence.0 + 8))),
            InlineStatsRequest::Global,
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved.order, latest.order);
        assert_eq!(kv.snapshot_timestamp_scan_count(), 0);
    }

    #[test]
    fn given_global_stats_requested_for_transaction_local_inline_table_when_table_missing_then_empty_stats_are_returned()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(TableId(1), "test2", SchemaId(1)),
        )
        .unwrap();
        let mut with_inline = created;
        with_inline.inlined_data_tables.push(InlinedTableRow::new(
            "ducklake_inlined_data_1_1",
            SchemaId(1).0,
        ));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            SchemaId(1),
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let latest_public = public_snapshot_sequence_for_order(&kv, catalog, latest.order)
            .unwrap()
            .unwrap()
            .0;

        let output = read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: "ducklake_inlined_data_2_3".to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(latest_public))),
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("inline_payload_count=0"), "{output}");
        assert!(output.contains("inline_table_stats\t0\t0"), "{output}");
        assert!(matches!(
            read_inline_rows_payload(
                &kv,
                catalog,
                ReadInlineRowsPayload {
                    table_name: "ducklake_inlined_data_2_3".to_owned(),
                    snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(latest_public))),
                    include_flushed: true,
                    include_deleted: false,
                },
            ),
            Err(CatalogError::NotFound(_))
        ));
    }

    #[test]
    fn given_inline_table_name_when_resolving_table_then_encoded_ids_define_identity() {
        let main_schema = SchemaId(0);
        let s1_schema = SchemaId(1);
        let main_table = TableId(10);
        let s1_table = TableId(11);
        let mut main = table_with_inline_schema(main_table, "test", main_schema);
        main.inlined_data_tables.push(InlinedTableRow::new(
            "ducklake_inlined_data_10_0",
            main_schema.0,
        ));
        let mut s1 = table_with_inline_schema(s1_table, "test", s1_schema);
        s1.inlined_data_tables.push(InlinedTableRow::new(
            "ducklake_inlined_data_11_1",
            s1_schema.0,
        ));

        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, s1).unwrap();
        commit_create_table_row(&mut kv, catalog, main).unwrap();
        let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let inline_table = InlineTableName::parse("ducklake_inlined_data_10_0");
        let (resolved, schema_id) =
            load_inlined_table(&kv, catalog, snapshot.order, inline_table).unwrap();

        assert_eq!(resolved.table_id, main_table);
        assert_eq!(schema_id, main_schema);
    }

    #[test]
    fn given_row_id_reinserted_at_delete_snapshot_when_reading_inline_rows_then_replacement_survives()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(42);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_versions_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_versions", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:1\nrow\t1\ti:2\n".to_vec(),
        )
        .unwrap();

        commit_delete_inline_table_rows(&mut kv, catalog, table_id, schema_id, &[0]).unwrap();
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:101\n".to_vec(),
            DuckLakeSnapshotId(4),
        );
        let replacement = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let replacement_public =
            public_snapshot_sequence_for_order(&kv, catalog, replacement.order)
                .unwrap()
                .unwrap()
                .0;

        let output = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(4))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(!output.contains("row_change\t2\t\t0\ti:1"));
        assert!(output.contains("row_change\t2\t\t1\ti:2"));
        assert!(output.contains(&format!("row_change\t{replacement_public}\t\t0\ti:101")));
    }

    #[test]
    fn given_inline_rows_committed_after_schema_registration_when_reading_later_snapshot_then_rows_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(43);
        let schema_id = SchemaId(3);
        let table_name = "ducklake_inlined_data_43_3";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush", schema_id),
        )
        .unwrap();

        let mut registered = created.clone();
        registered
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            registered,
            schema_id,
            b"row\t0\ti:42\n".to_vec(),
            DuckLakeSnapshotId(2),
        );

        let output = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(2))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("row_change\t2\t\t0\ti:42"));
    }

    #[test]
    fn given_deleted_inline_rows_when_flushing_then_delete_positions_are_returned_in_output_order()
    {
        let catalog = CatalogId(1);
        let table_id = TableId(46);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_46_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush_deletes", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            inline_payload_for_ids(0..20),
        )
        .unwrap();
        let after_insert = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(after_insert.sequence.0, 2);

        let row_ids = (0..20).collect::<Vec<_>>();
        commit_delete_inline_table_rows(&mut kv, catalog, table_id, schema_id, &row_ids).unwrap();

        let normal_read = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let normal_read = String::from_utf8(normal_read).unwrap();
        assert!(!normal_read.contains("row_change\t2\t\t0\ti:0"));

        let flush_read = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: true,
                include_deleted: true,
            },
        )
        .unwrap();
        let flush_read = String::from_utf8(flush_read).unwrap();
        assert!(
            flush_read.contains("row_change\t2\t\t0\ti:0"),
            "{flush_read}"
        );

        let output = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(3)),
                file_order: None,
                partition_filter: None,
                position_start: None,
                position_end: None,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        let expected = format!(
            "delete_position_count=20\n{}",
            (0..20)
                .map(|position| format!("delete_position\t3\t{position}\n"))
                .collect::<String>()
        );
        assert_eq!(output, expected);
    }

    #[test]
    fn given_sequential_inline_updates_when_reading_rows_for_flush_then_history_and_delete_positions_match()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(49);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_49_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush_versions", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:0\nrow\t1\ti:0\n".to_vec(),
        )
        .unwrap();

        for snapshot_id in 3..=5 {
            commit_delete_inline_table_rows_at_snapshot(
                &mut kv,
                catalog,
                table_id,
                schema_id,
                &[0, 1],
                Some(DuckLakeSnapshotId(snapshot_id)),
            )
            .unwrap();
            let value = snapshot_id - 2;
            register_inline_table_payload_with_table_at_snapshot_for_test(
                &mut kv,
                catalog,
                with_inline.clone(),
                schema_id,
                format!("row\t0\ti:{value}\nrow\t1\ti:{value}\n").into_bytes(),
                DuckLakeSnapshotId(snapshot_id),
            );
        }

        let flush_read = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(5))),
                include_flushed: true,
                include_deleted: true,
            },
        )
        .unwrap();
        let flush_read = String::from_utf8(flush_read).unwrap();

        assert!(
            flush_read.contains("row_change\t2\t\t0\ti:0"),
            "{flush_read}"
        );
        assert!(
            flush_read.contains("row_change\t3\t\t0\ti:1"),
            "{flush_read}"
        );
        assert!(
            flush_read.contains("row_change\t4\t\t0\ti:2"),
            "{flush_read}"
        );
        assert!(
            flush_read.contains("row_change\t5\t\t0\ti:3"),
            "{flush_read}"
        );
        assert!(
            flush_read.contains("row_change\t5\t\t1\ti:3"),
            "{flush_read}"
        );

        let delete_positions = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(5)),
                file_order: None,
                partition_filter: None,
                position_start: None,
                position_end: None,
            },
        )
        .unwrap();
        let delete_positions = String::from_utf8(delete_positions).unwrap();

        assert_eq!(
            delete_positions,
            "delete_position_count=6\n\
delete_position\t3\t0\n\
delete_position\t4\t1\n\
delete_position\t5\t2\n\
delete_position\t3\t4\n\
delete_position\t4\t5\n\
delete_position\t5\t6\n"
        );
    }

    #[test]
    fn given_sorted_inline_updates_when_listing_flush_delete_positions_then_positions_match_file_order()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(50);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_50_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush_sorted_versions", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:2\ti:0\nrow\t1\ti:1\ti:0\nrow\t2\ti:3\ti:0\n".to_vec(),
        )
        .unwrap();

        for snapshot_id in 3..=5 {
            commit_delete_inline_table_rows_at_snapshot(
                &mut kv,
                catalog,
                table_id,
                schema_id,
                &[0],
                Some(DuckLakeSnapshotId(snapshot_id)),
            )
            .unwrap();
            let value = snapshot_id - 2;
            register_inline_table_payload_with_table_at_snapshot_for_test(
                &mut kv,
                catalog,
                with_inline.clone(),
                schema_id,
                format!("row\t0\ti:2\ti:{value}\n").into_bytes(),
                DuckLakeSnapshotId(snapshot_id),
            );
        }

        let delete_positions = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(5)),
                file_order: Some(
                    "id ASC NULLS LAST, row_id ASC NULLS LAST, begin_snapshot ASC NULLS LAST"
                        .to_owned(),
                ),
                partition_filter: None,
                position_start: None,
                position_end: None,
            },
        )
        .unwrap();
        let delete_positions = String::from_utf8(delete_positions).unwrap();

        assert_eq!(
            delete_positions,
            "delete_position_count=3\n\
delete_position\t3\t1\n\
delete_position\t4\t2\n\
delete_position\t5\t3\n"
        );
    }

    #[test]
    fn given_sorted_flush_window_when_deleted_row_sorts_outside_window_then_count_matches_positions()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(54);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_54_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush_sorted_window", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:2\nrow\t1\ti:1\nrow\t2\ti:3\n".to_vec(),
        )
        .unwrap();
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[0],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();

        let delete_positions = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(3)),
                file_order: Some(
                    "id ASC NULLS LAST, row_id ASC NULLS LAST, begin_snapshot ASC NULLS LAST"
                        .to_owned(),
                ),
                partition_filter: None,
                position_start: Some(0),
                position_end: Some(1),
            },
        )
        .unwrap();
        let delete_positions = String::from_utf8(delete_positions).unwrap();

        assert_eq!(delete_positions, "delete_position_count=0\n");
    }

    #[test]
    fn given_sorted_inline_updates_when_file_order_uses_square_expression_then_positions_match_file_order()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(51);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_51_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(
                table_id,
                "runtime_inline_flush_sorted_expression",
                schema_id,
            ),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:2\nrow\t1\ti:1\nrow\t2\ti:3\n".to_vec(),
        )
        .unwrap();

        for snapshot_id in 3..=5 {
            commit_delete_inline_table_rows_at_snapshot(
                &mut kv,
                catalog,
                table_id,
                schema_id,
                &[0],
                Some(DuckLakeSnapshotId(snapshot_id)),
            )
            .unwrap();
            register_inline_table_payload_with_table_at_snapshot_for_test(
                &mut kv,
                catalog,
                with_inline.clone(),
                schema_id,
                b"row\t0\ti:2\n".to_vec(),
                DuckLakeSnapshotId(snapshot_id),
            );
        }

        let delete_positions = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(5)),
                file_order: Some(
                    "(id * id) DESC NULLS LAST, row_id ASC NULLS LAST, begin_snapshot ASC NULLS LAST"
                        .to_owned(),
                ),
                partition_filter: None,
                position_start: None,
                position_end: None,
            },
        )
        .unwrap();
        let delete_positions = String::from_utf8(delete_positions).unwrap();

        assert_eq!(
            delete_positions,
            "delete_position_count=3\n\
delete_position\t3\t1\n\
delete_position\t4\t2\n\
delete_position\t5\t3\n"
        );
    }

    #[test]
    fn given_partition_filter_when_listing_flush_delete_positions_then_positions_are_partition_local()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(52);
        let schema_id = SchemaId(0);
        let table_name = "ducklake_inlined_data_52_0";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            partitioned_inline_schema(table_id, "runtime_inline_flush_partitioned", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:1\ts:65617374\ti:100\n\
row\t1\ti:2\ts:77657374\ti:200\n\
row\t2\ti:3\ts:77657374\ti:300\n\
row\t3\ti:4\ts:63656e7472616c\ti:400\n"
                .to_vec(),
        )
        .unwrap();
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[1],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t4\ti:2\ts:77657374\ti:250\n".to_vec(),
            DuckLakeSnapshotId(3),
        );

        let delete_positions = inline_flush_delete_positions_payload(
            &kv,
            catalog,
            InlineFlushDeletePositionsPayload {
                table_name: table_name.to_owned(),
                snapshot: ReadSnapshot::new(DuckLakeSnapshotId(3)),
                file_order: None,
                partition_filter: Some("CAST(region AS VARCHAR) = 'west'".to_owned()),
                position_start: Some(0),
                position_end: Some(2),
            },
        )
        .unwrap();
        let delete_positions = String::from_utf8(delete_positions).unwrap();

        assert_eq!(
            delete_positions,
            "delete_position_count=1\n\
delete_position\t3\t0\n"
        );
    }

    #[test]
    fn given_transform_partition_filter_when_matching_flush_row_then_cast_argument_is_unwrapped() {
        let table = timestamp_inline_schema(
            TableId(53),
            "runtime_inline_flush_transform_filter",
            SchemaId(0),
        );
        let filter =
            InlineFlushPartitionFilter::parse(Some("year(CAST(ts AS TIMESTAMP)) = 2021"), &table)
                .unwrap();
        let row = InlineFlushRow {
            row_id: 0,
            begin_snapshot: 1,
            end_snapshot: None,
            values: vec!["i:1".to_owned(), "v:323032312d30362d3135".to_owned()],
        };

        assert!(filter.matches(&row));
    }

    #[test]
    fn given_inline_rows_flushed_when_listing_changes_at_flush_snapshot_then_original_insertions_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(45);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_flush_cdf_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_flush_cdf", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        for snapshot_id in 2..=11 {
            register_inline_table_payload_with_table_at_snapshot_for_test(
                &mut kv,
                catalog,
                with_inline.clone(),
                schema_id,
                format!("row\t{}\ti:{}\n", snapshot_id - 2, snapshot_id - 2).into_bytes(),
                DuckLakeSnapshotId(snapshot_id),
            );
        }
        let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(flush_snapshot.sequence.0, 11);
        commit_data_mutation(
            &mut kv,
            catalog,
            Vec::new(),
            Vec::new(),
            &[InlineTableFlush::new(
                table_id,
                schema_id,
                flush_snapshot.sequence,
            )],
        )
        .unwrap();
        assert_eq!(
            latest_snapshot(&kv, catalog).unwrap().unwrap().sequence.0,
            12
        );
        let flush = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let flush_public = public_snapshot_sequence_for_order(&kv, catalog, flush.order)
            .unwrap()
            .unwrap()
            .0;

        let insertions = inline_row_changes_payload(
            &kv,
            catalog,
            inline_row_changes_payload_request(table_name, flush_public, flush_public),
            crate::InlineRowChangeKind::Inserted,
        )
        .unwrap();
        let insertions = String::from_utf8(insertions).unwrap();

        assert!(
            insertions.contains("inline_row_change_count=1"),
            "{insertions}"
        );
        assert!(
            insertions.contains(&format!("row_change\t{flush_public}\t\t9\ti:9")),
            "{insertions}"
        );
    }

    #[test]
    fn given_inline_update_committed_at_one_snapshot_when_listing_changes_then_update_parts_are_returned()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(44);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_update_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_update", schema_id),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:1\n".to_vec(),
        )
        .unwrap();
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:101\n".to_vec(),
            DuckLakeSnapshotId(3),
        );
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[0],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();

        let visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();
        assert!(!visible.contains("row_change\t2\t\t0\ti:1"));
        assert!(visible.contains("row_change\t3\t\t0\ti:101"));
        assert!(visible.contains("inline_table_stats\t2\t1"));
        assert!(visible.contains("inline_column_stats\t1\ttrue\tfalse\ttrue\t1\ttrue\t101"));

        let insertions = inline_row_changes_payload(
            &kv,
            catalog,
            inline_row_changes_payload_request(table_name, 3, 3),
            crate::InlineRowChangeKind::Inserted,
        )
        .unwrap();
        let insertions = String::from_utf8(insertions).unwrap();
        assert!(insertions.contains("row_change\t3\t\t0\ti:101"));

        let deletions = inline_row_changes_payload(
            &kv,
            catalog,
            inline_row_changes_payload_request(table_name, 3, 3),
            crate::InlineRowChangeKind::Deleted,
        )
        .unwrap();
        let deletions = String::from_utf8(deletions).unwrap();
        assert!(deletions.contains("row_change\t\t3\t0\ti:1"));
    }

    #[test]
    fn given_inline_struct_values_when_reading_global_stats_then_child_columns_have_min_max() {
        let catalog = CatalogId(1);
        let table_id = TableId(50);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_struct_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            nested_stats_table(table_id, "runtime_inline_struct_stats", schema_id, "struct"),
        )
        .unwrap();

        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            format!(
                "row\t0\t{}\nrow\t1\t{}\nrow\t2\tn:\n",
                encoded_duckdb_text("{'i': 1, 'j': 2}"),
                encoded_duckdb_text("{'i': NULL, 'j': 3}")
            )
            .into_bytes(),
        )
        .unwrap();

        let output = read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: None,
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(
            output.contains("inline_column_stats\t2\ttrue\ttrue\ttrue\t1\ttrue\t1"),
            "{output}"
        );
        assert!(
            output.contains("inline_column_stats\t3\ttrue\ttrue\ttrue\t2\ttrue\t3"),
            "{output}"
        );
    }

    #[test]
    fn given_inline_rows_when_reading_aggregate_stats_then_rust_returns_query_shape_stats() {
        let catalog = CatalogId(1);
        let table_id = TableId(50);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_aggregate_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_aggregate_stats", schema_id),
        )
        .unwrap();

        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:2\nrow\t1\tn:\nrow\t2\ti:10\n".to_vec(),
        )
        .unwrap();

        let output = read_inline_rows_aggregate_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: None,
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("inline_aggregate_stats\t3"), "{output}");
        assert!(
            output.contains("inline_aggregate_column_stats\t1\t2\ttrue\t2\ttrue\t10"),
            "{output}"
        );
    }

    #[test]
    fn given_deleted_inline_row_when_reading_aggregate_stats_then_min_max_remain_exact_visible() {
        let catalog = CatalogId(1);
        let table_id = TableId(51);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_aggregate_deleted_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(
                table_id,
                "runtime_inline_aggregate_deleted_stats",
                schema_id,
            ),
        )
        .unwrap();

        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:1\nrow\t1\tn:\nrow\t2\ti:10\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[0],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:101\n".to_vec(),
            DuckLakeSnapshotId(3),
        );

        let output = read_inline_rows_aggregate_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("inline_aggregate_stats\t3"), "{output}");
        assert!(
            output.contains("inline_aggregate_column_stats\t1\t2\ttrue\t10\ttrue\t101"),
            "{output}"
        );
    }

    #[test]
    fn given_deleted_inline_row_when_reading_exact_global_stats_then_row_count_excludes_deleted_row()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(54);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_exact_global_deleted_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(
                table_id,
                "runtime_inline_exact_global_deleted_stats",
                schema_id,
            ),
        )
        .unwrap();

        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t201\ti:201\nrow\t249\ti:249\nrow\t250\ti:250\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[250],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();

        let output = read_inline_rows_payload_with_stats_request_and_mode(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
            InlineStatsRequest::Global,
            InlineStatsMode::ExactVisible,
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("inline_table_stats\t2\t251"), "{output}");
    }

    #[test]
    fn given_inline_struct_field_added_after_rows_when_reading_global_stats_then_new_child_contains_null()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(52);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_struct_evolved_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            nested_stats_table(
                table_id,
                "runtime_inline_struct_evolved_stats",
                schema_id,
                "struct",
            ),
        )
        .unwrap();

        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            format!("row\t0\t{}\n", encoded_duckdb_text("{'i': 1, 'j': 2}")).into_bytes(),
        )
        .unwrap();
        let before_evolution = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let mut evolved = with_inline;
        evolved.columns.push(TableColumnRow::new(
            ColumnId(4),
            "k",
            "INTEGER",
            true,
            Some(ColumnId(1)),
        ));
        kv.commit_table_replacements(
            catalog,
            before_evolution.sequence,
            vec![TableVersionReplacement::new(table_id, created, evolved)],
        )
        .unwrap();

        let output = read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: None,
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(
            output.contains("inline_column_stats\t4\ttrue\ttrue\tfalse\t\tfalse\t"),
            "{output}"
        );
    }

    #[test]
    fn given_inline_list_values_when_reading_global_stats_then_element_column_has_min_max() {
        let catalog = CatalogId(1);
        let table_id = TableId(51);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_list_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            nested_stats_table(table_id, "runtime_inline_list_stats", schema_id, "list"),
        )
        .unwrap();

        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            format!(
                "row\t0\t{}\nrow\t1\t{}\nrow\t2\tn:\nrow\t3\t{}\n",
                encoded_duckdb_text("[1]"),
                encoded_duckdb_text("[NULL]"),
                encoded_duckdb_text("[6, 7]")
            )
            .into_bytes(),
        )
        .unwrap();

        let output = read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: None,
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(
            output.contains("inline_column_stats\t2\ttrue\ttrue\ttrue\t1\ttrue\t7"),
            "{output}"
        );
    }

    #[test]
    fn given_deleted_inline_row_when_reading_visible_rows_then_stats_describe_only_visible_rows() {
        let catalog = CatalogId(1);
        let table_id = TableId(47);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_deleted_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_deleted_stats", schema_id),
        )
        .unwrap();
        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t200\ti:201\nrow\t249\ti:250\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[249],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();

        let exact_global = crate::runtime_inline_rows::read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let exact_global = String::from_utf8(exact_global).unwrap();
        assert!(
            exact_global.contains("inline_table_stats\t2\t250"),
            "{exact_global}"
        );
        assert!(
            exact_global.contains("inline_column_stats\t1\ttrue\tfalse\ttrue\t201\ttrue\t250"),
            "{exact_global}"
        );

        let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(99),
                table_id,
                "main/runtime_inline_deleted_stats/recomputed.parquet",
                1,
                100,
                delete_snapshot.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(delete_snapshot.order)),
        )
        .unwrap();

        let recomputed_global = crate::runtime_inline_rows::read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let recomputed_global = String::from_utf8(recomputed_global).unwrap();
        assert!(
            recomputed_global.contains("inline_table_stats\t2\t250"),
            "{recomputed_global}"
        );
        assert!(
            recomputed_global.contains("inline_column_stats\t1\ttrue\tfalse\ttrue\t201\ttrue\t250"),
            "{recomputed_global}"
        );

        let visible = crate::runtime_inline_rows::read_inline_rows_exact_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();

        assert!(visible.contains("row_change\t2\t\t200\ti:201"), "{visible}");
        assert!(
            !visible.contains("row_change\t2\t\t249\ti:250"),
            "{visible}"
        );
        assert!(visible.contains("inline_table_stats\t1\t250"), "{visible}");
        assert!(
            visible.contains("inline_column_stats\t1\ttrue\tfalse\ttrue\t201\ttrue\t201"),
            "{visible}"
        );
    }

    #[test]
    fn given_multiple_inline_insert_batches_when_deleting_one_row_then_only_that_row_is_hidden() {
        let catalog = CatalogId(1);
        let table_id = TableId(53);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_batch_delete_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_batch_delete", schema_id),
        )
        .unwrap();
        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));

        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            inline_payload_for_ids([0, 1, 2, 3, 4]),
            DuckLakeSnapshotId(2),
        );
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            inline_payload_for_ids(5..17),
            DuckLakeSnapshotId(3),
        );
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            inline_payload_for_ids(17..37),
            DuckLakeSnapshotId(4),
        );

        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[2],
            Some(DuckLakeSnapshotId(5)),
        )
        .unwrap();

        let visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(5))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();

        assert!(!visible.contains("row_change\t2\t\t2\ti:2"), "{visible}");
        assert!(visible.contains("row_change\t2\t\t1\ti:1"), "{visible}");
        assert!(visible.contains("row_change\t3\t\t5\ti:5"), "{visible}");
        assert!(visible.contains("row_change\t4\t\t17\ti:17"), "{visible}");
        assert!(visible.contains("inline_table_stats\t37\t37"), "{visible}");
    }

    #[test]
    fn given_live_inline_payloads_overlap_when_deleting_row_then_delete_is_rejected() {
        let catalog = CatalogId(1);
        let table_id = TableId(54);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_overlap_guard_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_overlap_guard", schema_id),
        )
        .unwrap();
        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));

        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            inline_payload_for_ids(0..5),
            DuckLakeSnapshotId(2),
        );
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            inline_payload_for_ids(2..7),
        )
        .unwrap();
        let result = commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[2],
            Some(DuckLakeSnapshotId(4)),
        );

        assert!(result.is_err(), "{result:?}");
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("inline row id 2 matches multiple live payload versions"),
            "{message}"
        );
    }

    #[test]
    fn given_inline_rows_are_covered_by_historical_merge_file_when_replacement_is_later_deleted_then_rows_are_suppressed()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(46);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_merge_file_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_merge_file", schema_id),
        )
        .unwrap();
        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t0\ti:10\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        let first_inline = latest_snapshot(&kv, catalog).unwrap().unwrap();
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t1\ti:20\n".to_vec(),
            DuckLakeSnapshotId(3),
        );
        let second_inline = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(99),
                table_id,
                "main/runtime_inline_merge_file/replacement.parquet",
                2,
                100,
                first_inline.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(second_inline.order)),
        )
        .unwrap();
        register_delete_file(
            &mut kv,
            catalog,
            DeleteFileRow::new(
                DeleteFileId(100),
                DataFileId(99),
                "main/runtime_inline_merge_file/delete-replacement.parquet",
                2,
                100,
                CatalogOrderId::uuid_v7(10),
            ),
        )
        .unwrap();

        let visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(2))),
                include_flushed: true,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();

        assert!(!visible.contains("row_change\t2\t\t0\ti:10"), "{visible}");
        assert!(visible.contains("inline_payload_count=1"), "{visible}");
        assert!(visible.contains("inline_table_stats\t1\t1"), "{visible}");
    }

    #[test]
    fn given_inline_rows_are_materialized_when_reading_global_stats_then_next_row_id_still_advances()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(46);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_materialized_stats_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_materialized_stats", schema_id),
        )
        .unwrap();
        let mut with_inline = created;
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline,
            schema_id,
            b"row\t0\ti:10\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        append_data_file(
            &mut kv,
            catalog,
            DataFileRow::new(
                DataFileId(99),
                table_id,
                "main/runtime_inline_materialized_stats/replacement.parquet",
                1,
                100,
                inline_snapshot.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(inline_snapshot.order)),
        )
        .unwrap();

        let global = read_inline_rows_global_stats_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(2))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let global = String::from_utf8(global).unwrap();

        assert!(global.contains("inline_table_stats\t0\t1"), "{global}");
    }

    #[test]
    fn given_inline_rows_are_materialized_by_rewrite_when_reading_normal_rows_then_no_inline_rows_are_live()
     {
        let catalog = CatalogId(1);
        let table_id = TableId(47);
        let schema_id = SchemaId(0);
        let table_name = "runtime_inline_rewrite_file_inlined";
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline_schema(table_id, "runtime_inline_rewrite_file", schema_id),
        )
        .unwrap();
        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        register_inline_table_payload_with_table_at_snapshot_for_test(
            &mut kv,
            catalog,
            with_inline.clone(),
            schema_id,
            b"row\t200\ti:201\nrow\t249\ti:250\n".to_vec(),
            DuckLakeSnapshotId(2),
        );
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        commit_delete_inline_table_rows_at_snapshot(
            &mut kv,
            catalog,
            table_id,
            schema_id,
            &[249],
            Some(DuckLakeSnapshotId(3)),
        )
        .unwrap();
        let before_rewrite = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(3))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let before_rewrite = String::from_utf8(before_rewrite).unwrap();
        assert!(before_rewrite.contains("row_change\t2\t\t200\ti:201"));
        assert!(!before_rewrite.contains("row_change\t2\t\t249\ti:250"));

        commit_data_mutation(
            &mut kv,
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(99),
                    table_id,
                    "main/runtime_inline_rewrite_file/replacement.parquet",
                    248,
                    100,
                    inline_snapshot.order,
                )
                .with_row_id_start(0)
                .with_max_partial_order(Some(inline_snapshot.order)),
            ],
            vec![],
            &[InlineTableFlush {
                table_id,
                schema_id,
                flush_snapshot_sequence: RawSnapshotSequence(3),
            }],
        )
        .unwrap();
        let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let rewrite_public =
            public_snapshot_sequence_for_order(&kv, catalog, rewrite_snapshot.order)
                .unwrap()
                .unwrap()
                .0;

        let visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(rewrite_public))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();

        assert!(visible.contains("inline_payload_count=0"), "{visible}");
        assert!(visible.contains("inline_table_stats\t0\t0"), "{visible}");
        assert!(
            !visible.contains("row_change\t2\t\t200\ti:201"),
            "{visible}"
        );

        let historical_visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(2))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let historical_visible = String::from_utf8(historical_visible).unwrap();

        assert!(
            historical_visible.contains("inline_payload_count=0"),
            "{historical_visible}"
        );
        assert!(
            !historical_visible.contains("row_change\t2\t\t200\ti:201"),
            "{historical_visible}"
        );
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_fdb_same_table_name_in_multiple_schemas_when_flushing_one_inline_table_then_other_schemas_remain_live()
     {
        if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
            eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
            return;
        }

        let catalog = CatalogId(1);
        let prefix = format!(
            "aux-ducklake-test/runtime-fdb-inline-flush-schema/{}/{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();

        let main = create_fdb_inline_table(
            &kv,
            catalog,
            TableId(10),
            SchemaId(0),
            "test",
            "ducklake_inlined_data_10_0",
            b"row\t1\ti:42\n".to_vec(),
        );
        let s1_test = create_fdb_inline_table(
            &kv,
            catalog,
            TableId(11),
            SchemaId(1),
            "test",
            "ducklake_inlined_data_11_1",
            b"row\t1\ti:43\n".to_vec(),
        );
        let s1_test2 = create_fdb_inline_table(
            &kv,
            catalog,
            TableId(12),
            SchemaId(1),
            "test2",
            "ducklake_inlined_data_12_1",
            b"row\t1\ti:44\n".to_vec(),
        );
        let s2_test = create_fdb_inline_table(
            &kv,
            catalog,
            TableId(13),
            SchemaId(2),
            "test",
            "ducklake_inlined_data_13_2",
            b"row\t1\ti:45\n".to_vec(),
        );
        let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        kv.commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![
                DataFileRow::new(
                    DataFileId(910),
                    main.table_id,
                    "main/test/flushed.parquet",
                    1,
                    100,
                    flush_snapshot.order,
                )
                .with_row_id_start(0)
                .with_max_partial_order(Some(flush_snapshot.order)),
            ],
            vec![],
            vec![InlineTableFlush {
                table_id: main.table_id,
                schema_id: main.schema_id,
                flush_snapshot_sequence: flush_snapshot.sequence,
            }],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let public_snapshot = public_snapshot_sequence_for_order(&kv, catalog, latest.order)
            .unwrap()
            .unwrap()
            .0;

        assert_inline_payload_count(
            &kv,
            catalog,
            "ducklake_inlined_data_10_0",
            public_snapshot,
            0,
        );
        assert_inline_payload_count(
            &kv,
            catalog,
            "ducklake_inlined_data_11_1",
            public_snapshot,
            1,
        );
        assert_inline_payload_count(
            &kv,
            catalog,
            "ducklake_inlined_data_12_1",
            public_snapshot,
            1,
        );
        assert_inline_payload_count(
            &kv,
            catalog,
            "ducklake_inlined_data_13_2",
            public_snapshot,
            1,
        );

        assert_eq!(s1_test.schema_id, SchemaId(1));
        assert_eq!(s1_test2.schema_id, SchemaId(1));
        assert_eq!(s2_test.schema_id, SchemaId(2));
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_fdb_inline_rows_are_materialized_by_rewrite_when_reading_normal_rows_then_no_inline_rows_are_live()
     {
        if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
            eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
            return;
        }

        let catalog = CatalogId(1);
        let table_id = TableId(48);
        let schema_id = SchemaId(0);
        let table_name = "runtime_fdb_inline_rewrite_file_inlined";
        let prefix = format!(
            "aux-ducklake-test/runtime-fdb-inline-rewrite/{}/{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        let created = kv
            .create_table_versionstamped(
                catalog,
                table_with_inline_schema(table_id, "runtime_fdb_inline_rewrite_file", schema_id),
                None,
            )
            .unwrap();
        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(table_name, schema_id.0));
        kv.register_inline_table_payload_with_table_versionstamped(
            catalog,
            with_inline,
            schema_id,
            b"row\t200\ti:201\nrow\t249\ti:250\n".to_vec(),
        )
        .unwrap();
        let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        kv.commit_delete_inline_table_rows_versionstamped(
            catalog,
            table_id,
            schema_id,
            &[249],
            None,
        )
        .unwrap();
        let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

        kv.commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![
                DataFileRow::new(
                    DataFileId(99),
                    table_id,
                    "main/runtime_fdb_inline_rewrite_file/replacement.parquet",
                    248,
                    100,
                    inline_snapshot.order,
                )
                .with_row_id_start(0)
                .with_max_partial_order(Some(inline_snapshot.order)),
            ],
            vec![],
            vec![InlineTableFlush {
                table_id,
                schema_id,
                flush_snapshot_sequence: delete_snapshot.sequence,
            }],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let rewrite_public =
            public_snapshot_sequence_for_order(&kv, catalog, rewrite_snapshot.order)
                .unwrap()
                .unwrap()
                .0;

        let visible = read_inline_rows_payload(
            &kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(rewrite_public))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();

        assert!(visible.contains("inline_payload_count=0"), "{visible}");
        assert!(visible.contains("inline_table_stats\t0\t0"), "{visible}");
        assert!(
            !visible.contains("row_change\t2\t\t200\ti:201"),
            "{visible}"
        );
    }

    #[test]
    fn given_opaque_duckdb_inline_value_when_accumulating_stats_then_value_is_non_null_without_min_max()
     {
        let mut stats = LiveColumnStats::default();

        stats.accumulate_encoded("d:001122", "VARIANT").unwrap();

        assert!(stats.has_contains_null);
        assert!(!stats.contains_null);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn given_null_and_opaque_duckdb_inline_values_when_accumulating_stats_then_only_null_marks_contains_null()
     {
        let mut stats = LiveColumnStats::default();

        stats.accumulate_encoded("d:001122", "VARIANT").unwrap();
        stats.accumulate_encoded("n:", "VARIANT").unwrap();

        assert!(stats.has_contains_null);
        assert!(stats.contains_null);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn given_geometry_inline_values_when_accumulating_stats_then_extra_stats_merge_extents() {
        let mut stats = LiveColumnStats::default();

        stats
            .accumulate_encoded("v:504f494e54282d32203229", "GEOMETRY")
            .unwrap();
        stats
            .accumulate_encoded(
                "v:4c494e45535452494e47205a202830203020302c20322032203229",
                "GEOMETRY",
            )
            .unwrap();

        assert!(stats.has_contains_null);
        assert!(!stats.contains_null);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
        let extra_stats = stats.extra_stats.unwrap().to_string();
        assert!(extra_stats.contains("\"xmin\": -2"), "{extra_stats}");
        assert!(extra_stats.contains("\"ymax\": 2"), "{extra_stats}");
        assert!(extra_stats.contains("\"zmax\": 2"), "{extra_stats}");
        assert!(extra_stats.contains("\"point\""), "{extra_stats}");
        assert!(extra_stats.contains("\"linestring_z\""), "{extra_stats}");
    }

    #[test]
    fn given_geometry_wkt_when_parsing_inline_stats_then_all_coordinate_dimensions_are_tracked() {
        let stats =
            InlineGeometryStats::parse_wkt("MULTILINESTRING ZM ((0 0 -10 10), (3 3 3 1))").unwrap();

        let rendered = stats.to_string();
        assert!(rendered.contains("\"xmax\": 3"), "{rendered}");
        assert!(rendered.contains("\"zmin\": -10"), "{rendered}");
        assert!(rendered.contains("\"mmax\": 10"), "{rendered}");
        assert!(rendered.contains("\"multilinestring_zm\""), "{rendered}");
    }

    fn register_inline_table_payload_with_table_at_snapshot_for_test(
        kv: &mut FakeOrderedCatalogKv,
        catalog: CatalogId,
        table: TableRow,
        schema_id: SchemaId,
        payload: Vec<u8>,
        commit_snapshot: DuckLakeSnapshotId,
    ) {
        crate::register_inline_table_payload_with_table_at_snapshot(
            kv,
            catalog,
            table,
            schema_id,
            payload,
            Some(commit_snapshot),
        )
        .unwrap();
    }

    fn inline_payload_for_ids(ids: impl IntoIterator<Item = u64>) -> Vec<u8> {
        ids.into_iter()
            .map(|id| format!("row\t{id}\ti:{id}\n"))
            .collect::<String>()
            .into_bytes()
    }

    fn table_with_inline_schema(table_id: TableId, name: &str, schema_id: SchemaId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                true,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        )
    }

    fn partitioned_inline_schema(table_id: TableId, name: &str, schema_id: SchemaId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            vec![
                TableColumnRow::new(ColumnId(1), "id", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(2), "region", "VARCHAR", true, None),
                TableColumnRow::new(ColumnId(3), "amount", "INTEGER", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        )
    }

    fn timestamp_inline_schema(table_id: TableId, name: &str, schema_id: SchemaId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            vec![
                TableColumnRow::new(ColumnId(1), "id", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(2), "ts", "TIMESTAMP", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        )
    }

    #[cfg(feature = "foundationdb")]
    fn create_fdb_inline_table(
        kv: &crate::FdbOrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        table_name: &str,
        inline_table_name: &str,
        payload: Vec<u8>,
    ) -> TableRow {
        let created = kv
            .create_table_versionstamped(
                catalog,
                table_with_inline_schema(table_id, table_name, schema_id),
                None,
            )
            .unwrap();
        let mut with_inline = created.clone();
        with_inline
            .inlined_data_tables
            .push(InlinedTableRow::new(inline_table_name, schema_id.0));
        kv.register_inline_table_payload_with_table_versionstamped(
            catalog,
            with_inline.clone(),
            schema_id,
            payload,
        )
        .unwrap();
        with_inline
    }

    #[cfg(feature = "foundationdb")]
    fn assert_inline_payload_count(
        kv: &crate::FdbOrderedCatalogKv,
        catalog: CatalogId,
        table_name: &str,
        snapshot_id: u64,
        expected: usize,
    ) {
        let visible = read_inline_rows_payload(
            kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(snapshot_id))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap();
        let visible = String::from_utf8(visible).unwrap();
        assert!(
            visible.contains(&format!("inline_payload_count={expected}")),
            "{visible}"
        );
    }

    fn nested_stats_table(
        table_id: TableId,
        name: &str,
        schema_id: SchemaId,
        nested_type: &str,
    ) -> TableRow {
        let top_type = match nested_type {
            "list" => "list",
            "struct" => "struct",
            _ => nested_type,
        };
        let child_name = if nested_type == "list" {
            "element"
        } else {
            "i"
        };
        let mut columns = vec![
            TableColumnRow::new(ColumnId(1), "nested", top_type, true, None),
            TableColumnRow::new(ColumnId(2), child_name, "INTEGER", true, Some(ColumnId(1))),
        ];
        if nested_type == "struct" {
            columns.push(TableColumnRow::new(
                ColumnId(3),
                "j",
                "INTEGER",
                true,
                Some(ColumnId(1)),
            ));
        }
        TableRow::with_catalog_metadata(
            table_id,
            schema_id,
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            columns,
            CatalogOrderId::uuid_v7(0),
        )
    }

    fn encoded_duckdb_text(value: &str) -> String {
        let mut out = String::from("v:");
        for byte in value.as_bytes() {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    fn inline_row_changes_payload_request(
        table_name: &str,
        start_snapshot_id: u64,
        end_snapshot_id: u64,
    ) -> InlineRowChangesPayload {
        InlineRowChangesPayload {
            table_name: table_name.to_owned(),
            start_snapshot: ChangeFeedStartSnapshot::new(DuckLakeSnapshotId(start_snapshot_id)),
            end_snapshot: ChangeFeedEndSnapshot::new(DuckLakeSnapshotId(end_snapshot_id)),
        }
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
}
