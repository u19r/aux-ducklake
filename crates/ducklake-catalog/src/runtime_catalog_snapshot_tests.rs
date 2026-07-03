#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use super::super::{
        CatalogSnapshotIdKind, catalog_snapshot_payload, catalog_snapshot_payload_with_kind,
        public_snapshot_payload, snapshot_payload, update_raw_data_change_watermark,
    };
    #[cfg(feature = "foundationdb")]
    use crate::FdbOrderedCatalogKv;
    use crate::{
        CatalogId, CatalogOrderId, CatalogResult, ColumnDrop, ColumnId, DataFileId, DataFileRow,
        DuckLakeSnapshotId, FakeOrderedCatalogKv, InlineTableFlush, InlinedTableRow, MacroId,
        MacroImplementationRow, MacroParameterRow, MacroRow, OrderedCatalogKv, RangeDirection,
        RangeItem, RawSnapshotSequence, SchemaId, SchemaRow, TableColumnRow, TableId, TableRow,
        commit_append_data_files, commit_append_table_columns, commit_create_macro_rows,
        commit_create_schema_rows, commit_create_table_row,
        commit_data_mutation_with_file_partitions, commit_drop_table_columns, commit_drop_tables,
        expire_snapshots, initialize_catalog_if_absent,
        keys::{
            current_table_row_prefix, macro_object_scan_prefix, schema_object_scan_prefix,
            snapshot_timestamp_prefix, table_object_scan_prefix, view_object_scan_prefix,
        },
        kv::MutableCatalogKv,
        latest_snapshot, public_snapshot_sequence_for_order, register_inline_table_payload,
        register_inline_table_payload_with_table,
        runtime_read_context::CatalogSnapshotRequestContext,
        runtime_snapshot_range::{SnapshotDataChangeOrder, SnapshotWatermarkCutoffOrder},
        snapshot_by_ducklake_sequence,
    };

    #[test]
    fn given_helper_schema_commit_coalesces_with_table_then_public_snapshot_loads_table() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_schema_rows(
            &mut kv,
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(10),
                SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders",
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

        let payload =
            String::from_utf8(catalog_snapshot_payload(&kv, catalog, 1).unwrap()).unwrap();

        assert!(payload.contains("schema\t0\tmain-uuid\tmain\tmain/"));
        assert!(payload.contains("table\t10\t0\torders-uuid\torders\tmain/orders\t"));
    }

    #[test]
    fn given_macro_parameter_has_duckdb_alias_then_catalog_snapshot_uses_parseable_type_name() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![MacroRow::new(
                MacroId(1),
                SchemaId(0),
                "macro_no_partition",
                vec![MacroImplementationRow {
                    dialect: "duckdb".to_owned(),
                    sql: "SELECT avg(val) OVER () AS avg_val FROM tbl".to_owned(),
                    macro_type: "table".to_owned(),
                    parameters: vec![MacroParameterRow {
                        parameter_name: "avg_val".to_owned(),
                        parameter_type: "float32".to_owned(),
                        default_value: "NULL".to_owned(),
                        default_value_type: "float64".to_owned(),
                    }],
                }],
                CatalogOrderId::uuid_v7(0),
            )],
            Some(crate::RawSnapshotSequence(1)),
        )
        .unwrap();

        let payload =
            String::from_utf8(catalog_snapshot_payload(&kv, catalog, 1).unwrap()).unwrap();

        assert!(payload.contains("macro_param\t1\t0\t0\tavg_val\tfloat\tNULL\tdouble\n"));
    }

    #[test]
    fn given_inline_only_commit_when_loading_snapshot_then_file_watermark_advances() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        register_inline_table_payload(
            &mut kv,
            catalog,
            TableId(10),
            SchemaId(1),
            b"row\t0\t10\n".to_vec(),
        )
        .unwrap();
        let snapshot = latest_snapshot(&kv, catalog).unwrap();

        let payload = String::from_utf8(snapshot_payload(&kv, catalog, snapshot).unwrap()).unwrap();

        assert!(payload.contains("ducklake_snapshot_id=1\n"));
        assert!(payload.contains("ducklake_next_file_id=2\n"));
    }

    #[test]
    fn given_future_data_change_order_when_updating_watermark_then_it_is_ignored() {
        let cutoff = CatalogOrderId::uuid_v7(20);
        let current_change = CatalogOrderId::uuid_v7(10);
        let future_change = CatalogOrderId::uuid_v7(30);
        let sequence_by_order = BTreeMap::from([(current_change, 4), (future_change, 8)]);
        let mut watermark = None;

        update_raw_data_change_watermark(
            &sequence_by_order,
            SnapshotWatermarkCutoffOrder::new(cutoff),
            SnapshotDataChangeOrder::new(future_change),
            &mut watermark,
        );
        update_raw_data_change_watermark(
            &sequence_by_order,
            SnapshotWatermarkCutoffOrder::new(cutoff),
            SnapshotDataChangeOrder::new(current_change),
            &mut watermark,
        );

        assert_eq!(watermark, Some(4));
    }

    #[test]
    fn given_future_catalog_snapshot_when_rendering_then_latest_catalog_is_returned_without_history_scan()
     {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(
            &mut inner,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(10),
                SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders",
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
        let latest = latest_snapshot(&inner, catalog).unwrap().unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(inner, catalog);

        catalog_snapshot_payload_with_kind(
            &kv,
            catalog,
            DuckLakeSnapshotId(latest.sequence.0 + 8),
            CatalogSnapshotIdKind::DuckLakeSequence,
        )
        .unwrap();

        assert_eq!(kv.snapshot_timestamp_scan_count(), 0);
    }

    #[test]
    fn given_historical_raw_catalog_snapshot_when_rendering_then_timestamp_history_is_not_scanned()
    {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(&mut inner, catalog, original_table(TableId(10))).unwrap();
        let kv = SnapshotTimestampScanRecordingKv::new(inner, catalog);

        catalog_snapshot_payload_with_kind(
            &kv,
            catalog,
            DuckLakeSnapshotId(0),
            CatalogSnapshotIdKind::DuckLakeSequence,
        )
        .unwrap();

        assert_eq!(kv.snapshot_timestamp_scan_count(), 0);
    }

    #[test]
    fn given_catalog_snapshot_request_context_when_rendering_multiple_snapshots_then_catalog_facts_are_loaded_once()
     {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(&mut inner, catalog, original_table(TableId(10))).unwrap();
        let first_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        commit_append_table_columns(
            &mut inner,
            catalog,
            TableId(10),
            vec![TableColumnRow::new(
                ColumnId(99),
                "amount",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        let latest_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        let kv = CatalogFactsScanRecordingKv::new(inner, catalog);

        let mut request = CatalogSnapshotRequestContext::for_current_catalog(&kv, catalog).unwrap();

        let first = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(first_snapshot.sequence.0),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap();
        let first_context = request.snapshot_context_for(&kv, catalog, first).unwrap();
        assert_eq!(first_context.snapshot.order, first_snapshot.order);

        let latest = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(latest_snapshot.sequence.0),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap();
        let latest_context = request.snapshot_context_for(&kv, catalog, latest).unwrap();
        assert_eq!(latest_context.snapshot.order, latest_snapshot.order);

        assert_eq!(kv.scan_count("schema"), 1);
        assert_eq!(kv.scan_count("table"), 1);
        assert_eq!(kv.scan_count("current_table"), 1);
        assert_eq!(kv.scan_count("view"), 1);
        assert_eq!(kv.scan_count("macro"), 1);
    }

    #[test]
    fn given_latest_catalog_snapshot_when_rendering_then_table_history_is_not_scanned() {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(&mut inner, catalog, original_table(TableId(10))).unwrap();
        commit_append_table_columns(
            &mut inner,
            catalog,
            TableId(10),
            vec![TableColumnRow::new(
                ColumnId(99),
                "amount",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        let latest_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        let kv = CatalogFactsScanRecordingKv::new(inner, catalog);
        let mut request = CatalogSnapshotRequestContext::for_current_catalog(&kv, catalog).unwrap();

        let snapshot = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(latest_snapshot.sequence.0),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap();
        let context = request
            .snapshot_context_for(&kv, catalog, snapshot)
            .unwrap();

        assert_eq!(context.snapshot.order, latest_snapshot.order);
        assert_eq!(context.tables.len(), 1);
        assert_eq!(kv.scan_count("table"), 0);
        assert_eq!(kv.scan_count("current_table"), 1);
    }

    #[test]
    fn given_request_renders_latest_then_historical_snapshot_then_facts_upgrade_to_complete_history()
     {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(&mut inner, catalog, original_table(TableId(10))).unwrap();
        let first_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        commit_append_table_columns(
            &mut inner,
            catalog,
            TableId(10),
            vec![TableColumnRow::new(
                ColumnId(99),
                "amount",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        let latest_snapshot = latest_snapshot(&inner, catalog).unwrap().unwrap();
        let kv = CatalogFactsScanRecordingKv::new(inner, catalog);
        let mut request = CatalogSnapshotRequestContext::for_current_catalog(&kv, catalog).unwrap();

        let latest = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(latest_snapshot.sequence.0),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap();
        let latest_context = request.snapshot_context_for(&kv, catalog, latest).unwrap();
        assert_eq!(latest_context.tables[0].columns.len(), 3);
        assert_eq!(kv.scan_count("table"), 0);

        let historical = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(first_snapshot.sequence.0),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap();
        let historical_context = request
            .snapshot_context_for(&kv, catalog, historical)
            .unwrap();

        assert_eq!(historical_context.snapshot.order, first_snapshot.order);
        assert_eq!(historical_context.tables[0].columns.len(), 2);
        assert_eq!(
            kv.scan_count("table"),
            1,
            "only the historical render should force complete table history"
        );
    }

    #[test]
    fn given_public_snapshot_before_column_drop_when_rendering_then_pre_drop_columns_are_returned()
    {
        let catalog = CatalogId(1);
        let table = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, original_table(table)).unwrap();
        commit_append_table_columns(
            &mut kv,
            catalog,
            table,
            vec![TableColumnRow::new(
                ColumnId(99),
                "amount",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        let pre_drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let pre_drop_public_snapshot =
            public_snapshot_sequence_for_order(&kv, catalog, pre_drop_snapshot.order)
                .unwrap()
                .unwrap();
        commit_drop_table_columns(&mut kv, catalog, &[ColumnDrop::new(table, ColumnId(2))])
            .unwrap();

        let snapshot_payload = String::from_utf8(
            public_snapshot_payload(&kv, catalog, pre_drop_public_snapshot).unwrap(),
        )
        .unwrap();
        assert!(
            snapshot_payload.contains("ducklake_schema_version=2\n"),
            "{snapshot_payload}"
        );

        let payload = String::from_utf8(
            catalog_snapshot_payload_with_kind(
                &kv,
                catalog,
                pre_drop_public_snapshot,
                CatalogSnapshotIdKind::PublicSnapshot,
            )
            .unwrap(),
        )
        .unwrap();

        assert!(
            payload.contains("column\t10\t1\tkey\tINTEGER\tfalse\t\t\t\tNULL\tliteral\n"),
            "{payload}"
        );
        assert!(
            payload.contains("column\t10\t2\tvalue\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\n"),
            "{payload}"
        );
        assert!(
            payload.contains("column\t10\t99\tamount\tINTEGER\ttrue\t\t\t\tNULL\tliteral\n"),
            "{payload}"
        );
    }

    #[test]
    fn given_public_catalog_snapshot_request_when_rendering_then_coalescing_facts_are_reused() {
        let catalog = CatalogId(1);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        commit_create_table_row(&mut inner, catalog, original_table(TableId(10))).unwrap();
        commit_append_table_columns(
            &mut inner,
            catalog,
            TableId(10),
            vec![TableColumnRow::new(
                ColumnId(99),
                "amount",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        let kv = CatalogFactsScanRecordingKv::new(inner, catalog);
        let mut request = CatalogSnapshotRequestContext::for_current_catalog(&kv, catalog).unwrap();

        let snapshot = request
            .resolve_snapshot(
                &kv,
                catalog,
                DuckLakeSnapshotId(1),
                CatalogSnapshotIdKind::PublicSnapshot,
            )
            .unwrap();
        let context = request
            .snapshot_context_for(&kv, catalog, snapshot)
            .unwrap();

        assert_eq!(context.snapshot.sequence.0, 1);
        assert_eq!(kv.scan_count("schema"), 1);
        assert_eq!(kv.scan_count("table"), 1);
        assert_eq!(kv.scan_count("current_table"), 1);
        assert_eq!(kv.scan_count("view"), 1);
        assert_eq!(kv.scan_count("macro"), 1);
    }

    #[test]
    fn given_highest_table_id_is_dropped_when_loading_snapshot_then_next_catalog_id_uses_history() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(1),
                SchemaId(0),
                "old-uuid",
                "example",
                "main/example",
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
        commit_drop_tables(&mut kv, catalog, &[TableId(1)]).unwrap();
        let snapshot = latest_snapshot(&kv, catalog).unwrap();

        let payload = String::from_utf8(snapshot_payload(&kv, catalog, snapshot).unwrap()).unwrap();

        assert!(
            payload.contains("ducklake_next_catalog_id=2\n"),
            "{payload}"
        );
    }

    #[test]
    fn given_table_without_stored_main_schema_when_loading_catalog_then_snapshot_includes_main_schema()
     {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            TableRow::with_catalog_metadata(
                TableId(2),
                SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders",
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

        let payload =
            String::from_utf8(catalog_snapshot_payload(&kv, catalog, 1).unwrap()).unwrap();

        assert!(payload.contains("schema_count=1\n"), "{payload}");
        assert!(
            payload.contains("schema\t0\t00000000-0000-0000-0000-000000000000\tmain\tmain/\n"),
            "{payload}"
        );
        assert!(
            payload.contains("table\t2\t0\torders-uuid\torders\tmain/orders\t\n"),
            "{payload}"
        );
    }

    #[test]
    fn given_public_version_differs_from_raw_sequence_when_getting_snapshot_at_then_payload_uses_raw_snapshot_state()
     {
        let catalog = CatalogId(1);
        let table = TableId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(
            &mut kv,
            catalog,
            table_with_inline(
                table,
                "orders",
                &[InlinedTableRow::new("ducklake_inlined_data_1_1", 1)],
            ),
        )
        .unwrap();
        register_inline_table_payload(
            &mut kv,
            catalog,
            table,
            SchemaId(1),
            b"row\t0\ti:100\n".to_vec(),
        )
        .unwrap();
        commit_data_mutation_with_file_partitions(
            &mut kv,
            catalog,
            vec![snapshot_payload_data_file(1, table)],
            Vec::new(),
            &[InlineTableFlush::new(
                table,
                SchemaId(1),
                crate::RawSnapshotSequence(2),
            )],
            Vec::new(),
        )
        .unwrap();
        commit_append_data_files(&mut kv, catalog, vec![snapshot_payload_data_file(2, table)])
            .unwrap();
        commit_append_data_files(&mut kv, catalog, vec![snapshot_payload_data_file(3, table)])
            .unwrap();
        let mut with_new_inline_table = table_with_inline(
            table,
            "orders",
            &[
                InlinedTableRow::new("ducklake_inlined_data_1_1", 1),
                InlinedTableRow::new("ducklake_inlined_data_1_2", 2),
            ],
        );
        with_new_inline_table.columns.push(TableColumnRow::new(
            ColumnId(2),
            "col2",
            "INTEGER",
            true,
            None,
        ));
        commit_append_table_columns(
            &mut kv,
            catalog,
            table,
            vec![TableColumnRow::new(
                ColumnId(2),
                "col2",
                "INTEGER",
                true,
                None,
            )],
        )
        .unwrap();
        register_inline_table_payload_with_table(
            &mut kv,
            catalog,
            with_new_inline_table,
            SchemaId(2),
            Vec::new(),
        )
        .unwrap();
        commit_append_data_files(&mut kv, catalog, vec![snapshot_payload_data_file(4, table)])
            .unwrap();

        let payload = String::from_utf8(
            public_snapshot_payload(&kv, catalog, crate::DuckLakeSnapshotId(6)).unwrap(),
        )
        .unwrap();

        assert!(payload.contains("ducklake_snapshot_id=7\n"));
        assert!(payload.contains("catalog_snapshot_sequence=7\n"));

        let raw_payload = String::from_utf8(
            snapshot_payload(
                &kv,
                catalog,
                snapshot_by_ducklake_sequence(&kv, catalog, crate::DuckLakeSnapshotId(6)).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();

        assert!(raw_payload.contains("ducklake_snapshot_id=6\n"));
        assert!(raw_payload.contains("catalog_snapshot_sequence=6\n"));
    }

    #[test]
    fn given_later_schema_changes_and_expired_snapshots_when_getting_raw_ducklake_snapshot_then_original_columns_are_returned()
     {
        let mut kv = FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);
        let table = TableId(1);
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_table_row(&mut kv, catalog, original_table(table)).unwrap();
        assert_raw_ducklake_snapshot_returns_original_columns_after_schema_changes(
            &mut kv, catalog, table,
        );
    }

    #[cfg(feature = "foundationdb")]
    #[test]
    fn given_later_schema_changes_on_fdb_when_getting_raw_ducklake_snapshot_then_original_columns_are_returned()
     {
        let Some(prefix) = live_foundationdb_prefix("catalog-snapshot-schema-history") else {
            return;
        };
        let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        let catalog = CatalogId(1);
        let table = TableId(1);
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        kv.create_table_versionstamped(catalog, original_table(table), None)
            .unwrap();
        assert_raw_ducklake_snapshot_returns_original_columns_after_schema_changes(
            &mut kv, catalog, table,
        );
    }

    fn assert_raw_ducklake_snapshot_returns_original_columns_after_schema_changes(
        kv: &mut impl MutableCatalogKv,
        catalog: CatalogId,
        table: TableId,
    ) {
        commit_append_table_columns(
            kv,
            catalog,
            table,
            vec![TableColumnRow::new(ColumnId(3), "j", "INTEGER", true, None)],
        )
        .unwrap();
        commit_drop_table_columns(kv, catalog, &[ColumnDrop::new(table, ColumnId(1))]).unwrap();
        expire_snapshots(
            kv,
            catalog,
            &[RawSnapshotSequence(1), RawSnapshotSequence(2)],
        )
        .unwrap();

        let payload = String::from_utf8(
            catalog_snapshot_payload_with_kind(
                kv,
                catalog,
                DuckLakeSnapshotId(1),
                CatalogSnapshotIdKind::DuckLakeSequence,
            )
            .unwrap(),
        )
        .unwrap();

        assert!(payload.contains("table\t1\t0\torders-uuid\torders\tmain/orders\t\n"));
        assert!(
            payload.contains("column\t1\t1\tkey\tINTEGER\tfalse\t\t\t\tNULL\tliteral\n"),
            "{payload}"
        );
        assert!(
            payload.contains("column\t1\t2\tvalue\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\n"),
            "{payload}"
        );
        assert!(
            !payload.contains("column\t1\t3\tj\tINTEGER\ttrue"),
            "{payload}"
        );
    }

    fn original_table(table: TableId) -> TableRow {
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-uuid",
            "orders",
            "main/orders",
            vec![
                TableColumnRow::new(ColumnId(1), "key", "INTEGER", false, None),
                TableColumnRow::new(ColumnId(2), "value", "VARCHAR", true, None),
            ],
            CatalogOrderId::uuid_v7(0),
        )
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
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
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
        ) -> CatalogResult<Vec<RangeItem>> {
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    struct CatalogFactsScanRecordingKv {
        inner: FakeOrderedCatalogKv,
        prefixes: BTreeMap<&'static str, Vec<u8>>,
        scans: Rc<RefCell<BTreeMap<&'static str, usize>>>,
    }

    impl CatalogFactsScanRecordingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                prefixes: BTreeMap::from([
                    ("schema", schema_object_scan_prefix(catalog)),
                    ("table", table_object_scan_prefix(catalog)),
                    ("current_table", current_table_row_prefix(catalog)),
                    ("view", view_object_scan_prefix(catalog)),
                    ("macro", macro_object_scan_prefix(catalog)),
                ]),
                scans: Rc::new(RefCell::new(BTreeMap::new())),
            }
        }

        fn scan_count(&self, name: &'static str) -> usize {
            self.scans.borrow().get(name).copied().unwrap_or(0)
        }

        fn record_scan(&self, prefix: &[u8]) {
            for (name, tracked_prefix) in &self.prefixes {
                if prefix == tracked_prefix.as_slice() {
                    let mut scans = self.scans.borrow_mut();
                    *scans.entry(name).or_insert(0) += 1;
                }
            }
        }
    }

    impl OrderedCatalogKv for CatalogFactsScanRecordingKv {
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            self.record_scan(prefix);
            OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    #[cfg(feature = "foundationdb")]
    fn live_foundationdb_prefix(name: &str) -> Option<String> {
        if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
            eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
            return None;
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Some(format!("aux-ducklake/unit/{name}/{nanos}/"))
    }

    fn table_with_inline(
        table: TableId,
        name: &str,
        inlined_tables: &[InlinedTableRow],
    ) -> TableRow {
        let mut row = TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            format!("{name}-uuid"),
            name,
            format!("main/{name}"),
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                false,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        );
        row.inlined_data_tables = inlined_tables.to_vec();
        row
    }

    fn snapshot_payload_data_file(id: u64, table: TableId) -> DataFileRow {
        DataFileRow::new(
            DataFileId(id),
            table,
            format!("file-{id}.parquet"),
            1,
            100,
            CatalogOrderId::uuid_v7(0),
        )
    }
}
