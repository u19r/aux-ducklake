use ducklake_catalog::{
    CatalogId, CatalogOrderId, CatalogOrderKind, ColumnId, DataFileId, DataFileRow,
    FakeOrderedCatalogKv, KvBatch, RangeDirection, SchemaId, SnapshotRow, TableColumnRow, TableId,
    TableRow, append_data_file, commit_append_data_files, commit_append_table_columns,
    commit_create_table, commit_create_table_row, initialize_catalog_if_absent,
    keys::{
        current_data_file_prefix, data_file_begin_key, decode_key, snapshot_key, table_object_key,
    },
    latest_snapshot, list_current_data_files, list_tables_at, load_table_at,
};

#[test]
fn given_fake_catalog_when_data_file_appended_then_current_file_list_uses_table_index() {
    let catalog = CatalogId(3);
    let table_id = TableId(44);
    let begin_order = CatalogOrderId::uuid_v7(50);
    let mut kv = FakeOrderedCatalogKv::new();
    let row = DataFileRow::new(
        DataFileId(9),
        table_id,
        "main/orders/file-0001.parquet",
        10,
        2048,
        begin_order,
    );

    append_data_file(&mut kv, catalog, row.clone()).unwrap();
    let files = list_current_data_files(&kv, catalog, table_id).unwrap();

    assert_eq!(files, vec![row]);
    assert_eq!(
        kv.scan_prefix(
            &current_data_file_prefix(catalog, table_id),
            RangeDirection::Forward,
            usize::MAX
        )
        .len(),
        1
    );
}

#[test]
fn given_data_files_committed_then_snapshot_advances_and_current_index_lists_files() {
    let catalog = CatalogId(3);
    let table = TableId(44);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let before = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let files = vec![DataFileRow::new(
        DataFileId(21),
        table,
        "main/walking_skeleton/file.parquet",
        1,
        1024,
        CatalogOrderId::uuid_v7(0),
    )];

    let committed = commit_append_data_files(&mut kv, catalog, files).unwrap();
    let after = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current = list_current_data_files(&kv, catalog, table).unwrap();

    assert_eq!(after.sequence, before.sequence.next());
    assert!(after.order > before.order);
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].validity.begin_order, after.order);
    assert_eq!(current, committed);
}

#[test]
fn given_versionstamped_snapshot_key_when_latest_loaded_then_key_order_is_authoritative() {
    let catalog = CatalogId(3);
    let mut kv = FakeOrderedCatalogKv::new();
    let stored_key_order = CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
    let placeholder_value_order =
        CatalogOrderId::fdb_versionstamp([0; CatalogOrderId::FDB_VERSIONSTAMP_LEN], 0);
    let row = SnapshotRow {
        order: placeholder_value_order,
        sequence: ducklake_catalog::RawSnapshotSequence(9),
        created_at_micros: 123_456,
        created_by: "fdb-test".to_owned(),
        commit_message: None,
        commit_extra_info: None,
    };
    let mut batch = KvBatch::new();
    batch.put(snapshot_key(catalog, stored_key_order), row.encode());
    kv.commit(batch).unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(latest.order, stored_key_order);
    assert_eq!(latest.order.kind(), CatalogOrderKind::FdbVersionstamp);
    assert_eq!(latest.sequence, ducklake_catalog::RawSnapshotSequence(9));
    assert_eq!(latest.created_at_micros, 123_456);
}

#[test]
fn given_old_snapshot_row_when_decoded_then_format_is_rejected() {
    let order = CatalogOrderId::uuid_v7(9);
    let mut encoded = Vec::new();
    encoded.push(1);
    encoded.extend_from_slice(&order.as_bytes());
    encoded.extend_from_slice(&7_u64.to_be_bytes());
    encoded.extend_from_slice(b"old");

    let error = SnapshotRow::decode(&encoded).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("unsupported snapshot row version 1")
    );
}

#[test]
fn given_appended_data_file_when_begin_timeline_key_is_decoded_then_key_is_readable() {
    let key = data_file_begin_key(
        CatalogId(3),
        TableId(44),
        CatalogOrderId::uuid_v7(50),
        DataFileId(9),
    );

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=3"));
    assert!(decoded.contains("family=data-file-begin"));
    assert!(decoded.contains("0000000000000032"));
}

#[test]
fn given_data_file_row_when_round_tripped_then_file_metadata_is_preserved() {
    let row = DataFileRow::new(
        DataFileId(11),
        TableId(8),
        "main/events/file-0001.parquet",
        100,
        4096,
        CatalogOrderId::uuid_v7(77),
    )
    .with_row_id_start(1234);

    let decoded = DataFileRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
}

#[test]
fn given_fdb_versionstamped_data_file_row_when_round_tripped_then_order_kind_is_preserved() {
    let row = DataFileRow::new(
        DataFileId(12),
        TableId(8),
        "main/events/file-0002.parquet",
        101,
        8192,
        CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0),
    );

    let decoded = DataFileRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
    assert_eq!(
        decoded.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
}

#[test]
fn given_fdb_versionstamped_table_row_when_round_tripped_then_order_kind_is_preserved() {
    let row = TableRow::with_catalog_metadata(
        TableId(8),
        SchemaId(0),
        "table-uuid",
        "events",
        "main/events",
        vec![TableColumnRow::new(
            ColumnId(1),
            "id",
            "INTEGER",
            true,
            None,
        )],
        CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0),
    );

    let decoded = TableRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
    assert_eq!(
        decoded.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
}

#[test]
fn given_old_table_row_v2_when_decoded_then_format_is_rejected() {
    let begin_order = CatalogOrderId::uuid_v7(88);
    let mut encoded = Vec::new();
    encoded.push(2);
    encoded.extend_from_slice(&TableId(14).0.to_be_bytes());
    encoded.extend_from_slice(&SchemaId(0).0.to_be_bytes());
    encoded.extend_from_slice(&begin_order.as_bytes());
    encoded.push(0);
    encoded.extend_from_slice(&CatalogOrderId::uuid_v7(0).as_bytes());
    encoded.extend_from_slice(&0_u32.to_be_bytes());
    encoded.extend_from_slice(&6_u32.to_be_bytes());
    encoded.extend_from_slice(b"orders");
    encoded.extend_from_slice(&0_u32.to_be_bytes());
    encoded.extend_from_slice(&0_u32.to_be_bytes());

    let error = TableRow::decode(&encoded).unwrap_err();

    assert_eq!(
        error.to_string(),
        "decode failed: unsupported table row version 2"
    );
}

#[test]
fn given_old_data_file_row_when_decoded_then_format_is_rejected() {
    let begin_order = CatalogOrderId::uuid_v7(77);
    let mut encoded = Vec::new();
    encoded.push(1);
    encoded.extend_from_slice(&DataFileId(13).0.to_be_bytes());
    encoded.extend_from_slice(&TableId(8).0.to_be_bytes());
    encoded.extend_from_slice(&102_u64.to_be_bytes());
    encoded.extend_from_slice(&16_384_u64.to_be_bytes());
    encoded.extend_from_slice(&begin_order.as_bytes());
    encoded.push(0);
    encoded.extend_from_slice(&CatalogOrderId::uuid_v7(0).as_bytes());
    encoded.extend_from_slice(b"main/events/old.parquet");

    let error = DataFileRow::decode(&encoded).unwrap_err();

    assert_eq!(
        error.to_string(),
        "decode failed: unsupported data file row version 1"
    );
}

#[test]
fn given_existing_catalog_when_initialized_again_then_original_snapshot_is_reused() {
    let catalog = CatalogId(3);
    let mut kv = FakeOrderedCatalogKv::new();

    let first = initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let second = initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    assert_eq!(second, first);
    assert_eq!(latest_snapshot(&kv, catalog).unwrap(), Some(first));
}

#[test]
fn given_initialized_catalog_when_table_created_then_table_is_visible_at_new_snapshot() {
    let catalog = CatalogId(3);
    let table = TableId(7);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    let row = commit_create_table(&mut kv, catalog, table, "walking_skeleton").unwrap();
    let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(snapshot.sequence, ducklake_catalog::RawSnapshotSequence(1));
    assert_eq!(row.name, "walking_skeleton");
    assert_eq!(
        load_table_at(&kv, catalog, table, snapshot.order)
            .unwrap()
            .map(|table| table.name),
        Some("walking_skeleton".to_owned())
    );
}

#[test]
fn given_versionstamped_table_object_key_when_loaded_then_key_order_is_authoritative() {
    let catalog = CatalogId(3);
    let table = TableId(17);
    let stored_key_order = CatalogOrderId::fdb_versionstamp([2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 0);
    let placeholder_value_order =
        CatalogOrderId::fdb_versionstamp([0; CatalogOrderId::FDB_VERSIONSTAMP_LEN], 0);
    let row = TableRow::new(table, "fdb_table", placeholder_value_order);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    batch.put(
        table_object_key(catalog, table, stored_key_order),
        row.encode(),
    );
    kv.commit(batch).unwrap();

    let loaded = load_table_at(&kv, catalog, table, stored_key_order)
        .unwrap()
        .unwrap();
    let listed = list_tables_at(&kv, catalog, stored_key_order).unwrap();

    assert_eq!(loaded.validity.begin_order, stored_key_order);
    assert_eq!(
        loaded.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(listed, vec![loaded]);
}

#[test]
fn given_created_table_with_columns_when_catalog_listed_then_columns_are_visible() {
    let catalog = CatalogId(3);
    let table = TableId(7);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let table_row = TableRow::with_catalog_metadata(
        table,
        SchemaId(0),
        "table-uuid",
        "walking_skeleton",
        "main/walking_skeleton",
        vec![
            TableColumnRow::new(ColumnId(1), "id", "INTEGER", true, None),
            TableColumnRow::new(ColumnId(2), "note", "VARCHAR", true, None),
        ],
        CatalogOrderId::uuid_v7(0),
    );

    commit_create_table_row(&mut kv, catalog, table_row).unwrap();
    let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let tables = list_tables_at(&kv, catalog, snapshot.order).unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].schema_id, SchemaId(0));
    assert_eq!(tables[0].uuid, "table-uuid");
    assert_eq!(tables[0].name, "walking_skeleton");
    assert_eq!(tables[0].columns.len(), 2);
    assert_eq!(tables[0].columns[0].name, "id");
    assert_eq!(tables[0].columns[0].column_type, "INTEGER");
    assert_eq!(tables[0].columns[1].name, "note");
    assert_eq!(tables[0].columns[1].column_type, "VARCHAR");
}

#[test]
fn given_table_columns_appended_when_time_traveling_then_schema_versions_are_isolated() {
    let catalog = CatalogId(3);
    let table = TableId(7);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_table_row(
        &mut kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "table-uuid",
            "walking_skeleton",
            "main/walking_skeleton",
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
    let before = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let updated = commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(2),
            "note",
            "VARCHAR",
            true,
            None,
        )],
    )
    .unwrap();
    let after = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let historical = load_table_at(&kv, catalog, table, before.order)
        .unwrap()
        .unwrap();
    let current = load_table_at(&kv, catalog, table, after.order)
        .unwrap()
        .unwrap();

    assert_eq!(updated.columns.len(), 2);
    assert_eq!(historical.columns.len(), 1);
    assert_eq!(historical.columns[0].name, "id");
    assert_eq!(current.columns.len(), 2);
    assert_eq!(current.columns[1].name, "note");
    assert_eq!(current.validity.begin_order, after.order);
}
