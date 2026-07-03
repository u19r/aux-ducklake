use ducklake_catalog::{
    CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    INLINE_PAYLOAD_LIMIT_BYTES, InlineDeletionChunkRow, InlineTableChunkRow,
    InlineTablePayloadCommit, RangeDirection, SchemaId, TableId,
    keys::{KeyFamily, family_prefix},
    latest_snapshot, list_current_data_files, list_inline_deletion_debug_chunks,
    list_inline_table_debug_chunks, load_inline_deletion_payload_at, load_inline_table_payload_at,
    register_inline_deletion_payload, register_inline_table_payload,
    route_inline_table_payload_or_data_file,
};

#[test]
fn given_inline_payload_at_limit_when_registered_then_chunks_round_trip_in_order() {
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(2);
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = small_inline_rows_payload(INLINE_PAYLOAD_LIMIT_BYTES);

    let chunks =
        register_inline_table_payload(&mut kv, catalog, table, schema, payload.clone()).unwrap();

    assert!(chunks.len() > 1);
    assert_eq!(chunks[0].chunk_index, 0);
    assert_eq!(
        chunks[chunks.len() - 1].chunk_index as usize,
        chunks.len() - 1
    );
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.chunk_count as usize == chunks.len())
    );
    let begin_order = chunks[0].validity.begin_order;
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        begin_order
    );
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, begin_order).unwrap(),
        Some(payload)
    );
}

#[test]
fn given_many_small_inline_rows_above_limit_when_registered_then_chunks_are_written() {
    let catalog = CatalogId(2);
    let table = TableId(11);
    let schema = SchemaId(3);
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = small_inline_rows_payload(INLINE_PAYLOAD_LIMIT_BYTES + 1);

    let chunks =
        register_inline_table_payload(&mut kv, catalog, table, schema, payload.clone()).unwrap();
    let begin_order = chunks[0].validity.begin_order;

    assert!(chunks.len() > 1);
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, begin_order).unwrap(),
        Some(payload)
    );
}

#[test]
fn given_single_inline_row_above_limit_when_routed_then_data_file_path_commits_without_inline_chunks()
 {
    let catalog = CatalogId(7);
    let table = TableId(16);
    let schema = SchemaId(5);
    let fallback = DataFileRow::new(
        DataFileId(201),
        table,
        "main/inline-fallback-0001.parquet",
        50,
        4096,
        CatalogOrderId::uuid_v7(0),
    );
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = oversized_inline_row_payload();

    let result =
        route_inline_table_payload_or_data_file(&mut kv, catalog, table, schema, payload, fallback)
            .unwrap();

    let InlineTablePayloadCommit::FileBacked(files) = result else {
        panic!("oversized inline payload should route to data-file path");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].data_file_id, DataFileId(201));
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        files[0].validity.begin_order
    );
    assert_eq!(list_current_data_files(&kv, catalog, table).unwrap(), files);
    assert!(
        kv.scan_prefix(
            &family_prefix(catalog, KeyFamily::InlineTable),
            RangeDirection::Forward,
            usize::MAX,
        )
        .is_empty()
    );
}

#[test]
fn given_single_inline_row_above_limit_when_registered_then_no_chunks_are_written() {
    let catalog = CatalogId(8);
    let table = TableId(17);
    let schema = SchemaId(6);
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = oversized_inline_row_payload();

    let error =
        register_inline_table_payload(&mut kv, catalog, table, schema, payload).unwrap_err();

    assert!(
        error
            .to_string()
            .contains(&format!("over {INLINE_PAYLOAD_LIMIT_BYTES} byte limit"))
    );
    assert!(
        kv.scan_prefix(
            &family_prefix(catalog, KeyFamily::InlineTable),
            RangeDirection::Forward,
            usize::MAX,
        )
        .is_empty()
    );
}

#[test]
fn given_multiple_inline_payloads_when_read_at_snapshot_then_latest_visible_payload_is_returned() {
    let catalog = CatalogId(3);
    let table = TableId(12);
    let schema = SchemaId(4);
    let mut kv = FakeOrderedCatalogKv::new();
    let first_payload = b"row\t1\ts:first-inline-row\n".to_vec();
    let second_payload = b"row\t2\ts:second-inline-row\n".to_vec();

    let first =
        register_inline_table_payload(&mut kv, catalog, table, schema, first_payload.clone())
            .unwrap();
    let second =
        register_inline_table_payload(&mut kv, catalog, table, schema, second_payload.clone())
            .unwrap();

    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, CatalogOrderId::uuid_v7(0))
            .unwrap(),
        None
    );
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, first_begin(&first)).unwrap(),
        Some(first_payload)
    );
    assert_eq!(
        load_inline_table_payload_at(&kv, catalog, table, schema, first_begin(&second)).unwrap(),
        Some(second_payload)
    );
}

#[test]
fn given_inline_deletion_payload_at_limit_when_registered_then_chunks_round_trip_in_order() {
    let catalog = CatalogId(4);
    let table = TableId(13);
    let data_file = DataFileId(100);
    let row_id = 7;
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = repeating_payload(INLINE_PAYLOAD_LIMIT_BYTES);

    let chunks = register_inline_deletion_payload(
        &mut kv,
        catalog,
        table,
        data_file,
        row_id,
        payload.clone(),
    )
    .unwrap();

    assert!(chunks.len() > 1);
    assert_eq!(chunks[0].chunk_index, 0);
    assert_eq!(
        chunks[chunks.len() - 1].chunk_index as usize,
        chunks.len() - 1
    );
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.chunk_count as usize == chunks.len())
    );
    let begin_order = chunks[0].validity.begin_order;
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        begin_order
    );
    assert_eq!(
        load_inline_deletion_payload_at(&kv, catalog, table, data_file, row_id, begin_order)
            .unwrap(),
        Some(payload)
    );
}

#[test]
fn given_inline_deletion_payload_above_limit_when_registered_then_no_chunks_are_written() {
    let catalog = CatalogId(5);
    let table = TableId(14);
    let data_file = DataFileId(101);
    let row_id = 8;
    let mut kv = FakeOrderedCatalogKv::new();
    let payload = repeating_payload(INLINE_PAYLOAD_LIMIT_BYTES + 1);

    let error =
        register_inline_deletion_payload(&mut kv, catalog, table, data_file, row_id, payload)
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains(&format!("over {INLINE_PAYLOAD_LIMIT_BYTES} byte limit"))
    );
    assert!(
        kv.scan_prefix(
            &family_prefix(catalog, KeyFamily::InlineDeletion),
            RangeDirection::Forward,
            usize::MAX,
        )
        .is_empty()
    );
}

#[test]
fn given_multiple_inline_deletions_when_read_at_snapshot_then_latest_visible_payload_is_returned() {
    let catalog = CatalogId(6);
    let table = TableId(15);
    let data_file = DataFileId(102);
    let row_id = 9;
    let mut kv = FakeOrderedCatalogKv::new();
    let first_payload = b"first-inline-delete".to_vec();
    let second_payload = b"second-inline-delete".to_vec();

    let first = register_inline_deletion_payload(
        &mut kv,
        catalog,
        table,
        data_file,
        row_id,
        first_payload.clone(),
    )
    .unwrap();
    let second = register_inline_deletion_payload(
        &mut kv,
        catalog,
        table,
        data_file,
        row_id,
        second_payload.clone(),
    )
    .unwrap();

    assert_eq!(
        load_inline_deletion_payload_at(
            &kv,
            catalog,
            table,
            data_file,
            row_id,
            CatalogOrderId::uuid_v7(0),
        )
        .unwrap(),
        None
    );
    assert_eq!(
        load_inline_deletion_payload_at(
            &kv,
            catalog,
            table,
            data_file,
            row_id,
            first_delete_begin(&first),
        )
        .unwrap(),
        Some(first_payload)
    );
    assert_eq!(
        load_inline_deletion_payload_at(
            &kv,
            catalog,
            table,
            data_file,
            row_id,
            first_delete_begin(&second),
        )
        .unwrap(),
        Some(second_payload)
    );
}

#[test]
fn given_inline_chunks_when_debug_exported_then_rows_are_decoded_and_bounded() {
    let catalog = CatalogId(8);
    let table = TableId(17);
    let schema = SchemaId(6);
    let data_file = DataFileId(202);
    let mut kv = FakeOrderedCatalogKv::new();
    let table_payload = b"row\t1\ts:debug-inline-table\n".to_vec();
    let deletion_payload = b"debug-inline-deletion".to_vec();

    let table_rows =
        register_inline_table_payload(&mut kv, catalog, table, schema, table_payload.clone())
            .unwrap();
    register_inline_table_payload(
        &mut kv,
        catalog,
        table,
        schema,
        b"row\t2\ts:second-debug-row\n".to_vec(),
    )
    .unwrap();
    let deletion_rows = register_inline_deletion_payload(
        &mut kv,
        catalog,
        table,
        data_file,
        19,
        deletion_payload.clone(),
    )
    .unwrap();

    let exported_tables = list_inline_table_debug_chunks(&kv, catalog, 1).unwrap();
    let exported_deletions = list_inline_deletion_debug_chunks(&kv, catalog, 10).unwrap();

    assert_eq!(exported_tables.len(), 1);
    assert_eq!(exported_tables[0], table_rows[0]);
    assert_eq!(exported_tables[0].payload, table_payload);
    assert_eq!(exported_deletions, deletion_rows);
    assert_eq!(exported_deletions[0].payload, deletion_payload);
}

fn first_begin(rows: &[InlineTableChunkRow]) -> CatalogOrderId {
    rows[0].validity.begin_order
}

fn first_delete_begin(rows: &[InlineDeletionChunkRow]) -> CatalogOrderId {
    rows[0].validity.begin_order
}

fn repeating_payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn small_inline_rows_payload(min_len: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut row_id = 0_u64;
    while payload.len() <= min_len {
        payload.extend_from_slice(format!("row\t{row_id}\ti:{row_id}\n").as_bytes());
        row_id += 1;
    }
    payload
}

fn oversized_inline_row_payload() -> Vec<u8> {
    let mut payload = b"row\t1\ts:".to_vec();
    payload.extend(std::iter::repeat_n(b'x', INLINE_PAYLOAD_LIMIT_BYTES));
    payload.push(b'\n');
    payload
}
