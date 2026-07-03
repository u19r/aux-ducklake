use super::*;
use crate::{
    CatalogOrderId, FdbOrderedCatalogKv, INLINE_PAYLOAD_LIMIT_BYTES,
    inline_data::validate_inline_table_rows_fit_fdb,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn inline_row_limit_stays_under_foundationdb_item_limit() {
    assert_eq!(INLINE_PAYLOAD_LIMIT_BYTES, 90 * 1024);
    assert!(INLINE_PAYLOAD_LIMIT_BYTES < 100 * 1024);
    assert!(
        validate_inline_table_rows_fit_fdb(&small_inline_rows_payload(
            INLINE_PAYLOAD_LIMIT_BYTES + 1
        ))
        .is_ok()
    );
    assert!(validate_inline_table_rows_fit_fdb(&oversized_inline_row_payload()).is_err());
}

#[test]
fn row_change_heavy_inline_payloads_are_rejected_before_commit() {
    let catalog = CatalogId(9);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let row = InlineTableChunkRow::new(
        TableId(33),
        SchemaId(44),
        ValidityWindow::new(incomplete_order(), None),
        0,
        1,
        vec![b'x'; INLINE_PAYLOAD_LIMIT_BYTES],
    );
    let row_changes = (0..25_000)
        .map(|index| {
            let key = table_inline_row_change_key(
                catalog,
                row.table_id,
                snapshot.order,
                InlineRowChangeKind::Inserted,
                row.schema_id,
                index,
            );
            VersionstampedInlineChangeKey {
                order_offset: table_inline_row_change_prefix(catalog, row.table_id).len(),
                key,
            }
        })
        .collect::<Vec<_>>();

    let estimated_bytes =
        estimate_inline_payload_bytes(catalog, &snapshot, None, &[row], &row_changes);

    assert!(estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES);
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

#[test]
fn row_change_heavy_inline_deletes_are_rejected_before_commit() {
    let catalog = CatalogId(9);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let table_id = TableId(33);
    let schema_id = SchemaId(44);
    let deleted_rows = (0..25_000).collect::<Vec<_>>();

    let estimated_bytes =
        estimate_inline_delete_bytes(catalog, &snapshot, table_id, schema_id, &deleted_rows);

    assert!(estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES);
}

#[test]
fn inline_table_end_order_offset_points_to_raw_versionstamp_bytes() {
    let end_order = CatalogOrderId::fdb_versionstamp([7; CatalogOrderId::FDB_VERSIONSTAMP_LEN], 0);
    let row = InlineTableChunkRow::new(
        TableId(33),
        SchemaId(44),
        ValidityWindow::new(CatalogOrderId::uuid_v7(7), Some(end_order)),
        0,
        1,
        b"row\t1\tvalue\n".to_vec(),
    );

    let encoded = row.encode();
    let offset = InlineTableChunkRow::END_ORDER_BYTES_OFFSET;

    assert_eq!(encoded[offset - 2], 1);
    assert_eq!(encoded[offset - 1], 1);
    assert_eq!(
        &encoded[offset..offset + CatalogOrderId::FDB_VERSIONSTAMP_LEN],
        &[7; CatalogOrderId::FDB_VERSIONSTAMP_LEN]
    );
}

#[cfg(feature = "foundationdb")]
#[test]
fn fdb_live_given_two_inline_snapshot_publishers_read_same_catalog_when_one_commits_then_other_conflicts()
 {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(7301);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-snapshot-conflict").as_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();

    let first = kv.create_transaction().unwrap();
    let second = kv.create_transaction().unwrap();
    add_snapshot_prefix_conflict(&kv, &first, catalog).unwrap();
    add_snapshot_prefix_conflict(&kv, &second, catalog).unwrap();

    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    stage_snapshot(&kv, &first, catalog, &snapshot).unwrap();
    futures::executor::block_on(first.commit()).unwrap();

    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    stage_snapshot(&kv, &second, catalog, &snapshot).unwrap();
    let error = futures::executor::block_on(second.commit()).unwrap_err();
    let error = crate::fdb_runtime::map_fdb_commit_error(error);

    assert!(
        error.to_string().contains("conflict")
            || error.to_string().contains("not_committed")
            || error.to_string().contains("retryable")
    );
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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
