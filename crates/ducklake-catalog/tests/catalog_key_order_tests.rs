use ducklake_catalog::{
    CatalogId, CatalogOrderId, CatalogOrderKind, FakeOrderedCatalogKv, TableId,
    create_table_version,
    keys::{decode_key, table_object_key},
    load_table_at,
};

#[test]
fn given_versioned_table_when_snapshot_order_changes_then_visibility_is_correct() {
    let catalog = CatalogId(1);
    let table_id = TableId(100);
    let before_table = CatalogOrderId::uuid_v7(10);
    let table_begin = CatalogOrderId::uuid_v7(20);
    let after_table = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();

    let created = create_table_version(&mut kv, catalog, table_id, "orders", table_begin).unwrap();

    assert_eq!(
        load_table_at(&kv, catalog, table_id, before_table).unwrap(),
        None
    );
    assert_eq!(
        load_table_at(&kv, catalog, table_id, table_begin).unwrap(),
        Some(created.clone())
    );
    assert_eq!(
        load_table_at(&kv, catalog, table_id, after_table).unwrap(),
        Some(created)
    );
}

#[test]
fn given_generated_order_ids_when_allocated_then_ids_are_not_reused() {
    let mut kv = FakeOrderedCatalogKv::new();

    let first = kv.generated_order_id();
    let second = kv.generated_order_id();
    let third = kv.generated_order_id();

    assert!(first < second);
    assert!(second < third);
    assert_eq!(first.kind(), CatalogOrderKind::UuidV7);
}

#[test]
fn given_fdb_versionstamp_order_when_encoded_then_suffix_preserves_in_transaction_order() {
    let versionstamp = [7, 0, 0, 0, 0, 0, 0, 0, 0, 1];

    let first = CatalogOrderId::fdb_versionstamp(versionstamp, 1);
    let second = CatalogOrderId::fdb_versionstamp(versionstamp, 2);

    assert_eq!(first.kind(), CatalogOrderKind::FdbVersionstamp);
    assert!(first < second);
}

#[test]
fn given_table_object_key_when_decoded_then_family_and_tail_are_readable() {
    let key = table_object_key(CatalogId(2), TableId(9), CatalogOrderId::uuid_v7(33));

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=2"));
    assert!(decoded.contains("family=object"));
    assert!(decoded.contains("0000000000000021"));
}

#[test]
fn given_bad_catalog_key_when_decoded_then_error_names_the_shape_problem() {
    let cases = [
        (Vec::new(), "key too short: 0 bytes"),
        (vec![0; 8], "key too short: 8 bytes"),
        (
            vec![0, 0, 0, 0, 0, 0, 0, 1, b'x', b's'],
            "missing catalog separator",
        ),
        (
            vec![0, 0, 0, 0, 0, 0, 0, 1, b'/', b'?'],
            "unknown family byte 0x3f",
        ),
    ];

    for (key, expected) in cases {
        let error = decode_key(&key).unwrap_err();
        assert_eq!(error.to_string(), format!("invalid key: {expected}"));
    }
}
