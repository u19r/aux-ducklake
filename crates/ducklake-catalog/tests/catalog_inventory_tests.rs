use ducklake_catalog::{
    CatalogId, FakeOrderedCatalogKv, KvBatch, RangeDirection, SUPPORTED_DUCKLAKE_COMMIT,
    initialize_empty_catalog,
    keys::{conflict_fence_key, decode_key, snapshot_prefix},
    latest_snapshot,
    workload::{ReleaseReadinessNeed, operation_inventory, risk_inventory},
};

#[test]
fn given_empty_catalog_when_initialized_then_latest_snapshot_is_read_by_reverse_scan() {
    let catalog = CatalogId(42);
    let mut kv = FakeOrderedCatalogKv::new();

    let inserted = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(latest, inserted);
    assert_eq!(latest.sequence, ducklake_catalog::RawSnapshotSequence(0));
}

#[test]
fn given_multiple_snapshots_when_reverse_scanning_then_latest_order_sorts_last() {
    let catalog = CatalogId(7);
    let mut kv = FakeOrderedCatalogKv::new();

    let first = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let second = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert!(first.order < second.order);
    assert_eq!(latest.order, second.order);
}

#[test]
fn given_snapshot_prefix_when_scanned_forward_or_reverse_then_byte_order_is_stable() {
    let catalog = CatalogId(7);
    let mut kv = FakeOrderedCatalogKv::new();
    let first = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let second = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let prefix = snapshot_prefix(catalog);

    let forward = kv.scan_prefix(&prefix, RangeDirection::Forward, 2);
    let reverse = kv.scan_prefix(&prefix, RangeDirection::Reverse, 2);

    assert!(forward[0].key < forward[1].key);
    assert!(reverse[0].key > reverse[1].key);
    assert!(forward[0].key.ends_with(&first.order.as_bytes()));
    assert!(forward[1].key.ends_with(&second.order.as_bytes()));
}

#[test]
fn given_conflict_fence_check_when_fence_changes_then_batch_fails_without_writes() {
    let catalog = CatalogId(5);
    let mut kv = FakeOrderedCatalogKv::new();
    let fence = conflict_fence_key(catalog, b"table/1");
    let observed = kv.read_conflict_fence(&fence);

    kv.write_conflict_fence(fence.clone());
    let mut batch = KvBatch::new();
    batch.check_value(fence.clone(), observed);
    batch.put(b"unpublished".to_vec(), b"value".to_vec());

    let error = kv.commit(batch).unwrap_err();
    assert!(error.to_string().contains("conflict fence changed"));
    assert_eq!(kv.get(b"unpublished"), None);
}

#[test]
fn given_operation_inventory_then_every_operation_has_a_typed_call_and_no_sql_path() {
    assert_eq!(SUPPORTED_DUCKLAKE_COMMIT.len(), 40);
    for item in operation_inventory() {
        assert!(!item.typed_call.is_empty(), "{item:?}");
        assert!(
            !item.typed_call.to_ascii_lowercase().contains("sql"),
            "{item:?}"
        );
        assert!(!item.transaction_boundary.is_empty(), "{item:?}");
        assert!(!item.maximum_mutation_size.is_empty(), "{item:?}");
    }
}

#[test]
fn given_release_readiness_risks_then_each_risk_has_classification_and_fixture_owner() {
    let risks = risk_inventory();
    assert!(
        risks
            .iter()
            .any(|risk| risk.need == ReleaseReadinessNeed::RequiredBeforeFdbProof)
    );
    assert!(
        risks
            .iter()
            .any(|risk| risk.need == ReleaseReadinessNeed::RequiredBeforeDuckLakeSmoke)
    );
    assert!(
        risks
            .iter()
            .any(|risk| risk.need == ReleaseReadinessNeed::BenchmarkGatedDeferred)
    );
    for risk in risks {
        assert!(!risk.mitigation.is_empty(), "{risk:?}");
        assert!(!risk.fixture_owner.is_empty(), "{risk:?}");
    }
}

#[test]
fn given_catalog_key_when_decoded_then_logs_are_human_readable() {
    let catalog = CatalogId(9);
    let mut kv = FakeOrderedCatalogKv::new();
    let row = initialize_empty_catalog(&mut kv, catalog).unwrap();
    let latest = kv
        .scan_prefix(&snapshot_prefix(catalog), RangeDirection::Reverse, 1)
        .pop()
        .unwrap();

    let decoded = decode_key(&latest.key).unwrap();

    assert!(decoded.contains("catalog=9"));
    assert!(decoded.contains("family=snapshot"));
    assert!(decoded.contains(&row.order.to_string()));
}
