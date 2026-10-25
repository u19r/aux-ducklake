use std::{
    cell::Cell,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use foundationdb::FdbError;
use futures::executor::block_on;

use super::FdbOrderedCatalogKv;
use crate::{
    CatalogError, CatalogResult, FakeOrderedCatalogKv, FoundationDbErrorClass, KvBatch,
    MutableCatalogKv, RangeDirection, RangeItem,
    fdb_async::MAX_FDB_READ_RETRIES,
    fdb_cleanup::{
        SyncFdbScanBudget, SyncFdbScanCursor, SyncFdbScanRequest, scan_pages,
        scan_range_all_with_kv_budget,
    },
    fdb_runtime::map_fdb_error,
};

#[test]
fn given_get_read_returns_1031_once_when_sync_policy_retries_then_value_is_returned() {
    let attempts = Cell::new(0usize);
    let result = FdbOrderedCatalogKv::retry_sync_fdb_read(
        "get",
        0usize,
        |state| {
            attempts.set(attempts.get().saturating_add(1));
            if *state == 0 {
                Err(FdbError::from_code(1031))
            } else {
                Ok(Some(b"value".to_vec()))
            }
        },
        |state, _| Ok(state.saturating_add(1)),
    );

    assert_eq!(result, Ok(Some(b"value".to_vec())));
    assert_eq!(attempts.get(), 2);
}

#[test]
fn given_batch_read_returns_1031_once_when_sync_policy_retries_then_cardinality_and_order_are_preserved()
 {
    let attempts = Cell::new(0usize);
    let result = FdbOrderedCatalogKv::retry_sync_fdb_read(
        "batch_get",
        0usize,
        |state| {
            attempts.set(attempts.get().saturating_add(1));
            if *state == 0 {
                Err(FdbError::from_code(1031))
            } else {
                Ok(vec![Some(b"first".to_vec()), None, Some(b"third".to_vec())])
            }
        },
        |state, _| Ok(state.saturating_add(1)),
    );

    assert_eq!(
        result,
        Ok(vec![Some(b"first".to_vec()), None, Some(b"third".to_vec()),])
    );
    assert_eq!(attempts.get(), 2);
}

#[test]
fn given_range_read_returns_1031_once_when_sync_policy_retries_then_continuation_order_is_preserved()
 {
    #[derive(Debug)]
    struct RangeReadState {
        attempt: usize,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    }

    let attempts = Cell::new(0usize);
    let result = FdbOrderedCatalogKv::retry_sync_fdb_read(
        "scan_range",
        RangeReadState {
            attempt: 0,
            start: b"start".to_vec(),
            end: b"end".to_vec(),
            limit: 3,
        },
        |state| {
            attempts.set(attempts.get().saturating_add(1));
            assert_eq!(state.start, b"start");
            assert_eq!(state.end, b"end");
            assert_eq!(state.limit, 3);
            if state.attempt == 0 {
                Err(FdbError::from_code(1031))
            } else {
                Ok(vec![
                    RangeItem {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    RangeItem {
                        key: b"b".to_vec(),
                        value: b"2".to_vec(),
                    },
                    RangeItem {
                        key: b"c".to_vec(),
                        value: b"3".to_vec(),
                    },
                ])
            }
        },
        |mut state, _| {
            state.attempt = state.attempt.saturating_add(1);
            Ok(state)
        },
    );

    let items = result.expect("range read should recover");
    assert_eq!(items.len(), 3);
    assert_eq!(
        items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
    assert_eq!(attempts.get(), 2);
}

#[test]
fn given_repeated_1031_reads_when_sync_policy_exhausts_then_final_typed_error_has_operation_context()
 {
    let attempts = Cell::new(0usize);
    let result: CatalogResult<()> = FdbOrderedCatalogKv::retry_sync_fdb_read(
        "scan_range",
        0usize,
        |_| {
            attempts.set(attempts.get().saturating_add(1));
            Err(FdbError::from_code(1031))
        },
        |state, _| Ok(state.saturating_add(1)),
    );

    let error = result.expect_err("retry budget should be exhausted");
    assert!(matches!(
        error,
        CatalogError::FoundationDbRetryExhausted {
            operation: "scan_range",
            attempts: MAX_FDB_READ_ATTEMPTS,
            code: 1031,
            class: FoundationDbErrorClass::RetryableNotCommitted,
            ..
        }
    ));
    assert_eq!(attempts.get(), MAX_FDB_READ_ATTEMPTS);
}

#[test]
fn given_nonretryable_read_failure_when_sync_policy_runs_then_error_is_returned_unchanged() {
    let attempts = Cell::new(0usize);
    let result: CatalogResult<()> = FdbOrderedCatalogKv::retry_sync_fdb_read(
        "get",
        0usize,
        |_| {
            attempts.set(attempts.get().saturating_add(1));
            Err(FdbError::from_code(2004))
        },
        |state, _| Ok(state.saturating_add(1)),
    );

    let error = result.expect_err("nonretryable errors must not retry");
    assert!(matches!(
        error,
        CatalogError::FoundationDb {
            code: 2004,
            class: FoundationDbErrorClass::NonRetryable,
            ..
        }
    ));
    assert_eq!(attempts.get(), 1);
}

#[test]
fn given_pinned_read_version_when_sync_retry_resets_then_reset_transaction_keeps_version() {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
        return;
    }
    let _guard = crate::FDB_ENV_LOCK.lock().unwrap();
    let prefix = format!(
        "aux-ducklake-test/pinned-read-version/{}/{}/",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut writer = FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let key = b"pinned-read-version".to_vec();
    let mut batch = KvBatch::new();
    batch.put(key.clone(), b"before".to_vec());
    writer.commit(batch).unwrap();

    let pinned = writer.with_read_version().unwrap();
    let mut newer_writer = writer.clone();
    let mut batch = KvBatch::new();
    batch.put(key.clone(), b"after".to_vec());
    newer_writer.commit(batch).unwrap();

    let transaction = pinned
        .reset_read_transaction_after_error(
            pinned.create_transaction().unwrap(),
            FdbError::from_code(1021),
        )
        .unwrap();
    let value = block_on(transaction.get(&pinned.namespaced_key(&key), false))
        .unwrap()
        .map(|value| value.to_vec());
    assert_eq!(value, Some(b"before".to_vec()));

    let database = crate::fdb_runtime::shared_foundationdb_database(
        std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE")
            .ok()
            .as_deref(),
    )
    .unwrap();
    crate::clear_foundationdb_prefix(&database, prefix.as_bytes()).unwrap();
}

const MAX_FDB_READ_ATTEMPTS: usize = MAX_FDB_READ_RETRIES + 1;

#[test]
fn given_unbounded_forward_range_when_sync_catalog_scans_then_transport_pages_remain_finite() {
    let cursor = SyncFdbScanCursor::try_new_with_limit(
        vec![0],
        vec![u8::MAX],
        RangeDirection::Forward,
        256,
        usize::MAX,
    )
    .expect("scan cursor should be valid");
    let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));
    let mut page_limits = Vec::new();
    scan_pages(cursor, &mut budget, |_, page_size, _| {
        page_limits.push(page_size);
        Ok(Vec::new())
    })
    .expect("empty range should finish");

    assert_eq!(page_limits, vec![256]);
    assert!(page_limits.iter().all(|page_size| *page_size < usize::MAX));
}

#[test]
fn given_large_forward_range_when_sync_catalog_scans_then_all_rows_are_returned_once() {
    let kv = fixture_kv(600);
    let request = SyncFdbScanRequest::new(&[0, 0], &[u8::MAX, u8::MAX], RangeDirection::Forward);
    let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));

    let items = scan_range_all_with_kv_budget(&kv, request, &mut budget).unwrap();

    assert_eq!(items.len(), 600);
    assert_eq!(items, fixture_items(600));
    assert_unique_and_ordered(&items, RangeDirection::Forward);
}

#[test]
fn given_large_reverse_range_when_sync_catalog_scans_then_all_rows_are_returned_once() {
    let kv = fixture_kv(600);
    let request = SyncFdbScanRequest::new(&[0, 0], &[u8::MAX, u8::MAX], RangeDirection::Reverse);
    let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));

    let items = scan_range_all_with_kv_budget(&kv, request, &mut budget).unwrap();

    assert_eq!(items.len(), 600);
    assert_eq!(
        items,
        fixture_items(600).into_iter().rev().collect::<Vec<_>>()
    );
    assert_unique_and_ordered(&items, RangeDirection::Reverse);
}

#[test]
fn given_finite_limit_crossing_page_boundary_when_sync_catalog_scans_then_limit_is_preserved() {
    let kv = fixture_kv(600);
    let request =
        SyncFdbScanRequest::with_limit(&[0, 0], &[u8::MAX, u8::MAX], RangeDirection::Forward, 300);
    let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));

    let items = scan_range_all_with_kv_budget(&kv, request, &mut budget).unwrap();

    assert_eq!(items, fixture_items(300));
}

#[test]
fn given_finite_reverse_limit_crossing_page_boundary_when_sync_catalog_scans_then_limit_is_preserved()
 {
    let kv = fixture_kv(600);
    let request =
        SyncFdbScanRequest::with_limit(&[0, 0], &[u8::MAX, u8::MAX], RangeDirection::Reverse, 300);
    let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));

    let items = scan_range_all_with_kv_budget(&kv, request, &mut budget).unwrap();

    assert_eq!(
        items,
        fixture_items(600)
            .into_iter()
            .rev()
            .take(300)
            .collect::<Vec<_>>()
    );
}

#[test]
fn given_malformed_or_non_progressing_page_when_sync_catalog_scans_then_page_is_rejected() {
    let malformed_cursor = SyncFdbScanCursor::try_new_with_limit(
        vec![0],
        vec![10],
        RangeDirection::Forward,
        2,
        usize::MAX,
    )
    .unwrap();
    let mut malformed_budget =
        SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));
    let malformed = scan_pages(malformed_cursor, &mut malformed_budget, |_, _, _| {
        Ok(vec![RangeItem {
            key: vec![10],
            value: vec![],
        }])
    })
    .expect_err("a key at the exclusive end must be rejected");
    assert!(matches!(malformed, CatalogError::InvalidMutation(_)));

    let progressing_cursor = SyncFdbScanCursor::try_new_with_limit(
        vec![0],
        vec![10],
        RangeDirection::Forward,
        2,
        usize::MAX,
    )
    .unwrap();
    let mut progressing_budget =
        SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));
    let reads = Cell::new(0usize);
    let non_progressing = scan_pages(progressing_cursor, &mut progressing_budget, |_, _, _| {
        let read = reads.get();
        reads.set(read.saturating_add(1));
        if read == 0 {
            Ok(vec![
                RangeItem {
                    key: vec![1],
                    value: vec![],
                },
                RangeItem {
                    key: vec![2],
                    value: vec![],
                },
            ])
        } else {
            Ok(vec![
                RangeItem {
                    key: vec![2],
                    value: vec![],
                },
                RangeItem {
                    key: vec![3],
                    value: vec![],
                },
            ])
        }
    })
    .expect_err("a repeated continuation key must be rejected");
    assert!(matches!(non_progressing, CatalogError::InvalidMutation(_)));
}

#[test]
fn given_retryable_failure_after_a_page_when_sync_scan_retries_then_budget_is_shared() {
    let cursor = SyncFdbScanCursor::try_new_with_limit(
        vec![0],
        vec![10],
        RangeDirection::Forward,
        2,
        usize::MAX,
    )
    .unwrap();
    let mut budget = SyncFdbScanBudget::with_operation_limits(
        "scan_prefix",
        1,
        Instant::now() + Duration::from_secs(5),
    );
    let reads = Cell::new(0usize);
    let error = scan_pages(cursor, &mut budget, |_, _, _| {
        let read = reads.get();
        reads.set(read.saturating_add(1));
        if read == 0 {
            return Ok(vec![
                RangeItem {
                    key: vec![1],
                    value: vec![],
                },
                RangeItem {
                    key: vec![2],
                    value: vec![],
                },
            ]);
        }
        Err(map_fdb_error(foundationdb::FdbError::from_code(1031)))
    })
    .expect_err("the page retry budget must apply across the operation");

    assert!(matches!(
        error,
        CatalogError::FoundationDbRetryExhausted {
            operation: "scan_prefix",
            attempts: 3,
            code: 1031,
            class: FoundationDbErrorClass::RetryableNotCommitted,
            ..
        }
    ));
    assert_eq!(reads.get(), 3);
}

fn fixture_kv(count: usize) -> FakeOrderedCatalogKv {
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for index in 0..count {
        batch.put(fixture_key(index), vec![index as u8]);
    }
    kv.commit(batch).unwrap();
    kv
}

fn fixture_items(count: usize) -> Vec<RangeItem> {
    (0..count)
        .map(|index| RangeItem {
            key: fixture_key(index),
            value: vec![index as u8],
        })
        .collect()
}

fn fixture_key(index: usize) -> Vec<u8> {
    (index as u16).to_be_bytes().to_vec()
}

fn assert_unique_and_ordered(items: &[RangeItem], direction: RangeDirection) {
    let keys = items.iter().map(|item| &item.key).collect::<Vec<_>>();
    let mut unique = keys.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), keys.len());
    assert!(items.windows(2).all(|pair| match direction {
        RangeDirection::Forward => pair[0].key < pair[1].key,
        RangeDirection::Reverse => pair[0].key > pair[1].key,
    }));
}
