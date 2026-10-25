use std::time::{Duration, Instant};

use foundationdb::FdbError;

use super::{ScanBudget, ScanCursor, ScanLimit, materialized_partial_file_requires_delete_lookup};
use crate::{
    CatalogError, CatalogOrderId, DataFileId, DataFileRow, FoundationDbErrorClass, RangeDirection,
    RangeItem, TableId, fdb_runtime::map_fdb_error,
};

#[test]
fn given_multi_page_fixture_when_advancing_scan_cursor_then_pages_are_bounded_and_ordered() {
    let mut cursor = ScanCursor::new(
        b"files/".to_vec(),
        b"files0".to_vec(),
        RangeDirection::Forward,
        2,
        ScanLimit::All,
    )
    .expect("fixture scan page size is valid");
    let pages = [
        page(&[(b"files/001", b"one"), (b"files/002", b"two")]),
        page(&[(b"files/003", b"three"), (b"files/004", b"four")]),
        page(&[(b"files/005", b"five")]),
    ];

    for page in pages {
        assert!(cursor.wants_page());
        assert!(page.len() <= cursor.page_size());
        let should_continue = cursor.accept_page(page).expect("fixture page is valid");
        if should_continue {
            assert!(cursor.page_size() <= 2);
        }
    }

    assert!(!cursor.wants_page());
    assert_eq!(cursor.start(), b"files/005\0");
    assert_eq!(
        cursor
            .into_items()
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"files/001".as_slice(),
            b"files/002".as_slice(),
            b"files/003".as_slice(),
            b"files/004".as_slice(),
            b"files/005".as_slice(),
        ]
    );
}

#[test]
fn given_reverse_multi_page_fixture_when_advancing_scan_cursor_then_continuation_excludes_page() {
    let mut cursor = ScanCursor::new(
        b"files/".to_vec(),
        b"files0".to_vec(),
        RangeDirection::Reverse,
        2,
        ScanLimit::All,
    )
    .expect("fixture scan page size is valid");
    let fixture = page(&[
        (b"files/005", b"five"),
        (b"files/004", b"four"),
        (b"files/003", b"three"),
        (b"files/002", b"two"),
        (b"files/001", b"one"),
    ]);

    while cursor.wants_page() {
        let page = fixture
            .iter()
            .filter(|item| item.key.as_slice() >= cursor.start())
            .filter(|item| item.key.as_slice() < cursor.end())
            .take(cursor.page_size())
            .cloned()
            .collect();
        cursor
            .accept_page(page)
            .expect("reverse continuation must not repeat the prior page");
    }

    assert_eq!(
        cursor
            .into_items()
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"files/005".as_slice(),
            b"files/004".as_slice(),
            b"files/003".as_slice(),
            b"files/002".as_slice(),
            b"files/001".as_slice(),
        ]
    );
}

#[test]
fn given_retryable_1031_failures_when_scan_budget_is_exhausted_then_error_is_typed() {
    let mut budget = ScanBudget::with_limits(2, Instant::now() + Duration::from_secs(1));
    let retryable = map_fdb_error(FdbError::from_code(1031));

    for _ in 0..2 {
        budget
            .begin_attempt()
            .expect("deadline should allow the injected attempt");
        budget
            .record_retryable_failure(retryable.clone())
            .expect("retry budget should allow the first failures");
    }
    budget
        .begin_attempt()
        .expect("deadline should allow the final injected attempt");
    let error = budget
        .record_retryable_failure(retryable)
        .expect_err("the operation-wide retry budget must be exhausted");

    assert!(matches!(
        error,
        CatalogError::FoundationDbRetryExhausted {
            operation: "scan_range",
            attempts: 3,
            code: 1031,
            class: FoundationDbErrorClass::RetryableNotCommitted,
            ..
        }
    ));
}

#[test]
fn given_expired_scan_deadline_when_starting_page_then_operation_is_rejected() {
    let mut budget = ScanBudget::with_limits(2, Instant::now() - Duration::from_millis(1));

    let error = budget
        .begin_attempt()
        .expect_err("an expired operation deadline must not start another page");

    assert!(matches!(
        error,
        CatalogError::InvalidMutation(message) if message.contains("async range scan deadline")
    ));
}

#[test]
fn given_materialized_partial_file_at_begin_snapshot_when_prechecking_deletes_then_lookup_is_required()
 {
    let file = data_file(CatalogOrderId::uuid_v7(10))
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));

    assert!(materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(10)
    ));
    assert!(materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(15)
    ));
    assert!(materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(20)
    ));
}

#[test]
fn given_materialized_partial_file_before_begin_snapshot_when_prechecking_deletes_then_lookup_is_not_forced()
 {
    let file = data_file(CatalogOrderId::uuid_v7(10))
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));

    assert!(!materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(9)
    ));
}

#[test]
fn given_materialized_partial_file_after_partial_span_when_prechecking_deletes_then_lookup_is_still_required()
 {
    let file = data_file(CatalogOrderId::uuid_v7(10))
        .with_max_partial_order(Some(CatalogOrderId::uuid_v7(20)));

    assert!(materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(21)
    ));
}

#[test]
fn given_ordinary_file_when_prechecking_deletes_then_lookup_is_not_forced() {
    let file = data_file(CatalogOrderId::uuid_v7(10));

    assert!(!materialized_partial_file_requires_delete_lookup(
        &file,
        CatalogOrderId::uuid_v7(10)
    ));
}

fn page(items: &[(&[u8], &[u8])]) -> Vec<RangeItem> {
    items
        .iter()
        .map(|(key, value)| RangeItem {
            key: (*key).to_vec(),
            value: (*value).to_vec(),
        })
        .collect()
}

fn data_file(begin_order: CatalogOrderId) -> DataFileRow {
    DataFileRow::new(
        DataFileId(1),
        TableId(10),
        "main/t/file.parquet",
        1,
        128,
        begin_order,
    )
}
