#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        time::{Duration, Instant},
    };

    use foundationdb::FdbError;

    use super::super::{SyncFdbScanBudget, SyncFdbScanCursor, scan_pages};
    use crate::{
        CatalogError, FoundationDbErrorClass, RangeDirection, RangeItem, fdb_runtime::map_fdb_error,
    };

    #[test]
    fn given_large_multi_page_fixture_with_retryable_failures_when_scanning_then_all_candidates_are_ordered_once()
     {
        let fixture = (0..24)
            .map(|index| RangeItem {
                key: vec![index],
                value: vec![index.saturating_add(1)],
            })
            .collect::<Vec<_>>();
        let cursor =
            SyncFdbScanCursor::try_new(vec![0], vec![u8::MAX], RangeDirection::Forward, 4).unwrap();
        let mut budget = SyncFdbScanBudget::with_limits(8, Instant::now() + Duration::from_secs(5));
        let attempts = Cell::new(0usize);
        let retryable_errors = Cell::new(0usize);
        let mut injected_1031 = false;
        let items = scan_pages(cursor, &mut budget, |cursor, page_size, timeout_ms| {
            attempts.set(attempts.get().saturating_add(1));
            assert_eq!(page_size, 4);
            assert!((1..=5_000).contains(&timeout_ms));
            if cursor.start().first() == Some(&7) && !injected_1031 {
                injected_1031 = true;
                retryable_errors.set(retryable_errors.get().saturating_add(1));
                return Err(map_fdb_error(FdbError::from_code(1031)));
            }
            Ok(fixture
                .iter()
                .filter(|item| {
                    item.key.as_slice() >= cursor.start() && item.key.as_slice() < cursor.end()
                })
                .take(page_size)
                .cloned()
                .collect())
        })
        .unwrap();

        assert_eq!(retryable_errors.get(), 1);
        assert_eq!(attempts.get(), 8);
        assert_eq!(budget.range.successful_pages, 7);
        assert_eq!(budget.range.retryable_failures, 1);
        assert_eq!(budget.range.items, fixture.len());
        assert_eq!(
            budget.range.bytes,
            fixture
                .iter()
                .map(|item| item.key.len() + item.value.len())
                .sum::<usize>()
        );
        assert_eq!(items, fixture);
        assert!(items.windows(2).all(|pair| pair[0].key < pair[1].key));
    }

    #[test]
    fn given_reverse_multi_page_fixture_when_scanning_then_all_candidates_are_descending_once() {
        let fixture = (0..10)
            .map(|index| RangeItem {
                key: vec![index],
                value: vec![index.saturating_add(1)],
            })
            .collect::<Vec<_>>();
        let cursor =
            SyncFdbScanCursor::try_new(vec![0], vec![10], RangeDirection::Reverse, 3).unwrap();
        let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));
        let attempts = Cell::new(0usize);
        let items = scan_pages(cursor, &mut budget, |cursor, page_size, timeout_ms| {
            attempts.set(attempts.get().saturating_add(1));
            assert_eq!(page_size, 3);
            assert!((1..=5_000).contains(&timeout_ms));
            Ok(fixture
                .iter()
                .filter(|item| {
                    item.key.as_slice() >= cursor.start() && item.key.as_slice() < cursor.end()
                })
                .rev()
                .take(page_size)
                .cloned()
                .collect())
        })
        .unwrap();

        assert_eq!(attempts.get(), 4);
        assert_eq!(items, fixture.iter().rev().cloned().collect::<Vec<_>>(),);
        assert!(items.windows(2).all(|pair| pair[0].key > pair[1].key));
    }

    #[test]
    fn given_repeated_timeout_and_1031_failures_when_scanning_then_retry_budget_returns_typed_exhaustion()
     {
        let cursor =
            SyncFdbScanCursor::try_new(vec![0], vec![u8::MAX], RangeDirection::Forward, 3).unwrap();
        let mut budget = SyncFdbScanBudget::with_limits(2, Instant::now() + Duration::from_secs(5));
        let attempts = Cell::new(0usize);
        let error = scan_pages(cursor, &mut budget, |_, _, _| {
            let attempt = attempts.get();
            attempts.set(attempt.saturating_add(1));
            let code = if attempt == 0 { 1007 } else { 1031 };
            Err(map_fdb_error(FdbError::from_code(code)))
        })
        .expect_err("retry budget must be bounded");

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
        assert_eq!(attempts.get(), 3);
    }

    #[test]
    fn given_expired_scan_deadline_when_starting_page_then_operation_is_rejected() {
        let cursor =
            SyncFdbScanCursor::try_new(vec![0], vec![u8::MAX], RangeDirection::Forward, 3).unwrap();
        let mut budget =
            SyncFdbScanBudget::with_limits(2, Instant::now() - Duration::from_millis(1));
        let attempts = Cell::new(0usize);
        let error = scan_pages(cursor, &mut budget, |_, _, _| {
            attempts.set(attempts.get().saturating_add(1));
            Ok(Vec::new())
        })
        .expect_err("an expired operation deadline must not start another page");

        assert!(matches!(
            error,
            CatalogError::InvalidMutation(message) if message.contains("sync range scan deadline")
        ));
        assert_eq!(attempts.get(), 0);
    }

    #[test]
    fn given_deadline_expires_after_a_retry_when_scanning_then_deadline_error_wins() {
        let mut budget = SyncFdbScanBudget::with_limits(2, Instant::now() + Duration::from_secs(5));
        budget
            .begin_attempt()
            .expect("the first page attempt should start");
        budget
            .record_retryable_failure(map_fdb_error(FdbError::from_code(1031)))
            .expect("one retry should remain");
        budget.deadline = Instant::now() - Duration::from_millis(1);
        let error = budget
            .begin_attempt()
            .expect_err("an expired operation deadline must not start a retry");

        assert!(matches!(
            error,
            CatalogError::InvalidMutation(message) if message.contains("sync range scan deadline")
        ));
        assert_eq!(budget.attempts, 1);
    }

    #[test]
    fn given_discovery_failure_after_a_page_when_scanning_then_partial_candidates_are_not_returned()
    {
        let cursor =
            SyncFdbScanCursor::try_new(vec![0], vec![u8::MAX], RangeDirection::Forward, 3).unwrap();
        let mut budget = SyncFdbScanBudget::with_limits(0, Instant::now() + Duration::from_secs(5));
        let attempts = Cell::new(0usize);
        let error = scan_pages(cursor, &mut budget, |cursor, page_size, _| {
            let attempt = attempts.get();
            attempts.set(attempt.saturating_add(1));
            if attempt == 1 {
                return Err(CatalogError::Decode("discovery failed".to_owned()));
            }
            Ok((0..3)
                .map(|index| RangeItem {
                    key: vec![index],
                    value: vec![index],
                })
                .filter(|item| {
                    item.key.as_slice() >= cursor.start() && item.key.as_slice() < cursor.end()
                })
                .take(page_size)
                .collect())
        })
        .expect_err("a discovery error must abort before mutation");

        assert!(matches!(error, CatalogError::Decode(message) if message == "discovery failed"));
        assert_eq!(attempts.get(), 2);
        assert_eq!(budget.range.successful_pages, 1);
        assert_eq!(budget.range.items, 3);
        assert_eq!(budget.range.bytes, 6);
        assert_eq!(budget.range.retryable_failures, 0);
    }
    #[test]
    fn given_inline_table_range_and_partial_progress_when_deadline_expires_then_only_safe_scan_context_is_reported()
     {
        let mut prefix =
            crate::keys::family_prefix(crate::CatalogId(91), crate::keys::KeyFamily::InlineTable);
        prefix.extend_from_slice(&17_u64.to_be_bytes());
        prefix.push(b'/');
        prefix.extend_from_slice(&23_u64.to_be_bytes());
        prefix.push(b'/');
        let mut budget = SyncFdbScanBudget::with_limits(8, Instant::now() + Duration::from_secs(5));
        budget.observe_range(&prefix);
        budget.begin_attempt().unwrap();
        budget.record_successful_page(256, 4_096);
        budget.deadline = Instant::now() - Duration::from_millis(1);
        let message = budget.begin_attempt().unwrap_err().to_string();
        for expected in [
            "family=inline-table",
            "table_schema=Some((17, 23))",
            "total_attempts=1",
            "current_range_attempts=1",
            "successful_pages=1",
            "items=256",
            "bytes=4096",
            "retryable_failures=0",
            "last_fdb=None",
        ] {
            assert!(
                message.contains(expected),
                "missing scan diagnostic field {expected}"
            );
        }
        budget.observe_range(b"private-customer-value");
        let message = budget.deadline_error().to_string();
        assert!(message.contains("family=unknown"));
        assert!(message.contains("table_schema=None"));
        assert!(!message.contains("private-customer-value"));
    }
    #[test]
    fn given_prior_range_progress_and_retry_when_next_range_expires_then_its_diagnostics_start_empty()
     {
        let first = crate::keys::family_prefix(
            crate::CatalogId(1),
            crate::keys::KeyFamily::FileColumnStats,
        );
        let second =
            crate::keys::family_prefix(crate::CatalogId(1), crate::keys::KeyFamily::Object);
        let mut budget = SyncFdbScanBudget::with_limits(2, Instant::now() + Duration::from_secs(5));
        budget.observe_range(&first);
        budget.begin_attempt().unwrap();
        budget
            .record_retryable_failure(map_fdb_error(FdbError::from_code(1031)))
            .unwrap();
        budget.begin_attempt().unwrap();
        budget.record_successful_page(4, 32);
        assert_eq!(budget.retries_remaining, 1);
        budget.observe_range(&second);
        budget.deadline = Instant::now() - Duration::from_millis(1);
        let message = budget.begin_attempt().unwrap_err().to_string();
        for expected in [
            "family=object",
            "total_attempts=2",
            "current_range_attempts=0",
            "successful_pages=0",
            "items=0",
            "bytes=0",
            "retryable_failures=0",
            "last_fdb=None",
        ] {
            assert!(
                message.contains(expected),
                "missing range-local diagnostic field {expected}"
            );
        }
        assert_eq!(
            budget.retries_remaining, 1,
            "moving to another range must not refill retries"
        );
    }
}
