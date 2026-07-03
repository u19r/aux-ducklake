#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::{CatalogOrderId, FakeOrderedCatalogKv, KvBatch};

    #[test]
    fn maybe_committed_recovery_uses_persisted_attempt_order() {
        let catalog = crate::CatalogId(88);
        let attempt = crate::CommitAttemptId(99);
        let order = CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
        let mut kv = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            commit_attempt_key(catalog, attempt),
            CommitAttemptRow::new(attempt, order).encode(),
        );
        kv.commit(batch).unwrap();

        assert_eq!(
            recover_maybe_committed_append(&kv, catalog, attempt).unwrap(),
            Some(AppendCommitResult::AlreadyCommitted {
                commit_order: order
            })
        );
    }

    #[test]
    fn maybe_committed_recovery_without_attempt_row_returns_none() {
        let kv = FakeOrderedCatalogKv::new();

        assert_eq!(
            recover_maybe_committed_append(&kv, crate::CatalogId(88), crate::CommitAttemptId(99))
                .unwrap(),
            None
        );
    }

    #[test]
    fn retry_decision_retries_retryable_not_committed_before_budget_is_exhausted() {
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 0, 3),
            AppendCommitFailureAction::Retry
        );
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 2, 3),
            AppendCommitFailureAction::Retry
        );
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 3, 3),
            AppendCommitFailureAction::ReturnError
        );
    }

    #[test]
    fn retry_decision_recovers_maybe_committed_and_rejects_other_classes() {
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::MaybeCommitted, 0, 3),
            AppendCommitFailureAction::RecoverMaybeCommitted
        );
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::Retryable, 0, 3),
            AppendCommitFailureAction::ReturnError
        );
        assert_eq!(
            append_commit_failure_action(FoundationDbErrorClass::NonRetryable, 0, 3),
            AppendCommitFailureAction::ReturnError
        );
    }

    #[test]
    fn exhausted_retryable_not_committed_error_reports_retry_budget() {
        let error = CatalogError::FoundationDb {
            code: 1020,
            message: "not_committed".to_owned(),
            class: FoundationDbErrorClass::RetryableNotCommitted,
        };

        assert_eq!(
            append_commit_final_error(error, FoundationDbErrorClass::RetryableNotCommitted, 3, 3),
            CatalogError::FoundationDbRetryExhausted {
                operation: "append commit",
                attempts: 4,
                code: 1020,
                message: "not_committed".to_owned(),
                class: FoundationDbErrorClass::RetryableNotCommitted,
            }
        );
    }

    #[test]
    fn final_error_keeps_non_exhausted_and_non_retryable_errors_unchanged() {
        let retryable = CatalogError::FoundationDb {
            code: 1020,
            message: "not_committed".to_owned(),
            class: FoundationDbErrorClass::RetryableNotCommitted,
        };
        assert_eq!(
            append_commit_final_error(
                retryable.clone(),
                FoundationDbErrorClass::RetryableNotCommitted,
                2,
                3
            ),
            retryable
        );

        let non_retryable = CatalogError::FoundationDb {
            code: 2004,
            message: "invalid_option".to_owned(),
            class: FoundationDbErrorClass::NonRetryable,
        };
        assert_eq!(
            append_commit_final_error(
                non_retryable.clone(),
                FoundationDbErrorClass::NonRetryable,
                0,
                3
            ),
            non_retryable
        );
    }
}
