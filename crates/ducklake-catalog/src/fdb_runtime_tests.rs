#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn fdb_error_classifier_marks_not_committed_as_retryable_not_committed() {
        let error = foundationdb::FdbError::from_code(1020);

        assert_eq!(
            classify_fdb_error(error),
            FoundationDbErrorClass::RetryableNotCommitted
        );
        assert!(map_fdb_error(error).to_string().contains("code=1020"));
        assert!(
            map_fdb_error(error)
                .to_string()
                .contains("retryable_not_committed")
        );
    }

    #[test]
    fn fdb_error_classifier_marks_commit_unknown_as_maybe_committed() {
        let error = foundationdb::FdbError::from_code(1021);

        assert_eq!(
            classify_fdb_error(error),
            FoundationDbErrorClass::MaybeCommitted
        );
        assert!(map_fdb_error(error).to_string().contains("code=1021"));
        assert!(map_fdb_error(error).to_string().contains("maybe_committed"));
    }

    #[test]
    fn fdb_error_classifier_marks_transaction_timeout_as_retryable_not_committed() {
        let error = foundationdb::FdbError::from_code(1031);

        assert_eq!(
            classify_fdb_error(error),
            FoundationDbErrorClass::RetryableNotCommitted
        );
        assert!(map_fdb_error(error).to_string().contains("code=1031"));
        assert!(
            map_fdb_error(error)
                .to_string()
                .contains("retryable_not_committed")
        );
    }

    #[test]
    fn fdb_error_classifier_marks_invalid_option_as_non_retryable() {
        let error = foundationdb::FdbError::from_code(2004);

        assert_eq!(
            classify_fdb_error(error),
            FoundationDbErrorClass::NonRetryable
        );
        assert!(map_fdb_error(error).to_string().contains("code=2004"));
        assert!(map_fdb_error(error).to_string().contains("non_retryable"));
    }
}
