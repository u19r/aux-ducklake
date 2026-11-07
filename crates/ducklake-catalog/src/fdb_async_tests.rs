use super::is_retryable_async_catalog_error;
use crate::{CatalogError, FoundationDbErrorClass};

#[test]
fn given_ambiguous_initialize_commit_when_classified_then_initialization_is_retried() {
    assert!(is_retryable_async_catalog_error(&foundationdb_error(
        FoundationDbErrorClass::MaybeCommitted
    )));
}

#[test]
fn given_retryable_initialize_failure_when_classified_then_initialization_is_retried() {
    for class in [
        FoundationDbErrorClass::RetryableNotCommitted,
        FoundationDbErrorClass::Retryable,
    ] {
        assert!(is_retryable_async_catalog_error(&foundationdb_error(class)));
    }
}

#[test]
fn given_permanent_initialize_failure_when_classified_then_initialization_is_not_retried() {
    assert!(!is_retryable_async_catalog_error(&foundationdb_error(
        FoundationDbErrorClass::NonRetryable
    )));
    assert!(!is_retryable_async_catalog_error(
        &CatalogError::InvalidMutation("invalid".to_owned())
    ));
}

fn foundationdb_error(class: FoundationDbErrorClass) -> CatalogError {
    CatalogError::FoundationDb {
        code: 1,
        message: "test".to_owned(),
        class,
    }
}
