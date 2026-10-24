use std::str;

use foundationdb::{Database, options::TransactionOption};
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogResult,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    keys::prefix_end,
};

const FOUNDATIONDB_MAX_KEY_BYTES: usize = 10_000;

/// Clears exactly one validated FoundationDB database key prefix.
///
/// The caller owns the database connection and must pass a prefix ending in
/// `/`. Validation is completed before a transaction is created, so rejected
/// prefixes cannot mutate the database.
pub fn clear_foundationdb_prefix(db: &Database, prefix: &[u8]) -> CatalogResult<()> {
    let end = validate_prefix_end(prefix)?;
    let trx = db.create_trx().map_err(map_fdb_error)?;
    trx.set_option(TransactionOption::Timeout(5_000))
        .map_err(map_fdb_error)?;
    trx.clear_range(prefix, &end);
    block_on(trx.commit()).map_err(map_fdb_commit_error)?;
    Ok(())
}

fn validate_prefix_end(prefix: &[u8]) -> CatalogResult<Vec<u8>> {
    if prefix.is_empty() {
        return Err(invalid_prefix("prefix must be non-empty"));
    }
    if prefix.len() >= FOUNDATIONDB_MAX_KEY_BYTES || prefix.iter().all(|byte| *byte == u8::MAX) {
        return Err(invalid_prefix("prefix range successor overflow"));
    }
    let text = str::from_utf8(prefix)
        .map_err(|_| invalid_prefix("prefix must contain valid UTF-8 bytes"))?;
    if text == "/" {
        return Err(invalid_prefix("root prefix is not allowed"));
    }
    if text.starts_with('/') {
        return Err(invalid_prefix(
            "prefix must be relative, not root or absolute",
        ));
    }
    if !text.ends_with('/') {
        return Err(invalid_prefix("prefix must end with a trailing slash"));
    }
    if text
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err(invalid_prefix("prefix contains a control byte"));
    }
    let body = &text[..text.len() - 1];
    if body
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(invalid_prefix(
            "prefix contains an empty or traversal path segment",
        ));
    }
    let end = prefix_end(prefix);
    if end.len() > FOUNDATIONDB_MAX_KEY_BYTES || end.as_slice() <= prefix {
        return Err(invalid_prefix("prefix range successor overflow"));
    }
    Ok(end)
}

fn invalid_prefix(message: &str) -> CatalogError {
    CatalogError::InvalidMutation(format!("invalid FoundationDB cleanup prefix: {message}"))
}

#[cfg(test)]
#[path = "fdb_prefix_cleanup_tests.rs"]
mod fdb_prefix_cleanup_tests;
