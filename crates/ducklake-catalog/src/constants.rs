// Bound SDK work per prefix request while filling completed slots continuously.
pub(crate) const FDB_PREFIX_SCAN_CONCURRENCY: usize = 1024;
pub(crate) const MAX_QUERY_READ_VERSIONS: usize = 4096;
pub(crate) const QUERY_READ_VERSION_MAX_AGE: std::time::Duration =
    std::time::Duration::from_secs(4);
pub(crate) const FDB_LOGICAL_FAMILY_PREFIX_BYTES: usize = 11;
pub(crate) const FDB_MAPPED_INDEX_PAGE_ROWS: usize = 256;
pub(crate) const FDB_READ_PAGE_TARGET_BYTES: usize = 64 * 1024;

pub(crate) const SCALAR_TYPE_TOKEN_BYTES: usize = 32;

pub(crate) const COMMIT_HEADER_RESERVE_BYTES: usize = 256;
