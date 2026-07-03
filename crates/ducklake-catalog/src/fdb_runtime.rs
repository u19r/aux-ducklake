use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use foundationdb::Database;

use crate::{CatalogError, CatalogResult, FoundationDbErrorClass};

static FDB_NETWORK: OnceLock<Mutex<Option<foundationdb::api::NetworkAutoStop>>> = OnceLock::new();
static FDB_DATABASES: OnceLock<Mutex<HashMap<Option<String>, Arc<Database>>>> = OnceLock::new();

pub(crate) fn shared_foundationdb_database(
    cluster_file: Option<&str>,
) -> CatalogResult<Arc<Database>> {
    boot_foundationdb_once();
    let key = cluster_file.map(ToOwned::to_owned);
    let databases = FDB_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut databases = databases.lock().map_err(|_| {
        CatalogError::Backend("foundationdb database cache lock is poisoned".to_owned())
    })?;
    if let Some(database) = databases.get(&key) {
        return Ok(Arc::clone(database));
    }
    let database = Arc::new(Database::new(cluster_file).map_err(map_fdb_error)?);
    databases.insert(key, Arc::clone(&database));
    Ok(database)
}

pub(crate) fn decode_fence_version(bytes: &[u8]) -> CatalogResult<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CatalogError::Decode("foundationdb fence value is not 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(array))
}

pub(crate) fn map_fdb_error(error: foundationdb::FdbError) -> CatalogError {
    CatalogError::FoundationDb {
        code: error.code(),
        message: error.message().to_owned(),
        class: classify_fdb_error(error),
    }
}

pub(crate) fn map_fdb_commit_error(error: foundationdb::TransactionCommitError) -> CatalogError {
    map_fdb_error(*error)
}

pub(crate) fn classify_fdb_error(error: foundationdb::FdbError) -> FoundationDbErrorClass {
    if is_retryable_not_committed_code(error.code()) {
        return FoundationDbErrorClass::RetryableNotCommitted;
    }
    if error.is_maybe_committed() {
        return FoundationDbErrorClass::MaybeCommitted;
    }
    if error.is_retryable_not_committed() {
        return FoundationDbErrorClass::RetryableNotCommitted;
    }
    if error.is_retryable() {
        return FoundationDbErrorClass::Retryable;
    }
    FoundationDbErrorClass::NonRetryable
}

fn is_retryable_not_committed_code(code: i32) -> bool {
    matches!(code, 1007 | 1031)
}

pub(crate) fn boot_foundationdb_once() {
    FDB_NETWORK.get_or_init(|| {
        let network = unsafe { foundationdb::boot() };
        Mutex::new(Some(network))
    });
}

pub fn shutdown_foundationdb_if_booted() {
    clear_foundationdb_database_cache();
    if let Some(network) = FDB_NETWORK.get()
        && let Ok(mut guard) = network.lock()
    {
        guard.take();
    }
}

pub(crate) fn abandon_foundationdb_for_process_shutdown() {
    // FDB Database destruction can run client cleanup on the network thread.
    // During process teardown the safer boundary is to let the OS reclaim both
    // database handles and the network runner instead of running libfdb
    // destructors after DuckDB has started global shutdown.
    if let Some(network) = FDB_NETWORK.get()
        && let Ok(mut guard) = network.lock()
        && let Some(network) = guard.take()
    {
        std::mem::forget(network);
    }
}

fn clear_foundationdb_database_cache() {
    if let Some(databases) = FDB_DATABASES.get()
        && let Ok(mut databases) = databases.lock()
    {
        databases.clear();
    }
}

#[cfg(test)]
#[path = "fdb_runtime_tests.rs"]
mod fdb_runtime_tests;
