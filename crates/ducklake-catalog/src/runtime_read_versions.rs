use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

use crate::{CatalogCacheNamespace, CatalogError, CatalogResult};

type VersionSlot = Arc<Mutex<Option<ReadVersionLease>>>;
type QueryVersions = BTreeMap<CatalogCacheNamespace, VersionSlot>;
type RequestVersions = BTreeMap<CatalogCacheNamespace, i64>;

#[derive(Clone, Copy)]
struct ReadVersionLease {
    version: i64,
    acquired: Instant,
}

static QUERY_VERSIONS: OnceLock<Mutex<QueryVersions>> = OnceLock::new();

thread_local! {
    static REQUEST_VERSIONS: RefCell<Option<RequestVersions>> = const { RefCell::new(None) };
}

pub(crate) struct ReadVersionRequestGuard {
    previous: Option<RequestVersions>,
}

pub(crate) fn begin_request() -> ReadVersionRequestGuard {
    ReadVersionRequestGuard {
        previous: REQUEST_VERSIONS.with(|versions| versions.replace(Some(BTreeMap::new()))),
    }
}

impl Drop for ReadVersionRequestGuard {
    fn drop(&mut self) {
        REQUEST_VERSIONS.with(|versions| versions.replace(self.previous.take()));
    }
}

pub(crate) fn fetch_version(
    namespace: CatalogCacheNamespace,
    fetch: impl FnOnce() -> CatalogResult<i64>,
) -> CatalogResult<i64> {
    if let Some(version) = request_version(namespace) {
        return Ok(version);
    }
    let slot = find_query_slot(namespace)?;
    let mut lease = slot.lock().map_err(|_| unavailable_registry())?;
    let version = match *lease {
        Some(value) if value.acquired.elapsed() < crate::constants::QUERY_READ_VERSION_MAX_AGE => {
            crate::runtime_metrics::record_runtime_measurement("read_version.query_reuse", 1, 0);
            value.version
        }
        _ => {
            let acquired = Instant::now();
            let version = fetch()?;
            *lease = Some(ReadVersionLease { version, acquired });
            crate::runtime_metrics::record_runtime_measurement("read_version.query_acquire", 1, 0);
            version
        }
    };
    REQUEST_VERSIONS.with(|versions| {
        if let Some(versions) = versions.borrow_mut().as_mut() {
            versions.insert(namespace, version);
        }
    });
    Ok(version)
}

pub(crate) fn request_version(namespace: CatalogCacheNamespace) -> Option<i64> {
    REQUEST_VERSIONS.with(|versions| versions.borrow().as_ref()?.get(&namespace).copied())
}

fn find_query_slot(namespace: CatalogCacheNamespace) -> CatalogResult<VersionSlot> {
    let mut versions = query_versions()
        .lock()
        .map_err(|_| unavailable_registry())?;
    if let Some(slot) = versions.get(&namespace) {
        return Ok(Arc::clone(slot));
    }
    let slot = Arc::new(Mutex::new(None));
    if versions.len() < crate::constants::MAX_QUERY_READ_VERSIONS {
        versions.insert(namespace, Arc::clone(&slot));
    }
    Ok(slot)
}

pub(crate) fn renew_request() {
    let previous = REQUEST_VERSIONS.with(|versions| versions.replace(Some(BTreeMap::new())));
    if let Some(previous) = previous
        && let Ok(mut versions) = query_versions().lock()
    {
        versions.retain(|key, _| !previous.contains_key(key));
    }
    crate::runtime_metrics::record_runtime_measurement("read_version.expired_retry", 1, 0);
}

pub(crate) fn end_context(namespace: CatalogCacheNamespace, context: u64) {
    let key = namespace.with_read_context(Some(context));
    if let Ok(mut versions) = query_versions().lock() {
        versions.remove(&key);
    }
    REQUEST_VERSIONS.with(|versions| {
        if let Some(versions) = versions.borrow_mut().as_mut() {
            versions.remove(&key);
        }
    });
}

pub(crate) fn invalidate_context(context: u64) {
    if let Ok(mut versions) = query_versions().lock() {
        versions.retain(|key, _| key.with_read_context(Some(context)) != *key);
    }
}

pub(crate) fn read_with_retry<T>(mut read: impl FnMut() -> CatalogResult<T>) -> CatalogResult<T> {
    let result = read();
    if crate::store::active_runtime_read_context_id().is_some()
        && result.as_ref().is_err_and(is_expired_read)
    {
        renew_request();
        crate::catalog_request_read_set::clear();
        return read();
    }
    result
}

fn is_expired_read(error: &CatalogError) -> bool {
    matches!(
        error,
        CatalogError::FoundationDb { code: 1007, .. }
            | CatalogError::FoundationDbRetryExhausted { code: 1007, .. }
    )
}

fn query_versions() -> &'static Mutex<QueryVersions> {
    QUERY_VERSIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn unavailable_registry() -> CatalogError {
    CatalogError::Backend("runtime physical read-version registry is unavailable".to_owned())
}
