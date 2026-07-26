#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::runtime_metrics::RuntimeMetricStage;
use crate::{
    CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SnapshotRow,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{
        decode_snapshot_timestamp_key, latest_snapshot_row_key, raw_snapshot_row_key, snapshot_key,
        snapshot_prefix, snapshot_timestamp_key, snapshot_timestamp_prefix,
    },
};
#[cfg(feature = "runtime-metrics")]
use crate::{
    runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed},
    runtime_protocol::RuntimeCatalogBackend,
};
#[cfg(feature = "runtime-metrics")]
use std::panic::Location;
#[cfg(not(test))]
use std::sync::OnceLock;
use std::{cell::RefCell, collections::BTreeMap};

pub const SUPPORTED_DUCKLAKE_COMMIT: &str = "7e3c8e97cc5acddbcd2a1ebfb8530e6c52efdacf";

#[cfg(feature = "runtime-metrics")]
macro_rules! record_latest_snapshot_callsite {
    ($started:expr) => {
        record_latest_snapshot_metric(Location::caller(), $started)
    };
}

#[cfg(not(feature = "runtime-metrics"))]
macro_rules! record_latest_snapshot_callsite {
    ($started:expr) => {{
        let _ = $started;
    }};
}

#[cfg(feature = "runtime-metrics")]
macro_rules! record_snapshot_list_callsite {
    ($operation:expr, $started:expr) => {
        record_snapshot_list_metric($operation, Location::caller(), $started)
    };
}

#[cfg(not(feature = "runtime-metrics"))]
macro_rules! record_snapshot_list_callsite {
    ($operation:expr, $started:expr) => {{
        let _ = ($operation, $started);
    }};
}

#[cfg(not(test))]
static LATEST_SNAPSHOT_CACHE: OnceLock<
    BoundedCache<(crate::CatalogCacheNamespace, CatalogId), Option<SnapshotRow>>,
> = OnceLock::new();

#[cfg(not(test))]
static SNAPSHOT_LIST_CACHE: OnceLock<
    BoundedCache<(crate::CatalogCacheNamespace, CatalogId), Vec<SnapshotRow>>,
> = OnceLock::new();

pub(crate) fn invalidate_runtime_read_context(catalog: CatalogId) {
    REQUEST_LATEST_SNAPSHOT_CACHE.with(|cache| {
        if let Some(cache) = cache.borrow_mut().as_mut() {
            cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
        }
    });
    #[cfg(not(test))]
    {
        if let Some(cache) = LATEST_SNAPSHOT_CACHE.get() {
            cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
        }
        if let Some(cache) = SNAPSHOT_LIST_CACHE.get() {
            cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
        }
        crate::runtime_read_context::invalidate_catalog_read_context(catalog);
        crate::runtime_read_context::invalidate_inline_deletion_read_context(catalog);
        crate::runtime_file_listing::invalidate_file_listing_read_context(catalog);
        crate::table_store::invalidate_runtime_table_read_context(catalog);
        crate::inline_data::invalidate_inline_table_payload_read_context(catalog);
        crate::runtime_inline_rows::invalidate_inline_read_context(catalog);
        crate::delete_change_feed::invalidate_delete_change_feed_context(catalog);
        crate::runtime_snapshots::invalidate_runtime_snapshot_context(catalog);
    }
}

type LatestSnapshotRequestCache =
    BTreeMap<(crate::CatalogCacheNamespace, CatalogId), Option<SnapshotRow>>;

thread_local! {
    static REQUEST_LATEST_SNAPSHOT_CACHE: RefCell<Option<LatestSnapshotRequestCache>> =
        const { RefCell::new(None) };
    static PERSISTENT_LATEST_SNAPSHOT_CACHES:
        RefCell<BTreeMap<u64, LatestSnapshotRequestCache>> = const { RefCell::new(BTreeMap::new()) };
    static ACTIVE_RUNTIME_READ_CONTEXT: RefCell<Option<u64>> = const { RefCell::new(None) };
}

pub(crate) struct RuntimeReadRequestGuard {
    previous: Option<LatestSnapshotRequestCache>,
    previous_context: Option<u64>,
    context_id: Option<u64>,
    invalidate_catalog: Option<CatalogId>,
    new_context: bool,
}

pub(crate) fn begin_runtime_read_request(context_id: Option<u64>) -> RuntimeReadRequestGuard {
    let (request_cache, new_context) = context_id.map_or_else(
        || (BTreeMap::new(), false),
        |context_id| {
            PERSISTENT_LATEST_SNAPSHOT_CACHES.with(|contexts| {
                let mut contexts = contexts.borrow_mut();
                match contexts.remove(&context_id) {
                    Some(cached) => (cached, false),
                    None => (BTreeMap::new(), true),
                }
            })
        },
    );
    let previous = REQUEST_LATEST_SNAPSHOT_CACHE.with(|cache| cache.replace(Some(request_cache)));
    let previous_context = ACTIVE_RUNTIME_READ_CONTEXT.with(|active| active.replace(context_id));
    RuntimeReadRequestGuard {
        previous,
        previous_context,
        context_id,
        invalidate_catalog: None,
        new_context,
    }
}

pub(crate) fn active_runtime_read_context_id() -> Option<u64> {
    ACTIVE_RUNTIME_READ_CONTEXT.with(|active| *active.borrow())
}

impl RuntimeReadRequestGuard {
    pub(crate) fn is_new_context(&self) -> bool {
        self.new_context
    }

    pub(crate) fn invalidate_catalog_on_drop(&mut self, catalog: CatalogId) {
        self.invalidate_catalog = Some(catalog);
    }
}

impl Drop for RuntimeReadRequestGuard {
    fn drop(&mut self) {
        if let Some(catalog) = self.invalidate_catalog {
            invalidate_runtime_read_context(catalog);
        }
        if let Some(context_id) = self.context_id {
            let current = REQUEST_LATEST_SNAPSHOT_CACHE
                .with(|cache| cache.borrow_mut().take())
                .unwrap_or_default();
            PERSISTENT_LATEST_SNAPSHOT_CACHES.with(|contexts| {
                let mut contexts = contexts.borrow_mut();
                contexts.insert(context_id, current);
                while contexts.len() > 16 {
                    contexts.pop_first();
                }
            });
        }
        let previous = self.previous.take();
        REQUEST_LATEST_SNAPSHOT_CACHE.with(|cache| {
            cache.replace(previous);
        });
        ACTIVE_RUNTIME_READ_CONTEXT.with(|active| {
            active.replace(self.previous_context.take());
        });
    }
}

pub fn initialize_empty_catalog(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<SnapshotRow> {
    invalidate_runtime_read_context(catalog);
    let order = kv.generated_order_id()?;
    let row = SnapshotRow::initial(order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &row);
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(row)
}

pub fn initialize_catalog_if_absent(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<SnapshotRow> {
    invalidate_runtime_read_context(catalog);
    match latest_snapshot(kv, catalog)? {
        Some(row) => Ok(row),
        None => initialize_empty_catalog(kv, catalog),
    }
}

#[track_caller]
pub fn latest_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<SnapshotRow>> {
    let cache_key = (kv.catalog_cache_namespace(), catalog);
    if let Some(snapshot) = REQUEST_LATEST_SNAPSHOT_CACHE
        .with(|cache| cache.borrow().as_ref()?.get(&cache_key).cloned())
    {
        return Ok(snapshot);
    }
    #[cfg(not(test))]
    if runtime_read_context_enabled()
        && let Some(snapshot) = latest_snapshot_cache().get(cache_key)
    {
        return Ok(snapshot);
    }
    let result = latest_snapshot_uncached(kv, catalog);
    record_latest_snapshot_callsite!(result.started);
    let result = result.row?;
    REQUEST_LATEST_SNAPSHOT_CACHE.with(|cache| {
        if let Some(cache) = cache.borrow_mut().as_mut() {
            cache.insert(cache_key, result.clone());
        }
    });
    #[cfg(not(test))]
    if runtime_read_context_enabled() {
        latest_snapshot_cache().insert(cache_key, result.clone());
    }
    Ok(result)
}

pub(crate) struct LatestSnapshotRead {
    pub(crate) row: CatalogResult<Option<SnapshotRow>>,
    started: RuntimeMetricStage,
}

pub(crate) fn latest_snapshot_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> LatestSnapshotRead {
    let started = RuntimeMetricStage::start();
    let row = latest_snapshot_uncached_row(kv, catalog);
    LatestSnapshotRead { row, started }
}

fn latest_snapshot_uncached_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<SnapshotRow>> {
    if let Some(value) = kv.get(&latest_snapshot_row_key(catalog))? {
        return Ok(Some(decode_latest_snapshot_value(&value)?));
    }
    let rows = kv.scan_prefix(&snapshot_prefix(catalog), RangeDirection::Reverse, 1)?;
    rows.first()
        .map(|item| decode_snapshot_item(catalog, &item.key, &item.value))
        .transpose()
}

#[cfg(feature = "runtime-metrics")]
fn record_latest_snapshot_metric(caller: &'static Location<'static>, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!(
            "latest_snapshot:{}:{}",
            caller.file().rsplit('/').next().unwrap_or(caller.file()),
            caller.line()
        ),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[track_caller]
pub fn list_snapshots(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<SnapshotRow>> {
    #[cfg(not(test))]
    let cache_key = (kv.catalog_cache_namespace(), catalog);
    #[cfg(not(test))]
    if runtime_read_context_enabled()
        && let Some(snapshots) = snapshot_list_cache().get(cache_key)
    {
        return Ok(snapshots);
    }
    let started = RuntimeMetricStage::start();
    let mut orders = Vec::new();
    for item in kv.scan_prefix(
        &snapshot_timestamp_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let (_, order) = decode_snapshot_timestamp_key(catalog, &item.key)?;
        orders.push(order);
    }
    let keys = orders
        .iter()
        .map(|order| snapshot_key(catalog, *order))
        .collect::<Vec<_>>();
    let values = kv.batch_get(&keys)?;
    let mut rows = Vec::new();
    for (key, value) in keys.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        rows.push(decode_snapshot_item(catalog, &key, &value)?);
    }
    rows.sort_by_key(|snapshot| snapshot.order);
    record_snapshot_list_callsite!("list_snapshots", started);
    #[cfg(not(test))]
    if runtime_read_context_enabled() {
        snapshot_list_cache().insert(cache_key, rows.clone());
    }
    Ok(rows)
}

#[track_caller]
pub fn list_all_snapshots(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<SnapshotRow>> {
    #[cfg(not(test))]
    let cache_key = (kv.catalog_cache_namespace(), catalog);
    #[cfg(not(test))]
    if runtime_read_context_enabled()
        && let Some(snapshots) = snapshot_list_cache().get(cache_key)
    {
        return Ok(snapshots);
    }
    let started = RuntimeMetricStage::start();
    let rows = kv
        .scan_prefix(
            &snapshot_prefix(catalog),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| decode_snapshot_item(catalog, &item.key, &item.value))
        .collect::<CatalogResult<Vec<_>>>();
    record_snapshot_list_callsite!("list_all_snapshots", started);
    let rows = rows?;
    #[cfg(not(test))]
    if runtime_read_context_enabled() {
        snapshot_list_cache().insert(cache_key, rows.clone());
    }
    Ok(rows)
}

#[cfg(not(test))]
fn latest_snapshot_cache()
-> &'static BoundedCache<(crate::CatalogCacheNamespace, CatalogId), Option<SnapshotRow>> {
    static_bounded_cache(&LATEST_SNAPSHOT_CACHE, 16)
}

#[cfg(not(test))]
fn snapshot_list_cache()
-> &'static BoundedCache<(crate::CatalogCacheNamespace, CatalogId), Vec<SnapshotRow>> {
    static_bounded_cache(&SNAPSHOT_LIST_CACHE, 16)
}

pub(crate) fn runtime_read_context_enabled() -> bool {
    active_runtime_read_context_id().is_some()
        || std::env::var_os("AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT").is_some()
}

#[cfg(feature = "runtime-metrics")]
fn record_snapshot_list_metric(
    operation: &str,
    caller: &'static Location<'static>,
    started: RuntimeMetricStage,
) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!(
            "{}:{}:{}",
            operation,
            caller.file().rsplit('/').next().unwrap_or(caller.file()),
            caller.line()
        ),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

pub fn list_snapshots_older_than(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    older_than_micros: i64,
) -> CatalogResult<Vec<SnapshotRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &snapshot_timestamp_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let (created_at_micros, order) = decode_snapshot_timestamp_key(catalog, &item.key)?;
        if created_at_micros >= older_than_micros {
            break;
        }
        let Some(value) = kv.get(&snapshot_key(catalog, order))? else {
            continue;
        };
        rows.push(decode_snapshot_item(
            catalog,
            &snapshot_key(catalog, order),
            &value,
        )?);
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotTimestampBound {
    Lower,
    Upper,
}

pub fn snapshot_by_timestamp(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    timestamp_micros: i64,
    bound: SnapshotTimestampBound,
) -> CatalogResult<Option<SnapshotRow>> {
    #[cfg(not(test))]
    if runtime_read_context_enabled() {
        return Ok(
            crate::runtime_snapshots::SnapshotReadContext::for_current_catalog(kv, catalog)?
                .snapshot_at_timestamp(timestamp_micros, bound),
        );
    }
    let mut selected = None;
    for item in kv.scan_prefix(
        &snapshot_timestamp_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let (created_at_micros, order) = decode_snapshot_timestamp_key(catalog, &item.key)?;
        match bound {
            SnapshotTimestampBound::Lower => {
                if created_at_micros < timestamp_micros {
                    continue;
                }
                let Some(value) = kv.get(&snapshot_key(catalog, order))? else {
                    continue;
                };
                return decode_snapshot_item(catalog, &snapshot_key(catalog, order), &value)
                    .map(Some);
            }
            SnapshotTimestampBound::Upper => {
                if created_at_micros > timestamp_micros {
                    break;
                }
                let Some(value) = kv.get(&snapshot_key(catalog, order))? else {
                    continue;
                };
                selected = Some(decode_snapshot_item(
                    catalog,
                    &snapshot_key(catalog, order),
                    &value,
                )?);
            }
        }
    }
    Ok(selected)
}

pub fn snapshot_by_raw_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    raw_sequence: RawSnapshotSequence,
) -> CatalogResult<Option<SnapshotRow>> {
    kv.get(&raw_snapshot_row_key(catalog, raw_sequence))?
        .map(|value| decode_latest_snapshot_value(&value))
        .transpose()
}

pub fn expire_snapshots(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    raw_sequences: &[RawSnapshotSequence],
) -> CatalogResult<Vec<SnapshotRow>> {
    if raw_sequences.is_empty() {
        return Ok(Vec::new());
    }
    let latest = latest_snapshot(kv, catalog)?.ok_or(crate::CatalogError::NotFound("snapshot"))?;
    let snapshots = list_snapshots(kv, catalog)?;
    let mut snapshots_by_sequence = BTreeMap::<RawSnapshotSequence, Vec<SnapshotRow>>::new();
    for snapshot in snapshots {
        snapshots_by_sequence
            .entry(snapshot.sequence)
            .or_default()
            .push(snapshot);
    }
    let mut expired = Vec::new();
    let mut batch = KvBatch::new();
    for raw_sequence in raw_sequences {
        if *raw_sequence == latest.sequence {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "cannot expire latest snapshot {}",
                latest.sequence
            )));
        }
        let Some(sequence_snapshots) = snapshots_by_sequence.get(raw_sequence) else {
            continue;
        };
        for snapshot in sequence_snapshots {
            stage_delete_snapshot(&mut batch, catalog, snapshot);
            expired.push(snapshot.clone());
        }
    }
    kv.commit(batch)?;
    Ok(expired)
}

pub(crate) fn snapshot_row_for_next_sequence(
    latest: Option<SnapshotRow>,
    order: CatalogOrderId,
) -> SnapshotRow {
    SnapshotRow::new(
        order,
        latest.map_or(RawSnapshotSequence::initial(), |snapshot| {
            snapshot.sequence.next()
        }),
    )
}

pub(crate) fn stage_snapshot(batch: &mut KvBatch, catalog: CatalogId, snapshot: &SnapshotRow) {
    batch.put(snapshot_key(catalog, snapshot.order), snapshot.encode());
    batch.put(
        snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
        snapshot.sequence.to_be_bytes().to_vec(),
    );
    batch.put(
        latest_snapshot_row_key(catalog),
        latest_snapshot_value(snapshot),
    );
    batch.put(
        raw_snapshot_row_key(catalog, snapshot.sequence),
        latest_snapshot_value(snapshot),
    );
}

pub(crate) fn latest_snapshot_value(snapshot: &SnapshotRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + snapshot.encode().len());
    out.push(1);
    out.push(match snapshot.order.kind() {
        CatalogOrderKind::UuidV7 => b'u',
        CatalogOrderKind::FdbVersionstamp => b'f',
    });
    out.extend_from_slice(&snapshot.encode());
    out
}

#[cfg(feature = "foundationdb")]
pub(crate) const fn latest_snapshot_value_order_offset() -> usize {
    2 + 1
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_snapshot_indexes(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    let value = crate::fdb_versionstamp::versionstamped_value(
        &latest_snapshot_value(snapshot),
        latest_snapshot_value_order_offset(),
    )?;
    for key in [
        latest_snapshot_row_key(catalog),
        raw_snapshot_row_key(catalog, snapshot.sequence),
    ] {
        trx.atomic_op(
            &kv.namespaced_key(&key),
            &value,
            foundationdb::options::MutationType::SetVersionstampedValue,
        );
    }
    Ok(())
}

fn decode_latest_snapshot_value(value: &[u8]) -> CatalogResult<SnapshotRow> {
    if value.len() < 2 || value[0] != 1 {
        return Err(crate::CatalogError::Decode(
            "latest snapshot row has invalid version".to_owned(),
        ));
    }
    let order_kind = match value[1] {
        b'u' => CatalogOrderKind::UuidV7,
        b'f' => CatalogOrderKind::FdbVersionstamp,
        other => {
            return Err(crate::CatalogError::Decode(format!(
                "latest snapshot row has unknown order kind 0x{other:02x}"
            )));
        }
    };
    let mut row = SnapshotRow::decode(&value[2..])?;
    row.order = CatalogOrderId::from_bytes(order_kind, row.order.as_bytes());
    Ok(row)
}

fn stage_delete_snapshot(batch: &mut KvBatch, catalog: CatalogId, snapshot: &SnapshotRow) {
    batch.delete(snapshot_timestamp_key(
        catalog,
        snapshot.created_at_micros,
        snapshot.order,
    ));
}

pub(crate) fn decode_snapshot_item(
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<SnapshotRow> {
    let mut row = SnapshotRow::decode(value)?;
    row.order = snapshot_order_from_key(catalog, key, row.order)?;
    Ok(row)
}

fn snapshot_order_from_key(
    catalog: CatalogId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = snapshot_prefix(catalog);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(crate::CatalogError::InvalidKey(
            "snapshot key has wrong prefix".to_owned(),
        ));
    };
    let bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        crate::CatalogError::InvalidKey(format!(
            "snapshot key order must be {} bytes, got {}",
            CatalogOrderId::LEN,
            tail.len()
        ))
    })?;
    let kind = if value_order.as_bytes() == bytes {
        value_order.kind()
    } else {
        CatalogOrderKind::FdbVersionstamp
    };
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod store_tests;
