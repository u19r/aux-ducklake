#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SnapshotRow,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{
        decode_snapshot_timestamp_key, latest_snapshot_row_key, snapshot_key, snapshot_prefix,
        snapshot_timestamp_key, snapshot_timestamp_prefix,
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

pub const SUPPORTED_DUCKLAKE_COMMIT: &str = "7e3c8e97cc5acddbcd2a1ebfb8530e6c52efdacf";

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

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
static LATEST_SNAPSHOT_CACHE: OnceLock<BoundedCache<CatalogId, Option<SnapshotRow>>> =
    OnceLock::new();

#[cfg(not(test))]
static SNAPSHOT_LIST_CACHE: OnceLock<BoundedCache<CatalogId, Vec<SnapshotRow>>> = OnceLock::new();

#[cfg(not(test))]
pub(crate) fn invalidate_runtime_read_context(catalog: CatalogId) {
    if let Some(cache) = LATEST_SNAPSHOT_CACHE.get() {
        cache.remove(catalog);
    }
    if let Some(cache) = SNAPSHOT_LIST_CACHE.get() {
        cache.remove(catalog);
    }
    crate::runtime_read_context::invalidate_catalog_read_context(catalog);
    crate::runtime_file_listing::invalidate_file_listing_read_context(catalog);
    crate::table_store::invalidate_runtime_table_read_context(catalog);
    crate::inline_data::invalidate_inline_table_payload_read_context(catalog);
    crate::runtime_inline_rows::invalidate_inline_read_context(catalog);
    crate::delete_change_feed::invalidate_delete_change_feed_context(catalog);
}

#[cfg(test)]
pub(crate) fn invalidate_runtime_read_context(_catalog: CatalogId) {}

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
    #[cfg(not(test))]
    if runtime_read_context_enabled()
        && let Some(snapshot) = latest_snapshot_cache().get(catalog)
    {
        return Ok(snapshot);
    }
    let result = latest_snapshot_uncached(kv, catalog);
    record_latest_snapshot_callsite!(result.started);
    let result = result.row?;
    #[cfg(not(test))]
    if runtime_read_context_enabled() {
        latest_snapshot_cache().insert(catalog, result.clone());
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
    if runtime_read_context_enabled()
        && let Some(snapshots) = snapshot_list_cache().get(catalog)
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
        snapshot_list_cache().insert(catalog, rows.clone());
    }
    Ok(rows)
}

#[track_caller]
pub fn list_all_snapshots(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<SnapshotRow>> {
    #[cfg(not(test))]
    if runtime_read_context_enabled()
        && let Some(snapshots) = snapshot_list_cache().get(catalog)
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
        snapshot_list_cache().insert(catalog, rows.clone());
    }
    Ok(rows)
}

#[cfg(not(test))]
fn latest_snapshot_cache() -> &'static BoundedCache<CatalogId, Option<SnapshotRow>> {
    static_bounded_cache(&LATEST_SNAPSHOT_CACHE, 16)
}

#[cfg(not(test))]
fn snapshot_list_cache() -> &'static BoundedCache<CatalogId, Vec<SnapshotRow>> {
    static_bounded_cache(&SNAPSHOT_LIST_CACHE, 16)
}

#[cfg(not(test))]
pub(crate) fn runtime_read_context_enabled() -> bool {
    std::env::var_os("AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT").is_some()
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
    Ok(list_all_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence == raw_sequence)
        .max_by_key(|snapshot| snapshot.order))
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
    let mut expired = Vec::new();
    let mut batch = KvBatch::new();
    for raw_sequence in raw_sequences {
        if *raw_sequence == latest.sequence {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "cannot expire latest snapshot {}",
                latest.sequence
            )));
        }
        let sequence_snapshots = snapshots
            .iter()
            .filter(|snapshot| snapshot.sequence == *raw_sequence)
            .collect::<Vec<_>>();
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
pub(crate) fn stage_fdb_latest_snapshot_value(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.namespaced_key(&latest_snapshot_row_key(catalog)),
        &crate::fdb_versionstamp::versionstamped_value(
            &latest_snapshot_value(snapshot),
            latest_snapshot_value_order_offset(),
        )?,
        foundationdb::options::MutationType::SetVersionstampedValue,
    );
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

fn decode_snapshot_item(
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
