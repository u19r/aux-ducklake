use std::ops::Deref;

use foundationdb::{
    RangeOption,
    options::{ConflictRangeType, TransactionOption},
};
use futures::{TryStreamExt, executor::block_on, future::try_join_all};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, FdbOrderedCatalogKv, KvBatch,
    MutableCatalogKv, OrderedCatalogKv, RangeDirection, RangeItem, RawSnapshotSequence,
    TableVersionReplacement,
    fdb_runtime::{decode_fence_version, map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::batch_exceeds_commit_limit,
    keys::prefix_end,
};
#[cfg(feature = "runtime-metrics")]
use crate::{
    keys::KeyFamily,
    runtime_metrics::{RuntimeMetricStatus, record_runtime_kv},
    runtime_protocol::RuntimeCatalogBackend,
};

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
macro_rules! record_kv_result {
    ($operation:expr, $key:expr, $bytes:expr, $started:expr $(,)?) => {
        record_kv_metric($operation, key_family_label($key), $bytes, $started)
    };
}

#[cfg(not(feature = "runtime-metrics"))]
macro_rules! record_kv_result {
    ($operation:expr, $key:expr, $bytes:expr, $started:expr $(,)?) => {{
        let _ = ($operation, $key, $started);
    }};
}

#[cfg(feature = "runtime-metrics")]
macro_rules! record_batch_get_result {
    ($keys:expr, $result:expr, $started:expr $(,)?) => {
        record_batch_get_metric(batch_key_family_label($keys), $result, $started)
    };
}

#[cfg(not(feature = "runtime-metrics"))]
macro_rules! record_batch_get_result {
    ($keys:expr, $result:expr, $started:expr $(,)?) => {{
        let _ = ($keys, $started);
    }};
}

#[cfg(feature = "runtime-metrics")]
macro_rules! record_range_result {
    ($operation:expr, $start:expr, $result:expr, $started:expr $(,)?) => {
        record_range_metric($operation, key_family_label($start), $result, $started)
    };
}

#[cfg(not(feature = "runtime-metrics"))]
macro_rules! record_range_result {
    ($operation:expr, $start:expr, $result:expr, $started:expr $(,)?) => {{
        let _ = ($operation, $start, $started);
    }};
}

impl FdbOrderedCatalogKv {
    fn commit_batch(&self, batch: KvBatch) -> CatalogResult<()> {
        let started = RuntimeMetricStage::start();
        let mutation_bytes = batch.estimated_mutation_bytes();
        let mutation_items = batch
            .checks()
            .len()
            .saturating_add(batch.writes().len())
            .saturating_add(batch.deletes().len())
            .saturating_add(batch.fence_writes().len());
        if batch_exceeds_commit_limit(&batch, Self::MAX_COMMIT_BYTES) {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb commit is {mutation_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        for (key, expected) in batch.checks() {
            let namespaced = self.namespaced_key(key);
            trx.add_conflict_range(
                &namespaced,
                &prefix_end(&namespaced),
                ConflictRangeType::Read,
            )
            .map_err(map_fdb_error)?;
            let read_started = RuntimeMetricStage::start();
            let actual = block_on(trx.get(&namespaced, false))
                .map_err(map_fdb_error)?
                .map(|value| value.deref().to_vec());
            record_kv_result!(
                "commit_check_get",
                key,
                Ok(actual.as_ref().map_or(0, Vec::len)),
                read_started,
            );
            if actual != *expected {
                return Err(CatalogError::ConflictFenceChanged { fence: key.clone() });
            }
        }
        for key in batch.deletes() {
            trx.clear(&self.namespaced_key(key));
        }
        for (key, value) in batch.writes() {
            trx.set(&self.namespaced_key(key), value);
        }
        for key in batch.fence_writes() {
            let namespaced = self.namespaced_key(key);
            trx.add_conflict_range(
                &namespaced,
                &prefix_end(&namespaced),
                ConflictRangeType::Write,
            )
            .map_err(map_fdb_error)?;
            let read_started = RuntimeMetricStage::start();
            let current_bytes = block_on(trx.get(&namespaced, false))
                .map_err(map_fdb_error)?
                .map(|value| value.deref().to_vec());
            record_kv_result!(
                "commit_fence_get",
                key,
                Ok(current_bytes.as_ref().map_or(0, Vec::len)),
                read_started,
            );
            let current = current_bytes
                .as_deref()
                .map(decode_fence_version)
                .transpose()?
                .unwrap_or(0);
            let next = current.saturating_add(1);
            trx.set(&namespaced, &next.to_be_bytes());
        }
        let result = block_on(trx.commit()).map_err(map_fdb_commit_error);
        record_commit_result(mutation_items, mutation_bytes, result.as_ref(), started);
        result.map(|_| ())
    }
}

impl OrderedCatalogKv for FdbOrderedCatalogKv {
    fn catalog_cache_namespace(&self) -> crate::CatalogCacheNamespace {
        FdbOrderedCatalogKv::catalog_cache_namespace(self)
    }

    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        let started = RuntimeMetricStage::start();
        let trx = self.create_transaction()?;
        let result = block_on(trx.get(&self.namespaced_key(key), false))
            .map_err(map_fdb_error)
            .map(|value| value.map(|bytes| bytes.deref().to_vec()));
        record_kv_result!(
            "get",
            key,
            result
                .as_ref()
                .map(|value| value.as_ref().map_or(0, Vec::len)),
            started,
        );
        result
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        let started = RuntimeMetricStage::start();
        let trx = self.create_transaction()?;
        let trx = &trx;
        let reads = keys.iter().map(|key| {
            let namespaced = self.namespaced_key(key);
            async move {
                trx.get(&namespaced, false)
                    .await
                    .map_err(map_fdb_error)
                    .map(|value| value.map(|bytes| bytes.deref().to_vec()))
            }
        });
        let result = block_on(try_join_all(reads));
        record_batch_get_result!(keys, result.as_ref(), started);
        result
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        self.scan_range_with_operation("scan_prefix", prefix, &prefix_end(prefix), direction, limit)
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        self.scan_range_with_operation("scan_range", start, end, direction, limit)
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        self.get(key)
    }
}

impl FdbOrderedCatalogKv {
    fn scan_range_with_operation(
        &self,
        operation: &str,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let started = RuntimeMetricStage::start();
        let trx = self.create_transaction()?;
        trx.set_option(TransactionOption::Timeout(5_000))
            .map_err(map_fdb_error)?;
        let mut options = RangeOption::from(self.namespaced_key(start)..self.namespaced_key(end));
        if limit != usize::MAX {
            options.limit = Some(limit);
        }
        if direction == RangeDirection::Reverse {
            options = options.rev();
        }
        let values = block_on(
            trx.get_ranges_keyvalues(options, false)
                .try_collect::<Vec<_>>(),
        )
        .map_err(map_fdb_error)?;
        let result = values
            .into_iter()
            .map(|item| {
                Ok(RangeItem {
                    key: self.strip_namespace(item.key())?,
                    value: item.value().to_vec(),
                })
            })
            .collect::<CatalogResult<Vec<_>>>();
        record_range_result!(operation, start, result.as_ref(), started);
        result
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_kv_metric(
    operation: &str,
    key_family: &'static str,
    bytes: Result<usize, &CatalogError>,
    started: RuntimeMetricStage,
) {
    match bytes {
        Ok(bytes) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.{operation}.{key_family}"),
            RuntimeMetricStatus::Ok,
            usize::from(bytes > 0) as u64,
            bytes as u64,
            started.elapsed_micros(),
        ),
        Err(_) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.{operation}.{key_family}"),
            RuntimeMetricStatus::Error,
            0,
            0,
            started.elapsed_micros(),
        ),
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_batch_get_metric(
    key_family: &'static str,
    result: Result<&Vec<Option<Vec<u8>>>, &CatalogError>,
    started: RuntimeMetricStage,
) {
    match result {
        Ok(values) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.batch_get.{key_family}"),
            RuntimeMetricStatus::Ok,
            values.iter().filter(|value| value.is_some()).count() as u64,
            values
                .iter()
                .map(|value| value.as_ref().map_or(0, Vec::len))
                .sum::<usize>() as u64,
            started.elapsed_micros(),
        ),
        Err(_) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.batch_get.{key_family}"),
            RuntimeMetricStatus::Error,
            0,
            0,
            started.elapsed_micros(),
        ),
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_range_metric(
    operation: &str,
    key_family: &'static str,
    result: Result<&Vec<RangeItem>, &CatalogError>,
    started: RuntimeMetricStage,
) {
    match result {
        Ok(items) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.{operation}.{key_family}"),
            RuntimeMetricStatus::Ok,
            items.len() as u64,
            items
                .iter()
                .map(|item| item.key.len().saturating_add(item.value.len()))
                .sum::<usize>() as u64,
            started.elapsed_micros(),
        ),
        Err(_) => record_runtime_kv(
            RuntimeCatalogBackend::FoundationDb,
            &format!("fdb.{operation}.{key_family}"),
            RuntimeMetricStatus::Error,
            0,
            0,
            started.elapsed_micros(),
        ),
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_commit_result(
    mutation_items: usize,
    mutation_bytes: usize,
    result: Result<&foundationdb::TransactionCommitted, &CatalogError>,
    started: RuntimeMetricStage,
) {
    let status = if result.is_ok() {
        RuntimeMetricStatus::Ok
    } else {
        RuntimeMetricStatus::Error
    };
    record_runtime_kv(
        RuntimeCatalogBackend::FoundationDb,
        "fdb.commit.batch",
        status,
        mutation_items as u64,
        mutation_bytes as u64,
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
fn record_commit_result(
    _mutation_items: usize,
    _mutation_bytes: usize,
    _result: Result<&foundationdb::TransactionCommitted, &CatalogError>,
    _started: RuntimeMetricStage,
) {
}

#[cfg(feature = "runtime-metrics")]
fn key_family_label(key: &[u8]) -> &'static str {
    let Some(family) = key.get(9).and_then(|code| KeyFamily::from_code(*code).ok()) else {
        return "unknown";
    };
    match family {
        KeyFamily::Object => match key.get(11).copied() {
            Some(b't') => "object.table",
            Some(b'T') => "object.current-table",
            Some(b'V') => "object.table-visibility",
            Some(b's') => "object.schema",
            Some(b'v') => "object.view",
            Some(b'm') => "object.macro",
            Some(b'n') => "object.current-table-name",
            Some(_) => "object.unknown",
            None => "object",
        },
        KeyFamily::MetadataVersion => metadata_version_key_label(key),
        _ => family.label(),
    }
}

#[cfg(feature = "runtime-metrics")]
fn metadata_version_key_label(key: &[u8]) -> &'static str {
    let suffix = key.get(11..).unwrap_or_default();
    if suffix == b"current-schema-version" {
        return "metadata-version.current-schema-version";
    }
    if suffix == b"catalog-snapshot-version" {
        return "metadata-version.catalog-snapshot-version";
    }
    if suffix == b"latest-snapshot-row" {
        return "metadata-version.latest-snapshot-row";
    }
    if suffix == b"catalog-file-stats" {
        return "metadata-version.catalog-file-stats";
    }
    if suffix.starts_with(b"table-file-stats/") {
        return "metadata-version.table-file-stats";
    }
    "metadata-version"
}

#[cfg(feature = "runtime-metrics")]
fn batch_key_family_label(keys: &[Vec<u8>]) -> &'static str {
    let mut labels = keys.iter().map(|key| key_family_label(key));
    let Some(first) = labels.next() else {
        return "empty";
    };
    if labels.all(|label| label == first) {
        first
    } else {
        "mixed"
    }
}

impl MutableCatalogKv for FdbOrderedCatalogKv {
    fn generated_order_id(&mut self) -> CatalogResult<CatalogOrderId> {
        Err(CatalogError::InvalidMutation(
            "foundationdb catalog order ids must come from post-commit versionstamps".to_owned(),
        ))
    }

    fn commit(&mut self, batch: KvBatch) -> CatalogResult<()> {
        self.commit_batch(batch)
    }

    fn commit_table_replacements(
        &mut self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        self.commit_table_replacements_versionstamped(catalog, previous_sequence, replacements)
    }
}
