use foundationdb::{RangeOption, options::StreamingMode};
use futures::{StreamExt, TryStreamExt, future::try_join_all, stream};

use crate::{
    CatalogError, CatalogResult, FdbOrderedCatalogKv,
    fdb_async::{MAX_FDB_READ_RETRIES, is_retryable_fdb_read_error},
    fdb_runtime::map_fdb_error,
};

impl FdbOrderedCatalogKv {
    pub(super) fn create_scan_options(
        &self,
        start: &[u8],
        end: &[u8],
        direction: crate::RangeDirection,
        limit: usize,
    ) -> RangeOption<'static> {
        let mut options =
            RangeOption::from(self.namespaced_range_bound(start)..self.namespaced_range_bound(end));
        options.limit = Some(limit);
        options.mode = StreamingMode::WantAll;
        options.target_bytes = crate::constants::FDB_READ_PAGE_TARGET_BYTES;
        if direction == crate::RangeDirection::Reverse {
            options.rev()
        } else {
            options
        }
    }

    pub(super) fn fetch_read_set(
        &self,
        keys: &[Vec<u8>],
        ranges: &[crate::kv::CatalogReadRange],
    ) -> CatalogResult<crate::kv::CatalogReadResult> {
        let kv = self.with_read_version()?;
        let result = crate::fdb_runtime::wait_for_fdb(
            "wait.fdb.read_set",
            futures::future::try_join(
                kv.fetch_values_with_retries(keys),
                stream::iter(ranges.iter().map(|range| {
                    kv.scan_range_async(&range.start, &range.end, range.direction, range.limit)
                }))
                .buffered(crate::constants::FDB_PREFIX_SCAN_CONCURRENCY)
                .try_collect(),
            ),
        );
        let result = result.map(|(values, ranges)| crate::kv::CatalogReadResult { values, ranges });
        #[cfg(feature = "runtime-metrics")]
        if let Ok(reads) = &result {
            record_read_set(keys.len(), ranges.len(), reads);
        }
        result
    }

    pub(super) fn retry_sync_fdb_read<T, State, Read, Reset>(
        operation: &'static str,
        mut state: State,
        mut read: Read,
        mut reset: Reset,
    ) -> CatalogResult<T>
    where
        Read: FnMut(&State) -> Result<T, foundationdb::FdbError>,
        Reset: FnMut(State, foundationdb::FdbError) -> CatalogResult<State>,
    {
        for attempt_index in 0..=MAX_FDB_READ_RETRIES {
            match read(&state) {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let catalog_error = map_fdb_error(error);
                    if !is_retryable_fdb_read_error(&catalog_error) {
                        return Err(catalog_error);
                    }
                    if attempt_index == MAX_FDB_READ_RETRIES {
                        return Err(sync_fdb_read_retry_exhausted(
                            operation,
                            attempt_index.saturating_add(1),
                            catalog_error,
                        ));
                    }
                    crate::runtime_metrics::record_runtime_measurement("fdb.read.retry", 1, 0);
                    state = reset(state, error)?;
                }
            }
        }
        unreachable!("foundationdb sync read retry loop did not run")
    }

    pub(super) async fn fetch_values_with_retries(
        &self,
        keys: &[Vec<u8>],
    ) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let logical_keys = keys;
        let keys = keys
            .iter()
            .map(|key| self.namespaced_key(key))
            .collect::<Vec<_>>();
        let mut trx = self.create_transaction()?;
        for attempt in 0..=MAX_FDB_READ_RETRIES {
            let result = try_join_all(keys.iter().map(|key| trx.get(key, false))).await;
            crate::fdb_runtime::record_transaction_usage(&trx);
            match result {
                Ok(values) => {
                    return logical_keys
                        .iter()
                        .zip(values)
                        .map(|(key, value)| {
                            value
                                .map(|bytes| crate::fdb_key_codec::decode_value(key, &bytes))
                                .transpose()
                        })
                        .collect();
                }
                Err(error) => {
                    let catalog_error = map_fdb_error(error);
                    if !is_retryable_fdb_read_error(&catalog_error) {
                        return Err(catalog_error);
                    }
                    if attempt == MAX_FDB_READ_RETRIES {
                        return Err(sync_fdb_read_retry_exhausted(
                            "batch_get",
                            attempt + 1,
                            catalog_error,
                        ));
                    }
                    crate::runtime_metrics::record_runtime_measurement("fdb.read.retry", 1, 0);
                    trx = trx.on_error(error).await.map_err(map_fdb_error)?;
                    self.apply_read_version(&trx);
                }
            }
        }
        unreachable!("read retry loop always returns")
    }

    pub(crate) fn fetch_transaction_values(
        &self,
        trx: &foundationdb::Transaction,
        keys: &[Vec<u8>],
    ) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        let reads = keys.iter().map(|key| {
            let namespaced = self.namespaced_key(key);
            async move { trx.get(&namespaced, false).await.map_err(map_fdb_error) }
        });
        let values =
            crate::fdb_runtime::wait_for_fdb("wait.fdb.validation_batch_get", try_join_all(reads))?;
        keys.iter()
            .zip(values)
            .map(|(key, value)| {
                value
                    .map(|bytes| crate::fdb_key_codec::decode_value(key, &bytes))
                    .transpose()
            })
            .collect()
    }
}

fn sync_fdb_read_retry_exhausted(
    operation: &'static str,
    attempts: usize,
    error: CatalogError,
) -> CatalogError {
    let CatalogError::FoundationDb {
        code,
        message,
        class,
    } = error
    else {
        return error;
    };
    CatalogError::FoundationDbRetryExhausted {
        operation,
        attempts,
        code,
        message,
        class,
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_read_set(keys: usize, ranges: usize, reads: &crate::kv::CatalogReadResult) {
    crate::runtime_metrics::record_runtime_measurement(
        "fdb.read_set.keys",
        keys as u64,
        reads
            .values
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .sum(),
    );
    crate::runtime_metrics::record_runtime_measurement(
        "fdb.read_set.ranges",
        ranges as u64,
        reads
            .ranges
            .iter()
            .flatten()
            .map(|row| (row.key.len() + row.value.len()) as u64)
            .sum(),
    );
}
