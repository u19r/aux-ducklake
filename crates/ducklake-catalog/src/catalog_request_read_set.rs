use std::{cell::RefCell, collections::BTreeMap};

use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, CatalogResult,
    OrderedCatalogKv, RangeDirection, SnapshotRow,
    keys::{
        catalog_file_stats_version_key, catalog_snapshot_version_key, conflict_max_catalog_id_key,
        conflict_max_file_id_key, conflict_watermarks_initialized_key, current_schema_version_key,
        latest_snapshot_row_key, snapshot_prefix,
    },
};

type ReadKey = (CatalogCacheNamespace, CatalogId);
type RequestReads = BTreeMap<ReadKey, PreparedRead>;

#[derive(Default)]
struct PreparedRead {
    // Outer None means that this request has not pinned an identity yet.
    snapshot: Option<Option<SnapshotRow>>,
    preparation: Option<CatalogPreparation>,
}

struct CatalogPreparation {
    snapshot: Option<SnapshotRow>,
    values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[derive(Clone, Copy)]
pub(crate) enum CatalogHeaderProjection {
    Mutation(Option<crate::CommitAttemptId>),
    Conflict,
    Catalog,
    FileStatistics,
}

impl CatalogHeaderProjection {
    fn build_keys(self, catalog: CatalogId) -> Vec<Vec<u8>> {
        let mut keys = vec![latest_snapshot_row_key(catalog)];
        keys.extend(match self {
            Self::Mutation(recovery) => std::iter::once(current_schema_version_key(catalog))
                .chain(recovery.map(|id| crate::conflict::commit_attempt_key(catalog, id)))
                .collect(),
            Self::Conflict => vec![
                conflict_watermarks_initialized_key(catalog),
                conflict_max_catalog_id_key(catalog),
                conflict_max_file_id_key(catalog),
                current_schema_version_key(catalog),
            ],
            Self::Catalog => vec![catalog_snapshot_version_key(catalog)],
            Self::FileStatistics => vec![catalog_file_stats_version_key(catalog)],
        });
        keys
    }
}

thread_local! {
    static REQUEST_READS: RefCell<Option<RequestReads>> = const { RefCell::new(None) };
}

pub(crate) struct RequestReadSetGuard {
    previous: Option<RequestReads>,
}

pub(crate) fn begin() -> RequestReadSetGuard {
    RequestReadSetGuard {
        previous: REQUEST_READS.with(|reads| reads.replace(Some(BTreeMap::new()))),
    }
}

impl Drop for RequestReadSetGuard {
    fn drop(&mut self) {
        REQUEST_READS.with(|reads| reads.replace(self.previous.take()));
    }
}

pub(crate) fn clear() {
    REQUEST_READS.with(|reads| reads.replace(Some(BTreeMap::new())));
}

pub(crate) fn find_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> Option<Option<SnapshotRow>> {
    REQUEST_READS.with(|reads| {
        reads
            .borrow()
            .as_ref()?
            .get(&build_read_key(kv, catalog))?
            .snapshot
            .clone()
    })
}

pub(crate) fn remember_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: Option<SnapshotRow>,
) {
    REQUEST_READS.with(|reads| {
        if let Some(reads) = reads.borrow_mut().as_mut() {
            reads
                .entry(build_read_key(kv, catalog))
                .or_default()
                .snapshot = Some(snapshot);
        }
    });
}

pub(crate) fn fetch_latest_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<SnapshotRow>> {
    fetch_snapshot_row(kv, catalog, kv.get(&latest_snapshot_row_key(catalog))?)
}

pub(crate) fn fetch_preparation_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    projection: CatalogHeaderProjection,
) -> CatalogResult<Option<SnapshotRow>> {
    let keys = projection.build_keys(catalog);
    if let Some(snapshot) = find_prepared_snapshot(kv, catalog, &keys) {
        crate::runtime_metrics::record_runtime_measurement("read_set.header_reuse", 1, 0);
        return Ok(snapshot);
    }
    let values = kv.batch_get(&keys)?;
    if values.len() != keys.len() {
        return Err(CatalogError::Backend(format!(
            "catalog preparation batch returned {} values for {} keys",
            values.len(),
            keys.len()
        )));
    }
    let count = keys.len() - 1;
    let mut values = keys.into_iter().zip(values).collect::<BTreeMap<_, _>>();
    let snapshot = fetch_snapshot_row(
        kv,
        catalog,
        values.remove(&latest_snapshot_row_key(catalog)).flatten(),
    )?;
    REQUEST_READS.with(|reads| {
        if let Some(reads) = reads.borrow_mut().as_mut() {
            reads
                .entry(build_read_key(kv, catalog))
                .or_default()
                .preparation = Some(CatalogPreparation {
                snapshot: snapshot.clone(),
                values,
            });
        }
    });
    crate::runtime_metrics::record_runtime_measurement("read_set.prepared_keys", count as u64, 0);
    Ok(snapshot)
}

pub(crate) fn fetch_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    key: &[u8],
) -> CatalogResult<Option<Vec<u8>>> {
    if let Some(values) = find_values(kv, catalog, &[key.to_vec()]) {
        return Ok(values.into_iter().next().flatten());
    }
    kv.get(key)
}

pub(crate) fn find_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    keys: &[Vec<u8>],
) -> Option<Vec<Option<Vec<u8>>>> {
    let values = REQUEST_READS.with(|reads| {
        let reads = reads.borrow();
        let read = reads
            .as_ref()?
            .get(&build_read_key(kv, catalog))?
            .preparation
            .as_ref()?;
        keys.iter()
            .map(|key| read.values.get(key).cloned())
            .collect::<Option<Vec<_>>>()
    });
    if values.is_some() {
        crate::runtime_metrics::record_runtime_measurement(
            "read_set.fact_hits",
            keys.len() as u64,
            0,
        );
    }
    values
}

pub(crate) fn find_value_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    key: &[u8],
) -> Option<Option<Vec<u8>>> {
    let value = REQUEST_READS.with(|reads| {
        let reads = reads.borrow();
        let read = reads
            .as_ref()?
            .get(&build_read_key(kv, catalog))?
            .preparation
            .as_ref()?;
        if read.snapshot.as_ref()?.order != order {
            return None;
        }
        read.values.get(key).cloned()
    });
    if value.as_ref().and_then(Option::as_ref).is_some() {
        crate::runtime_metrics::record_runtime_measurement("read_set.schema_hits", 1, 0);
    }
    value
}

pub(crate) fn invalidate_after_mutation(catalog: CatalogId) {
    REQUEST_READS.with(|reads| {
        if let Some(reads) = reads.borrow_mut().as_mut() {
            reads.retain(|(namespace, cached_catalog), read| {
                if *cached_catalog != catalog {
                    return true;
                }
                read.preparation = None;
                namespace.has_read_context()
            });
        }
    });
}

pub(crate) fn invalidate_snapshot(catalog: CatalogId) {
    REQUEST_READS.with(|reads| {
        if let Some(reads) = reads.borrow_mut().as_mut() {
            reads.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
        }
    });
}

pub(crate) fn end_context(namespace: CatalogCacheNamespace, catalog: CatalogId, context: u64) {
    REQUEST_READS.with(|reads| {
        if let Some(reads) = reads.borrow_mut().as_mut() {
            reads.remove(&(namespace.with_read_context(Some(context)), catalog));
        }
    });
}

fn find_prepared_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    keys: &[Vec<u8>],
) -> Option<Option<SnapshotRow>> {
    REQUEST_READS.with(|reads| {
        let reads = reads.borrow();
        let prepared = reads
            .as_ref()?
            .get(&build_read_key(kv, catalog))?
            .preparation
            .as_ref()?;
        keys.iter()
            .skip(1)
            .all(|key| prepared.values.contains_key(key))
            .then(|| prepared.snapshot.clone())
    })
}

fn build_read_key(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> ReadKey {
    (kv.catalog_cache_namespace(), catalog)
}

fn fetch_snapshot_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: Option<Vec<u8>>,
) -> CatalogResult<Option<SnapshotRow>> {
    if let Some(value) = value {
        return crate::store::decode_latest_snapshot_value(&value).map(Some);
    }
    kv.scan_prefix(&snapshot_prefix(catalog), RangeDirection::Reverse, 1)?
        .first()
        .map(|item| crate::store::decode_snapshot_item(catalog, &item.key, &item.value))
        .transpose()
}
