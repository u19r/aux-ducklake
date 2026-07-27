use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "foundationdb")]
use crate::keys::schema_version_history_key_order_offset;
use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId, KvBatch,
    OrderedCatalogKv, RangeDirection, SnapshotRow,
    keys::{
        catalog_snapshot_version_key, current_schema_version_key,
        schema_version_begin_snapshot_key, schema_version_history_key,
        schema_version_history_prefix, schema_version_history_scan_end,
    },
};

pub(crate) fn load_current_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_schema_version(kv, &current_schema_version_key(catalog))
}

pub(crate) fn load_catalog_snapshot_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_schema_version(kv, &catalog_snapshot_version_key(catalog))
}

pub(crate) fn load_schema_version_begin_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    schema_version: u64,
) -> CatalogResult<Option<DuckLakeSnapshotId>> {
    let key = schema_version_begin_snapshot_key(catalog, schema_version);
    kv.get(&key)?
        .map(|value| decode_schema_version(&key, &value).map(DuckLakeSnapshotId))
        .transpose()
}

pub(crate) fn load_schema_version_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    let prefix = schema_version_history_prefix(catalog);
    let end = schema_version_history_scan_end(catalog, order);
    let row = kv
        .scan_range(&prefix, &end, RangeDirection::Reverse, 1)?
        .into_iter()
        .next();
    row.map_or(Ok(0), |item| decode_schema_version(&item.key, &item.value))
}

pub(crate) fn load_schema_versions_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    orders: &BTreeSet<CatalogOrderId>,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    let Some(first_order) = orders.first() else {
        return Ok(BTreeMap::new());
    };
    let last_order = orders.last().copied().unwrap_or(*first_order);
    let prefix = schema_version_history_prefix(catalog);
    let rows = kv.scan_range(
        &prefix,
        &schema_version_history_scan_end(catalog, last_order),
        RangeDirection::Forward,
        usize::MAX,
    )?;
    let mut history = Vec::with_capacity(rows.len());
    for item in rows {
        let tail = item.key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
            CatalogError::InvalidKey("schema version history key has wrong prefix".to_owned())
        })?;
        let order_bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
            CatalogError::InvalidKey("schema version history order has wrong length".to_owned())
        })?;
        history.push((
            CatalogOrderId::from_bytes(first_order.kind(), order_bytes),
            decode_schema_version(&item.key, &item.value)?,
        ));
    }
    let mut versions = BTreeMap::new();
    let mut history = history.into_iter().peekable();
    let mut current = 0;
    for order in orders {
        while history
            .peek()
            .is_some_and(|(change_order, _)| change_order <= order)
        {
            if let Some((_, version)) = history.next() {
                current = version;
            }
        }
        versions.insert(*order, current);
    }
    Ok(versions)
}

pub(crate) fn stage_next_schema_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    let current = load_schema_version(kv, &current_schema_version_key(catalog))?.unwrap_or(0);
    let next = current.saturating_add(1);
    batch.put(
        current_schema_version_key(catalog),
        next.to_be_bytes().to_vec(),
    );
    batch.put(
        schema_version_history_key(catalog, snapshot.order),
        next.to_be_bytes().to_vec(),
    );
    batch.put(
        schema_version_begin_snapshot_key(catalog, next),
        snapshot.sequence.to_be_bytes().to_vec(),
    );
    stage_next_catalog_snapshot_version(kv, batch, catalog)?;
    Ok(())
}

pub(crate) fn stage_next_catalog_snapshot_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
) -> CatalogResult<()> {
    let current = load_schema_version(kv, &catalog_snapshot_version_key(catalog))?.unwrap_or(0);
    batch.put(
        catalog_snapshot_version_key(catalog),
        current.saturating_add(1).to_be_bytes().to_vec(),
    );
    Ok(())
}

fn load_schema_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    key: &[u8],
) -> CatalogResult<Option<u64>> {
    kv.get(key)?
        .map(|value| decode_schema_version(key, &value))
        .transpose()
}

fn decode_schema_version(key: &[u8], value: &[u8]) -> CatalogResult<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        let key_label = crate::keys::decode_key(key).unwrap_or_else(|_| "<invalid-key>".to_owned());
        CatalogError::InvalidKey(format!(
            "invalid current schema version value for key {}",
            key_label
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_next_schema_version(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    let next = next_fdb_version(kv, trx, current_schema_version_key(catalog))?;
    trx.set(
        &kv.namespaced_key(&current_schema_version_key(catalog)),
        &next.to_be_bytes(),
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &schema_version_history_key(catalog, snapshot.order),
            schema_version_history_key_order_offset(catalog),
        )?,
        &next.to_be_bytes(),
        foundationdb::options::MutationType::SetVersionstampedKey,
    );
    trx.set(
        &kv.namespaced_key(&schema_version_begin_snapshot_key(catalog, next)),
        &snapshot.sequence.to_be_bytes(),
    );
    stage_fdb_next_catalog_snapshot_version(kv, trx, catalog)?;
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_next_catalog_snapshot_version(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
) -> CatalogResult<()> {
    stage_fdb_next_version_key(kv, trx, catalog_snapshot_version_key(catalog))
}

#[cfg(feature = "foundationdb")]
fn stage_fdb_next_version_key(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    key: Vec<u8>,
) -> CatalogResult<()> {
    let next = next_fdb_version(kv, trx, key.clone())?;
    trx.set(&kv.namespaced_key(&key), &next.to_be_bytes());
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn next_fdb_version(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    key: Vec<u8>,
) -> CatalogResult<u64> {
    let namespaced = kv.namespaced_key(&key);
    let current = futures::executor::block_on(trx.get(&namespaced, false))
        .map_err(crate::fdb_runtime::map_fdb_error)?
        .map(|value| decode_schema_version(&key, &value))
        .transpose()?
        .unwrap_or(0);
    Ok(current.saturating_add(1))
}

#[cfg(test)]
#[path = "schema_version_state_tests.rs"]
mod schema_version_state_tests;
