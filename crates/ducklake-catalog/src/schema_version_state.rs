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

pub(crate) fn load_catalog_snapshot_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    let key = catalog_snapshot_version_key(catalog);
    crate::catalog_request_read_set::fetch_value(kv, catalog, &key)?
        .map(|value| decode_schema_version(&key, &value))
        .transpose()
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
    let key = current_schema_version_key(catalog);
    if let Some(Some(value)) =
        crate::catalog_request_read_set::find_value_at(kv, catalog, order, &key)
    {
        return decode_schema_version(&key, &value);
    }
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
    if orders.is_empty() {
        return Ok(BTreeMap::new());
    }
    if orders.len() == 1 {
        let order = *orders.first().expect("one requested order");
        return Ok(BTreeMap::from([(
            order,
            load_schema_version_at(kv, catalog, order)?,
        )]));
    }
    let ranges = schema_version_read_ranges(catalog, orders);
    let rows = kv.read_set(&[], &ranges)?.ranges;
    resolve_schema_version_reads(catalog, orders, rows)
}

pub(crate) fn schema_version_read_ranges(
    catalog: CatalogId,
    orders: &BTreeSet<CatalogOrderId>,
) -> Vec<crate::kv::CatalogReadRange> {
    let Some(first) = orders.first() else {
        return Vec::new();
    };
    let boundary = schema_version_history_scan_end(catalog, *first);
    let mut ranges = vec![crate::kv::CatalogReadRange {
        start: schema_version_history_prefix(catalog),
        end: boundary.clone(),
        direction: RangeDirection::Reverse,
        limit: 1,
    }];
    if let Some(last) = orders.last().filter(|last| *last != first) {
        ranges.push(crate::kv::CatalogReadRange {
            start: boundary,
            end: schema_version_history_scan_end(catalog, *last),
            direction: RangeDirection::Forward,
            limit: usize::MAX,
        });
    }
    ranges
}

pub(crate) fn resolve_schema_version_reads(
    catalog: CatalogId,
    orders: &BTreeSet<CatalogOrderId>,
    mut ranges: Vec<Vec<crate::RangeItem>>,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    let Some(first) = orders.first() else {
        return Ok(BTreeMap::new());
    };
    if ranges.len() != usize::from(orders.len() > 1) + 1 {
        return Err(CatalogError::Backend(
            "schema version read set has wrong range count".to_owned(),
        ));
    }
    let prefix = schema_version_history_prefix(catalog);
    let current = ranges[0]
        .first()
        .map_or(Ok(0), |item| decode_schema_version(&item.key, &item.value))?;
    let rows = if ranges.len() > 1 {
        ranges.pop().unwrap_or_default()
    } else {
        Vec::new()
    };
    crate::runtime_metrics::record_runtime_measurement(
        "schema_history.interval_rows",
        rows.len() as u64,
        0,
    );
    let history = rows
        .into_iter()
        .map(|item| decode_schema_history_row(item, &prefix, first.kind()))
        .collect::<CatalogResult<Vec<_>>>()?;
    Ok(orders
        .iter()
        .map(|order| {
            let end = history.partition_point(|(change_order, _)| change_order <= order);
            (
                *order,
                end.checked_sub(1).map_or(current, |index| history[index].1),
            )
        })
        .collect())
}

fn decode_schema_history_row(
    item: crate::RangeItem,
    prefix: &[u8],
    kind: crate::CatalogOrderKind,
) -> CatalogResult<(CatalogOrderId, u64)> {
    let tail = item.key.strip_prefix(prefix).ok_or_else(|| {
        CatalogError::InvalidKey("schema version history key has wrong prefix".to_owned())
    })?;
    let order_bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey("schema version history order has wrong length".to_owned())
    })?;
    Ok((
        CatalogOrderId::from_bytes(kind, order_bytes),
        decode_schema_version(&item.key, &item.value)?,
    ))
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

pub(crate) fn decode_schema_version(key: &[u8], value: &[u8]) -> CatalogResult<u64> {
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
