use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, SnapshotRow,
    keys::{snapshot_key, snapshot_timestamp_key},
    store::decode_snapshot_item,
};

/// Data-changing snapshots keep their raw sequence when public groups coalesce:
/// a data group can merge into its predecessor only when both sequences match.
/// Callers supply orders from inline payloads or deletion events, never metadata helpers.
pub(crate) fn active_data_snapshot_sequences(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    orders: &BTreeSet<CatalogOrderId>,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    if orders.is_empty() {
        return Ok(BTreeMap::new());
    }
    let snapshots = fetch_data_snapshots(kv, catalog, orders)?;
    let keys = snapshots
        .iter()
        .map(|row| snapshot_timestamp_key(catalog, row.created_at_micros, row.order))
        .collect::<Vec<_>>();
    let mut sequences = BTreeMap::new();
    for (snapshot, active) in snapshots.into_iter().zip(kv.batch_get(&keys)?) {
        if let Some(value) = active {
            let row =
                decode_snapshot_item(catalog, &snapshot_key(catalog, snapshot.order), &value)?;
            sequences.insert(snapshot.order, row.sequence.0);
        }
    }
    crate::runtime_metrics::record_runtime_measurement(
        "snapshot.inline_data.resolved_orders",
        sequences.len() as u64,
        0,
    );
    Ok(sequences)
}

fn fetch_data_snapshots(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    orders: &BTreeSet<CatalogOrderId>,
) -> CatalogResult<Vec<SnapshotRow>> {
    let keys = orders
        .iter()
        .map(|order| snapshot_key(catalog, *order))
        .collect::<Vec<_>>();
    keys.iter()
        .zip(kv.batch_get(&keys)?)
        .filter_map(|(key, value)| value.map(|value| decode_snapshot_item(catalog, key, &value)))
        .collect()
}
