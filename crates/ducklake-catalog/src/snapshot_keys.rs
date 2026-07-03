use crate::{
    CatalogError, CatalogResult,
    ids::{CatalogId, CatalogOrderId},
    keys::{KeyFamily, family_prefix},
};

#[must_use]
pub fn snapshot_key(catalog: CatalogId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Snapshot);
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn snapshot_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::Snapshot)
}

#[must_use]
pub fn snapshot_timestamp_key(
    catalog: CatalogId,
    created_at_micros: i64,
    order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::SnapshotByTimestamp);
    key.extend_from_slice(&sortable_i64(created_at_micros));
    key.push(b'/');
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn snapshot_timestamp_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::SnapshotByTimestamp)
}

#[must_use]
pub fn decode_snapshot_timestamp_key(
    catalog: CatalogId,
    key: &[u8],
) -> CatalogResult<(i64, CatalogOrderId)> {
    let prefix = snapshot_timestamp_prefix(catalog);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "snapshot timestamp key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = 8 + 1 + CatalogOrderId::LEN;
    if tail.len() != minimum_len {
        return Err(CatalogError::InvalidKey(format!(
            "snapshot timestamp key tail must be {minimum_len} bytes, got {}",
            tail.len()
        )));
    }
    if tail[8] != b'/' {
        return Err(CatalogError::InvalidKey(
            "snapshot timestamp key is missing separator".to_owned(),
        ));
    }
    let timestamp_bytes: [u8; 8] = tail[..8]
        .try_into()
        .map_err(|_| CatalogError::InvalidKey("snapshot timestamp is truncated".to_owned()))?;
    let order_bytes: [u8; CatalogOrderId::LEN] = tail[9..].try_into().map_err(|_| {
        CatalogError::InvalidKey("snapshot timestamp order is truncated".to_owned())
    })?;
    Ok((
        unsortable_i64(timestamp_bytes),
        CatalogOrderId::uuid_v7(u128::from_be_bytes(order_bytes)),
    ))
}

fn sortable_i64(value: i64) -> [u8; 8] {
    (value ^ i64::MIN).to_be_bytes()
}

fn unsortable_i64(bytes: [u8; 8]) -> i64 {
    i64::from_be_bytes(bytes) ^ i64::MIN
}
