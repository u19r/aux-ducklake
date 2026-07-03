use std::collections::BTreeSet;

use crate::{
    CatalogId, CatalogResult, DuckLakeSnapshotId, RawSnapshotSequence, SnapshotRow, list_snapshots,
    runtime_payload::payload_string_value, runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshot_ops::parse_ducklake_snapshot_ids,
    runtime_snapshots::public_snapshot_order_span,
};

#[cfg(feature = "foundationdb")]
use crate::expire_snapshots;

pub(crate) fn expire_snapshots_for_runtime(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let expired = { expire_foundationdb_snapshots_for_payload(catalog, payload)? };
    Ok(expired_snapshots_payload(&expired).into_bytes())
}

#[cfg(feature = "foundationdb")]
fn expire_foundationdb_snapshots_for_payload(
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<SnapshotRow>> {
    let mut kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    let raw_sequences = snapshot_sequences_payload_values(&kv, catalog, payload)?;
    expire_snapshots(&mut kv, catalog, &raw_sequences)
}

#[cfg(not(feature = "foundationdb"))]
fn expire_foundationdb_snapshots_for_payload(
    _catalog: CatalogId,
    _payload: &[u8],
) -> CatalogResult<Vec<SnapshotRow>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

fn raw_snapshot_sequences_payload_values(
    payload: &[u8],
) -> CatalogResult<Vec<RawSnapshotSequence>> {
    let raw = payload_string_value(
        payload,
        "snapshot_ids",
        "runtime payload missing snapshot_ids",
    )?;
    Ok(parse_ducklake_snapshot_ids(&raw)?
        .into_iter()
        .map(|id| RawSnapshotSequence(id.0))
        .collect())
}

fn snapshot_sequences_payload_values(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<RawSnapshotSequence>> {
    match snapshot_kind_payload_value(payload)?.as_deref() {
        Some("ducklake") => raw_snapshot_sequences_payload_values(payload),
        Some("public") | None => public_snapshot_ids_payload_values(kv, catalog, payload),
        Some(other) => Err(crate::CatalogError::Decode(format!(
            "unsupported ExpireSnapshots snapshot_kind {other}"
        ))),
    }
}

fn snapshot_kind_payload_value(payload: &[u8]) -> CatalogResult<Option<String>> {
    let text = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("ExpireSnapshots payload is not utf8: {error}"))
    })?;
    for line in text.lines() {
        let Some(value) = line.strip_prefix("snapshot_kind=") else {
            continue;
        };
        return Ok(Some(value.to_owned()));
    }
    Ok(None)
}

fn public_snapshot_ids_payload_values(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<RawSnapshotSequence>> {
    let public_ids = raw_snapshot_sequences_payload_values(payload)?;
    let snapshots = list_snapshots(kv, catalog)?;
    let mut sequences = BTreeSet::new();
    for public_id in public_ids {
        let Some((first_order, last_order)) =
            public_snapshot_order_span(kv, catalog, DuckLakeSnapshotId(public_id.0))?
        else {
            continue;
        };
        for snapshot in &snapshots {
            if snapshot.order >= first_order && snapshot.order <= last_order {
                sequences.insert(snapshot.sequence);
            }
        }
    }
    Ok(sequences.into_iter().collect())
}

fn expired_snapshots_payload(expired: &[SnapshotRow]) -> String {
    let mut payload = format!("expired_snapshot_count={}\n", expired.len());
    for snapshot in expired {
        payload.push_str(&format!("expired_snapshot\t{}\n", snapshot.sequence));
    }
    payload
}
