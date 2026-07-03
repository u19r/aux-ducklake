use crate::{
    CatalogId, CatalogResult, DuckLakeSnapshotId, SnapshotTimestampBound,
    runtime_catalog_snapshot::CatalogSnapshotIdKind,
    runtime_foundationdb::{
        runtime_get_foundationdb_catalog_for_snapshot, runtime_get_foundationdb_conflict_snapshot,
        runtime_get_foundationdb_snapshot, runtime_get_foundationdb_snapshot_at,
        runtime_get_foundationdb_snapshot_at_timestamp,
        runtime_list_foundationdb_snapshot_changes_after, runtime_list_foundationdb_snapshots,
    },
    runtime_payload::{
        optional_payload_i64_value, optional_payload_string_value, payload_i64_value,
        payload_u64_value,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshots::ListSnapshotsPayload,
};

pub(crate) fn get_snapshot(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    runtime_get_foundationdb_snapshot(catalog)
}

pub(crate) fn get_conflict_snapshot(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    runtime_get_foundationdb_conflict_snapshot(catalog)
}

pub(crate) fn get_snapshot_at(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let snapshot_id = snapshot_id_payload_value(payload)?;
    runtime_get_foundationdb_snapshot_at(catalog, snapshot_id)
}

pub(crate) fn get_snapshot_at_timestamp(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let timestamp_micros = payload_i64_value(
        payload,
        "timestamp_micros",
        "runtime payload missing timestamp_micros",
    )?;
    let bound = timestamp_bound_payload_value(payload)?;
    runtime_get_foundationdb_snapshot_at_timestamp(catalog, timestamp_micros, bound)
}

pub(crate) fn get_catalog_for_snapshot(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let snapshot_id = snapshot_id_payload_value(payload)?;
    let snapshot_kind = snapshot_id_kind_payload_value(payload)?;
    runtime_get_foundationdb_catalog_for_snapshot(catalog, snapshot_id, snapshot_kind)
}

pub(crate) fn list_snapshots(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = list_snapshots_payload_values(payload)?;
    runtime_list_foundationdb_snapshots(catalog, payload)
}

pub(crate) fn list_snapshot_changes_after(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let base_snapshot_id = DuckLakeSnapshotId(snapshot_id_payload_value(payload)?);
    runtime_list_foundationdb_snapshot_changes_after(catalog, base_snapshot_id)
}

fn snapshot_id_payload_value(payload: &[u8]) -> CatalogResult<u64> {
    payload_u64_value(
        payload,
        "snapshot_id",
        "runtime payload missing snapshot_id",
    )
}

fn snapshot_id_kind_payload_value(payload: &[u8]) -> CatalogResult<CatalogSnapshotIdKind> {
    match optional_snapshot_kind(payload)?.as_deref() {
        Some("ducklake") => Ok(CatalogSnapshotIdKind::DuckLakeSequence),
        Some("public") | None => Ok(CatalogSnapshotIdKind::PublicSnapshot),
        Some(other) => Err(crate::CatalogError::InvalidMutation(format!(
            "unsupported snapshot_kind '{other}'"
        ))),
    }
}

fn optional_snapshot_kind(payload: &[u8]) -> CatalogResult<Option<String>> {
    let text = std::str::from_utf8(payload)
        .map_err(|error| crate::CatalogError::Decode(format!("payload is not utf-8: {error}")))?;
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key == "snapshot_kind" {
            return Ok(Some(value.to_owned()));
        }
    }
    Ok(None)
}

fn timestamp_bound_payload_value(payload: &[u8]) -> CatalogResult<SnapshotTimestampBound> {
    match crate::runtime_payload::payload_str_value(
        payload,
        "bound",
        "runtime payload missing timestamp bound",
    )? {
        "lower" => Ok(SnapshotTimestampBound::Lower),
        "upper" => Ok(SnapshotTimestampBound::Upper),
        value => Err(crate::CatalogError::Decode(format!(
            "unsupported timestamp snapshot bound {value}"
        ))),
    }
}

fn list_snapshots_payload_values(payload: &[u8]) -> CatalogResult<ListSnapshotsPayload> {
    Ok(ListSnapshotsPayload {
        older_than_micros: optional_payload_i64_value(payload, "older_than_micros")?,
        requested_ducklake_ids: optional_payload_string_value(payload, "snapshot_ids")?
            .map(|raw| parse_ducklake_snapshot_ids(&raw))
            .transpose()?,
        protect_latest: crate::runtime_payload::optional_payload_str_value(
            payload,
            "protect_latest",
        )?
        .is_some_and(|value| value == "true"),
    })
}

pub(crate) fn parse_ducklake_snapshot_ids(raw: &str) -> CatalogResult<Vec<DuckLakeSnapshotId>> {
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<u64>()
                .map(DuckLakeSnapshotId)
                .map_err(|error| {
                    crate::CatalogError::Decode(format!(
                        "invalid runtime snapshot id {part}: {error}"
                    ))
                })
        })
        .collect()
}
