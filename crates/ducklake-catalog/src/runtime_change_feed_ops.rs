use crate::{
    CatalogId, CatalogResult, DuckLakeSnapshotId, TableId,
    runtime_change_feed::ChangeFeedPayload,
    runtime_foundationdb::{
        runtime_list_foundationdb_data_file_changes, runtime_list_foundationdb_table_deletions,
    },
    runtime_payload::payload_u64_value,
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot},
};

pub(crate) fn list_data_file_changes(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = change_feed_payload_values(payload, "ListDataFileChanges")?;
    runtime_list_foundationdb_data_file_changes(catalog, payload)
}

pub(crate) fn list_table_deletions(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = change_feed_payload_values(payload, "ListTableDeletions")?;
    runtime_list_foundationdb_table_deletions(catalog, payload)
}

fn change_feed_payload_values(payload: &[u8], operation: &str) -> CatalogResult<ChangeFeedPayload> {
    Ok(ChangeFeedPayload {
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            &format!("{operation} missing table_id"),
        )?),
        start_snapshot: ChangeFeedStartSnapshot::new(DuckLakeSnapshotId(payload_u64_value(
            payload,
            "start_snapshot_id",
            &format!("{operation} missing start_snapshot_id"),
        )?)),
        end_snapshot: ChangeFeedEndSnapshot::new(DuckLakeSnapshotId(payload_u64_value(
            payload,
            "end_snapshot_id",
            &format!("{operation} missing end_snapshot_id"),
        )?)),
    })
}
