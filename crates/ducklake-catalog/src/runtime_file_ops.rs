use crate::{
    CatalogId, CatalogResult, DataFileChangeKind, DataFileId, OrderedCatalogKv, PartitionKeyIndex,
    TableId,
    runtime_file_listing::{
        CurrentPartitionFilesBatchPayload, CurrentPartitionFilesPayload,
        CurrentPartitionPruneFilesPayload, ListDataFilesAtPayload, PartitionFilesAtBatchPayload,
        PartitionFilesAtPayload, PartitionPruneComparison, PartitionPruneFilesAtPayload,
    },
    runtime_foundationdb::{
        runtime_list_foundationdb_current_partition_files,
        runtime_list_foundationdb_current_partition_files_batch,
        runtime_list_foundationdb_current_partition_prune_files,
        runtime_list_foundationdb_data_files_at, runtime_list_foundationdb_partition_files_at,
        runtime_list_foundationdb_partition_files_at_batch,
        runtime_list_foundationdb_partition_prune_files_at,
        runtime_list_foundationdb_removed_data_files_after,
    },
    runtime_payload::{
        payload_string_value, payload_string_values, payload_u32_value, payload_u64_value,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshots::snapshot_data_file_changes_at,
    store::list_snapshots,
};
use std::collections::BTreeSet;

pub(crate) fn list_data_files_at(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = list_data_files_at_payload_values(payload)?;
    runtime_list_foundationdb_data_files_at(catalog, payload)
}

pub(crate) fn list_current_partition_files(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = current_partition_files_payload_values(payload)?;
    runtime_list_foundationdb_current_partition_files(catalog, payload)
}

pub(crate) fn list_current_partition_files_batch(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = current_partition_files_batch_payload_values(payload)?;
    runtime_list_foundationdb_current_partition_files_batch(catalog, payload)
}

pub(crate) fn list_current_partition_prune_files(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = current_partition_prune_files_payload_values(payload)?;
    runtime_list_foundationdb_current_partition_prune_files(catalog, payload)
}

pub(crate) fn list_partition_files_at(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = partition_files_at_payload_values(payload)?;
    runtime_list_foundationdb_partition_files_at(catalog, payload)
}

pub(crate) fn list_partition_files_at_batch(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = partition_files_at_batch_payload_values(payload)?;
    runtime_list_foundationdb_partition_files_at_batch(catalog, payload)
}

pub(crate) fn list_partition_prune_files_at(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = partition_prune_files_at_payload_values(payload)?;
    runtime_list_foundationdb_partition_prune_files_at(catalog, payload)
}

pub(crate) fn list_removed_data_files_after(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let snapshot_id = payload_u64_value(
        payload,
        "snapshot_id",
        "ListRemovedDataFilesAfter missing snapshot_id",
    )?;
    runtime_list_foundationdb_removed_data_files_after(catalog, snapshot_id)
}

pub(crate) fn removed_data_files_after_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    let mut data_file_ids = BTreeSet::new();
    for snapshot in list_snapshots(kv, catalog)? {
        if snapshot.sequence.0 <= snapshot_id {
            continue;
        }
        for change in snapshot_data_file_changes_at(kv, catalog, snapshot.order)? {
            if change.kind == DataFileChangeKind::Removed {
                data_file_ids.insert(change.data_file_id);
            }
        }
    }
    let mut out = format!("removed_data_file_count={}\n", data_file_ids.len());
    for DataFileId(data_file_id) in data_file_ids {
        out.push_str(&format!("data_file_id\t{data_file_id}\n"));
    }
    Ok(out.into_bytes())
}

fn current_partition_files_payload_values(
    payload: &[u8],
) -> CatalogResult<CurrentPartitionFilesPayload> {
    Ok(CurrentPartitionFilesPayload {
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListCurrentDataFilesForPartitionScan missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListCurrentDataFilesForPartitionScan missing partition_key_index",
        )?),
        partition_value: payload_string_value(
            payload,
            "partition_value",
            "ListCurrentDataFilesForPartitionScan missing partition_value",
        )?,
    })
}

fn current_partition_files_batch_payload_values(
    payload: &[u8],
) -> CatalogResult<CurrentPartitionFilesBatchPayload> {
    let partition_values = payload_string_values(payload, "partition_value")?;
    if partition_values.is_empty() {
        return Err(crate::CatalogError::Decode(
            "ListCurrentDataFilesForPartitionScans missing partition_value".to_owned(),
        ));
    }
    Ok(CurrentPartitionFilesBatchPayload {
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListCurrentDataFilesForPartitionScans missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListCurrentDataFilesForPartitionScans missing partition_key_index",
        )?),
        partition_values,
    })
}

fn current_partition_prune_files_payload_values(
    payload: &[u8],
) -> CatalogResult<CurrentPartitionPruneFilesPayload> {
    Ok(CurrentPartitionPruneFilesPayload {
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListCurrentDataFilesForPartitionPrune missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListCurrentDataFilesForPartitionPrune missing partition_key_index",
        )?),
        column_type: payload_string_value(
            payload,
            "partition_column_type",
            "ListCurrentDataFilesForPartitionPrune missing partition_column_type",
        )?,
        comparison: partition_prune_comparison(payload_string_value(
            payload,
            "comparison",
            "ListCurrentDataFilesForPartitionPrune missing comparison",
        )?)?,
        partition_value: payload_string_value(
            payload,
            "partition_value",
            "ListCurrentDataFilesForPartitionPrune missing partition_value",
        )?,
    })
}

fn list_data_files_at_payload_values(payload: &[u8]) -> CatalogResult<ListDataFilesAtPayload> {
    Ok(ListDataFilesAtPayload {
        snapshot_id: payload_u64_value(
            payload,
            "snapshot_id",
            "ListDataFilesAt missing snapshot_id",
        )?,
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListDataFilesAt missing table_id",
        )?),
    })
}

fn partition_files_at_payload_values(payload: &[u8]) -> CatalogResult<PartitionFilesAtPayload> {
    Ok(PartitionFilesAtPayload {
        snapshot_id: payload_u64_value(
            payload,
            "snapshot_id",
            "ListDataFilesForPartitionScanAt missing snapshot_id",
        )?,
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListDataFilesForPartitionScanAt missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListDataFilesForPartitionScanAt missing partition_key_index",
        )?),
        partition_value: payload_string_value(
            payload,
            "partition_value",
            "ListDataFilesForPartitionScanAt missing partition_value",
        )?,
    })
}

fn partition_files_at_batch_payload_values(
    payload: &[u8],
) -> CatalogResult<PartitionFilesAtBatchPayload> {
    let partition_values = payload_string_values(payload, "partition_value")?;
    if partition_values.is_empty() {
        return Err(crate::CatalogError::Decode(
            "ListDataFilesForPartitionScansAt missing partition_value".to_owned(),
        ));
    }
    Ok(PartitionFilesAtBatchPayload {
        snapshot_id: payload_u64_value(
            payload,
            "snapshot_id",
            "ListDataFilesForPartitionScansAt missing snapshot_id",
        )?,
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListDataFilesForPartitionScansAt missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListDataFilesForPartitionScansAt missing partition_key_index",
        )?),
        partition_values,
    })
}

fn partition_prune_files_at_payload_values(
    payload: &[u8],
) -> CatalogResult<PartitionPruneFilesAtPayload> {
    Ok(PartitionPruneFilesAtPayload {
        snapshot_id: payload_u64_value(
            payload,
            "snapshot_id",
            "ListDataFilesForPartitionPruneAt missing snapshot_id",
        )?,
        table_id: TableId(payload_u64_value(
            payload,
            "table_id",
            "ListDataFilesForPartitionPruneAt missing table_id",
        )?),
        partition_key_index: PartitionKeyIndex(payload_u32_value(
            payload,
            "partition_key_index",
            "ListDataFilesForPartitionPruneAt missing partition_key_index",
        )?),
        column_type: payload_string_value(
            payload,
            "partition_column_type",
            "ListDataFilesForPartitionPruneAt missing partition_column_type",
        )?,
        comparison: partition_prune_comparison(payload_string_value(
            payload,
            "comparison",
            "ListDataFilesForPartitionPruneAt missing comparison",
        )?)?,
        partition_value: payload_string_value(
            payload,
            "partition_value",
            "ListDataFilesForPartitionPruneAt missing partition_value",
        )?,
    })
}

fn partition_prune_comparison(value: String) -> CatalogResult<PartitionPruneComparison> {
    match value.as_str() {
        "equal" => Ok(PartitionPruneComparison::Equal),
        "greater_than" => Ok(PartitionPruneComparison::GreaterThan),
        "greater_than_or_equal" => Ok(PartitionPruneComparison::GreaterThanOrEqual),
        "less_than" => Ok(PartitionPruneComparison::LessThan),
        "less_than_or_equal" => Ok(PartitionPruneComparison::LessThanOrEqual),
        _ => Err(crate::CatalogError::Decode(format!(
            "invalid partition prune comparison: {value}"
        ))),
    }
}
