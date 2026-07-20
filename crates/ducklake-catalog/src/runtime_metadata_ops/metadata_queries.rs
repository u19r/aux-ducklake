use std::collections::BTreeSet;

use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv, RangeDirection, TableId,
    keys::{KeyFamily, data_file_key, delete_file_key, family_prefix},
};

use crate::runtime_metadata_ops::*;

pub(super) fn list_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DeleteFileRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::DeleteFile),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| DeleteFileRow::decode(&item.value))
    .collect()
}

pub(super) fn list_current_data_files_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    let data_file_ids = unique_data_file_ids(data_file_ids);
    let mut rows = kv
        .batch_get(
            &data_file_ids
                .iter()
                .map(|data_file_id| data_file_key(catalog, *data_file_id))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .flatten()
        .map(|value| DataFileRow::decode(&value))
        .filter(|row| {
            row.as_ref()
                .map(|row| row.validity.end_order.is_none())
                .unwrap_or(true)
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.data_file_id.0);
    Ok(rows)
}

pub(super) fn list_delete_files_for_delete_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_ids: &[DeleteFileId],
) -> CatalogResult<Vec<DeleteFileRow>> {
    let delete_file_ids = unique_delete_file_ids(delete_file_ids);
    let mut rows = kv
        .batch_get(
            &delete_file_ids
                .iter()
                .map(|delete_file_id| delete_file_key(catalog, *delete_file_id))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .flatten()
        .map(|value| DeleteFileRow::decode(&value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.delete_file_id.0);
    Ok(rows)
}

pub(super) fn list_file_column_stats_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if data_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    let requested_ids = data_file_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut rows = current_metadata_file_column_stats(kv, catalog)?
        .into_iter()
        .filter(|row| requested_ids.contains(&row.data_file_id))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    Ok(rows)
}

pub(super) fn unique_data_file_ids(data_file_ids: &[DataFileId]) -> Vec<DataFileId> {
    data_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn unique_delete_file_ids(delete_file_ids: &[DeleteFileId]) -> Vec<DeleteFileId> {
    delete_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn data_file_ids_payload_values(payload: &[u8]) -> CatalogResult<Vec<DataFileId>> {
    parse_id_payload(payload, "data_file_id").map(|ids| ids.into_iter().map(DataFileId).collect())
}

pub(super) fn delete_file_ids_payload_values(payload: &[u8]) -> CatalogResult<Vec<DeleteFileId>> {
    parse_id_payload(payload, "delete_file_id")
        .map(|ids| ids.into_iter().map(DeleteFileId).collect())
}

pub(super) fn bounded_append_mirror_payload_values(
    payload: &[u8],
) -> CatalogResult<(Vec<DataFileId>, Vec<FilePartitionValueRow>)> {
    let input = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("invalid bounded append mirror payload: {error}"))
    })?;
    let mut data_file_ids = Vec::new();
    let mut partition_rows = Vec::new();
    for line in input.lines().filter(|line| !line.is_empty()) {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["data_file_id", value] => {
                data_file_ids.push(DataFileId(parse_u64(value, "data file id")?));
            }
            [
                "file_partition",
                data_file_id,
                table_id,
                partition_key_index,
                partition_value,
            ] => {
                partition_rows.push(FilePartitionValueRow::new(
                    DataFileId(parse_u64(data_file_id, "partition value data file id")?),
                    TableId(parse_u64(table_id, "partition value table id")?),
                    crate::PartitionKeyIndex(
                        parse_u64(partition_key_index, "partition key index")?
                            .try_into()
                            .map_err(|_| {
                                crate::CatalogError::Decode(format!(
                                    "partition key index is out of range: {partition_key_index}"
                                ))
                            })?,
                    ),
                    *partition_value,
                ));
            }
            _ => {}
        }
    }
    data_file_ids.sort_unstable();
    data_file_ids.dedup();
    Ok((data_file_ids, partition_rows))
}

pub(super) fn data_file_ids_sql(ids: &[DataFileId]) -> String {
    let mut values = ids.iter().map(|id| id.0).collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    id_sql(values)
}

pub(super) fn delete_file_ids_sql(ids: &[DeleteFileId]) -> String {
    let mut values = ids.iter().map(|id| id.0).collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    id_sql(values)
}

pub(super) fn id_sql(values: Vec<u64>) -> String {
    if values.is_empty() {
        return "NULL".to_owned();
    }
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn parse_id_payload(payload: &[u8], row_label: &str) -> CatalogResult<Vec<u64>> {
    let input = std::str::from_utf8(payload)
        .map_err(|error| crate::CatalogError::Decode(format!("invalid id payload: {error}")))?;
    let mut ids = Vec::new();
    for line in input.lines().filter(|line| !line.is_empty()) {
        let Some((label, value)) = line.split_once('\t') else {
            return Err(crate::CatalogError::Decode(format!(
                "invalid id payload row: {line}"
            )));
        };
        if label != row_label {
            return Err(crate::CatalogError::Decode(format!(
                "expected {row_label} row, found {label}"
            )));
        }
        ids.push(value.parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid {row_label} value {value}: {error}"))
        })?);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

pub(super) fn snapshot_sequence_for_order(
    snapshots: &[crate::SnapshotRow],
    order: crate::CatalogOrderId,
) -> Option<u64> {
    snapshots
        .iter()
        .find(|snapshot| snapshot.order == order)
        .map(|snapshot| snapshot.sequence.0)
}

pub(super) fn snapshot_sequence_for_optional_end_order(
    snapshots: &[crate::SnapshotRow],
    order: Option<crate::CatalogOrderId>,
) -> String {
    match order {
        Some(order) => snapshot_sequence_for_order(snapshots, order)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "0".to_owned()),
        None => String::new(),
    }
}
