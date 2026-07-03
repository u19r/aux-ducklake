use crate::{
    CatalogError, CatalogResult, DataFileChangeKind, DataFileRow, DeleteFileRow, KvBatch,
    SnapshotRow,
    conflict::{CommitAttemptRow, commit_attempt_key},
    ids::{CatalogOrderId, CommitAttemptId, DataFileId, TableId, incomplete_fdb_order},
    keys::{
        current_data_file_key, current_delete_file_key, current_table_name_key,
        current_table_row_key, data_file_begin_key, data_file_begin_prefix, data_file_end_key,
        data_file_key, delete_file_key, delete_file_timeline_key, delete_file_timeline_prefix,
        order_delete_file_change_key, order_delete_file_change_prefix, schema_object_key,
        schema_object_prefix, snapshot_data_file_change_key, snapshot_data_file_change_prefix,
        snapshot_key, snapshot_timestamp_key, table_data_file_change_key,
        table_data_file_change_prefix, table_delete_file_change_key,
        table_delete_file_change_prefix, table_object_key, table_object_prefix,
        table_visibility_key, table_visibility_prefix,
    },
    schema_rows::SchemaRow,
    table_rows::TableRow,
};

pub(crate) fn incomplete_order() -> CatalogOrderId {
    incomplete_fdb_order()
}

pub(crate) fn committed_order(versionstamp: &[u8]) -> CatalogResult<CatalogOrderId> {
    let bytes: [u8; CatalogOrderId::FDB_VERSIONSTAMP_LEN] =
        versionstamp.try_into().map_err(|_| {
            CatalogError::Decode(format!(
                "foundationdb versionstamp must be {} bytes, got {}",
                CatalogOrderId::FDB_VERSIONSTAMP_LEN,
                versionstamp.len()
            ))
        })?;
    Ok(CatalogOrderId::fdb_versionstamp(bytes, 0))
}

pub(crate) fn versionstamped_value(value: &[u8], order_offset: usize) -> CatalogResult<Vec<u8>> {
    let mut out = value.to_vec();
    append_versionstamp_offset(&mut out, order_offset)?;
    Ok(out)
}

pub(crate) fn append_versionstamp_offset(
    bytes: &mut Vec<u8>,
    order_offset: usize,
) -> CatalogResult<()> {
    let offset = u32::try_from(order_offset).map_err(|_| {
        CatalogError::InvalidMutation("versionstamp offset does not fit in u32".to_owned())
    })?;
    bytes.extend_from_slice(&offset.to_le_bytes());
    Ok(())
}

pub(crate) fn snapshot_key_order_offset(catalog: crate::CatalogId) -> usize {
    snapshot_key(catalog, incomplete_order())
        .len()
        .saturating_sub(CatalogOrderId::LEN)
}

pub(crate) fn snapshot_timestamp_key_order_offset(
    catalog: crate::CatalogId,
    created_at_micros: i64,
) -> usize {
    snapshot_timestamp_key(catalog, created_at_micros, incomplete_order())
        .len()
        .saturating_sub(CatalogOrderId::LEN)
}

pub(crate) fn data_file_begin_key_order_offset(
    catalog: crate::CatalogId,
    table_id: TableId,
) -> usize {
    data_file_begin_prefix(catalog, table_id).len()
}

pub(crate) fn table_data_file_change_key_order_offset(
    catalog: crate::CatalogId,
    table_id: TableId,
) -> usize {
    table_data_file_change_prefix(catalog, table_id).len()
}

pub(crate) fn snapshot_data_file_change_key_order_offset(catalog: crate::CatalogId) -> usize {
    snapshot_data_file_change_prefix(catalog).len()
}

pub(crate) fn delete_file_timeline_key_order_offset(
    catalog: crate::CatalogId,
    data_file_id: DataFileId,
) -> usize {
    delete_file_timeline_prefix(catalog, data_file_id).len()
}

pub(crate) fn table_delete_file_change_key_order_offset(
    catalog: crate::CatalogId,
    table_id: TableId,
) -> usize {
    table_delete_file_change_prefix(catalog, table_id).len()
}

pub(crate) fn order_delete_file_change_key_order_offset(catalog: crate::CatalogId) -> usize {
    order_delete_file_change_prefix(catalog).len()
}

pub(crate) fn snapshot_operation_key_order_offset(catalog: crate::CatalogId) -> usize {
    crate::snapshot_operations::snapshot_operation_order_offset(catalog)
}

pub(crate) fn data_file_end_key_order_offset(
    catalog: crate::CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
) -> usize {
    data_file_end_key(catalog, table_id, incomplete_order(), data_file_id)
        .len()
        .saturating_sub(CatalogOrderId::LEN + 1 + 8)
}

pub(crate) fn table_object_key_order_offset(catalog: crate::CatalogId, table_id: TableId) -> usize {
    table_object_prefix(catalog, table_id).len()
}

pub(crate) fn table_visibility_key_order_offset(catalog: crate::CatalogId) -> usize {
    table_visibility_prefix(catalog).len()
}

pub(crate) fn schema_object_key_order_offset(
    catalog: crate::CatalogId,
    schema_id: crate::SchemaId,
) -> usize {
    schema_object_prefix(catalog, schema_id).len()
}

pub(crate) fn estimate_versionstamped_schema_create_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    schemas: &[SchemaRow],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let schema_bytes = schemas
        .iter()
        .map(|schema| {
            schema_object_key(catalog, schema.schema_id, schema.validity.begin_order)
                .len()
                .saturating_add(schema.encode().len())
        })
        .sum::<usize>();
    snapshot_bytes.saturating_add(schema_bytes)
}

pub(crate) fn estimate_versionstamped_table_create_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    table: &TableRow,
) -> usize {
    let row_len = table.encode().len();
    snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8)
        .saturating_add(table_object_key(catalog, table.table_id, table.validity.begin_order).len())
        .saturating_add(row_len)
        .saturating_add(current_table_name_key(catalog, table.schema_id, &table.name).len())
        .saturating_add(current_table_row_key(catalog, table.table_id).len())
        .saturating_add(row_len)
        .saturating_add(
            table_visibility_key(catalog, table.validity.begin_order, table.table_id).len(),
        )
        .saturating_add(row_len)
}

pub(crate) fn estimate_versionstamped_append_commit_bytes(
    catalog: crate::CatalogId,
    attempt_id: CommitAttemptId,
    attempt: &CommitAttemptRow,
    row: &DataFileRow,
) -> usize {
    let row_len = row.encode().len();
    commit_attempt_key(catalog, attempt_id)
        .len()
        .saturating_add(attempt.encode().len())
        .saturating_add(data_file_key(catalog, row.data_file_id).len())
        .saturating_add(row_len)
        .saturating_add(current_data_file_key(catalog, row.table_id, row.data_file_id).len())
        .saturating_add(row_len)
        .saturating_add(
            data_file_begin_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                row.data_file_id,
            )
            .len(),
        )
        .saturating_add(row_len)
        .saturating_add(
            table_data_file_change_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                DataFileChangeKind::Added,
                row.data_file_id,
            )
            .len(),
        )
        .saturating_add(
            snapshot_data_file_change_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                DataFileChangeKind::Added,
                row.data_file_id,
            )
            .len(),
        )
}

pub(crate) fn estimate_versionstamped_append_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    rows: &[DataFileRow],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let row_bytes = rows
        .iter()
        .map(|row| {
            let row_len = row.encode().len();
            data_file_key(catalog, row.data_file_id)
                .len()
                .saturating_add(row_len)
                .saturating_add(
                    current_data_file_key(catalog, row.table_id, row.data_file_id).len(),
                )
                .saturating_add(row_len)
                .saturating_add(
                    data_file_begin_key(
                        catalog,
                        row.table_id,
                        row.validity.begin_order,
                        row.data_file_id,
                    )
                    .len(),
                )
                .saturating_add(row_len)
                .saturating_add(
                    table_data_file_change_key(
                        catalog,
                        row.table_id,
                        row.validity.begin_order,
                        DataFileChangeKind::Added,
                        row.data_file_id,
                    )
                    .len(),
                )
                .saturating_add(
                    snapshot_data_file_change_key(
                        catalog,
                        row.table_id,
                        row.validity.begin_order,
                        DataFileChangeKind::Added,
                        row.data_file_id,
                    )
                    .len(),
                )
        })
        .sum::<usize>();
    snapshot_bytes.saturating_add(row_bytes)
}

pub(crate) fn estimate_versionstamped_delete_bytes(
    catalog: crate::CatalogId,
    table_id: TableId,
    row: &DeleteFileRow,
) -> usize {
    let row_len = row.encode().len();
    delete_file_key(catalog, row.delete_file_id)
        .len()
        .saturating_add(row_len)
        .saturating_add(current_delete_file_key(catalog, row.data_file_id).len())
        .saturating_add(row_len)
        .saturating_add(
            delete_file_timeline_key(
                catalog,
                row.data_file_id,
                row.validity.begin_order,
                row.delete_file_id,
            )
            .len(),
        )
        .saturating_add(row_len)
        .saturating_add(
            table_delete_file_change_key(
                catalog,
                table_id,
                row.validity.begin_order,
                row.delete_file_id,
            )
            .len(),
        )
        .saturating_add(
            order_delete_file_change_key(
                catalog,
                row.validity.begin_order,
                table_id,
                row.delete_file_id,
            )
            .len(),
        )
}

pub(crate) fn estimate_versionstamped_expire_bytes(
    catalog: crate::CatalogId,
    row: &DataFileRow,
) -> usize {
    let row_len = row.encode().len();
    data_file_key(catalog, row.data_file_id)
        .len()
        .saturating_add(row_len)
        .saturating_add(current_data_file_key(catalog, row.table_id, row.data_file_id).len())
        .saturating_add(row_len)
        .saturating_add(
            data_file_begin_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                row.data_file_id,
            )
            .len(),
        )
        .saturating_add(row_len)
        .saturating_add(
            data_file_end_key(
                catalog,
                row.table_id,
                row.validity.end_order.unwrap_or_else(incomplete_order),
                row.data_file_id,
            )
            .len(),
        )
        .saturating_add(
            table_data_file_change_key(
                catalog,
                row.table_id,
                row.validity.end_order.unwrap_or_else(incomplete_order),
                DataFileChangeKind::Removed,
                row.data_file_id,
            )
            .len(),
        )
        .saturating_add(
            snapshot_data_file_change_key(
                catalog,
                row.table_id,
                row.validity.end_order.unwrap_or_else(incomplete_order),
                DataFileChangeKind::Removed,
                row.data_file_id,
            )
            .len(),
        )
}

pub(crate) fn strip_namespace(key_prefix: &[u8], key: &[u8]) -> CatalogResult<Vec<u8>> {
    let Some(tail) = key.strip_prefix(key_prefix) else {
        return Err(CatalogError::InvalidKey(
            "foundationdb key escaped catalog prefix".to_owned(),
        ));
    };
    Ok(tail.to_vec())
}

pub(crate) fn batch_exceeds_commit_limit(batch: &KvBatch, max_commit_bytes: usize) -> bool {
    batch.estimated_mutation_bytes() > max_commit_bytes
}

#[cfg(test)]
#[path = "fdb_versionstamp_tests.rs"]
mod tests;
