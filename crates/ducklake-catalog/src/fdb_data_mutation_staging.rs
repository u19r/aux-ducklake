use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogOrderId, CatalogResult, DataFileChangeKind, DataFileId, DataFileRow,
    DeleteFileId, DeleteFileRow, FdbOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow,
    InlineFileDeletionRow, RangeDirection, SnapshotRow, TableId,
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    fdb_runtime::map_fdb_error,
    fdb_versionstamp::{
        data_file_begin_key_order_offset, data_file_end_key_order_offset,
        delete_file_timeline_key_order_offset, incomplete_order,
        order_delete_file_change_key_order_offset, snapshot_data_file_change_key_order_offset,
        snapshot_key_order_offset, snapshot_operation_key_order_offset,
        snapshot_timestamp_key_order_offset, table_data_file_change_key_order_offset,
        table_delete_file_change_key_order_offset, versionstamped_value,
    },
    file_partitions::encode_partition_lookup_value,
    keys::{
        catalog_file_stats_version_key, current_data_file_key, current_delete_file_key,
        data_file_begin_key, data_file_end_key, data_file_key, delete_file_end_key,
        delete_file_key, delete_file_timeline_key, delete_file_timeline_prefix,
        file_column_stats_key, file_column_stats_lookup_key, file_partition_value_key,
        file_partition_value_prefix, inline_file_deletion_file_prefix, inline_file_deletion_key,
        order_delete_file_change_key, partition_value_lookup_key,
        scheduled_delete_file_cleanup_key, snapshot_data_file_change_key, snapshot_key,
        snapshot_timestamp_key, table_data_file_change_key, table_delete_file_change_key,
        table_file_stats_version_key,
    },
    kv::OrderedCatalogKv,
    maintenance::encode_scheduled_delete_cleanup_value,
    rows::{STORED_ORDER_LEN, current_timestamp_micros},
    snapshot_operations::{SnapshotOperationKind, snapshot_operation_key},
    store::stage_fdb_latest_snapshot_value,
};

const DELETE_FILE_END_ORDER_BYTES_OFFSET: usize =
    DeleteFileRow::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

pub(crate) fn stage_snapshot(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_key(catalog, snapshot.order),
            snapshot_key_order_offset(catalog),
        )?,
        &snapshot.encode(),
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
        )?,
        &snapshot.sequence.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    stage_fdb_latest_snapshot_value(kv, trx, catalog, snapshot)?;
    Ok(())
}

pub(crate) fn stage_snapshot_operation(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
    table_id: TableId,
) -> CatalogResult<()> {
    if order == incomplete_order() {
        trx.atomic_op(
            &kv.versionstamped_key(
                &snapshot_operation_key(catalog, order, kind, table_id),
                snapshot_operation_key_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        return Ok(());
    }
    trx.set(
        &kv.namespaced_key(&snapshot_operation_key(catalog, order, kind, table_id)),
        &[],
    );
    Ok(())
}

pub(crate) fn stage_data_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    stage_data_file_without_watermark(kv, trx, catalog, row)?;
    stage_fdb_max_file_id_watermark(kv, trx, catalog, row.data_file_id.0);
    Ok(())
}

pub(crate) fn stage_data_file_without_watermark(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    if row.max_partial_order.is_some() {
        stage_compacted_data_file(kv, trx, catalog, row);
        return stage_data_file_change(kv, trx, catalog, row.table_id, row.data_file_id);
    }
    trx.atomic_op(
        &kv.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        )),
        &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    trx.atomic_op(
        &kv.namespaced_key(&data_file_key(catalog, row.data_file_id)),
        &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &data_file_begin_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                row.data_file_id,
            ),
            data_file_begin_key_order_offset(catalog, row.table_id),
        )?,
        &row.encode(),
        MutationType::SetVersionstampedKey,
    );
    stage_data_file_change(kv, trx, catalog, row.table_id, row.data_file_id)
}

fn stage_compacted_data_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &DataFileRow,
) {
    trx.set(
        &kv.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        )),
        &row.encode(),
    );
    trx.set(
        &kv.namespaced_key(&data_file_key(catalog, row.data_file_id)),
        &row.encode(),
    );
    trx.set(
        &kv.namespaced_key(&data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        )),
        &row.encode(),
    );
}

pub(crate) fn stage_delete_file_without_watermark(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    data_file: &DataFileRow,
    row: &DeleteFileRow,
    delete_file_begin_order: crate::CatalogOrderId,
    timeline_order: crate::CatalogOrderId,
    table_change_order: crate::CatalogOrderId,
) -> CatalogResult<()> {
    let mut row = row.clone();
    if let Some(inherited_begin_order) =
        stage_close_current_delete_file(kv, trx, catalog, data_file.table_id, &row)?
    {
        row.validity.begin_order = inherited_begin_order;
        if row.max_partial_order.is_none() {
            row.max_partial_order = Some(timeline_order);
        }
    }
    let delete_file_begin_order = if row.validity.begin_order == incomplete_order() {
        delete_file_begin_order
    } else {
        row.validity.begin_order
    };
    if delete_file_begin_order == incomplete_order() {
        trx.atomic_op(
            &kv.namespaced_key(&current_delete_file_key(catalog, row.data_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    } else if row.max_partial_order == Some(incomplete_order()) {
        trx.atomic_op(
            &kv.namespaced_key(&current_delete_file_key(catalog, row.data_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::MAX_PARTIAL_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    } else {
        trx.set(
            &kv.namespaced_key(&current_delete_file_key(catalog, row.data_file_id)),
            &row.encode(),
        );
    }
    if delete_file_begin_order == incomplete_order() {
        trx.atomic_op(
            &kv.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    } else if row.max_partial_order == Some(incomplete_order()) {
        trx.atomic_op(
            &kv.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::MAX_PARTIAL_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    } else {
        trx.set(
            &kv.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
            &row.encode(),
        );
    }
    if timeline_order == incomplete_order() {
        trx.atomic_op(
            &kv.versionstamped_key(
                &delete_file_timeline_key(
                    catalog,
                    row.data_file_id,
                    timeline_order,
                    row.delete_file_id,
                ),
                delete_file_timeline_key_order_offset(catalog, row.data_file_id),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
    } else {
        trx.set(
            &kv.namespaced_key(&delete_file_timeline_key(
                catalog,
                row.data_file_id,
                timeline_order,
                row.delete_file_id,
            )),
            &row.encode(),
        );
    }
    stage_table_delete_file_change(
        kv,
        trx,
        catalog,
        data_file.table_id,
        table_change_order,
        row.delete_file_id,
    )?;
    Ok(())
}

fn stage_table_delete_file_change(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
    order: crate::CatalogOrderId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<()> {
    if order == incomplete_order() {
        trx.atomic_op(
            &kv.versionstamped_key(
                &table_delete_file_change_key(catalog, table_id, order, delete_file_id),
                table_delete_file_change_key_order_offset(catalog, table_id),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &kv.versionstamped_key(
                &order_delete_file_change_key(catalog, order, table_id, delete_file_id),
                order_delete_file_change_key_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        return Ok(());
    }
    trx.set(
        &kv.namespaced_key(&table_delete_file_change_key(
            catalog,
            table_id,
            order,
            delete_file_id,
        )),
        &[],
    );
    trx.set(
        &kv.namespaced_key(&order_delete_file_change_key(
            catalog,
            order,
            table_id,
            delete_file_id,
        )),
        &[],
    );
    Ok(())
}

pub(crate) fn stage_file_partition_value(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &FilePartitionValueRow,
    data_file: &DataFileRow,
) {
    trx.set(
        &kv.namespaced_key(&file_partition_value_key(
            catalog,
            row.data_file_id,
            row.partition_key_index,
        )),
        &row.encode(),
    );
    trx.set(
        &kv.namespaced_key(&partition_value_lookup_key(
            catalog,
            row.table_id,
            row.partition_key_index,
            &row.partition_value,
            row.data_file_id,
        )),
        &encode_partition_lookup_value(row, data_file),
    );
}

pub(crate) fn stage_file_column_stats(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &FileColumnStatsRow,
) {
    let encoded = row.encode();
    trx.set(
        &kv.namespaced_key(&file_column_stats_key(
            catalog,
            row.data_file_id,
            row.column_id,
        )),
        &encoded,
    );
    trx.set(
        &kv.namespaced_key(&file_column_stats_lookup_key(
            catalog,
            row.table_id,
            row.column_id,
            row.data_file_id,
        )),
        &encoded,
    );
}

pub(crate) fn stage_table_file_stats_version(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.namespaced_key(&table_file_stats_version_key(catalog, table_id)),
        &versionstamped_value(&incomplete_order().as_bytes(), 0)?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(crate) fn stage_catalog_file_stats_version(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.namespaced_key(&catalog_file_stats_version_key(catalog)),
        &versionstamped_value(&incomplete_order().as_bytes(), 0)?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(crate) fn stage_inline_file_deletion(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &InlineFileDeletionRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &inline_file_deletion_key(
                catalog,
                row.table_id,
                row.data_file_id,
                row.validity.begin_order,
                row.row_id,
            ),
            inline_file_deletion_key_order_offset(catalog, row.table_id, row.data_file_id),
        )?,
        &row.encode(),
        MutationType::SetVersionstampedKey,
    );
    Ok(())
}

pub(crate) fn stage_expired_data_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    let end_order = row.validity.end_order.unwrap_or_else(incomplete_order);
    trx.clear(&kv.namespaced_key(&current_data_file_key(
        catalog,
        row.table_id,
        row.data_file_id,
    )));
    clear_partition_values_for_data_file(kv, trx, catalog, row.data_file_id)?;
    trx.atomic_op(
        &kv.namespaced_key(&data_file_key(catalog, row.data_file_id)),
        &versionstamped_value(&row.encode(), DataFileRow::END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    trx.atomic_op(
        &kv.namespaced_key(&data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        )),
        &versionstamped_value(&row.encode(), DataFileRow::END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &data_file_end_key(catalog, row.table_id, end_order, row.data_file_id),
            data_file_end_key_order_offset(catalog, row.table_id, row.data_file_id),
        )?,
        &row.data_file_id.0.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    stage_removed_data_file_change(kv, trx, catalog, row.table_id, row.data_file_id, end_order)
}

fn clear_partition_values_for_data_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    for item in kv.scan_prefix(
        &file_partition_value_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = FilePartitionValueRow::decode(&item.value)?;
        trx.clear(&kv.namespaced_key(&partition_value_lookup_key(
            catalog,
            row.table_id,
            row.partition_key_index,
            &row.partition_value,
            row.data_file_id,
        )));
    }
    Ok(())
}

pub(crate) fn stage_expired_delete_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
    row: &DeleteFileRow,
) -> CatalogResult<()> {
    let end_order = row.validity.end_order.unwrap_or_else(incomplete_order);
    trx.clear(&kv.namespaced_key(&current_delete_file_key(catalog, row.data_file_id)));
    trx.atomic_op(
        &kv.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
        &versionstamped_value(&row.encode(), DELETE_FILE_END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    if let Some(key) = delete_file_timeline_key_for_row(kv, catalog, row)? {
        trx.atomic_op(
            &kv.namespaced_key(&key),
            &versionstamped_value(&row.encode(), DELETE_FILE_END_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    }
    trx.atomic_op(
        &kv.versionstamped_key(
            &delete_file_end_key(catalog, table_id, end_order, row.delete_file_id),
            delete_file_end_key_order_offset(catalog, table_id),
        )?,
        &row.delete_file_id.0.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    Ok(())
}

fn stage_data_file_change(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    let order = incomplete_order();
    trx.atomic_op(
        &kv.versionstamped_key(
            &table_data_file_change_key(
                catalog,
                table_id,
                order,
                DataFileChangeKind::Added,
                data_file_id,
            ),
            table_data_file_change_key_order_offset(catalog, table_id),
        )?,
        &[],
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_data_file_change_key(
                catalog,
                table_id,
                order,
                DataFileChangeKind::Added,
                data_file_id,
            ),
            snapshot_data_file_change_key_order_offset(catalog),
        )?,
        &[],
        MutationType::SetVersionstampedKey,
    );
    Ok(())
}

fn stage_removed_data_file_change(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    order: CatalogOrderId,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &table_data_file_change_key(
                catalog,
                table_id,
                order,
                DataFileChangeKind::Removed,
                data_file_id,
            ),
            table_data_file_change_key_order_offset(catalog, table_id),
        )?,
        &[],
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_data_file_change_key(
                catalog,
                table_id,
                order,
                DataFileChangeKind::Removed,
                data_file_id,
            ),
            snapshot_data_file_change_key_order_offset(catalog),
        )?,
        &[],
        MutationType::SetVersionstampedKey,
    );
    Ok(())
}

fn stage_close_current_delete_file(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    table_id: TableId,
    next_delete_file: &DeleteFileRow,
) -> CatalogResult<Option<crate::CatalogOrderId>> {
    let Some(pointer) = block_on(trx.get(
        &kv.namespaced_key(&current_delete_file_key(
            catalog,
            next_delete_file.data_file_id,
        )),
        false,
    ))
    .map_err(map_fdb_error)?
    else {
        return Ok(None);
    };
    let mut row = match DeleteFileRow::decode(pointer.deref()) {
        Ok(row) => row,
        Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(pointer.deref())?)?,
    };
    if row.delete_file_id == next_delete_file.delete_file_id {
        return Err(CatalogError::InvalidMutation(format!(
            "delete file {} is already current",
            row.delete_file_id.0
        )));
    }
    let inherited_begin_order = row.validity.begin_order;
    row.validity.end_order = Some(incomplete_order());
    trx.atomic_op(
        &kv.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
        &versionstamped_value(&row.encode(), DELETE_FILE_END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    if let Some(key) = delete_file_timeline_key_for_row(kv, catalog, &row)? {
        trx.atomic_op(
            &kv.namespaced_key(&key),
            &versionstamped_value(&row.encode(), DELETE_FILE_END_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    }
    trx.atomic_op(
        &kv.versionstamped_key(
            &delete_file_end_key(catalog, table_id, incomplete_order(), row.delete_file_id),
            delete_file_end_key_order_offset(catalog, table_id),
        )?,
        &row.delete_file_id.0.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    trx.set(
        &kv.namespaced_key(&scheduled_delete_file_cleanup_key(
            catalog,
            row.delete_file_id,
        )),
        &encode_scheduled_delete_cleanup_value(table_id, current_timestamp_micros()),
    );
    Ok(Some(inherited_begin_order))
}

fn delete_file_end_key_order_offset(catalog: crate::CatalogId, table_id: TableId) -> usize {
    delete_file_end_key(catalog, table_id, incomplete_order(), DeleteFileId(0))
        .len()
        .saturating_sub(crate::CatalogOrderId::LEN + 1 + 8)
}

fn inline_file_deletion_key_order_offset(
    catalog: crate::CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
) -> usize {
    inline_file_deletion_file_prefix(catalog, table_id, data_file_id).len()
}

fn load_delete_file(
    kv: &FdbOrderedCatalogKv,
    catalog: crate::CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

fn delete_file_timeline_key_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<Option<Vec<u8>>> {
    for item in kv.scan_prefix(
        &delete_file_timeline_prefix(catalog, row.data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let delete_file_id = match DeleteFileRow::decode(&item.value) {
            Ok(existing) => existing.delete_file_id,
            Err(_) => decode_delete_file_id(&item.value)?,
        };
        if delete_file_id == row.delete_file_id {
            return Ok(Some(item.key));
        }
    }
    Ok(None)
}

fn decode_delete_file_id(bytes: &[u8]) -> CatalogResult<DeleteFileId> {
    if bytes.len() != 8 {
        return Err(CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DeleteFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| CatalogError::Decode("delete file id pointer is truncated".to_owned()),
    )?)))
}
