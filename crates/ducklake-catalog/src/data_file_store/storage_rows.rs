use crate::data_file_store::*;
use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow, KvBatch,
    OrderedCatalogKv, RangeDirection, TableId,
    ids::{CatalogOrderId, incomplete_fdb_order},
    keys::{
        current_delete_file_key, data_file_begin_prefix, data_file_key, delete_file_end_key,
        delete_file_key, delete_file_timeline_order_from_key, delete_file_timeline_prefix,
        delete_file_timeline_scan_end,
    },
};
pub(super) fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "data file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DataFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("data file id pointer is truncated".to_owned()),
    )?)))
}

pub(super) fn decode_delete_file_id(bytes: &[u8]) -> CatalogResult<DeleteFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DeleteFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("delete file id pointer is truncated".to_owned()),
    )?)))
}

pub(super) fn data_file_from_current_index_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(row) => Ok(row),
        Err(_) => load_data_file(kv, catalog, decode_data_file_id(value)?),
    }
}

pub(super) fn data_file_from_begin_index_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(mut row) => {
            row.validity.begin_order = data_file_begin_order_from_key(
                catalog,
                row.table_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            Ok(row)
        }
        Err(_) => load_data_file(kv, catalog, decode_data_file_id(value)?),
    }
}

pub(super) fn data_file_begin_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    kind: crate::CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = data_file_begin_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(crate::CatalogError::InvalidKey(
            "data file begin key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(crate::CatalogError::InvalidKey(format!(
            "data file begin key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("data file begin order is truncated".to_owned())
        })?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

pub(super) fn load_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

pub(super) fn load_delete_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

pub(super) fn stage_close_current_delete_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file: &DataFileRow,
    next_delete_file: &DeleteFileRow,
    close_order: CatalogOrderId,
) -> CatalogResult<()> {
    let Some(value) = kv.get(&current_delete_file_key(
        catalog,
        next_delete_file.data_file_id,
    ))?
    else {
        return Ok(());
    };
    let mut row = current_delete_file_from_index_value(kv, catalog, &value)?;
    if row.delete_file_id == next_delete_file.delete_file_id {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} is already current",
            row.delete_file_id.0
        )));
    }
    if close_order <= row.validity.begin_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} cannot close newer delete file {}",
            next_delete_file.delete_file_id.0, row.delete_file_id.0
        )));
    }
    if next_delete_file.validity.begin_order < row.validity.begin_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "delete file {} cannot replace newer delete file {}",
            next_delete_file.delete_file_id.0, row.delete_file_id.0
        )));
    }
    row.validity.end_order = Some(close_order);
    batch.put(delete_file_key(catalog, row.delete_file_id), row.encode());
    if let Some(key) = delete_file_timeline_key_for_row(kv, catalog, &row)? {
        batch.put(key, row.encode());
    }
    batch.put(
        delete_file_end_key(catalog, data_file.table_id, close_order, row.delete_file_id),
        row.delete_file_id.0.to_be_bytes().to_vec(),
    );
    Ok(())
}

pub(super) fn inherit_current_delete_begin_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    next_delete_file: &mut DeleteFileRow,
) -> CatalogResult<()> {
    let Some(pointer) = kv.get(&current_delete_file_key(
        catalog,
        next_delete_file.data_file_id,
    ))?
    else {
        return Ok(());
    };
    let current = current_delete_file_from_index_value(kv, catalog, &pointer)?;
    if current.delete_file_id == next_delete_file.delete_file_id {
        return Ok(());
    }
    next_delete_file.validity.begin_order = current.validity.begin_order;
    Ok(())
}

pub(super) fn load_delete_file_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<DeleteFileRow>> {
    let started = RuntimeMetricStage::start();
    for item in kv.scan_range(
        &delete_file_timeline_prefix(catalog, data_file_id),
        &delete_file_timeline_scan_end(catalog, data_file_id, snapshot_order),
        RangeDirection::Reverse,
        1,
    )? {
        let row =
            delete_file_from_timeline_item(kv, catalog, data_file_id, &item.key, &item.value)?;
        let timeline_order = delete_file_timeline_order_from_key(
            catalog,
            data_file_id,
            &item.key,
            row.validity.begin_order.kind(),
        )?;
        if delete_file_can_answer_snapshot(&row, timeline_order, snapshot_order) {
            record_runtime_method_stage("method.data_file_store.load_delete_file_at", started);
            return Ok(Some(row));
        }
    }
    record_runtime_method_stage("method.data_file_store.load_delete_file_at", started);
    Ok(None)
}

pub(crate) fn delete_file_can_answer_snapshot(
    row: &DeleteFileRow,
    timeline_order: CatalogOrderId,
    snapshot_order: CatalogOrderId,
) -> bool {
    if !row.validity.is_visible_at(snapshot_order) {
        return false;
    }
    timeline_order <= snapshot_order
}

pub(crate) fn current_delete_file_from_index_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<DeleteFileRow> {
    match DeleteFileRow::decode(value) {
        Ok(row) => Ok(row),
        Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(value)?),
    }
}

pub(super) fn delete_file_from_timeline_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DeleteFileRow> {
    match DeleteFileRow::decode(value) {
        Ok(mut row) => {
            let timeline_order = delete_file_timeline_order_from_key(
                catalog,
                data_file_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            if row.validity.begin_order == incomplete_fdb_order() {
                row.validity.begin_order = timeline_order;
            }
            if row.max_partial_order == Some(incomplete_fdb_order()) {
                row.max_partial_order = Some(timeline_order);
            }
            Ok(row)
        }
        Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(value)?),
    }
}

pub(super) fn delete_file_timeline_key_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<Option<Vec<u8>>> {
    let started = RuntimeMetricStage::start();
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
            record_runtime_method_stage(
                "method.data_file_store.delete_file_timeline_key_for_row",
                started,
            );
            return Ok(Some(item.key));
        }
    }
    record_runtime_method_stage(
        "method.data_file_store.delete_file_timeline_key_for_row",
        started,
    );
    Ok(None)
}
