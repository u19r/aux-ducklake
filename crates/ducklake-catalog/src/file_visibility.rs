use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DataFileChangeKind, DataFileId,
    DataFileRow, DeleteFileId, DeleteFileRow, KvBatch, OrderedCatalogKv,
    conflict::write_data_file_change,
    file_partitions::delete_partition_lookups_for_data_file,
    keys::{
        current_data_file_key, current_delete_file_key, data_file_begin_key, data_file_end_key,
        data_file_key, delete_file_end_key, delete_file_key, delete_file_timeline_prefix,
    },
};

pub(crate) fn stage_expire_current_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
    end_order: CatalogOrderId,
) -> CatalogResult<DataFileRow> {
    let mut row = load_data_file(kv, catalog, data_file_id)?;
    if end_order <= row.validity.begin_order {
        return Err(CatalogError::InvalidMutation(format!(
            "data file {} cannot end at or before its begin order",
            data_file_id.0
        )));
    }
    if row.validity.end_order.is_some() {
        return Err(CatalogError::InvalidMutation(format!(
            "data file {} is already expired",
            data_file_id.0
        )));
    }

    row.validity.end_order = Some(end_order);
    batch.put(data_file_key(catalog, row.data_file_id), row.encode());
    batch.put(
        data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        ),
        row.encode(),
    );
    batch.delete(current_data_file_key(
        catalog,
        row.table_id,
        row.data_file_id,
    ));
    batch.put(
        data_file_end_key(catalog, row.table_id, end_order, row.data_file_id),
        row.data_file_id.0.to_be_bytes().to_vec(),
    );
    write_data_file_change(
        batch,
        catalog,
        row.table_id,
        end_order,
        DataFileChangeKind::Removed,
        row.data_file_id,
    );
    delete_partition_lookups_for_data_file(kv, batch, catalog, data_file_id)?;
    Ok(row)
}

pub(crate) fn stage_expire_current_delete_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file: &DataFileRow,
    delete_file_id: DeleteFileId,
    end_order: CatalogOrderId,
) -> CatalogResult<DeleteFileRow> {
    let mut row = load_delete_file(kv, catalog, delete_file_id)?;
    if row.data_file_id != data_file.data_file_id {
        return Err(CatalogError::InvalidMutation(format!(
            "delete file {} belongs to data file {}, not {}",
            delete_file_id.0, row.data_file_id.0, data_file.data_file_id.0
        )));
    }
    if end_order <= row.validity.begin_order {
        return Err(CatalogError::InvalidMutation(format!(
            "delete file {} cannot end at or before its begin order",
            delete_file_id.0
        )));
    }
    if row.validity.end_order.is_some() {
        return Err(CatalogError::InvalidMutation(format!(
            "delete file {} is already expired",
            delete_file_id.0
        )));
    }

    row.validity.end_order = Some(end_order);
    batch.put(delete_file_key(catalog, delete_file_id), row.encode());
    if let Some(key) = delete_file_timeline_key_for_row(kv, catalog, &row)? {
        batch.put(key, row.encode());
    }
    batch.delete(current_delete_file_key(catalog, data_file.data_file_id));
    batch.put(
        delete_file_end_key(catalog, data_file.table_id, end_order, delete_file_id),
        delete_file_id.0.to_be_bytes().to_vec(),
    );
    Ok(row)
}

pub(crate) fn current_delete_file_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Option<DeleteFileId>> {
    let Some(value) = kv.get(&current_delete_file_key(catalog, data_file_id))? else {
        return Ok(None);
    };
    decode_current_delete_file_id(&value).map(Some)
}

pub(crate) fn current_delete_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<BTreeMap<DataFileId, DeleteFileId>> {
    if data_file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = data_file_ids
        .iter()
        .map(|data_file_id| current_delete_file_key(catalog, *data_file_id))
        .collect::<Vec<_>>();
    let mut ids = BTreeMap::new();
    for (data_file_id, value) in data_file_ids.iter().copied().zip(kv.batch_get(&keys)?) {
        let Some(value) = value else {
            continue;
        };
        ids.insert(data_file_id, decode_current_delete_file_id(&value)?);
    }
    Ok(ids)
}

fn decode_current_delete_file_id(value: &[u8]) -> CatalogResult<DeleteFileId> {
    if let Ok(row) = DeleteFileRow::decode(value) {
        return Ok(row.delete_file_id);
    }
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            value.len()
        ))
    })?;
    Ok(DeleteFileId(u64::from_be_bytes(bytes)))
}

fn load_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

fn load_delete_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

fn delete_file_timeline_key_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<Option<Vec<u8>>> {
    for item in kv.scan_prefix(
        &delete_file_timeline_prefix(catalog, row.data_file_id),
        crate::RangeDirection::Forward,
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
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CatalogError::Decode("delete file id pointer is truncated".to_owned()))?;
    Ok(DeleteFileId(u64::from_be_bytes(bytes)))
}
