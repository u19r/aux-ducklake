use crate::{
    CatalogId, CatalogResult, DataFileChange, DataFileChangeKind, DataFileId, OrderedCatalogKv,
    RangeDirection, TableId,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{
        table_data_file_change_prefix, table_data_file_change_scan_end,
        table_data_file_change_scan_start,
    },
};

pub fn list_data_file_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileChange>> {
    if end_order < start_order {
        return Err(crate::CatalogError::InvalidMutation(
            "change-feed end order cannot precede start order".to_owned(),
        ));
    }
    let prefix = table_data_file_change_prefix(catalog, table_id);
    let order_kind = if start_order.kind() == end_order.kind() {
        end_order.kind()
    } else {
        CatalogOrderKind::UuidV7
    };
    let mut changes = Vec::new();
    for item in kv.scan_range(
        &table_data_file_change_scan_start(catalog, table_id, start_order),
        &table_data_file_change_scan_end(catalog, table_id, end_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        changes.push(decode_data_file_change_key(
            table_id, &prefix, &item.key, order_kind,
        )?);
    }
    Ok(changes)
}

fn decode_data_file_change_key(
    table_id: TableId,
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<DataFileChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(crate::CatalogError::InvalidKey(
            "table data-file change key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 1 + 1 + 8;
    if tail.len() != minimum_len {
        return Err(crate::CatalogError::InvalidKey(format!(
            "table data-file change key tail must be {minimum_len} bytes, got {}",
            tail.len()
        )));
    }
    let order_end = CatalogOrderId::LEN;
    if tail[order_end] != b'/' || tail[order_end + 2] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "table data-file change key separators are invalid".to_owned(),
        ));
    }
    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end]
            .try_into()
            .map_err(|_| crate::CatalogError::InvalidKey("change order is truncated".to_owned()))?,
    );
    let kind = DataFileChangeKind::from_code(tail[order_end + 1])?;
    let data_file_id_start = order_end + 3;
    let data_file_id = DataFileId(u64::from_be_bytes(
        tail[data_file_id_start..]
            .try_into()
            .map_err(|_| crate::CatalogError::InvalidKey("data file id is truncated".to_owned()))?,
    ));
    Ok(DataFileChange::new(table_id, order, kind, data_file_id))
}
