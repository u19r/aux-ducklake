use crate::{
    CatalogError, CatalogResult, DataFileChangeKind,
    ids::{CatalogId, CatalogOrderId, DataFileId, DeleteFileId, SchemaId, TableId},
    inline_change_feed::InlineRowChangeKind,
    keys::{KeyFamily, family_prefix},
};

#[must_use]
pub fn table_data_file_change_key(
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    kind: DataFileChangeKind,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = table_data_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn snapshot_data_file_change_key(
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    kind: DataFileChangeKind,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = snapshot_data_file_change_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn snapshot_data_file_change_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::SnapshotChanges)
}

#[must_use]
pub fn table_data_file_change_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::TableChanges);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn table_data_file_change_scan_start(
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_data_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&start_order.as_bytes());
    key
}

#[must_use]
pub fn table_data_file_change_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_data_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&end_order.as_bytes());
    key.push(0xff);
    key
}

#[must_use]
pub fn table_delete_file_change_key(
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    delete_file_id: DeleteFileId,
) -> Vec<u8> {
    let mut key = table_delete_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&delete_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn table_delete_file_change_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DeleteFileChange);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn table_delete_file_change_scan_start(
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_delete_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&start_order.as_bytes());
    key
}

#[must_use]
pub fn table_delete_file_change_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_delete_file_change_prefix(catalog, table_id);
    key.extend_from_slice(&end_order.as_bytes());
    key.push(0xff);
    key
}

#[must_use]
pub fn order_delete_file_change_key(
    catalog: CatalogId,
    order: CatalogOrderId,
    table_id: TableId,
    delete_file_id: DeleteFileId,
) -> Vec<u8> {
    let mut key = order_delete_file_change_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&delete_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn order_delete_file_change_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::DeleteFileChangeByOrder)
}

#[must_use]
pub fn order_delete_file_change_scan_start(
    catalog: CatalogId,
    start_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = order_delete_file_change_prefix(catalog);
    key.extend_from_slice(&start_order.as_bytes());
    key
}

#[must_use]
pub fn order_delete_file_change_scan_end(catalog: CatalogId, end_order: CatalogOrderId) -> Vec<u8> {
    let mut key = order_delete_file_change_prefix(catalog);
    key.extend_from_slice(&end_order.as_bytes());
    key.push(0xff);
    key
}

pub fn decode_order_delete_file_change_table_id(
    prefix: &[u8],
    key: &[u8],
) -> CatalogResult<TableId> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(CatalogError::InvalidKey(
            "order delete-file change key has wrong prefix".to_owned(),
        ));
    };
    let table_start = CatalogOrderId::LEN + 1;
    let table_end = table_start + 8;
    let expected_len = table_end + 1 + 8;
    if tail.len() != expected_len {
        return Err(CatalogError::InvalidKey(format!(
            "order delete-file change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    if tail[CatalogOrderId::LEN] != b'/' || tail[table_end] != b'/' {
        return Err(CatalogError::InvalidKey(
            "order delete-file change key separator is invalid".to_owned(),
        ));
    }
    Ok(TableId(u64::from_be_bytes(
        tail[table_start..table_end].try_into().map_err(|_| {
            CatalogError::InvalidKey("delete-file change table id is truncated".to_owned())
        })?,
    )))
}

#[must_use]
pub fn table_inline_row_change_key(
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
    schema_id: SchemaId,
    row_id: u64,
) -> Vec<u8> {
    let mut key = table_inline_row_change_prefix(catalog, table_id);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

#[must_use]
pub fn inline_table_change_key(
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
    table_id: TableId,
) -> Vec<u8> {
    let mut key = inline_table_change_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn inline_table_change_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::InlineTableChange)
}

#[must_use]
pub fn table_schema_kind_inline_row_change_key(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    kind: InlineRowChangeKind,
    order: CatalogOrderId,
    row_id: u64,
) -> Vec<u8> {
    let mut key = table_schema_kind_inline_row_change_prefix(catalog, table_id, schema_id, kind);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

#[must_use]
pub fn table_schema_kind_inline_row_change_prefix(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    kind: InlineRowChangeKind,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::InlineRowChangeBySchemaKind);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key
}

#[must_use]
pub fn table_schema_kind_inline_row_change_scan_start(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    kind: InlineRowChangeKind,
    start_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_schema_kind_inline_row_change_prefix(catalog, table_id, schema_id, kind);
    key.extend_from_slice(&start_order.as_bytes());
    key
}

#[must_use]
pub fn table_schema_kind_inline_row_change_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    kind: InlineRowChangeKind,
    end_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_schema_kind_inline_row_change_prefix(catalog, table_id, schema_id, kind);
    key.extend_from_slice(&end_order.as_bytes());
    key.push(0xff);
    key
}

#[must_use]
pub fn table_inline_row_change_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::InlineRowChange);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn table_inline_row_change_scan_start(
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_inline_row_change_prefix(catalog, table_id);
    key.extend_from_slice(&start_order.as_bytes());
    key
}

#[must_use]
pub fn table_inline_row_change_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = table_inline_row_change_prefix(catalog, table_id);
    key.extend_from_slice(&end_order.as_bytes());
    key.push(0xff);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_order_delete_file_change_table_id_returns_encoded_table() {
        let catalog = CatalogId(7);
        let table_id = TableId(42);
        let key = order_delete_file_change_key(
            catalog,
            CatalogOrderId::uuid_v7(11),
            table_id,
            DeleteFileId(99),
        );

        assert_eq!(
            decode_order_delete_file_change_table_id(
                &order_delete_file_change_prefix(catalog),
                &key
            )
            .unwrap(),
            table_id
        );
    }

    #[test]
    fn decode_order_delete_file_change_table_id_rejects_wrong_prefix() {
        let catalog = CatalogId(7);
        let key = order_delete_file_change_key(
            catalog,
            CatalogOrderId::uuid_v7(11),
            TableId(42),
            DeleteFileId(99),
        );

        assert!(decode_order_delete_file_change_table_id(b"wrong/", &key).is_err());
    }
}
