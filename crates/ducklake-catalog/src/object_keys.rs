use crate::{
    ids::{CatalogId, CatalogOrderId, MacroId, SchemaId, TableId},
    keys::{KeyFamily, family_prefix},
};

#[must_use]
pub fn conflict_fence_key(catalog: CatalogId, scope: &[u8]) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::ConflictFence);
    key.extend_from_slice(scope);
    key
}

#[must_use]
pub fn table_object_key(catalog: CatalogId, table_id: TableId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = table_object_prefix(catalog, table_id);
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn table_object_scan_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b't');
    key.push(b'/');
    key
}

#[must_use]
pub fn table_object_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = table_object_scan_prefix(catalog);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn current_table_row_key(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = current_table_row_prefix(catalog);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn current_table_row_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b'T');
    key.push(b'/');
    key
}

#[must_use]
pub fn current_table_name_key(catalog: CatalogId, schema_id: SchemaId, name: &str) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b'n');
    key.push(b'/');
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(name.to_ascii_lowercase().as_bytes());
    key
}

#[must_use]
pub fn table_visibility_key(
    catalog: CatalogId,
    begin_order: CatalogOrderId,
    table_id: TableId,
) -> Vec<u8> {
    let mut key = table_visibility_prefix(catalog);
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn table_visibility_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b'V');
    key.push(b'/');
    key
}

#[must_use]
pub fn table_visibility_scan_end(catalog: CatalogId, snapshot_order: CatalogOrderId) -> Vec<u8> {
    let mut key = table_visibility_prefix(catalog);
    key.extend_from_slice(&snapshot_order.as_bytes());
    key.push(0xff);
    key
}

#[must_use]
pub fn schema_object_key(
    catalog: CatalogId,
    schema_id: SchemaId,
    order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = schema_object_prefix(catalog, schema_id);
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn schema_object_scan_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b's');
    key.push(b'/');
    key
}

#[must_use]
pub fn schema_object_prefix(catalog: CatalogId, schema_id: SchemaId) -> Vec<u8> {
    let mut key = schema_object_scan_prefix(catalog);
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn view_object_key(catalog: CatalogId, view_id: TableId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = view_object_prefix(catalog, view_id);
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn view_object_scan_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b'v');
    key.push(b'/');
    key
}

#[must_use]
pub fn view_object_prefix(catalog: CatalogId, view_id: TableId) -> Vec<u8> {
    let mut key = view_object_scan_prefix(catalog);
    key.extend_from_slice(&view_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn macro_object_key(catalog: CatalogId, macro_id: MacroId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = macro_object_prefix(catalog, macro_id);
    key.extend_from_slice(&order.as_bytes());
    key
}

#[must_use]
pub fn macro_object_scan_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::Object);
    key.push(b'm');
    key.push(b'/');
    key
}

#[must_use]
pub fn macro_object_prefix(catalog: CatalogId, macro_id: MacroId) -> Vec<u8> {
    let mut key = macro_object_scan_prefix(catalog);
    key.extend_from_slice(&macro_id.0.to_be_bytes());
    key.push(b'/');
    key
}
