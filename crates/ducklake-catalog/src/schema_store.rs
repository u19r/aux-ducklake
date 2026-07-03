#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, SchemaId, SchemaRow, ValidityWindow,
    conflict_watermarks::stage_max_catalog_id_watermark,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{prefix_end, schema_object_key, schema_object_prefix, schema_object_scan_prefix},
    schema_version_state::stage_next_schema_version,
    store::{latest_snapshot, snapshot_row_for_next_sequence, stage_snapshot},
};

#[cfg(not(test))]
static SCHEMA_ROWS_CACHE: OnceLock<BoundedCache<(CatalogId, CatalogOrderId), Vec<SchemaRow>>> =
    OnceLock::new();

pub fn commit_create_schema_rows(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut schemas: Vec<SchemaRow>,
) -> CatalogResult<Vec<SchemaRow>> {
    if schemas.is_empty() {
        return Ok(Vec::new());
    }
    let latest = latest_snapshot(kv, catalog)?;
    let order = kv.generated_order_id()?;
    let snapshot = crate::store::snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    for schema in &mut schemas {
        schema.validity = ValidityWindow::new(order, None);
        batch.put(
            schema_object_key(catalog, schema.schema_id, order),
            schema.encode(),
        );
    }
    if let Some(max_schema_id) = schemas.iter().map(|schema| schema.schema_id.0).max() {
        stage_max_catalog_id_watermark(kv, &mut batch, catalog, max_schema_id)?;
    }
    kv.commit(batch)?;
    Ok(schemas)
}

pub fn commit_drop_schema_rows(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    schema_ids: &[SchemaId],
) -> CatalogResult<Vec<SchemaRow>> {
    if schema_ids.is_empty() {
        return Ok(Vec::new());
    }
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(Some(latest.clone()), order);
    let mut batch = KvBatch::new();
    let mut dropped = Vec::with_capacity(schema_ids.len());
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    for schema_id in schema_ids {
        let mut schema = load_schema_at(kv, catalog, *schema_id, latest.order)?
            .ok_or(CatalogError::NotFound("schema"))?;
        schema.validity.end_order = Some(order);
        batch.put(
            schema_object_key(catalog, schema.schema_id, schema.validity.begin_order),
            schema.encode(),
        );
        dropped.push(schema);
    }
    kv.commit(batch)?;
    Ok(dropped)
}

pub fn list_schemas_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<SchemaRow>> {
    let mut schemas = Vec::new();
    for item in kv.scan_prefix(
        &schema_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_schema_item(catalog, &item.key, &item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            schemas.push(row);
        }
    }
    Ok(schemas)
}

pub(crate) fn list_schema_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<SchemaRow>> {
    #[cfg(test)]
    {
        return list_schema_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return list_schema_rows_uncached(kv, catalog);
        };
        let key = (catalog, latest.order);
        let cache = static_bounded_cache(&SCHEMA_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_schema_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

pub(crate) fn list_schema_rows_for_snapshot_cache(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<SchemaRow>> {
    #[cfg(test)]
    {
        let _ = snapshot_order;
        return list_schema_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let key = (catalog, snapshot_order);
        let cache = static_bounded_cache(&SCHEMA_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_schema_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

fn list_schema_rows_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<SchemaRow>> {
    kv.scan_prefix(
        &schema_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| decode_schema_item(catalog, &item.key, &item.value))
    .collect()
}

pub fn load_schema_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<SchemaRow>> {
    let prefix = schema_object_prefix(catalog, schema_id);
    let end = prefix_end(&schema_object_key(catalog, schema_id, snapshot_order));
    let Some(item) = kv
        .scan_range(&prefix, &end, RangeDirection::Reverse, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let row = decode_schema_item(catalog, &item.key, &item.value)?;
    if row.validity.is_visible_at(snapshot_order) {
        return Ok(Some(row));
    }
    Ok(None)
}

fn decode_schema_item(catalog: CatalogId, key: &[u8], value: &[u8]) -> CatalogResult<SchemaRow> {
    let mut row = SchemaRow::decode(value)?;
    let expected_prefix = schema_object_prefix(catalog, row.schema_id);
    if !key.starts_with(&expected_prefix) {
        return Err(CatalogError::InvalidKey(
            "schema key prefix does not match decoded row".to_owned(),
        ));
    }
    row.validity.begin_order =
        order_from_key_tail(&key[expected_prefix.len()..], row.validity.begin_order)?;
    Ok(row)
}

fn order_from_key_tail(tail: &[u8], value_order: CatalogOrderId) -> CatalogResult<CatalogOrderId> {
    let bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey(format!(
            "schema object key order must be {} bytes, got {}",
            CatalogOrderId::LEN,
            tail.len()
        ))
    })?;
    let kind = if value_order.as_bytes() == bytes {
        value_order.kind()
    } else {
        CatalogOrderKind::FdbVersionstamp
    };
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}
