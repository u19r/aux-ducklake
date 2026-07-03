use std::collections::BTreeSet;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MacroId, MacroRow, MutableCatalogKv,
    OrderedCatalogKv, RangeDirection, RawSnapshotSequence, SnapshotRow, ValidityWindow,
    conflict_watermarks::stage_max_catalog_id_watermark,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{macro_object_key, macro_object_prefix, macro_object_scan_prefix, prefix_end},
    schema_version_state::stage_next_schema_version,
    store::{latest_snapshot, stage_snapshot},
};

#[cfg(not(test))]
static MACRO_ROWS_CACHE: OnceLock<BoundedCache<(CatalogId, CatalogOrderId), Vec<MacroRow>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedMacro {
    pub macro_row: MacroRow,
}

pub fn commit_create_macro_rows(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut macros: Vec<MacroRow>,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<MacroRow>> {
    if macros.is_empty() {
        return Ok(Vec::new());
    }
    let latest = latest_snapshot(kv, catalog)?;
    if let Some(latest) = &latest {
        reject_macro_create_conflicts(kv, catalog, latest.order, &macros)?;
    }
    let order = kv.generated_order_id()?;
    let snapshot = commit_raw_snapshot.map_or_else(
        || crate::store::snapshot_row_for_next_sequence(latest, order),
        |sequence| SnapshotRow::new(order, sequence),
    );
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    for macro_row in &mut macros {
        macro_row.validity = ValidityWindow::new(order, None);
        batch.put(
            macro_object_key(catalog, macro_row.macro_id, order),
            macro_row.encode(),
        );
    }
    if let Some(max_macro_id) = macros.iter().map(|macro_row| macro_row.macro_id.0).max() {
        stage_max_catalog_id_watermark(kv, &mut batch, catalog, max_macro_id)?;
    }
    kv.commit(batch)?;
    Ok(macros)
}

pub fn commit_drop_macros(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    macro_ids: &[MacroId],
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<DroppedMacro>> {
    if macro_ids.is_empty() {
        return Ok(Vec::new());
    }
    reject_duplicate_macro_ids(macro_ids)?;

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(
        order,
        commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next()),
    );
    let mut batch = KvBatch::new();
    let mut dropped = Vec::with_capacity(macro_ids.len());
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;

    for macro_id in macro_ids {
        let mut macro_row =
            load_macro_at(kv, catalog, *macro_id, latest.order)?.ok_or_else(|| {
                CatalogError::InvalidMutation(format!(
                    "conflict dropping macro {}: macro no longer exists",
                    macro_id.0
                ))
            })?;
        macro_row.validity.end_order = Some(order);
        batch.put(
            macro_object_key(catalog, macro_row.macro_id, macro_row.validity.begin_order),
            macro_row.encode(),
        );
        dropped.push(DroppedMacro { macro_row });
    }

    kv.commit(batch)?;
    Ok(dropped)
}

pub fn list_macros_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<MacroRow>> {
    let mut macros = Vec::new();
    for item in kv.scan_prefix(
        &macro_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_macro_item(catalog, &item.key, &item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            macros.push(row);
        }
    }
    Ok(macros)
}

pub(crate) fn list_macro_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<MacroRow>> {
    #[cfg(test)]
    {
        return list_macro_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return list_macro_rows_uncached(kv, catalog);
        };
        let key = (catalog, latest.order);
        let cache = static_bounded_cache(&MACRO_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_macro_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

pub(crate) fn list_macro_rows_for_snapshot_cache(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<MacroRow>> {
    #[cfg(test)]
    {
        let _ = snapshot_order;
        return list_macro_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let key = (catalog, snapshot_order);
        let cache = static_bounded_cache(&MACRO_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_macro_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

fn list_macro_rows_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<MacroRow>> {
    kv.scan_prefix(
        &macro_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| decode_macro_item(catalog, &item.key, &item.value))
    .collect()
}

pub fn load_macro_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    macro_id: MacroId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<MacroRow>> {
    let prefix = macro_object_prefix(catalog, macro_id);
    let end = prefix_end(&macro_object_key(catalog, macro_id, snapshot_order));
    let Some(item) = kv
        .scan_range(&prefix, &end, RangeDirection::Reverse, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let row = decode_macro_item(catalog, &item.key, &item.value)?;
    if row.validity.is_visible_at(snapshot_order) {
        return Ok(Some(row));
    }
    Ok(None)
}

pub(crate) fn reject_macro_create_conflicts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    latest_order: CatalogOrderId,
    macros: &[MacroRow],
) -> CatalogResult<()> {
    let current_macros = list_macros_at(kv, catalog, latest_order)?;
    let mut requested = BTreeSet::new();
    let mut requested_ids = BTreeSet::new();
    for macro_row in macros {
        if !requested_ids.insert(macro_row.macro_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating macro {}: macro id {} is used more than once",
                macro_row.name, macro_row.macro_id.0
            )));
        }
        if current_macros
            .iter()
            .any(|current| current.macro_id == macro_row.macro_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating macro {}: macro id {} already exists",
                macro_row.name, macro_row.macro_id.0
            )));
        }
        for macro_type in macro_types(macro_row) {
            let identity = (macro_row.schema_id, macro_row.name.as_str(), macro_type);
            if !requested.insert(identity) {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating macro {}: duplicate {} macro in schema {}",
                    macro_row.name, macro_type, macro_row.schema_id.0
                )));
            }
            if current_macros.iter().any(|current| {
                current.schema_id == macro_row.schema_id
                    && current.name == macro_row.name
                    && macro_types(current).contains(&macro_type)
            }) {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating macro {}: {} macro already exists in schema {}",
                    macro_row.name, macro_type, macro_row.schema_id.0
                )));
            }
        }
    }
    Ok(())
}

fn decode_macro_item(catalog: CatalogId, key: &[u8], value: &[u8]) -> CatalogResult<MacroRow> {
    let mut row = MacroRow::decode(value)?;
    row.validity.begin_order =
        macro_order_from_key(catalog, row.macro_id, key, row.validity.begin_order)?;
    Ok(row)
}

fn macro_order_from_key(
    catalog: CatalogId,
    macro_id: MacroId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = macro_object_prefix(catalog, macro_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "macro object key has wrong prefix".to_owned(),
        ));
    };
    let bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey(format!(
            "macro object key order must be {} bytes, got {}",
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

fn reject_duplicate_macro_ids(macro_ids: &[MacroId]) -> CatalogResult<()> {
    for (index, macro_id) in macro_ids.iter().enumerate() {
        if macro_ids[..index].iter().any(|prior| prior == macro_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "macro {} is listed more than once for drop",
                macro_id.0
            )));
        }
    }
    Ok(())
}

fn macro_types(macro_row: &MacroRow) -> BTreeSet<&str> {
    macro_row
        .implementations
        .iter()
        .map(|implementation| implementation.macro_type.as_str())
        .collect()
}

#[cfg(test)]
#[path = "macro_store_tests.rs"]
mod macro_store_tests;
