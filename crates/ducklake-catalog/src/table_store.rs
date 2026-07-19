use std::collections::BTreeMap;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, SnapshotRow, TableColumnRow, TableId, TableRow, TableVersionReplacement,
    conflict::reject_table_metadata_conflicts_since_base,
    conflict_watermarks::stage_max_catalog_id_watermark,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{
        current_table_row_key, current_table_row_prefix, prefix_end, table_object_key,
        table_object_prefix, table_object_scan_prefix, table_visibility_key,
        table_visibility_prefix, table_visibility_scan_end,
    },
    schema_version_state::stage_next_schema_version,
    store::{latest_snapshot, stage_snapshot},
};

#[cfg(not(test))]
static TABLE_ROWS_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId, CatalogOrderId), Vec<TableRow>>,
> = OnceLock::new();

#[cfg(not(test))]
static CURRENT_TABLE_ROWS_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId), Vec<TableRow>>,
> = OnceLock::new();

#[cfg(not(test))]
pub(crate) fn invalidate_runtime_table_read_context(catalog: CatalogId) {
    if let Some(cache) = TABLE_ROWS_CACHE.get() {
        cache.retain(|(_, cached_catalog, _), _| *cached_catalog != catalog);
    }
    if let Some(cache) = CURRENT_TABLE_ROWS_CACHE.get() {
        cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_runtime_table_read_context(_catalog: CatalogId) {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRename {
    pub table_id: TableId,
    pub new_name: String,
}

impl TableRename {
    #[must_use]
    pub fn new(table_id: TableId, new_name: impl Into<String>) -> Self {
        Self {
            table_id,
            new_name: new_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenamedTable {
    pub previous: TableRow,
    pub renamed: TableRow,
}

pub fn create_table_version(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    name: impl Into<String>,
    begin_order: CatalogOrderId,
) -> CatalogResult<TableRow> {
    let row = TableRow::new(table_id, name, begin_order);
    let mut batch = KvBatch::new();
    batch.put(
        table_object_key(catalog, table_id, begin_order),
        row.encode(),
    );
    stage_table_visibility_row(&mut batch, catalog, &row);
    stage_max_catalog_id_watermark(kv, &mut batch, catalog, table_id.0)?;
    kv.commit(batch)?;
    Ok(row)
}

pub fn commit_create_table(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    name: impl Into<String>,
) -> CatalogResult<TableRow> {
    let table = TableRow::new(table_id, name, CatalogOrderId::uuid_v7(0));
    commit_create_table_row(kv, catalog, table)
}

pub fn commit_create_table_row(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut table: TableRow,
) -> CatalogResult<TableRow> {
    let latest = latest_snapshot(kv, catalog)?;
    if let Some(snapshot) = &latest {
        reject_create_table_conflict(kv, catalog, &table, snapshot.order)?;
    }
    let order = kv.generated_order_id()?;
    let snapshot = crate::store::snapshot_row_for_next_sequence(latest, order);
    table.validity = crate::ValidityWindow::new(order, None);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    batch.put(
        table_object_key(catalog, table.table_id, order),
        table.encode(),
    );
    stage_current_table_row(&mut batch, catalog, &table);
    stage_table_visibility_row(&mut batch, catalog, &table);
    stage_max_catalog_id_watermark(kv, &mut batch, catalog, table.table_id.0)?;
    kv.commit(batch)?;
    Ok(table)
}

fn reject_create_table_conflict(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table: &TableRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<()> {
    for current in list_tables_at(kv, catalog, snapshot_order)? {
        if current.table_id == table.table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "table id {} already exists",
                table.table_id.0
            )));
        }
        if current.schema_id == table.schema_id && current.name.eq_ignore_ascii_case(&table.name) {
            return Err(CatalogError::InvalidMutation(format!(
                "table name {} already exists in schema {}",
                table.name, table.schema_id.0
            )));
        }
    }
    Ok(())
}

pub fn commit_append_table_columns(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    columns: Vec<TableColumnRow>,
) -> CatalogResult<TableRow> {
    if columns.is_empty() {
        return load_current_table(kv, catalog, table_id);
    }
    let latest = latest_snapshot(kv, catalog)?;
    let current_snapshot = latest
        .as_ref()
        .ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;
    reject_duplicate_columns(&previous, &columns)?;

    let mut next = previous.clone();
    next.columns.extend(columns);

    kv.commit_table_replacements(
        catalog,
        current_snapshot.sequence,
        vec![TableVersionReplacement::new(table_id, previous, next)],
    )?;
    load_current_table(kv, catalog, table_id)
}

pub fn commit_append_table_columns_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    columns: Vec<TableColumnRow>,
) -> CatalogResult<TableRow> {
    reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    commit_append_table_columns(kv, catalog, table_id, columns)
}

pub fn commit_rename_tables(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    renames: &[TableRename],
) -> CatalogResult<Vec<RenamedTable>> {
    if renames.is_empty() {
        return Ok(Vec::new());
    }
    reject_duplicate_renames(renames)?;

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let current_tables = list_tables_at(kv, catalog, latest.order)?;
    let mut renamed = Vec::with_capacity(renames.len());
    let mut replacements = Vec::with_capacity(renames.len());

    for rename in renames {
        let previous = current_tables
            .iter()
            .find(|table| table.table_id == rename.table_id)
            .cloned()
            .ok_or(CatalogError::NotFound("table"))?;
        reject_table_name_conflict(
            &current_tables,
            renames,
            &renamed,
            &previous,
            &rename.new_name,
        )?;

        let mut next = previous.clone();
        next.name = rename.new_name.clone();
        replacements.push(TableVersionReplacement::new(
            previous.table_id,
            previous.clone(),
            next.clone(),
        ));
        renamed.push(RenamedTable {
            previous,
            renamed: next,
        });
    }

    kv.commit_table_replacements(catalog, latest.sequence, replacements)?;
    Ok(renamed)
}

pub fn commit_rename_tables_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    renames: &[TableRename],
) -> CatalogResult<Vec<RenamedTable>> {
    for rename in renames {
        reject_table_conflicts_since_base(kv, catalog, rename.table_id, base_order, through_order)?;
    }
    commit_rename_tables(kv, catalog, renames)
}

pub fn load_table_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<TableRow>> {
    let prefix = table_object_prefix(catalog, table_id);
    let end = prefix_end(&table_object_key(catalog, table_id, snapshot_order));
    let Some(item) = kv
        .scan_range(&prefix, &end, RangeDirection::Reverse, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let row = decode_table_item(catalog, &item.key, &item.value)?;
    if row.validity.is_visible_at(snapshot_order) {
        return Ok(Some(row));
    }
    Ok(None)
}

fn load_current_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<TableRow> {
    let _snapshot =
        latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))
}

pub(crate) fn load_current_table_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Option<TableRow>> {
    kv.get(&current_table_row_key(catalog, table_id))?
        .map(|bytes| TableRow::decode(&bytes))
        .transpose()
}

fn reject_duplicate_renames(renames: &[TableRename]) -> CatalogResult<()> {
    for (index, rename) in renames.iter().enumerate() {
        if renames[..index]
            .iter()
            .any(|previous| previous.table_id == rename.table_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "table {} is listed more than once for rename",
                rename.table_id.0
            )));
        }
    }
    Ok(())
}

fn reject_table_name_conflict(
    current_tables: &[TableRow],
    renames: &[TableRename],
    renamed: &[RenamedTable],
    previous: &TableRow,
    new_name: &str,
) -> CatalogResult<()> {
    let conflicts_with_current = current_tables.iter().any(|table| {
        table.schema_id == previous.schema_id
            && table.table_id != previous.table_id
            && !renames
                .iter()
                .any(|rename| rename.table_id == table.table_id)
            && table.name.eq_ignore_ascii_case(new_name)
    });
    let conflicts_with_renamed = renamed.iter().any(|table| {
        table.renamed.schema_id == previous.schema_id
            && table.renamed.name.eq_ignore_ascii_case(new_name)
    });
    if conflicts_with_current || conflicts_with_renamed {
        return Err(CatalogError::InvalidMutation(format!(
            "table name {new_name} already exists in schema {}",
            previous.schema_id.0
        )));
    }
    Ok(())
}

fn reject_duplicate_columns(table: &TableRow, columns: &[TableColumnRow]) -> CatalogResult<()> {
    for column in columns {
        if table
            .columns
            .iter()
            .any(|existing| existing.column_id == column.column_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column id {} already exists on table {}",
                column.column_id.0, table.table_id.0
            )));
        }
        if table.columns.iter().any(|existing| {
            existing.parent_id == column.parent_id
                && existing.name.eq_ignore_ascii_case(&column.name)
        }) {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} already exists on table {}",
                column.name, table.table_id.0
            )));
        }
    }
    Ok(())
}

pub fn list_tables_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<TableRow>> {
    list_tables_at_with_latest(kv, catalog, snapshot_order, latest_snapshot(kv, catalog)?)
}

pub(crate) fn list_tables_at_with_latest(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    latest: Option<SnapshotRow>,
) -> CatalogResult<Vec<TableRow>> {
    if latest.is_some_and(|snapshot| snapshot.order == snapshot_order) {
        let current = list_current_table_rows(kv, catalog)?;
        if !current.is_empty() {
            return Ok(current);
        }
    }
    let visible = list_tables_at_from_visibility_index(kv, catalog, snapshot_order)?;
    if !visible.is_empty() {
        return Ok(visible);
    }
    list_tables_at_from_history(kv, catalog, snapshot_order)
}

fn list_tables_at_from_visibility_index(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<TableRow>> {
    let mut tables = BTreeMap::new();
    for item in kv.scan_range(
        &table_visibility_prefix(catalog),
        &table_visibility_scan_end(catalog, snapshot_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_table_visibility_item(catalog, &item.key, &item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            tables.insert(row.table_id, row);
        }
    }
    Ok(tables.into_values().collect())
}

fn list_tables_at_from_history(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<TableRow>> {
    let mut tables = BTreeMap::new();
    for item in kv.scan_prefix(
        &table_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_table_item(catalog, &item.key, &item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            tables.insert(row.table_id, row);
        }
    }
    Ok(tables.into_values().collect())
}

pub(crate) fn list_current_table_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<TableRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        let cache = static_bounded_cache(&CURRENT_TABLE_ROWS_CACHE, 64);
        let key = (kv.catalog_cache_namespace(), catalog);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_current_table_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        return Ok(rows);
    }
    list_current_table_rows_uncached(kv, catalog)
}

fn list_current_table_rows_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<TableRow>> {
    let mut rows = kv
        .scan_prefix(
            &current_table_row_prefix(catalog),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| TableRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.table_id.0);
    Ok(rows)
}

pub(crate) fn list_table_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<TableRow>> {
    #[cfg(test)]
    {
        list_table_rows_uncached(kv, catalog)
    }
    #[cfg(not(test))]
    {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return list_table_rows_uncached(kv, catalog);
        };
        let key = (kv.catalog_cache_namespace(), catalog, latest.order);
        let cache = static_bounded_cache(&TABLE_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_table_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

pub(crate) fn list_table_rows_with_snapshot_cache(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<TableRow>> {
    #[cfg(test)]
    {
        let _ = snapshot_order;
        list_table_rows_uncached(kv, catalog)
    }
    #[cfg(not(test))]
    {
        let key = (kv.catalog_cache_namespace(), catalog, snapshot_order);
        let cache = static_bounded_cache(&TABLE_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_table_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

pub(crate) fn stage_current_table_row(batch: &mut KvBatch, catalog: CatalogId, table: &TableRow) {
    batch.put(
        current_table_row_key(catalog, table.table_id),
        table.encode(),
    );
}

pub(crate) fn stage_table_visibility_row(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table: &TableRow,
) {
    batch.put(
        table_visibility_key(catalog, table.validity.begin_order, table.table_id),
        table.encode(),
    );
}

pub(crate) fn stage_remove_current_table_row(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
) {
    batch.delete(current_table_row_key(catalog, table_id));
}

fn list_table_rows_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<TableRow>> {
    kv.scan_prefix(
        &table_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| decode_table_item(catalog, &item.key, &item.value))
    .collect()
}

pub(crate) fn reject_table_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<()> {
    reject_table_metadata_conflicts_since_base(kv, catalog, table_id, base_order, through_order)
}

fn decode_table_item(catalog: CatalogId, key: &[u8], value: &[u8]) -> CatalogResult<TableRow> {
    let mut row = TableRow::decode(value)?;
    row.validity.begin_order =
        table_order_from_key(catalog, row.table_id, key, row.validity.begin_order)?;
    Ok(row)
}

fn decode_table_visibility_item(
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<TableRow> {
    let mut row = TableRow::decode(value)?;
    let prefix = table_visibility_prefix(catalog);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "table visibility key has wrong prefix".to_owned(),
        ));
    };
    let expected_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != expected_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "table visibility key tail must be {expected_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let begin_order = CatalogOrderId::from_bytes(
        row.validity.begin_order.kind(),
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            CatalogError::InvalidKey("table visibility begin order is truncated".to_owned())
        })?,
    );
    let table_id = TableId(u64::from_be_bytes(
        tail[CatalogOrderId::LEN + 1..].try_into().map_err(|_| {
            CatalogError::InvalidKey("table visibility table id is truncated".to_owned())
        })?,
    ));
    if row.table_id != table_id {
        return Err(CatalogError::InvalidKey(
            "table visibility key table id does not match row".to_owned(),
        ));
    }
    row.validity.begin_order = begin_order;
    Ok(row)
}

fn table_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = table_object_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "table object key has wrong prefix".to_owned(),
        ));
    };
    let bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey(format!(
            "table object key order must be {} bytes, got {}",
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

#[cfg(test)]
#[path = "table_store_tests.rs"]
mod table_store_tests;
