use std::collections::BTreeSet;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::data_file_store::*;
use crate::{
    AttachedDataFile, CatalogCacheNamespace, CatalogId, CatalogResult, DataFileRow,
    OrderedCatalogKv, RangeDirection, TableId,
    ids::CatalogOrderId,
    keys::{
        decode_order_delete_file_change_table_id, order_delete_file_change_prefix,
        order_delete_file_change_scan_end,
    },
};
pub fn list_data_files_with_deletes_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    let rows = attach_delete_files_at(
        kv,
        catalog,
        list_data_files_at(kv, catalog, table_id, snapshot_order)?,
        snapshot_order,
    )?;
    record_runtime_method_stage(
        "method.data_file_store.list_data_files_with_deletes_at",
        started,
    );
    Ok(rows)
}

pub(crate) fn attach_delete_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    if !delete_file_changes_exist_for_files_at(kv, catalog, &data_files, snapshot_order)? {
        record_runtime_method_stage("method.data_file_store.attach_delete_files_at", started);
        return Ok(attach_data_files_without_deletes(data_files));
    }
    let mut attached = Vec::with_capacity(data_files.len());
    for data_file in data_files {
        let delete_file = load_delete_file_at(kv, catalog, data_file.data_file_id, snapshot_order)?;
        attached.push(AttachedDataFile::new(data_file, delete_file));
    }
    record_runtime_method_stage("method.data_file_store.attach_delete_files_at", started);
    Ok(attached)
}

pub(super) fn delete_file_changes_exist_for_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: &[DataFileRow],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    let mut table_ids = data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<Vec<_>>();
    table_ids.sort_unstable();
    table_ids.dedup();
    for table_id in table_ids {
        if table_delete_file_changes_exist_at(kv, catalog, table_id, snapshot_order)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn table_delete_file_changes_exist_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    #[cfg(not(test))]
    {
        Ok(delete_file_changed_tables_at(kv, catalog, snapshot_order)?.contains(&table_id))
    }
    #[cfg(test)]
    {
        Ok(
            delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)?
                .contains(&table_id),
        )
    }
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn delete_file_changed_tables_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    #[cfg(not(test))]
    {
        if let Some(tables) = cached_delete_file_changed_tables_at(
            kv.catalog_cache_namespace(),
            catalog,
            snapshot_order,
        ) {
            return Ok(tables);
        }
        let tables = delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)?;
        cache_delete_file_changed_tables_at(
            kv.catalog_cache_namespace(),
            catalog,
            snapshot_order,
            tables.clone(),
        );
        Ok(tables)
    }
    #[cfg(test)]
    {
        delete_file_changed_tables_at_uncached(kv, catalog, snapshot_order)
    }
}

pub(super) fn delete_file_changed_tables_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    let prefix = order_delete_file_change_prefix(catalog);
    let mut tables = BTreeSet::new();
    for item in kv.scan_range(
        &prefix,
        &order_delete_file_change_scan_end(catalog, snapshot_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        tables.insert(decode_order_delete_file_change_table_id(
            &prefix, &item.key,
        )?);
    }
    Ok(tables)
}

#[cfg(not(test))]
pub(super) static DELETE_FILE_CHANGED_TABLES_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId, CatalogOrderId), BTreeSet<TableId>>,
> = OnceLock::new();

#[cfg(not(test))]
pub(super) static TABLE_DELETE_FILE_CHANGES_EXIST_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId, TableId, CatalogOrderId), bool>,
> = OnceLock::new();

#[cfg(not(test))]
pub(super) fn delete_file_changed_tables_cache()
-> &'static BoundedCache<(CatalogCacheNamespace, CatalogId, CatalogOrderId), BTreeSet<TableId>> {
    static_bounded_cache(&DELETE_FILE_CHANGED_TABLES_CACHE, 512)
}

#[cfg(not(test))]
pub(super) fn table_delete_file_changes_exist_cache()
-> &'static BoundedCache<(CatalogCacheNamespace, CatalogId, TableId, CatalogOrderId), bool> {
    static_bounded_cache(&TABLE_DELETE_FILE_CHANGES_EXIST_CACHE, 4096)
}

#[cfg(not(test))]
pub(crate) fn cached_delete_file_changed_tables_at(
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> Option<BTreeSet<TableId>> {
    DELETE_FILE_CHANGED_TABLES_CACHE
        .get()
        .and_then(|cache| cache.get((namespace, catalog, snapshot_order)))
}

#[cfg(test)]
pub(crate) fn cached_delete_file_changed_tables_at(
    _namespace: CatalogCacheNamespace,
    _catalog: CatalogId,
    _snapshot_order: CatalogOrderId,
) -> Option<BTreeSet<TableId>> {
    None
}

#[cfg(not(test))]
pub(crate) fn cache_delete_file_changed_tables_at(
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    tables: BTreeSet<TableId>,
) {
    delete_file_changed_tables_cache().insert((namespace, catalog, snapshot_order), tables);
}

#[cfg(test)]
pub(crate) fn cache_delete_file_changed_tables_at(
    _namespace: CatalogCacheNamespace,
    _catalog: CatalogId,
    _snapshot_order: CatalogOrderId,
    _tables: BTreeSet<TableId>,
) {
}

#[cfg(not(test))]
pub(crate) fn cached_table_delete_file_changes_exist_at(
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> Option<bool> {
    TABLE_DELETE_FILE_CHANGES_EXIST_CACHE
        .get()
        .and_then(|cache| cache.get((namespace, catalog, table_id, snapshot_order)))
}

#[cfg(test)]
pub(crate) fn cached_table_delete_file_changes_exist_at(
    _namespace: CatalogCacheNamespace,
    _catalog: CatalogId,
    _table_id: TableId,
    _snapshot_order: CatalogOrderId,
) -> Option<bool> {
    None
}

#[cfg(not(test))]
pub(crate) fn cache_table_delete_file_changes_exist_at(
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    exists: bool,
) {
    table_delete_file_changes_exist_cache()
        .insert((namespace, catalog, table_id, snapshot_order), exists);
}

#[cfg(test)]
pub(crate) fn cache_table_delete_file_changes_exist_at(
    _namespace: CatalogCacheNamespace,
    _catalog: CatalogId,
    _table_id: TableId,
    _snapshot_order: CatalogOrderId,
    _exists: bool,
) {
}
