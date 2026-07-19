use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::data_file_store::*;
use crate::{
    AttachedDataFile, CatalogCacheNamespace, CatalogId, CatalogResult, DataFileRow,
    OrderedCatalogKv, RangeDirection, TableId,
    ids::CatalogOrderId,
    keys::{
        KeyFamily, current_data_file_prefix, current_delete_file_key, data_file_begin_prefix,
        data_file_begin_scan_end, family_prefix,
    },
    store::latest_snapshot,
};
pub fn list_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return Ok(
            crate::runtime_read_context::CatalogCurrentFilesContext::for_current_files(
                kv, catalog,
            )?
            .current_data_files_for_table(table_id),
        );
    }
    let started = RuntimeMetricStage::start();
    let rows = scan_current_data_files(kv, catalog, table_id)?;
    record_runtime_method_stage("method.data_file_store.list_current_data_files", started);
    Ok(rows)
}

pub(super) fn list_current_data_files_for_mutation_validation(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    _latest_order: Option<CatalogOrderId>,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return match _latest_order {
            Some(latest_order) => TableCurrentDataFilesContext::for_table_at_order(
                kv,
                catalog,
                table_id,
                latest_order,
            ),
            None => TableCurrentDataFilesContext::for_table(kv, catalog, table_id),
        }
        .map(|context| context.rows);
    }
    scan_current_data_files(kv, catalog, table_id)
}

pub(super) fn scan_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &current_data_file_prefix(catalog, table_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_current_index_value(kv, catalog, &item.value)?;
        if row.validity.end_order.is_none() {
            rows.push(row);
        }
    }
    Ok(rows)
}

#[derive(Clone)]
pub(super) struct TableCurrentDataFilesContext {
    pub(super) rows: Vec<DataFileRow>,
}

impl TableCurrentDataFilesContext {
    pub(super) fn for_table(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
    ) -> CatalogResult<Self> {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return Ok(Self { rows: Vec::new() });
        };
        Self::for_table_at_order(kv, catalog, table_id, latest.order)
    }

    pub(super) fn for_table_at_order(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let key = TableCurrentDataFilesContextKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            table_id,
            latest_order,
        };
        let cache = table_current_data_files_context_cache();
        if let Some(context) = cache.get(key) {
            return Ok(context);
        }
        let context = Self {
            rows: scan_current_data_files(kv, catalog, table_id)?,
        };
        cache.insert(key, context.clone());
        Ok(context)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TableCurrentDataFilesContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    latest_order: CatalogOrderId,
}

pub(crate) fn list_all_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::CurrentDataFile),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_current_index_value(kv, catalog, &item.value)?;
        if row.validity.end_order.is_none() {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0));
    record_runtime_method_stage(
        "method.data_file_store.list_all_current_data_files",
        started,
    );
    Ok(rows)
}

pub fn list_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = kv
        .scan_prefix(
            &crate::keys::family_prefix(catalog, crate::keys::KeyFamily::DataFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DataFileRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.data_file_id.0);
    Ok(rows)
}

pub fn list_current_data_files_with_deletes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    attach_current_delete_files(kv, catalog, list_current_data_files(kv, catalog, table_id)?)
}

pub(crate) fn attach_current_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    if data_files.is_empty() {
        record_runtime_method_stage(
            "method.data_file_store.attach_current_delete_files",
            started,
        );
        return Ok(Vec::new());
    }
    if let Some(latest) = latest_snapshot(kv, catalog)?
        && !delete_file_changes_exist_for_files_at(kv, catalog, &data_files, latest.order)?
    {
        record_runtime_method_stage(
            "method.data_file_store.attach_current_delete_files",
            started,
        );
        return Ok(attach_data_files_without_deletes(data_files));
    }
    let keys = data_files
        .iter()
        .map(|file| current_delete_file_key(catalog, file.data_file_id))
        .collect::<Vec<_>>();
    let delete_pointers = kv.batch_get(&keys)?;
    let mut attached = Vec::with_capacity(data_files.len());
    for (data_file, pointer) in data_files.into_iter().zip(delete_pointers) {
        let delete_file = pointer
            .map(|value| current_delete_file_from_index_value(kv, catalog, &value))
            .transpose()?
            .and_then(|row| row.validity.end_order.is_none().then_some(row));
        attached.push(AttachedDataFile::new(data_file, delete_file));
    }
    record_runtime_method_stage(
        "method.data_file_store.attach_current_delete_files",
        started,
    );
    Ok(attached)
}

pub(crate) fn attach_data_files_without_deletes(
    data_files: Vec<DataFileRow>,
) -> Vec<AttachedDataFile> {
    data_files
        .into_iter()
        .map(|data_file| AttachedDataFile::new(data_file, None))
        .collect()
}

pub(crate) fn attach_delete_file_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file: DataFileRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<AttachedDataFile> {
    let started = RuntimeMetricStage::start();
    let delete_file = load_delete_file_at(kv, catalog, data_file.data_file_id, snapshot_order)?;
    record_runtime_method_stage("method.data_file_store.attach_delete_file_at", started);
    Ok(AttachedDataFile::new(data_file, delete_file))
}

pub fn list_data_files_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        return Ok(TableDataFilesAtContext::for_table_snapshot(
            kv,
            catalog,
            table_id,
            snapshot_order,
        )?
        .visible_rows());
    }
    list_data_files_at_uncached(kv, catalog, table_id, snapshot_order)
}

pub(super) fn list_data_files_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let rows = TableDataFilesAtContext::load(kv, catalog, table_id, snapshot_order)?.visible_rows();
    record_runtime_method_stage("method.data_file_store.list_data_files_at", started);
    Ok(rows)
}

#[derive(Clone)]
pub(super) struct TableDataFilesAtContext {
    visible_rows: Vec<DataFileRow>,
}

impl TableDataFilesAtContext {
    #[cfg_attr(test, allow(dead_code))]
    fn for_table_snapshot(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = TableDataFilesAtContextKey {
                namespace: kv.catalog_cache_namespace(),
                catalog,
                table_id,
                snapshot_order,
            };
            let cache = table_data_files_at_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog, table_id, snapshot_order)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog, table_id, snapshot_order)
        }
    }

    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let mut seen = BTreeSet::new();
        let mut rows = Vec::new();
        for item in kv.scan_range(
            &data_file_begin_prefix(catalog, table_id),
            &data_file_begin_scan_end(catalog, table_id, snapshot_order),
            RangeDirection::Forward,
            usize::MAX,
        )? {
            let row = data_file_from_begin_index_item(kv, catalog, &item.key, &item.value)?;
            if data_file_visible_at(&row, snapshot_order) {
                seen.insert(row.data_file_id);
                rows.push(row);
            }
        }
        let current_rows = list_current_data_files(kv, catalog, table_id)?;
        for row in &current_rows {
            if !seen.contains(&row.data_file_id) && data_file_visible_at(row, snapshot_order) {
                rows.push(row.clone());
            }
        }
        Ok(Self {
            visible_rows: without_backfilled_source_duplicates(kv, catalog, rows)?,
        })
    }

    pub(super) fn visible_rows(&self) -> Vec<DataFileRow> {
        self.visible_rows.clone()
    }
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TableDataFilesAtContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
pub(super) static TABLE_DATA_FILES_AT_CONTEXT_CACHE: OnceLock<
    BoundedCache<TableDataFilesAtContextKey, TableDataFilesAtContext>,
> = OnceLock::new();

pub(super) static TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE: OnceLock<
    BoundedCache<TableCurrentDataFilesContextKey, TableCurrentDataFilesContext>,
> = OnceLock::new();

#[cfg(not(test))]
pub(super) fn table_data_files_at_context_cache()
-> &'static BoundedCache<TableDataFilesAtContextKey, TableDataFilesAtContext> {
    static_bounded_cache(&TABLE_DATA_FILES_AT_CONTEXT_CACHE, 512)
}

pub(super) fn table_current_data_files_context_cache()
-> &'static BoundedCache<TableCurrentDataFilesContextKey, TableCurrentDataFilesContext> {
    static_bounded_cache(&TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE, 1024)
}

#[cfg(not(test))]
pub(crate) fn invalidate_data_file_read_context(catalog: CatalogId) {
    if let Some(cache) = TABLE_DATA_FILES_AT_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = DELETE_FILE_CHANGED_TABLES_CACHE.get() {
        cache.retain(|key, _| key.1 != catalog);
    }
    if let Some(cache) = TABLE_DELETE_FILE_CHANGES_EXIST_CACHE.get() {
        cache.retain(|key, _| key.1 != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_data_file_read_context(catalog: CatalogId) {
    if let Some(cache) = TABLE_CURRENT_DATA_FILES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}
