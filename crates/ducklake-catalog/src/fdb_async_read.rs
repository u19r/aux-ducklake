use std::{
    collections::BTreeSet,
    ops::Deref,
    time::{Duration, Instant},
};

use foundationdb::options::TransactionOption;
use futures::{
    StreamExt, TryStreamExt,
    future::{try_join, try_join_all},
    lock::Mutex,
    stream,
};

use crate::{
    AttachedDataFile, CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult,
    DataFileId, DataFileRow, DeleteFileId, DeleteFileRow, FdbOrderedCatalogKv, RangeDirection,
    RangeItem, TableId,
    data_file_store::{
        attach_data_files_without_deletes, cache_delete_file_changed_tables_at,
        cache_table_delete_file_changes_exist_at, cached_delete_file_changed_tables_at,
        cached_table_delete_file_changes_exist_at, data_file_visible_at,
        delete_file_can_answer_snapshot,
    },
    fdb_async::{MAX_FDB_READ_RETRIES, is_retryable_fdb_read_error},
    fdb_runtime::map_fdb_error,
    fdb_versionstamp::incomplete_order,
    keys::{
        current_data_file_prefix, current_delete_file_key, data_file_begin_prefix,
        data_file_begin_scan_end, data_file_key, decode_order_delete_file_change_table_id,
        delete_file_key, delete_file_timeline_prefix, delete_file_timeline_scan_end,
        order_delete_file_change_prefix, order_delete_file_change_scan_end, prefix_end,
        table_delete_file_change_prefix, table_delete_file_change_scan_end,
    },
};

const MAX_TABLE_SCOPED_DELETE_CHANGE_PROBES: usize = 8;
const DEFAULT_ASYNC_SCAN_PAGE_SIZE: usize = 256;
const ASYNC_SCAN_DEADLINE: Duration = Duration::from_secs(5);

impl FdbOrderedCatalogKv {
    pub async fn list_current_data_files_async(
        &self,
        catalog: CatalogId,
        table: TableId,
    ) -> CatalogResult<Vec<DataFileRow>> {
        Ok(self
            .list_current_data_files_bounded_async(catalog, table, DEFAULT_ASYNC_SCAN_PAGE_SIZE)
            .await?
            .data_files)
    }

    pub async fn attach_delete_files_at_async(
        &self,
        catalog: CatalogId,
        data_files: Vec<DataFileRow>,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Vec<AttachedDataFile>> {
        if data_files.is_empty() {
            return Ok(Vec::new());
        }
        let trx = self.create_transaction()?;
        if !delete_file_changes_exist_for_files_at(self, &trx, catalog, &data_files, snapshot_order)
            .await?
        {
            return Ok(attach_data_files_without_deletes(data_files));
        }
        let delete_files = delete_file_rows_at_snapshot(
            self,
            &trx,
            catalog,
            data_files.iter().map(|file| file.data_file_id),
            snapshot_order,
        )
        .await?;

        Ok(data_files
            .into_iter()
            .zip(delete_files)
            .map(|(data_file, delete_file)| AttachedDataFile::new(data_file, delete_file))
            .collect())
    }

    pub async fn attach_current_delete_files_async(
        &self,
        catalog: CatalogId,
        data_files: Vec<DataFileRow>,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Vec<AttachedDataFile>> {
        if data_files.is_empty() {
            return Ok(Vec::new());
        }
        let trx = self.create_transaction()?;
        if !delete_file_changes_exist_for_files_at(self, &trx, catalog, &data_files, latest_order)
            .await?
        {
            return Ok(attach_data_files_without_deletes(data_files));
        }
        let delete_files = current_delete_file_rows(
            self,
            &trx,
            catalog,
            data_files.iter().map(|file| file.data_file_id),
        )
        .await?;

        Ok(data_files
            .into_iter()
            .zip(delete_files)
            .map(|(data_file, delete_file)| AttachedDataFile::new(data_file, delete_file))
            .collect())
    }

    pub async fn list_data_files_at_async(
        &self,
        catalog: CatalogId,
        table: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Vec<DataFileRow>> {
        let trx = self.create_transaction()?;
        let historical_start = data_file_begin_prefix(catalog, table);
        let historical_end = data_file_begin_scan_end(catalog, table, snapshot_order);
        let current_start = current_data_file_prefix(catalog, table);
        let current_end = prefix_end(&current_start);
        let budget = Mutex::new(ScanBudget::new());
        let (historical_scan, current_scan) = try_join(
            self.scan_range_with_budget(
                &historical_start,
                &historical_end,
                RangeDirection::Forward,
                ScanLimit::All,
                DEFAULT_ASYNC_SCAN_PAGE_SIZE,
                &budget,
            ),
            self.scan_range_with_budget(
                &current_start,
                &current_end,
                RangeDirection::Forward,
                ScanLimit::All,
                DEFAULT_ASYNC_SCAN_PAGE_SIZE,
                &budget,
            ),
        )
        .await?;

        let mut seen = BTreeSet::new();
        let mut rows = Vec::new();
        let (historical_rows, current_rows) = try_join(
            fetch_historical_data_file_rows(self, &trx, catalog, table, historical_scan.items),
            fetch_current_data_file_rows(self, &trx, catalog, current_scan.items),
        )
        .await?;
        for row in historical_rows {
            if data_file_visible_at(&row, snapshot_order) {
                seen.insert(row.data_file_id);
                rows.push(row);
            }
        }
        for row in current_rows {
            if !seen.contains(&row.data_file_id) && data_file_visible_at(&row, snapshot_order) {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub async fn list_current_data_files_bounded_async(
        &self,
        catalog: CatalogId,
        table: TableId,
        scan_chunk_size: usize,
    ) -> CatalogResult<BoundedDataFileRead> {
        let scan = self
            .scan_prefix_bounded_async(&current_data_file_prefix(catalog, table), scan_chunk_size)
            .await?;
        let trx = self.create_transaction()?;
        let mut rows = Vec::new();
        for row in fetch_current_data_file_rows(self, &trx, catalog, scan.items).await? {
            if row.validity.end_order.is_none() {
                rows.push(row);
            }
        }
        Ok(BoundedDataFileRead {
            data_files: rows,
            scan_transaction_count: scan.transaction_count,
        })
    }

    pub async fn count_data_files_bounded_async(
        &self,
        catalog: CatalogId,
        table: TableId,
        scan_chunk_size: usize,
    ) -> CatalogResult<BoundedDataFileCount> {
        let scan = self
            .scan_prefix_bounded_async(&current_data_file_prefix(catalog, table), scan_chunk_size)
            .await?;
        Ok(BoundedDataFileCount {
            count: scan.items.len(),
            scan_transaction_count: scan.transaction_count,
        })
    }

    pub(crate) async fn get_async(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        let mut last_retry_error = None;
        for _ in 0..=MAX_FDB_READ_RETRIES {
            match self.get_once_async(key).await {
                Ok(value) => return Ok(value),
                Err(error) if is_retryable_fdb_read_error(&error) => {
                    last_retry_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb async point-read retry loop did not run".to_owned(),
            )
        }))
    }

    async fn get_once_async(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        let trx = self.create_transaction()?;
        trx.get(&self.namespaced_key(key), false)
            .await
            .map_err(map_fdb_error)
            .map(|value| value.map(|bytes| bytes.deref().to_vec()))
    }

    pub(crate) async fn scan_prefix_async(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        self.scan_range_async(prefix, &prefix_end(prefix), direction, limit)
            .await
    }

    pub(crate) async fn scan_prefixes_with_budget(
        &self,
        prefixes: &[Vec<u8>],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<Vec<RangeItem>>> {
        if limit == 0 {
            return Ok(vec![Vec::new(); prefixes.len()]);
        }
        let budget = Mutex::new(ScanBudget::new());
        let budget = &budget;
        stream::iter(prefixes.iter().map(|prefix| async move {
            self.scan_range_with_budget(
                prefix,
                &prefix_end(prefix),
                direction,
                ScanLimit::from_legacy(limit),
                DEFAULT_ASYNC_SCAN_PAGE_SIZE,
                budget,
            )
            .await
            .map(|scan| scan.items)
        }))
        .buffered(crate::constants::FDB_PREFIX_SCAN_CONCURRENCY)
        .try_collect()
        .await
    }

    async fn scan_prefix_bounded_async(
        &self,
        prefix: &[u8],
        chunk_size: usize,
    ) -> CatalogResult<BoundedScan> {
        let budget = Mutex::new(ScanBudget::new());
        self.scan_range_with_budget(
            prefix,
            &prefix_end(prefix),
            RangeDirection::Forward,
            ScanLimit::All,
            chunk_size,
            &budget,
        )
        .await
    }

    pub(crate) async fn scan_range_async(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let budget = Mutex::new(ScanBudget::new());
        let scan_limit = ScanLimit::from_legacy(limit);
        Ok(self
            .scan_range_with_budget(
                start,
                end,
                direction,
                scan_limit,
                DEFAULT_ASYNC_SCAN_PAGE_SIZE,
                &budget,
            )
            .await?
            .items)
    }

    async fn scan_all_range_async(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
    ) -> CatalogResult<Vec<RangeItem>> {
        let budget = Mutex::new(ScanBudget::new());
        Ok(self
            .scan_range_with_budget(
                start,
                end,
                direction,
                ScanLimit::All,
                DEFAULT_ASYNC_SCAN_PAGE_SIZE,
                &budget,
            )
            .await?
            .items)
    }

    async fn scan_range_with_budget(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: ScanLimit,
        page_size: usize,
        budget: &Mutex<ScanBudget>,
    ) -> CatalogResult<BoundedScan> {
        let mut cursor =
            ScanCursor::new(start.to_vec(), end.to_vec(), direction, page_size, limit)?;
        let mut transaction_count: usize = 0;
        while cursor.wants_page() {
            let page = self
                .scan_range_page_with_budget(
                    cursor.start(),
                    cursor.end(),
                    direction,
                    cursor.page_size(),
                    budget,
                )
                .await?;
            transaction_count = transaction_count.saturating_add(1);
            cursor.accept_page(page)?;
        }
        Ok(BoundedScan {
            items: cursor.into_items(),
            transaction_count,
        })
    }

    async fn scan_range_page_with_budget(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
        budget: &Mutex<ScanBudget>,
    ) -> CatalogResult<Vec<RangeItem>> {
        loop {
            let timeout_ms = budget.lock().await.begin_attempt()?;
            match self
                .scan_range_once_async(start, end, direction, limit, timeout_ms)
                .await
            {
                Ok(items) => return Ok(items),
                Err(error) if is_retryable_fdb_read_error(&error) => {
                    budget.lock().await.record_retryable_failure(error)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn scan_range_once_async(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
        timeout_ms: i32,
    ) -> CatalogResult<Vec<RangeItem>> {
        let trx = self.create_transaction()?;
        trx.set_option(TransactionOption::Timeout(i64::from(timeout_ms)))
            .map_err(map_fdb_error)?;
        let options = self.create_scan_options(start, end, direction, limit);
        let values = trx
            .get_ranges_keyvalues(options, false)
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_fdb_error)?;
        crate::fdb_runtime::record_transaction_usage(&trx);
        crate::runtime_metrics::record_runtime_measurement(
            "fdb.range.logical_page",
            values.len() as u64,
            values
                .iter()
                .map(|item| (item.key().len() + item.value().len()) as u64)
                .sum(),
        );
        values
            .into_iter()
            .map(|item| {
                let key = self.strip_namespace(item.key())?;
                let value = crate::fdb_key_codec::decode_value(&key, item.value())?;
                Ok(RangeItem { key, value })
            })
            .collect()
    }

    pub(crate) fn clear_prefix_in_transaction(
        &self,
        trx: &foundationdb::Transaction,
        prefix: &[u8],
    ) {
        trx.clear_range(
            &self.namespaced_range_bound(prefix),
            &self.namespaced_range_bound(&prefix_end(prefix)),
        );
    }
}

async fn delete_file_changes_exist_for_files_at(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    data_files: &[DataFileRow],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    let table_ids = data_files
        .iter()
        .map(|file| file.table_id)
        .collect::<BTreeSet<_>>();
    if data_files
        .iter()
        .any(|file| materialized_partial_file_requires_delete_lookup(file, snapshot_order))
    {
        return Ok(true);
    }
    if let Some(changed_tables) =
        cached_delete_file_changed_tables_at(kv.catalog_cache_namespace(), catalog, snapshot_order)
    {
        return Ok(table_ids
            .iter()
            .any(|table_id| changed_tables.contains(table_id)));
    }
    if table_ids.len() > MAX_TABLE_SCOPED_DELETE_CHANGE_PROBES {
        let changed_tables = delete_file_changed_tables_at(kv, catalog, snapshot_order).await?;
        return Ok(table_ids
            .iter()
            .any(|table_id| changed_tables.contains(table_id)));
    }
    for table_id in table_ids {
        if table_delete_file_changes_exist_at(kv, trx, catalog, table_id, snapshot_order).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn materialized_partial_file_requires_delete_lookup(
    file: &DataFileRow,
    snapshot_order: CatalogOrderId,
) -> bool {
    file.max_partial_order.is_some() && file.validity.begin_order <= snapshot_order
}

async fn delete_file_changed_tables_at(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeSet<TableId>> {
    if let Some(tables) =
        cached_delete_file_changed_tables_at(kv.catalog_cache_namespace(), catalog, snapshot_order)
    {
        return Ok(tables);
    }
    let prefix = order_delete_file_change_prefix(catalog);
    let items = kv
        .scan_all_range_async(
            &prefix,
            &order_delete_file_change_scan_end(catalog, snapshot_order),
            RangeDirection::Forward,
        )
        .await?;
    let mut tables = BTreeSet::new();
    for item in items {
        tables.insert(decode_order_delete_file_change_table_id(
            &prefix, &item.key,
        )?);
    }
    cache_delete_file_changed_tables_at(
        kv.catalog_cache_namespace(),
        catalog,
        snapshot_order,
        tables.clone(),
    );
    Ok(tables)
}

async fn table_delete_file_changes_exist_at(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    if let Some(exists) = cached_table_delete_file_changes_exist_at(
        kv.catalog_cache_namespace(),
        catalog,
        table_id,
        snapshot_order,
    ) {
        return Ok(exists);
    }
    let prefix = table_delete_file_change_prefix(catalog, table_id);
    let items = scan_range_in_transaction(
        kv,
        trx,
        &prefix,
        &table_delete_file_change_scan_end(catalog, table_id, snapshot_order),
        RangeDirection::Forward,
        1,
    )
    .await?;
    let exists = !items.is_empty();
    cache_table_delete_file_changes_exist_at(
        kv.catalog_cache_namespace(),
        catalog,
        table_id,
        snapshot_order,
        exists,
    );
    Ok(exists)
}

async fn scan_range_in_transaction(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    start: &[u8],
    end: &[u8],
    direction: RangeDirection,
    limit: usize,
) -> CatalogResult<Vec<RangeItem>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let options = kv.create_scan_options(start, end, direction, limit);
    let values = trx
        .get_ranges_keyvalues(options, false)
        .try_collect::<Vec<_>>()
        .await
        .map_err(map_fdb_error)?;
    values
        .into_iter()
        .map(|item| {
            let key = kv.strip_namespace(item.key())?;
            let value = crate::fdb_key_codec::decode_value(&key, item.value())?;
            Ok(RangeItem { key, value })
        })
        .collect()
}

async fn fetch_current_data_file_rows(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    index_items: Vec<RangeItem>,
) -> CatalogResult<Vec<DataFileRow>> {
    fetch_data_file_rows_from_index_values(
        kv,
        trx,
        catalog,
        index_items.into_iter().map(|item| item.value),
    )
    .await
}

async fn fetch_historical_data_file_rows(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: TableId,
    index_items: Vec<RangeItem>,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut decoded = Vec::with_capacity(index_items.len());
    let mut fallback_keys = Vec::new();
    for item in index_items {
        match DataFileRow::decode(&item.value) {
            Ok(mut row) => {
                row.validity.begin_order = data_file_begin_order_from_key(
                    catalog,
                    table,
                    &item.key,
                    row.validity.begin_order.kind(),
                )?;
                decoded.push(DataFileIndexValue::Row(row));
            }
            Err(_) => {
                let data_file_id = decode_data_file_id(&item.value)?;
                fallback_keys.push(data_file_key(catalog, data_file_id));
                decoded.push(DataFileIndexValue::Fallback(fallback_keys.len() - 1));
            }
        }
    }
    rows_from_decoded_data_file_index(kv, trx, decoded, fallback_keys).await
}

async fn fetch_data_file_rows_from_index_values(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    values: impl Iterator<Item = Vec<u8>>,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut decoded = Vec::new();
    let mut fallback_keys = Vec::new();
    for value in values {
        match DataFileRow::decode(&value) {
            Ok(row) => decoded.push(DataFileIndexValue::Row(row)),
            Err(_) => {
                let data_file_id = decode_data_file_id(&value)?;
                fallback_keys.push(data_file_key(catalog, data_file_id));
                decoded.push(DataFileIndexValue::Fallback(fallback_keys.len() - 1));
            }
        }
    }
    rows_from_decoded_data_file_index(kv, trx, decoded, fallback_keys).await
}

async fn rows_from_decoded_data_file_index(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    decoded: Vec<DataFileIndexValue>,
    fallback_keys: Vec<Vec<u8>>,
) -> CatalogResult<Vec<DataFileRow>> {
    let fallback_values = get_many_in_transaction(kv, trx, &fallback_keys).await?;
    let mut rows = Vec::with_capacity(decoded.len());
    for value in decoded {
        match value {
            DataFileIndexValue::Row(row) => rows.push(row),
            DataFileIndexValue::Fallback(index) => {
                let Some(bytes) = fallback_values.get(index).and_then(Option::as_ref) else {
                    return Err(CatalogError::NotFound("data file"));
                };
                rows.push(DataFileRow::decode(bytes)?);
            }
        }
    }
    Ok(rows)
}

enum DataFileIndexValue {
    Row(DataFileRow),
    Fallback(usize),
}

async fn delete_file_rows_at_snapshot(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    data_file_ids: impl Iterator<Item = DataFileId>,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<Option<DeleteFileRow>>> {
    let scans = try_join_all(data_file_ids.map(|data_file_id| async move {
        let items = scan_range_in_transaction(
            kv,
            trx,
            &delete_file_timeline_prefix(catalog, data_file_id),
            &delete_file_timeline_scan_end(catalog, data_file_id, snapshot_order),
            RangeDirection::Reverse,
            1,
        )
        .await?;
        for item in &items {
            let row = delete_file_from_timeline_item(kv, trx, catalog, data_file_id, item).await?;
            let timeline_order = delete_file_timeline_order_from_key(
                catalog,
                data_file_id,
                &item.key,
                row.validity.begin_order.kind(),
            )?;
            if delete_file_can_answer_snapshot(&row, timeline_order, snapshot_order) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }))
    .await?;
    Ok(scans)
}

async fn current_delete_file_rows(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    data_file_ids: impl Iterator<Item = DataFileId>,
) -> CatalogResult<Vec<Option<DeleteFileRow>>> {
    let reads = try_join_all(data_file_ids.map(|data_file_id| async move {
        let Some(value) = trx
            .get(
                &kv.namespaced_key(&current_delete_file_key(catalog, data_file_id)),
                false,
            )
            .await
            .map_err(map_fdb_error)?
        else {
            return Ok(None);
        };
        current_delete_file_from_index_value(kv, trx, catalog, value.deref()).await
    }))
    .await?;
    Ok(reads)
}

async fn get_many_in_transaction(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    keys: &[Vec<u8>],
) -> CatalogResult<Vec<Option<Vec<u8>>>> {
    try_join_all(keys.iter().map(|key| async move {
        trx.get(&kv.namespaced_key(key), false)
            .await
            .map_err(map_fdb_error)
            .map(|value| value.map(|bytes| bytes.deref().to_vec()))
    }))
    .await
}

async fn delete_file_from_timeline_item(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    data_file_id: DataFileId,
    item: &RangeItem,
) -> CatalogResult<DeleteFileRow> {
    match DeleteFileRow::decode(&item.value) {
        Ok(mut row) => {
            let timeline_order = delete_file_timeline_order_from_key(
                catalog,
                data_file_id,
                &item.key,
                row.validity.begin_order.kind(),
            )?;
            if row.validity.begin_order == incomplete_order() {
                row.validity.begin_order = timeline_order;
            }
            if row.max_partial_order == Some(incomplete_order()) {
                row.max_partial_order = Some(timeline_order);
            }
            Ok(row)
        }
        Err(_) => {
            let delete_file_id = decode_delete_file_id(&item.value)?;
            let Some(value) = trx
                .get(
                    &kv.namespaced_key(&delete_file_key(catalog, delete_file_id)),
                    false,
                )
                .await
                .map_err(map_fdb_error)?
            else {
                return Err(CatalogError::NotFound("delete file"));
            };
            DeleteFileRow::decode(value.deref())
        }
    }
}

async fn current_delete_file_from_index_value(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<Option<DeleteFileRow>> {
    match DeleteFileRow::decode(value) {
        Ok(row) => Ok(row.validity.end_order.is_none().then_some(row)),
        Err(_) => {
            let delete_file_id = decode_delete_file_id(value)?;
            let Some(value) = trx
                .get(
                    &kv.namespaced_key(&delete_file_key(catalog, delete_file_id)),
                    false,
                )
                .await
                .map_err(map_fdb_error)?
            else {
                return Ok(None);
            };
            let row = DeleteFileRow::decode(value.deref())?;
            Ok(row.validity.end_order.is_none().then_some(row))
        }
    }
}

fn delete_file_timeline_order_from_key(
    catalog: CatalogId,
    data_file_id: DataFileId,
    key: &[u8],
    kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = delete_file_timeline_prefix(catalog, data_file_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "delete file timeline key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "delete file timeline key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            CatalogError::InvalidKey("delete file timeline order is truncated".to_owned())
        })?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

fn data_file_begin_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = data_file_begin_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "data file begin key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "data file begin key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] = tail[..CatalogOrderId::LEN]
        .try_into()
        .map_err(|_| CatalogError::InvalidKey("data file begin order is truncated".to_owned()))?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedDataFileRead {
    pub data_files: Vec<DataFileRow>,
    pub scan_transaction_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedDataFileCount {
    pub count: usize,
    pub scan_transaction_count: usize,
}

struct BoundedScan {
    items: Vec<RangeItem>,
    transaction_count: usize,
}

#[derive(Clone, Copy)]
enum ScanLimit {
    All,
    AtMost(usize),
}

impl ScanLimit {
    fn from_legacy(limit: usize) -> Self {
        if limit == usize::MAX {
            Self::All
        } else {
            Self::AtMost(limit)
        }
    }
}

struct ScanCursor {
    start: Vec<u8>,
    end: Vec<u8>,
    direction: RangeDirection,
    page_size: usize,
    remaining: Option<usize>,
    done: bool,
    items: Vec<RangeItem>,
}

impl ScanCursor {
    fn new(
        start: Vec<u8>,
        end: Vec<u8>,
        direction: RangeDirection,
        page_size: usize,
        limit: ScanLimit,
    ) -> CatalogResult<Self> {
        if page_size == 0 {
            return Err(CatalogError::InvalidMutation(
                "bounded async scan page size must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            start,
            end,
            direction,
            page_size,
            remaining: match limit {
                ScanLimit::All => None,
                ScanLimit::AtMost(limit) => Some(limit),
            },
            done: matches!(limit, ScanLimit::AtMost(0)),
            items: Vec::new(),
        })
    }

    fn wants_page(&self) -> bool {
        !self.done
    }

    fn page_size(&self) -> usize {
        self.remaining
            .map_or(self.page_size, |remaining| remaining.min(self.page_size))
    }

    fn start(&self) -> &[u8] {
        &self.start
    }

    fn end(&self) -> &[u8] {
        &self.end
    }

    fn into_items(self) -> Vec<RangeItem> {
        self.items
    }

    fn accept_page(&mut self, page: Vec<RangeItem>) -> CatalogResult<bool> {
        if page.is_empty() {
            self.done = true;
            return Ok(false);
        }
        let requested = self.page_size();
        if page.len() > requested || !page_is_ordered(&page, self.direction) {
            return Err(CatalogError::InvalidMutation(
                "foundationdb async scan page violated its ordering or size bound".to_owned(),
            ));
        }
        if let Some(previous) = self.items.last()
            && !keys_are_ordered(previous, &page[0], self.direction)
        {
            return Err(CatalogError::InvalidMutation(
                "foundationdb async scan continuation was not ordered".to_owned(),
            ));
        }
        let should_continue = page.len() == requested
            && self
                .remaining
                .is_none_or(|remaining| remaining > page.len());
        if let Some(remaining) = &mut self.remaining {
            *remaining -= page.len();
        }
        match self.direction {
            RangeDirection::Forward => self.start = key_after(&page[page.len() - 1].key),
            RangeDirection::Reverse => self.end = page[page.len() - 1].key.clone(),
        }
        self.items.extend(page);
        self.done = !should_continue;
        Ok(should_continue)
    }
}

fn page_is_ordered(page: &[RangeItem], direction: RangeDirection) -> bool {
    page.windows(2)
        .all(|pair| keys_are_ordered(&pair[0], &pair[1], direction))
}

fn keys_are_ordered(previous: &RangeItem, next: &RangeItem, direction: RangeDirection) -> bool {
    match direction {
        RangeDirection::Forward => previous.key < next.key,
        RangeDirection::Reverse => previous.key > next.key,
    }
}

struct ScanBudget {
    deadline: Instant,
    retries_remaining: usize,
    attempts: usize,
    last_retry_error: Option<CatalogError>,
}

impl ScanBudget {
    fn new() -> Self {
        Self::with_limits(MAX_FDB_READ_RETRIES, Instant::now() + ASYNC_SCAN_DEADLINE)
    }

    fn with_limits(retries: usize, deadline: Instant) -> Self {
        Self {
            deadline,
            retries_remaining: retries,
            attempts: 0,
            last_retry_error: None,
        }
    }

    fn begin_attempt(&mut self) -> CatalogResult<i32> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.exhausted_error());
        }
        self.attempts = self.attempts.saturating_add(1);
        let timeout_ms = remaining.as_millis().max(1);
        Ok(i32::try_from(timeout_ms).unwrap_or(i32::MAX))
    }

    fn record_retryable_failure(&mut self, error: CatalogError) -> CatalogResult<()> {
        crate::runtime_metrics::record_runtime_measurement("fdb.range.retry", 1, 0);
        self.last_retry_error = Some(error);
        if self.retries_remaining == 0 {
            return Err(self.exhausted_error());
        }
        self.retries_remaining -= 1;
        Ok(())
    }

    fn exhausted_error(&self) -> CatalogError {
        self.last_retry_error.as_ref().map_or_else(
            || {
                CatalogError::InvalidMutation(format!(
                    "foundationdb async range scan deadline exhausted after {} attempt(s)",
                    self.attempts
                ))
            },
            |error| scan_retry_exhausted(self.attempts, error.clone()),
        )
    }
}

fn scan_retry_exhausted(attempts: usize, error: CatalogError) -> CatalogError {
    match error {
        CatalogError::FoundationDb {
            code,
            message,
            class,
        } => CatalogError::FoundationDbRetryExhausted {
            operation: "scan_range",
            attempts,
            code,
            message,
            class,
        },
        other => other,
    }
}

fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    let bytes = bytes.try_into().map_err(|_| {
        CatalogError::Decode(format!(
            "data file id value must be 8 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(DataFileId(u64::from_be_bytes(bytes)))
}

fn decode_delete_file_id(bytes: &[u8]) -> CatalogResult<DeleteFileId> {
    let bytes = bytes.try_into().map_err(|_| {
        CatalogError::Decode(format!(
            "delete file id value must be 8 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(DeleteFileId(u64::from_be_bytes(bytes)))
}

fn key_after(key: &[u8]) -> Vec<u8> {
    let mut out = key.to_vec();
    out.push(0);
    out
}

#[cfg(test)]
mod fdb_async_read_tests;
