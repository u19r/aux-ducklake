use std::{
    ops::Deref,
    time::{Duration, Instant},
};

use foundationdb::{RangeOption, options::TransactionOption};
use futures::{TryStreamExt, executor::block_on};

use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileCleanupRow,
    DeleteFileId, FdbOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow,
    InlineTableCleanupId, InlineTableCleanupRow, RangeDirection,
    fdb_async::{MAX_FDB_READ_RETRIES, is_retryable_fdb_read_error},
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    file_partitions::remove_cached_file_partition_values,
    file_stats::remove_cached_file_column_stats_for_data_file,
    inline_data::{decode_inline_table_item, inline_table_payload_prefix},
    keys::{
        KeyFamily, current_data_file_key, data_file_begin_key, data_file_end_key, data_file_key,
        delete_file_end_key, delete_file_key, delete_file_timeline_key, family_prefix,
        file_column_stats_lookup_key, file_partition_value_prefix, inline_table_end_key,
        partition_value_lookup_key, prefix_end, scheduled_data_file_cleanup_key,
        scheduled_delete_file_cleanup_key,
    },
    kv::{OrderedCatalogKv, RangeItem},
    maintenance::{
        delete_file_cleanup_is_allowed, list_old_data_files_for_cleanup,
        list_old_delete_files_for_cleanup, list_old_inline_table_payloads_for_cleanup,
        row_is_unreachable,
    },
    store::list_snapshots,
};

impl FdbOrderedCatalogKv {
    pub fn remove_old_data_files_checked(
        &self,
        catalog: CatalogId,
        data_file_ids: &[DataFileId],
    ) -> CatalogResult<Vec<DataFileRow>> {
        let requested = requested_ids(data_file_ids);
        let snapshots = list_snapshots(self, catalog)?;
        let mut removed = Vec::new();
        for row in list_old_data_files_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.data_file_id.0))
        {
            let is_scheduled = self.is_scheduled_data_file_cleanup(catalog, row.data_file_id)?;
            if row_is_unreachable(&row, &snapshots) {
                self.remove_data_file_metadata(catalog, &row)?;
            }
            if is_scheduled {
                self.clear_scheduled_data_file_cleanup(catalog, row.data_file_id)?;
            }
            if row_is_unreachable(&row, &snapshots) || is_scheduled {
                removed.push(row);
            }
        }
        Ok(removed)
    }

    pub fn remove_old_delete_files_checked(
        &self,
        catalog: CatalogId,
        delete_file_ids: &[DeleteFileId],
    ) -> CatalogResult<Vec<DeleteFileCleanupRow>> {
        let requested = requested_ids(delete_file_ids);
        let snapshots = list_snapshots(self, catalog)?;
        let mut removed = Vec::new();
        for row in list_old_delete_files_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.delete_file.delete_file_id.0))
        {
            if !delete_file_cleanup_is_allowed(self, catalog, &row.delete_file, &snapshots)? {
                continue;
            }
            self.remove_delete_file_metadata(catalog, &row)?;
            if self.is_scheduled_delete_file_cleanup(catalog, row.delete_file.delete_file_id)? {
                self.clear_scheduled_delete_file_cleanup(catalog, row.delete_file.delete_file_id)?;
            }
            removed.push(row);
        }
        Ok(removed)
    }

    pub fn remove_old_inline_table_payloads_checked(
        &self,
        catalog: CatalogId,
        inline_ids: &[InlineTableCleanupId],
    ) -> CatalogResult<Vec<InlineTableCleanupRow>> {
        let requested = inline_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut removed = Vec::new();
        for row in list_old_inline_table_payloads_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.id))
        {
            if self.remove_inline_payload_if_unchanged(catalog, &row)? {
                removed.push(row);
            }
        }
        Ok(removed)
    }

    fn remove_data_file_metadata(
        &self,
        catalog: CatalogId,
        row: &DataFileRow,
    ) -> CatalogResult<()> {
        let mut budget = SyncFdbScanBudget::new();
        let partition_prefix = file_partition_value_prefix(catalog, row.data_file_id);
        let partition_rows = scan_prefix_all_with_budget(self, &partition_prefix, &mut budget)?;
        let stats_prefix = file_column_stats_prefix(catalog, row.data_file_id);
        let stats_rows = scan_prefix_all_with_budget(self, &stats_prefix, &mut budget)?;
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&data_file_key(catalog, row.data_file_id)));
        trx.clear(&self.namespaced_key(&data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        )));
        if let Some(end_order) = row.validity.end_order {
            trx.clear(&self.namespaced_key(&data_file_end_key(
                catalog,
                row.table_id,
                end_order,
                row.data_file_id,
            )));
        }
        trx.clear(&self.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        )));
        for item in partition_rows {
            let partition = FilePartitionValueRow::decode(&item.value)?;
            trx.clear(&self.namespaced_key(&item.key));
            trx.clear(&self.namespaced_key(&partition_value_lookup_key(
                catalog,
                partition.table_id,
                partition.partition_key_index,
                &partition.partition_value,
                partition.data_file_id,
            )));
        }
        for item in stats_rows {
            let stats = FileColumnStatsRow::decode(&item.value)?;
            trx.clear(&self.namespaced_key(&item.key));
            trx.clear(&self.namespaced_key(&file_column_stats_lookup_key(
                catalog,
                stats.table_id,
                stats.column_id,
                stats.data_file_id,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        crate::immutable_file_metadata::remove_immutable_data_file_metadata(
            self,
            catalog,
            row.data_file_id,
        );
        remove_cached_file_partition_values(self, catalog, row.data_file_id);
        remove_cached_file_column_stats_for_data_file(self, catalog, row.data_file_id);
        Ok(())
    }

    fn is_scheduled_data_file_cleanup(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<bool> {
        let key = scheduled_data_file_cleanup_key(catalog, data_file_id);
        Ok(self.get(&key)?.is_some())
    }

    fn clear_scheduled_data_file_cleanup(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<()> {
        let key = scheduled_data_file_cleanup_key(catalog, data_file_id);
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&key));
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn is_scheduled_delete_file_cleanup(
        &self,
        catalog: CatalogId,
        delete_file_id: DeleteFileId,
    ) -> CatalogResult<bool> {
        let key = scheduled_delete_file_cleanup_key(catalog, delete_file_id);
        Ok(self.get(&key)?.is_some())
    }

    fn clear_scheduled_delete_file_cleanup(
        &self,
        catalog: CatalogId,
        delete_file_id: DeleteFileId,
    ) -> CatalogResult<()> {
        let key = scheduled_delete_file_cleanup_key(catalog, delete_file_id);
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&key));
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn remove_delete_file_metadata(
        &self,
        catalog: CatalogId,
        row: &DeleteFileCleanupRow,
    ) -> CatalogResult<()> {
        let delete_file = &row.delete_file;
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&delete_file_key(catalog, delete_file.delete_file_id)));
        trx.clear(&self.namespaced_key(&delete_file_timeline_key(
            catalog,
            delete_file.data_file_id,
            delete_file.validity.begin_order,
            delete_file.delete_file_id,
        )));
        if let Some(end_order) = delete_file.validity.end_order {
            trx.clear(&self.namespaced_key(&delete_file_end_key(
                catalog,
                row.table_id,
                end_order,
                delete_file.delete_file_id,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn remove_inline_payload_if_unchanged(
        &self,
        catalog: CatalogId,
        row: &InlineTableCleanupRow,
    ) -> CatalogResult<bool> {
        let prefix = inline_table_payload_prefix(
            catalog,
            row.id.table_id,
            row.id.schema_id,
            row.id.begin_order,
        );
        let mut budget = SyncFdbScanBudget::new();
        let chunks = scan_prefix_all_with_budget(self, &prefix, &mut budget)?;
        if chunks.len() != row.chunk_count {
            return Ok(false);
        }
        let trx = self.create_transaction()?;
        let mut end_order = None;
        for chunk in &chunks {
            if !self.trx_range_item_matches(&trx, chunk)? {
                return Ok(false);
            }
            let decoded = decode_inline_table_item(catalog, &chunk.key, &chunk.value)?;
            end_order = decoded.validity.end_order;
            trx.clear(&self.namespaced_key(&chunk.key));
        }
        if let Some(order) = end_order {
            trx.clear(&self.namespaced_key(&inline_table_end_key(
                catalog,
                row.id.table_id,
                order,
                row.id.schema_id,
                row.id.begin_order,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(true)
    }

    fn trx_range_item_matches(
        &self,
        trx: &foundationdb::Transaction,
        item: &RangeItem,
    ) -> CatalogResult<bool> {
        self.trx_value_matches(trx, &item.key, &item.value)
    }

    fn trx_value_matches(
        &self,
        trx: &foundationdb::Transaction,
        key: &[u8],
        expected: &[u8],
    ) -> CatalogResult<bool> {
        let actual = block_on(trx.get(&self.namespaced_key(key), false))
            .map_err(map_fdb_error)?
            .map(|value| value.deref().to_vec());
        Ok(actual.as_deref() == Some(expected))
    }
}

fn requested_ids<T: Copy + IntoId>(ids: &[T]) -> std::collections::BTreeSet<u64> {
    ids.iter().map(|id| id.into_id()).collect()
}

trait IntoId {
    fn into_id(self) -> u64;
}

impl IntoId for DataFileId {
    fn into_id(self) -> u64 {
        self.0
    }
}

impl IntoId for DeleteFileId {
    fn into_id(self) -> u64 {
        self.0
    }
}

fn file_column_stats_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut prefix = family_prefix(catalog, KeyFamily::FileColumnStats);
    prefix.extend_from_slice(&data_file_id.0.to_be_bytes());
    prefix.push(b'/');
    prefix
}

const DEFAULT_SYNC_FDB_SCAN_PAGE_SIZE: usize = 256;
const SYNC_FDB_SCAN_DEADLINE: Duration = Duration::from_secs(5);
const SYNC_FDB_SCAN_TIMEOUT_MS: i32 = 5_000;

pub(crate) struct SyncFdbScanRequest {
    start: Vec<u8>,
    end: Vec<u8>,
    direction: RangeDirection,
    limit: usize,
}

impl SyncFdbScanRequest {
    #[must_use]
    pub(crate) fn new(start: &[u8], end: &[u8], direction: RangeDirection) -> Self {
        Self::with_limit(start, end, direction, usize::MAX)
    }

    #[must_use]
    pub(crate) fn with_limit(
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> Self {
        Self {
            start: start.to_vec(),
            end: end.to_vec(),
            direction,
            limit,
        }
    }
}

pub(crate) struct SyncFdbScanBudget {
    operation: &'static str,
    deadline: Instant,
    retries_remaining: usize,
    attempts: usize,
    last_retry_error: Option<CatalogError>,
    key_family: &'static str,
    table_schema: Option<(u64, u64)>,
    range: SyncFdbScanProgress,
}

#[derive(Default)]
struct SyncFdbScanProgress {
    attempts: usize,
    successful_pages: usize,
    items: usize,
    bytes: usize,
    retryable_failures: usize,
    last_fdb: Option<(i32, crate::error::FoundationDbErrorClass)>,
}

impl SyncFdbScanBudget {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::new_for_operation("scan_range")
    }

    #[must_use]
    pub(crate) fn new_for_operation(operation: &'static str) -> Self {
        Self::with_operation_limits(
            operation,
            MAX_FDB_READ_RETRIES,
            Instant::now() + SYNC_FDB_SCAN_DEADLINE,
        )
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn with_limits(retries: usize, deadline: Instant) -> Self {
        Self::with_operation_limits("scan_range", retries, deadline)
    }

    #[must_use]
    pub(crate) fn with_operation_limits(
        operation: &'static str,
        retries: usize,
        deadline: Instant,
    ) -> Self {
        Self {
            operation,
            deadline,
            retries_remaining: retries,
            attempts: 0,
            last_retry_error: None,
            key_family: "unknown",
            table_schema: None,
            range: SyncFdbScanProgress::default(),
        }
    }

    // Retry/deadline limits span the operation; diagnostics describe only this
    // range.
    fn observe_range(&mut self, start: &[u8]) {
        self.range = SyncFdbScanProgress::default();
        let family_offset = std::mem::size_of::<u64>() + 1;
        let family = if start.get(family_offset - 1) == Some(&b'/')
            && start.get(family_offset + 1) == Some(&b'/')
        {
            start
                .get(family_offset)
                .and_then(|code| KeyFamily::from_code(*code).ok())
        } else {
            None
        };
        self.key_family = family.map_or("unknown", KeyFamily::label);
        self.table_schema = family.and_then(|family| {
            if family != KeyFamily::InlineTable {
                return None;
            }
            let table_start = family_prefix(CatalogId(0), family).len();
            let schema_start = table_start + std::mem::size_of::<u64>() + 1;
            if start.get(table_start - 1) != Some(&b'/')
                || start.get(schema_start - 1) != Some(&b'/')
                || start.get(schema_start + std::mem::size_of::<u64>()) != Some(&b'/')
            {
                return None;
            }
            Some((
                u64::from_be_bytes(start.get(table_start..table_start + 8)?.try_into().ok()?),
                u64::from_be_bytes(start.get(schema_start..schema_start + 8)?.try_into().ok()?),
            ))
        });
    }

    fn record_successful_page(&mut self, items: usize, bytes: usize) {
        self.range.successful_pages = self.range.successful_pages.saturating_add(1);
        self.range.items = self.range.items.saturating_add(items);
        self.range.bytes = self.range.bytes.saturating_add(bytes);
    }

    fn begin_attempt(&mut self) -> CatalogResult<i32> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.deadline_error());
        }
        self.attempts = self.attempts.saturating_add(1);
        self.range.attempts = self.range.attempts.saturating_add(1);
        let timeout_ms = remaining
            .as_millis()
            .min(u128::from(SYNC_FDB_SCAN_TIMEOUT_MS as u32))
            .max(1);
        Ok(i32::try_from(timeout_ms).unwrap_or(SYNC_FDB_SCAN_TIMEOUT_MS))
    }

    fn record_retryable_failure(&mut self, error: CatalogError) -> CatalogResult<()> {
        self.range.retryable_failures = self.range.retryable_failures.saturating_add(1);
        self.range.last_fdb = match &error {
            CatalogError::FoundationDb { code, class, .. }
            | CatalogError::FoundationDbRetryExhausted { code, class, .. } => Some((*code, *class)),
            _ => None,
        };
        self.last_retry_error = Some(error);
        if self.retries_remaining == 0 {
            return Err(self.exhausted_error());
        }
        self.retries_remaining -= 1;
        Ok(())
    }

    fn exhausted_error(&self) -> CatalogError {
        self.last_retry_error.as_ref().map_or_else(
            || self.deadline_error(),
            |error| scan_retry_exhausted(self.operation, self.attempts, error.clone()),
        )
    }

    fn deadline_error(&self) -> CatalogError {
        CatalogError::InvalidMutation(format!(
            "foundationdb sync range scan deadline exhausted for {}: family={} table_schema={:?} \
             total_attempts={} current_range_attempts={} successful_pages={} items={} bytes={} \
             retryable_failures={} last_fdb={:?}",
            self.operation,
            self.key_family,
            self.table_schema,
            self.attempts,
            self.range.attempts,
            self.range.successful_pages,
            self.range.items,
            self.range.bytes,
            self.range.retryable_failures,
            self.range.last_fdb,
        ))
    }
}

pub(crate) struct SyncFdbScanCursor {
    start: Vec<u8>,
    end: Vec<u8>,
    direction: RangeDirection,
    page_size: usize,
    remaining: Option<usize>,
    done: bool,
    items: Vec<RangeItem>,
}

impl SyncFdbScanCursor {
    #[cfg(test)]
    pub(crate) fn try_new(
        start: Vec<u8>,
        end: Vec<u8>,
        direction: RangeDirection,
        page_size: usize,
    ) -> CatalogResult<Self> {
        Self::try_new_with_limit(start, end, direction, page_size, usize::MAX)
    }

    pub(crate) fn try_new_with_limit(
        start: Vec<u8>,
        end: Vec<u8>,
        direction: RangeDirection,
        page_size: usize,
        limit: usize,
    ) -> CatalogResult<Self> {
        if page_size == 0 {
            return Err(CatalogError::InvalidMutation(
                "bounded sync scan page size must be greater than zero".to_owned(),
            ));
        }
        if start > end {
            return Err(CatalogError::InvalidMutation(
                "bounded sync scan start must precede its end".to_owned(),
            ));
        }
        let empty = start == end;
        Ok(Self {
            start,
            end,
            direction,
            page_size,
            remaining: (limit != usize::MAX).then_some(limit),
            done: limit == 0 || empty,
            items: Vec::new(),
        })
    }

    fn wants_page(&self) -> bool {
        !self.done
    }

    fn start(&self) -> &[u8] {
        &self.start
    }

    fn end(&self) -> &[u8] {
        &self.end
    }

    fn page_size(&self) -> usize {
        self.remaining
            .map_or(self.page_size, |remaining| remaining.min(self.page_size))
    }

    fn into_items(self) -> Vec<RangeItem> {
        self.items
    }

    fn accept_page(&mut self, page: Vec<RangeItem>) -> CatalogResult<()> {
        if page.is_empty() {
            self.done = true;
            return Ok(());
        }
        let requested = self.page_size();
        if page.len() > requested
            || !page_is_ordered(&page, self.direction)
            || page
                .iter()
                .any(|item| !key_is_in_range(&item.key, &self.start, &self.end))
        {
            return Err(CatalogError::InvalidMutation(
                "foundationdb sync scan page violated its ordering or size bound".to_owned(),
            ));
        }
        if let Some(previous) = self.items.last()
            && !keys_are_ordered(previous, &page[0], self.direction)
        {
            return Err(CatalogError::InvalidMutation(
                "foundationdb sync scan continuation was not ordered".to_owned(),
            ));
        }
        let last_key = &page[page.len() - 1].key;
        match self.direction {
            RangeDirection::Forward => {
                let next_start = key_after(last_key);
                if next_start <= self.start {
                    return Err(CatalogError::InvalidMutation(
                        "foundationdb sync scan continuation did not advance".to_owned(),
                    ));
                }
            }
            RangeDirection::Reverse => {
                if last_key >= &self.end {
                    return Err(CatalogError::InvalidMutation(
                        "foundationdb sync scan continuation did not advance".to_owned(),
                    ));
                }
            }
        }
        let should_continue = page.len() == requested
            && self
                .remaining
                .is_none_or(|remaining| remaining > page.len());
        if let Some(remaining) = &mut self.remaining {
            *remaining -= page.len();
        }
        match self.direction {
            RangeDirection::Forward => {
                self.start = key_after(last_key);
            }
            RangeDirection::Reverse => {
                self.end = last_key.clone();
            }
        }
        self.items.extend(page);
        self.done = !should_continue;
        Ok(())
    }
}

pub(crate) fn scan_pages<Read>(
    mut cursor: SyncFdbScanCursor,
    budget: &mut SyncFdbScanBudget,
    mut read_page: Read,
) -> CatalogResult<Vec<RangeItem>>
where
    Read: FnMut(&SyncFdbScanCursor, usize, i32) -> CatalogResult<Vec<RangeItem>>,
{
    while cursor.wants_page() {
        let page = loop {
            let timeout_ms = budget.begin_attempt()?;
            match read_page(&cursor, cursor.page_size(), timeout_ms) {
                Ok(page) => break page,
                Err(error) if is_retryable_fdb_read_error(&error) => {
                    budget.record_retryable_failure(error)?;
                }
                Err(error) => return Err(error),
            }
        };
        let items = page.len();
        let bytes = page
            .iter()
            .map(|item| item.key.len().saturating_add(item.value.len()))
            .sum();
        cursor.accept_page(page)?;
        budget.record_successful_page(items, bytes);
    }
    Ok(cursor.into_items())
}

pub(crate) fn scan_range_all_with_budget(
    kv: &FdbOrderedCatalogKv,
    request: SyncFdbScanRequest,
    budget: &mut SyncFdbScanBudget,
) -> CatalogResult<Vec<RangeItem>> {
    budget.observe_range(&request.start);
    let cursor = SyncFdbScanCursor::try_new_with_limit(
        request.start,
        request.end,
        request.direction,
        DEFAULT_SYNC_FDB_SCAN_PAGE_SIZE,
        request.limit,
    )?;
    scan_pages(cursor, budget, |cursor, page_size, timeout_ms| {
        scan_range_page(kv, cursor, page_size, timeout_ms)
    })
}

#[cfg(test)]
pub(crate) fn scan_range_all_with_kv_budget(
    kv: &impl OrderedCatalogKv,
    request: SyncFdbScanRequest,
    budget: &mut SyncFdbScanBudget,
) -> CatalogResult<Vec<RangeItem>> {
    let direction = request.direction;
    budget.observe_range(&request.start);
    let cursor = SyncFdbScanCursor::try_new_with_limit(
        request.start,
        request.end,
        direction,
        DEFAULT_SYNC_FDB_SCAN_PAGE_SIZE,
        request.limit,
    )?;
    scan_pages(cursor, budget, |cursor, page_size, _timeout_ms| {
        kv.scan_range(cursor.start(), cursor.end(), direction, page_size)
    })
}

fn scan_prefix_all_with_budget(
    kv: &FdbOrderedCatalogKv,
    prefix: &[u8],
    budget: &mut SyncFdbScanBudget,
) -> CatalogResult<Vec<RangeItem>> {
    let end = prefix_end(prefix);
    scan_range_all_with_budget(
        kv,
        SyncFdbScanRequest::new(prefix, &end, RangeDirection::Forward),
        budget,
    )
}

fn scan_range_page(
    kv: &FdbOrderedCatalogKv,
    cursor: &SyncFdbScanCursor,
    page_size: usize,
    timeout_ms: i32,
) -> CatalogResult<Vec<RangeItem>> {
    let trx = kv.create_transaction()?;
    trx.set_option(TransactionOption::Timeout(i64::from(timeout_ms)))
        .map_err(map_fdb_error)?;
    let mut options =
        RangeOption::from(kv.namespaced_key(cursor.start())..kv.namespaced_key(cursor.end()));
    options.limit = Some(page_size);
    if cursor.direction == RangeDirection::Reverse {
        options = options.rev();
    }
    let values = block_on(
        trx.get_ranges_keyvalues(options, false)
            .try_collect::<Vec<_>>(),
    )
    .map_err(map_fdb_error)?;
    values
        .into_iter()
        .map(|item| {
            Ok(RangeItem {
                key: kv.strip_namespace(item.key())?,
                value: item.value().to_vec(),
            })
        })
        .collect()
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

fn key_is_in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    key >= start && key < end
}

fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len().saturating_add(1));
    next.extend_from_slice(key);
    next.push(0);
    next
}

fn scan_retry_exhausted(
    operation: &'static str,
    attempts: usize,
    error: CatalogError,
) -> CatalogError {
    match error {
        CatalogError::FoundationDb {
            code,
            message,
            class,
        } => CatalogError::FoundationDbRetryExhausted {
            operation,
            attempts,
            code,
            message,
            class,
        },
        other => other,
    }
}

#[cfg(test)]
#[path = "fdb_cleanup_tests.rs"]
mod fdb_cleanup_tests;
