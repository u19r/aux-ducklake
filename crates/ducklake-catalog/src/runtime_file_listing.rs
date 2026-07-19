use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    AttachedDataFile, CatalogId, CatalogOrderId, CatalogResult, DataFileId, DuckLakeSnapshotId,
    OrderedCatalogKv, PartitionKeyIndex, SchemaId, SnapshotRow, TableId,
    inline_data::{
        inline_table_all_end_orders, list_inline_file_deletions_for_data_files_at,
        list_unflushed_inline_table_payloads_at_with_end_orders,
    },
    list_all_snapshots, load_table_at,
    runtime_snapshots::{
        SharedOrderMap, SnapshotReadContext, public_snapshot_sequences_by_order_containing,
        snapshot_schema_version, snapshot_schema_versions_by_order_shared,
    },
};

#[cfg(feature = "foundationdb")]
use crate::data_file_store::without_backfilled_source_duplicates;
#[cfg(feature = "foundationdb")]
use crate::file_partitions::{
    PartitionScanFilter, list_current_data_files_for_partition_scan,
    list_partition_lookup_values_for_key,
};
#[cfg(feature = "foundationdb")]
use crate::latest_snapshot;
#[cfg(test)]
use crate::list_data_files_with_deletes_at;
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
#[cfg(feature = "runtime-metrics")]
use crate::{
    runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed},
    runtime_protocol::RuntimeCatalogBackend,
};
#[cfg(feature = "foundationdb")]
use futures::executor::block_on;

#[derive(Clone, Copy)]
pub(crate) struct ListDataFilesAtPayload {
    pub(crate) snapshot_id: u64,
    pub(crate) table_id: TableId,
}

#[derive(Clone)]
pub(crate) struct CurrentPartitionFilesPayload {
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) partition_value: String,
}

#[derive(Clone)]
pub(crate) struct CurrentPartitionFilesBatchPayload {
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) partition_values: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct PartitionFilesAtPayload {
    pub(crate) snapshot_id: u64,
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) partition_value: String,
}

#[derive(Clone)]
pub(crate) struct PartitionFilesAtBatchPayload {
    pub(crate) snapshot_id: u64,
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) partition_values: Vec<String>,
}

#[derive(Clone, Copy)]
pub(crate) enum PartitionPruneComparison {
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Clone)]
pub(crate) struct CurrentPartitionPruneFilesPayload {
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) column_type: String,
    pub(crate) comparison: PartitionPruneComparison,
    pub(crate) partition_value: String,
}

#[derive(Clone)]
pub(crate) struct PartitionPruneFilesAtPayload {
    pub(crate) snapshot_id: u64,
    pub(crate) table_id: TableId,
    pub(crate) partition_key_index: PartitionKeyIndex,
    pub(crate) column_type: String,
    pub(crate) comparison: PartitionPruneComparison,
    pub(crate) partition_value: String,
}

#[cfg(test)]
pub(crate) fn data_files_at_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ListDataFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let mut request = FileListingRequestContext::for_current_catalog(kv, catalog)?;
    let snapshot = request.resolve_snapshot(kv, catalog, payload.snapshot_id)?;
    let files = suppress_unflushed_inline_materialization_files(
        kv,
        catalog,
        payload.table_id,
        snapshot.order,
        list_data_files_with_deletes_at(kv, catalog, payload.table_id, snapshot.order)?,
    )?;
    let mut out = String::new();
    push_files_payload(
        &mut out,
        kv,
        catalog,
        snapshot.order,
        files,
        Some(&mut request),
    )?;
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_data_files_at_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: ListDataFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let started = RuntimeMetricStage::start();
    #[cfg(not(test))]
    let cache_key = FileListingPayloadCacheKey::data_files_at(
        kv.catalog_cache_namespace(),
        catalog,
        payload.table_id,
        payload.snapshot_id,
    );
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled()
        && let Some(payload) = file_listing_payload_cache().get_ref(&cache_key)
    {
        return Ok(payload);
    }
    let mut request = FileListingRequestContext::for_current_catalog(kv, catalog)?;
    let snapshot = request.resolve_snapshot(kv, catalog, payload.snapshot_id)?;
    record_list_data_files_at_stage("Snapshot", started);
    let started = RuntimeMetricStage::start();
    let files = foundationdb_visible_data_files_at(kv, catalog, payload.table_id, snapshot.order)?;
    record_list_data_files_at_stage("ScanAndDedupe", started);
    let started = RuntimeMetricStage::start();
    let files = foundationdb_attached_visible_data_files_at(
        kv,
        catalog,
        payload.table_id,
        snapshot.order,
        files,
    )?;
    record_list_data_files_at_stage("AttachDeletes", started);
    let started = RuntimeMetricStage::start();
    let files = suppress_unflushed_inline_materialization_files(
        kv,
        catalog,
        payload.table_id,
        snapshot.order,
        files,
    )?;
    record_list_data_files_at_stage("InlineSuppression", started);
    let started = RuntimeMetricStage::start();
    let mut out = String::new();
    push_files_payload(
        &mut out,
        kv,
        catalog,
        snapshot.order,
        files,
        Some(&mut request),
    )?;
    record_list_data_files_at_stage("Render", started);
    let payload = out.into_bytes();
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        file_listing_payload_cache().insert(cache_key, payload.clone());
    }
    Ok(payload)
}

#[cfg(all(feature = "foundationdb", feature = "runtime-metrics"))]
fn record_list_data_files_at_stage(stage: &str, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!("ListDataFilesAtStage{stage}"),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[cfg(all(feature = "foundationdb", not(feature = "runtime-metrics")))]
#[inline]
fn record_list_data_files_at_stage(_stage: &str, _started: RuntimeMetricStage) {}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_current_partition_files_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: CurrentPartitionFilesPayload,
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(test))]
    let cache_key = FileListingPayloadCacheKey::current_partition_files(
        kv.catalog_cache_namespace(),
        catalog,
        payload.table_id,
        payload.partition_key_index,
        payload.partition_value.clone(),
    );
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled()
        && let Some(payload) = file_listing_payload_cache().get_ref(&cache_key)
    {
        return Ok(payload);
    }
    let snapshot = latest_snapshot(kv, catalog)?
        .ok_or_else(|| crate::CatalogError::Decode("catalog snapshot does not exist".to_owned()))?;
    let mut request = FileListingRequestContext::from_latest(Some(snapshot.clone()));
    let files = list_current_data_files_for_partition_scan(
        kv,
        catalog,
        payload.table_id,
        payload.partition_key_index,
        &payload.partition_value,
    )?;
    let files = block_on(kv.attach_current_delete_files_async(catalog, files, snapshot.order))?;
    let rendered = partition_files_payload_with_context(
        kv,
        catalog,
        snapshot.order,
        payload.partition_value,
        files,
        Some(&mut request),
    )?;
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        file_listing_payload_cache().insert(cache_key, rendered.clone());
    }
    Ok(rendered)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_current_partition_files_batch_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: CurrentPartitionFilesBatchPayload,
) -> CatalogResult<Vec<u8>> {
    let snapshot = latest_snapshot(kv, catalog)?
        .ok_or_else(|| crate::CatalogError::Decode("catalog snapshot does not exist".to_owned()))?;
    let mut request = FileListingRequestContext::from_latest(Some(snapshot.clone()));
    let mut out = String::new();
    for partition_value in payload.partition_values {
        let files = list_current_data_files_for_partition_scan(
            kv,
            catalog,
            payload.table_id,
            payload.partition_key_index,
            &partition_value,
        )?;
        let files = block_on(kv.attach_current_delete_files_async(catalog, files, snapshot.order))?;
        push_partition_files_payload_with_context(
            &mut out,
            kv,
            catalog,
            snapshot.order,
            &partition_value,
            files,
            Some(&mut request),
        )?;
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_current_partition_prune_files_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: CurrentPartitionPruneFilesPayload,
) -> CatalogResult<Vec<u8>> {
    let partition_values = matching_partition_values(
        kv,
        catalog,
        payload.table_id,
        payload.partition_key_index,
        &payload.column_type,
        payload.comparison,
        &payload.partition_value,
    )?;
    if partition_values.is_empty() {
        return Ok(Vec::new());
    }
    foundationdb_current_partition_files_batch_payload(
        kv,
        catalog,
        CurrentPartitionFilesBatchPayload {
            table_id: payload.table_id,
            partition_key_index: payload.partition_key_index,
            partition_values,
        },
    )
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_partition_files_at_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: PartitionFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(test))]
    let cache_key = FileListingPayloadCacheKey::partition_files_at(
        kv.catalog_cache_namespace(),
        catalog,
        payload.table_id,
        payload.snapshot_id,
        payload.partition_key_index,
        payload.partition_value.clone(),
    );
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled()
        && let Some(payload) = file_listing_payload_cache().get_ref(&cache_key)
    {
        return Ok(payload);
    }
    let mut request = FileListingRequestContext::for_current_catalog(kv, catalog)?;
    let snapshot = request.resolve_snapshot(kv, catalog, payload.snapshot_id)?;
    let filter = PartitionScanFilter::load(
        kv,
        catalog,
        payload.table_id,
        payload.partition_key_index,
        &payload.partition_value,
    )?;
    let files = match foundationdb_cached_attached_visible_data_files_at(
        kv,
        catalog,
        payload.table_id,
        snapshot.order,
    ) {
        Some(files) => files
            .into_iter()
            .filter(|attached| filter.includes(attached.data_file.data_file_id))
            .collect(),
        None => {
            let files =
                foundationdb_visible_data_files_at(kv, catalog, payload.table_id, snapshot.order)?
                    .into_iter()
                    .filter(|data_file| filter.includes(data_file.data_file_id))
                    .collect();
            block_on(kv.attach_delete_files_at_async(catalog, files, snapshot.order))?
        }
    };
    let rendered = partition_files_payload_with_context(
        kv,
        catalog,
        snapshot.order,
        payload.partition_value,
        files,
        Some(&mut request),
    )?;
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        file_listing_payload_cache().insert(cache_key, rendered.clone());
    }
    Ok(rendered)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_partition_files_at_batch_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: PartitionFilesAtBatchPayload,
) -> CatalogResult<Vec<u8>> {
    let mut request = FileListingRequestContext::for_current_catalog(kv, catalog)?;
    let snapshot = request.resolve_snapshot(kv, catalog, payload.snapshot_id)?;
    let attached_files = match foundationdb_cached_attached_visible_data_files_at(
        kv,
        catalog,
        payload.table_id,
        snapshot.order,
    ) {
        Some(files) => files,
        None => {
            let files =
                foundationdb_visible_data_files_at(kv, catalog, payload.table_id, snapshot.order)?;
            foundationdb_attached_visible_data_files_at(
                kv,
                catalog,
                payload.table_id,
                snapshot.order,
                files,
            )?
        }
    };
    let mut out = String::new();
    for partition_value in payload.partition_values {
        let filter = PartitionScanFilter::load(
            kv,
            catalog,
            payload.table_id,
            payload.partition_key_index,
            &partition_value,
        )?;
        let files = attached_files
            .iter()
            .filter(|attached| filter.includes(attached.data_file.data_file_id))
            .cloned()
            .collect();
        push_partition_files_payload_with_context(
            &mut out,
            kv,
            catalog,
            snapshot.order,
            &partition_value,
            files,
            Some(&mut request),
        )?;
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn foundationdb_partition_prune_files_at_payload(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    payload: PartitionPruneFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let partition_values = matching_partition_values(
        kv,
        catalog,
        payload.table_id,
        payload.partition_key_index,
        &payload.column_type,
        payload.comparison,
        &payload.partition_value,
    )?;
    if partition_values.is_empty() {
        return Ok(Vec::new());
    }
    foundationdb_partition_files_at_batch_payload(
        kv,
        catalog,
        PartitionFilesAtBatchPayload {
            snapshot_id: payload.snapshot_id,
            table_id: payload.table_id,
            partition_key_index: payload.partition_key_index,
            partition_values,
        },
    )
}

#[cfg(feature = "foundationdb")]
fn matching_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    column_type: &str,
    comparison: PartitionPruneComparison,
    partition_value: &str,
) -> CatalogResult<Vec<String>> {
    let mut values = BTreeSet::new();
    for row in list_partition_lookup_values_for_key(kv, catalog, table_id, partition_key_index)? {
        if partition_value_matches(
            &row.partition_value,
            column_type,
            comparison,
            partition_value,
        ) {
            values.insert(row.partition_value);
        }
    }
    Ok(values.into_iter().collect())
}

#[cfg(feature = "foundationdb")]
fn partition_value_matches(
    candidate: &str,
    column_type: &str,
    comparison: PartitionPruneComparison,
    boundary: &str,
) -> bool {
    if candidate == "__HIVE_DEFAULT_PARTITION__" {
        return false;
    }
    let normalized_type = column_type.to_ascii_uppercase();
    if normalized_type.contains("INT")
        && let (Ok(left), Ok(right)) = (candidate.parse::<i128>(), boundary.parse::<i128>())
    {
        return compare_values(left, right, comparison);
    }
    if (normalized_type.contains("FLOAT")
        || normalized_type.contains("DOUBLE")
        || normalized_type.contains("REAL")
        || normalized_type.contains("DECIMAL"))
        && let (Ok(left), Ok(right)) = (candidate.parse::<f64>(), boundary.parse::<f64>())
    {
        return compare_partial_values(left, right, comparison);
    }
    compare_values(candidate, boundary, comparison)
}

#[cfg(feature = "foundationdb")]
fn compare_values<T: Ord>(left: T, right: T, comparison: PartitionPruneComparison) -> bool {
    match comparison {
        PartitionPruneComparison::Equal => left == right,
        PartitionPruneComparison::GreaterThan => left > right,
        PartitionPruneComparison::GreaterThanOrEqual => left >= right,
        PartitionPruneComparison::LessThan => left < right,
        PartitionPruneComparison::LessThanOrEqual => left <= right,
    }
}

#[cfg(feature = "foundationdb")]
fn compare_partial_values<T: PartialOrd>(
    left: T,
    right: T,
    comparison: PartitionPruneComparison,
) -> bool {
    match comparison {
        PartitionPruneComparison::Equal => left == right,
        PartitionPruneComparison::GreaterThan => left > right,
        PartitionPruneComparison::GreaterThanOrEqual => left >= right,
        PartitionPruneComparison::LessThan => left < right,
        PartitionPruneComparison::LessThanOrEqual => left <= right,
    }
}

fn partition_files_payload_with_context(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    partition_value: String,
    files: Vec<AttachedDataFile>,
    request_context: Option<&mut FileListingRequestContext>,
) -> CatalogResult<Vec<u8>> {
    let mut out = String::new();
    push_partition_files_payload_with_context(
        &mut out,
        kv,
        catalog,
        snapshot_order,
        &partition_value,
        files,
        request_context,
    )?;
    Ok(out.into_bytes())
}

fn push_partition_files_payload_with_context(
    out: &mut String,
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    partition_value: &str,
    files: Vec<AttachedDataFile>,
    request_context: Option<&mut FileListingRequestContext>,
) -> CatalogResult<()> {
    out.push_str(&format!("partition_value={partition_value}\n"));
    push_files_payload(out, kv, catalog, snapshot_order, files, request_context)?;
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn foundationdb_visible_data_files_at(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<crate::DataFileRow>> {
    #[cfg(not(test))]
    let cache_key = VisibleDataFilesAtCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        table_id,
        snapshot_order,
    };
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled()
        && let Some(rows) = visible_data_files_at_cache().get(cache_key)
    {
        return Ok(rows);
    }

    let rows = without_backfilled_source_duplicates(
        kv,
        catalog,
        block_on(kv.list_data_files_at_async(catalog, table_id, snapshot_order))?,
    )?;

    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        visible_data_files_at_cache().insert(cache_key, rows.clone());
    }

    Ok(rows)
}

#[cfg(feature = "foundationdb")]
fn foundationdb_attached_visible_data_files_at(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    data_files: Vec<crate::DataFileRow>,
) -> CatalogResult<Vec<AttachedDataFile>> {
    #[cfg(test)]
    let _ = table_id;
    #[cfg(not(test))]
    let cache_key = VisibleDataFilesAtCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        table_id,
        snapshot_order,
    };
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled()
        && let Some(files) = attached_visible_data_files_at_cache().get(cache_key)
    {
        return Ok(files);
    }

    let attached = block_on(kv.attach_delete_files_at_async(catalog, data_files, snapshot_order))?;

    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        attached_visible_data_files_at_cache().insert(cache_key, attached.clone());
    }

    Ok(attached)
}

#[cfg(all(feature = "foundationdb", not(test)))]
fn foundationdb_cached_attached_visible_data_files_at(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> Option<Vec<AttachedDataFile>> {
    if !crate::store::runtime_read_context_enabled() {
        return None;
    }
    attached_visible_data_files_at_cache().get(VisibleDataFilesAtCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        table_id,
        snapshot_order,
    })
}

#[cfg(all(feature = "foundationdb", test))]
fn foundationdb_cached_attached_visible_data_files_at(
    _kv: &crate::FdbOrderedCatalogKv,
    _catalog: CatalogId,
    _table_id: TableId,
    _snapshot_order: CatalogOrderId,
) -> Option<Vec<AttachedDataFile>> {
    None
}

struct FileListingRequestContext {
    latest: Option<SnapshotRow>,
    snapshots: Option<SnapshotReadContext>,
}

impl FileListingRequestContext {
    fn for_current_catalog(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Self> {
        Ok(Self {
            latest: crate::latest_snapshot(kv, catalog)?,
            snapshots: None,
        })
    }

    fn from_latest(latest: Option<SnapshotRow>) -> Self {
        Self {
            latest,
            snapshots: None,
        }
    }

    fn resolve_snapshot(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot_id: u64,
    ) -> CatalogResult<SnapshotRow> {
        let snapshot_id = DuckLakeSnapshotId(snapshot_id);
        let latest = self.latest.clone();
        if let Some(snapshot) = latest
            .as_ref()
            .filter(|snapshot| snapshot_id.0 >= snapshot.sequence.0)
        {
            return Ok(snapshot.clone());
        }
        let snapshot_context = self.snapshot_context(kv, catalog)?;
        if let Some(snapshot) = snapshot_context.public_snapshot(snapshot_id).or_else(|| {
            latest_or_next_public_snapshot(latest.as_ref(), snapshot_context, snapshot_id)
        }) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = snapshot_context.ducklake_snapshot(snapshot_id) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.latest_or_next_raw_snapshot(snapshot_id) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = list_all_snapshots(kv, catalog)?
            .into_iter()
            .filter(|snapshot| snapshot.sequence.0 == snapshot_id.0)
            .max_by_key(|snapshot| snapshot.order)
        {
            return Ok(snapshot);
        }
        self.latest_committed_snapshot().ok_or_else(|| {
            crate::CatalogError::Decode(format!("snapshot {} does not exist", snapshot_id.0))
        })
    }

    fn render_mappings(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        required_orders: &BTreeSet<CatalogOrderId>,
    ) -> CatalogResult<(SharedOrderMap, SharedOrderMap)> {
        let context = self.snapshot_context(kv, catalog)?;
        let sequences = context.sequences_by_order();
        if public_snapshot_sequences_cover(&sequences, required_orders) {
            return Ok((sequences, context.schema_versions_by_order()));
        }
        Ok((
            public_snapshot_sequences_containing_orders(kv, catalog, required_orders)?,
            context.schema_versions_by_order(),
        ))
    }

    fn latest_or_next_raw_snapshot(&self, snapshot_id: DuckLakeSnapshotId) -> Option<SnapshotRow> {
        let latest = self.latest.clone()?;
        let latest_sequence = DuckLakeSnapshotId(latest.sequence.0);
        (snapshot_id >= latest_sequence).then_some(latest)
    }

    fn latest_committed_snapshot(&self) -> Option<SnapshotRow> {
        self.latest.clone()
    }

    fn snapshot_context(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<&SnapshotReadContext> {
        if self.snapshots.is_none() {
            self.snapshots = Some(SnapshotReadContext::for_current_catalog_uncached(
                kv, catalog,
            )?);
        }
        self.snapshots.as_ref().ok_or_else(|| {
            crate::CatalogError::Decode("file-listing snapshot context was not initialized".into())
        })
    }
}

#[cfg(not(test))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FileListingPayloadCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    kind: FileListingPayloadKind,
}

#[cfg(not(test))]
impl FileListingPayloadCacheKey {
    fn data_files_at(
        namespace: CatalogCacheNamespace,
        catalog: CatalogId,
        table_id: TableId,
        requested_snapshot_id: u64,
    ) -> Self {
        Self {
            namespace,
            catalog,
            table_id,
            kind: FileListingPayloadKind::DataFilesAt {
                requested_snapshot_id,
            },
        }
    }

    fn current_partition_files(
        namespace: CatalogCacheNamespace,
        catalog: CatalogId,
        table_id: TableId,
        partition_key_index: PartitionKeyIndex,
        partition_value: String,
    ) -> Self {
        Self {
            namespace,
            catalog,
            table_id,
            kind: FileListingPayloadKind::CurrentPartitionFiles {
                partition_key_index,
                partition_value,
            },
        }
    }

    fn partition_files_at(
        namespace: CatalogCacheNamespace,
        catalog: CatalogId,
        table_id: TableId,
        requested_snapshot_id: u64,
        partition_key_index: PartitionKeyIndex,
        partition_value: String,
    ) -> Self {
        Self {
            namespace,
            catalog,
            table_id,
            kind: FileListingPayloadKind::PartitionFilesAt {
                requested_snapshot_id,
                partition_key_index,
                partition_value,
            },
        }
    }
}

#[cfg(not(test))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum FileListingPayloadKind {
    DataFilesAt {
        requested_snapshot_id: u64,
    },
    CurrentPartitionFiles {
        partition_key_index: PartitionKeyIndex,
        partition_value: String,
    },
    PartitionFilesAt {
        requested_snapshot_id: u64,
        partition_key_index: PartitionKeyIndex,
        partition_value: String,
    },
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VisibleDataFilesAtCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static FILE_LISTING_PAYLOAD_CACHE: OnceLock<BoundedCache<FileListingPayloadCacheKey, Vec<u8>>> =
    OnceLock::new();

#[cfg(not(test))]
static VISIBLE_DATA_FILES_AT_CACHE: OnceLock<
    BoundedCache<VisibleDataFilesAtCacheKey, Vec<crate::DataFileRow>>,
> = OnceLock::new();

#[cfg(not(test))]
static ATTACHED_VISIBLE_DATA_FILES_AT_CACHE: OnceLock<
    BoundedCache<VisibleDataFilesAtCacheKey, Vec<AttachedDataFile>>,
> = OnceLock::new();

#[cfg(not(test))]
fn file_listing_payload_cache() -> &'static BoundedCache<FileListingPayloadCacheKey, Vec<u8>> {
    static_bounded_cache(&FILE_LISTING_PAYLOAD_CACHE, 1024)
}

#[cfg(not(test))]
fn visible_data_files_at_cache()
-> &'static BoundedCache<VisibleDataFilesAtCacheKey, Vec<crate::DataFileRow>> {
    static_bounded_cache(&VISIBLE_DATA_FILES_AT_CACHE, 512)
}

#[cfg(not(test))]
fn attached_visible_data_files_at_cache()
-> &'static BoundedCache<VisibleDataFilesAtCacheKey, Vec<AttachedDataFile>> {
    static_bounded_cache(&ATTACHED_VISIBLE_DATA_FILES_AT_CACHE, 512)
}

#[cfg(not(test))]
pub(crate) fn invalidate_file_listing_read_context(catalog: CatalogId) {
    if let Some(cache) = FILE_LISTING_PAYLOAD_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = VISIBLE_DATA_FILES_AT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = ATTACHED_VISIBLE_DATA_FILES_AT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_file_listing_read_context(_catalog: CatalogId) {}

fn latest_or_next_public_snapshot(
    latest: Option<&SnapshotRow>,
    snapshots: &SnapshotReadContext,
    snapshot_id: DuckLakeSnapshotId,
) -> Option<SnapshotRow> {
    let latest = latest.cloned()?;
    let latest_public_snapshot_id = snapshots.latest_public_snapshot_id()?;
    (snapshot_id > latest_public_snapshot_id).then_some(latest)
}

fn public_snapshot_sequences_containing_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    required_orders: &BTreeSet<CatalogOrderId>,
) -> CatalogResult<SharedOrderMap> {
    let Some(first_order) = required_orders.first().copied() else {
        return Err(crate::CatalogError::Decode(
            "file payload has no snapshot orders".into(),
        ));
    };
    let sequences = public_snapshot_sequences_by_order_containing(kv, catalog, first_order)?;
    if public_snapshot_sequences_cover(&sequences, required_orders) {
        return Ok(sequences);
    }
    let last_order = required_orders
        .last()
        .copied()
        .ok_or_else(|| crate::CatalogError::Decode("file payload has no snapshot orders".into()))?;
    public_snapshot_sequences_by_order_containing(kv, catalog, last_order)
}

fn public_snapshot_sequences_cover(
    sequences: &SharedOrderMap,
    required_orders: &BTreeSet<CatalogOrderId>,
) -> bool {
    required_orders
        .iter()
        .all(|order| sequences.get(order).is_some())
}

fn suppress_unflushed_inline_materialization_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
    files: Vec<AttachedDataFile>,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let started = RuntimeMetricStage::start();
    if files.is_empty() {
        record_runtime_method_stage(
            "method.runtime_file_listing.suppress_unflushed_inline_materialization_files",
            started,
        );
        return Ok(files);
    }
    let context =
        InlineSuppressionContext::for_table_snapshot(kv, catalog, table_id, snapshot_order)?;
    if context.is_empty() {
        record_runtime_method_stage(
            "method.runtime_file_listing.suppress_unflushed_inline_materialization_files",
            started,
        );
        return Ok(files);
    }
    let files = files
        .into_iter()
        .filter(|attached| {
            let file = &attached.data_file;
            !should_suppress_unflushed_inline_materialization_file(file, &context)
        })
        .collect();
    record_runtime_method_stage(
        "method.runtime_file_listing.suppress_unflushed_inline_materialization_files",
        started,
    );
    Ok(files)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileListingRole {
    OrdinaryFile,
    InlineMaterializationFile { begin_order: CatalogOrderId },
    BackfilledCompactionReplacement,
}

fn should_suppress_unflushed_inline_materialization_file(
    file: &crate::DataFileRow,
    context: &InlineSuppressionContext,
) -> bool {
    match file_listing_role(file) {
        FileListingRole::InlineMaterializationFile { begin_order } => {
            context.suppresses(begin_order)
        }
        FileListingRole::OrdinaryFile | FileListingRole::BackfilledCompactionReplacement => false,
    }
}

pub(crate) fn file_listing_role(file: &crate::DataFileRow) -> FileListingRole {
    let Some(max_partial_order) = file.max_partial_order else {
        return FileListingRole::OrdinaryFile;
    };
    if file.row_id_start_known {
        return FileListingRole::OrdinaryFile;
    }
    if max_partial_order != file.validity.begin_order {
        return FileListingRole::BackfilledCompactionReplacement;
    }
    FileListingRole::InlineMaterializationFile {
        begin_order: file.validity.begin_order,
    }
}

#[derive(Clone)]
struct InlineSuppressionContext {
    unmaterialized_inline_begin_orders: Vec<CatalogOrderId>,
}

impl InlineSuppressionContext {
    fn for_table_snapshot(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = InlineSuppressionContextKey {
                namespace: kv.catalog_cache_namespace(),
                catalog,
                table_id,
                snapshot_order,
            };
            let cache = inline_suppression_context_cache();
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

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let Some(table) = load_table_at(kv, catalog, table_id, snapshot_order)? else {
            return Ok(Self {
                unmaterialized_inline_begin_orders: Vec::new(),
            });
        };
        let mut unmaterialized_inline_begin_orders = Vec::new();
        for inlined_table in &table.inlined_data_tables {
            let schema_id = SchemaId(inlined_table.schema_version);
            let ended_begin_orders =
                inline_table_all_end_orders(kv, catalog, table_id, schema_id, snapshot_order)?;
            for payload in list_unflushed_inline_table_payloads_at_with_end_orders(
                kv,
                catalog,
                table_id,
                schema_id,
                snapshot_order,
                &ended_begin_orders,
            )? {
                if !ended_begin_orders.contains_key(&payload.begin_order) {
                    unmaterialized_inline_begin_orders.push(payload.begin_order);
                }
            }
        }
        unmaterialized_inline_begin_orders.sort_unstable();
        unmaterialized_inline_begin_orders.dedup();
        Ok(Self {
            unmaterialized_inline_begin_orders,
        })
    }

    fn is_empty(&self) -> bool {
        self.unmaterialized_inline_begin_orders.is_empty()
    }

    fn suppresses(&self, begin_order: CatalogOrderId) -> bool {
        self.unmaterialized_inline_begin_orders
            .binary_search(&begin_order)
            .is_ok()
    }
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InlineSuppressionContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_SUPPRESSION_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineSuppressionContextKey, InlineSuppressionContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_suppression_context_cache()
-> &'static BoundedCache<InlineSuppressionContextKey, InlineSuppressionContext> {
    static_bounded_cache(&INLINE_SUPPRESSION_CONTEXT_CACHE, 512)
}

fn push_files_payload(
    out: &mut String,
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    files: Vec<AttachedDataFile>,
    request_context: Option<&mut FileListingRequestContext>,
) -> CatalogResult<()> {
    let started = RuntimeMetricStage::start();
    if files.is_empty() {
        out.push_str("file_count=0\n");
        record_list_data_files_at_render_stage("InlineFileDeletions", started);
        record_list_data_files_at_render_stage("FormatRows", started);
        return Ok(());
    }
    let inline_file_deletions =
        list_inline_file_deletions_for_visible_files(kv, catalog, snapshot_order, &files)?;
    record_list_data_files_at_render_stage("InlineFileDeletions", started);
    let started = RuntimeMetricStage::start();
    let public_snapshot_orders = file_payload_public_snapshot_orders(snapshot_order, &files);
    let mut render_context =
        FilePayloadRenderContext::new(kv, catalog, &public_snapshot_orders, request_context)?;
    record_list_data_files_at_render_stage("Context", started);
    let started = RuntimeMetricStage::start();
    out.push_str(&format!("file_count={}\n", files.len()));
    for attached in files {
        let file = attached.data_file;
        let inline_row_ids = inline_file_deletions
            .get(&file.data_file_id)
            .map(|rows| {
                rows.iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let delete_file_id = attached
            .delete_file
            .as_ref()
            .map_or(String::new(), |delete_file| {
                delete_file.delete_file_id.0.to_string()
            });
        let delete_path = attached
            .delete_file
            .as_ref()
            .map_or(String::new(), |delete_file| delete_file.path.clone());
        let delete_count = attached
            .delete_file
            .as_ref()
            .map_or(String::new(), |delete_file| {
                delete_file.record_count.to_string()
            });
        let delete_file_size = attached
            .delete_file
            .as_ref()
            .map_or(String::new(), |delete_file| {
                delete_file.file_size_bytes.to_string()
            });
        let delete_begin_snapshot =
            attached
                .delete_file
                .as_ref()
                .map_or(Ok(String::new()), |delete_file| {
                    render_context
                        .public_snapshot_id_for_order(delete_file.validity.begin_order)
                        .map(|sequence| sequence.to_string())
                })?;
        let delete_max_partial_snapshot = attached
            .delete_file
            .as_ref()
            .and_then(|delete_file| delete_file.max_partial_order)
            .map(|order| {
                render_context
                    .public_snapshot_id_for_order(order)
                    .map(|sequence| sequence.to_string())
            })
            .transpose()?
            .unwrap_or_default();
        let begin_snapshot = render_context
            .public_snapshot_id_for_order(file.validity.begin_order)?
            .to_string();
        let max_partial_snapshot = file
            .max_partial_order
            .map(|order| {
                render_context
                    .public_snapshot_id_for_order(order)
                    .map(|sequence| sequence.to_string())
            })
            .transpose()?
            .unwrap_or_default();
        let snapshot_filter_max = file
            .max_partial_order
            .filter(|max_partial_order| *max_partial_order > snapshot_order)
            .map(|_| {
                render_context
                    .public_snapshot_id_for_order(snapshot_order)
                    .map(|sequence| sequence.to_string())
            })
            .transpose()?
            .unwrap_or_default();
        let footer_size = file
            .footer_size
            .map(|value| value.to_string())
            .unwrap_or_default();
        let schema_version =
            render_context.schema_version_for_order(kv, file_schema_order(&file))?;
        let row_id_start = if file.row_id_start_known {
            file.row_id_start.to_string()
        } else {
            Default::default()
        };
        out.push_str(&format!(
            "file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            file.data_file_id.0,
            file.table_id.0,
            file.path,
            file.record_count,
            file.file_size_bytes,
            row_id_start,
            file.mapping_id.map_or(String::new(), |id| id.to_string()),
            delete_file_id,
            delete_path,
            delete_count,
            delete_file_size,
            delete_begin_snapshot,
            inline_row_ids,
            begin_snapshot,
            max_partial_snapshot,
            footer_size,
            schema_version,
            delete_max_partial_snapshot,
            snapshot_filter_max,
            file.encryption_key,
            attached
                .delete_file
                .as_ref()
                .map_or("", |delete_file| delete_file.encryption_key.as_str())
        ));
    }
    record_list_data_files_at_render_stage("FormatRows", started);
    Ok(())
}

fn list_inline_file_deletions_for_visible_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    files: &[AttachedDataFile],
) -> CatalogResult<BTreeMap<DataFileId, BTreeSet<u64>>> {
    let mut files_by_table = BTreeMap::<TableId, BTreeSet<DataFileId>>::new();
    for attached in files {
        files_by_table
            .entry(attached.data_file.table_id)
            .or_default()
            .insert(attached.data_file.data_file_id);
    }
    let mut inline_file_deletions = BTreeMap::new();
    for (table_id, data_file_ids) in files_by_table {
        inline_file_deletions.extend(list_inline_file_deletions_for_data_files_at(
            kv,
            catalog,
            table_id,
            snapshot_order,
            &data_file_ids,
        )?);
    }
    Ok(inline_file_deletions)
}

fn file_payload_public_snapshot_orders(
    snapshot_order: CatalogOrderId,
    files: &[AttachedDataFile],
) -> BTreeSet<CatalogOrderId> {
    let mut orders = BTreeSet::new();
    orders.insert(snapshot_order);
    for attached in files {
        let file = &attached.data_file;
        orders.insert(file.validity.begin_order);
        if let Some(order) = file.max_partial_order {
            orders.insert(order);
        }
        if let Some(delete_file) = &attached.delete_file {
            orders.insert(delete_file.validity.begin_order);
            if let Some(order) = delete_file.max_partial_order {
                orders.insert(order);
            }
        }
    }
    orders
}

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_method_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_method_stage(_operation: &str, _started: RuntimeMetricStage) {}

#[cfg(feature = "runtime-metrics")]
fn record_list_data_files_at_render_stage(stage: &str, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!("ListDataFilesAtRenderStage{stage}"),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_list_data_files_at_render_stage(_stage: &str, _started: RuntimeMetricStage) {}

struct FilePayloadRenderContext {
    catalog: CatalogId,
    public_snapshot_by_order: SharedOrderMap,
    schema_version_by_order: SharedOrderMap,
    exact_schema_version_by_order: BTreeMap<CatalogOrderId, u64>,
}

impl FilePayloadRenderContext {
    fn new(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        public_snapshot_orders: &BTreeSet<CatalogOrderId>,
        mut request_context: Option<&mut FileListingRequestContext>,
    ) -> CatalogResult<Self> {
        let started = RuntimeMetricStage::start();
        let request_mappings = match &mut request_context {
            Some(context) => Some(context.render_mappings(kv, catalog, public_snapshot_orders)?),
            None => None,
        };
        let public_snapshot_by_order = match request_mappings.as_ref() {
            Some((sequences, _)) => sequences.clone(),
            None => {
                public_snapshot_sequences_containing_orders(kv, catalog, public_snapshot_orders)?
            }
        };
        record_list_data_files_at_render_stage("ContextPublicSnapshots", started);
        let started = RuntimeMetricStage::start();
        let schema_version_by_order = match request_mappings {
            Some((_, schema_versions)) => schema_versions,
            None => snapshot_schema_versions_by_order_shared(kv, catalog)?,
        };
        record_list_data_files_at_render_stage("ContextSchemaVersions", started);
        Ok(Self {
            catalog,
            public_snapshot_by_order,
            schema_version_by_order,
            exact_schema_version_by_order: BTreeMap::new(),
        })
    }

    fn public_snapshot_id_for_order(&self, order: CatalogOrderId) -> CatalogResult<u64> {
        order_mapping_at_or_after_oldest(self.public_snapshot_by_order.as_ref(), order)
            .copied()
            .ok_or_else(|| {
                crate::CatalogError::Decode(format!("snapshot order {order} does not exist"))
            })
    }

    fn schema_version_for_order(
        &mut self,
        kv: &impl OrderedCatalogKv,
        order: CatalogOrderId,
    ) -> CatalogResult<u64> {
        if let Some(version) = self.schema_version_by_order.get(&order).copied() {
            return Ok(version);
        }
        if let Some(version) = self.exact_schema_version_by_order.get(&order).copied() {
            return Ok(version);
        }
        let previous = self
            .schema_version_by_order
            .as_ref()
            .range(..=order)
            .next_back()
            .map(|(_, version)| *version);
        let next = self
            .schema_version_by_order
            .as_ref()
            .range(order..)
            .next()
            .map(|(_, version)| *version);
        if let (Some(previous_version), Some(next_version)) = (previous, next)
            && previous_version != next_version
        {
            let version = snapshot_schema_version(kv, self.catalog, order)?;
            self.exact_schema_version_by_order.insert(order, version);
            return Ok(version);
        }
        if let Some(version) = previous {
            return Ok(version);
        }
        let all_schema_versions = snapshot_schema_versions_by_order_shared(kv, self.catalog)?;
        if let Some(version) = all_schema_versions.get(&order).copied() {
            return Ok(version);
        }
        let previous = all_schema_versions
            .as_ref()
            .range(..=order)
            .next_back()
            .map(|(_, version)| *version);
        let next = all_schema_versions
            .as_ref()
            .range(order..)
            .next()
            .map(|(_, version)| *version);
        if let (Some(previous_version), Some(next_version)) = (previous, next)
            && previous_version != next_version
        {
            let version = snapshot_schema_version(kv, self.catalog, order)?;
            self.exact_schema_version_by_order.insert(order, version);
            return Ok(version);
        }
        previous
            .or_else(|| {
                all_schema_versions
                    .as_ref()
                    .iter()
                    .next()
                    .map(|(_, value)| *value)
            })
            .ok_or_else(|| {
                crate::CatalogError::Decode(format!("snapshot order {order} does not exist"))
            })
    }
}

fn order_mapping_at_or_after_oldest<T>(
    mappings: &std::collections::BTreeMap<CatalogOrderId, T>,
    order: CatalogOrderId,
) -> Option<&T> {
    mappings
        .range(..=order)
        .next_back()
        .or_else(|| mappings.iter().next())
        .map(|(_, value)| value)
}

fn file_schema_order(file: &crate::DataFileRow) -> CatalogOrderId {
    file.max_partial_order.unwrap_or(file.validity.begin_order)
}

#[cfg(test)]
#[path = "runtime_file_listing_tests.rs"]
mod runtime_file_listing_tests;
