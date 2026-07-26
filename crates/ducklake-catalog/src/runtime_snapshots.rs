use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(not(test))]
use crate::runtime_metrics::RuntimeMetricStage;
#[cfg(not(test))]
use crate::schema_version_state::load_catalog_snapshot_version;
use crate::{
    CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataFileChangeKind,
    DuckLakeSnapshotId, InlineRowChangeKind, MacroRow, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SchemaRow, SnapshotRow, TableRow, ViewRow,
    inline_data::inline_file_deletion_changed_table_ids_at,
    keys::{
        inline_table_change_prefix, order_delete_file_change_prefix,
        order_delete_file_change_scan_end, order_delete_file_change_scan_start,
        snapshot_data_file_change_prefix,
    },
    latest_snapshot, list_all_snapshots, list_snapshots, list_snapshots_older_than,
    macro_store::{list_macro_rows, list_macro_rows_for_snapshot_cache},
    runtime_catalog_snapshot::snapshot_watermarks,
    runtime_read_context::CatalogInlineDeletionReadContext,
    schema_store::{list_schema_rows, list_schema_rows_for_snapshot_cache, load_schema_at},
    schema_version_state::{load_schema_version_at, load_schema_versions_at},
    snapshot_operations::{
        SnapshotOperationKind, snapshot_operation_table_ids_at, snapshot_operations_by_order,
    },
    table_store::{list_table_rows, list_table_rows_with_snapshot_cache},
    view_store::{list_view_rows, list_view_rows_for_snapshot_cache},
};
#[cfg(all(not(test), feature = "runtime-metrics"))]
use crate::{
    runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed},
    runtime_protocol::RuntimeCatalogBackend,
};

const EMPTY_SNAPSHOT_STRING_FIELD: &str = "\\0";

pub fn snapshot_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    load_schema_version_at(kv, catalog, order)
}

pub(crate) fn snapshot_schema_versions_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    snapshot_schema_versions_by_order_shared(kv, catalog).map(|versions| versions.as_ref().clone())
}

pub(crate) fn snapshot_schema_versions_by_order_shared(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<SharedOrderMap> {
    #[cfg(test)]
    {
        snapshot_schema_versions_by_order_uncached(kv, catalog).map(SharedOrderMap::new)
    }
    #[cfg(not(test))]
    {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return Ok(SharedOrderMap::new(BTreeMap::new()));
        };
        let key = CatalogVersionCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            latest_order: latest.order,
        };
        let cache = snapshot_schema_versions_cache();
        if let Some(versions) = cache.get(key) {
            return Ok(versions);
        }
        let versions =
            SharedOrderMap::new(snapshot_schema_versions_by_order_uncached(kv, catalog)?);
        cache.insert(key, versions.clone());
        Ok(versions)
    }
}

fn snapshot_schema_versions_by_order_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    let orders = list_all_snapshots(kv, catalog)?
        .into_iter()
        .map(|snapshot| snapshot.order)
        .collect::<BTreeSet<_>>();
    load_schema_versions_at(kv, catalog, &orders)
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogVersionCacheKey {
    namespace: crate::CatalogCacheNamespace,
    catalog: CatalogId,
    latest_order: CatalogOrderId,
}

#[cfg(not(test))]
static SNAPSHOT_SCHEMA_VERSIONS_CACHE: OnceLock<
    BoundedCache<CatalogVersionCacheKey, SharedOrderMap>,
> = OnceLock::new();

#[cfg(not(test))]
static INLINE_ROW_CHANGE_INDEX_CACHE: OnceLock<
    BoundedCache<CatalogVersionCacheKey, InlineRowChangeIndex>,
> = OnceLock::new();

#[cfg(not(test))]
fn snapshot_schema_versions_cache() -> &'static BoundedCache<CatalogVersionCacheKey, SharedOrderMap>
{
    static_bounded_cache(&SNAPSHOT_SCHEMA_VERSIONS_CACHE, 1024)
}

#[cfg(not(test))]
fn inline_row_change_index_cache()
-> &'static BoundedCache<CatalogVersionCacheKey, InlineRowChangeIndex> {
    static_bounded_cache(&INLINE_ROW_CHANGE_INDEX_CACHE, 1024)
}

#[derive(Clone)]
pub(crate) struct SharedOrderMap {
    #[cfg(not(test))]
    inner: Arc<BTreeMap<CatalogOrderId, u64>>,
    #[cfg(test)]
    inner: BTreeMap<CatalogOrderId, u64>,
}

impl SharedOrderMap {
    fn new(inner: BTreeMap<CatalogOrderId, u64>) -> Self {
        Self {
            #[cfg(not(test))]
            inner: Arc::new(inner),
            #[cfg(test)]
            inner,
        }
    }

    pub(crate) fn get(&self, order: &CatalogOrderId) -> Option<&u64> {
        self.as_ref().get(order)
    }

    pub(crate) fn as_ref(&self) -> &BTreeMap<CatalogOrderId, u64> {
        &self.inner
    }
}

fn catalog_schema_change_orders(
    schemas: &[SchemaRow],
    tables: &[TableRow],
    views: &[ViewRow],
    macros: &[MacroRow],
) -> BTreeSet<CatalogOrderId> {
    let mut change_orders = BTreeSet::new();
    for schema in schemas {
        push_all_validity_orders(
            &mut change_orders,
            schema.validity.begin_order,
            schema.validity.end_order,
        );
    }
    for view in views {
        push_all_validity_orders(
            &mut change_orders,
            view.validity.begin_order,
            view.validity.end_order,
        );
    }
    for macro_row in macros {
        push_all_validity_orders(
            &mut change_orders,
            macro_row.validity.begin_order,
            macro_row.validity.end_order,
        );
    }

    let begun_tables = tables
        .iter()
        .map(|table| ((table.validity.begin_order, table.table_id), table))
        .collect::<BTreeMap<_, _>>();
    let ended_tables = tables
        .iter()
        .filter_map(|table| {
            table
                .validity
                .end_order
                .map(|order| ((order, table.table_id), table))
        })
        .collect::<BTreeMap<_, _>>();
    for table in tables {
        let begin_key = (table.validity.begin_order, table.table_id);
        if ended_tables
            .get(&begin_key)
            .is_none_or(|previous| !previous.same_user_visible_schema_as(table))
        {
            change_orders.insert(table.validity.begin_order);
        }
        if let Some(end_order) = table.validity.end_order
            && !begun_tables.contains_key(&(end_order, table.table_id))
        {
            change_orders.insert(end_order);
        }
    }
    change_orders
}

fn push_all_validity_orders(
    orders: &mut BTreeSet<CatalogOrderId>,
    begin_order: CatalogOrderId,
    end_order: Option<CatalogOrderId>,
) {
    orders.insert(begin_order);
    if let Some(end_order) = end_order {
        orders.insert(end_order);
    }
}

#[derive(Clone)]
pub(crate) struct ListSnapshotsPayload {
    pub(crate) older_than_micros: Option<i64>,
    pub(crate) requested_ducklake_ids: Option<Vec<DuckLakeSnapshotId>>,
    pub(crate) protect_latest: bool,
}

pub(crate) fn list_snapshots_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ListSnapshotsPayload,
) -> CatalogResult<Vec<u8>> {
    let selected = selected_snapshots(kv, catalog, payload)?;
    let (snapshots, context) =
        coalesced_public_snapshot_groups_with_context(kv, catalog, selected)?;
    let public_schema_versions = public_schema_versions_for_groups(&snapshots, &context);
    let metadata_changes = SnapshotMetadataChangeIndex::new(
        &context.schemas,
        &context.tables,
        &context.views,
        &context.macros,
    );
    let watermarks = snapshots
        .last()
        .map(|snapshot| snapshot_watermarks(kv, catalog, snapshot.last_order()))
        .transpose()?;
    let renderer = SnapshotListRenderer {
        kv,
        catalog,
        metadata_changes,
        watermarks,
        context: &context,
    };
    let mut out = format!("snapshot_count={}\n", snapshots.len());
    for public_sequence in 0..snapshots.len() {
        let snapshot_id = snapshots[public_sequence].representative.sequence.0;
        push_snapshot(
            &mut out,
            &renderer,
            snapshot_id,
            &snapshots[public_sequence],
            public_schema_versions[public_sequence],
        )?;
    }
    Ok(out.into_bytes())
}

pub(crate) fn snapshot_changes_after_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    base_public_snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Vec<u8>> {
    let mut changes = Vec::new();
    for snapshot in list_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence.0 > base_public_snapshot_id.0)
    {
        let changes_made = snapshot_changes_made(kv, catalog, snapshot.order)?;
        if !changes_made.is_empty() {
            changes.push(changes_made);
        }
    }
    Ok(format!("changes_made={}\n", changes.join(",")).into_bytes())
}

pub fn snapshot_by_public_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<SnapshotRow>> {
    if let Some(latest) = latest_snapshot(kv, catalog)?
        && latest.sequence.0 == snapshot_id.0
    {
        return Ok(Some(latest));
    }
    #[cfg(test)]
    {
        snapshot_by_public_sequence_uncached(kv, catalog, snapshot_id)
    }
    #[cfg(not(test))]
    {
        let context = SnapshotReadContext::for_public_snapshot(kv, catalog, snapshot_id)?;
        Ok(context.public_snapshot(snapshot_id))
    }
}

#[derive(Clone)]
pub(crate) struct SnapshotReadContext {
    #[cfg_attr(test, allow(dead_code))]
    latest: Option<SnapshotRow>,
    #[cfg_attr(test, allow(dead_code))]
    by_order: Arc<BTreeMap<CatalogOrderId, SnapshotRow>>,
    by_public_sequence: Arc<BTreeMap<DuckLakeSnapshotId, SnapshotRow>>,
    public_span_by_sequence: Arc<BTreeMap<DuckLakeSnapshotId, (CatalogOrderId, CatalogOrderId)>>,
    by_ducklake_sequence: Arc<BTreeMap<DuckLakeSnapshotId, SnapshotRow>>,
    ducklake_span_by_sequence: Arc<BTreeMap<DuckLakeSnapshotId, (CatalogOrderId, CatalogOrderId)>>,
    #[allow(dead_code)]
    sequences_by_order: SharedOrderMap,
    schemas: Arc<Vec<SchemaRow>>,
    tables: Arc<Vec<TableRow>>,
    views: Arc<Vec<ViewRow>>,
    macros: Arc<Vec<MacroRow>>,
}

impl SnapshotReadContext {
    pub(crate) fn for_current_catalog(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        #[cfg(test)]
        {
            Self::load(kv, catalog)
        }
        #[cfg(not(test))]
        {
            let latest = latest_snapshot(kv, catalog)?.map(|snapshot| snapshot.order);
            let key = (kv.catalog_cache_namespace(), catalog);
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(key)
                && context.latest_order_is(latest)
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
    }

    pub(crate) fn for_public_snapshot(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot_id: DuckLakeSnapshotId,
    ) -> CatalogResult<Self> {
        #[cfg(test)]
        {
            let _ = snapshot_id;
            Self::load(kv, catalog)
        }
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog);
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(key)
                && context.public_snapshot(snapshot_id).is_some()
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
    }

    pub(crate) fn for_ducklake_snapshot(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot_id: DuckLakeSnapshotId,
    ) -> CatalogResult<Self> {
        #[cfg(test)]
        {
            let _ = snapshot_id;
            Self::load(kv, catalog)
        }
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog);
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(key)
                && context.ducklake_snapshot(snapshot_id).is_some()
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
    }

    pub(crate) fn public_snapshot(&self, snapshot_id: DuckLakeSnapshotId) -> Option<SnapshotRow> {
        self.by_public_sequence.get(&snapshot_id).cloned()
    }

    pub(crate) fn latest_public_snapshot_id(&self) -> Option<DuckLakeSnapshotId> {
        self.by_public_sequence.keys().next_back().copied()
    }

    pub(crate) fn ducklake_snapshot(&self, snapshot_id: DuckLakeSnapshotId) -> Option<SnapshotRow> {
        self.by_ducklake_sequence.get(&snapshot_id).cloned()
    }

    pub(crate) fn public_snapshot_order_span(
        &self,
        snapshot_id: DuckLakeSnapshotId,
    ) -> Option<(CatalogOrderId, CatalogOrderId)> {
        self.public_span_by_sequence.get(&snapshot_id).copied()
    }

    pub(crate) fn ducklake_snapshot_order_span(
        &self,
        snapshot_id: DuckLakeSnapshotId,
    ) -> Option<(CatalogOrderId, CatalogOrderId)> {
        self.ducklake_span_by_sequence.get(&snapshot_id).copied()
    }

    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn snapshot_at_timestamp(
        &self,
        timestamp_micros: i64,
        bound: crate::SnapshotTimestampBound,
    ) -> Option<SnapshotRow> {
        let mut snapshots = self.by_order.values().cloned().collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| (snapshot.created_at_micros, snapshot.order));
        match bound {
            crate::SnapshotTimestampBound::Lower => snapshots
                .into_iter()
                .find(|snapshot| snapshot.created_at_micros >= timestamp_micros),
            crate::SnapshotTimestampBound::Upper => snapshots
                .into_iter()
                .take_while(|snapshot| snapshot.created_at_micros <= timestamp_micros)
                .last(),
        }
    }

    #[cfg(not(test))]
    fn latest_order_is(&self, latest: Option<CatalogOrderId>) -> bool {
        self.latest.as_ref().map(|snapshot| snapshot.order) == latest
    }

    #[allow(dead_code)]
    pub(crate) fn sequences_by_order(&self) -> SharedOrderMap {
        self.sequences_by_order.clone()
    }

    pub(crate) fn schemas(&self) -> &[SchemaRow] {
        self.schemas.as_slice()
    }

    pub(crate) fn tables(&self) -> &[TableRow] {
        self.tables.as_slice()
    }

    pub(crate) fn views(&self) -> &[ViewRow] {
        self.views.as_slice()
    }

    pub(crate) fn macros(&self) -> &[MacroRow] {
        self.macros.as_slice()
    }

    fn load(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Self> {
        let snapshots = list_snapshots(kv, catalog)?;
        let (groups, coalesce_context) =
            coalesced_public_snapshot_groups_with_context(kv, catalog, snapshots.clone())?;
        Ok(Self::from_snapshots(snapshots, groups, coalesce_context))
    }

    fn from_snapshots(
        snapshots: Vec<SnapshotRow>,
        groups: Vec<PublicSnapshot>,
        coalesce_context: PublicSnapshotCoalesceContext,
    ) -> Self {
        let latest = snapshots
            .iter()
            .max_by_key(|snapshot| snapshot.order)
            .cloned();
        let mut by_order = BTreeMap::new();
        let mut by_ducklake_sequence = BTreeMap::new();
        let mut ducklake_span_by_sequence = BTreeMap::new();
        for snapshot in snapshots {
            by_order.insert(snapshot.order, snapshot.clone());
            ducklake_span_by_sequence
                .entry(DuckLakeSnapshotId(snapshot.sequence.0))
                .and_modify(|span: &mut (CatalogOrderId, CatalogOrderId)| {
                    span.0 = span.0.min(snapshot.order);
                    span.1 = span.1.max(snapshot.order);
                })
                .or_insert((snapshot.order, snapshot.order));
            by_ducklake_sequence
                .entry(DuckLakeSnapshotId(snapshot.sequence.0))
                .and_modify(|existing: &mut SnapshotRow| {
                    if snapshot.order > existing.order {
                        *existing = snapshot.clone();
                    }
                })
                .or_insert(snapshot);
        }
        let mut by_public_sequence = BTreeMap::new();
        let mut public_span_by_sequence = BTreeMap::new();
        let mut sequences_by_order = BTreeMap::new();
        for group in groups {
            let sequence = group.representative.sequence.0;
            public_span_by_sequence.insert(
                DuckLakeSnapshotId(sequence),
                (group.first_order(), group.last_order()),
            );
            for order in &group.orders {
                sequences_by_order.insert(*order, sequence);
            }
            if let Some(snapshot) = by_order.get(&group.last_order()) {
                by_public_sequence.insert(DuckLakeSnapshotId(sequence), snapshot.clone());
            }
        }
        Self {
            latest,
            by_order: Arc::new(by_order),
            by_public_sequence: Arc::new(by_public_sequence),
            public_span_by_sequence: Arc::new(public_span_by_sequence),
            by_ducklake_sequence: Arc::new(by_ducklake_sequence),
            ducklake_span_by_sequence: Arc::new(ducklake_span_by_sequence),
            sequences_by_order: SharedOrderMap::new(sequences_by_order),
            schemas: coalesce_context.schemas,
            tables: coalesce_context.tables,
            views: coalesce_context.views,
            macros: coalesce_context.macros,
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_loaded_facts_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.by_order, &other.by_order)
            && Arc::ptr_eq(&self.by_public_sequence, &other.by_public_sequence)
            && Arc::ptr_eq(
                &self.public_span_by_sequence,
                &other.public_span_by_sequence,
            )
            && Arc::ptr_eq(&self.by_ducklake_sequence, &other.by_ducklake_sequence)
            && Arc::ptr_eq(
                &self.ducklake_span_by_sequence,
                &other.ducklake_span_by_sequence,
            )
            && Arc::ptr_eq(&self.schemas, &other.schemas)
            && Arc::ptr_eq(&self.tables, &other.tables)
            && Arc::ptr_eq(&self.views, &other.views)
            && Arc::ptr_eq(&self.macros, &other.macros)
    }
}

#[cfg(not(test))]
static SNAPSHOT_READ_CONTEXT_CACHE: OnceLock<
    BoundedCache<(crate::CatalogCacheNamespace, CatalogId), SnapshotReadContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn snapshot_read_context_cache()
-> &'static BoundedCache<(crate::CatalogCacheNamespace, CatalogId), SnapshotReadContext> {
    static_bounded_cache(&SNAPSHOT_READ_CONTEXT_CACHE, 64)
}

#[cfg(not(test))]
pub(crate) fn invalidate_runtime_snapshot_context(catalog: CatalogId) {
    if let Some(cache) = SNAPSHOT_READ_CONTEXT_CACHE.get() {
        cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
    }
    if let Some(cache) = SNAPSHOT_SCHEMA_VERSIONS_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = INLINE_ROW_CHANGE_INDEX_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(test)]
fn snapshot_by_public_sequence_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<SnapshotRow>> {
    let snapshots = list_snapshots(kv, catalog)?;
    let (groups, _) =
        coalesced_public_snapshot_groups_with_context(kv, catalog, snapshots.clone())?;
    let Some(group) = groups
        .into_iter()
        .rev()
        .find(|group| group.representative.sequence.0 == snapshot_id.0)
    else {
        return Ok(None);
    };
    let order = group.last_order();
    Ok(snapshots
        .into_iter()
        .find(|snapshot| snapshot.order == order))
}

pub fn snapshot_by_ducklake_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<SnapshotRow>> {
    if let Some(latest) = latest_snapshot(kv, catalog)?
        && latest.sequence.0 == snapshot_id.0
    {
        return Ok(Some(latest));
    }
    let context = SnapshotReadContext::for_ducklake_snapshot(kv, catalog, snapshot_id)?;
    if let Some(snapshot) = context.ducklake_snapshot(snapshot_id) {
        return Ok(Some(snapshot));
    }
    crate::snapshot_by_raw_sequence(kv, catalog, RawSnapshotSequence(snapshot_id.0))
}

pub fn next_public_snapshot_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<DuckLakeSnapshotId> {
    Ok(
        latest_snapshot(kv, catalog)?.map_or(DuckLakeSnapshotId(0), |snapshot| {
            DuckLakeSnapshotId(snapshot.sequence.next().0)
        }),
    )
}

pub fn public_snapshot_sequence_for_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<Option<DuckLakeSnapshotId>> {
    Ok(
        public_snapshot_sequences_by_order_containing(kv, catalog, order)?
            .get(&order)
            .copied()
            .map(DuckLakeSnapshotId),
    )
}

pub(crate) fn public_snapshot_sequences_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<std::collections::BTreeMap<CatalogOrderId, u64>> {
    public_snapshot_sequences_by_order_shared(kv, catalog)
        .map(|sequences| sequences.as_ref().clone())
}

pub(crate) fn public_snapshot_sequences_by_order_shared(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<SharedOrderMap> {
    #[cfg(test)]
    {
        public_snapshot_sequences_by_order_uncached(kv, catalog).map(SharedOrderMap::new)
    }
    #[cfg(not(test))]
    {
        let latest = latest_snapshot(kv, catalog)?.map(|snapshot| snapshot.order);
        let key = (kv.catalog_cache_namespace(), catalog);
        let cache = snapshot_read_context_cache();
        if let Some(context) = cache.get(key)
            && context.latest_order_is(latest)
        {
            record_public_snapshot_sequences_cache("Hit", RuntimeMetricStage::zero());
            return Ok(context.sequences_by_order());
        }
        let started = RuntimeMetricStage::start();
        let context = SnapshotReadContext::load(kv, catalog)?;
        let sequences = context.sequences_by_order();
        record_public_snapshot_sequences_cache("Load", started);
        cache.insert(key, context);
        Ok(sequences)
    }
}

pub(crate) fn public_snapshot_sequences_by_order_containing(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    required_order: CatalogOrderId,
) -> CatalogResult<SharedOrderMap> {
    #[cfg(test)]
    {
        let _ = required_order;
        public_snapshot_sequences_by_order_uncached(kv, catalog).map(SharedOrderMap::new)
    }
    #[cfg(not(test))]
    {
        let key = (kv.catalog_cache_namespace(), catalog);
        let cache = snapshot_read_context_cache();
        if let Some(context) = cache.get(key)
            && context.sequences_by_order().get(&required_order).is_some()
        {
            record_public_snapshot_sequences_cache("Hit", RuntimeMetricStage::zero());
            return Ok(context.sequences_by_order());
        }
        let started = RuntimeMetricStage::start();
        let context = SnapshotReadContext::load(kv, catalog)?;
        let sequences = context.sequences_by_order();
        record_public_snapshot_sequences_cache("Load", started);
        cache.insert(key, context);
        Ok(sequences)
    }
}

#[cfg(all(not(test), feature = "runtime-metrics"))]
#[allow(dead_code)]
fn record_public_snapshot_sequences_cache(stage: &str, started: RuntimeMetricStage) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        &format!("PublicSnapshotSequencesCache{stage}"),
        RuntimeMetricStatus::Ok,
        started.elapsed_micros(),
    );
}

#[cfg(all(not(test), not(feature = "runtime-metrics")))]
#[inline]
fn record_public_snapshot_sequences_cache(_stage: &str, _started: RuntimeMetricStage) {}

#[cfg(test)]
fn public_snapshot_sequences_by_order_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<std::collections::BTreeMap<CatalogOrderId, u64>> {
    let (groups, _) =
        coalesced_public_snapshot_groups_with_context(kv, catalog, list_snapshots(kv, catalog)?)?;
    let mut sequences = std::collections::BTreeMap::new();
    for group in groups {
        let sequence = group.representative.sequence.0;
        for order in group.orders {
            sequences.insert(order, sequence);
        }
    }
    Ok(sequences)
}

pub(crate) fn public_snapshot_order_span(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<(CatalogOrderId, CatalogOrderId)>> {
    Ok(
        SnapshotReadContext::for_public_snapshot(kv, catalog, snapshot_id)?
            .public_snapshot_order_span(snapshot_id),
    )
}

pub(crate) fn ducklake_snapshot_order_span(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<(CatalogOrderId, CatalogOrderId)>> {
    Ok(
        SnapshotReadContext::for_ducklake_snapshot(kv, catalog, snapshot_id)?
            .ducklake_snapshot_order_span(snapshot_id),
    )
}

fn selected_snapshots(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ListSnapshotsPayload,
) -> CatalogResult<Vec<SnapshotRow>> {
    let mut snapshots = if let Some(older_than_micros) = payload.older_than_micros {
        list_snapshots_older_than(kv, catalog, older_than_micros)?
    } else {
        list_snapshots(kv, catalog)?
    };
    if let Some(requested) = payload.requested_ducklake_ids {
        let requested = requested
            .into_iter()
            .map(|id| RawSnapshotSequence(id.0))
            .collect::<BTreeSet<_>>();
        snapshots.retain(|snapshot| requested.contains(&snapshot.sequence));
    }
    if payload.protect_latest
        && let Some(latest) = latest_snapshot(kv, catalog)?
    {
        snapshots.retain(|snapshot| snapshot.sequence != latest.sequence);
    }
    Ok(snapshots)
}

struct SnapshotListRenderer<'a, K> {
    kv: &'a K,
    catalog: CatalogId,
    metadata_changes: SnapshotMetadataChangeIndex<'a>,
    watermarks: Option<crate::runtime_catalog_snapshot::SnapshotWatermarks>,
    context: &'a PublicSnapshotCoalesceContext,
}

fn push_snapshot<K: OrderedCatalogKv>(
    out: &mut String,
    renderer: &SnapshotListRenderer<'_, K>,
    public_sequence: u64,
    snapshot: &PublicSnapshot,
    schema_version: u64,
) -> CatalogResult<()> {
    let watermarks = renderer.watermarks.ok_or_else(|| {
        crate::CatalogError::Decode(
            "public snapshot list is missing allocator watermarks".to_owned(),
        )
    })?;
    let changes_made = public_snapshot_changes_made(
        renderer.kv,
        renderer.catalog,
        snapshot,
        &renderer.metadata_changes,
        renderer.context,
    )?;
    let changes_made = if changes_made.is_empty()
        && snapshot.representative.sequence == crate::RawSnapshotSequence::initial()
    {
        "created_schema:\"main\"".to_owned()
    } else {
        changes_made
    };
    out.push_str(&format!(
        "snapshot\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        public_sequence,
        snapshot.representative.created_at_micros,
        schema_version,
        watermarks.next_file_id,
        changes_made,
        snapshot_author_field(&snapshot.representative),
        snapshot_optional_field(snapshot.representative.commit_message.as_deref())
    ));
    if let Some(commit_extra_info) = snapshot.representative.commit_extra_info.as_deref() {
        out.push('\t');
        out.push_str(snapshot_string_field(commit_extra_info));
    }
    out.push('\n');
    Ok(())
}

fn snapshot_author_field(snapshot: &SnapshotRow) -> &str {
    if snapshot.created_by == "aux-ducklake" {
        ""
    } else {
        snapshot_string_field(snapshot.created_by.as_str())
    }
}

fn snapshot_optional_field(value: Option<&str>) -> &str {
    value.map_or("", snapshot_string_field)
}

fn snapshot_string_field(value: &str) -> &str {
    if value.is_empty() {
        EMPTY_SNAPSHOT_STRING_FIELD
    } else {
        value
    }
}

fn public_schema_versions_for_groups(
    snapshots: &[PublicSnapshot],
    context: &PublicSnapshotCoalesceContext,
) -> Vec<u64> {
    let change_orders = catalog_schema_change_orders(
        &context.schemas,
        &context.tables,
        &context.views,
        &context.macros,
    );
    let mut schema_version = 0;
    let mut change_orders = change_orders.into_iter().peekable();
    snapshots
        .iter()
        .map(|snapshot| {
            let group_orders = group_order_set(snapshot);
            let mut group_changed_schema = false;
            while change_orders
                .peek()
                .is_some_and(|change_order| *change_order <= snapshot.last_order())
            {
                let change_order = change_orders.next().unwrap();
                if group_orders.contains(&change_order) {
                    group_changed_schema = true;
                } else {
                    schema_version += 1;
                }
            }
            if group_changed_schema {
                schema_version += 1;
            }
            schema_version
        })
        .collect()
}

#[derive(Clone)]
struct PublicSnapshot {
    representative: SnapshotRow,
    orders: Vec<CatalogOrderId>,
}

impl PublicSnapshot {
    fn first_order(&self) -> CatalogOrderId {
        *self.orders.first().unwrap_or(&self.representative.order)
    }

    fn last_order(&self) -> CatalogOrderId {
        *self.orders.last().unwrap_or(&self.representative.order)
    }
}

fn public_snapshot_groups(snapshots: impl IntoIterator<Item = SnapshotRow>) -> Vec<PublicSnapshot> {
    snapshots
        .into_iter()
        .map(|snapshot| PublicSnapshot {
            orders: vec![snapshot.order],
            representative: snapshot,
        })
        .collect()
}

fn coalesced_public_snapshot_groups_with_context(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshots: impl IntoIterator<Item = SnapshotRow>,
) -> CatalogResult<(Vec<PublicSnapshot>, PublicSnapshotCoalesceContext)> {
    let groups = public_snapshot_groups(snapshots);
    let context = PublicSnapshotCoalesceContext::load(kv, catalog, &groups)?;
    let mut coalesced = Vec::<PublicSnapshot>::with_capacity(groups.len());
    for current in groups {
        if let Some(previous) = coalesced.last_mut()
            && should_merge_public_snapshot_groups(&context, previous, &current)?
        {
            previous.orders.extend(current.orders);
        } else {
            coalesced.push(current);
        }
    }
    Ok((coalesced, context))
}

struct PublicSnapshotCoalesceContext {
    schemas: Arc<Vec<SchemaRow>>,
    tables: Arc<Vec<TableRow>>,
    views: Arc<Vec<ViewRow>>,
    macros: Arc<Vec<MacroRow>>,
    data_changes: BTreeMap<CatalogOrderId, Vec<SnapshotDataFileChange>>,
    inline_rows: InlineRowChangeIndex,
    inline_file_deletions: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
    delete_file_changes: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
    operations: BTreeMap<(CatalogOrderId, SnapshotOperationKind), BTreeSet<crate::TableId>>,
}

impl PublicSnapshotCoalesceContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        groups: &[PublicSnapshot],
    ) -> CatalogResult<Self> {
        let order_kind = groups
            .first()
            .map(|group| group.representative.order.kind())
            .unwrap_or(CatalogOrderKind::UuidV7);
        let latest_order = groups
            .last()
            .map(PublicSnapshot::last_order)
            .unwrap_or_else(|| CatalogOrderId::from_bytes(order_kind, [0; CatalogOrderId::LEN]));
        let facts = PublicSnapshotCoalesceFacts::for_catalog(kv, catalog, latest_order)?;
        Ok(Self {
            schemas: facts.schemas,
            tables: facts.tables,
            views: facts.views,
            macros: facts.macros,
            data_changes: data_file_changes_by_order(kv, catalog, order_kind)?,
            inline_rows: InlineRowChangeIndex::load(kv, catalog, order_kind)?,
            inline_file_deletions: inline_file_deletions_by_order(kv, catalog)?,
            delete_file_changes: delete_file_changes_by_order(kv, catalog, order_kind)?,
            operations: snapshot_operations_by_order(kv, catalog, order_kind)?,
        })
    }

    fn group_has_data_changes(&self, group: &PublicSnapshot) -> bool {
        group.orders.iter().any(|order| {
            self.data_changes
                .get(order)
                .is_some_and(|changes| !changes.is_empty())
                || self.inline_rows.has_any(*order)
                || self
                    .inline_file_deletions
                    .get(order)
                    .is_some_and(|tables| !tables.is_empty())
                || self
                    .delete_file_changes
                    .get(order)
                    .is_some_and(|tables| !tables.is_empty())
        })
    }

    fn group_has_metadata_changes(&self, group: &PublicSnapshot) -> bool {
        let orders = group_order_set(group);
        self.schemas.iter().any(|schema| {
            row_touches_orders(
                schema.validity.begin_order,
                schema.validity.end_order,
                &orders,
            )
        }) || self.tables.iter().any(|table| {
            row_touches_orders(
                table.validity.begin_order,
                table.validity.end_order,
                &orders,
            )
        }) || self.views.iter().any(|view| {
            row_touches_orders(view.validity.begin_order, view.validity.end_order, &orders)
        }) || self.macros.iter().any(|macro_row| {
            row_touches_orders(
                macro_row.validity.begin_order,
                macro_row.validity.end_order,
                &orders,
            )
        })
    }

    fn created_table_ids(&self, group: &PublicSnapshot) -> BTreeSet<crate::TableId> {
        let orders = group_order_set(group);
        self.tables
            .iter()
            .filter(|table| orders.contains(&table.validity.begin_order))
            .map(|table| table.table_id)
            .collect()
    }

    fn inserted_table_ids(&self, group: &PublicSnapshot) -> BTreeSet<crate::TableId> {
        let mut tables = BTreeSet::new();
        for order in &group.orders {
            if let Some(changes) = self.data_changes.get(order) {
                tables.extend(
                    changes
                        .iter()
                        .filter(|change| change.kind == DataFileChangeKind::Added)
                        .map(|change| change.table_id),
                );
            }
            tables.extend(
                self.inline_rows
                    .tables(*order, InlineRowChangeKind::Inserted),
            );
        }
        tables
    }

    fn touched_table_ids(&self, group: &PublicSnapshot) -> BTreeSet<crate::TableId> {
        let orders = group_order_set(group);
        self.tables
            .iter()
            .filter(|table| {
                row_touches_orders(
                    table.validity.begin_order,
                    table.validity.end_order,
                    &orders,
                )
            })
            .map(|table| table.table_id)
            .collect()
    }

    fn only_creates_default_main_schema(&self, group: &PublicSnapshot) -> bool {
        let orders = group_order_set(group);
        let created = self
            .schemas
            .iter()
            .filter(|schema| orders.contains(&schema.validity.begin_order))
            .collect::<Vec<_>>();
        created.len() == 1 && created[0].schema_id.0 == 0 && created[0].name == "main"
    }

    fn only_replaces_tables_without_schema_change(&self, group: &PublicSnapshot) -> bool {
        let orders = group_order_set(group);
        if self.schemas.iter().any(|schema| {
            row_touches_orders(
                schema.validity.begin_order,
                schema.validity.end_order,
                &orders,
            )
        }) {
            return false;
        }
        if self.views.iter().any(|view| {
            row_touches_orders(view.validity.begin_order, view.validity.end_order, &orders)
        }) {
            return false;
        }

        let mut replaced = false;
        for order in orders {
            for table in self.tables.iter().filter(|table| {
                table.validity.begin_order == order || table.validity.end_order == Some(order)
            }) {
                let paired = if table.validity.begin_order == order {
                    self.tables.iter().any(|previous| {
                        previous.table_id == table.table_id
                            && previous.validity.end_order == Some(order)
                            && previous.same_user_visible_schema_as(table)
                    })
                } else {
                    self.tables.iter().any(|next| {
                        next.table_id == table.table_id
                            && next.validity.begin_order == order
                            && table.same_user_visible_schema_as(next)
                    })
                };
                if !paired {
                    return false;
                }
                replaced = true;
            }
        }
        replaced
    }
}

#[derive(Clone)]
struct PublicSnapshotCoalesceFacts {
    schemas: Arc<Vec<SchemaRow>>,
    tables: Arc<Vec<TableRow>>,
    views: Arc<Vec<ViewRow>>,
    macros: Arc<Vec<MacroRow>>,
}

impl PublicSnapshotCoalesceFacts {
    fn for_catalog(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let version = load_catalog_snapshot_version(kv, catalog)?.map_or(
                PublicSnapshotCoalesceFactsVersion::LatestOrder(latest_order),
                PublicSnapshotCoalesceFactsVersion::Maintained,
            );
            let key = PublicSnapshotCoalesceFactsCacheKey {
                namespace: kv.catalog_cache_namespace().without_read_context(),
                catalog,
                version,
            };
            let cache = public_snapshot_coalesce_facts_cache();
            if let Some(facts) = cache.get(key) {
                return Ok(facts);
            }
            let facts = Self::load(kv, catalog, latest_order)?;
            cache.insert(key, facts.clone());
            Ok(facts)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog, latest_order)
        }
    }

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        Ok(Self {
            schemas: Arc::new(list_schema_rows_for_snapshot_cache(
                kv,
                catalog,
                latest_order,
            )?),
            tables: Arc::new(list_table_rows_with_snapshot_cache(
                kv,
                catalog,
                latest_order,
            )?),
            views: Arc::new(list_view_rows_for_snapshot_cache(
                kv,
                catalog,
                latest_order,
            )?),
            macros: Arc::new(list_macro_rows_for_snapshot_cache(
                kv,
                catalog,
                latest_order,
            )?),
        })
    }
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PublicSnapshotCoalesceFactsCacheKey {
    namespace: crate::CatalogCacheNamespace,
    catalog: CatalogId,
    version: PublicSnapshotCoalesceFactsVersion,
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PublicSnapshotCoalesceFactsVersion {
    Maintained(u64),
    LatestOrder(CatalogOrderId),
}

#[cfg(not(test))]
static PUBLIC_SNAPSHOT_COALESCE_FACTS_CACHE: OnceLock<
    BoundedCache<PublicSnapshotCoalesceFactsCacheKey, PublicSnapshotCoalesceFacts>,
> = OnceLock::new();

#[cfg(not(test))]
fn public_snapshot_coalesce_facts_cache()
-> &'static BoundedCache<PublicSnapshotCoalesceFactsCacheKey, PublicSnapshotCoalesceFacts> {
    static_bounded_cache(&PUBLIC_SNAPSHOT_COALESCE_FACTS_CACHE, 64)
}

fn group_order_set(group: &PublicSnapshot) -> BTreeSet<CatalogOrderId> {
    group.orders.iter().copied().collect()
}

fn row_touches_orders(
    begin_order: CatalogOrderId,
    end_order: Option<CatalogOrderId>,
    orders: &BTreeSet<CatalogOrderId>,
) -> bool {
    orders.contains(&begin_order) || end_order.is_some_and(|order| orders.contains(&order))
}

fn data_file_changes_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order_kind: CatalogOrderKind,
) -> CatalogResult<BTreeMap<CatalogOrderId, Vec<SnapshotDataFileChange>>> {
    let prefix = snapshot_data_file_change_prefix(catalog);
    let mut by_order = BTreeMap::<CatalogOrderId, Vec<SnapshotDataFileChange>>::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let Some(tail) = item.key.strip_prefix(prefix.as_slice()) else {
            return Err(crate::CatalogError::InvalidKey(
                "snapshot data-file change key has wrong prefix".to_owned(),
            ));
        };
        if tail.len() < CatalogOrderId::LEN {
            return Err(crate::CatalogError::InvalidKey(
                "snapshot data-file change order is truncated".to_owned(),
            ));
        }
        let order = CatalogOrderId::from_bytes(
            order_kind,
            tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
                crate::CatalogError::InvalidKey(
                    "snapshot data-file change order is truncated".to_owned(),
                )
            })?,
        );
        by_order
            .entry(order)
            .or_default()
            .push(decode_snapshot_data_file_change_key(
                &prefix, &item.key, order_kind,
            )?);
    }
    Ok(by_order)
}

fn inline_file_deletions_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>> {
    let mut by_order = BTreeMap::<CatalogOrderId, BTreeSet<crate::TableId>>::new();
    for row in CatalogInlineDeletionReadContext::for_catalog(kv, catalog)?.rows() {
        let begin_order = row.validity.begin_order;
        by_order
            .entry(begin_order)
            .or_default()
            .insert(row.table_id);
    }
    Ok(by_order)
}

fn delete_file_changes_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order_kind: CatalogOrderKind,
) -> CatalogResult<BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>> {
    let prefix = order_delete_file_change_prefix(catalog);
    let mut by_order = BTreeMap::<CatalogOrderId, BTreeSet<crate::TableId>>::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let (order, table_id) =
            decode_order_delete_file_change_key(&prefix, &item.key, order_kind)?;
        by_order.entry(order).or_default().insert(table_id);
    }
    Ok(by_order)
}

fn should_merge_public_snapshot_groups(
    context: &PublicSnapshotCoalesceContext,
    previous: &PublicSnapshot,
    current: &PublicSnapshot,
) -> CatalogResult<bool> {
    if should_merge_metadata_groups_in_same_ducklake_commit(context, previous, current) {
        return Ok(true);
    }
    if should_merge_metadata_with_created_table_group(context, previous, current) {
        return Ok(true);
    }
    if should_merge_data_with_created_table_group(context, previous, current) {
        return Ok(true);
    }
    Ok(should_merge_schema_helper_table_group(
        context, previous, current,
    ))
}

fn should_merge_metadata_groups_in_same_ducklake_commit(
    context: &PublicSnapshotCoalesceContext,
    previous: &PublicSnapshot,
    current: &PublicSnapshot,
) -> bool {
    if previous.representative.sequence != current.representative.sequence {
        return false;
    }
    if context.group_has_data_changes(previous) || context.group_has_data_changes(current) {
        return false;
    }
    context.group_has_metadata_changes(previous) && context.group_has_metadata_changes(current)
}

fn should_merge_metadata_with_created_table_group(
    context: &PublicSnapshotCoalesceContext,
    previous: &PublicSnapshot,
    current: &PublicSnapshot,
) -> bool {
    if previous.representative.sequence != current.representative.sequence {
        return false;
    }
    if context.group_has_data_changes(current) {
        return false;
    }
    !context
        .created_table_ids(previous)
        .is_disjoint(&context.touched_table_ids(current))
}

fn should_merge_data_with_created_table_group(
    context: &PublicSnapshotCoalesceContext,
    previous: &PublicSnapshot,
    current: &PublicSnapshot,
) -> bool {
    if previous.representative.sequence != current.representative.sequence {
        return false;
    }
    let current_inserted_tables = context.inserted_table_ids(current);
    if current_inserted_tables.is_empty() {
        return false;
    }
    !context
        .created_table_ids(previous)
        .is_disjoint(&current_inserted_tables)
}

fn should_merge_schema_helper_table_group(
    context: &PublicSnapshotCoalesceContext,
    previous: &PublicSnapshot,
    current: &PublicSnapshot,
) -> bool {
    if context.group_has_data_changes(current) {
        return false;
    }
    if !context.group_has_metadata_changes(current) {
        return true;
    }
    if context.only_replaces_tables_without_schema_change(current) {
        return true;
    }
    if context.group_has_data_changes(previous) {
        return false;
    }
    context.only_creates_default_main_schema(previous)
        && !context.created_table_ids(current).is_empty()
}

#[derive(Clone, Copy)]
enum MetadataEvent<'a, T> {
    Begin(&'a T),
    End(&'a T),
}

struct SnapshotMetadataChangeIndex<'a> {
    schemas: BTreeMap<CatalogOrderId, Vec<MetadataEvent<'a, SchemaRow>>>,
    tables: BTreeMap<CatalogOrderId, Vec<MetadataEvent<'a, TableRow>>>,
    table_begins: BTreeMap<(CatalogOrderId, crate::TableId), &'a TableRow>,
    table_ends: BTreeMap<(CatalogOrderId, crate::TableId), &'a TableRow>,
    views: BTreeMap<CatalogOrderId, Vec<MetadataEvent<'a, ViewRow>>>,
    view_begins: BTreeMap<(CatalogOrderId, crate::TableId), &'a ViewRow>,
    macros: BTreeMap<CatalogOrderId, Vec<MetadataEvent<'a, MacroRow>>>,
}

impl<'a> SnapshotMetadataChangeIndex<'a> {
    fn new(
        schemas: &'a [SchemaRow],
        tables: &'a [TableRow],
        views: &'a [ViewRow],
        macros: &'a [MacroRow],
    ) -> Self {
        let mut index = Self {
            schemas: BTreeMap::new(),
            tables: BTreeMap::new(),
            table_begins: BTreeMap::new(),
            table_ends: BTreeMap::new(),
            views: BTreeMap::new(),
            view_begins: BTreeMap::new(),
            macros: BTreeMap::new(),
        };
        for schema in schemas {
            index
                .schemas
                .entry(schema.validity.begin_order)
                .or_default()
                .push(MetadataEvent::Begin(schema));
            if let Some(order) = schema.validity.end_order {
                index
                    .schemas
                    .entry(order)
                    .or_default()
                    .push(MetadataEvent::End(schema));
            }
        }
        for table in tables {
            index
                .tables
                .entry(table.validity.begin_order)
                .or_default()
                .push(MetadataEvent::Begin(table));
            index
                .table_begins
                .insert((table.validity.begin_order, table.table_id), table);
            if let Some(order) = table.validity.end_order {
                index
                    .tables
                    .entry(order)
                    .or_default()
                    .push(MetadataEvent::End(table));
                index.table_ends.insert((order, table.table_id), table);
            }
        }
        for view in views {
            index
                .views
                .entry(view.validity.begin_order)
                .or_default()
                .push(MetadataEvent::Begin(view));
            index
                .view_begins
                .insert((view.validity.begin_order, view.view_id), view);
            if let Some(order) = view.validity.end_order {
                index
                    .views
                    .entry(order)
                    .or_default()
                    .push(MetadataEvent::End(view));
            }
        }
        for macro_row in macros {
            index
                .macros
                .entry(macro_row.validity.begin_order)
                .or_default()
                .push(MetadataEvent::Begin(macro_row));
            if let Some(order) = macro_row.validity.end_order {
                index
                    .macros
                    .entry(order)
                    .or_default()
                    .push(MetadataEvent::End(macro_row));
            }
        }
        index
    }

    fn changes_at(
        &self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        order: CatalogOrderId,
    ) -> CatalogResult<Vec<String>> {
        let mut changes = Vec::new();
        for event in self.schemas.get(&order).into_iter().flatten() {
            match event {
                MetadataEvent::Begin(schema) => {
                    changes.push(format!("created_schema:{}", quoted_value(&schema.name)));
                }
                MetadataEvent::End(schema) => {
                    changes.push(format!("dropped_schema:{}", schema.schema_id.0));
                }
            }
        }

        let table_events = self.tables.get(&order).map_or(&[][..], Vec::as_slice);
        let altered_tables = table_events
            .iter()
            .filter_map(|event| match event {
                MetadataEvent::End(table) => self
                    .table_begins
                    .get(&(order, table.table_id))
                    .filter(|next| !table.same_user_visible_schema_as(next))
                    .map(|_| table.table_id),
                MetadataEvent::Begin(_) => None,
            })
            .collect::<BTreeSet<_>>();
        let renamed_tables = table_events
            .iter()
            .filter_map(|event| match event {
                MetadataEvent::Begin(table) => self
                    .table_ends
                    .get(&(order, table.table_id))
                    .filter(|previous| {
                        previous.name != table.name || previous.schema_id != table.schema_id
                    })
                    .map(|_| table.table_id),
                MetadataEvent::End(_) => None,
            })
            .collect::<BTreeSet<_>>();
        for event in table_events {
            match event {
                MetadataEvent::Begin(table) => {
                    if self
                        .table_ends
                        .get(&(order, table.table_id))
                        .is_some_and(|previous| previous.same_user_visible_schema_as(table))
                    {
                        continue;
                    }
                    if altered_tables.contains(&table.table_id)
                        && !renamed_tables.contains(&table.table_id)
                    {
                        continue;
                    }
                    let schema_name = schema_name_at(kv, catalog, table.schema_id, order)?;
                    changes.push(format!(
                        "created_table:{}.{}",
                        quoted_value(&schema_name),
                        quoted_value(&table.name)
                    ));
                    if !renamed_tables.contains(&table.table_id)
                        && (table.partition.is_some() || table.sort.is_some())
                    {
                        changes.push(format!("altered_table:{}", table.table_id.0));
                    }
                }
                MetadataEvent::End(table) => {
                    if altered_tables.contains(&table.table_id) {
                        changes.push(format!("altered_table:{}", table.table_id.0));
                    } else if self
                        .table_begins
                        .get(&(order, table.table_id))
                        .is_some_and(|next| table.same_user_visible_schema_as(next))
                    {
                        continue;
                    } else {
                        changes.push(format!("dropped_table:{}", table.table_id.0));
                    }
                }
            }
        }

        let view_events = self.views.get(&order).map_or(&[][..], Vec::as_slice);
        let altered_views = view_events
            .iter()
            .filter_map(|event| match event {
                MetadataEvent::End(view)
                    if self.view_begins.contains_key(&(order, view.view_id)) =>
                {
                    Some(view.view_id)
                }
                MetadataEvent::Begin(_) | MetadataEvent::End(_) => None,
            })
            .collect::<BTreeSet<_>>();
        for event in view_events {
            match event {
                MetadataEvent::Begin(view) => {
                    if altered_views.contains(&view.view_id) {
                        continue;
                    }
                    let schema_name = schema_name_at(kv, catalog, view.schema_id, order)?;
                    changes.push(format!(
                        "created_view:{}.{}",
                        quoted_value(&schema_name),
                        quoted_value(&view.name)
                    ));
                }
                MetadataEvent::End(view) => {
                    if altered_views.contains(&view.view_id) {
                        changes.push(format!("altered_view:{}", view.view_id.0));
                    } else {
                        changes.push(format!("dropped_view:{}", view.view_id.0));
                    }
                }
            }
        }

        for event in self.macros.get(&order).into_iter().flatten() {
            let (macro_row, prefix) = match event {
                MetadataEvent::Begin(macro_row) => {
                    let prefix = if macro_row
                        .implementations
                        .iter()
                        .any(|implementation| implementation.macro_type == "table")
                    {
                        "created_table_macro"
                    } else {
                        "created_scalar_macro"
                    };
                    (macro_row, prefix)
                }
                MetadataEvent::End(macro_row) => {
                    let prefix = if macro_row
                        .implementations
                        .iter()
                        .any(|implementation| implementation.macro_type == "table")
                    {
                        "dropped_table_macro"
                    } else {
                        "dropped_scalar_macro"
                    };
                    changes.push(format!("{prefix}:{}", macro_row.macro_id.0));
                    continue;
                }
            };
            let schema_name = schema_name_at(kv, catalog, macro_row.schema_id, order)?;
            changes.push(format!(
                "{prefix}:{}.{}",
                quoted_value(&schema_name),
                quoted_value(&macro_row.name)
            ));
        }
        Ok(changes)
    }
}

fn public_snapshot_changes_made(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: &PublicSnapshot,
    metadata_changes: &SnapshotMetadataChangeIndex<'_>,
    context: &PublicSnapshotCoalesceContext,
) -> CatalogResult<String> {
    let mut changes = Vec::new();
    for order in &snapshot.orders {
        let order_changes = snapshot_changes_made_from_facts(
            kv,
            catalog,
            *order,
            metadata_changes,
            SnapshotChangeFacts::from_context(context, *order),
        )?;
        if !order_changes.is_empty() {
            changes.push(order_changes);
        }
    }
    Ok(changes.join(","))
}

pub fn snapshot_changes_made(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<String> {
    let schemas = list_schema_rows(kv, catalog)?;
    let tables = list_table_rows(kv, catalog)?;
    let views = list_view_rows(kv, catalog)?;
    let macros = list_macro_rows(kv, catalog)?;
    let metadata = SnapshotMetadataChangeIndex::new(&schemas, &tables, &views, &macros);
    snapshot_changes_made_with_metadata(kv, catalog, order, &metadata)
}

fn snapshot_changes_made_with_metadata(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    metadata: &SnapshotMetadataChangeIndex<'_>,
) -> CatalogResult<String> {
    let facts = SnapshotChangeFacts::load(kv, catalog, order)?;
    snapshot_changes_made_from_facts(kv, catalog, order, metadata, facts)
}

struct SnapshotChangeFacts {
    data_file_changes: Vec<SnapshotDataFileChange>,
    delete_marked: BTreeSet<crate::TableId>,
    inlined_inserted: BTreeSet<crate::TableId>,
    inlined_deleted: BTreeSet<crate::TableId>,
    inline_file_deleted: BTreeSet<crate::TableId>,
    flushed_inlined: BTreeSet<crate::TableId>,
    explicit_rewrite_deletes: BTreeSet<crate::TableId>,
}

impl SnapshotChangeFacts {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        Ok(Self {
            data_file_changes: snapshot_data_file_changes_at(kv, catalog, order)?,
            delete_marked: snapshot_delete_file_changed_table_ids_at(kv, catalog, order)?,
            inlined_inserted: snapshot_inline_row_changed_table_ids_at(
                kv,
                catalog,
                order,
                InlineRowChangeKind::Inserted,
            )?,
            inlined_deleted: snapshot_inline_row_changed_table_ids_at(
                kv,
                catalog,
                order,
                InlineRowChangeKind::Deleted,
            )?,
            inline_file_deleted: inline_file_deletion_changed_table_ids_at(kv, catalog, order)?,
            flushed_inlined: snapshot_flushed_inline_table_ids_at(kv, catalog, order)?,
            explicit_rewrite_deletes: snapshot_operation_table_ids_at(
                kv,
                catalog,
                order,
                SnapshotOperationKind::RewriteDelete,
            )?,
        })
    }

    fn from_context(context: &PublicSnapshotCoalesceContext, order: CatalogOrderId) -> Self {
        Self {
            data_file_changes: context
                .data_changes
                .get(&order)
                .cloned()
                .unwrap_or_default(),
            delete_marked: context
                .delete_file_changes
                .get(&order)
                .cloned()
                .unwrap_or_default(),
            inlined_inserted: context
                .inline_rows
                .tables(order, InlineRowChangeKind::Inserted),
            inlined_deleted: context
                .inline_rows
                .tables(order, InlineRowChangeKind::Deleted),
            inline_file_deleted: context
                .inline_file_deletions
                .get(&order)
                .cloned()
                .unwrap_or_default(),
            flushed_inlined: context
                .operations
                .get(&(order, SnapshotOperationKind::InlineFlush))
                .cloned()
                .unwrap_or_default(),
            explicit_rewrite_deletes: context
                .operations
                .get(&(order, SnapshotOperationKind::RewriteDelete))
                .cloned()
                .unwrap_or_default(),
        }
    }
}

fn snapshot_changes_made_from_facts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    metadata: &SnapshotMetadataChangeIndex<'_>,
    facts: SnapshotChangeFacts,
) -> CatalogResult<String> {
    let mut added = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for change in &facts.data_file_changes {
        match change.kind {
            DataFileChangeKind::Added => {
                added.insert(change.table_id);
            }
            DataFileChangeKind::Removed => {
                removed.insert(change.table_id);
            }
        }
    }
    let mut delete_marked = facts.delete_marked;
    let flushed_inlined = facts
        .flushed_inlined
        .intersection(&added)
        .copied()
        .collect::<BTreeSet<_>>();
    let rewrites = added
        .intersection(&removed)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut rewrite_delete_candidates = delete_marked
        .union(&facts.inline_file_deleted)
        .copied()
        .collect::<BTreeSet<_>>();
    rewrite_delete_candidates.extend(facts.explicit_rewrite_deletes);
    let rewrite_deletes = rewrites
        .intersection(&rewrite_delete_candidates)
        .copied()
        .collect::<BTreeSet<_>>();
    let merge_adjacent = rewrites
        .difference(&rewrite_deletes)
        .copied()
        .collect::<BTreeSet<_>>();
    let inserted = added
        .difference(&rewrites)
        .copied()
        .filter(|table_id| !flushed_inlined.contains(table_id))
        .collect::<BTreeSet<_>>();
    let deleted = removed
        .difference(&rewrites)
        .copied()
        .collect::<BTreeSet<_>>();
    delete_marked.extend(deleted);
    delete_marked = delete_marked
        .difference(&rewrite_deletes)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut changes = metadata.changes_at(kv, catalog, order)?;
    changes.extend(
        facts
            .inlined_inserted
            .into_iter()
            .map(|table_id| format!("inlined_insert:{}", table_id.0)),
    );
    changes.extend(
        facts
            .inlined_deleted
            .into_iter()
            .chain(facts.inline_file_deleted)
            .map(|table_id| format!("inlined_delete:{}", table_id.0)),
    );
    changes.extend(
        flushed_inlined
            .into_iter()
            .map(|table_id| format!("flushed_inlined:{}", table_id.0)),
    );
    changes.extend(
        inserted
            .into_iter()
            .map(|table_id| format!("inserted_into_table:{}", table_id.0)),
    );
    changes.extend(
        delete_marked
            .into_iter()
            .map(|table_id| format!("deleted_from_table:{}", table_id.0)),
    );
    changes.extend(
        merge_adjacent
            .into_iter()
            .map(|table_id| format!("merge_adjacent:{}", table_id.0)),
    );
    changes.extend(
        rewrite_deletes
            .into_iter()
            .map(|table_id| format!("rewrite_delete:{}", table_id.0)),
    );
    Ok(changes.join(","))
}

fn snapshot_inline_row_changed_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
) -> CatalogResult<BTreeSet<crate::TableId>> {
    #[cfg(test)]
    {
        snapshot_inline_row_changed_table_ids_at_uncached(kv, catalog, order, kind)
    }
    #[cfg(not(test))]
    {
        Ok(inline_row_change_index(kv, catalog)?.tables(order, kind))
    }
}

#[cfg(test)]
fn snapshot_inline_row_changed_table_ids_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
) -> CatalogResult<BTreeSet<crate::TableId>> {
    let prefix = crate::keys::family_prefix(catalog, crate::keys::KeyFamily::InlineRowChange);
    let table_start = prefix.len();
    let order_start = table_start + 8 + 1;
    let kind_index = order_start + CatalogOrderId::LEN + 1;
    let mut tables = BTreeSet::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        if item.key.len() <= kind_index || item.key[table_start + 8] != b'/' {
            continue;
        }
        if item.key[order_start..order_start + CatalogOrderId::LEN] != order.as_bytes() {
            continue;
        }
        if InlineRowChangeKind::from_code(item.key[kind_index])? != kind {
            continue;
        }
        let table_id = crate::TableId(u64::from_be_bytes(
            item.key[table_start..table_start + 8]
                .try_into()
                .map_err(|_| {
                    crate::CatalogError::InvalidKey(
                        "inline row change table id is truncated".to_owned(),
                    )
                })?,
        ));
        tables.insert(table_id);
    }
    Ok(tables)
}

#[derive(Clone)]
struct InlineRowChangeIndex {
    inserted: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
    deleted: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
}

impl InlineRowChangeIndex {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        order_kind: CatalogOrderKind,
    ) -> CatalogResult<Self> {
        let prefix = inline_table_change_prefix(catalog);
        let order_start = prefix.len();
        let kind_index = order_start + CatalogOrderId::LEN + 1;
        let table_start = kind_index + 2;
        let mut inserted = BTreeMap::<CatalogOrderId, BTreeSet<crate::TableId>>::new();
        let mut deleted = BTreeMap::<CatalogOrderId, BTreeSet<crate::TableId>>::new();
        for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
            if item.key.len() != table_start + 8
                || item.key[order_start + CatalogOrderId::LEN] != b'/'
                || item.key[kind_index + 1] != b'/'
            {
                continue;
            }
            let order = CatalogOrderId::from_bytes(
                order_kind,
                item.key[order_start..order_start + CatalogOrderId::LEN]
                    .try_into()
                    .map_err(|_| {
                        crate::CatalogError::InvalidKey(
                            "inline row change order is truncated".to_owned(),
                        )
                    })?,
            );
            let table_id = crate::TableId(u64::from_be_bytes(
                item.key[table_start..table_start + 8]
                    .try_into()
                    .map_err(|_| {
                        crate::CatalogError::InvalidKey(
                            "inline table change table id is truncated".to_owned(),
                        )
                    })?,
            ));
            match InlineRowChangeKind::from_code(item.key[kind_index])? {
                InlineRowChangeKind::Inserted => {
                    inserted.entry(order).or_default().insert(table_id)
                }
                InlineRowChangeKind::Deleted => deleted.entry(order).or_default().insert(table_id),
            };
        }
        Ok(Self { inserted, deleted })
    }

    fn has_any(&self, order: CatalogOrderId) -> bool {
        self.inserted
            .get(&order)
            .is_some_and(|tables| !tables.is_empty())
            || self
                .deleted
                .get(&order)
                .is_some_and(|tables| !tables.is_empty())
    }

    fn tables(&self, order: CatalogOrderId, kind: InlineRowChangeKind) -> BTreeSet<crate::TableId> {
        let source = match kind {
            InlineRowChangeKind::Inserted => &self.inserted,
            InlineRowChangeKind::Deleted => &self.deleted,
        };
        source.get(&order).cloned().unwrap_or_default()
    }
}

#[cfg(not(test))]
fn inline_row_change_index(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<InlineRowChangeIndex> {
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(InlineRowChangeIndex {
            inserted: BTreeMap::new(),
            deleted: BTreeMap::new(),
        });
    };
    let key = CatalogVersionCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
    };
    let cache = inline_row_change_index_cache();
    if let Some(index) = cache.get(key) {
        return Ok(index);
    }
    let index = InlineRowChangeIndex::load(kv, catalog, latest.order.kind())?;
    cache.insert(key, index.clone());
    Ok(index)
}

fn snapshot_flushed_inline_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<BTreeSet<crate::TableId>> {
    snapshot_operation_table_ids_at(kv, catalog, order, SnapshotOperationKind::InlineFlush)
}

fn schema_name_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    schema_id: crate::SchemaId,
    order: CatalogOrderId,
) -> CatalogResult<String> {
    if schema_id.0 == 0 {
        return Ok("main".to_owned());
    }
    let schema_name = load_schema_at(kv, catalog, schema_id, order)?
        .map(|schema| schema.name)
        .unwrap_or_else(|| schema_id.0.to_string());
    Ok(schema_name)
}

fn quoted_value(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn snapshot_delete_file_changed_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<BTreeSet<crate::TableId>> {
    let prefix = order_delete_file_change_prefix(catalog);
    let mut tables = BTreeSet::new();
    for item in kv.scan_range(
        &order_delete_file_change_scan_start(catalog, order),
        &order_delete_file_change_scan_end(catalog, order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let (change_order, table_id) =
            decode_order_delete_file_change_key(&prefix, &item.key, order.kind())?;
        if change_order != order {
            continue;
        }
        tables.insert(table_id);
    }
    Ok(tables)
}

fn decode_order_delete_file_change_key(
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<(CatalogOrderId, crate::TableId)> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(crate::CatalogError::InvalidKey(
            "order delete-file change key has wrong prefix".to_owned(),
        ));
    };
    let order_end = CatalogOrderId::LEN;
    let table_start = order_end + 1;
    let table_end = table_start + 8;
    let expected_len = table_end + 1 + 8;
    if tail.len() != expected_len {
        return Err(crate::CatalogError::InvalidKey(format!(
            "order delete-file change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    if tail[order_end] != b'/' || tail[table_end] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "order delete-file change key separator is invalid".to_owned(),
        ));
    }
    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("delete change order is truncated".to_owned())
        })?,
    );
    let table_id = crate::TableId(u64::from_be_bytes(
        tail[table_start..table_end].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("delete change table id is truncated".to_owned())
        })?,
    ));
    Ok((order, table_id))
}

#[derive(Clone)]
pub(crate) struct SnapshotDataFileChange {
    pub(crate) table_id: crate::TableId,
    pub(crate) kind: DataFileChangeKind,
    pub(crate) data_file_id: crate::DataFileId,
}

pub(crate) fn snapshot_data_file_changes_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<Vec<SnapshotDataFileChange>> {
    let prefix = snapshot_data_file_change_prefix(catalog);
    let mut start = prefix.clone();
    start.extend_from_slice(&order.as_bytes());
    let mut end = start.clone();
    end.push(0xff);
    kv.scan_range(&start, &end, RangeDirection::Forward, usize::MAX)?
        .into_iter()
        .map(|item| decode_snapshot_data_file_change_key(&prefix, &item.key, order.kind()))
        .collect()
}

fn decode_snapshot_data_file_change_key(
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<SnapshotDataFileChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(crate::CatalogError::InvalidKey(
            "snapshot data-file change key has wrong prefix".to_owned(),
        ));
    };
    let expected_len = CatalogOrderId::LEN + 1 + 8 + 1 + 1 + 1 + 8;
    if tail.len() != expected_len {
        return Err(crate::CatalogError::InvalidKey(format!(
            "snapshot data-file change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }

    let order_end = CatalogOrderId::LEN;
    let table_start = order_end + 1;
    let table_end = table_start + 8;
    let kind_index = table_end + 1;
    if tail[order_end] != b'/' || tail[table_end] != b'/' || tail[kind_index + 1] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "snapshot data-file change key separators are invalid".to_owned(),
        ));
    }

    let _order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end]
            .try_into()
            .map_err(|_| crate::CatalogError::InvalidKey("change order is truncated".to_owned()))?,
    );
    let table_id = crate::TableId(u64::from_be_bytes(
        tail[table_start..table_end]
            .try_into()
            .map_err(|_| crate::CatalogError::InvalidKey("table id is truncated".to_owned()))?,
    ));
    let data_file_start = kind_index + 2;
    let data_file_id = crate::DataFileId(u64::from_be_bytes(
        tail[data_file_start..]
            .try_into()
            .map_err(|_| crate::CatalogError::InvalidKey("data file id is truncated".to_owned()))?,
    ));
    Ok(SnapshotDataFileChange {
        table_id,
        kind: DataFileChangeKind::from_code(tail[kind_index])?,
        data_file_id,
    })
}

#[cfg(test)]
#[path = "runtime_snapshots_tests.rs"]
mod runtime_snapshots_tests;
