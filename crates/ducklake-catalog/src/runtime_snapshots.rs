use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::{Arc, OnceLock};

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataFileChangeKind, DataFileId,
    DuckLakeSnapshotId, InlineRowChangeKind, MacroRow, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SchemaRow, SnapshotRow, TableRow, ViewRow,
    data_file_store::attach_delete_file_at,
    inline_data::{inline_file_deletion_changed_table_ids_at, inline_table_flushes_ending_at},
    keys::{
        data_file_key, inline_table_change_prefix, order_delete_file_change_prefix,
        order_delete_file_change_scan_end, order_delete_file_change_scan_start,
        snapshot_data_file_change_prefix,
    },
    latest_snapshot, list_all_snapshots, list_snapshots, list_snapshots_older_than,
    macro_store::{list_macro_rows, list_macro_rows_for_snapshot_cache},
    rows::DataFileRow,
    runtime_catalog_snapshot::snapshot_watermarks,
    runtime_read_context::CatalogInlineDeletionReadContext,
    schema_store::{list_schema_rows, list_schema_rows_for_snapshot_cache, load_schema_at},
    snapshot_operations::{SnapshotOperationKind, snapshot_operation_table_ids_at},
    table_store::{list_table_rows, list_table_rows_with_snapshot_cache},
    view_store::{list_view_rows, list_view_rows_for_snapshot_cache},
};
#[cfg(all(not(test), feature = "runtime-metrics"))]
use crate::{
    runtime_metrics::{RuntimeMetricStatus, record_runtime_request_elapsed},
    runtime_protocol::RuntimeCatalogBackend,
};

const EMPTY_SNAPSHOT_STRING_FIELD: &str = "\\0";

#[cfg(all(not(test), feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(Option<std::time::Instant>);

#[cfg(all(not(test), not(feature = "runtime-metrics")))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

#[cfg(not(test))]
impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(Some(std::time::Instant::now()))
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[inline]
    fn zero() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(None)
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        self.0
            .map(|started| u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

pub fn snapshot_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    let mut schema_version = 0;
    for change_order in catalog_schema_change_candidate_orders(kv, catalog, order)? {
        if catalog_schema_changed_at(kv, catalog, change_order)? {
            schema_version += 1;
        }
    }
    Ok(schema_version)
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
    let schemas = list_schema_rows(kv, catalog)?;
    let tables = list_table_rows(kv, catalog)?;
    let views = list_view_rows(kv, catalog)?;
    let macros = list_macro_rows(kv, catalog)?;
    let change_orders = catalog_schema_change_orders(&schemas, &tables, &views, &macros);
    let mut versions = BTreeMap::new();
    let mut schema_version = 0;
    let mut change_orders = change_orders.into_iter().peekable();
    for snapshot in list_all_snapshots(kv, catalog)? {
        while change_orders
            .peek()
            .is_some_and(|change_order| *change_order <= snapshot.order)
        {
            schema_version += 1;
            change_orders.next();
        }
        versions.insert(snapshot.order, schema_version);
    }
    Ok(versions)
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogVersionCacheKey {
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
    let mut candidate_orders = BTreeSet::new();
    for schema in schemas {
        push_all_validity_orders(
            &mut candidate_orders,
            schema.validity.begin_order,
            schema.validity.end_order,
        );
    }
    for table in tables {
        push_all_validity_orders(
            &mut candidate_orders,
            table.validity.begin_order,
            table.validity.end_order,
        );
    }
    for view in views {
        push_all_validity_orders(
            &mut candidate_orders,
            view.validity.begin_order,
            view.validity.end_order,
        );
    }
    for macro_row in macros {
        push_all_validity_orders(
            &mut candidate_orders,
            macro_row.validity.begin_order,
            macro_row.validity.end_order,
        );
    }
    candidate_orders
        .into_iter()
        .filter(|order| catalog_schema_changed_at_from_rows(*order, schemas, tables, views, macros))
        .collect()
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

fn catalog_schema_changed_at_from_rows(
    order: CatalogOrderId,
    schemas: &[SchemaRow],
    tables: &[TableRow],
    views: &[ViewRow],
    macros: &[MacroRow],
) -> bool {
    schemas.iter().any(|schema| {
        schema.validity.begin_order == order || schema.validity.end_order == Some(order)
    }) || table_schema_changed_at_from_rows(tables, order)
        || views.iter().any(|view| {
            view.validity.begin_order == order || view.validity.end_order == Some(order)
        })
        || macros.iter().any(|macro_row| {
            macro_row.validity.begin_order == order || macro_row.validity.end_order == Some(order)
        })
}

fn table_schema_changed_at_from_rows(tables: &[TableRow], order: CatalogOrderId) -> bool {
    for table in tables {
        if table.validity.begin_order == order {
            let previous = tables.iter().find(|previous| {
                previous.table_id == table.table_id && previous.validity.end_order == Some(order)
            });
            if previous.is_none_or(|previous| !previous.same_user_visible_schema_as(table)) {
                return true;
            }
        }
        if table.validity.end_order == Some(order)
            && !tables
                .iter()
                .any(|next| next.table_id == table.table_id && next.validity.begin_order == order)
        {
            return true;
        }
    }
    false
}

fn catalog_schema_change_candidate_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<BTreeSet<CatalogOrderId>> {
    let mut orders = BTreeSet::new();
    for schema in list_schema_rows(kv, catalog)? {
        push_validity_orders(
            &mut orders,
            schema.validity.begin_order,
            schema.validity.end_order,
            order,
        );
    }
    for table in list_table_rows(kv, catalog)? {
        push_validity_orders(
            &mut orders,
            table.validity.begin_order,
            table.validity.end_order,
            order,
        );
    }
    for view in list_view_rows(kv, catalog)? {
        push_validity_orders(
            &mut orders,
            view.validity.begin_order,
            view.validity.end_order,
            order,
        );
    }
    for macro_row in list_macro_rows(kv, catalog)? {
        push_validity_orders(
            &mut orders,
            macro_row.validity.begin_order,
            macro_row.validity.end_order,
            order,
        );
    }
    Ok(orders)
}

fn push_validity_orders(
    orders: &mut BTreeSet<CatalogOrderId>,
    begin_order: CatalogOrderId,
    end_order: Option<CatalogOrderId>,
    upper_bound: CatalogOrderId,
) {
    if begin_order <= upper_bound {
        orders.insert(begin_order);
    }
    if let Some(end_order) = end_order
        && end_order <= upper_bound
    {
        orders.insert(end_order);
    }
}

fn catalog_schema_changed_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<bool> {
    for schema in list_schema_rows(kv, catalog)? {
        if schema.validity.begin_order == order || schema.validity.end_order == Some(order) {
            return Ok(true);
        }
    }
    if table_schema_changed_at(kv, catalog, order)? {
        return Ok(true);
    }
    for view in list_view_rows(kv, catalog)? {
        if view.validity.begin_order == order || view.validity.end_order == Some(order) {
            return Ok(true);
        }
    }
    for macro_row in list_macro_rows(kv, catalog)? {
        if macro_row.validity.begin_order == order || macro_row.validity.end_order == Some(order) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_schema_changed_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<bool> {
    let table_rows = list_table_rows(kv, catalog)?;
    for table in &table_rows {
        if table.validity.begin_order == order {
            let previous = table_rows.iter().find(|previous| {
                previous.table_id == table.table_id && previous.validity.end_order == Some(order)
            });
            if previous.is_none_or(|previous| !previous.same_user_visible_schema_as(table)) {
                return Ok(true);
            }
        }
        if table.validity.end_order == Some(order)
            && !table_rows
                .iter()
                .any(|next| next.table_id == table.table_id && next.validity.begin_order == order)
        {
            return Ok(true);
        }
    }
    Ok(false)
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
    let mut out = format!("snapshot_count={}\n", snapshots.len());
    for public_sequence in 0..snapshots.len() {
        let snapshot_id = snapshots[public_sequence].representative.sequence.0;
        push_snapshot(
            &mut out,
            kv,
            catalog,
            snapshot_id,
            &snapshots,
            public_sequence,
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
    by_order: BTreeMap<CatalogOrderId, SnapshotRow>,
    by_public_sequence: BTreeMap<DuckLakeSnapshotId, SnapshotRow>,
    public_span_by_sequence: BTreeMap<DuckLakeSnapshotId, (CatalogOrderId, CatalogOrderId)>,
    by_ducklake_sequence: BTreeMap<DuckLakeSnapshotId, SnapshotRow>,
    ducklake_span_by_sequence: BTreeMap<DuckLakeSnapshotId, (CatalogOrderId, CatalogOrderId)>,
    #[allow(dead_code)]
    sequences_by_order: SharedOrderMap,
    schema_versions_by_order: SharedOrderMap,
    schemas: Vec<SchemaRow>,
    tables: Vec<TableRow>,
    views: Vec<ViewRow>,
    macros: Vec<MacroRow>,
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
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(catalog)
                && context.latest_order_is(latest)
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(catalog, context.clone());
            Ok(context)
        }
    }

    pub(crate) fn for_current_catalog_uncached(
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
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(catalog)
                && context.latest_order_is(latest)
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(catalog, context.clone());
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
            return Self::load(kv, catalog);
        }
        #[cfg(not(test))]
        {
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(catalog)
                && context.public_snapshot(snapshot_id).is_some()
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(catalog, context.clone());
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
            return Self::load(kv, catalog);
        }
        #[cfg(not(test))]
        {
            let cache = snapshot_read_context_cache();
            if let Some(context) = cache.get(catalog)
                && context.ducklake_snapshot(snapshot_id).is_some()
            {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(catalog, context.clone());
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

    pub(crate) fn schema_versions_by_order(&self) -> SharedOrderMap {
        self.schema_versions_by_order.clone()
    }

    pub(crate) fn schemas(&self) -> &[SchemaRow] {
        &self.schemas
    }

    pub(crate) fn tables(&self) -> &[TableRow] {
        &self.tables
    }

    pub(crate) fn views(&self) -> &[ViewRow] {
        &self.views
    }

    pub(crate) fn macros(&self) -> &[MacroRow] {
        &self.macros
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
        let schema_versions_by_order =
            schema_versions_by_order_from_loaded_facts(&by_order, &coalesce_context);
        Self {
            latest,
            by_order,
            by_public_sequence,
            public_span_by_sequence,
            by_ducklake_sequence,
            ducklake_span_by_sequence,
            sequences_by_order: SharedOrderMap::new(sequences_by_order),
            schema_versions_by_order: SharedOrderMap::new(schema_versions_by_order),
            schemas: coalesce_context.schemas,
            tables: coalesce_context.tables,
            views: coalesce_context.views,
            macros: coalesce_context.macros,
        }
    }
}

fn schema_versions_by_order_from_loaded_facts(
    snapshots_by_order: &BTreeMap<CatalogOrderId, SnapshotRow>,
    context: &PublicSnapshotCoalesceContext,
) -> BTreeMap<CatalogOrderId, u64> {
    let change_orders = catalog_schema_change_orders(
        &context.schemas,
        &context.tables,
        &context.views,
        &context.macros,
    );
    let mut versions = BTreeMap::new();
    let mut schema_version = 0;
    let mut change_orders = change_orders.into_iter().peekable();
    for order in snapshots_by_order.keys() {
        while change_orders
            .peek()
            .is_some_and(|change_order| change_order <= order)
        {
            schema_version += 1;
            change_orders.next();
        }
        versions.insert(*order, schema_version);
    }
    versions
}

#[cfg(not(test))]
static SNAPSHOT_READ_CONTEXT_CACHE: OnceLock<BoundedCache<CatalogId, SnapshotReadContext>> =
    OnceLock::new();

#[cfg(not(test))]
fn snapshot_read_context_cache() -> &'static BoundedCache<CatalogId, SnapshotReadContext> {
    static_bounded_cache(&SNAPSHOT_READ_CONTEXT_CACHE, 64)
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
    let context = SnapshotReadContext::for_ducklake_snapshot(kv, catalog, snapshot_id)?;
    if let Some(snapshot) = context.ducklake_snapshot(snapshot_id) {
        return Ok(Some(snapshot));
    }
    Ok(list_all_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence.0 == snapshot_id.0)
        .max_by_key(|snapshot| snapshot.order))
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
        let cache = snapshot_read_context_cache();
        if let Some(context) = cache.get(catalog)
            && context.latest_order_is(latest)
        {
            record_public_snapshot_sequences_cache("Hit", RuntimeMetricStage::zero());
            return Ok(context.sequences_by_order());
        }
        let started = RuntimeMetricStage::start();
        let context = SnapshotReadContext::load(kv, catalog)?;
        let sequences = context.sequences_by_order();
        record_public_snapshot_sequences_cache("Load", started);
        cache.insert(catalog, context);
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
        let cache = snapshot_read_context_cache();
        if let Some(context) = cache.get(catalog)
            && context.sequences_by_order().get(&required_order).is_some()
        {
            record_public_snapshot_sequences_cache("Hit", RuntimeMetricStage::zero());
            return Ok(context.sequences_by_order());
        }
        let started = RuntimeMetricStage::start();
        let context = SnapshotReadContext::load(kv, catalog)?;
        let sequences = context.sequences_by_order();
        record_public_snapshot_sequences_cache("Load", started);
        cache.insert(catalog, context);
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
    if payload.protect_latest {
        if let Some(latest) = list_snapshots(kv, catalog)?
            .into_iter()
            .max_by_key(|row| row.sequence)
        {
            snapshots.retain(|snapshot| snapshot.sequence != latest.sequence);
        }
    }
    Ok(snapshots)
}

fn push_snapshot(
    out: &mut String,
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    public_sequence: u64,
    snapshots: &[PublicSnapshot],
    snapshot_index: usize,
    schema_version: u64,
) -> CatalogResult<()> {
    let snapshot = &snapshots[snapshot_index];
    let watermarks = snapshot_watermarks(kv, catalog, snapshot.representative.order)?;
    let changes_made = public_snapshot_changes_made(kv, catalog, &snapshot)?;
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
    let mut groups = public_snapshot_groups(snapshots);
    let context = PublicSnapshotCoalesceContext::load(kv, catalog, &groups)?;
    let mut index = 1;
    while index < groups.len() {
        if should_merge_public_snapshot_groups(&context, &groups[index - 1], &groups[index])? {
            let current = groups.remove(index);
            groups[index - 1].orders.extend(current.orders);
        } else {
            index += 1;
        }
    }
    Ok((groups, context))
}

struct PublicSnapshotCoalesceContext {
    schemas: Vec<SchemaRow>,
    tables: Vec<TableRow>,
    views: Vec<ViewRow>,
    macros: Vec<MacroRow>,
    data_changes: BTreeMap<CatalogOrderId, Vec<SnapshotDataFileChange>>,
    inline_rows: InlineRowChangeIndex,
    inline_file_deletions: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
    delete_file_changes: BTreeMap<CatalogOrderId, BTreeSet<crate::TableId>>,
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
        Ok(Self {
            schemas: list_schema_rows_for_snapshot_cache(kv, catalog, latest_order)?,
            tables: list_table_rows_with_snapshot_cache(kv, catalog, latest_order)?,
            views: list_view_rows_for_snapshot_cache(kv, catalog, latest_order)?,
            macros: list_macro_rows_for_snapshot_cache(kv, catalog, latest_order)?,
            data_changes: data_file_changes_by_order(kv, catalog, order_kind)?,
            inline_rows: InlineRowChangeIndex::load(kv, catalog, order_kind)?,
            inline_file_deletions: inline_file_deletions_by_order(kv, catalog)?,
            delete_file_changes: delete_file_changes_by_order(kv, catalog, order_kind)?,
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

fn public_snapshot_changes_made(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: &PublicSnapshot,
) -> CatalogResult<String> {
    let mut changes = Vec::new();
    for order in &snapshot.orders {
        let order_changes = snapshot_changes_made(kv, catalog, *order)?;
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
    let mut added = BTreeSet::new();
    let mut removed = BTreeSet::new();
    let data_file_changes = snapshot_data_file_changes_at(kv, catalog, order)?;
    for change in &data_file_changes {
        match change.kind {
            DataFileChangeKind::Added => {
                added.insert(change.table_id);
            }
            DataFileChangeKind::Removed => {
                removed.insert(change.table_id);
            }
        }
    }
    let mut delete_marked = snapshot_delete_file_changed_table_ids_at(kv, catalog, order)?;
    delete_marked.extend(rewrite_source_delete_evidence_table_ids(
        kv,
        catalog,
        order,
        &data_file_changes,
    )?);
    let inlined_inserted = snapshot_inline_row_changed_table_ids_at(
        kv,
        catalog,
        order,
        InlineRowChangeKind::Inserted,
    )?;
    let inlined_deleted =
        snapshot_inline_row_changed_table_ids_at(kv, catalog, order, InlineRowChangeKind::Deleted)?;
    let inline_file_deleted = inline_file_deletion_changed_table_ids_at(kv, catalog, order)?;
    let flushed_inlined = snapshot_flushed_inline_table_ids_at(kv, catalog, order)?
        .intersection(&added)
        .copied()
        .collect::<BTreeSet<_>>();
    let rewrites = added
        .intersection(&removed)
        .copied()
        .collect::<BTreeSet<_>>();
    let explicit_rewrite_deletes =
        snapshot_operation_table_ids_at(kv, catalog, order, SnapshotOperationKind::RewriteDelete)?;
    let mut rewrite_delete_candidates = delete_marked
        .union(&inline_file_deleted)
        .copied()
        .collect::<BTreeSet<_>>();
    rewrite_delete_candidates.extend(explicit_rewrite_deletes);
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
    let mut changes = Vec::new();
    for schema in list_schema_rows(kv, catalog)? {
        if schema.validity.begin_order == order {
            changes.push(format!("created_schema:{}", quoted_value(&schema.name)));
        }
        if schema.validity.end_order == Some(order) {
            changes.push(format!("dropped_schema:{}", schema.schema_id.0));
        }
    }
    let table_rows = list_table_rows(kv, catalog)?;
    let altered_tables = table_rows
        .iter()
        .filter(|table| table.validity.end_order == Some(order))
        .filter(|dropped| {
            table_rows.iter().any(|created| {
                created.table_id == dropped.table_id
                    && created.validity.begin_order == order
                    && !dropped.same_user_visible_schema_as(created)
            })
        })
        .map(|table| table.table_id)
        .collect::<BTreeSet<_>>();
    let renamed_tables = renamed_table_ids_at_order(&table_rows, order);
    for table in &table_rows {
        if table.validity.begin_order == order {
            if table_rows.iter().any(|previous| {
                previous.table_id == table.table_id
                    && previous.validity.end_order == Some(order)
                    && previous.same_user_visible_schema_as(table)
            }) {
                continue;
            }
            if renamed_tables.contains(&table.table_id) {
                let schema_name = schema_name_at(kv, catalog, table.schema_id, order)?;
                changes.push(format!(
                    "created_table:{}.{}",
                    quoted_value(&schema_name),
                    quoted_value(&table.name)
                ));
                continue;
            }
            if altered_tables.contains(&table.table_id) {
                continue;
            }
            let schema_name = schema_name_at(kv, catalog, table.schema_id, order)?;
            changes.push(format!(
                "created_table:{}.{}",
                quoted_value(&schema_name),
                quoted_value(&table.name)
            ));
            if table.partition.is_some() || table.sort.is_some() {
                changes.push(format!("altered_table:{}", table.table_id.0));
            }
        }
        if table.validity.end_order == Some(order) {
            if altered_tables.contains(&table.table_id) {
                changes.push(format!("altered_table:{}", table.table_id.0));
            } else if table_rows.iter().any(|next| {
                next.table_id == table.table_id
                    && next.validity.begin_order == order
                    && table.same_user_visible_schema_as(next)
            }) {
                continue;
            } else {
                changes.push(format!("dropped_table:{}", table.table_id.0));
            }
        }
    }
    let view_rows = list_view_rows(kv, catalog)?;
    let altered_views = view_rows
        .iter()
        .filter(|view| view.validity.end_order == Some(order))
        .filter(|dropped| {
            view_rows.iter().any(|created| {
                created.view_id == dropped.view_id && created.validity.begin_order == order
            })
        })
        .map(|view| view.view_id)
        .collect::<BTreeSet<_>>();
    for view in view_rows {
        if view.validity.begin_order == order {
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
        if view.validity.end_order == Some(order) {
            if altered_views.contains(&view.view_id) {
                changes.push(format!("altered_view:{}", view.view_id.0));
            } else {
                changes.push(format!("dropped_view:{}", view.view_id.0));
            }
        }
    }
    for macro_row in list_macro_rows(kv, catalog)? {
        if macro_row.validity.begin_order == order {
            let schema_name = schema_name_at(kv, catalog, macro_row.schema_id, order)?;
            let change = if macro_row
                .implementations
                .iter()
                .any(|implementation| implementation.macro_type == "table")
            {
                "created_table_macro"
            } else {
                "created_scalar_macro"
            };
            changes.push(format!(
                "{change}:{}.{}",
                quoted_value(&schema_name),
                quoted_value(&macro_row.name)
            ));
        }
        if macro_row.validity.end_order == Some(order) {
            let change = if macro_row
                .implementations
                .iter()
                .any(|implementation| implementation.macro_type == "table")
            {
                "dropped_table_macro"
            } else {
                "dropped_scalar_macro"
            };
            changes.push(format!("{change}:{}", macro_row.macro_id.0));
        }
    }
    changes.extend(
        inlined_inserted
            .into_iter()
            .map(|table_id| format!("inlined_insert:{}", table_id.0)),
    );
    changes.extend(
        inlined_deleted
            .into_iter()
            .chain(inline_file_deleted)
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

fn rewrite_source_delete_evidence_table_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    changes: &[SnapshotDataFileChange],
) -> CatalogResult<BTreeSet<crate::TableId>> {
    let mut table_ids = BTreeSet::new();
    for change in changes
        .iter()
        .filter(|change| change.kind == DataFileChangeKind::Removed)
    {
        let Some(source) = load_snapshot_data_file(kv, catalog, change.data_file_id)? else {
            continue;
        };
        if attach_delete_file_at(kv, catalog, source, order)?
            .delete_file
            .is_some()
        {
            table_ids.insert(change.table_id);
        }
    }
    Ok(table_ids)
}

fn load_snapshot_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Option<DataFileRow>> {
    kv.get(&data_file_key(catalog, data_file_id))?
        .map(|value| DataFileRow::decode(&value))
        .transpose()
}

fn renamed_table_ids_at_order(
    table_rows: &[crate::TableRow],
    order: CatalogOrderId,
) -> BTreeSet<crate::TableId> {
    table_rows
        .iter()
        .filter(|table| table.validity.begin_order == order)
        .filter(|created| {
            table_rows.iter().any(|previous| {
                previous.table_id == created.table_id
                    && previous.validity.end_order == Some(order)
                    && (previous.name != created.name || previous.schema_id != created.schema_id)
            })
        })
        .map(|table| table.table_id)
        .collect()
}

fn snapshot_inline_row_changed_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
) -> CatalogResult<BTreeSet<crate::TableId>> {
    #[cfg(test)]
    {
        return snapshot_inline_row_changed_table_ids_at_uncached(kv, catalog, order, kind);
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
    inline_table_flushes_ending_at(kv, catalog, order)
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
