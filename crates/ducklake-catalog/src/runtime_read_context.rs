use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(not(test))]
use crate::schema_version_state::load_catalog_snapshot_version;
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, ColumnId, DataFileId, DataFileRow,
    DuckLakeSnapshotId, FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow,
    OrderedCatalogKv, RangeDirection, RawSnapshotSequence, SchemaId, SchemaRow, SnapshotRow,
    TableId, TableRow,
    data_file_store::list_all_current_data_files,
    file_partitions::list_file_partition_values_for_data_files,
    file_stats::list_file_column_stats_for_data_files,
    inline_data::inline_file_deletion_begin_order,
    keys::{KeyFamily, family_prefix, inline_file_deletion_table_prefix},
    list_file_column_stats, list_snapshots,
    runtime_snapshots::SnapshotReadContext,
    store::latest_snapshot,
    table_store::list_current_table_rows,
};

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CatalogReadContext {
    latest: Option<SnapshotRow>,
    snapshots: Arc<Vec<SnapshotRow>>,
    current_tables: Arc<Vec<TableRow>>,
    current_data_files: Arc<Vec<DataFileRow>>,
    current_file_column_stats: Arc<Vec<FileColumnStatsRow>>,
    current_file_partition_values: Arc<Vec<FilePartitionValueRow>>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CatalogCurrentFilesContext {
    current_data_files: Arc<Vec<DataFileRow>>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CatalogCurrentFileColumnStatsContext {
    latest: Option<SnapshotRow>,
    current_file_column_stats: Arc<Vec<FileColumnStatsRow>>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CatalogCurrentFilePartitionValuesContext {
    current_file_partition_values: Arc<Vec<FilePartitionValueRow>>,
}

#[derive(Clone)]
pub(crate) struct CatalogSnapshotRequestContext {
    latest: Option<SnapshotRow>,
    snapshots: Option<SnapshotReadContext>,
    facts: Option<CatalogSnapshotFacts>,
}

#[derive(Clone)]
struct CatalogSnapshotFacts {
    latest: Option<SnapshotRow>,
    schemas: Arc<Vec<SchemaRow>>,
    tables: Arc<Vec<TableRow>>,
    table_scope: CatalogSnapshotTableScope,
    current_tables: Arc<Vec<TableRow>>,
    views: Arc<Vec<crate::ViewRow>>,
    macros: Arc<Vec<crate::MacroRow>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatalogSnapshotTableScope {
    CompleteHistory,
    CurrentOnly,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct InlineDeletionReadContext {
    rows: Arc<Vec<InlineFileDeletionRow>>,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CatalogInlineDeletionReadContext {
    rows: Arc<Vec<InlineFileDeletionRow>>,
}

#[allow(dead_code)]
impl CatalogInlineDeletionReadContext {
    pub(crate) fn for_catalog(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog);
            let cache = catalog_inline_deletion_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog)
        }
    }

    pub(crate) fn rows(&self) -> &[InlineFileDeletionRow] {
        &self.rows
    }

    fn load(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Self> {
        let mut rows = Vec::new();
        for item in kv.scan_prefix(
            &family_prefix(catalog, KeyFamily::InlineFileDeletion),
            RangeDirection::Forward,
            usize::MAX,
        )? {
            let mut row = InlineFileDeletionRow::decode(&item.value)?;
            row.validity.begin_order =
                inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
            rows.push(row);
        }
        rows.sort_by_key(|row| {
            (
                row.table_id,
                row.data_file_id,
                row.row_id,
                row.validity.begin_order,
            )
        });
        Ok(Self {
            rows: Arc::new(rows),
        })
    }
}

#[allow(dead_code)]
impl InlineDeletionReadContext {
    pub(crate) fn for_table(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
    ) -> CatalogResult<Self> {
        Self::for_scope(kv, catalog, table_id, None)
    }

    pub(crate) fn for_file(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        file_id: DataFileId,
    ) -> CatalogResult<Self> {
        Self::for_scope(kv, catalog, table_id, Some(file_id))
    }

    fn for_scope(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        file_id: Option<DataFileId>,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = InlineDeletionContextKey {
                namespace: kv.catalog_cache_namespace(),
                catalog,
                table_id,
                file_id,
            };
            let cache = inline_deletion_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog, table_id, file_id)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog, table_id, file_id)
        }
    }

    pub(crate) fn rows_at(
        &self,
        snapshot_order: crate::CatalogOrderId,
    ) -> Vec<InlineFileDeletionRow> {
        self.rows
            .iter()
            .filter(|row| row.validity.is_visible_at(snapshot_order))
            .cloned()
            .collect()
    }

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        file_id: Option<DataFileId>,
    ) -> CatalogResult<Self> {
        let prefix = file_id.map_or_else(
            || inline_file_deletion_table_prefix(catalog, table_id),
            |file| crate::keys::inline_file_deletion_file_prefix(catalog, table_id, file),
        );
        let mut rows = Vec::new();
        for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
            let mut row = InlineFileDeletionRow::decode(&item.value)?;
            row.validity.begin_order =
                inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
            rows.push(row);
        }
        rows.sort_by_key(|row| (row.data_file_id, row.row_id, row.validity.begin_order));
        Ok(Self {
            rows: Arc::new(rows),
        })
    }
}

#[cfg_attr(test, allow(dead_code))]
pub(crate) fn inline_file_deletions_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeMap<DataFileId, BTreeSet<u64>>> {
    #[cfg(not(test))]
    {
        let key = InlineDeletionSnapshotContextKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            table_id,
            snapshot_order,
        };
        let cache = inline_deletion_snapshot_context_cache();
        if let Some(grouped) = cache.get(key) {
            return Ok(grouped);
        }
        let grouped = group_inline_file_deletions(
            InlineDeletionReadContext::for_table(kv, catalog, table_id)?.rows_at(snapshot_order),
        );
        cache.insert(key, grouped.clone());
        Ok(grouped)
    }
    #[cfg(test)]
    {
        Ok(group_inline_file_deletions(
            InlineDeletionReadContext::for_table(kv, catalog, table_id)?.rows_at(snapshot_order),
        ))
    }
}

#[cfg_attr(test, allow(dead_code))]
fn group_inline_file_deletions(
    rows: Vec<InlineFileDeletionRow>,
) -> BTreeMap<DataFileId, BTreeSet<u64>> {
    let mut grouped = BTreeMap::<DataFileId, BTreeSet<u64>>::new();
    for row in rows {
        grouped
            .entry(row.data_file_id)
            .or_default()
            .insert(row.row_id);
    }
    grouped
}

#[allow(dead_code)]
impl CatalogCurrentFilesContext {
    pub(crate) fn for_current_files(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return Ok(Self {
                current_data_files: Arc::default(),
            });
        };
        #[cfg(test)]
        let _ = latest;
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog, latest.order);
            let cache = current_files_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog)
        }
    }

    pub(crate) fn current_data_files(&self) -> &[DataFileRow] {
        &self.current_data_files
    }

    pub(crate) fn current_data_files_for_table(&self, table_id: TableId) -> &[DataFileRow] {
        let start = self
            .current_data_files
            .partition_point(|row| row.table_id < table_id);
        let end = self.current_data_files[start..].partition_point(|row| row.table_id == table_id)
            + start;
        &self.current_data_files[start..end]
    }

    fn load(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Self> {
        Ok(Self {
            current_data_files: Arc::new(list_all_current_data_files(kv, catalog)?),
        })
    }
}

#[allow(dead_code)]
impl CatalogCurrentFileColumnStatsContext {
    pub(crate) fn for_current_file_column_stats(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        let Some(latest) = crate::store::prepared_latest_snapshot(
            kv,
            catalog,
            crate::catalog_request_read_set::CatalogHeaderProjection::FileStatistics,
        )?
        else {
            return Ok(Self {
                latest: None,
                current_file_column_stats: Arc::default(),
            });
        };
        let current_tables = list_current_table_rows(kv, catalog)?;
        let current_files_context = CatalogCurrentFilesContext::for_current_files(kv, catalog)?;
        #[cfg(not(test))]
        {
            let key = current_file_column_stats_context_key(
                kv,
                catalog,
                &current_tables,
                current_files_context.current_data_files(),
            )?;
            let cache = current_file_column_stats_context_cache();
            if let Some(mut context) = cache.get_ref(&key) {
                context.latest = Some(latest);
                return Ok(context);
            }
            let context = Self::load(
                kv,
                catalog,
                latest,
                &current_tables,
                current_files_context.current_data_files(),
            )?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(
                kv,
                catalog,
                latest,
                &current_tables,
                current_files_context.current_data_files(),
            )
        }
    }

    pub(crate) fn latest(&self) -> Option<SnapshotRow> {
        self.latest.clone()
    }

    pub(crate) fn current_file_column_stats(&self) -> &[FileColumnStatsRow] {
        &self.current_file_column_stats
    }

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        latest: SnapshotRow,
        current_tables: &[TableRow],
        current_data_files: &[DataFileRow],
    ) -> CatalogResult<Self> {
        Ok(Self {
            latest: Some(latest),
            current_file_column_stats: Arc::new(current_file_column_stats(
                kv,
                catalog,
                current_tables,
                current_data_files,
            )?),
        })
    }
}

#[allow(dead_code)]
impl CatalogCurrentFilePartitionValuesContext {
    pub(crate) fn for_current_file_partition_values(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return Ok(Self {
                current_file_partition_values: Arc::default(),
            });
        };
        #[cfg(test)]
        let _ = latest;
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog, latest.order);
            let cache = current_file_partition_values_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load(kv, catalog)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog)
        }
    }

    pub(crate) fn current_file_partition_values(&self) -> &[FilePartitionValueRow] {
        &self.current_file_partition_values
    }

    fn load(kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Self> {
        let current_files_context = CatalogCurrentFilesContext::for_current_files(kv, catalog)?;
        Ok(Self {
            current_file_partition_values: Arc::new(current_file_partition_values(
                kv,
                catalog,
                current_files_context.current_data_files(),
            )?),
        })
    }
}

#[allow(dead_code)]
impl CatalogReadContext {
    pub(crate) fn for_current_metadata(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        let latest = latest_snapshot(kv, catalog)?;
        let Some(latest_row) = latest else {
            return Ok(Self::empty());
        };
        #[cfg(not(test))]
        {
            let key = (kv.catalog_cache_namespace(), catalog, latest_row.order);
            let cache = current_metadata_context_cache();
            if let Some(context) = cache.get(key) {
                return Ok(context);
            }
            let context = Self::load_current_metadata(kv, catalog, Some(latest_row))?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load_current_metadata(kv, catalog, Some(latest_row))
        }
    }

    pub(crate) fn latest(&self) -> Option<SnapshotRow> {
        self.latest.clone()
    }

    pub(crate) fn snapshots(&self) -> &[SnapshotRow] {
        &self.snapshots
    }

    pub(crate) fn current_tables(&self) -> &[TableRow] {
        &self.current_tables
    }

    pub(crate) fn current_data_files(&self) -> &[DataFileRow] {
        &self.current_data_files
    }

    pub(crate) fn current_file_column_stats(&self) -> &[FileColumnStatsRow] {
        &self.current_file_column_stats
    }

    pub(crate) fn current_file_partition_values(&self) -> &[FilePartitionValueRow] {
        &self.current_file_partition_values
    }

    fn load_current_metadata(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        latest: Option<SnapshotRow>,
    ) -> CatalogResult<Self> {
        let snapshots = list_snapshots(kv, catalog)?;
        let current_tables = list_current_table_rows(kv, catalog)?;
        let current_data_files = list_all_current_data_files(kv, catalog)?;
        let current_file_partition_values =
            current_file_partition_values(kv, catalog, &current_data_files)?;
        let current_file_column_stats =
            current_file_column_stats(kv, catalog, &current_tables, &current_data_files)?;
        Ok(Self {
            latest,
            snapshots: Arc::new(snapshots),
            current_tables: Arc::new(current_tables),
            current_data_files: Arc::new(current_data_files),
            current_file_column_stats: Arc::new(current_file_column_stats),
            current_file_partition_values: Arc::new(current_file_partition_values),
        })
    }

    fn empty() -> Self {
        Self {
            latest: None,
            snapshots: Arc::default(),
            current_tables: Arc::default(),
            current_data_files: Arc::default(),
            current_file_column_stats: Arc::default(),
            current_file_partition_values: Arc::default(),
        }
    }
}

impl CatalogSnapshotRequestContext {
    pub(crate) fn for_current_catalog(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<Self> {
        Ok(Self {
            latest: crate::store::prepared_latest_snapshot(
                kv,
                catalog,
                crate::catalog_request_read_set::CatalogHeaderProjection::Catalog,
            )?,
            snapshots: None,
            facts: None,
        })
    }

    pub(crate) fn resolve_snapshot(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot_id: DuckLakeSnapshotId,
        kind: crate::runtime_catalog_snapshot::CatalogSnapshotIdKind,
    ) -> CatalogResult<SnapshotRow> {
        match kind {
            crate::runtime_catalog_snapshot::CatalogSnapshotIdKind::PublicSnapshot => {
                let latest = self.latest.clone();
                if let Some(snapshot) = latest
                    .as_ref()
                    .filter(|snapshot| snapshot.sequence.0 == snapshot_id.0)
                {
                    return Ok(snapshot.clone());
                }
                let snapshot_context = self.snapshot_context(kv, catalog)?;
                snapshot_context
                    .public_snapshot(snapshot_id)
                    .or_else(|| {
                        latest_or_next_public_snapshot(
                            latest.as_ref(),
                            snapshot_context,
                            snapshot_id,
                        )
                    })
                    .ok_or_else(|| missing_catalog_snapshot_error(snapshot_id))
            }
            crate::runtime_catalog_snapshot::CatalogSnapshotIdKind::DuckLakeSequence => {
                if let Some(snapshot) = self.latest_or_next_raw_snapshot(snapshot_id) {
                    return Ok(snapshot);
                }
                crate::snapshot_by_raw_sequence(kv, catalog, RawSnapshotSequence(snapshot_id.0))?
                    .or_else(|| self.latest_committed_snapshot())
                    .ok_or_else(|| missing_catalog_snapshot_error(snapshot_id))
            }
        }
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
            self.snapshots = Some(SnapshotReadContext::for_current_catalog(kv, catalog)?);
        }
        self.snapshots.as_ref().ok_or_else(|| {
            crate::CatalogError::Decode("snapshot context was not initialized".to_owned())
        })
    }

    pub(crate) fn snapshot_context_for(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot: SnapshotRow,
    ) -> CatalogResult<CatalogSnapshotReadContext> {
        if self
            .facts
            .as_ref()
            .is_none_or(|facts| !facts.can_render_snapshot(snapshot.order))
        {
            self.facts = Some(match self.snapshots.as_ref() {
                Some(snapshots) => {
                    CatalogSnapshotFacts::from_snapshot_context(kv, catalog, snapshots)?
                }
                None => CatalogSnapshotFacts::for_catalog(kv, catalog, snapshot.order)?,
            });
        }
        self.facts
            .as_ref()
            .map(|facts| facts.context_for_snapshot(snapshot))
            .ok_or_else(|| {
                crate::CatalogError::Decode(
                    "catalog snapshot facts were not initialized".to_owned(),
                )
            })
    }
}

fn latest_or_next_public_snapshot(
    latest: Option<&SnapshotRow>,
    snapshots: &SnapshotReadContext,
    snapshot_id: DuckLakeSnapshotId,
) -> Option<SnapshotRow> {
    let latest_public_snapshot_id = snapshots.latest_public_snapshot_id()?;
    let latest = latest.cloned()?;
    (snapshot_id > latest_public_snapshot_id).then_some(latest)
}

fn missing_catalog_snapshot_error(snapshot_id: DuckLakeSnapshotId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!(
        "snapshot {snapshot_id} does not exist and is not the next commit snapshot"
    ))
}

impl CatalogSnapshotFacts {
    fn from_snapshot_context(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshots: &SnapshotReadContext,
    ) -> CatalogResult<Self> {
        Ok(Self {
            latest: crate::store::latest_snapshot_uncached(kv, catalog).row?,
            schemas: Arc::new(snapshots.schemas().to_vec()),
            tables: Arc::new(snapshots.tables().to_vec()),
            table_scope: CatalogSnapshotTableScope::CompleteHistory,
            current_tables: Arc::new(crate::table_store::list_current_table_rows_uncached(
                kv, catalog,
            )?),
            views: Arc::new(snapshots.views().to_vec()),
            macros: Arc::new(snapshots.macros().to_vec()),
        })
    }

    fn for_catalog(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        requested_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let latest = crate::store::prepared_snapshot_read(
            kv,
            catalog,
            crate::catalog_request_read_set::CatalogHeaderProjection::Catalog,
        )
        .row?;
        let table_scope = table_scope_for_request(latest.as_ref(), requested_order);
        #[cfg(not(test))]
        {
            let key =
                catalog_snapshot_facts_context_key(kv, catalog, latest.as_ref(), table_scope)?;
            let cache = catalog_snapshot_facts_context_cache();
            if let Some(mut context) = cache.get(key) {
                context.latest = latest;
                return Ok(context);
            }
            let context = Self::load(kv, catalog, latest, table_scope)?;
            cache.insert(key, context.clone());
            Ok(context)
        }
        #[cfg(test)]
        {
            Self::load(kv, catalog, latest, table_scope)
        }
    }

    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        latest: Option<SnapshotRow>,
        table_scope: CatalogSnapshotTableScope,
    ) -> CatalogResult<Self> {
        let current_tables = crate::table_store::list_current_table_rows_uncached(kv, catalog)?;
        let tables = match table_scope {
            CatalogSnapshotTableScope::CompleteHistory => {
                crate::table_store::list_table_rows_uncached(kv, catalog)?
            }
            CatalogSnapshotTableScope::CurrentOnly => Vec::new(),
        };
        Ok(Self {
            latest,
            schemas: Arc::new(crate::schema_store::list_schema_rows_uncached(kv, catalog)?),
            tables: Arc::new(tables),
            table_scope,
            current_tables: Arc::new(current_tables),
            views: Arc::new(crate::view_store::list_view_rows_uncached(kv, catalog)?),
            macros: Arc::new(crate::macro_store::list_macro_rows_uncached(kv, catalog)?),
        })
    }

    fn context_for_snapshot(&self, snapshot: SnapshotRow) -> CatalogSnapshotReadContext {
        let snapshot_order = snapshot.order;
        let tables = self.visible_tables_at(snapshot_order);
        let schemas = schemas_with_implicit_main_if_needed(
            visible_rows_at(self.schemas.clone(), snapshot_order),
            &tables,
        );
        CatalogSnapshotReadContext {
            snapshot,
            schemas,
            tables,
            views: visible_rows_at(self.views.clone(), snapshot_order),
            macros: visible_rows_at(self.macros.clone(), snapshot_order),
        }
    }

    fn visible_tables_at(&self, snapshot_order: CatalogOrderId) -> Arc<Vec<TableRow>> {
        if self
            .latest
            .as_ref()
            .is_some_and(|latest| latest.order == snapshot_order)
        {
            return self.current_tables.clone();
        }
        assert_eq!(
            self.table_scope,
            CatalogSnapshotTableScope::CompleteHistory,
            "historical catalog snapshot rendering requires complete table history"
        );
        visible_rows_at(self.tables.clone(), snapshot_order)
    }

    fn can_render_snapshot(&self, snapshot_order: CatalogOrderId) -> bool {
        self.latest
            .as_ref()
            .is_some_and(|latest| latest.order == snapshot_order)
            || self.table_scope == CatalogSnapshotTableScope::CompleteHistory
    }
}

fn table_scope_for_request(
    latest: Option<&SnapshotRow>,
    requested_order: CatalogOrderId,
) -> CatalogSnapshotTableScope {
    if latest.is_some_and(|row| row.order == requested_order) {
        CatalogSnapshotTableScope::CurrentOnly
    } else {
        CatalogSnapshotTableScope::CompleteHistory
    }
}

pub(crate) struct CatalogSnapshotReadContext {
    pub(crate) snapshot: SnapshotRow,
    pub(crate) schemas: Arc<Vec<SchemaRow>>,
    pub(crate) tables: Arc<Vec<TableRow>>,
    pub(crate) views: Arc<Vec<crate::ViewRow>>,
    pub(crate) macros: Arc<Vec<crate::MacroRow>>,
}

trait CatalogVisibleRow {
    fn is_visible_at(&self, snapshot_order: CatalogOrderId) -> bool;
}

impl CatalogVisibleRow for SchemaRow {
    fn is_visible_at(&self, snapshot_order: CatalogOrderId) -> bool {
        self.validity.is_visible_at(snapshot_order)
    }
}

impl CatalogVisibleRow for TableRow {
    fn is_visible_at(&self, snapshot_order: CatalogOrderId) -> bool {
        self.validity.is_visible_at(snapshot_order)
    }
}

impl CatalogVisibleRow for crate::ViewRow {
    fn is_visible_at(&self, snapshot_order: CatalogOrderId) -> bool {
        self.validity.is_visible_at(snapshot_order)
    }
}

impl CatalogVisibleRow for crate::MacroRow {
    fn is_visible_at(&self, snapshot_order: CatalogOrderId) -> bool {
        self.validity.is_visible_at(snapshot_order)
    }
}

fn visible_rows_at<Row: CatalogVisibleRow + Clone>(
    rows: Arc<Vec<Row>>,
    snapshot_order: CatalogOrderId,
) -> Arc<Vec<Row>> {
    if rows.iter().all(|row| row.is_visible_at(snapshot_order)) {
        return rows;
    }
    Arc::new(
        rows.iter()
            .filter(|row| row.is_visible_at(snapshot_order))
            .cloned()
            .collect(),
    )
}

#[cfg(not(test))]
pub(crate) fn invalidate_catalog_read_context(catalog: CatalogId) {
    crate::runtime_metrics::record_runtime_measurement("cache.catalog.invalidate", 1, 0);
    if let Some(cache) = CURRENT_METADATA_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.1 != catalog);
    }
    if let Some(cache) = CURRENT_FILES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.1 != catalog);
    }
    // This context is keyed by current file membership plus table stats version
    // keys, so generic snapshot churn must not discard it. Mutations that
    // change files or stats produce a different key and naturally miss the
    // cache.
    if let Some(cache) = CURRENT_FILE_PARTITION_VALUES_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.1 != catalog);
    }
    crate::data_file_store::invalidate_data_file_read_context(catalog);
}

#[cfg(not(test))]
pub(crate) fn invalidate_inline_deletion_read_context(catalog: CatalogId) {
    if let Some(cache) = INLINE_DELETION_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = INLINE_DELETION_SNAPSHOT_CONTEXT_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
    if let Some(cache) = CATALOG_INLINE_DELETION_CONTEXT_CACHE.get() {
        cache.retain(|(_, cached_catalog), _| *cached_catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_catalog_read_context(_catalog: CatalogId) {}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_inline_deletion_read_context(_catalog: CatalogId) {}

fn current_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    current_files: &[DataFileRow],
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let current_file_ids = current_file_ids(current_files);
    list_file_partition_values_for_data_files(kv, catalog, &current_file_ids)
}

fn current_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    current_tables: &[TableRow],
    current_files: &[DataFileRow],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let columns_by_table = columns_by_table(current_tables);
    if columns_by_table.is_empty() {
        let current_file_ids = current_file_ids(current_files);
        return Ok(list_file_column_stats(kv, catalog)?
            .into_iter()
            .filter(|row| current_file_ids.contains(&row.data_file_id))
            .collect());
    }
    list_file_column_stats_for_data_files(kv, catalog, current_files, &columns_by_table)
}

fn current_file_ids(current_files: &[DataFileRow]) -> BTreeSet<DataFileId> {
    current_files.iter().map(|row| row.data_file_id).collect()
}

fn columns_by_table(current_tables: &[TableRow]) -> BTreeMap<TableId, Vec<ColumnId>> {
    current_tables
        .iter()
        .map(|table| {
            (
                table.table_id,
                table
                    .columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect(),
            )
        })
        .collect()
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogSnapshotFactsContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    version: CatalogSnapshotFactsVersion,
    table_scope: CatalogSnapshotTableScopeKey,
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CatalogSnapshotFactsVersion {
    Maintained(u64),
    LatestOrder(CatalogOrderId),
    Empty,
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CatalogSnapshotTableScopeKey {
    CompleteHistory,
    CurrentOnly,
}

#[cfg(not(test))]
static CATALOG_SNAPSHOT_FACTS_CONTEXT_CACHE: OnceLock<
    BoundedCache<CatalogSnapshotFactsContextKey, CatalogSnapshotFacts>,
> = OnceLock::new();

#[cfg(not(test))]
fn catalog_snapshot_facts_context_cache()
-> &'static BoundedCache<CatalogSnapshotFactsContextKey, CatalogSnapshotFacts> {
    static_bounded_cache(&CATALOG_SNAPSHOT_FACTS_CONTEXT_CACHE, 1024)
}

#[cfg(not(test))]
fn catalog_snapshot_facts_context_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    latest: Option<&SnapshotRow>,
    table_scope: CatalogSnapshotTableScope,
) -> CatalogResult<CatalogSnapshotFactsContextKey> {
    let version = match load_catalog_snapshot_version(kv, catalog)? {
        Some(version) => CatalogSnapshotFactsVersion::Maintained(version),
        None => latest.map_or(CatalogSnapshotFactsVersion::Empty, |row| {
            CatalogSnapshotFactsVersion::LatestOrder(row.order)
        }),
    };
    let namespace = kv.catalog_cache_namespace();
    Ok(CatalogSnapshotFactsContextKey {
        namespace: match version {
            CatalogSnapshotFactsVersion::Maintained(_) => namespace.without_read_context(),
            _ => namespace,
        },
        catalog,
        version,
        table_scope: match table_scope {
            CatalogSnapshotTableScope::CompleteHistory => {
                CatalogSnapshotTableScopeKey::CompleteHistory
            }
            CatalogSnapshotTableScope::CurrentOnly => CatalogSnapshotTableScopeKey::CurrentOnly,
        },
    })
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CatalogSnapshotPayloadIdentity {
    facts: CatalogSnapshotFactsContextKey,
    historical_order: Option<CatalogOrderId>,
}

#[cfg(not(test))]
pub(crate) fn catalog_snapshot_payload_identity(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    requested: &SnapshotRow,
) -> CatalogResult<CatalogSnapshotPayloadIdentity> {
    // The physical header can be newer than the context's pinned logical
    // snapshot.
    let latest = crate::store::prepared_snapshot_read(
        kv,
        catalog,
        crate::catalog_request_read_set::CatalogHeaderProjection::Catalog,
    )
    .row?;
    let scope = table_scope_for_request(latest.as_ref(), requested.order);
    let facts = catalog_snapshot_facts_context_key(kv, catalog, latest.as_ref(), scope)?;
    let is_current_version = matches!(facts.version, CatalogSnapshotFactsVersion::Maintained(_))
        && scope == CatalogSnapshotTableScope::CurrentOnly;
    crate::runtime_metrics::record_runtime_measurement(
        if is_current_version {
            "catalog_payload.current_identity"
        } else {
            "catalog_payload.historical_identity"
        },
        1,
        0,
    );
    Ok(CatalogSnapshotPayloadIdentity {
        facts,
        historical_order: (!is_current_version).then_some(requested.order),
    })
}

#[cfg(not(test))]
fn current_file_column_stats_context_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    current_tables: &[TableRow],
    current_files: &[DataFileRow],
) -> CatalogResult<CurrentFileColumnStatsContextKey> {
    let mut current_file_ids = current_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<Vec<_>>();
    current_file_ids.sort_unstable();
    current_file_ids.dedup();
    let mut table_columns = current_tables
        .iter()
        .map(|row| {
            let mut column_ids = row
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            column_ids.sort_unstable();
            column_ids.dedup();
            (row.table_id, column_ids)
        })
        .collect::<Vec<_>>();
    table_columns.sort_unstable_by_key(|(table_id, _)| *table_id);
    table_columns.dedup_by_key(|(table_id, _)| *table_id);
    Ok(CurrentFileColumnStatsContextKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        current_file_ids,
        table_columns,
        stats_version: crate::catalog_request_read_set::fetch_value(
            kv,
            catalog,
            &crate::keys::catalog_file_stats_version_key(catalog),
        )?,
    })
}

fn schemas_with_implicit_main_if_needed(
    mut schemas: Arc<Vec<SchemaRow>>,
    tables: &[TableRow],
) -> Arc<Vec<SchemaRow>> {
    let has_main_schema = schemas.iter().any(|schema| schema.schema_id == SchemaId(0));
    let needs_main_schema = tables.iter().any(|table| table.schema_id == SchemaId(0));
    if !has_main_schema && needs_main_schema {
        Arc::make_mut(&mut schemas).push(SchemaRow::new(
            SchemaId(0),
            "00000000-0000-0000-0000-000000000000",
            "main",
            "main/",
            crate::CatalogOrderId::uuid_v7(0),
        ));
    }
    schemas
}

#[cfg(not(test))]
static CURRENT_METADATA_CONTEXT_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId, crate::CatalogOrderId), CatalogReadContext>,
> = OnceLock::new();

#[cfg(not(test))]
static CURRENT_FILES_CONTEXT_CACHE: OnceLock<
    BoundedCache<
        (CatalogCacheNamespace, CatalogId, crate::CatalogOrderId),
        CatalogCurrentFilesContext,
    >,
> = OnceLock::new();

#[cfg(not(test))]
static CURRENT_FILE_COLUMN_STATS_CONTEXT_CACHE: OnceLock<
    BoundedCache<CurrentFileColumnStatsContextKey, CatalogCurrentFileColumnStatsContext>,
> = OnceLock::new();

#[cfg(not(test))]
static CURRENT_FILE_PARTITION_VALUES_CONTEXT_CACHE: OnceLock<
    BoundedCache<
        (CatalogCacheNamespace, CatalogId, crate::CatalogOrderId),
        CatalogCurrentFilePartitionValuesContext,
    >,
> = OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CurrentFileColumnStatsContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    current_file_ids: Vec<DataFileId>,
    table_columns: Vec<(TableId, Vec<ColumnId>)>,
    stats_version: Option<Vec<u8>>,
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InlineDeletionContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    file_id: Option<DataFileId>,
}

#[cfg(not(test))]
static INLINE_DELETION_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineDeletionContextKey, InlineDeletionReadContext>,
> = OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InlineDeletionSnapshotContextKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
static INLINE_DELETION_SNAPSHOT_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineDeletionSnapshotContextKey, BTreeMap<DataFileId, BTreeSet<u64>>>,
> = OnceLock::new();

#[cfg(not(test))]
static CATALOG_INLINE_DELETION_CONTEXT_CACHE: OnceLock<
    BoundedCache<(CatalogCacheNamespace, CatalogId), CatalogInlineDeletionReadContext>,
> = OnceLock::new();

#[cfg(not(test))]
fn current_metadata_context_cache() -> &'static BoundedCache<
    (CatalogCacheNamespace, CatalogId, crate::CatalogOrderId),
    CatalogReadContext,
> {
    static_bounded_cache(&CURRENT_METADATA_CONTEXT_CACHE, 32)
}

#[cfg(not(test))]
fn current_files_context_cache() -> &'static BoundedCache<
    (CatalogCacheNamespace, CatalogId, crate::CatalogOrderId),
    CatalogCurrentFilesContext,
> {
    static_bounded_cache(&CURRENT_FILES_CONTEXT_CACHE, 32)
}

#[cfg(not(test))]
fn current_file_column_stats_context_cache()
-> &'static BoundedCache<CurrentFileColumnStatsContextKey, CatalogCurrentFileColumnStatsContext> {
    static_bounded_cache(&CURRENT_FILE_COLUMN_STATS_CONTEXT_CACHE, 32)
}

#[cfg(not(test))]
fn current_file_partition_values_context_cache() -> &'static BoundedCache<
    (CatalogCacheNamespace, CatalogId, crate::CatalogOrderId),
    CatalogCurrentFilePartitionValuesContext,
> {
    static_bounded_cache(&CURRENT_FILE_PARTITION_VALUES_CONTEXT_CACHE, 32)
}

#[cfg(not(test))]
fn inline_deletion_context_cache()
-> &'static BoundedCache<InlineDeletionContextKey, InlineDeletionReadContext> {
    static_bounded_cache(&INLINE_DELETION_CONTEXT_CACHE, 128)
}

#[cfg(not(test))]
fn inline_deletion_snapshot_context_cache()
-> &'static BoundedCache<InlineDeletionSnapshotContextKey, BTreeMap<DataFileId, BTreeSet<u64>>> {
    static_bounded_cache(&INLINE_DELETION_SNAPSHOT_CONTEXT_CACHE, 512)
}

#[cfg(not(test))]
fn catalog_inline_deletion_context_cache()
-> &'static BoundedCache<(CatalogCacheNamespace, CatalogId), CatalogInlineDeletionReadContext> {
    static_bounded_cache(&CATALOG_INLINE_DELETION_CONTEXT_CACHE, 16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataFileId;

    fn data_file(data_file_id: u64, table_id: u64) -> DataFileRow {
        DataFileRow::new(
            DataFileId(data_file_id),
            TableId(table_id),
            format!("file-{data_file_id}.parquet"),
            0,
            1,
            CatalogOrderId::uuid_v7(data_file_id.into()),
        )
    }

    #[test]
    fn given_sorted_catalog_current_files_when_listing_table_then_only_table_range_is_returned() {
        let context = CatalogCurrentFilesContext {
            current_data_files: Arc::new(vec![
                data_file(1, 1),
                data_file(2, 1),
                data_file(5, 3),
                data_file(8, 4),
                data_file(13, 4),
            ]),
        };

        let rows = context.current_data_files_for_table(TableId(4));

        assert_eq!(
            rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
            vec![DataFileId(8), DataFileId(13)]
        );
    }

    #[test]
    fn given_sorted_catalog_current_files_when_table_is_absent_then_empty_range_is_returned() {
        let context = CatalogCurrentFilesContext {
            current_data_files: Arc::new(vec![data_file(1, 1), data_file(5, 3), data_file(8, 4)]),
        };

        let rows = context.current_data_files_for_table(TableId(2));

        assert!(rows.is_empty());
    }
}
