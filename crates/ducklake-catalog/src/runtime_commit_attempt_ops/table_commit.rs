use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, OrderedCatalogKv,
    RawSnapshotSequence, SchemaId, SchemaRow, SnapshotCommitMetadata, SnapshotRow, TableId,
    TableRow, TableVersionReplacement, ViewCommentChange, ViewRename, ViewRow, latest_snapshot,
    runtime_schema_change_ops::RuntimeMutableCatalog, schema_version_state::load_schema_version_at,
};

use crate::runtime_commit_attempt_ops::*;
#[cfg(test)]
use crate::{
    KvBatch, ValidityWindow,
    keys::{schema_object_key, table_object_key},
    schema_version_state::stage_next_schema_version,
    store::stage_snapshot,
    table_store::{
        load_current_table_row, stage_current_table_row, stage_remove_current_table_row,
        stage_table_visibility_row,
    },
};
#[derive(Debug, Default)]
pub(super) struct CommitMetadataResult {
    pub(super) changed_table_count: usize,
    pub(super) created_tables: Vec<CreatedTable>,
    pub(super) public_schema_changed: bool,
}

impl CommitMetadataResult {
    pub(super) fn table_id_remaps(&self) -> BTreeMap<TableId, TableId> {
        self.created_tables
            .iter()
            .filter(|table| table.requested_table_id != table.persisted.table_id)
            .map(|table| (table.requested_table_id, table.persisted.table_id))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(super) struct CurrentCatalogState {
    pub(super) latest: SnapshotRow,
    public_schema_version: u64,
}

impl CurrentCatalogState {
    pub(super) fn final_schema_version(&self, public_schema_changed: bool) -> u64 {
        self.public_schema_version + u64::from(public_schema_changed)
    }
}

pub(super) fn current_catalog_state(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<CurrentCatalogState> {
    let started = RuntimeMetricStage::start();
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let public_schema_version = load_schema_version_at(kv, catalog, latest.order)?;
    record_commit_attempt_stage("CurrentCatalogState", started);
    Ok(CurrentCatalogState {
        latest,
        public_schema_version,
    })
}

pub(super) fn public_schema_changed_by_metadata(
    created_schemas: bool,
    dropped_schemas: bool,
    table_changes: &TableCommitParts,
    dropped_tables: bool,
    replacement_tables: bool,
    view_comment_changes: bool,
) -> bool {
    created_schemas
        || dropped_schemas
        || !table_changes.created.is_empty()
        || table_changes.replacements.iter().any(|replacement| {
            !replacement
                .previous
                .same_user_visible_schema_as(&replacement.next)
        })
        || dropped_tables
        || replacement_tables
        || view_comment_changes
}

#[derive(Clone, Debug)]
pub(super) struct CreatedTable {
    pub(super) requested_table_id: TableId,
    pub(super) persisted: TableRow,
}

impl CreatedTable {
    pub(super) fn new(requested_table_id: TableId, persisted: TableRow) -> Self {
        Self {
            requested_table_id,
            persisted,
        }
    }

    pub(super) fn unremapped(persisted: TableRow) -> Self {
        Self::new(persisted.table_id, persisted)
    }
}

pub(super) struct TableCommitParts {
    pub(super) created: Vec<TableRow>,
    pub(super) replacements: Vec<TableVersionReplacement>,
    pub(super) created_tables: Vec<CreatedTable>,
}

pub(crate) struct MetadataCommitChanges {
    pub(crate) sequence: RawSnapshotSequence,
    pub(crate) commit_metadata: Option<SnapshotCommitMetadata>,
    pub(crate) public_schema_changed: bool,
    pub(crate) created_schemas: Vec<SchemaRow>,
    pub(crate) dropped_schema_ids: Vec<SchemaId>,
    pub(crate) created_tables: Vec<TableRow>,
    pub(crate) table_replacements: Vec<TableVersionReplacement>,
    pub(crate) dropped_table_ids: Vec<TableId>,
    pub(crate) replacement_tables: Vec<TableRow>,
    pub(crate) created_views: Vec<ViewRow>,
    pub(crate) view_renames: Vec<ViewRename>,
    pub(crate) dropped_view_ids: Vec<TableId>,
    pub(crate) view_comment_changes: Vec<ViewCommentChange>,
}

pub(super) trait CommitAttemptTableReplacements: MutableCatalogKv {
    fn commit_metadata_changes(
        &mut self,
        catalog: CatalogId,
        changes: MetadataCommitChanges,
    ) -> CatalogResult<Vec<TableRow>>;
}

impl CommitAttemptTableReplacements for RuntimeMutableCatalog {
    fn commit_metadata_changes(
        &mut self,
        catalog: CatalogId,
        changes: MetadataCommitChanges,
    ) -> CatalogResult<Vec<TableRow>> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.commit_metadata_changes_versionstamped(catalog, changes),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                let _ = (catalog, changes);
                Err(crate::CatalogError::Backend(
                    "foundationdb runtime requires ducklake-catalog --features foundationdb"
                        .to_owned(),
                ))
            }
        }
    }
}

#[cfg(test)]
impl CommitAttemptTableReplacements for crate::FakeOrderedCatalogKv {
    fn commit_metadata_changes(
        &mut self,
        catalog: CatalogId,
        changes: MetadataCommitChanges,
    ) -> CatalogResult<Vec<TableRow>> {
        commit_schema_changes_at(
            self,
            catalog,
            changes.sequence,
            changes.commit_metadata.as_ref(),
            changes.created_schemas,
            changes.dropped_schema_ids,
        )?;
        commit_created_tables_at(
            self,
            catalog,
            changes.sequence,
            changes.commit_metadata.as_ref(),
            changes.created_tables,
        )?;
        self.commit_table_replacements(
            catalog,
            previous_sequence(changes.sequence)?,
            changes.table_replacements,
        )?;
        let created = commit_replaced_tables_at(
            self,
            catalog,
            changes.sequence,
            &changes.dropped_table_ids,
            changes.replacement_tables,
        )?;
        for view in changes.created_views {
            crate::commit_create_view_row(self, catalog, view)?;
        }
        for rename in changes.view_renames {
            crate::commit_rename_views(self, catalog, &[rename])?;
        }
        for change in changes.view_comment_changes {
            crate::commit_change_view_comment(self, catalog, &change)?;
        }
        for view_id in changes.dropped_view_ids {
            crate::commit_drop_views(self, catalog, &[view_id])?;
        }
        Ok(created)
    }
}

#[cfg(test)]
pub(super) fn commit_replaced_tables_at(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    sequence: RawSnapshotSequence,
    dropped_table_ids: &[TableId],
    tables: Vec<TableRow>,
) -> CatalogResult<Vec<TableRow>> {
    if dropped_table_ids.is_empty() && tables.is_empty() {
        return Ok(Vec::new());
    }
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, sequence);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog, &snapshot)?;
    for table_id in dropped_table_ids {
        let mut table = load_current_table_row(kv, catalog, *table_id)?
            .ok_or(CatalogError::NotFound("table"))?;
        table.validity.end_order = Some(order);
        batch.put(
            table_object_key(catalog, table.table_id, table.validity.begin_order),
            table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &table);
        stage_remove_current_table_row(&mut batch, catalog, table.table_id);
    }
    let created = tables
        .into_iter()
        .map(|mut table| {
            table.validity = ValidityWindow::new(order, None);
            batch.put(
                table_object_key(catalog, table.table_id, order),
                table.encode(),
            );
            stage_table_visibility_row(&mut batch, catalog, &table);
            stage_current_table_row(&mut batch, catalog, &table);
            table
        })
        .collect::<Vec<_>>();
    kv.commit(batch)?;
    Ok(created)
}

#[cfg(test)]
pub(super) fn commit_schema_changes_at(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    sequence: RawSnapshotSequence,
    commit_metadata: Option<&SnapshotCommitMetadata>,
    mut created: Vec<SchemaRow>,
    dropped: Vec<SchemaId>,
) -> CatalogResult<()> {
    if created.is_empty() && dropped.is_empty() {
        return Ok(());
    }
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, sequence).with_optional_commit_metadata(commit_metadata);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog, &snapshot)?;
    for schema_id in dropped {
        let mut schema = crate::schema_store::load_schema_at(kv, catalog, schema_id, latest.order)?
            .ok_or(CatalogError::NotFound("schema"))?;
        schema.validity.end_order = Some(order);
        batch.put(
            schema_object_key(catalog, schema.schema_id, schema.validity.begin_order),
            schema.encode(),
        );
    }
    for schema in &mut created {
        schema.validity = ValidityWindow::new(order, None);
        batch.put(
            schema_object_key(catalog, schema.schema_id, order),
            schema.encode(),
        );
    }
    kv.commit(batch)
}

#[cfg(test)]
pub(super) fn commit_created_tables_at(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    sequence: RawSnapshotSequence,
    commit_metadata: Option<&SnapshotCommitMetadata>,
    tables: Vec<TableRow>,
) -> CatalogResult<()> {
    if tables.is_empty() {
        return Ok(());
    }
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, sequence).with_optional_commit_metadata(commit_metadata);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog, &snapshot)?;
    for mut table in tables {
        table.validity = ValidityWindow::new(order, None);
        batch.put(
            table_object_key(catalog, table.table_id, order),
            table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &table);
        stage_current_table_row(&mut batch, catalog, &table);
    }
    kv.commit(batch)
}

#[cfg(test)]
pub(super) fn previous_sequence(
    sequence: RawSnapshotSequence,
) -> CatalogResult<RawSnapshotSequence> {
    sequence
        .0
        .checked_sub(1)
        .map(RawSnapshotSequence)
        .ok_or_else(|| {
            CatalogError::InvalidMutation("commit snapshot id must be greater than 0".to_owned())
        })
}
