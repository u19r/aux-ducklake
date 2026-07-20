use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, OrderedCatalogKv,
    RawSnapshotSequence, SchemaId, SchemaRow, SnapshotCommitMetadata, SnapshotRow, TableId,
    TableRow, TableVersionReplacement, ViewRename, ViewRow, latest_snapshot,
    runtime_schema_change_ops::RuntimeMutableCatalog,
    runtime_snapshots::snapshot_schema_versions_by_order_shared,
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
    let versions = snapshot_schema_versions_by_order_shared(kv, catalog)?;
    let public_schema_version = versions
        .get(&latest.order)
        .copied()
        .ok_or(CatalogError::NotFound("catalog schema version"))?;
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

pub(super) trait CommitAttemptTableReplacements: MutableCatalogKv {
    fn commit_schema_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<SchemaRow>,
        dropped: Vec<SchemaId>,
    ) -> CatalogResult<()>;

    fn commit_table_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<TableRow>,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()>;

    fn commit_replace_tables_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        dropped_table_ids: &[TableId],
        tables: Vec<TableRow>,
        commit_metadata: Option<&SnapshotCommitMetadata>,
    ) -> CatalogResult<Vec<TableRow>>;

    fn commit_view_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        created: Vec<ViewRow>,
        renamed: Vec<ViewRename>,
        dropped: Vec<TableId>,
        changes: Vec<crate::ViewCommentChange>,
    ) -> CatalogResult<()>;
}

impl CommitAttemptTableReplacements for RuntimeMutableCatalog {
    fn commit_schema_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<SchemaRow>,
        dropped: Vec<SchemaId>,
    ) -> CatalogResult<()> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => {
                let _ = commit_metadata;
                kv.change_schemas_versionstamped_at(catalog, created, &dropped, sequence)?;
                Ok(())
            }
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                let _ = (catalog, sequence, commit_metadata, created, dropped);
                Err(crate::CatalogError::Backend(
                    "foundationdb runtime requires ducklake-catalog --features foundationdb"
                        .to_owned(),
                ))
            }
        }
    }

    fn commit_table_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<TableRow>,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.commit_table_changes_with_sequence_versionstamped(
                catalog,
                sequence,
                commit_metadata,
                created,
                replacements,
            ),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                let _ = (catalog, sequence, commit_metadata, created, replacements);
                Err(crate::CatalogError::Backend(
                    "foundationdb runtime requires ducklake-catalog --features foundationdb"
                        .to_owned(),
                ))
            }
        }
    }

    fn commit_replace_tables_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        dropped_table_ids: &[TableId],
        tables: Vec<TableRow>,
        _commit_metadata: Option<&SnapshotCommitMetadata>,
    ) -> CatalogResult<Vec<TableRow>> {
        if dropped_table_ids.is_empty() && tables.is_empty() {
            return Ok(Vec::new());
        }
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv
                .replace_tables_versionstamped_recoverable(
                    catalog,
                    dropped_table_ids,
                    tables,
                    Some(sequence),
                    None,
                )
                .inspect(|_tables| {
                    let _ = _commit_metadata;
                }),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                let _ = (
                    catalog,
                    sequence,
                    dropped_table_ids,
                    tables,
                    _commit_metadata,
                );
                Err(crate::CatalogError::Backend(
                    "foundationdb runtime requires ducklake-catalog --features foundationdb"
                        .to_owned(),
                ))
            }
        }
    }

    fn commit_view_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        created: Vec<ViewRow>,
        renamed: Vec<ViewRename>,
        dropped: Vec<TableId>,
        changes: Vec<crate::ViewCommentChange>,
    ) -> CatalogResult<()> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.change_views_versionstamped_at(
                catalog, created, renamed, &dropped, changes, sequence,
            ),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                let _ = (catalog, sequence, created, renamed, dropped, changes);
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
    fn commit_schema_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<SchemaRow>,
        dropped: Vec<SchemaId>,
    ) -> CatalogResult<()> {
        commit_schema_changes_at(self, catalog, sequence, commit_metadata, created, dropped)
    }

    fn commit_table_changes_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<TableRow>,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        commit_created_tables_at(self, catalog, sequence, commit_metadata, created)?;
        self.commit_table_replacements(catalog, previous_sequence(sequence)?, replacements)
    }

    fn commit_replace_tables_at(
        &mut self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        dropped_table_ids: &[TableId],
        tables: Vec<TableRow>,
        _commit_metadata: Option<&SnapshotCommitMetadata>,
    ) -> CatalogResult<Vec<TableRow>> {
        commit_replaced_tables_at(self, catalog, sequence, dropped_table_ids, tables)
    }

    fn commit_view_changes_at(
        &mut self,
        catalog: CatalogId,
        _sequence: RawSnapshotSequence,
        created: Vec<ViewRow>,
        renamed: Vec<ViewRename>,
        dropped: Vec<TableId>,
        changes: Vec<crate::ViewCommentChange>,
    ) -> CatalogResult<()> {
        for view in created {
            crate::commit_create_view_row(self, catalog, view)?;
        }
        for rename in renamed {
            crate::commit_rename_views(self, catalog, &[rename])?;
        }
        for change in changes {
            crate::commit_change_view_comment(self, catalog, &change)?;
        }
        for view_id in dropped {
            crate::commit_drop_views(self, catalog, &[view_id])?;
        }
        Ok(())
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
    stage_next_schema_version(kv, &mut batch, catalog)?;
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
    stage_next_schema_version(kv, &mut batch, catalog)?;
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
    stage_next_schema_version(kv, &mut batch, catalog)?;
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
