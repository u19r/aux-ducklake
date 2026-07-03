use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{
    RuntimeMetricStatus, record_runtime_method_elapsed, record_runtime_request,
};
use crate::{
    CatalogError, CatalogId, CatalogResult, CommitAttemptId, DuckLakeSnapshotId, MutableCatalogKv,
    OrderedCatalogKv, RawSnapshotSequence, SchemaId, SchemaRow, SnapshotCommitMetadata,
    SnapshotRow, TableColumnRow, TableId, TableRow, TableVersionReplacement, ViewRename, ViewRow,
    latest_snapshot, load_table_at, load_view_at,
    runtime_compaction_ops::commit_compaction_intent,
    runtime_data_mutation_ops::{
        commit_data_mutation, data_mutation_payload_values,
        proposed_commit_snapshot_covering_inline_flushes,
    },
    runtime_inline_ops::{delete_inline_rows, register_inline_rows, register_inline_tables},
    runtime_object_ops::{
        CHANGE_VIEW_COMMENT, CREATE_VIEWS, DROP_VIEWS, RENAME_VIEWS, create_view_rows,
        drop_view_ids, view_comment_change, view_renames,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_schema_change_ops::{RuntimeMutableCatalog, open_runtime_catalog},
    runtime_schema_change_payload::{
        ADD_COLUMNS, CHANGE_COLUMN_DEFAULTS, CHANGE_COLUMN_TYPES, CHANGE_COMMENTS,
        CHANGE_PARTITION_KEYS, CHANGE_SORT_KEYS, DROP_COLUMNS, DdlPayload, RENAME_COLUMNS,
        RENAME_TABLES, one_column_table, parse_column_drops, parse_column_rows,
        parse_comment_changes, parse_partition_changes, parse_sort_change, parse_table_renames,
    },
    runtime_schema_ops::{
        CREATE_SCHEMAS, DROP_SCHEMAS, create_schemas_payload_values, drop_schemas_payload_values,
    },
    runtime_snapshot_range::ProposedCommitSnapshot,
    runtime_snapshots::snapshot_schema_versions_by_order_shared,
    runtime_table_ops::{
        CREATE_TABLES, REPLACE_TABLES, create_tables_payload_values, replace_tables_payload_values,
    },
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
    schema_store::list_schemas_at,
    snapshot_by_ducklake_sequence, snapshot_by_public_sequence,
    table_store::{list_tables_at, load_current_table_row},
};

#[cfg(test)]
use crate::{
    KvBatch, ValidityWindow,
    keys::{schema_object_key, table_object_key},
    schema_version_state::stage_next_schema_version,
    store::stage_snapshot,
    table_store::{
        stage_current_table_row, stage_remove_current_table_row, stage_table_visibility_row,
    },
};

const COMMIT_ATTEMPT: &str = "CommitAttempt";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommitAttemptIntent {
    pub(crate) read_snapshot: Option<DuckLakeSnapshotId>,
    pub(crate) proposed_commit_snapshot: ProposedCommitSnapshot,
    pub(crate) commit_metadata: SnapshotCommitMetadata,
    pub(crate) metadata_intents: Vec<RuntimeMetadataIntent>,
    pub(crate) compaction_intents: Vec<RuntimeCompactionIntent>,
    pub(crate) data_mutation_payload: Vec<u8>,
    pub(crate) inline_payloads: Vec<RuntimeInlineIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeMetadataIntent {
    pub(crate) operation: RuntimeMetadataOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeMetadataOperation {
    CreateSchemas,
    DropSchemas,
    CreateTables,
    ReplaceTables,
    CreateViews,
    DropViews,
    RenameViews,
    AddColumns,
    RenameColumns,
    RenameTables,
    ChangeColumnDefaults,
    ChangeColumnTypes,
    ChangeComments,
    ChangeViewComment,
    DropColumns,
    ChangeSortKeys,
    ChangePartitionKeys,
}

impl RuntimeMetadataOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            CREATE_SCHEMAS => Ok(Self::CreateSchemas),
            DROP_SCHEMAS => Ok(Self::DropSchemas),
            CREATE_TABLES => Ok(Self::CreateTables),
            REPLACE_TABLES => Ok(Self::ReplaceTables),
            CREATE_VIEWS => Ok(Self::CreateViews),
            DROP_VIEWS => Ok(Self::DropViews),
            RENAME_VIEWS => Ok(Self::RenameViews),
            ADD_COLUMNS => Ok(Self::AddColumns),
            RENAME_COLUMNS => Ok(Self::RenameColumns),
            RENAME_TABLES => Ok(Self::RenameTables),
            CHANGE_COLUMN_DEFAULTS => Ok(Self::ChangeColumnDefaults),
            CHANGE_COLUMN_TYPES => Ok(Self::ChangeColumnTypes),
            CHANGE_COMMENTS => Ok(Self::ChangeComments),
            CHANGE_VIEW_COMMENT => Ok(Self::ChangeViewComment),
            DROP_COLUMNS => Ok(Self::DropColumns),
            CHANGE_SORT_KEYS => Ok(Self::ChangeSortKeys),
            CHANGE_PARTITION_KEYS => Ok(Self::ChangePartitionKeys),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support metadata operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CreateSchemas => CREATE_SCHEMAS,
            Self::DropSchemas => DROP_SCHEMAS,
            Self::CreateTables => CREATE_TABLES,
            Self::ReplaceTables => REPLACE_TABLES,
            Self::CreateViews => CREATE_VIEWS,
            Self::DropViews => DROP_VIEWS,
            Self::RenameViews => RENAME_VIEWS,
            Self::AddColumns => ADD_COLUMNS,
            Self::RenameColumns => RENAME_COLUMNS,
            Self::RenameTables => RENAME_TABLES,
            Self::ChangeColumnDefaults => CHANGE_COLUMN_DEFAULTS,
            Self::ChangeColumnTypes => CHANGE_COLUMN_TYPES,
            Self::ChangeComments => CHANGE_COMMENTS,
            Self::ChangeViewComment => CHANGE_VIEW_COMMENT,
            Self::DropColumns => DROP_COLUMNS,
            Self::ChangeSortKeys => CHANGE_SORT_KEYS,
            Self::ChangePartitionKeys => CHANGE_PARTITION_KEYS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeInlineIntent {
    pub(crate) operation: RuntimeInlineOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeInlineOperation {
    RegisterInlineTables,
    RegisterInlineRows,
    DeleteInlineRows,
}

impl RuntimeInlineOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            "RegisterInlineTables" => Ok(Self::RegisterInlineTables),
            "RegisterInlineRows" => Ok(Self::RegisterInlineRows),
            "DeleteInlineRows" => Ok(Self::DeleteInlineRows),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support inline operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::RegisterInlineTables => "RegisterInlineTables",
            Self::RegisterInlineRows => "RegisterInlineRows",
            Self::DeleteInlineRows => "DeleteInlineRows",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCompactionIntent {
    pub(crate) operation: RuntimeCompactionOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeCompactionOperation {
    MergeAdjacentFiles,
    RewriteDeleteFiles,
}

impl RuntimeCompactionOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            "MergeAdjacentFiles" => Ok(Self::MergeAdjacentFiles),
            "RewriteDeleteFiles" => Ok(Self::RewriteDeleteFiles),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support compaction operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::MergeAdjacentFiles => "MergeAdjacentFiles",
            Self::RewriteDeleteFiles => "RewriteDeleteFiles",
        }
    }
}

pub(crate) fn commit_attempt(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let started = RuntimeMetricStage::start();
    let intent = commit_attempt_intent(payload)?;
    record_commit_attempt_stage("ParseIntent", started);
    let started = RuntimeMetricStage::start();
    let mut kv = open_runtime_catalog(backend)?;
    let current = current_catalog_state(&kv, catalog)?;
    record_commit_attempt_stage("OpenForMetadata", started);
    let started = RuntimeMetricStage::start();
    let metadata = commit_metadata_intents(&mut kv, catalog, &intent, &current)?;
    record_commit_attempt_stage("MetadataIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let schema_version = current.final_schema_version(metadata.public_schema_changed);
    let table_id_remaps = metadata.table_id_remaps();
    drop(kv);
    let started = RuntimeMetricStage::start();
    let inline_output_bytes = commit_inline_intents(backend, catalog, &intent, &table_id_remaps)?;
    record_commit_attempt_stage("InlineIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let started = RuntimeMetricStage::start();
    let compaction_output_bytes =
        commit_compaction_intents(backend, catalog, &intent, &table_id_remaps)?;
    record_commit_attempt_stage("CompactionIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let started = RuntimeMetricStage::start();
    let data_mutation_payload_bytes =
        commit_data_mutation_intent(backend, catalog, &intent, &table_id_remaps)?
            .map_or(0, |payload| payload.len());
    record_commit_attempt_stage("DataMutationIntent", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let mut output = format!(
        "commit_attempt_intent=true\nducklake_schema_version={schema_version}\nmetadata_intent_count={}\ninline_intent_count={}\ncompaction_intent_count={}\ndata_mutation_payload_bytes={}\ninline_output_bytes={inline_output_bytes}\ncompaction_output_bytes={compaction_output_bytes}\nchanged_table_count={}\ncreated_table_count={}\n",
        intent.metadata_intents.len(),
        intent.inline_payloads.len(),
        intent.compaction_intents.len(),
        data_mutation_payload_bytes,
        metadata.changed_table_count,
        metadata.created_tables.len(),
    );
    for table in metadata.created_tables {
        output.push_str(&format!(
            "created_table\t{}\t{}\t{}\t{}\n",
            table.requested_table_id.0,
            table.persisted.table_id.0,
            table.persisted.schema_id.0,
            table.persisted.name
        ));
    }
    record_commit_attempt_child_metrics(backend, &intent)?;
    Ok(output.into_bytes())
}

#[cfg(feature = "runtime-metrics")]
fn record_commit_attempt_stage(stage: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(
        &format!("method.runtime_commit_attempt.{stage}"),
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_commit_attempt_stage(_stage: &str, _started: RuntimeMetricStage) {}

#[cfg(feature = "runtime-metrics")]
fn record_commit_attempt_child_metrics(
    backend: RuntimeCatalogBackend,
    intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<()> {
    for metadata in &intent.metadata_intents {
        record_runtime_request(backend, metadata.operation.name(), RuntimeMetricStatus::Ok);
    }
    for inline in &intent.inline_payloads {
        record_runtime_request(backend, inline.operation.name(), RuntimeMetricStatus::Ok);
    }
    for compaction in &intent.compaction_intents {
        record_runtime_request(
            backend,
            compaction.operation.name(),
            RuntimeMetricStatus::Ok,
        );
    }
    if !intent.data_mutation_payload.is_empty() {
        record_runtime_request(backend, "CommitDataMutation", RuntimeMetricStatus::Ok);
    }
    Ok(())
}

#[cfg(not(feature = "runtime-metrics"))]
fn record_commit_attempt_child_metrics(
    _backend: RuntimeCatalogBackend,
    _intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<()> {
    Ok(())
}

fn commit_compaction_intents(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<usize> {
    let mut output_bytes = 0;
    for compaction in &intent.compaction_intents {
        let operation = compaction.operation.name();
        let compaction_payload =
            remap_compaction_payload(compaction.operation, &compaction.payload, table_id_remaps)?;
        let output = commit_compaction_intent(
            backend,
            catalog,
            operation,
            &compaction_payload,
            intent.read_snapshot,
            intent.proposed_commit_snapshot,
            intent.commit_metadata.clone(),
        )?;
        output_bytes += output.len();
    }
    Ok(output_bytes)
}

fn commit_data_mutation_intent(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<Option<Vec<u8>>> {
    if intent.data_mutation_payload.is_empty() {
        return Ok(None);
    }
    let data_mutation_payload =
        remap_data_mutation_payload(&intent.data_mutation_payload, table_id_remaps)?;
    let payload = payload_with_commit_header(
        intent,
        "CommitDataMutation",
        &data_mutation_payload,
        include_read_snapshot_for_storage_intents(intent),
        true,
    )?;
    commit_data_mutation(backend, catalog, &payload).map(Some)
}

fn commit_inline_intents(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<usize> {
    let mut output_bytes = 0;
    for inline in &intent.inline_payloads {
        let operation = inline.operation.name();
        let inline_payload =
            remap_inline_payload(inline.operation, &inline.payload, table_id_remaps)?;
        let payload = payload_with_commit_header(
            intent,
            operation,
            &inline_payload,
            include_read_snapshot_for_storage_intents(intent),
            true,
        )?;
        let output = match inline.operation {
            RuntimeInlineOperation::RegisterInlineTables => {
                register_inline_tables(backend, catalog, &payload)?
            }
            RuntimeInlineOperation::RegisterInlineRows => {
                register_inline_rows(backend, catalog, &payload)?
            }
            RuntimeInlineOperation::DeleteInlineRows => {
                delete_inline_rows(backend, catalog, &payload)?
            }
        };
        output_bytes += output.len();
    }
    Ok(output_bytes)
}

fn commit_metadata_intents(
    kv: &mut RuntimeMutableCatalog,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    current: &CurrentCatalogState,
) -> CatalogResult<CommitMetadataResult> {
    commit_metadata_intents_with_current_kv(kv, catalog, intent, current)
}

#[cfg(test)]
fn commit_metadata_intents_with_kv(
    kv: &mut impl CommitAttemptTableReplacements,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<CommitMetadataResult> {
    let current = current_catalog_state(kv, catalog)?;
    commit_metadata_intents_with_current_kv(kv, catalog, intent, &current)
}

fn commit_metadata_intents_with_current_kv(
    kv: &mut impl CommitAttemptTableReplacements,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    current: &CurrentCatalogState,
) -> CatalogResult<CommitMetadataResult> {
    if intent.metadata_intents.is_empty() {
        return Ok(CommitMetadataResult::default());
    }
    let started = RuntimeMetricStage::start();
    let mut tables = TableIntentAssembler::new(kv, catalog, current.latest.order)?;
    let mut created_schemas = Vec::new();
    let mut dropped_schema_ids = Vec::new();
    let mut dropped_table_ids = Vec::new();
    let mut replacement_tables = Vec::new();
    let mut created_views = Vec::new();
    let mut dropped_view_ids = Vec::new();
    let mut view_rename_rows = Vec::new();
    let mut view_comment_changes = Vec::new();
    let mut touched_existing_table_ids = BTreeSet::new();
    for metadata in &intent.metadata_intents {
        let operation = metadata.operation.name();
        let payload =
            payload_with_commit_header(intent, operation, &metadata.payload, true, false)?;
        match metadata.operation {
            RuntimeMetadataOperation::CreateSchemas => {
                let (_, schemas) = create_schemas_payload_values(&payload)?;
                created_schemas.extend(schemas);
            }
            RuntimeMetadataOperation::DropSchemas => {
                dropped_schema_ids.extend(drop_schemas_payload_values(&payload)?);
            }
            RuntimeMetadataOperation::CreateTables => {
                let (_, created_tables) = create_tables_payload_values(&payload)?;
                for table in created_tables {
                    tables.apply_create_table_fact(table)?;
                }
            }
            RuntimeMetadataOperation::ReplaceTables => {
                let (_, table_ids, tables) = replace_tables_payload_values(&payload)?;
                dropped_table_ids.extend(table_ids);
                replacement_tables.extend(tables);
            }
            RuntimeMetadataOperation::CreateViews => {
                created_views.extend(create_view_rows(&payload)?);
            }
            RuntimeMetadataOperation::DropViews => {
                dropped_view_ids.extend(drop_view_ids(&payload)?);
            }
            RuntimeMetadataOperation::RenameViews => {
                view_rename_rows.extend(view_renames(&payload)?);
            }
            RuntimeMetadataOperation::AddColumns => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                let columns = parse_column_rows(ADD_COLUMNS, &ddl.rows)?;
                if let Some(table_id) = one_column_table(ADD_COLUMNS, &columns)? {
                    touched_existing_table_ids.insert(table_id);
                    for (column_table_id, column) in columns {
                        debug_assert_eq!(column_table_id, table_id);
                        tables.apply_add_column_fact(table_id, column)?;
                    }
                }
            }
            RuntimeMetadataOperation::RenameColumns => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for (table_id, column) in parse_column_rows(RENAME_COLUMNS, &ddl.rows)? {
                    touched_existing_table_ids.insert(table_id);
                    tables.apply_rename_column_fact(table_id, column)?;
                }
            }
            RuntimeMetadataOperation::ChangeColumnDefaults => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for (table_id, column) in parse_column_rows(CHANGE_COLUMN_DEFAULTS, &ddl.rows)? {
                    touched_existing_table_ids.insert(table_id);
                    tables.apply_column_default_fact(table_id, column)?;
                }
            }
            RuntimeMetadataOperation::ChangeColumnTypes => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for (table_id, column) in parse_column_rows(CHANGE_COLUMN_TYPES, &ddl.rows)? {
                    touched_existing_table_ids.insert(table_id);
                    tables.apply_column_type_fact(table_id, column)?;
                }
            }
            RuntimeMetadataOperation::RenameTables => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for rename in parse_table_renames(&ddl.rows)? {
                    touched_existing_table_ids.insert(rename.table_id);
                    tables.apply_rename_table_fact(rename.table_id, rename.new_name)?;
                }
            }
            RuntimeMetadataOperation::ChangeComments => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                let (table_comments, column_comments) = parse_comment_changes(&ddl.rows)?;
                for change in table_comments {
                    touched_existing_table_ids.insert(change.table_id);
                    tables.apply_table_comment_fact(change.table_id, change.comment)?;
                }
                for change in column_comments {
                    touched_existing_table_ids.insert(change.table_id);
                    tables.apply_column_comment_fact(
                        change.table_id,
                        change.column_id,
                        change.comment,
                    )?;
                }
            }
            RuntimeMetadataOperation::DropColumns => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for drop in parse_column_drops(&ddl.rows)? {
                    touched_existing_table_ids.insert(drop.table_id);
                    tables.apply_drop_column_fact(drop.table_id, drop.column_id)?;
                }
            }
            RuntimeMetadataOperation::ChangeSortKeys => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                let change = parse_sort_change(&ddl.rows)?;
                touched_existing_table_ids.insert(change.table_id);
                tables.table_mut(change.table_id)?.sort = change.sort;
            }
            RuntimeMetadataOperation::ChangeViewComment => {
                let (_, change) = view_comment_change(&payload)?;
                view_comment_changes.push(change);
            }
            RuntimeMetadataOperation::ChangePartitionKeys => {
                let ddl = DdlPayload::parse(operation, &payload)?;
                for change in parse_partition_changes(&ddl.rows)? {
                    touched_existing_table_ids.insert(change.table_id);
                    tables.table_mut(change.table_id)?.partition = change.partition;
                }
            }
        }
    }
    record_commit_attempt_stage("MetadataAssembleFacts", started);
    let started = RuntimeMetricStage::start();
    let table_changes = tables.into_commit_parts()?;
    record_commit_attempt_stage("MetadataTableParts", started);
    let public_schema_changed = public_schema_changed_by_metadata(
        !created_schemas.is_empty(),
        !dropped_schema_ids.is_empty(),
        &table_changes,
        !dropped_table_ids.is_empty(),
        !replacement_tables.is_empty(),
        !created_views.is_empty()
            || !dropped_view_ids.is_empty()
            || !view_rename_rows.is_empty()
            || !view_comment_changes.is_empty(),
    );
    let result = CommitMetadataResult {
        changed_table_count: table_changes
            .created
            .len()
            .saturating_add(table_changes.replacements.len())
            .saturating_add(created_schemas.len())
            .saturating_add(dropped_schema_ids.len())
            .saturating_add(dropped_table_ids.len())
            .saturating_add(replacement_tables.len())
            .saturating_add(created_views.len())
            .saturating_add(dropped_view_ids.len())
            .saturating_add(view_rename_rows.len())
            .saturating_add(view_comment_changes.len()),
        created_tables: table_changes.created_tables.clone(),
        public_schema_changed,
    };
    let sequence = commit_attempt_sequence(intent.proposed_commit_snapshot)?;
    let started = RuntimeMetricStage::start();
    reject_schema_create_conflicts(
        kv,
        catalog,
        current.latest.order,
        &created_schemas,
        &dropped_schema_ids,
    )?;
    reject_table_target_schema_conflicts(
        kv,
        catalog,
        current.latest.order,
        &created_schemas,
        &dropped_schema_ids,
        &table_changes.created,
        &replacement_tables,
    )?;
    reject_schema_drop_conflicts(
        kv,
        catalog,
        current.latest.order,
        &dropped_schema_ids,
        &dropped_table_ids,
    )?;
    reject_stale_existing_table_changes(
        kv,
        catalog,
        intent.read_snapshot,
        &table_changes.created_tables,
        &touched_existing_table_ids,
    )?;
    reject_view_comment_conflicts(kv, catalog, intent.read_snapshot, &view_comment_changes)?;
    record_commit_attempt_stage("MetadataConflictChecks", started);
    let started = RuntimeMetricStage::start();
    kv.commit_schema_changes_at(
        catalog,
        sequence,
        Some(&intent.commit_metadata),
        created_schemas,
        dropped_schema_ids,
    )?;
    record_commit_attempt_stage("MetadataCommitSchemas", started);
    let started = RuntimeMetricStage::start();
    kv.commit_table_changes_at(
        catalog,
        sequence,
        Some(&intent.commit_metadata),
        table_changes.created,
        table_changes.replacements,
    )?;
    record_commit_attempt_stage("MetadataCommitTables", started);
    let started = RuntimeMetricStage::start();
    let replaced_tables = kv.commit_replace_tables_at(
        catalog,
        sequence,
        &dropped_table_ids,
        replacement_tables,
        Some(&intent.commit_metadata),
    )?;
    record_commit_attempt_stage("MetadataCommitReplacedTables", started);
    let mut result = result;
    result
        .created_tables
        .extend(replaced_tables.into_iter().map(CreatedTable::unremapped));
    let started = RuntimeMetricStage::start();
    kv.commit_view_changes_at(
        catalog,
        sequence,
        created_views,
        view_rename_rows,
        dropped_view_ids,
        view_comment_changes,
    )?;
    record_commit_attempt_stage("MetadataCommitViews", started);
    Ok(result)
}

fn reject_schema_create_conflicts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    latest_order: crate::CatalogOrderId,
    created_schemas: &[SchemaRow],
    dropped_schema_ids: &[SchemaId],
) -> CatalogResult<()> {
    if created_schemas.is_empty() {
        return Ok(());
    }
    let current = list_schemas_at(kv, catalog, latest_order)?;
    let replacement_schemas: BTreeSet<SchemaId> = dropped_schema_ids.iter().copied().collect();
    for schema in created_schemas {
        if current.iter().any(|existing| {
            existing.schema_id == schema.schema_id
                && !replacement_schemas.contains(&existing.schema_id)
        }) || created_schemas
            .iter()
            .filter(|candidate| candidate.schema_id == schema.schema_id)
            .count()
            > 1
        {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating schema {}: schema id {} already exists",
                schema.name, schema.schema_id.0
            )));
        }
        if current.iter().any(|existing| {
            existing.name.eq_ignore_ascii_case(&schema.name)
                && !replacement_schemas.contains(&existing.schema_id)
        }) || created_schemas
            .iter()
            .filter(|candidate| candidate.name.eq_ignore_ascii_case(&schema.name))
            .count()
            > 1
        {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating schema {}: schema name already exists",
                schema.name
            )));
        }
    }
    Ok(())
}

fn reject_table_target_schema_conflicts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    latest_order: crate::CatalogOrderId,
    created_schemas: &[SchemaRow],
    dropped_schema_ids: &[SchemaId],
    created_tables: &[TableRow],
    replacement_tables: &[TableRow],
) -> CatalogResult<()> {
    let mut visible_schema_ids = list_schemas_at(kv, catalog, latest_order)?
        .into_iter()
        .map(|schema| schema.schema_id)
        .collect::<BTreeSet<_>>();
    for schema_id in dropped_schema_ids {
        visible_schema_ids.remove(schema_id);
    }
    for schema in created_schemas {
        visible_schema_ids.insert(schema.schema_id);
    }
    for table in created_tables.iter().chain(replacement_tables.iter()) {
        if table.schema_id != SchemaId(0) && !visible_schema_ids.contains(&table.schema_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating table {}: schema {} no longer exists",
                table.name, table.schema_id.0
            )));
        }
    }
    Ok(())
}

fn reject_schema_drop_conflicts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    latest_order: crate::CatalogOrderId,
    dropped_schema_ids: &[SchemaId],
    dropped_table_ids: &[TableId],
) -> CatalogResult<()> {
    if dropped_schema_ids.is_empty() {
        return Ok(());
    }
    let dropped_schema_ids = dropped_schema_ids.iter().copied().collect::<BTreeSet<_>>();
    let dropped_table_ids = dropped_table_ids.iter().copied().collect::<BTreeSet<_>>();
    for table in list_tables_at(kv, catalog, latest_order)? {
        if dropped_schema_ids.contains(&table.schema_id)
            && !dropped_table_ids.contains(&table.table_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict dropping schema {}: table {} exists",
                table.schema_id.0, table.name
            )));
        }
    }
    Ok(())
}

fn reject_stale_existing_table_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_snapshot: Option<DuckLakeSnapshotId>,
    created_tables: &[CreatedTable],
    touched_existing_table_ids: &BTreeSet<TableId>,
) -> CatalogResult<()> {
    if touched_existing_table_ids.is_empty() {
        return Ok(());
    }
    let created_table_ids = created_tables
        .iter()
        .flat_map(|table| [table.requested_table_id, table.persisted.table_id])
        .collect::<BTreeSet<_>>();
    let stale_checked_table_ids = touched_existing_table_ids
        .difference(&created_table_ids)
        .copied()
        .collect::<BTreeSet<_>>();
    if stale_checked_table_ids.is_empty() {
        return Ok(());
    }
    let Some(read_snapshot) = read_snapshot else {
        return Ok(());
    };
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let read_snapshot = conflict_read_snapshot(kv, catalog, read_snapshot, &latest)?;
    for table_id in &stale_checked_table_ids {
        let read_table = load_table_at(kv, catalog, *table_id, read_snapshot.order)?
            .ok_or(CatalogError::NotFound("table at read snapshot"))?;
        let Some(current_table) = load_current_table_row(kv, catalog, *table_id)? else {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict changing table {}: table was dropped after read snapshot {}",
                table_id.0, read_snapshot.sequence.0
            )));
        };
        if read_table.validity.begin_order != current_table.validity.begin_order {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict changing table {}: table metadata changed after read snapshot {}",
                table_id.0, read_snapshot.sequence.0
            )));
        }
    }
    Ok(())
}

fn reject_view_comment_conflicts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_snapshot: Option<DuckLakeSnapshotId>,
    changes: &[crate::ViewCommentChange],
) -> CatalogResult<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let Some(read_snapshot) = read_snapshot else {
        return Ok(());
    };
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let read_snapshot = conflict_read_snapshot(kv, catalog, read_snapshot, &latest)?;
    for change in changes {
        let read_view = load_view_at(kv, catalog, change.view_id, read_snapshot.order)?;
        let Some(read_view) = read_view else {
            continue;
        };
        let current_view = load_view_at(kv, catalog, change.view_id, latest.order)?
            .ok_or(CatalogError::NotFound("view"))?;
        if read_view.validity.begin_order != current_view.validity.begin_order {
            return Err(CatalogError::InvalidMutation(format!(
                "another transaction has altered it: view {} changed after read snapshot {}",
                change.view_id.0, read_snapshot.sequence.0
            )));
        }
    }
    Ok(())
}

fn conflict_read_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_snapshot: DuckLakeSnapshotId,
    latest: &SnapshotRow,
) -> CatalogResult<SnapshotRow> {
    if let Some(snapshot) = snapshot_by_public_sequence(kv, catalog, read_snapshot)? {
        return Ok(snapshot);
    }
    if let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, read_snapshot)? {
        return Ok(snapshot);
    }
    if latest.sequence.0.saturating_add(1) == read_snapshot.0 {
        return Ok(latest.clone());
    }
    Err(CatalogError::InvalidMutation(format!(
        "read snapshot {} is not available for conflict checks; latest catalog snapshot is {}",
        read_snapshot.0, latest.sequence.0
    )))
}

#[derive(Debug, Default)]
struct CommitMetadataResult {
    changed_table_count: usize,
    created_tables: Vec<CreatedTable>,
    public_schema_changed: bool,
}

impl CommitMetadataResult {
    fn table_id_remaps(&self) -> BTreeMap<TableId, TableId> {
        self.created_tables
            .iter()
            .filter(|table| table.requested_table_id != table.persisted.table_id)
            .map(|table| (table.requested_table_id, table.persisted.table_id))
            .collect()
    }
}

#[derive(Clone, Debug)]
struct CurrentCatalogState {
    latest: SnapshotRow,
    public_schema_version: u64,
}

impl CurrentCatalogState {
    fn final_schema_version(&self, public_schema_changed: bool) -> u64 {
        self.public_schema_version + u64::from(public_schema_changed)
    }
}

fn current_catalog_state(
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

fn public_schema_changed_by_metadata(
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
struct CreatedTable {
    requested_table_id: TableId,
    persisted: TableRow,
}

impl CreatedTable {
    fn new(requested_table_id: TableId, persisted: TableRow) -> Self {
        Self {
            requested_table_id,
            persisted,
        }
    }

    fn unremapped(persisted: TableRow) -> Self {
        Self::new(persisted.table_id, persisted)
    }
}

struct TableCommitParts {
    created: Vec<TableRow>,
    replacements: Vec<TableVersionReplacement>,
    created_tables: Vec<CreatedTable>,
}

trait CommitAttemptTableReplacements: MutableCatalogKv {
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
                .map(|tables| {
                    let _ = _commit_metadata;
                    tables
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
fn commit_replaced_tables_at(
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
fn commit_schema_changes_at(
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
fn commit_created_tables_at(
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
fn previous_sequence(sequence: RawSnapshotSequence) -> CatalogResult<RawSnapshotSequence> {
    sequence
        .0
        .checked_sub(1)
        .map(RawSnapshotSequence)
        .ok_or_else(|| {
            CatalogError::InvalidMutation("commit snapshot id must be greater than 0".to_owned())
        })
}

struct TableIntentAssembler<'a, K>
where
    K: OrderedCatalogKv,
{
    kv: &'a K,
    catalog: CatalogId,
    base_order: crate::CatalogOrderId,
    tables: BTreeMap<TableId, TableIntentTable>,
    created_tables: BTreeMap<TableId, TableRow>,
    created_table_ids: BTreeMap<TableId, TableId>,
}

impl<'a, K> TableIntentAssembler<'a, K>
where
    K: OrderedCatalogKv,
{
    fn new(
        kv: &'a K,
        catalog: CatalogId,
        base_order: crate::CatalogOrderId,
    ) -> CatalogResult<Self> {
        Ok(Self {
            kv,
            catalog,
            base_order,
            tables: BTreeMap::new(),
            created_tables: BTreeMap::new(),
            created_table_ids: BTreeMap::new(),
        })
    }

    fn table_mut(&mut self, table_id: TableId) -> CatalogResult<&mut TableRow> {
        if self.created_tables.contains_key(&table_id) {
            return self
                .created_tables
                .get_mut(&table_id)
                .ok_or(CatalogError::NotFound("table"));
        }
        if !self.tables.contains_key(&table_id) {
            let table = load_table_at(self.kv, self.catalog, table_id, self.base_order)?
                .ok_or(CatalogError::NotFound("table"))?;
            self.tables.insert(
                table_id,
                TableIntentTable {
                    previous: table.clone(),
                    next: table,
                },
            );
        }
        self.tables
            .get_mut(&table_id)
            .map(|table| &mut table.next)
            .ok_or(CatalogError::NotFound("table"))
    }

    fn apply_create_table_fact(&mut self, table: TableRow) -> CatalogResult<()> {
        if self.created_tables.contains_key(&table.table_id)
            || self.tables.contains_key(&table.table_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "table id {} appears more than once in CommitAttempt",
                table.table_id.0
            )));
        }
        let requested_table_id = table.table_id;
        let persisted_table_id = self.persisted_table_id_for_create(requested_table_id)?;
        let mut persisted = table;
        persisted.table_id = persisted_table_id;
        self.created_tables.insert(requested_table_id, persisted);
        self.created_table_ids
            .insert(requested_table_id, persisted_table_id);
        Ok(())
    }

    fn apply_rename_table_fact(
        &mut self,
        table_id: TableId,
        new_name: String,
    ) -> CatalogResult<()> {
        self.table_mut(table_id)?.name = new_name;
        Ok(())
    }

    fn apply_add_column_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        match table
            .columns
            .iter_mut()
            .find(|existing| existing.column_id == column.column_id)
        {
            Some(existing) if same_column_identity(existing, &column) => {
                apply_column_default(existing, &column);
                Ok(())
            }
            Some(existing) if same_column_shape_except_name(existing, &column) => {
                apply_column_default(existing, &column);
                existing.name = column.name;
                Ok(())
            }
            Some(existing) => {
                apply_column_default(existing, &column);
                existing.name = column.name;
                existing.column_type = column.column_type;
                existing.nulls_allowed = column.nulls_allowed;
                existing.parent_id = column.parent_id;
                Ok(())
            }
            None => {
                if let Some(existing_index) = table.columns.iter().position(|existing| {
                    existing.parent_id == column.parent_id
                        && existing.name.eq_ignore_ascii_case(&column.name)
                }) {
                    table.columns[existing_index] = column;
                    return Ok(());
                }
                table.columns.push(column);
                Ok(())
            }
        }
    }

    fn apply_rename_column_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing_index) = table
            .columns
            .iter()
            .position(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        if table.columns.iter().enumerate().any(|(index, existing)| {
            index != existing_index && existing.name.eq_ignore_ascii_case(&column.name)
        }) {
            return Err(CatalogError::InvalidMutation(format!(
                "column name {} already exists on table {}",
                column.name, table_id.0
            )));
        }
        let existing = &mut table.columns[existing_index];
        reject_column_shape_change(existing, &column, table_id)?;
        apply_column_default(existing, &column);
        existing.name = column.name;
        Ok(())
    }

    fn apply_column_default_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing) = table
            .columns
            .iter_mut()
            .find(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        reject_column_shape_change(existing, &column, table_id)?;
        if !existing.name.eq_ignore_ascii_case(&column.name) {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} default change cannot rename column on table {}",
                column.column_id.0, table_id.0
            )));
        }
        apply_column_default(existing, &column);
        Ok(())
    }

    fn apply_column_type_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing_index) = table
            .columns
            .iter()
            .position(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        if table
            .columns
            .iter()
            .any(|existing| existing.parent_id == Some(column.column_id))
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change a parent column on table {}",
                column.column_id.0, table_id.0
            )));
        }
        let existing = &mut table.columns[existing_index];
        if !existing.name.eq_ignore_ascii_case(&column.name)
            || existing.parent_id != column.parent_id
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change identity metadata on table {}",
                column.column_id.0, table_id.0
            )));
        }
        if existing.parent_id.is_none() && existing.nulls_allowed != column.nulls_allowed {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change top-level nullability on table {}",
                column.column_id.0, table_id.0
            )));
        }
        if existing.column_type == column.column_type {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type is unchanged on table {}",
                column.column_id.0, table_id.0
            )));
        }
        existing.column_type = column.column_type.clone();
        existing.nulls_allowed = column.nulls_allowed;
        apply_column_default(existing, &column);
        Ok(())
    }

    fn apply_drop_column_fact(
        &mut self,
        table_id: TableId,
        column_id: crate::ColumnId,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(index) = table
            .columns
            .iter()
            .position(|column| column.column_id == column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        table.columns.remove(index);
        Ok(())
    }

    fn apply_table_comment_fact(
        &mut self,
        table_id: TableId,
        comment: Option<String>,
    ) -> CatalogResult<()> {
        self.table_mut(table_id)?.comment = comment;
        Ok(())
    }

    fn apply_column_comment_fact(
        &mut self,
        table_id: TableId,
        column_id: crate::ColumnId,
        comment: Option<String>,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(column) = table
            .columns
            .iter_mut()
            .find(|column| column.column_id == column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        column.comment = comment;
        Ok(())
    }

    fn into_commit_parts(self) -> CatalogResult<TableCommitParts> {
        self.reject_duplicate_table_names()?;
        let replacements = self
            .tables
            .into_iter()
            .filter(|(_, table)| table.previous != table.next)
            .map(|(table_id, table)| -> CatalogResult<_> {
                reject_duplicate_column_names(&table.next, table_id)?;
                Ok(TableVersionReplacement::new(
                    table_id,
                    table.previous,
                    table.next,
                ))
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let created_tables = self
            .created_tables
            .into_iter()
            .map(|(requested_table_id, table)| {
                reject_duplicate_column_names(&table, table.table_id)?;
                Ok(CreatedTable::new(requested_table_id, table))
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let created = created_tables
            .iter()
            .map(|table| table.persisted.clone())
            .collect();
        Ok(TableCommitParts {
            created,
            replacements,
            created_tables,
        })
    }

    fn reject_duplicate_table_names(&self) -> CatalogResult<()> {
        let mut tables = list_tables_at(self.kv, self.catalog, self.base_order)?;
        for (table_id, table) in &self.tables {
            if let Some(existing) = tables
                .iter_mut()
                .find(|existing| existing.table_id == *table_id)
            {
                *existing = table.next.clone();
            }
        }
        tables.extend(self.created_tables.values().cloned());
        for (index, table) in tables.iter().enumerate() {
            if tables[..index].iter().any(|previous| {
                previous.schema_id == table.schema_id
                    && previous.name.eq_ignore_ascii_case(&table.name)
            }) {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: name already exists in schema {}",
                    table.name, table.schema_id.0
                )));
            }
        }
        Ok(())
    }

    fn persisted_table_id_for_create(&self, requested_table_id: TableId) -> CatalogResult<TableId> {
        let current_tables = list_tables_at(self.kv, self.catalog, self.base_order)?;
        if !current_tables
            .iter()
            .any(|table| table.table_id == requested_table_id)
            && !self
                .created_tables
                .values()
                .any(|table| table.table_id == requested_table_id)
        {
            return Ok(requested_table_id);
        }
        let max_current = current_tables
            .iter()
            .map(|table| table.table_id.0)
            .max()
            .unwrap_or(0);
        let max_created = self
            .created_tables
            .values()
            .map(|table| table.table_id.0)
            .max()
            .unwrap_or(0);
        Ok(TableId(max_current.max(max_created).saturating_add(1)))
    }
}

struct TableIntentTable {
    previous: TableRow,
    next: TableRow,
}

fn payload_with_commit_header(
    intent: &RuntimeCommitAttemptIntent,
    operation: &'static str,
    payload: &[u8],
    include_read_snapshot: bool,
    include_commit_metadata: bool,
) -> CatalogResult<Vec<u8>> {
    let mut out = Vec::new();
    writeln!(
        &mut out,
        "commit_snapshot\t{}",
        commit_snapshot_u64(intent.proposed_commit_snapshot)?
    )
    .map_err(|error| CatalogError::Backend(format!("failed to render commit header: {error}")))?;
    if include_commit_metadata {
        push_commit_metadata_rows(&mut out, &intent.commit_metadata)?;
    }
    if include_read_snapshot {
        if let Some(read_snapshot) = intent.read_snapshot {
            writeln!(&mut out, "read_snapshot\t{}", read_snapshot.0).map_err(|error| {
                CatalogError::Backend(format!("failed to render read snapshot header: {error}"))
            })?;
        }
    }
    for row in TabularPayload::new(operation, payload)? {
        let row = row?;
        if row.has_fields("commit_snapshot", true) || row.has_fields("read_snapshot", true) {
            continue;
        }
        out.extend_from_slice(row.line().as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

fn push_commit_metadata_rows(
    out: &mut Vec<u8>,
    metadata: &SnapshotCommitMetadata,
) -> CatalogResult<()> {
    if let Some(author) = metadata.author.as_ref() {
        writeln!(out, "commit_author\t{author}").map_err(|error| {
            CatalogError::Backend(format!("failed to render commit author: {error}"))
        })?;
    }
    if let Some(message) = metadata.commit_message.as_ref() {
        writeln!(out, "commit_message\t{message}").map_err(|error| {
            CatalogError::Backend(format!("failed to render commit message: {error}"))
        })?;
    }
    if let Some(extra_info) = metadata.commit_extra_info.as_ref() {
        writeln!(out, "commit_extra_info\t{extra_info}").map_err(|error| {
            CatalogError::Backend(format!("failed to render commit extra info: {error}"))
        })?;
    }
    Ok(())
}

fn include_read_snapshot_for_storage_intents(intent: &RuntimeCommitAttemptIntent) -> bool {
    intent.metadata_intents.is_empty()
}

fn remap_inline_payload(
    operation: RuntimeInlineOperation,
    payload: &[u8],
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<Vec<u8>> {
    remap_tabular_payload(payload, table_id_remaps, |fields, remaps| match operation {
        RuntimeInlineOperation::RegisterInlineTables
        | RuntimeInlineOperation::RegisterInlineRows
            if fields.len() >= 4 && fields[0] == "table" =>
        {
            remap_table_id_field(fields, 1, remaps)?;
            remap_inline_table_name_field(fields, 3, remaps);
            Ok(())
        }
        RuntimeInlineOperation::DeleteInlineRows if fields.len() == 4 && fields[0] == "delete" => {
            remap_table_id_field(fields, 1, remaps)?;
            remap_inline_table_name_field(fields, 2, remaps);
            Ok(())
        }
        _ => Ok(()),
    })
}

fn remap_compaction_payload(
    operation: RuntimeCompactionOperation,
    payload: &[u8],
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<Vec<u8>> {
    remap_tabular_payload(payload, table_id_remaps, |fields, remaps| match operation {
        RuntimeCompactionOperation::MergeAdjacentFiles
        | RuntimeCompactionOperation::RewriteDeleteFiles
            if fields.len() >= 3 && fields[0] == "source_file" =>
        {
            remap_table_id_field(fields, 1, remaps)?;
            Ok(())
        }
        RuntimeCompactionOperation::MergeAdjacentFiles
        | RuntimeCompactionOperation::RewriteDeleteFiles
            if fields.len() >= 3 && fields[0] == "file" =>
        {
            remap_table_id_field(fields, 2, remaps)?;
            Ok(())
        }
        RuntimeCompactionOperation::MergeAdjacentFiles
        | RuntimeCompactionOperation::RewriteDeleteFiles
            if fields.len() >= 3 && fields[0] == "file_partition" =>
        {
            remap_table_id_field(fields, 2, remaps)?;
            Ok(())
        }
        RuntimeCompactionOperation::MergeAdjacentFiles
        | RuntimeCompactionOperation::RewriteDeleteFiles
            if fields.len() >= 3 && fields[0] == "file_column_stats" =>
        {
            remap_table_id_field(fields, 2, remaps)?;
            Ok(())
        }
        _ => Ok(()),
    })
}

fn remap_data_mutation_payload(
    payload: &[u8],
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<Vec<u8>> {
    remap_tabular_payload(payload, table_id_remaps, |fields, remaps| {
        match fields.first().map(String::as_str) {
            Some("file") => remap_table_id_field(fields, 2, remaps)?,
            Some("file_partition") => remap_table_id_field(fields, 2, remaps)?,
            Some("file_partition_set") => remap_table_id_field(fields, 2, remaps)?,
            Some("file_column_stats") => remap_table_id_field(fields, 2, remaps)?,
            Some("delete_file") => remap_table_id_field(fields, 2, remaps)?,
            Some("inline") => remap_table_id_field(fields, 1, remaps)?,
            Some("inline_table") => remap_inline_table_name_field(fields, 1, remaps),
            Some("inline_file_delete") => remap_table_id_field(fields, 1, remaps)?,
            _ => {}
        }
        Ok(())
    })
}

fn remap_tabular_payload(
    payload: &[u8],
    table_id_remaps: &BTreeMap<TableId, TableId>,
    mut remap_row: impl FnMut(&mut Vec<String>, &BTreeMap<TableId, TableId>) -> CatalogResult<()>,
) -> CatalogResult<Vec<u8>> {
    if table_id_remaps.is_empty() || payload.is_empty() {
        return Ok(payload.to_vec());
    }
    let text = std::str::from_utf8(payload).map_err(|error| {
        CatalogError::Decode(format!("CommitAttempt payload is not UTF-8: {error}"))
    })?;
    let mut out = String::new();
    for line in text.lines() {
        let mut fields = line.split('\t').map(ToOwned::to_owned).collect::<Vec<_>>();
        remap_row(&mut fields, table_id_remaps)?;
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    Ok(out.into_bytes())
}

fn remap_table_id_field(
    fields: &mut [String],
    index: usize,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<()> {
    let Some(value) = fields.get(index) else {
        return Ok(());
    };
    let table_id = TableId(parse_u64_field(COMMIT_ATTEMPT, value, "table id remap")?);
    if let Some(remapped) = table_id_remaps.get(&table_id) {
        fields[index] = remapped.0.to_string();
    }
    Ok(())
}

fn remap_inline_table_name_field(
    fields: &mut [String],
    index: usize,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) {
    let Some(value) = fields.get_mut(index) else {
        return;
    };
    for (requested, persisted) in table_id_remaps {
        let prefix = format!("ducklake_inlined_data_{}_", requested.0);
        if let Some(suffix) = value.strip_prefix(&prefix) {
            *value = format!("ducklake_inlined_data_{}_{}", persisted.0, suffix);
            return;
        }
    }
}

fn commit_snapshot_u64(snapshot: ProposedCommitSnapshot) -> CatalogResult<u64> {
    let snapshot = snapshot.commit_attempt_id();
    u64::try_from(snapshot.0).map_err(|_| {
        CatalogError::InvalidMutation(format!(
            "commit snapshot id {} does not fit in u64",
            snapshot.0
        ))
    })
}

fn commit_attempt_sequence(snapshot: ProposedCommitSnapshot) -> CatalogResult<RawSnapshotSequence> {
    commit_snapshot_u64(snapshot).map(RawSnapshotSequence)
}

fn same_column_identity(existing: &TableColumnRow, proposed: &TableColumnRow) -> bool {
    existing.column_id == proposed.column_id
        && existing.name.eq_ignore_ascii_case(&proposed.name)
        && same_column_shape_except_name(existing, proposed)
}

fn same_column_shape_except_name(existing: &TableColumnRow, proposed: &TableColumnRow) -> bool {
    existing.column_id == proposed.column_id
        && existing.column_type == proposed.column_type
        && existing.nulls_allowed == proposed.nulls_allowed
        && existing.parent_id == proposed.parent_id
}

fn reject_column_shape_change(
    existing: &TableColumnRow,
    proposed: &TableColumnRow,
    table_id: TableId,
) -> CatalogResult<()> {
    if same_column_shape_except_name(existing, proposed) {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "column {} shape changed unexpectedly on table {}",
        proposed.column_id.0, table_id.0
    )))
}

fn reject_duplicate_column_names(table: &TableRow, table_id: TableId) -> CatalogResult<()> {
    let mut seen = BTreeSet::new();
    for column in &table.columns {
        let sibling_key = (column.parent_id, column.name.to_lowercase());
        if !seen.insert(sibling_key) {
            return Err(CatalogError::InvalidMutation(format!(
                "table {} has duplicate column name {}",
                table_id.0, column.name
            )));
        }
    }
    Ok(())
}

fn apply_column_default(existing: &mut TableColumnRow, proposed: &TableColumnRow) {
    existing.initial_default = proposed.initial_default.clone();
    existing.default_value = proposed.default_value.clone();
    existing.default_value_type = proposed.default_value_type.clone();
}

pub(crate) fn commit_attempt_intent(payload: &[u8]) -> CatalogResult<RuntimeCommitAttemptIntent> {
    let mut read_snapshot = None;
    let mut proposed_commit_snapshot = None;
    let mut metadata_intents = Vec::new();
    let mut compaction_intents = Vec::new();
    let mut data_mutation_payload = Vec::new();
    let mut inline_payloads = Vec::new();
    let mut commit_metadata = SnapshotCommitMetadata::default();
    let mut current_section: Option<RuntimeCommitAttemptSection> = None;

    for row in TabularPayload::new(COMMIT_ATTEMPT, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["read_snapshot", snapshot_id] => {
                read_snapshot = Some(DuckLakeSnapshotId(parse_u64_field(
                    COMMIT_ATTEMPT,
                    snapshot_id,
                    "read snapshot id",
                )?));
            }
            ["commit_snapshot", snapshot_id] => {
                proposed_commit_snapshot = Some(ProposedCommitSnapshot::new(CommitAttemptId(
                    parse_u64_field(COMMIT_ATTEMPT, snapshot_id, "commit snapshot id")?.into(),
                )));
            }
            ["commit_author", author] => {
                commit_metadata.author = Some((*author).to_owned());
            }
            ["commit_message", message] => {
                commit_metadata.commit_message = Some((*message).to_owned());
            }
            ["commit_extra_info", extra_info] => {
                commit_metadata.commit_extra_info = Some((*extra_info).to_owned());
            }
            ["metadata", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::metadata(operation)?);
            }
            ["data_mutation"] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::data_mutation());
            }
            ["inline", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::inline(operation)?);
            }
            ["compaction", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::compaction(operation)?);
            }
            _ => {
                let Some(section) = current_section.as_mut() else {
                    return Err(CatalogError::Decode(format!(
                        "{COMMIT_ATTEMPT} payload row appears before an intent section"
                    )));
                };
                section.push_tabular_row(fields.to_vec());
            }
        }
    }
    finish_section(
        current_section,
        &mut metadata_intents,
        &mut compaction_intents,
        &mut data_mutation_payload,
        &mut inline_payloads,
    )?;
    let mut proposed_commit_snapshot = proposed_commit_snapshot.ok_or_else(|| {
        CatalogError::Decode(format!(
            "{COMMIT_ATTEMPT} payload requires commit_snapshot row"
        ))
    })?;
    if !data_mutation_payload.is_empty() {
        let data_mutation = data_mutation_payload_values(&data_mutation_payload)?;
        proposed_commit_snapshot = proposed_commit_snapshot_covering_inline_flushes(
            Some(proposed_commit_snapshot),
            &data_mutation.inline_flushes,
        )
        .ok_or_else(|| {
            CatalogError::Decode(
                "CommitAttempt proposed snapshot resolution unexpectedly returned no snapshot"
                    .to_owned(),
            )
        })?;
    }
    Ok(RuntimeCommitAttemptIntent {
        read_snapshot,
        proposed_commit_snapshot,
        commit_metadata,
        metadata_intents,
        compaction_intents,
        data_mutation_payload,
        inline_payloads,
    })
}

enum RuntimeCommitAttemptSection {
    Metadata {
        operation: RuntimeMetadataOperation,
        payload: Vec<u8>,
    },
    Compaction {
        operation: RuntimeCompactionOperation,
        payload: Vec<u8>,
    },
    DataMutation {
        payload: Vec<u8>,
    },
    Inline {
        operation: RuntimeInlineOperation,
        payload: Vec<u8>,
    },
}

impl RuntimeCommitAttemptSection {
    fn metadata(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Metadata {
            operation: RuntimeMetadataOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn data_mutation() -> Self {
        Self::DataMutation {
            payload: Vec::new(),
        }
    }

    fn compaction(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Compaction {
            operation: RuntimeCompactionOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn inline(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Inline {
            operation: RuntimeInlineOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn push_tabular_row(&mut self, fields: Vec<&str>) {
        let payload = match self {
            Self::Metadata { payload, .. }
            | Self::Compaction { payload, .. }
            | Self::DataMutation { payload }
            | Self::Inline { payload, .. } => payload,
        };
        if !payload.is_empty() {
            payload.push(b'\n');
        }
        payload.extend(fields.join("\t").as_bytes());
    }
}

fn finish_section(
    section: Option<RuntimeCommitAttemptSection>,
    metadata_intents: &mut Vec<RuntimeMetadataIntent>,
    compaction_intents: &mut Vec<RuntimeCompactionIntent>,
    data_mutation_payload: &mut Vec<u8>,
    inline_payloads: &mut Vec<RuntimeInlineIntent>,
) -> CatalogResult<()> {
    match section {
        None => Ok(()),
        Some(RuntimeCommitAttemptSection::Metadata { operation, payload }) => {
            metadata_intents.push(RuntimeMetadataIntent { operation, payload });
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::Compaction { operation, payload }) => {
            compaction_intents.push(RuntimeCompactionIntent { operation, payload });
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::DataMutation { payload }) => {
            if !data_mutation_payload.is_empty() {
                return Err(CatalogError::Decode(
                    "CommitAttempt payload contains multiple data_mutation sections".to_owned(),
                ));
            }
            *data_mutation_payload = payload;
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::Inline { operation, payload }) => {
            inline_payloads.push(RuntimeInlineIntent { operation, payload });
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "runtime_commit_attempt_ops_tests.rs"]
mod runtime_commit_attempt_ops_tests;
