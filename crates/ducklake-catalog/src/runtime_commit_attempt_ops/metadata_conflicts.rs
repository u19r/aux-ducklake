use std::collections::BTreeSet;

use crate::{
    CatalogError, CatalogId, CatalogResult, DuckLakeSnapshotId, OrderedCatalogKv, SchemaId,
    SchemaRow, SnapshotRow, TableId, TableRow, TableVersionReplacement, latest_snapshot,
    load_table_at, load_view_at,
    runtime_object_ops::{create_view_rows, drop_view_ids, view_comment_change, view_renames},
    runtime_schema_change_payload::{
        ADD_COLUMNS, CHANGE_COLUMN_DEFAULTS, CHANGE_COLUMN_TYPES, DdlPayload, RENAME_COLUMNS,
        one_column_table, parse_column_drops, parse_column_rows, parse_comment_changes,
        parse_partition_changes, parse_sort_change, parse_table_renames,
    },
    runtime_schema_ops::{create_schemas_payload_values, drop_schemas_payload_values},
    runtime_table_ops::{create_tables_payload_values, replace_tables_payload_values},
    schema_store::list_schemas_at,
    snapshot_by_ducklake_sequence, snapshot_by_public_sequence,
    table_store::{list_tables_at, load_current_table_row},
};

use crate::runtime_commit_attempt_ops::*;
pub(super) fn commit_metadata_intents_with_current_kv(
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
        &table_changes.replacements,
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

pub(super) fn reject_schema_create_conflicts(
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

pub(super) fn reject_table_target_schema_conflicts(
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

pub(super) fn reject_schema_drop_conflicts(
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

pub(super) fn reject_stale_existing_table_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_snapshot: Option<DuckLakeSnapshotId>,
    created_tables: &[CreatedTable],
    replacements: &[TableVersionReplacement],
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
            let replacement = replacements
                .iter()
                .find(|replacement| replacement.next.table_id == *table_id);
            if replacement.is_none_or(|replacement| {
                replacement.next.same_user_visible_schema_as(&current_table)
            }) {
                continue;
            }
            return Err(CatalogError::InvalidMutation(format!(
                "conflict changing table {}: table metadata changed after read snapshot {}",
                table_id.0, read_snapshot.sequence.0
            )));
        }
    }
    Ok(())
}

pub(super) fn reject_view_comment_conflicts(
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

pub(super) fn conflict_read_snapshot(
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
