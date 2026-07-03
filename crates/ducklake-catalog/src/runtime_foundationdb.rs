use crate::{
    CatalogId, CatalogResult, RawSnapshotSequence, SchemaRow, SnapshotTimestampBound, TableId,
    TableRow,
    runtime_catalog_snapshot::CatalogSnapshotIdKind,
    runtime_change_feed::ChangeFeedPayload,
    runtime_cleanup::OldFilesCleanupRequest,
    runtime_data_mutation_ops::RuntimeDataMutation,
    runtime_file_listing::{
        CurrentPartitionFilesBatchPayload, CurrentPartitionFilesPayload,
        CurrentPartitionPruneFilesPayload, ListDataFilesAtPayload, PartitionFilesAtBatchPayload,
        PartitionFilesAtPayload, PartitionPruneFilesAtPayload,
    },
    runtime_snapshots::ListSnapshotsPayload,
};

#[cfg(feature = "foundationdb")]
use crate::{
    DataFileId, DataFileRow, DeleteFileId, DeleteFileRow, DuckLakeSnapshotId, FdbOrderedCatalogKv,
    FilePartitionValueRow,
    fdb_tables::{current_table_name_value_id, table_metadata_recovery_attempt_id},
    keys::{
        current_data_file_key, current_delete_file_key, current_table_name_key, data_file_key,
        delete_file_key,
    },
    kv::OrderedCatalogKv,
    latest_snapshot, list_schemas_at, list_tables_at, load_commit_attempt,
    runtime_catalog_snapshot::{
        catalog_snapshot_payload_with_kind, conflict_snapshot_payload,
        conflict_snapshot_payload_for_row, public_snapshot_payload, snapshot_payload,
    },
    runtime_change_feed::{data_file_changes_payload, table_deletions_payload},
    runtime_cleanup::{known_files_cleanup_payload, old_files_cleanup_payload},
    runtime_file_listing::{
        foundationdb_current_partition_files_batch_payload,
        foundationdb_current_partition_files_payload,
        foundationdb_current_partition_prune_files_payload, foundationdb_data_files_at_payload,
        foundationdb_partition_files_at_batch_payload, foundationdb_partition_files_at_payload,
        foundationdb_partition_prune_files_at_payload,
    },
    runtime_snapshots::{
        list_snapshots_payload, snapshot_by_public_sequence, snapshot_changes_after_payload,
    },
    snapshot_by_timestamp,
    store::latest_snapshot_uncached,
    table_store::load_current_table_row,
};

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_get_foundationdb_snapshot(catalog: CatalogId) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    snapshot_payload(&kv, catalog, latest_snapshot(&kv, catalog)?)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_get_foundationdb_conflict_snapshot(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot_uncached(&kv, catalog).row? else {
        return conflict_snapshot_payload(&kv, catalog);
    };
    conflict_snapshot_payload_for_row(&kv, catalog, latest)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_get_foundationdb_snapshot_at(
    catalog: CatalogId,
    snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    public_snapshot_payload(&kv, catalog, DuckLakeSnapshotId(snapshot_id))
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_get_foundationdb_snapshot_at_timestamp(
    catalog: CatalogId,
    timestamp_micros: i64,
    bound: SnapshotTimestampBound,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    snapshot_payload(
        &kv,
        catalog,
        snapshot_by_timestamp(&kv, catalog, timestamp_micros, bound)?,
    )
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_get_foundationdb_catalog_for_snapshot(
    catalog: CatalogId,
    snapshot_id: u64,
    snapshot_kind: CatalogSnapshotIdKind,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    catalog_snapshot_payload_with_kind(&kv, catalog, DuckLakeSnapshotId(snapshot_id), snapshot_kind)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_snapshots(
    catalog: CatalogId,
    payload: ListSnapshotsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    list_snapshots_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_snapshot_changes_after(
    catalog: CatalogId,
    base_snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    snapshot_changes_after_payload(&kv, catalog, base_snapshot_id)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_data_files_at(
    catalog: CatalogId,
    payload: ListDataFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_data_files_at_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_current_partition_files(
    catalog: CatalogId,
    payload: CurrentPartitionFilesPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_current_partition_files_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_current_partition_files_batch(
    catalog: CatalogId,
    payload: CurrentPartitionFilesBatchPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_current_partition_files_batch_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_current_partition_prune_files(
    catalog: CatalogId,
    payload: CurrentPartitionPruneFilesPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_current_partition_prune_files_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_partition_files_at(
    catalog: CatalogId,
    payload: PartitionFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_partition_files_at_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_partition_files_at_batch(
    catalog: CatalogId,
    payload: PartitionFilesAtBatchPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_partition_files_at_batch_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_partition_prune_files_at(
    catalog: CatalogId,
    payload: PartitionPruneFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    foundationdb_partition_prune_files_at_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_removed_data_files_after(
    catalog: CatalogId,
    snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    crate::runtime_file_ops::removed_data_files_after_payload(&kv, catalog, snapshot_id)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_data_file_changes(
    catalog: CatalogId,
    payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    data_file_changes_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_table_deletions(
    catalog: CatalogId,
    payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    table_deletions_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_old_files_for_cleanup(
    catalog: CatalogId,
    request: OldFilesCleanupRequest,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    old_files_cleanup_payload(&kv, catalog, request)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_list_foundationdb_known_files_for_cleanup(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    known_files_cleanup_payload(&kv, catalog)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_touch_catalog(catalog: CatalogId) -> CatalogResult<()> {
    let kv = open_foundationdb_catalog()?;
    latest_snapshot(&kv, catalog)?;
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_metadata_exists(catalog: CatalogId) -> CatalogResult<bool> {
    let kv = open_foundationdb_catalog()?;
    Ok(latest_snapshot(&kv, catalog)?.is_some())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_initialize_ducklake(
    catalog: CatalogId,
) -> CatalogResult<crate::CatalogOrderId> {
    let kv = open_foundationdb_catalog()?;
    Ok(kv
        .initialize_catalog_if_absent_versionstamped(catalog)?
        .order)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_create_schemas(
    catalog: CatalogId,
    schemas: Vec<SchemaRow>,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<SchemaRow>> {
    let kv = open_foundationdb_catalog()?;
    reject_current_schema_create_conflicts(&kv, catalog, &schemas)?;
    kv.create_schemas_versionstamped(catalog, schemas, commit_raw_snapshot)
}

#[cfg(feature = "foundationdb")]
fn reject_current_schema_create_conflicts(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    schemas: &[SchemaRow],
) -> CatalogResult<()> {
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let current = list_schemas_at(kv, catalog, latest.order)?;
    for schema in schemas {
        if current
            .iter()
            .any(|existing| existing.schema_id == schema.schema_id)
        {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating schema {}: schema id {} already exists",
                schema.name, schema.schema_id.0
            )));
        }
        if current
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&schema.name))
        {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating schema {}: schema name already exists",
                schema.name
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_drop_schemas(
    catalog: CatalogId,
    schema_ids: &[crate::SchemaId],
) -> CatalogResult<Vec<SchemaRow>> {
    let kv = open_foundationdb_catalog()?;
    reject_current_schema_drop_conflicts(&kv, catalog, schema_ids)?;
    kv.drop_schemas_versionstamped(catalog, schema_ids)
}

#[cfg(feature = "foundationdb")]
fn reject_current_schema_drop_conflicts(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    schema_ids: &[crate::SchemaId],
) -> CatalogResult<()> {
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let current_tables = list_tables_at(kv, catalog, latest.order)?;
    for schema_id in schema_ids {
        if current_tables
            .iter()
            .any(|table| table.schema_id == *schema_id)
        {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict dropping schema {}: schema contains a table",
                schema_id.0
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_create_tables(
    catalog: CatalogId,
    tables: Vec<TableRow>,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<TableRow>> {
    let kv = open_foundationdb_catalog()?;
    let recovery_id = table_metadata_recovery_attempt_id(1, commit_raw_snapshot, &[], &tables);
    if !is_committed_recovery_attempt(&kv, catalog, recovery_id)? {
        reject_current_table_create_conflicts(&kv, catalog, &tables)?;
    }
    let commit_raw_snapshot = fresh_commit_raw_snapshot(&kv, catalog, commit_raw_snapshot)?;
    kv.create_tables_versionstamped(catalog, tables, commit_raw_snapshot, None, recovery_id)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_replace_tables(
    catalog: CatalogId,
    table_ids: Vec<TableId>,
    tables: Vec<TableRow>,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<TableRow>> {
    let kv = open_foundationdb_catalog()?;
    let recovery_id =
        table_metadata_recovery_attempt_id(2, commit_raw_snapshot, &table_ids, &tables);
    let commit_raw_snapshot = fresh_commit_raw_snapshot(&kv, catalog, commit_raw_snapshot)?;
    kv.replace_tables_versionstamped_recoverable(
        catalog,
        &table_ids,
        tables,
        commit_raw_snapshot,
        recovery_id,
    )
}

#[cfg(feature = "foundationdb")]
fn is_committed_recovery_attempt(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    recovery_id: Option<crate::CommitAttemptId>,
) -> CatalogResult<bool> {
    let Some(recovery_id) = recovery_id else {
        return Ok(false);
    };
    Ok(load_commit_attempt(kv, catalog, recovery_id)?.is_some())
}

#[cfg(feature = "foundationdb")]
fn fresh_commit_raw_snapshot(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    proposed: Option<RawSnapshotSequence>,
) -> CatalogResult<Option<RawSnapshotSequence>> {
    let Some(proposed) = proposed else {
        return Ok(None);
    };
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(Some(proposed));
    };
    Ok(Some(if proposed > latest.sequence {
        proposed
    } else {
        latest.sequence.next()
    }))
}

#[cfg(feature = "foundationdb")]
fn reject_current_table_create_conflicts(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    tables: &[TableRow],
) -> CatalogResult<()> {
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let mut requested_names = std::collections::BTreeSet::new();
    let current_schemas = if tables
        .iter()
        .any(|table| table.schema_id != crate::SchemaId(0))
    {
        Some(list_schemas_at(kv, catalog, latest.order)?)
    } else {
        None
    };
    for table in tables {
        if table.schema_id != crate::SchemaId(0) {
            let schema_exists = current_schemas.as_ref().is_some_and(|schemas| {
                schemas
                    .iter()
                    .any(|schema| schema.schema_id == table.schema_id)
            });
            if !schema_exists {
                return Err(crate::CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: schema {} no longer exists",
                    table.name, table.schema_id.0
                )));
            }
        }
        if load_current_table_row(kv, catalog, table.table_id)?.is_some() {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating table {}: table id {} already exists",
                table.name, table.table_id.0
            )));
        }
        let normalized_name = table.name.to_ascii_lowercase();
        if !requested_names.insert((table.schema_id, normalized_name)) {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating table {}: table name already exists in schema {}",
                table.name, table.schema_id.0
            )));
        }
        if let Some(existing) = kv.get(&current_table_name_key(
            catalog,
            table.schema_id,
            &table.name,
        ))? {
            let existing_table_id = current_table_name_value_id(&existing)?;
            if existing_table_id != table.table_id {
                return Err(crate::CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: table name already exists in schema {}",
                    table.name, table.schema_id.0
                )));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_commit_data_mutation(
    catalog: CatalogId,
    mut mutation: RuntimeDataMutation,
) -> CatalogResult<(crate::DataMutationCommit, Vec<crate::TableId>)> {
    let kv = open_foundationdb_catalog()?;
    crate::runtime_data_mutation_ops::resolve_data_file_visibility(&kv, catalog, &mut mutation)?;
    crate::runtime_data_mutation_ops::complete_inline_flushes_from_materialized_files(
        &kv,
        catalog,
        &mut mutation,
    )?;
    reject_stale_data_mutation(&kv, catalog, &mutation)?;
    let affected_table_ids = crate::runtime_data_mutation_ops::affected_table_ids(&mutation)?;
    let materialized_delete_files = mutation.materialized_delete_files();
    let commit = kv.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
        catalog,
        mutation
            .proposed_commit_snapshot
            .map(crate::runtime_snapshot_range::ProposedCommitSnapshot::commit_attempt_id),
        mutation.commit_metadata,
        mutation.data_files,
        materialized_delete_files,
        mutation.inline_flushes,
        mutation.partition_values,
        mutation.inline_file_deletions,
        mutation.file_column_stats,
        mutation.dropped_data_file_ids,
    )?;
    Ok((commit, affected_table_ids))
}

#[cfg(feature = "foundationdb")]
fn reject_stale_data_mutation(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    mutation: &RuntimeDataMutation,
) -> CatalogResult<()> {
    let Some(read_snapshot) = mutation.read_snapshot else {
        return Ok(());
    };
    let Some(read_snapshot) = snapshot_by_public_sequence(kv, catalog, read_snapshot)? else {
        return Ok(());
    };
    reject_append_files_incompatible_with_current_tables(
        kv,
        catalog,
        read_snapshot.order,
        &mutation.data_files,
        &mutation.partition_values,
        &mutation.file_partition_sets,
    )?;
    reject_delete_targets_changed_after_read(
        kv,
        catalog,
        read_snapshot.order,
        &mutation.data_files,
        &mutation.materialized_delete_files(),
        &mutation.dropped_data_file_ids,
    )
}

#[cfg(feature = "foundationdb")]
fn reject_append_files_incompatible_with_current_tables(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    data_files: &[DataFileRow],
    partition_values: &[FilePartitionValueRow],
    file_partition_sets: &[crate::runtime_data_mutation_ops::RuntimeFilePartitionSet],
) -> CatalogResult<()> {
    if data_files.is_empty() {
        return Ok(());
    }
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let current_tables = list_tables_at(kv, catalog, latest.order)?;
    for data_file in data_files {
        let table_id = data_file.table_id;
        let Some(current_table) = current_tables
            .iter()
            .find(|table| table.table_id == table_id)
        else {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: table {} was dropped after read snapshot",
                table_id.0
            )));
        };
        if !append_partition_metadata_matches_table(
            data_file.data_file_id,
            table_id,
            current_table,
            partition_values,
            file_partition_sets,
        ) {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: table {} partition metadata is stale",
                table_id.0
            )));
        }
        let _ = read_order;
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn reject_delete_targets_changed_after_read(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    data_files: &[DataFileRow],
    delete_files: &[DeleteFileRow],
    dropped_data_file_ids: &[DataFileId],
) -> CatalogResult<()> {
    for row in delete_files {
        if data_files
            .iter()
            .any(|data_file| data_file.data_file_id == row.data_file_id)
        {
            continue;
        }
        reject_data_file_changed_after_read(kv, catalog, read_order, row.data_file_id)?;
    }
    for data_file_id in dropped_data_file_ids {
        reject_data_file_changed_after_read(kv, catalog, read_order, *data_file_id)?;
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn reject_data_file_changed_after_read(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    let data_file = load_data_file_for_conflict_check(kv, catalog, data_file_id)?;
    reject_target_table_changed_after_read(kv, catalog, read_order, data_file.table_id)?;
    if data_file.validity.begin_order > read_order {
        return Ok(());
    }
    if data_file
        .validity
        .end_order
        .is_some_and(|end_order| end_order > read_order)
    {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: data file {} was dropped after read snapshot",
            data_file_id.0
        )));
    }
    if kv
        .get(&current_data_file_key(
            catalog,
            data_file.table_id,
            data_file.data_file_id,
        ))?
        .is_none()
    {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: data file {} is no longer current",
            data_file_id.0
        )));
    }
    let Some(current_delete_file_id) = current_delete_file_id(kv, catalog, data_file_id)? else {
        return Ok(());
    };
    let current_delete_file =
        load_delete_file_for_conflict_check(kv, catalog, current_delete_file_id)?;
    if current_delete_file.validity.begin_order > read_order {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: data file {} was deleted from after read snapshot",
            data_file_id.0
        )));
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn reject_target_table_changed_after_read(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    table_id: crate::TableId,
) -> CatalogResult<()> {
    let Some(read_table) = crate::load_table_at(kv, catalog, table_id, read_order)? else {
        return Ok(());
    };
    let Some(current_table) = load_current_table_row(kv, catalog, table_id)? else {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: table {} was dropped after read snapshot",
            table_id.0
        )));
    };
    if read_table.columns != current_table.columns
        || read_table.partition != current_table.partition
    {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: another transaction has altered it; table {} changed after read snapshot",
            table_id.0
        )));
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn append_partition_metadata_matches_table(
    data_file_id: DataFileId,
    table_id: crate::TableId,
    table: &TableRow,
    partition_values: &[FilePartitionValueRow],
    file_partition_sets: &[crate::runtime_data_mutation_ops::RuntimeFilePartitionSet],
) -> bool {
    let value_count = partition_values
        .iter()
        .filter(|value| value.data_file_id == data_file_id)
        .count();
    let partition_set = file_partition_sets
        .iter()
        .find(|set| set.data_file_id == data_file_id);
    match &table.partition {
        Some(partition) => {
            value_count == partition.fields.len()
                && partition_set.is_some_and(|set| {
                    set.table_id == table_id && set.partition_id == partition.partition_id
                })
        }
        None => value_count == 0 && partition_set.is_none(),
    }
}

#[cfg(feature = "foundationdb")]
fn current_delete_file_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Option<DeleteFileId>> {
    let Some(value) = kv.get(&current_delete_file_key(catalog, data_file_id))? else {
        return Ok(None);
    };
    if let Ok(row) = DeleteFileRow::decode(&value) {
        return Ok(Some(row.delete_file_id));
    }
    if value.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "current delete file pointer must be 8 bytes, got {}",
            value.len()
        )));
    }
    Ok(Some(DeleteFileId(u64::from_be_bytes(
        value.try_into().map_err(|_| {
            crate::CatalogError::Decode("current delete file pointer is truncated".to_owned())
        })?,
    ))))
}

#[cfg(feature = "foundationdb")]
fn load_data_file_for_conflict_check(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

#[cfg(feature = "foundationdb")]
fn load_delete_file_for_conflict_check(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

#[cfg(feature = "foundationdb")]
const DEFAULT_FDB_PREFIX: &str = "dl/";

#[cfg(feature = "foundationdb")]
fn foundationdb_key_prefix_from_env() -> String {
    foundationdb_key_prefix(std::env::var("AUX_DUCKLAKE_FDB_PREFIX").ok())
}

#[cfg(feature = "foundationdb")]
fn foundationdb_key_prefix(configured: Option<String>) -> String {
    configured.unwrap_or_else(|| DEFAULT_FDB_PREFIX.to_owned())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn open_foundationdb_catalog() -> CatalogResult<FdbOrderedCatalogKv> {
    let key_prefix = foundationdb_key_prefix_from_env();
    let cluster_file = std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
    FdbOrderedCatalogKv::open_with_prefix(cluster_file.as_deref(), key_prefix.as_bytes())
}

#[cfg(all(test, feature = "foundationdb"))]
#[cfg(test)]
#[path = "runtime_foundationdb_tests.rs"]
mod runtime_foundationdb_tests;

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_get_foundationdb_snapshot(_catalog: CatalogId) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_get_foundationdb_conflict_snapshot(
    _catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_get_foundationdb_snapshot_at(
    _catalog: CatalogId,
    _snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_get_foundationdb_snapshot_at_timestamp(
    _catalog: CatalogId,
    _timestamp_micros: i64,
    _bound: SnapshotTimestampBound,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_get_foundationdb_catalog_for_snapshot(
    _catalog: CatalogId,
    _snapshot_id: u64,
    _snapshot_kind: CatalogSnapshotIdKind,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_snapshots(
    _catalog: CatalogId,
    _payload: ListSnapshotsPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_snapshot_changes_after(
    _catalog: CatalogId,
    _base_snapshot_id: crate::DuckLakeSnapshotId,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_data_files_at(
    _catalog: CatalogId,
    _payload: ListDataFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_current_partition_files(
    _catalog: CatalogId,
    _payload: CurrentPartitionFilesPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_current_partition_files_batch(
    _catalog: CatalogId,
    _payload: CurrentPartitionFilesBatchPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_current_partition_prune_files(
    _catalog: CatalogId,
    _payload: CurrentPartitionPruneFilesPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_partition_files_at(
    _catalog: CatalogId,
    _payload: PartitionFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_partition_files_at_batch(
    _catalog: CatalogId,
    _payload: PartitionFilesAtBatchPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_partition_prune_files_at(
    _catalog: CatalogId,
    _payload: PartitionPruneFilesAtPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_removed_data_files_after(
    _catalog: CatalogId,
    _snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_data_file_changes(
    _catalog: CatalogId,
    _payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_table_deletions(
    _catalog: CatalogId,
    _payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_old_files_for_cleanup(
    _catalog: CatalogId,
    _request: OldFilesCleanupRequest,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_list_foundationdb_known_files_for_cleanup(
    _catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_touch_catalog(_catalog: CatalogId) -> CatalogResult<()> {
    foundationdb_runtime_unit_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_metadata_exists(_catalog: CatalogId) -> CatalogResult<bool> {
    foundationdb_runtime_bool_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_initialize_ducklake(
    _catalog: CatalogId,
) -> CatalogResult<crate::CatalogOrderId> {
    foundationdb_runtime_order_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_create_schemas(
    _catalog: CatalogId,
    _schemas: Vec<SchemaRow>,
    _commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<SchemaRow>> {
    foundationdb_runtime_schema_rows_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_drop_schemas(
    _catalog: CatalogId,
    _schema_ids: &[crate::SchemaId],
) -> CatalogResult<Vec<SchemaRow>> {
    foundationdb_runtime_schema_rows_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_create_tables(
    _catalog: CatalogId,
    _tables: Vec<TableRow>,
    _commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<TableRow>> {
    foundationdb_runtime_table_rows_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_replace_tables(
    _catalog: CatalogId,
    _table_ids: Vec<TableId>,
    _tables: Vec<TableRow>,
    _commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<TableRow>> {
    foundationdb_runtime_table_rows_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_commit_data_mutation(
    _catalog: CatalogId,
    _mutation: RuntimeDataMutation,
) -> CatalogResult<(crate::DataMutationCommit, Vec<crate::TableId>)> {
    foundationdb_runtime_data_mutation_error()
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_error() -> CatalogResult<Vec<u8>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_unit_error() -> CatalogResult<()> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_bool_error() -> CatalogResult<bool> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_order_error() -> CatalogResult<crate::CatalogOrderId> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_schema_rows_error() -> CatalogResult<Vec<SchemaRow>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_table_rows_error() -> CatalogResult<Vec<TableRow>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_data_mutation_error()
-> CatalogResult<(crate::DataMutationCommit, Vec<crate::TableId>)> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}
