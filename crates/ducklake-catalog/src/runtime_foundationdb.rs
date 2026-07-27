#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct DataMutationMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct DataMutationMetricStage;

impl DataMutationMetricStage {
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
}

#[cfg(feature = "runtime-metrics")]
fn record_data_mutation_stage(stage: &str, started: DataMutationMetricStage) {
    crate::runtime_metrics::record_runtime_method_elapsed(
        &format!("method.runtime_foundationdb_data_mutation.{stage}"),
        u64::try_from(started.0.elapsed().as_micros()).unwrap_or(u64::MAX),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
fn record_data_mutation_stage(_stage: &str, _started: DataMutationMetricStage) {}

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
    DataFileId, DataFileRow, DeleteFileRow, DuckLakeSnapshotId, FdbOrderedCatalogKv,
    FilePartitionValueRow,
    fdb_tables::{current_table_name_value_id, table_metadata_recovery_attempt_id},
    keys::{current_table_name_key, data_file_key},
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
    table_store::{load_current_table_row, load_current_table_rows},
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
    let latest = if crate::store::runtime_read_context_enabled() {
        latest_snapshot(&kv, catalog)?
    } else {
        latest_snapshot_uncached(&kv, catalog).row?
    };
    let Some(latest) = latest else {
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
    metadata: &[crate::MetadataSettingRow],
) -> CatalogResult<crate::CatalogOrderId> {
    let kv = open_foundationdb_catalog()?;
    Ok(kv
        .initialize_catalog_with_metadata_if_absent_versionstamped(catalog, metadata)?
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
    let validated = reject_stale_data_mutation(&kv, catalog, &mutation)?;
    let affected_table_ids = crate::runtime_data_mutation_ops::affected_table_ids(&mutation)?;
    let materialized_delete_files = mutation.materialized_delete_files();
    let commit = kv.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
        catalog,
        crate::fdb_data_mutations::FdbMutationAttempt {
            proposed_snapshot: mutation
                .proposed_commit_snapshot
                .map(crate::runtime_snapshot_range::ProposedCommitSnapshot::commit_attempt_id),
            recovery: mutation.recovery_attempt_id,
        },
        mutation.commit_metadata,
        crate::FdbDataMutation {
            data_files: mutation.data_files,
            delete_files: materialized_delete_files,
            inline_flushes: mutation.inline_flushes,
            partition_values: mutation.partition_values,
            inline_file_deletions: mutation.inline_file_deletions,
            file_column_stats: mutation.file_column_stats,
            dropped_data_file_ids: mutation.dropped_data_file_ids,
        },
        validated,
    )?;
    Ok((commit, affected_table_ids))
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_commit_data_and_inline_mutation(
    catalog: CatalogId,
    mut mutation: RuntimeDataMutation,
    inline_rows: Vec<crate::runtime_inline_ops::RuntimeInlineRows>,
    inline_deletes: Vec<crate::runtime_inline_ops::RuntimeInlineDelete>,
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<(crate::DataMutationCommit, Vec<crate::TableId>)> {
    let started = DataMutationMetricStage::start();
    let kv = open_foundationdb_catalog()?;
    record_data_mutation_stage("Open", started);
    let started = DataMutationMetricStage::start();
    crate::runtime_data_mutation_ops::resolve_data_file_visibility(&kv, catalog, &mut mutation)?;
    record_data_mutation_stage("ResolveVisibility", started);
    let started = DataMutationMetricStage::start();
    crate::runtime_data_mutation_ops::complete_inline_flushes_from_materialized_files(
        &kv,
        catalog,
        &mut mutation,
    )?;
    record_data_mutation_stage("CompleteInlineFlushes", started);
    let started = DataMutationMetricStage::start();
    let validated = reject_stale_data_mutation(&kv, catalog, &mutation)?;
    record_data_mutation_stage("RejectStale", started);
    let started = DataMutationMetricStage::start();
    let affected_table_ids = crate::runtime_data_mutation_ops::affected_table_ids(&mutation)?;
    record_data_mutation_stage("AffectedTables", started);
    let started = DataMutationMetricStage::start();
    let (inline_tables, inline_payloads, inline_deletes) =
        crate::runtime_foundationdb_inline::prepare_foundationdb_inline_mutations(
            &kv,
            catalog,
            inline_rows,
            inline_deletes,
            commit_snapshot,
        )?;
    record_data_mutation_stage("PrepareInline", started);
    let materialized_delete_files = mutation.materialized_delete_files();
    let started = DataMutationMetricStage::start();
    let commit = kv.commit_data_and_inline_mutation_versionstamped(
        catalog,
        crate::fdb_data_mutations::FdbMutationAttempt {
            proposed_snapshot: mutation
                .proposed_commit_snapshot
                .map(crate::runtime_snapshot_range::ProposedCommitSnapshot::commit_attempt_id),
            recovery: mutation.recovery_attempt_id,
        },
        mutation.commit_metadata,
        crate::FdbDataMutation {
            data_files: mutation.data_files,
            delete_files: materialized_delete_files,
            inline_flushes: mutation.inline_flushes,
            partition_values: mutation.partition_values,
            inline_file_deletions: mutation.inline_file_deletions,
            file_column_stats: mutation.file_column_stats,
            dropped_data_file_ids: mutation.dropped_data_file_ids,
        },
        crate::fdb_data_mutations::FdbInlineMutation {
            tables: inline_tables,
            payloads: inline_payloads,
            deletes: inline_deletes,
        },
        validated,
    )?;
    record_data_mutation_stage("Commit", started);
    Ok((commit, affected_table_ids))
}

#[cfg(feature = "foundationdb")]
fn reject_stale_data_mutation(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    mutation: &RuntimeDataMutation,
) -> CatalogResult<crate::fdb_data_mutations::FdbMutationReadContext> {
    let started = DataMutationMetricStage::start();
    let read_snapshot = if let Some(read_snapshot) = mutation.read_snapshot {
        snapshot_by_public_sequence(kv, catalog, read_snapshot)?
    } else if let Some(flush_snapshot) = mutation
        .inline_flushes
        .iter()
        .map(|flush| flush.flush_snapshot_sequence)
        .max()
    {
        crate::snapshot_by_raw_sequence(kv, catalog, flush_snapshot)?
    } else {
        None
    };
    let Some(read_snapshot) = read_snapshot else {
        return Ok(crate::fdb_data_mutations::FdbMutationReadContext::default());
    };
    record_data_mutation_stage("RejectStaleResolveSnapshot", started);
    let started = DataMutationMetricStage::start();
    let latest = latest_snapshot(kv, catalog)?;
    record_data_mutation_stage("RejectStaleLatest", started);
    let proposed_sequence = mutation
        .proposed_commit_snapshot
        .and_then(|snapshot| u64::try_from(snapshot.commit_attempt_id().0).ok())
        .map(crate::RawSnapshotSequence);
    if !mutation.inline_flushes.is_empty()
        && latest.as_ref().is_some_and(|latest| {
            latest.order > read_snapshot.order && Some(latest.sequence) != proposed_sequence
        })
    {
        return Err(crate::CatalogError::InvalidMutation(
            "conflict flushing inline data: catalog changed after read snapshot".to_owned(),
        ));
    }
    let started = DataMutationMetricStage::start();
    let append_partitions = append_partition_expectations(
        &mutation.data_files,
        &mutation.partition_values,
        &mutation.file_partition_sets,
    );
    record_data_mutation_stage("RejectStaleAppend", started);
    let started = DataMutationMetricStage::start();
    let preloaded_data_files = reject_delete_targets_changed_after_read(
        kv,
        catalog,
        read_snapshot.order,
        latest.as_ref().map(|snapshot| snapshot.order),
        &mutation.data_files,
        &mutation.materialized_delete_files(),
        &mutation.dropped_data_file_ids,
    )?;
    record_data_mutation_stage("RejectStaleDeletes", started);
    Ok(crate::fdb_data_mutations::FdbMutationReadContext {
        order: Some(read_snapshot.order),
        data_files: preloaded_data_files,
        append_partitions,
    })
}

#[cfg(feature = "foundationdb")]
fn append_partition_expectations(
    data_files: &[DataFileRow],
    partition_values: &[FilePartitionValueRow],
    file_partition_sets: &[crate::runtime_data_mutation_ops::RuntimeFilePartitionSet],
) -> Vec<crate::fdb_data_mutations::FdbAppendPartitionExpectation> {
    data_files
        .iter()
        .map(|data_file| {
            let partition_set = file_partition_sets
                .iter()
                .find(|set| set.data_file_id == data_file.data_file_id);
            crate::fdb_data_mutations::FdbAppendPartitionExpectation {
                data_file_id: data_file.data_file_id,
                table_id: data_file.table_id,
                partition_table_id: partition_set.map(|set| set.table_id),
                partition_id: partition_set.map(|set| set.partition_id),
                value_count: partition_values
                    .iter()
                    .filter(|value| value.data_file_id == data_file.data_file_id)
                    .count(),
            }
        })
        .collect()
}

#[cfg(feature = "foundationdb")]
fn reject_delete_targets_changed_after_read(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    latest_order: Option<crate::CatalogOrderId>,
    data_files: &[DataFileRow],
    delete_files: &[DeleteFileRow],
    dropped_data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    let appended_data_file_ids = data_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let target_ids = delete_files
        .iter()
        .map(|row| row.data_file_id)
        .chain(dropped_data_file_ids.iter().copied())
        .filter(|data_file_id| !appended_data_file_ids.contains(data_file_id))
        .collect::<BTreeSet<_>>();
    let target_files = load_data_files_for_conflict_check(kv, catalog, &target_ids)?;
    if target_files.is_empty() {
        return Ok(target_files);
    }
    if latest_order != Some(read_order) {
        reject_target_tables_changed_after_read(kv, catalog, read_order, &target_files)?;
    }
    for row in target_files
        .iter()
        .filter(|row| row.validity.begin_order <= read_order)
    {
        if row
            .validity
            .end_order
            .is_some_and(|end_order| end_order > read_order)
        {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: data file {} was dropped after read snapshot",
                row.data_file_id.0
            )));
        }
    }
    Ok(target_files)
}

#[cfg(feature = "foundationdb")]
fn reject_target_tables_changed_after_read(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    read_order: crate::CatalogOrderId,
    data_files: &[DataFileRow],
) -> CatalogResult<()> {
    let table_ids = data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<BTreeSet<_>>();
    let current_tables = load_current_table_rows(kv, catalog, &table_ids)?
        .into_iter()
        .map(|table| (table.table_id, table))
        .collect::<BTreeMap<_, _>>();
    for table_id in table_ids {
        let Some(read_table) = crate::load_table_at(kv, catalog, table_id, read_order)? else {
            continue;
        };
        let Some(current_table) = current_tables.get(&table_id) else {
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
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn load_data_files_for_conflict_check(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<Vec<DataFileRow>> {
    let keys = data_file_ids
        .iter()
        .map(|data_file_id| data_file_key(catalog, *data_file_id))
        .collect::<Vec<_>>();
    data_file_ids
        .iter()
        .copied()
        .zip(kv.batch_get(&keys)?)
        .map(|(data_file_id, value)| {
            let Some(value) = value else {
                return Err(crate::CatalogError::NotFound("data file"));
            };
            let row = DataFileRow::decode(&value)?;
            if row.data_file_id != data_file_id {
                return Err(crate::CatalogError::Decode(format!(
                    "data file key {} decoded as data file {}",
                    data_file_id.0, row.data_file_id.0
                )));
            }
            Ok(row)
        })
        .collect()
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
    _metadata: &[crate::MetadataSettingRow],
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
pub(crate) fn runtime_foundationdb_commit_data_and_inline_mutation(
    _catalog: CatalogId,
    _mutation: RuntimeDataMutation,
    _inline_rows: Vec<crate::runtime_inline_ops::RuntimeInlineRows>,
    _inline_deletes: Vec<crate::runtime_inline_ops::RuntimeInlineDelete>,
    _commit_snapshot: Option<crate::DuckLakeSnapshotId>,
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
