use std::collections::BTreeSet;

use crate::{
    CatalogId, CatalogResult, ColumnId, DataFileId, DataFileRow, DuckLakeSnapshotId,
    FileColumnStatsRow, FilePartitionValueRow, MergeAdjacentCompaction, PartitionKeyIndex,
    RewriteDeleteCompaction, SnapshotCommitMetadata, TableId,
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshot_range::ProposedCommitSnapshot,
    runtime_tabular_payload::{TabularPayload, parse_u32_field, parse_u64_field},
    snapshot_by_ducklake_sequence,
};

const MERGE_ADJACENT_FILES: &str = "MergeAdjacentFiles";
const REWRITE_DELETE_FILES: &str = "RewriteDeleteFiles";

pub(crate) fn merge_adjacent_files(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let parsed = compaction_payload_values(MERGE_ADJACENT_FILES, payload)?;
    let source_count = parsed.source_file_ids.len();
    let new_file_count = parsed.new_files.len();
    {
        merge_foundationdb_adjacent_files_from_payload(catalog, parsed)?;
    }
    invalidate_compaction_read_context(catalog);
    Ok(format!(
        "compacted_source_file_count={source_count}\ncompacted_new_file_count={new_file_count}\n"
    )
    .into_bytes())
}

pub(crate) fn commit_compaction_intent(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    operation: &str,
    payload: &[u8],
    read_snapshot: Option<DuckLakeSnapshotId>,
    proposed_commit_snapshot: ProposedCommitSnapshot,
    commit_metadata: SnapshotCommitMetadata,
) -> CatalogResult<Vec<u8>> {
    match operation {
        MERGE_ADJACENT_FILES => {
            let parsed = compaction_payload_values(MERGE_ADJACENT_FILES, payload)?;
            let source_count = parsed.source_file_ids.len();
            let new_file_count = parsed.new_files.len();
            {
                merge_foundationdb_adjacent_files_from_payload_at(
                    catalog,
                    parsed,
                    read_snapshot,
                    Some(proposed_commit_snapshot),
                    commit_metadata,
                )?;
            }
            invalidate_compaction_read_context(catalog);
            Ok(format!(
                "compacted_source_file_count={source_count}\ncompacted_new_file_count={new_file_count}\n"
            )
            .into_bytes())
        }
        REWRITE_DELETE_FILES => {
            let parsed = compaction_payload_values(REWRITE_DELETE_FILES, payload)?;
            let source_count = parsed.source_file_ids.len();
            let new_file_count = parsed.new_files.len();
            let operation = RewriteDeleteOperation::from_payload(parsed)?;
            {
                rewrite_foundationdb_delete_files_at(
                    catalog,
                    operation.compactions,
                    read_snapshot,
                    Some(proposed_commit_snapshot),
                    commit_metadata,
                )?;
            }
            invalidate_compaction_read_context(catalog);
            Ok(format!(
                "rewritten_source_file_count={source_count}\nrewritten_new_file_count={new_file_count}\n"
            )
            .into_bytes())
        }
        _ => Err(crate::CatalogError::InvalidMutation(format!(
            "CommitAttempt does not support compaction operation {operation}"
        ))),
    }
}

pub(crate) fn rewrite_delete_files(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let parsed = compaction_payload_values(REWRITE_DELETE_FILES, payload)?;
    let source_count = parsed.source_file_ids.len();
    let new_file_count = parsed.new_files.len();
    let operation = RewriteDeleteOperation::from_payload(parsed)?;
    {
        rewrite_foundationdb_delete_files(catalog, operation.compactions)?;
    }
    invalidate_compaction_read_context(catalog);
    Ok(format!(
        "rewritten_source_file_count={source_count}\nrewritten_new_file_count={new_file_count}\n"
    )
    .into_bytes())
}

fn invalidate_compaction_read_context(catalog: CatalogId) {
    crate::store::invalidate_runtime_read_context(catalog);
}

#[cfg(feature = "foundationdb")]
fn merge_foundationdb_adjacent_files_from_payload(
    catalog: CatalogId,
    parsed: CompactionPayload,
) -> CatalogResult<()> {
    merge_foundationdb_adjacent_files_from_payload_at(
        catalog,
        parsed,
        None,
        None,
        SnapshotCommitMetadata::default(),
    )
}

#[cfg(feature = "foundationdb")]
fn merge_foundationdb_adjacent_files_from_payload_at(
    catalog: CatalogId,
    parsed: CompactionPayload,
    read_snapshot: Option<DuckLakeSnapshotId>,
    proposed_commit_snapshot: Option<ProposedCommitSnapshot>,
    commit_metadata: SnapshotCommitMetadata,
) -> CatalogResult<()> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    for mut intent in merge_adjacent_compactions_from_payload(parsed)? {
        resolve_compaction_file_visibility(
            &kv,
            catalog,
            &mut intent.compaction,
            &intent.file_visibility,
        )?;
        if let Some(read_snapshot) = read_snapshot {
            let Some(base) = crate::snapshot_by_public_sequence(&kv, catalog, read_snapshot)?
            else {
                return Err(crate::CatalogError::NotFound("read snapshot"));
            };
            let through = crate::latest_snapshot(&kv, catalog)?
                .ok_or(crate::CatalogError::NotFound("catalog snapshot"))?;
            kv.commit_merge_adjacent_data_files_versionstamped_with_conflict_check_and_metadata(
                catalog,
                base.order,
                through.order,
                proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
                commit_metadata.clone(),
                intent.compaction,
            )?;
        } else {
            kv.commit_merge_adjacent_data_files_versionstamped_with_metadata(
                catalog,
                proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
                commit_metadata.clone(),
                intent.compaction,
            )?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "foundationdb"))]
fn merge_foundationdb_adjacent_files_from_payload(
    _catalog: CatalogId,
    _parsed: CompactionPayload,
) -> CatalogResult<()> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn merge_foundationdb_adjacent_files_from_payload_at(
    _catalog: CatalogId,
    _parsed: CompactionPayload,
    _read_snapshot: Option<DuckLakeSnapshotId>,
    _proposed_commit_snapshot: Option<ProposedCommitSnapshot>,
    _commit_metadata: SnapshotCommitMetadata,
) -> CatalogResult<()> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(feature = "foundationdb")]
fn rewrite_foundationdb_delete_files(
    catalog: CatalogId,
    compactions: Vec<RewriteDeleteCompaction>,
) -> CatalogResult<()> {
    rewrite_foundationdb_delete_files_at(
        catalog,
        compactions,
        None,
        None,
        SnapshotCommitMetadata::default(),
    )
}

#[cfg(feature = "foundationdb")]
fn rewrite_foundationdb_delete_files_at(
    catalog: CatalogId,
    compactions: Vec<RewriteDeleteCompaction>,
    read_snapshot: Option<DuckLakeSnapshotId>,
    proposed_commit_snapshot: Option<ProposedCommitSnapshot>,
    commit_metadata: SnapshotCommitMetadata,
) -> CatalogResult<()> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    let conflict_window = if let Some(read_snapshot) = read_snapshot {
        let Some(base) = crate::snapshot_by_public_sequence(&kv, catalog, read_snapshot)? else {
            return Err(crate::CatalogError::NotFound("read snapshot"));
        };
        let through = crate::latest_snapshot(&kv, catalog)?
            .ok_or(crate::CatalogError::NotFound("catalog snapshot"))?;
        Some((base.order, through.order))
    } else {
        None
    };
    for compaction in compactions {
        if let Some((base_order, through_order)) = conflict_window {
            kv.commit_rewrite_delete_data_files_versionstamped_with_conflict_check_and_metadata(
                catalog,
                base_order,
                through_order,
                proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
                commit_metadata.clone(),
                compaction,
            )?;
        } else {
            kv.commit_rewrite_delete_data_files_versionstamped_with_metadata(
                catalog,
                proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
                commit_metadata.clone(),
                compaction,
            )?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "foundationdb"))]
fn rewrite_foundationdb_delete_files(
    _catalog: CatalogId,
    _compactions: Vec<RewriteDeleteCompaction>,
) -> CatalogResult<()> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn rewrite_foundationdb_delete_files_at(
    _catalog: CatalogId,
    _compactions: Vec<RewriteDeleteCompaction>,
    _read_snapshot: Option<DuckLakeSnapshotId>,
    _proposed_commit_snapshot: Option<ProposedCommitSnapshot>,
    _commit_metadata: SnapshotCommitMetadata,
) -> CatalogResult<()> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

struct MergeAdjacentCompactionIntent {
    compaction: MergeAdjacentCompaction,
    file_visibility: Vec<CompactionFileVisibility>,
}

fn merge_adjacent_compactions_from_payload(
    parsed: CompactionPayload,
) -> CatalogResult<Vec<MergeAdjacentCompactionIntent>> {
    let table_ids = parsed
        .source_files
        .iter()
        .map(|source| source.table_id)
        .collect::<BTreeSet<_>>();
    let mut compactions = Vec::new();
    for table_id in table_ids {
        let source_file_ids = parsed
            .source_files
            .iter()
            .filter(|source| source.table_id == table_id)
            .map(|source| source.data_file_id)
            .collect::<Vec<_>>();
        let new_file_ids = parsed
            .new_files
            .iter()
            .filter(|file| file.table_id == table_id)
            .map(|file| file.data_file_id)
            .collect::<BTreeSet<_>>();
        compactions.push(MergeAdjacentCompactionIntent {
            compaction: MergeAdjacentCompaction {
                source_file_ids,
                new_files: parsed
                    .new_files
                    .iter()
                    .filter(|file| file.table_id == table_id)
                    .cloned()
                    .collect(),
                partition_values: parsed
                    .partition_values
                    .iter()
                    .filter(|row| row.table_id == table_id)
                    .cloned()
                    .collect(),
                file_column_stats: parsed
                    .file_column_stats
                    .iter()
                    .filter(|row| row.table_id == table_id)
                    .cloned()
                    .collect(),
            },
            file_visibility: parsed
                .file_visibility
                .iter()
                .filter(|visibility| new_file_ids.contains(&visibility.data_file_id))
                .copied()
                .collect(),
        });
    }
    Ok(compactions)
}

struct RewriteDeleteOperation {
    compactions: Vec<RewriteDeleteCompaction>,
}

impl RewriteDeleteOperation {
    fn from_payload(parsed: CompactionPayload) -> CatalogResult<Self> {
        let mut table_ids = parsed
            .source_files
            .iter()
            .map(|source| source.table_id)
            .collect::<BTreeSet<_>>();
        table_ids.extend(parsed.new_files.iter().map(|file| file.table_id));

        let mut compactions = Vec::new();
        for table_id in table_ids {
            let source_file_ids = parsed
                .source_files
                .iter()
                .filter(|source| source.table_id == table_id)
                .map(|source| source.data_file_id)
                .collect::<Vec<_>>();
            if source_file_ids.is_empty() {
                return Err(crate::CatalogError::Decode(format!(
                    "rewrite-delete payload has replacement files for table {} without source files",
                    table_id.0
                )));
            }
            compactions.push(RewriteDeleteCompaction {
                source_file_ids,
                new_files: parsed
                    .new_files
                    .iter()
                    .filter(|file| file.table_id == table_id)
                    .cloned()
                    .collect(),
                partition_values: parsed
                    .partition_values
                    .iter()
                    .filter(|row| row.table_id == table_id)
                    .cloned()
                    .collect(),
                file_column_stats: parsed
                    .file_column_stats
                    .iter()
                    .filter(|row| row.table_id == table_id)
                    .cloned()
                    .collect(),
            });
        }
        Ok(Self { compactions })
    }
}

struct CompactionPayload {
    source_files: Vec<CompactionSourceFile>,
    source_file_ids: Vec<DataFileId>,
    new_files: Vec<DataFileRow>,
    partition_values: Vec<FilePartitionValueRow>,
    file_column_stats: Vec<FileColumnStatsRow>,
    file_visibility: Vec<CompactionFileVisibility>,
}

#[derive(Clone, Copy)]
struct CompactionSourceFile {
    table_id: TableId,
    data_file_id: DataFileId,
}

#[derive(Clone, Copy)]
struct CompactionFileVisibility {
    data_file_id: DataFileId,
    begin_snapshot: DuckLakeSnapshotId,
    max_partial_snapshot: Option<DuckLakeSnapshotId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResolvedCompactionFileVisibility {
    data_file_id: DataFileId,
    begin_order: crate::CatalogOrderId,
    max_partial_order: Option<crate::CatalogOrderId>,
}

fn compaction_payload_values(
    operation: &'static str,
    payload: &[u8],
) -> CatalogResult<CompactionPayload> {
    let mut source_files = Vec::new();
    let mut source_file_ids = Vec::new();
    let mut new_files = Vec::new();
    let mut partition_values = Vec::new();
    let mut file_column_stats = Vec::new();
    let mut file_visibility = Vec::new();
    for row in TabularPayload::new(operation, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["source_file", _table_id, data_file_id] => {
                let table_id = TableId(parse_u64_field(operation, _table_id, "source table id")?);
                let data_file_id = DataFileId(parse_u64_field(
                    operation,
                    data_file_id,
                    "source data file id",
                )?);
                source_files.push(CompactionSourceFile {
                    table_id,
                    data_file_id,
                });
                source_file_ids.push(data_file_id);
            }
            [
                "file",
                id,
                table_id,
                path,
                row_count,
                file_size_bytes,
                row_id_start,
            ] => {
                new_files.push(compaction_file_row(
                    operation,
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    row_id_start,
                    None,
                )?);
            }
            [
                "file",
                id,
                table_id,
                path,
                row_count,
                file_size_bytes,
                row_id_start,
                mapping_id,
            ] => {
                new_files.push(compaction_file_row(
                    operation,
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    row_id_start,
                    Some(mapping_id),
                )?);
            }
            [
                "file",
                id,
                table_id,
                path,
                row_count,
                file_size_bytes,
                row_id_start,
                mapping_id,
                _begin_snapshot,
                _max_partial_snapshot,
            ] => {
                let data_file_id = DataFileId(parse_u64_field(operation, id, "data file id")?);
                new_files.push(compaction_file_row(
                    operation,
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    row_id_start,
                    Some(mapping_id),
                )?);
                if !_begin_snapshot.is_empty() || !_max_partial_snapshot.is_empty() {
                    file_visibility.push(CompactionFileVisibility {
                        data_file_id,
                        begin_snapshot: DuckLakeSnapshotId(parse_u64_field(
                            operation,
                            _begin_snapshot,
                            "file begin snapshot",
                        )?),
                        max_partial_snapshot: optional_u64_field(
                            operation,
                            _max_partial_snapshot,
                            "file max partial snapshot",
                        )?
                        .map(DuckLakeSnapshotId),
                    });
                }
            }
            [
                "file_partition",
                data_file_id,
                table_id,
                partition_key_index,
                partition_value,
            ] => {
                partition_values.push(FilePartitionValueRow::new(
                    DataFileId(parse_u64_field(
                        operation,
                        data_file_id,
                        "partition data file id",
                    )?),
                    TableId(parse_u64_field(operation, table_id, "partition table id")?),
                    PartitionKeyIndex(parse_u32_field(
                        operation,
                        partition_key_index,
                        "partition key index",
                    )?),
                    (*partition_value).to_owned(),
                ));
            }
            [
                "file_column_stats",
                data_file_id,
                table_id,
                column_id,
                value_count,
                null_count,
                min_value,
                max_value,
                extra_stats,
            ] => {
                file_column_stats.push(
                    FileColumnStatsRow::new(
                        DataFileId(parse_u64_field(
                            operation,
                            data_file_id,
                            "file column stats data file id",
                        )?),
                        TableId(parse_u64_field(
                            operation,
                            table_id,
                            "file column stats table id",
                        )?),
                        ColumnId(parse_u64_field(
                            operation,
                            column_id,
                            "file column stats column id",
                        )?),
                        parse_u64_field(operation, null_count, "file column stats null count")?,
                        optional_string_field(min_value),
                        optional_string_field(max_value),
                    )
                    .with_extra_stats(optional_string_field(extra_stats))
                    .with_value_count(optional_u64_field(
                        operation,
                        value_count,
                        "file column stats value count",
                    )?),
                );
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok(CompactionPayload {
        source_files,
        source_file_ids,
        new_files,
        partition_values,
        file_column_stats,
        file_visibility,
    })
}

fn resolve_compaction_file_visibility(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    compaction: &mut MergeAdjacentCompaction,
    file_visibility: &[CompactionFileVisibility],
) -> CatalogResult<()> {
    for visibility in file_visibility {
        let resolved = resolve_compaction_file_visibility_orders(kv, catalog, *visibility)?;
        let Some(file) = compaction
            .new_files
            .iter_mut()
            .find(|file| file.data_file_id == resolved.data_file_id)
        else {
            return Err(crate::CatalogError::Decode(format!(
                "compaction visibility references missing data file {}",
                resolved.data_file_id.0
            )));
        };
        file.validity.begin_order = resolved.begin_order;
        file.max_partial_order = resolved.max_partial_order;
    }
    Ok(())
}

fn resolve_compaction_file_visibility_orders(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    visibility: CompactionFileVisibility,
) -> CatalogResult<ResolvedCompactionFileVisibility> {
    Ok(ResolvedCompactionFileVisibility {
        data_file_id: visibility.data_file_id,
        begin_order: compaction_visibility_order(
            kv,
            catalog,
            visibility.begin_snapshot,
            visibility.data_file_id,
            "begin",
        )?,
        max_partial_order: visibility
            .max_partial_snapshot
            .map(|snapshot_id| {
                compaction_visibility_order(
                    kv,
                    catalog,
                    snapshot_id,
                    visibility.data_file_id,
                    "max partial",
                )
            })
            .transpose()?,
    })
}

fn compaction_visibility_order(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
    data_file_id: DataFileId,
    label: &str,
) -> CatalogResult<crate::CatalogOrderId> {
    if let Some(snapshot) = crate::snapshot_by_public_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    if let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    Err(crate::CatalogError::Decode(format!(
        "compaction data file {} references missing {label} snapshot {}",
        data_file_id.0, snapshot_id.0
    )))
}

fn optional_u64_field(
    operation: &'static str,
    value: &str,
    field: &str,
) -> CatalogResult<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_u64_field(operation, value, field)?))
}

fn optional_string_field(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn compaction_file_row(
    operation: &'static str,
    id: &str,
    table_id: &str,
    path: &str,
    row_count: &str,
    file_size_bytes: &str,
    row_id_start: &str,
    mapping_id: Option<&str>,
) -> CatalogResult<DataFileRow> {
    let mut row = DataFileRow::new(
        DataFileId(parse_u64_field(operation, id, "data file id")?),
        TableId(parse_u64_field(operation, table_id, "table id")?),
        path.to_owned(),
        parse_u64_field(operation, row_count, "file row count")?,
        parse_u64_field(operation, file_size_bytes, "file size bytes")?,
        crate::CatalogOrderId::uuid_v7(0),
    );
    if !row_id_start.is_empty() {
        row = row.with_row_id_start(parse_u64_field(operation, row_id_start, "row id start")?);
    }
    if let Some(mapping_id) = mapping_id
        && !mapping_id.is_empty()
    {
        row.mapping_id = Some(parse_u64_field(operation, mapping_id, "mapping id")?);
    }
    Ok(row)
}

#[cfg(test)]
#[path = "runtime_compaction_ops_tests.rs"]
mod runtime_compaction_ops_tests;
