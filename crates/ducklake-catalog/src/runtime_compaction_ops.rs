use std::collections::BTreeMap;

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
    let source_count = parsed.source_files.len();
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
            let source_count = parsed.source_files.len();
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
            let source_count = parsed.source_files.len();
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
    let source_count = parsed.source_files.len();
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
    let mut compactions = Vec::new();
    for mut intent in merge_adjacent_compactions_from_payload(parsed)? {
        resolve_compaction_file_visibility(
            &kv,
            catalog,
            &mut intent.compaction,
            &intent.file_visibility,
        )?;
        compactions.push(intent.compaction);
    }
    let conflict_window = compaction_conflict_window(&kv, catalog, read_snapshot)?;
    kv.commit_merge_adjacent_data_files_batch_versionstamped(
        catalog,
        conflict_window,
        proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
        commit_metadata,
        compactions,
    )
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
    let conflict_window = compaction_conflict_window(&kv, catalog, read_snapshot)?;
    kv.commit_rewrite_delete_data_files_batch_versionstamped(
        catalog,
        conflict_window,
        proposed_commit_snapshot.map(ProposedCommitSnapshot::commit_attempt_id),
        commit_metadata,
        compactions,
    )
}

#[cfg(feature = "foundationdb")]
fn compaction_conflict_window(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    read_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<Option<(crate::CatalogOrderId, crate::CatalogOrderId)>> {
    let Some(read_snapshot) = read_snapshot else {
        return Ok(None);
    };
    let base = crate::snapshot_by_public_sequence(kv, catalog, read_snapshot)?
        .ok_or(crate::CatalogError::NotFound("read snapshot"))?;
    let through = crate::latest_snapshot(kv, catalog)?
        .ok_or(crate::CatalogError::NotFound("catalog snapshot"))?;
    Ok(Some((base.order, through.order)))
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
    compaction_tables_from_payload(parsed)?
        .into_iter()
        .map(|(table_id, table)| {
            require_compaction_sources(table_id, &table)?;
            Ok(MergeAdjacentCompactionIntent {
                compaction: MergeAdjacentCompaction {
                    source_file_ids: table.source_file_ids,
                    new_files: table.new_files,
                    partition_values: table.partition_values,
                    file_column_stats: table.file_column_stats,
                },
                file_visibility: table.file_visibility,
            })
        })
        .collect()
}

#[derive(Default)]
struct CompactionTablePayload {
    source_file_ids: Vec<DataFileId>,
    new_files: Vec<DataFileRow>,
    partition_values: Vec<FilePartitionValueRow>,
    file_column_stats: Vec<FileColumnStatsRow>,
    file_visibility: Vec<CompactionFileVisibility>,
}

fn compaction_tables_from_payload(
    parsed: CompactionPayload,
) -> CatalogResult<BTreeMap<TableId, CompactionTablePayload>> {
    let CompactionPayload {
        source_files,
        new_files,
        partition_values,
        file_column_stats,
        file_visibility,
    } = parsed;
    let mut tables = BTreeMap::<TableId, CompactionTablePayload>::new();
    for source in source_files {
        tables
            .entry(source.table_id)
            .or_default()
            .source_file_ids
            .push(source.data_file_id);
    }
    let mut new_file_tables = BTreeMap::new();
    for file in new_files {
        if new_file_tables
            .insert(file.data_file_id, file.table_id)
            .is_some()
        {
            return Err(crate::CatalogError::Decode(format!(
                "compaction payload repeats replacement data file {}",
                file.data_file_id.0
            )));
        }
        tables
            .entry(file.table_id)
            .or_default()
            .new_files
            .push(file);
    }
    for row in partition_values {
        tables
            .entry(row.table_id)
            .or_default()
            .partition_values
            .push(row);
    }
    for row in file_column_stats {
        tables
            .entry(row.table_id)
            .or_default()
            .file_column_stats
            .push(row);
    }
    for visibility in file_visibility {
        let Some(table_id) = new_file_tables.get(&visibility.data_file_id) else {
            return Err(crate::CatalogError::Decode(format!(
                "compaction visibility references missing data file {}",
                visibility.data_file_id.0
            )));
        };
        tables
            .entry(*table_id)
            .or_default()
            .file_visibility
            .push(visibility);
    }
    Ok(tables)
}

fn require_compaction_sources(
    table_id: TableId,
    table: &CompactionTablePayload,
) -> CatalogResult<()> {
    if table.source_file_ids.is_empty() {
        return Err(crate::CatalogError::Decode(format!(
            "compaction payload has replacement metadata for table {} without source files",
            table_id.0
        )));
    }
    Ok(())
}

struct RewriteDeleteOperation {
    compactions: Vec<RewriteDeleteCompaction>,
}

impl RewriteDeleteOperation {
    fn from_payload(parsed: CompactionPayload) -> CatalogResult<Self> {
        let compactions = compaction_tables_from_payload(parsed)?
            .into_iter()
            .map(|(table_id, table)| {
                require_compaction_sources(table_id, &table)?;
                Ok(RewriteDeleteCompaction {
                    source_file_ids: table.source_file_ids,
                    new_files: table.new_files,
                    partition_values: table.partition_values,
                    file_column_stats: table.file_column_stats,
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        Ok(Self { compactions })
    }
}

struct CompactionPayload {
    source_files: Vec<CompactionSourceFile>,
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
                    CompactionFileInput {
                        id,
                        table_id,
                        path,
                        row_count,
                        file_size_bytes,
                        row_id_start,
                        mapping_id: None,
                        encryption_key: None,
                    },
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
                    CompactionFileInput {
                        id,
                        table_id,
                        path,
                        row_count,
                        file_size_bytes,
                        row_id_start,
                        mapping_id: Some(mapping_id),
                        encryption_key: None,
                    },
                )?);
            }
            fields @ ["file", ..] if matches!(fields.len(), 10 | 11) => {
                let id = fields[1];
                let table_id = fields[2];
                let path = fields[3];
                let row_count = fields[4];
                let file_size_bytes = fields[5];
                let row_id_start = fields[6];
                let mapping_id = fields[7];
                let begin_snapshot = fields[8];
                let max_partial_snapshot = fields[9];
                let encryption_key = fields.get(10).copied();
                let data_file_id = DataFileId(parse_u64_field(operation, id, "data file id")?);
                new_files.push(compaction_file_row(
                    operation,
                    CompactionFileInput {
                        id,
                        table_id,
                        path,
                        row_count,
                        file_size_bytes,
                        row_id_start,
                        mapping_id: Some(mapping_id),
                        encryption_key,
                    },
                )?);
                if !begin_snapshot.is_empty() || !max_partial_snapshot.is_empty() {
                    file_visibility.push(CompactionFileVisibility {
                        data_file_id,
                        begin_snapshot: DuckLakeSnapshotId(parse_u64_field(
                            operation,
                            begin_snapshot,
                            "file begin snapshot",
                        )?),
                        max_partial_snapshot: optional_u64_field(
                            operation,
                            max_partial_snapshot,
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
    let file_positions = compaction
        .new_files
        .iter()
        .enumerate()
        .map(|(index, file)| (file.data_file_id, index))
        .collect::<BTreeMap<_, _>>();
    if file_positions.len() != compaction.new_files.len() {
        return Err(crate::CatalogError::Decode(
            "compaction replacement data file ids are not unique".to_owned(),
        ));
    }
    let mut snapshot_orders = BTreeMap::new();
    for visibility in file_visibility {
        let resolved = resolve_compaction_file_visibility_orders_with_cache(
            kv,
            catalog,
            *visibility,
            &mut snapshot_orders,
        )?;
        let Some(index) = file_positions.get(&resolved.data_file_id) else {
            return Err(crate::CatalogError::Decode(format!(
                "compaction visibility references missing data file {}",
                resolved.data_file_id.0
            )));
        };
        let file = &mut compaction.new_files[*index];
        file.validity.begin_order = resolved.begin_order;
        file.max_partial_order = resolved.max_partial_order;
    }
    Ok(())
}

#[cfg(test)]
fn resolve_compaction_file_visibility_orders(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    visibility: CompactionFileVisibility,
) -> CatalogResult<ResolvedCompactionFileVisibility> {
    resolve_compaction_file_visibility_orders_with_cache(
        kv,
        catalog,
        visibility,
        &mut BTreeMap::new(),
    )
}

fn resolve_compaction_file_visibility_orders_with_cache(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    visibility: CompactionFileVisibility,
    snapshot_orders: &mut BTreeMap<DuckLakeSnapshotId, crate::CatalogOrderId>,
) -> CatalogResult<ResolvedCompactionFileVisibility> {
    Ok(ResolvedCompactionFileVisibility {
        data_file_id: visibility.data_file_id,
        begin_order: compaction_visibility_order(
            kv,
            catalog,
            visibility.begin_snapshot,
            visibility.data_file_id,
            "begin",
            snapshot_orders,
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
                    snapshot_orders,
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
    snapshot_orders: &mut BTreeMap<DuckLakeSnapshotId, crate::CatalogOrderId>,
) -> CatalogResult<crate::CatalogOrderId> {
    if let Some(order) = snapshot_orders.get(&snapshot_id) {
        return Ok(*order);
    }
    if let Some(snapshot) = crate::snapshot_by_public_sequence(kv, catalog, snapshot_id)? {
        snapshot_orders.insert(snapshot_id, snapshot.order);
        return Ok(snapshot.order);
    }
    if let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, snapshot_id)? {
        snapshot_orders.insert(snapshot_id, snapshot.order);
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

struct CompactionFileInput<'a> {
    id: &'a str,
    table_id: &'a str,
    path: &'a str,
    row_count: &'a str,
    file_size_bytes: &'a str,
    row_id_start: &'a str,
    mapping_id: Option<&'a str>,
    encryption_key: Option<&'a str>,
}

fn compaction_file_row(
    operation: &'static str,
    input: CompactionFileInput<'_>,
) -> CatalogResult<DataFileRow> {
    let mut row = DataFileRow::new(
        DataFileId(parse_u64_field(operation, input.id, "data file id")?),
        TableId(parse_u64_field(operation, input.table_id, "table id")?),
        input.path.to_owned(),
        parse_u64_field(operation, input.row_count, "file row count")?,
        parse_u64_field(operation, input.file_size_bytes, "file size bytes")?,
        crate::CatalogOrderId::uuid_v7(0),
    );
    if !input.row_id_start.is_empty() {
        row = row.with_row_id_start(parse_u64_field(
            operation,
            input.row_id_start,
            "row id start",
        )?);
    }
    if let Some(mapping_id) = input.mapping_id
        && !mapping_id.is_empty()
    {
        row.mapping_id = Some(parse_u64_field(operation, mapping_id, "mapping id")?);
    }
    row.encryption_key = input.encryption_key.unwrap_or_default().to_owned();
    Ok(row)
}

#[cfg(test)]
#[path = "runtime_compaction_ops_tests.rs"]
mod runtime_compaction_ops_tests;
