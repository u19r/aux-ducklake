use crate::{
    CatalogId, CatalogOrderId, CatalogResult, ColumnId, CommitAttemptId, DataFileId, DataFileRow,
    DataMutationCommit, DeleteFileId, DeleteFileRow, DuckLakeSnapshotId, FileColumnStatsRow,
    FilePartitionValueRow, InlineFileDeletionRow, InlineTableFlush, PartitionKeyIndex, SchemaId,
    SnapshotCommitMetadata, TableId,
    data_mutation_intents::DeleteFileMaterialization,
    inline_change_feed::data_file_covers_inline_begin,
    inline_data::list_unflushed_inline_table_payloads_at,
    runtime_foundationdb::runtime_foundationdb_commit_data_mutation,
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshot_range::{ProposedCommitSnapshot, SemanticDeleteCoverageBegin},
    runtime_tabular_payload::{TabularPayload, parse_u32_field, parse_u64_field},
    snapshot_by_ducklake_sequence,
    table_store::load_table_at,
};
use std::collections::BTreeSet;

const COMMIT_DATA_MUTATION: &str = "CommitDataMutation";

#[derive(Default)]
pub(crate) struct RuntimeDataMutation {
    pub(crate) proposed_commit_snapshot: Option<ProposedCommitSnapshot>,
    pub(crate) read_snapshot: Option<DuckLakeSnapshotId>,
    pub(crate) commit_metadata: SnapshotCommitMetadata,
    pub(crate) data_files: Vec<DataFileRow>,
    pub(crate) data_file_visibility: Vec<RuntimeDataFileVisibility>,
    pub(crate) file_partition_sets: Vec<RuntimeFilePartitionSet>,
    pub(crate) materialized_delete_files: Vec<DeleteFileMaterialization>,
    pub(crate) delete_file_visibility: Vec<RuntimeDeleteFileVisibility>,
    pub(crate) inline_flushes: Vec<InlineTableFlush>,
    pub(crate) partition_values: Vec<FilePartitionValueRow>,
    pub(crate) inline_file_deletions: Vec<InlineFileDeletionRow>,
    pub(crate) file_column_stats: Vec<FileColumnStatsRow>,
    pub(crate) dropped_data_file_ids: Vec<DataFileId>,
}

impl RuntimeDataMutation {
    pub(crate) fn materialized_delete_files(&self) -> Vec<DeleteFileRow> {
        DeleteFileMaterialization::rows(&self.materialized_delete_files)
    }

    pub(crate) fn resolve_proposed_commit_snapshot_from_inline_flushes(&mut self) {
        self.proposed_commit_snapshot = proposed_commit_snapshot_covering_inline_flushes(
            self.proposed_commit_snapshot,
            &self.inline_flushes,
        );
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeDataFileVisibility {
    pub(crate) data_file_id: DataFileId,
    pub(crate) begin_snapshot: DuckLakeSnapshotId,
    pub(crate) max_partial_snapshot: Option<DuckLakeSnapshotId>,
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct RuntimeFilePartitionSet {
    pub(crate) data_file_id: DataFileId,
    pub(crate) table_id: TableId,
    pub(crate) partition_id: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeDeleteFileVisibility {
    pub(crate) delete_file_id: DeleteFileId,
    pub(crate) begin_snapshot: SemanticDeleteCoverageBegin,
    pub(crate) max_partial_snapshot: Option<DuckLakeSnapshotId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedRuntimeVisibility {
    pub(crate) begin_order: CatalogOrderId,
    pub(crate) max_partial_order: Option<CatalogOrderId>,
}

pub(crate) fn commit_data_mutation(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let mut mutation = data_mutation_payload_values(payload)?;
    mutation.resolve_proposed_commit_snapshot_from_inline_flushes();
    let (commit, affected_table_ids) =
        { runtime_foundationdb_commit_data_mutation(catalog, mutation)? };
    Ok(data_mutation_payload(commit, &affected_table_ids))
}

pub(crate) fn resolve_data_file_visibility(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    mutation: &mut RuntimeDataMutation,
) -> CatalogResult<()> {
    for visibility in &mutation.data_file_visibility {
        let resolved = resolve_data_file_visibility_orders(kv, catalog, *visibility)?;
        let Some(file) = mutation
            .data_files
            .iter_mut()
            .find(|file| file.data_file_id == visibility.data_file_id)
        else {
            return Err(crate::CatalogError::Decode(format!(
                "visibility references missing data file {}",
                visibility.data_file_id.0
            )));
        };
        file.validity.begin_order = resolved.begin_order;
        file.max_partial_order = resolved.max_partial_order;
    }
    for visibility in &mutation.delete_file_visibility {
        let resolved = resolve_delete_file_visibility_orders(kv, catalog, *visibility)?;
        let Some(file) = mutation
            .materialized_delete_files
            .iter_mut()
            .find(|intent| intent.row().delete_file_id == visibility.delete_file_id)
        else {
            return Err(crate::CatalogError::Decode(format!(
                "visibility references missing delete file {}",
                visibility.delete_file_id.0
            )));
        };
        file.row_mut().validity.begin_order = resolved.begin_order;
        file.row_mut().max_partial_order = resolved.max_partial_order;
    }
    Ok(())
}

pub(crate) fn resolve_data_file_visibility_orders(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    visibility: RuntimeDataFileVisibility,
) -> CatalogResult<ResolvedRuntimeVisibility> {
    Ok(ResolvedRuntimeVisibility {
        begin_order: data_file_visibility_order(
            kv,
            catalog,
            visibility.begin_snapshot,
            visibility.data_file_id,
            "begin",
        )?,
        max_partial_order: visibility
            .max_partial_snapshot
            .map(|snapshot_id| {
                data_file_visibility_order(
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

pub(crate) fn resolve_delete_file_visibility_orders(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    visibility: RuntimeDeleteFileVisibility,
) -> CatalogResult<ResolvedRuntimeVisibility> {
    Ok(ResolvedRuntimeVisibility {
        begin_order: delete_file_visibility_order(
            kv,
            catalog,
            visibility.delete_file_id,
            visibility.begin_snapshot.public_id(),
            "begin",
        )?,
        max_partial_order: visibility
            .max_partial_snapshot
            .map(|snapshot_id| {
                delete_file_visibility_order(
                    kv,
                    catalog,
                    visibility.delete_file_id,
                    snapshot_id,
                    "max partial",
                )
            })
            .transpose()?,
    })
}

pub(crate) fn complete_inline_flushes_from_materialized_files(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    mutation: &mut RuntimeDataMutation,
) -> CatalogResult<()> {
    let files = mutation
        .data_files
        .iter()
        .filter(|file| file.max_partial_order.is_some())
        .cloned()
        .collect::<Vec<_>>();
    for file in files {
        complete_inline_flushes_for_materialized_file(kv, catalog, mutation, &file)?;
    }
    Ok(())
}

fn complete_inline_flushes_for_materialized_file(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    mutation: &mut RuntimeDataMutation,
    file: &DataFileRow,
) -> CatalogResult<()> {
    let Some(max_partial_order) = file.max_partial_order else {
        return Ok(());
    };
    let table = load_table_at(kv, catalog, file.table_id, file.validity.begin_order)?
        .ok_or(crate::CatalogError::NotFound("table"))?;
    for inlined in &table.inlined_data_tables {
        let schema_id = SchemaId(inlined.schema_version);
        let payloads = list_unflushed_inline_table_payloads_at(
            kv,
            catalog,
            file.table_id,
            schema_id,
            max_partial_order,
        )?;
        for payload in payloads {
            if !data_file_covers_inline_begin(file, payload.begin_order) {
                continue;
            }
            let Some(public_sequence) =
                crate::public_snapshot_sequence_for_order(kv, catalog, payload.begin_order)?
            else {
                return Err(crate::CatalogError::Decode(format!(
                    "inline payload for table {} at order {} has no public snapshot sequence",
                    file.table_id.0, payload.begin_order
                )));
            };
            let flush = InlineTableFlush::new(
                file.table_id,
                schema_id,
                crate::RawSnapshotSequence(public_sequence.0),
            );
            if !mutation.inline_flushes.contains(&flush) {
                mutation.inline_flushes.push(flush);
            }
            for delete_file in &mut mutation.materialized_delete_files {
                if delete_file.data_file_id() == file.data_file_id {
                    delete_file.mark_materializes_inline_deletes();
                }
            }
        }
    }
    Ok(())
}

fn delete_file_visibility_order(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
    snapshot_id: DuckLakeSnapshotId,
    label: &str,
) -> CatalogResult<crate::CatalogOrderId> {
    if let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    if let Some(snapshot) = crate::snapshot_by_public_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    Err(crate::CatalogError::Decode(format!(
        "delete file {} references missing {label} snapshot {}",
        delete_file_id.0, snapshot_id.0
    )))
}

fn data_file_visibility_order(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
    data_file_id: crate::DataFileId,
    label: &str,
) -> CatalogResult<crate::CatalogOrderId> {
    if let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    if let Some(snapshot) = crate::snapshot_by_public_sequence(kv, catalog, snapshot_id)? {
        return Ok(snapshot.order);
    }
    Err(crate::CatalogError::Decode(format!(
        "data file {} references missing {label} snapshot {}",
        data_file_id.0, snapshot_id.0
    )))
}

pub(crate) fn proposed_commit_snapshot_covering_inline_flushes(
    current: Option<ProposedCommitSnapshot>,
    inline_flushes: &[InlineTableFlush],
) -> Option<ProposedCommitSnapshot> {
    let Some(max_flush_snapshot) = inline_flushes
        .iter()
        .map(|flush| flush.flush_snapshot_sequence.0)
        .max()
    else {
        return current;
    };
    let minimum_commit_snapshot: u128 = max_flush_snapshot.saturating_add(1).into();
    match current {
        Some(snapshot) if snapshot.commit_attempt_id().0 >= minimum_commit_snapshot => {
            Some(snapshot)
        }
        _ => Some(ProposedCommitSnapshot::new(CommitAttemptId(
            minimum_commit_snapshot,
        ))),
    }
}

pub(crate) fn data_mutation_payload_values(payload: &[u8]) -> CatalogResult<RuntimeDataMutation> {
    let mut mutation = RuntimeDataMutation::default();
    for row in TabularPayload::new(COMMIT_DATA_MUTATION, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_attempt", snapshot_id] | ["commit_snapshot", snapshot_id] => {
                mutation.proposed_commit_snapshot =
                    Some(ProposedCommitSnapshot::new(CommitAttemptId(
                        parse_u64_field(COMMIT_DATA_MUTATION, snapshot_id, "commit snapshot id")?
                            .into(),
                    )));
            }
            ["read_snapshot", snapshot_id] => {
                mutation.read_snapshot = Some(DuckLakeSnapshotId(parse_u64_field(
                    COMMIT_DATA_MUTATION,
                    snapshot_id,
                    "read snapshot id",
                )?));
            }
            ["commit_author", author] => {
                mutation.commit_metadata.author = Some((*author).to_owned());
            }
            ["commit_message", message] => {
                mutation.commit_metadata.commit_message = Some((*message).to_owned());
            }
            ["commit_extra_info", extra_info] => {
                mutation.commit_metadata.commit_extra_info = Some((*extra_info).to_owned());
            }
            ["file", id, table_id, path, row_count, file_size_bytes] => {
                mutation.data_files.push(data_file_row(
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    None,
                    None,
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
            ] => {
                mutation.data_files.push(data_file_row(
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    Some(*row_id_start),
                    None,
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
                mutation.data_files.push(data_file_row(
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    Some(*row_id_start),
                    optional_u64_field(mapping_id, "mapping id")?,
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
                footer_size,
            ] => {
                mutation.data_files.push(data_file_row(
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    Some(*row_id_start),
                    optional_u64_field(mapping_id, "mapping id")?,
                    optional_u64_field(footer_size, "footer size")?,
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
                footer_size,
                begin_snapshot,
                max_partial_snapshot,
            ] => {
                let data_file_id =
                    DataFileId(parse_u64_field(COMMIT_DATA_MUTATION, id, "data file id")?);
                mutation.data_files.push(data_file_row(
                    id,
                    table_id,
                    path,
                    row_count,
                    file_size_bytes,
                    Some(*row_id_start),
                    optional_u64_field(mapping_id, "mapping id")?,
                    optional_u64_field(footer_size, "footer size")?,
                )?);
                if !begin_snapshot.is_empty() || !max_partial_snapshot.is_empty() {
                    mutation
                        .data_file_visibility
                        .push(RuntimeDataFileVisibility {
                            data_file_id,
                            begin_snapshot: DuckLakeSnapshotId(parse_u64_field(
                                COMMIT_DATA_MUTATION,
                                begin_snapshot,
                                "file begin snapshot",
                            )?),
                            max_partial_snapshot: optional_u64_field(
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
                mutation.partition_values.push(FilePartitionValueRow::new(
                    DataFileId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        data_file_id,
                        "partition data file id",
                    )?),
                    TableId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        table_id,
                        "partition table id",
                    )?),
                    PartitionKeyIndex(parse_u32_field(
                        COMMIT_DATA_MUTATION,
                        partition_key_index,
                        "partition key index",
                    )?),
                    (*partition_value).to_owned(),
                ));
            }
            ["file_partition_set", data_file_id, table_id, partition_id] => {
                mutation.file_partition_sets.push(RuntimeFilePartitionSet {
                    data_file_id: DataFileId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        data_file_id,
                        "partition set data file id",
                    )?),
                    table_id: TableId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        table_id,
                        "partition set table id",
                    )?),
                    partition_id: parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        partition_id,
                        "partition id",
                    )?,
                });
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
                mutation.file_column_stats.push(file_column_stats_row(
                    data_file_id,
                    table_id,
                    column_id,
                    Some(*value_count),
                    null_count,
                    min_value,
                    max_value,
                    extra_stats,
                )?);
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
            ] => {
                mutation.file_column_stats.push(file_column_stats_row(
                    data_file_id,
                    table_id,
                    column_id,
                    Some(*value_count),
                    null_count,
                    min_value,
                    max_value,
                    "",
                )?);
            }
            [
                "file_column_stats",
                data_file_id,
                table_id,
                column_id,
                null_count,
                min_value,
                max_value,
            ] => {
                mutation.file_column_stats.push(file_column_stats_row(
                    data_file_id,
                    table_id,
                    column_id,
                    None,
                    null_count,
                    min_value,
                    max_value,
                    "",
                )?);
            }
            [
                "delete_file",
                id,
                _table_id,
                data_file_id,
                path,
                delete_count,
                file_size_bytes,
                begin_snapshot,
                max_partial_snapshot,
            ] => {
                let delete_file_id =
                    DeleteFileId(parse_u64_field(COMMIT_DATA_MUTATION, id, "delete file id")?);
                push_delete_file_from_payload(
                    &mut mutation,
                    delete_file_id,
                    data_file_id,
                    path,
                    delete_count,
                    file_size_bytes,
                    begin_snapshot,
                    max_partial_snapshot,
                )?;
            }
            [
                "delete_file",
                id,
                _table_id,
                data_file_id,
                path,
                delete_count,
                file_size_bytes,
                begin_snapshot,
            ] => {
                let delete_file_id =
                    DeleteFileId(parse_u64_field(COMMIT_DATA_MUTATION, id, "delete file id")?);
                push_delete_file_from_payload(
                    &mut mutation,
                    delete_file_id,
                    data_file_id,
                    path,
                    delete_count,
                    file_size_bytes,
                    begin_snapshot,
                    "",
                )?;
            }
            ["inline", table_id, schema_id, flush_snapshot] => {
                mutation.inline_flushes.push(InlineTableFlush::new(
                    TableId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        table_id,
                        "inline flush table id",
                    )?),
                    SchemaId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        schema_id,
                        "inline flush schema id",
                    )?),
                    crate::RawSnapshotSequence(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        flush_snapshot,
                        "inline flush snapshot id",
                    )?),
                ));
            }
            ["inline_table", table_name, flush_snapshot] => {
                let inlined_table = parse_inlined_table_name(table_name)?;
                mutation.inline_flushes.push(InlineTableFlush::new(
                    inlined_table.table_id,
                    inlined_table.schema_id,
                    crate::RawSnapshotSequence(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        flush_snapshot,
                        "inline flush snapshot id",
                    )?),
                ));
            }
            ["inline_file_delete", table_id, data_file_id, row_id] => {
                mutation
                    .inline_file_deletions
                    .push(InlineFileDeletionRow::new(
                        TableId(parse_u64_field(
                            COMMIT_DATA_MUTATION,
                            table_id,
                            "inline file deletion table id",
                        )?),
                        DataFileId(parse_u64_field(
                            COMMIT_DATA_MUTATION,
                            data_file_id,
                            "inline file deletion data file id",
                        )?),
                        parse_u64_field(
                            COMMIT_DATA_MUTATION,
                            row_id,
                            "inline file deletion row id",
                        )?,
                        CatalogOrderId::uuid_v7(0),
                    ));
            }
            ["dropped_data_file", data_file_id] => {
                mutation
                    .dropped_data_file_ids
                    .push(DataFileId(parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        data_file_id,
                        "dropped data file id",
                    )?));
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok(mutation)
}

#[derive(Debug)]
struct ParsedInlinedTableName {
    table_id: TableId,
    schema_id: SchemaId,
}

fn parse_inlined_table_name(table_name: &str) -> CatalogResult<ParsedInlinedTableName> {
    let tail = table_name
        .strip_prefix("ducklake_inlined_data_")
        .ok_or_else(|| invalid_inlined_table_name(table_name))?;
    let Some((table_id, schema_id)) = tail.split_once('_') else {
        return Err(invalid_inlined_table_name(table_name));
    };
    if table_id.is_empty() || schema_id.is_empty() || schema_id.contains('_') {
        return Err(invalid_inlined_table_name(table_name));
    }
    Ok(ParsedInlinedTableName {
        table_id: TableId(parse_u64_field(
            COMMIT_DATA_MUTATION,
            table_id,
            "inline table id",
        )?),
        schema_id: SchemaId(parse_u64_field(
            COMMIT_DATA_MUTATION,
            schema_id,
            "inline schema id",
        )?),
    })
}

fn invalid_inlined_table_name(table_name: &str) -> crate::CatalogError {
    crate::CatalogError::Decode(format!(
        "CommitDataMutation inline table name is invalid: {table_name}"
    ))
}

fn push_delete_file_from_payload(
    mutation: &mut RuntimeDataMutation,
    delete_file_id: DeleteFileId,
    data_file_id: &str,
    path: &str,
    delete_count: &str,
    file_size_bytes: &str,
    begin_snapshot: &str,
    max_partial_snapshot: &str,
) -> CatalogResult<()> {
    mutation
        .materialized_delete_files
        .push(DeleteFileMaterialization::historical_delete_file(
            DeleteFileRow::new(
                delete_file_id,
                DataFileId(parse_u64_field(
                    COMMIT_DATA_MUTATION,
                    data_file_id,
                    "data file id",
                )?),
                path.to_owned(),
                parse_u64_field(COMMIT_DATA_MUTATION, delete_count, "delete count")?,
                parse_u64_field(
                    COMMIT_DATA_MUTATION,
                    file_size_bytes,
                    "delete file size bytes",
                )?,
                CatalogOrderId::uuid_v7(0),
            ),
        ));
    if !begin_snapshot.is_empty() {
        mutation
            .delete_file_visibility
            .push(RuntimeDeleteFileVisibility {
                delete_file_id,
                begin_snapshot: SemanticDeleteCoverageBegin::new(DuckLakeSnapshotId(
                    parse_u64_field(
                        COMMIT_DATA_MUTATION,
                        begin_snapshot,
                        "delete file begin snapshot",
                    )?,
                )),
                max_partial_snapshot: optional_u64_field(
                    max_partial_snapshot,
                    "delete file max partial snapshot",
                )?
                .map(DuckLakeSnapshotId),
            });
    }
    Ok(())
}

fn data_file_row(
    id: &str,
    table_id: &str,
    path: &str,
    row_count: &str,
    file_size_bytes: &str,
    row_id_start: Option<&str>,
    mapping_id: Option<u64>,
    footer_size: Option<u64>,
) -> CatalogResult<DataFileRow> {
    let row = DataFileRow::new(
        DataFileId(parse_u64_field(COMMIT_DATA_MUTATION, id, "data file id")?),
        TableId(parse_u64_field(COMMIT_DATA_MUTATION, table_id, "table id")?),
        path.to_owned(),
        parse_u64_field(COMMIT_DATA_MUTATION, row_count, "file row count")?,
        parse_u64_field(COMMIT_DATA_MUTATION, file_size_bytes, "file size bytes")?,
        CatalogOrderId::uuid_v7(0),
    )
    .with_mapping_id(mapping_id)
    .with_footer_size(footer_size);
    let Some(row_id_start) = row_id_start else {
        return Ok(row);
    };
    Ok(row.with_row_id_start(parse_u64_field(
        COMMIT_DATA_MUTATION,
        row_id_start,
        "row id start",
    )?))
}

fn optional_u64_field(value: &str, field: &str) -> CatalogResult<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_u64_field(COMMIT_DATA_MUTATION, value, field)?))
}

fn data_mutation_payload(commit: DataMutationCommit, affected_table_ids: &[TableId]) -> Vec<u8> {
    let affected_table_ids = affected_table_ids
        .iter()
        .map(|table_id| table_id.0.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "appended_file_count={}\n\
         added_delete_file_count={}\n\
         inline_file_deletion_count={}\n\
         file_partition_value_count={}\n\
         file_column_stats_count={}\n\
         flushed_inline_table_count={}\n\
         dropped_data_file_count={}\n\
         affected_table_ids={}\n",
        commit.data_files.len(),
        commit.delete_files.len(),
        commit.inline_file_deletion_count,
        commit.partition_value_count,
        commit.file_column_stats_count,
        commit.flushed_inline_count,
        commit.dropped_data_file_count,
        affected_table_ids,
    )
    .into_bytes()
}

pub(crate) fn affected_table_ids(mutation: &RuntimeDataMutation) -> CatalogResult<Vec<TableId>> {
    let mut table_ids = BTreeSet::new();
    let visibility_overrides = mutation
        .data_file_visibility
        .iter()
        .map(|visibility| visibility.data_file_id)
        .collect::<BTreeSet<_>>();
    table_ids.extend(
        mutation
            .data_files
            .iter()
            .filter(|file| !visibility_overrides.contains(&file.data_file_id))
            .map(|file| file.table_id),
    );
    Ok(table_ids.into_iter().collect())
}

fn optional_string_field(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn file_column_stats_row(
    data_file_id: &str,
    table_id: &str,
    column_id: &str,
    value_count: Option<&str>,
    null_count: &str,
    min_value: &str,
    max_value: &str,
    extra_stats: &str,
) -> CatalogResult<FileColumnStatsRow> {
    Ok(FileColumnStatsRow::new(
        DataFileId(parse_u64_field(
            COMMIT_DATA_MUTATION,
            data_file_id,
            "file column stats data file id",
        )?),
        TableId(parse_u64_field(
            COMMIT_DATA_MUTATION,
            table_id,
            "file column stats table id",
        )?),
        ColumnId(parse_u64_field(
            COMMIT_DATA_MUTATION,
            column_id,
            "file column stats column id",
        )?),
        parse_u64_field(
            COMMIT_DATA_MUTATION,
            null_count,
            "file column stats null count",
        )?,
        optional_string_field(min_value),
        optional_string_field(max_value),
    )
    .with_value_count(match value_count {
        Some(value) => Some(parse_u64_field(
            COMMIT_DATA_MUTATION,
            value,
            "file column stats value count",
        )?),
        None => None,
    })
    .with_extra_stats(optional_string_field(extra_stats)))
}

#[cfg(test)]
#[path = "runtime_data_mutation_ops_tests.rs"]
mod runtime_data_mutation_ops_tests;
