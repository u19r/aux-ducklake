use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DataFileId, DuckLakeSnapshotId,
    InlineRowChangeKind, InlinedTableRow, RawSnapshotSequence, SchemaId, SnapshotCommitMetadata,
    TableId, TableRow, TableVersionReplacement, latest_snapshot, list_data_files_with_deletes_at,
    list_inline_file_deletion_rows_for_table_at, list_inline_file_deletions_at, list_tables_at,
    public_snapshot_sequence_for_order,
    runtime_foundationdb_inline::{
        runtime_foundationdb_delete_inline_rows, runtime_foundationdb_list_inline_row_changes,
        runtime_foundationdb_read_inline_rows,
        runtime_foundationdb_read_inline_rows_aggregate_stats,
        runtime_foundationdb_read_inline_rows_global_stats,
        runtime_foundationdb_read_inline_rows_global_stats_batch,
        runtime_foundationdb_register_inline_rows,
    },
    runtime_inline_rows::{
        InlineFlushDeletePositionsPayload, InlineRowChangesPayload, ReadInlineRowsPayload,
        inline_flush_delete_positions_payload,
    },
    runtime_payload::{
        optional_payload_string_value, optional_payload_u64_value, payload_string_value,
        payload_string_values, payload_u64_value,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot, ReadSnapshot},
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
    snapshot_by_raw_sequence,
    table_store::load_current_table_row,
};

pub(crate) fn read_inline_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = read_inline_rows_payload_values(payload)?;
    runtime_foundationdb_read_inline_rows(catalog, payload)
}

pub(crate) fn read_inline_rows_for_flush(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = read_inline_rows_for_flush_payload_values(payload)?;
    runtime_foundationdb_read_inline_rows(catalog, payload)
}

fn read_inline_rows_for_flush_payload_values(
    payload: &[u8],
) -> CatalogResult<ReadInlineRowsPayload> {
    let mut payload = read_inline_rows_payload_values(payload)?;
    payload.include_flushed = true;
    payload.include_deleted = true;
    Ok(payload)
}

pub(crate) fn read_inline_rows_for_global_stats(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = read_inline_rows_payload_values(payload)?;
    runtime_foundationdb_read_inline_rows_global_stats(catalog, payload)
}

pub(crate) fn read_inline_rows_for_aggregate_stats(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let mut payload = read_inline_rows_payload_values(payload)?;
    payload.include_flushed = false;
    payload.include_deleted = false;
    runtime_foundationdb_read_inline_rows_aggregate_stats(catalog, payload)
}

pub(crate) fn read_inline_rows_for_global_stats_batch(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payloads = read_inline_rows_for_global_stats_batch_payload_values(payload)?;
    runtime_foundationdb_read_inline_rows_global_stats_batch(catalog, payloads)
}

pub(crate) fn list_inline_row_insertions(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    list_inline_row_changes(
        backend,
        catalog,
        payload,
        "ListInlineRowInsertions",
        InlineRowChangeKind::Inserted,
    )
}

pub(crate) fn list_inline_row_deletions(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    list_inline_row_changes(
        backend,
        catalog,
        payload,
        "ListInlineRowDeletions",
        InlineRowChangeKind::Deleted,
    )
}

pub(crate) fn register_inline_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let requests = inline_rows_payloads(payload)?;
    let mut chunk_count = 0;
    for request in requests {
        chunk_count += runtime_foundationdb_register_inline_rows(catalog, request)?.len();
    }
    Ok(format!("inline_chunk_count={chunk_count}\n").into_bytes())
}

pub(crate) fn register_inline_tables(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let request = inline_materialization_request(payload)?;
    if request.materialization_files.is_empty() {
        return Ok(b"inline_table_count=0\n".to_vec());
    }
    let count = {
        let mut kv = open_foundationdb_catalog()?;
        register_inline_tables_with_foundationdb(&mut kv, catalog, request)?
    };
    Ok(format!("inline_table_count={count}\n").into_bytes())
}

#[cfg(any(test, not(feature = "foundationdb")))]
fn register_inline_tables_with_catalog(
    kv: &mut impl crate::MutableCatalogKv,
    catalog: CatalogId,
    request: InlineMaterializationRequest,
) -> CatalogResult<usize> {
    let latest =
        latest_snapshot(&*kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut replacements = Vec::new();
    for materialization in request.materialization_files {
        let previous = load_current_table_row(&*kv, catalog, materialization.table_id)?
            .ok_or(CatalogError::NotFound("inline table"))?;
        if previous
            .inlined_data_tables
            .iter()
            .any(|entry| entry.table_name == materialization.table_name)
        {
            continue;
        }
        let mut next = previous.clone();
        if !register_inlined_table(
            &mut next,
            materialization.table_name,
            materialization.schema_version,
        ) {
            continue;
        }
        replacements.push(TableVersionReplacement::new(
            materialization.table_id,
            previous,
            next,
        ));
    }
    let count = replacements.len();
    kv.commit_table_replacements(catalog, latest.sequence, replacements)?;
    Ok(count)
}

#[cfg(feature = "foundationdb")]
fn register_inline_tables_with_foundationdb(
    kv: &mut crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    request: InlineMaterializationRequest,
) -> CatalogResult<usize> {
    let latest =
        latest_snapshot(&*kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut replacements = Vec::new();
    for materialization in request.materialization_files {
        let previous = load_current_table_row(&*kv, catalog, materialization.table_id)?
            .ok_or(CatalogError::NotFound("inline table"))?;
        if previous
            .inlined_data_tables
            .iter()
            .any(|entry| entry.table_name == materialization.table_name)
        {
            continue;
        }
        let mut next = previous.clone();
        if !register_inlined_table(
            &mut next,
            materialization.table_name,
            materialization.schema_version,
        ) {
            continue;
        }
        replacements.push(TableVersionReplacement::new(
            materialization.table_id,
            previous,
            next,
        ));
    }
    let count = replacements.len();
    if let Some(commit_snapshot) = request.commit_snapshot {
        kv.commit_table_replacements_with_sequence_versionstamped(
            catalog,
            RawSnapshotSequence(commit_snapshot.0),
            None,
            replacements,
        )?;
    } else {
        kv.commit_table_replacements_versionstamped(catalog, latest.sequence, replacements)?;
    }
    Ok(count)
}

#[cfg(not(feature = "foundationdb"))]
fn register_inline_tables_with_foundationdb(
    kv: &mut impl crate::MutableCatalogKv,
    catalog: CatalogId,
    request: InlineMaterializationRequest,
) -> CatalogResult<usize> {
    register_inline_tables_with_catalog(kv, catalog, request)
}

pub(crate) fn delete_inline_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let request = inline_delete_payload(payload)?;
    let commit = { runtime_foundationdb_delete_inline_rows(catalog, request)? };
    Ok(format!(
        "deleted_inline_row_count={}\nrewritten_inline_payload_count={}\n",
        commit.deleted_row_count, commit.rewritten_payload_count
    )
    .into_bytes())
}

pub(crate) fn inline_file_deletions_exist(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_id = TableId(payload_u64_value(
        payload,
        "table_id",
        "InlineFileDeletionsExist missing table_id",
    )?);
    let snapshot_id = payload_u64_value(
        payload,
        "snapshot_id",
        "InlineFileDeletionsExist missing snapshot_id",
    )?;
    let (file_count, row_count) = {
        let kv = open_foundationdb_catalog()?;
        inline_file_deletion_counts(&kv, catalog, table_id, snapshot_id)?
    };
    Ok(format!(
        "inline_file_deletion_table_id={}\ninline_file_deletion_file_count={file_count}\ninline_file_deletion_row_count={row_count}\ninline_file_deletion_exists={}\n",
        table_id.0,
        row_count > 0
    )
    .into_bytes())
}

pub(crate) fn list_inline_file_deletions_for_flush(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_id = TableId(payload_u64_value(
        payload,
        "table_id",
        "ListInlineFileDeletionsForFlush missing table_id",
    )?);
    let snapshot_id = payload_u64_value(
        payload,
        "snapshot_id",
        "ListInlineFileDeletionsForFlush missing snapshot_id",
    )?;
    {
        let kv = open_foundationdb_catalog()?;
        inline_file_deletions_for_flush_payload(&kv, catalog, table_id, snapshot_id)
    }
}

pub(crate) fn list_inline_file_deletions(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_id = TableId(payload_u64_value(
        payload,
        "table_id",
        "ListInlineFileDeletions missing table_id",
    )?);
    let snapshot_id = payload_u64_value(
        payload,
        "snapshot_id",
        "ListInlineFileDeletions missing snapshot_id",
    )?;
    {
        let kv = open_foundationdb_catalog()?;
        inline_file_deletions_payload(&kv, catalog, table_id, snapshot_id)
    }
}

pub(crate) fn list_inline_flush_delete_positions(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload =
        inline_flush_delete_positions_payload_values(payload, "ListInlineFlushDeletePositions")?;
    list_inline_flush_delete_positions_for_payload(backend, catalog, payload)
}

fn list_inline_flush_delete_positions_for_payload(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: InlineFlushDeletePositionsPayload,
) -> CatalogResult<Vec<u8>> {
    {
        let kv = open_foundationdb_catalog()?;
        inline_flush_delete_positions_payload(&kv, catalog, payload)
    }
}

pub(crate) fn list_current_inline_flush_delete_positions(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let payload = inline_flush_delete_positions_payload_values(
        payload,
        "ListCurrentInlineFlushDeletePositions",
    )?;
    list_inline_flush_delete_positions_for_payload(backend, catalog, payload)
}

pub(crate) struct RuntimeInlineRows {
    #[allow(dead_code)]
    pub(crate) read_snapshot: Option<crate::DuckLakeSnapshotId>,
    pub(crate) commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    #[allow(dead_code)]
    pub(crate) commit_metadata: SnapshotCommitMetadata,
    pub(crate) table_id: TableId,
    pub(crate) schema_version: u64,
    pub(crate) table_name: String,
    pub(crate) payload: String,
}

pub(crate) struct RuntimeInlineDelete {
    pub(crate) commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    pub(crate) targets: Vec<RuntimeInlineDeleteTarget>,
}

#[derive(Clone)]
pub(crate) struct RuntimeInlineDeleteTarget {
    pub(crate) table_id: TableId,
    pub(crate) table_name: String,
    pub(crate) row_ids: Vec<u64>,
}

pub(crate) fn inline_table_for_register(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    request: &RuntimeInlineRows,
) -> CatalogResult<TableRow> {
    let _ = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut table = load_current_table_row(kv, catalog, request.table_id)?
        .ok_or(CatalogError::NotFound("inline table"))?;
    register_inlined_table(
        &mut table,
        request.table_name.clone(),
        request.schema_version,
    );
    Ok(table)
}

fn register_inlined_table(table: &mut TableRow, table_name: String, schema_version: u64) -> bool {
    if table
        .inlined_data_tables
        .iter()
        .any(|inlined| inlined.table_name == table_name && inlined.schema_version == schema_version)
    {
        return false;
    }
    table
        .inlined_data_tables
        .push(InlinedTableRow::new(table_name, schema_version));
    true
}

#[cfg(test)]
pub(crate) fn delete_inline_rows_with_catalog(
    kv: &mut impl crate::MutableCatalogKv,
    catalog: CatalogId,
    request: RuntimeInlineDelete,
) -> CatalogResult<crate::InlineTableDeleteCommit> {
    let mut commit = crate::InlineTableDeleteCommit {
        deleted_row_count: 0,
        rewritten_payload_count: 0,
    };
    for target in &request.targets {
        let schema_id = inline_delete_schema_id(kv, catalog, target)?;
        let next = crate::commit_delete_inline_table_rows_at_snapshot(
            kv,
            catalog,
            target.table_id,
            schema_id,
            &target.row_ids,
            request.commit_snapshot,
        )?;
        commit.deleted_row_count += next.deleted_row_count;
        commit.rewritten_payload_count += next.rewritten_payload_count;
    }
    Ok(commit)
}

pub(crate) fn inline_delete_schema_id(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    request: &RuntimeInlineDeleteTarget,
) -> CatalogResult<SchemaId> {
    let snapshot =
        latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    if let Some((table_id, schema_id)) = generated_inline_table_ids(&request.table_name)? {
        if table_id != request.table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "inline delete table id {} does not match inlined table {}",
                request.table_id.0, request.table_name
            )));
        }
        let table = load_current_table_row(kv, catalog, table_id)?
            .ok_or(CatalogError::NotFound("inlined table"))?;
        let registered = table.inlined_data_tables.iter().any(|inlined| {
            inlined.table_name == request.table_name && inlined.schema_version == schema_id.0
        });
        if registered {
            return Ok(schema_id);
        }
        return Err(CatalogError::NotFound("inlined table registration"));
    }
    let tables = list_tables_at(kv, catalog, snapshot.order)?;
    let (table, schema_id) = find_inlined_table(&tables, &request.table_name)
        .ok_or(CatalogError::NotFound("inlined table"))?;
    if table.table_id != request.table_id {
        return Err(CatalogError::InvalidMutation(format!(
            "inline delete table id {} does not match inlined table {}",
            request.table_id.0, request.table_name
        )));
    }
    Ok(schema_id)
}

fn generated_inline_table_ids(table_name: &str) -> CatalogResult<Option<(TableId, SchemaId)>> {
    let Some(tail) = table_name.strip_prefix("ducklake_inlined_data_") else {
        return Ok(None);
    };
    let Some((table_id, schema_id)) = tail.split_once('_') else {
        return Ok(None);
    };
    if table_id.is_empty() || schema_id.is_empty() || schema_id.contains('_') {
        return Ok(None);
    }
    Ok(Some((
        TableId(parse_u64_field(
            "DeleteInlineRows",
            table_id,
            "inline table id",
        )?),
        SchemaId(parse_u64_field(
            "DeleteInlineRows",
            schema_id,
            "inline schema id",
        )?),
    )))
}

fn list_inline_row_changes(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
    operation: &str,
    kind: InlineRowChangeKind,
) -> CatalogResult<Vec<u8>> {
    let payload = inline_row_changes_payload_values(payload, operation)?;
    runtime_foundationdb_list_inline_row_changes(catalog, payload, kind)
}

#[cfg(feature = "foundationdb")]
fn open_foundationdb_catalog() -> CatalogResult<crate::FdbOrderedCatalogKv> {
    crate::runtime_foundationdb::open_foundationdb_catalog()
}

#[cfg(not(feature = "foundationdb"))]
fn open_foundationdb_catalog() -> CatalogResult<crate::FakeOrderedCatalogKv> {
    Err(CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

fn inline_file_deletion_counts(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_id: u64,
) -> CatalogResult<(usize, usize)> {
    let snapshot = snapshot_by_raw_sequence(kv, catalog, RawSnapshotSequence(snapshot_id))?
        .ok_or(CatalogError::NotFound("snapshot"))?;
    let deletions = list_inline_file_deletions_at(kv, catalog, table_id, snapshot.order)?;
    let row_count = deletions.values().map(|rows| rows.len()).sum();
    Ok((deletions.len(), row_count))
}

fn inline_file_deletions_for_flush_payload(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    let snapshot = snapshot_by_raw_sequence(kv, catalog, RawSnapshotSequence(snapshot_id))?
        .ok_or(CatalogError::NotFound("snapshot"))?;
    let files = list_data_files_with_deletes_at(kv, catalog, table_id, snapshot.order)?
        .into_iter()
        .map(|attached| (attached.data_file.data_file_id, attached))
        .collect::<std::collections::BTreeMap<DataFileId, _>>();
    let deletions =
        list_inline_file_deletion_rows_for_table_at(kv, catalog, table_id, snapshot.order)?;
    let mut out = format!("inline_file_deletion_count={}\n", deletions.len());
    for deletion in deletions {
        let Some(attached) = files.get(&deletion.data_file_id) else {
            continue;
        };
        let begin_snapshot =
            public_snapshot_id_for_order(kv, catalog, deletion.validity.begin_order)?;
        let delete = attached.delete_file.as_ref();
        let delete_file_id = delete.map_or(String::new(), |row| row.delete_file_id.0.to_string());
        let delete_path = delete.map_or(String::new(), |row| row.path.clone());
        let delete_begin_snapshot = delete
            .map(|row| public_snapshot_id_for_order(kv, catalog, row.validity.begin_order))
            .transpose()?
            .map(|id| id.to_string())
            .unwrap_or_default();
        out.push_str(&format!(
            "inline_file_delete\t{}\t{}\tfalse\t{}\t{}\t{}\t{}\tfalse\t{}\t\t\n",
            deletion.data_file_id.0,
            attached.data_file.path,
            deletion.row_id,
            begin_snapshot,
            delete_file_id,
            delete_path,
            delete_begin_snapshot
        ));
    }
    Ok(out.into_bytes())
}

fn inline_file_deletions_payload(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
    let snapshot = snapshot_by_raw_sequence(kv, catalog, RawSnapshotSequence(snapshot_id))?
        .ok_or(CatalogError::NotFound("snapshot"))?;
    let deletions =
        list_inline_file_deletion_rows_for_table_at(kv, catalog, table_id, snapshot.order)?;
    let mut out = format!("inline_file_deletion_count={}\n", deletions.len());
    for deletion in deletions {
        out.push_str(&format!(
            "inline_file_delete\t{}\t{}\n",
            deletion.data_file_id.0, deletion.row_id
        ));
    }
    Ok(out.into_bytes())
}

fn public_snapshot_id_for_order(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    public_snapshot_sequence_for_order(kv, catalog, order)?
        .map(|snapshot| snapshot.0)
        .ok_or_else(|| CatalogError::Decode(format!("snapshot order {order} does not exist")))
}

#[cfg(test)]
fn inline_rows_payload(payload: &[u8]) -> CatalogResult<RuntimeInlineRows> {
    let mut rows = inline_rows_payloads(payload)?;
    match rows.len() {
        1 => Ok(rows.remove(0)),
        0 => Err(CatalogError::Decode(
            "RegisterInlineRows missing table header".to_owned(),
        )),
        _ => Err(CatalogError::Decode(
            "RegisterInlineRows contains multiple table sections".to_owned(),
        )),
    }
}

fn inline_rows_payloads(payload: &[u8]) -> CatalogResult<Vec<RuntimeInlineRows>> {
    let mut read_snapshot = None;
    let mut commit_snapshot = None;
    let mut commit_metadata = SnapshotCommitMetadata::default();
    let mut current = None;
    let mut requests = Vec::new();
    for row in TabularPayload::new("RegisterInlineRows", payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["read_snapshot", snapshot_id] => {
                read_snapshot = Some(crate::DuckLakeSnapshotId(parse_u64_field(
                    "RegisterInlineRows",
                    snapshot_id,
                    "read snapshot id",
                )?));
            }
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(crate::DuckLakeSnapshotId(parse_u64_field(
                    "RegisterInlineRows",
                    snapshot_id,
                    "commit snapshot id",
                )?));
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
            ["table", id, version, name] => {
                finish_inline_rows_section(
                    &mut requests,
                    &mut current,
                    read_snapshot,
                    commit_snapshot,
                    &commit_metadata,
                );
                current = Some(InlineRowsSection {
                    table_id: TableId(parse_u64_field(
                        "RegisterInlineRows",
                        id,
                        "inline table id",
                    )?),
                    schema_version: parse_u64_field(
                        "RegisterInlineRows",
                        version,
                        "inline schema version",
                    )?,
                    table_name: (*name).to_owned(),
                    payload: String::new(),
                });
            }
            ["row", ..] => {
                let Some(section) = current.as_mut() else {
                    return Err(CatalogError::Decode(
                        "RegisterInlineRows row appears before table header".to_owned(),
                    ));
                };
                section.payload.push_str(&fields.join("\t"));
                section.payload.push('\n');
            }
            ["row_begin", row_id, begin_snapshot, values @ ..] => {
                let commit_snapshot = commit_snapshot.ok_or_else(|| {
                    CatalogError::Decode(
                        "RegisterInlineRows row_begin requires commit_snapshot".to_owned(),
                    )
                })?;
                let begin_snapshot = crate::DuckLakeSnapshotId(parse_u64_field(
                    "RegisterInlineRows",
                    begin_snapshot,
                    "row begin snapshot id",
                )?);
                if begin_snapshot > commit_snapshot {
                    return Err(CatalogError::InvalidMutation(format!(
                        "inline row begin snapshot {} is after commit snapshot {}",
                        begin_snapshot.0, commit_snapshot.0
                    )));
                }
                if should_stage_row_begin(read_snapshot, begin_snapshot, commit_snapshot) {
                    let Some(section) = current.as_mut() else {
                        return Err(CatalogError::Decode(
                            "RegisterInlineRows row_begin appears before table header".to_owned(),
                        ));
                    };
                    section.payload.push_str("row\t");
                    section.payload.push_str(row_id);
                    for value in values {
                        section.payload.push('\t');
                        section.payload.push_str(value);
                    }
                    section.payload.push('\n');
                }
            }
            _ => return Err(row.invalid()),
        }
    }
    finish_inline_rows_section(
        &mut requests,
        &mut current,
        read_snapshot,
        commit_snapshot,
        &commit_metadata,
    );
    Ok(requests)
}

fn should_stage_row_begin(
    read_snapshot: Option<crate::DuckLakeSnapshotId>,
    begin_snapshot: crate::DuckLakeSnapshotId,
    commit_snapshot: crate::DuckLakeSnapshotId,
) -> bool {
    begin_snapshot == commit_snapshot
        || read_snapshot.is_some_and(|read_snapshot| begin_snapshot > read_snapshot)
}

struct InlineRowsSection {
    table_id: TableId,
    schema_version: u64,
    table_name: String,
    payload: String,
}

fn finish_inline_rows_section(
    requests: &mut Vec<RuntimeInlineRows>,
    current: &mut Option<InlineRowsSection>,
    read_snapshot: Option<crate::DuckLakeSnapshotId>,
    commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    commit_metadata: &SnapshotCommitMetadata,
) {
    let Some(section) = current.take() else {
        return;
    };
    requests.push(RuntimeInlineRows {
        read_snapshot,
        commit_snapshot,
        commit_metadata: commit_metadata.clone(),
        table_id: section.table_id,
        schema_version: section.schema_version,
        table_name: section.table_name,
        payload: section.payload,
    });
}

struct InlineMaterializationFile {
    table_id: TableId,
    schema_version: u64,
    table_name: String,
}

struct InlineMaterializationRequest {
    #[allow(dead_code)]
    commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    materialization_files: Vec<InlineMaterializationFile>,
}

fn inline_materialization_request(payload: &[u8]) -> CatalogResult<InlineMaterializationRequest> {
    let mut commit_snapshot = None;
    let mut materialization_files = Vec::new();
    for row in TabularPayload::new("RegisterInlineTables", payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(crate::DuckLakeSnapshotId(parse_u64_field(
                    "RegisterInlineTables",
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["table", id, version, name] => materialization_files.push(InlineMaterializationFile {
                table_id: TableId(parse_u64_field("RegisterInlineTables", id, "table id")?),
                schema_version: parse_u64_field("RegisterInlineTables", version, "schema version")?,
                table_name: (*name).to_owned(),
            }),
            _ => return Err(row.invalid()),
        }
    }
    Ok(InlineMaterializationRequest {
        commit_snapshot,
        materialization_files,
    })
}

fn inline_delete_payload(payload: &[u8]) -> CatalogResult<RuntimeInlineDelete> {
    let mut commit_snapshot = None;
    let mut targets = Vec::<RuntimeInlineDeleteTarget>::new();
    for row in TabularPayload::new("DeleteInlineRows", payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(crate::DuckLakeSnapshotId(parse_u64_field(
                    "DeleteInlineRows",
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["delete", raw_table_id, raw_table_name, row_id] => {
                let parsed_table_id = TableId(parse_u64_field(
                    "DeleteInlineRows",
                    raw_table_id,
                    "inline delete table id",
                )?);
                let parsed_table_name = (*raw_table_name).to_owned();
                let target = delete_target(&mut targets, parsed_table_id, parsed_table_name);
                target.row_ids.push(parse_u64_field(
                    "DeleteInlineRows",
                    row_id,
                    "inline delete row id",
                )?);
            }
            _ => return Err(row.invalid()),
        }
    }
    if targets.is_empty() {
        return Err(CatalogError::Decode(
            "DeleteInlineRows missing delete rows".to_owned(),
        ));
    }
    Ok(RuntimeInlineDelete {
        commit_snapshot,
        targets,
    })
}

#[cfg(test)]
#[path = "runtime_inline_ops_tests.rs"]
mod runtime_inline_ops_tests;

fn delete_target(
    targets: &mut Vec<RuntimeInlineDeleteTarget>,
    table_id: TableId,
    table_name: String,
) -> &mut RuntimeInlineDeleteTarget {
    if let Some(index) = targets
        .iter()
        .position(|target| target.table_id == table_id && target.table_name == table_name)
    {
        return &mut targets[index];
    }
    targets.push(RuntimeInlineDeleteTarget {
        table_id,
        table_name,
        row_ids: Vec::new(),
    });
    let index = targets.len() - 1;
    &mut targets[index]
}

fn find_inlined_table<'a>(
    tables: &'a [TableRow],
    table_name: &str,
) -> Option<(&'a TableRow, SchemaId)> {
    for table in tables {
        for inlined in &table.inlined_data_tables {
            if inlined.table_name == table_name {
                return Some((table, SchemaId(inlined.schema_version)));
            }
        }
    }
    None
}

fn read_inline_rows_payload_values(payload: &[u8]) -> CatalogResult<ReadInlineRowsPayload> {
    Ok(ReadInlineRowsPayload {
        table_name: payload_string_value(
            payload,
            "inlined_table_name",
            "ReadInlineRows missing inlined_table_name",
        )?,
        snapshot: optional_payload_u64_value(payload, "snapshot_id")?
            .map(|snapshot_id| ReadSnapshot::new(DuckLakeSnapshotId(snapshot_id))),
        include_flushed: optional_payload_bool_value(payload, "include_flushed")?,
        include_deleted: optional_payload_bool_value(payload, "include_deleted")?,
    })
}

fn read_inline_rows_for_global_stats_batch_payload_values(
    payload: &[u8],
) -> CatalogResult<Vec<ReadInlineRowsPayload>> {
    let snapshot_id = DuckLakeSnapshotId(payload_u64_value(
        payload,
        "snapshot_id",
        "ReadInlineRowsForGlobalStatsBatch missing snapshot_id",
    )?);
    let table_names = payload_string_values(payload, "inlined_table_name")?;
    if table_names.is_empty() {
        return Err(CatalogError::Decode(
            "ReadInlineRowsForGlobalStatsBatch missing inlined_table_name".to_owned(),
        ));
    }
    Ok(table_names
        .into_iter()
        .map(|table_name| ReadInlineRowsPayload {
            table_name,
            snapshot: Some(ReadSnapshot::new(snapshot_id)),
            include_flushed: false,
            include_deleted: false,
        })
        .collect())
}

fn optional_payload_bool_value(payload: &[u8], key: &str) -> CatalogResult<bool> {
    match crate::runtime_payload::optional_payload_str_value(payload, key)? {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(value) => Err(crate::CatalogError::Decode(format!(
            "ReadInlineRows payload has invalid {key} {value}"
        ))),
    }
}

fn inline_row_changes_payload_values(
    payload: &[u8],
    operation: &str,
) -> CatalogResult<InlineRowChangesPayload> {
    Ok(InlineRowChangesPayload {
        table_name: payload_string_value(
            payload,
            "inlined_table_name",
            &format!("{operation} missing inlined_table_name"),
        )?,
        start_snapshot: ChangeFeedStartSnapshot::new(DuckLakeSnapshotId(payload_u64_value(
            payload,
            "start_snapshot_id",
            &format!("{operation} missing start_snapshot_id"),
        )?)),
        end_snapshot: ChangeFeedEndSnapshot::new(DuckLakeSnapshotId(payload_u64_value(
            payload,
            "end_snapshot_id",
            &format!("{operation} missing end_snapshot_id"),
        )?)),
    })
}

fn inline_flush_delete_positions_payload_values(
    payload: &[u8],
    operation: &str,
) -> CatalogResult<InlineFlushDeletePositionsPayload> {
    Ok(InlineFlushDeletePositionsPayload {
        table_name: payload_string_value(
            payload,
            "inlined_table_name",
            &format!("{operation} missing inlined_table_name"),
        )?,
        snapshot: ReadSnapshot::new(DuckLakeSnapshotId(payload_u64_value(
            payload,
            "snapshot_id",
            &format!("{operation} missing snapshot_id"),
        )?)),
        file_order: optional_payload_string_value(payload, "file_order")?,
        partition_filter: optional_payload_string_value(payload, "partition_filter")?,
        position_start: optional_payload_u64_value(payload, "position_start")?,
        position_end: optional_payload_u64_value(payload, "position_end")?,
    })
}
