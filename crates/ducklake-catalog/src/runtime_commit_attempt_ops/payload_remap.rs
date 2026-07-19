use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
};

use crate::{
    CatalogError, CatalogResult, RawSnapshotSequence, SnapshotCommitMetadata, TableColumnRow,
    TableId, TableRow,
    runtime_snapshot_range::ProposedCommitSnapshot,
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
};

use crate::runtime_commit_attempt_ops::*;
pub(super) struct TableIntentTable {
    pub(super) previous: TableRow,
    pub(super) next: TableRow,
}

pub(super) fn payload_with_commit_header(
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
    if include_read_snapshot && let Some(read_snapshot) = intent.read_snapshot {
        writeln!(&mut out, "read_snapshot\t{}", read_snapshot.0).map_err(|error| {
            CatalogError::Backend(format!("failed to render read snapshot header: {error}"))
        })?;
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

pub(super) fn push_commit_metadata_rows(
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

pub(super) fn include_read_snapshot_for_storage_intents(
    intent: &RuntimeCommitAttemptIntent,
) -> bool {
    intent.metadata_intents.is_empty()
}

pub(super) fn remap_inline_payload(
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

pub(super) fn remap_compaction_payload(
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

pub(super) fn remap_data_mutation_payload(
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

pub(super) fn remap_tabular_payload(
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

pub(super) fn remap_table_id_field(
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

pub(super) fn remap_inline_table_name_field(
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

pub(super) fn commit_snapshot_u64(snapshot: ProposedCommitSnapshot) -> CatalogResult<u64> {
    let snapshot = snapshot.commit_attempt_id();
    u64::try_from(snapshot.0).map_err(|_| {
        CatalogError::InvalidMutation(format!(
            "commit snapshot id {} does not fit in u64",
            snapshot.0
        ))
    })
}

pub(super) fn commit_attempt_sequence(
    snapshot: ProposedCommitSnapshot,
) -> CatalogResult<RawSnapshotSequence> {
    commit_snapshot_u64(snapshot).map(RawSnapshotSequence)
}

pub(super) fn same_column_identity(existing: &TableColumnRow, proposed: &TableColumnRow) -> bool {
    existing.column_id == proposed.column_id
        && existing.name.eq_ignore_ascii_case(&proposed.name)
        && same_column_shape_except_name(existing, proposed)
}

pub(super) fn same_column_shape_except_name(
    existing: &TableColumnRow,
    proposed: &TableColumnRow,
) -> bool {
    existing.column_id == proposed.column_id
        && existing.column_type == proposed.column_type
        && existing.nulls_allowed == proposed.nulls_allowed
        && existing.parent_id == proposed.parent_id
}

pub(super) fn reject_column_shape_change(
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

pub(super) fn reject_duplicate_column_names(
    table: &TableRow,
    table_id: TableId,
) -> CatalogResult<()> {
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

pub(super) fn apply_column_default(existing: &mut TableColumnRow, proposed: &TableColumnRow) {
    existing.initial_default = proposed.initial_default.clone();
    existing.default_value = proposed.default_value.clone();
    existing.default_value_type = proposed.default_value_type.clone();
}
