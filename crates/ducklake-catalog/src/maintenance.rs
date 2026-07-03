use std::collections::BTreeSet;

use crate::{
    CatalogId, CatalogOrderId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection, SchemaId, SnapshotRow, TableId,
    data_file_store::{data_file_visible_at, delete_file_can_answer_snapshot},
    file_partitions::{
        delete_partition_values_for_data_file, list_file_partition_values_for_data_files,
    },
    file_stats::remove_cached_file_column_stats_for_data_file,
    inline_data::{InlineTableChunkRow, decode_inline_table_item, inline_table_payload_prefix},
    keys::{
        KeyFamily, current_data_file_key, current_table_row_key, data_file_begin_key,
        data_file_end_key, data_file_key, delete_file_end_key, delete_file_key,
        delete_file_timeline_key, delete_file_timeline_order_from_key, delete_file_timeline_prefix,
        family_prefix, file_column_stats_lookup_key, inline_table_end_key,
        order_delete_file_change_key, prefix_end, scheduled_data_file_cleanup_key,
        scheduled_data_file_cleanup_prefix, scheduled_delete_file_cleanup_key,
        scheduled_delete_file_cleanup_prefix, table_delete_file_change_key, table_object_prefix,
        table_visibility_key,
    },
    rows::current_timestamp_micros,
    store::{invalidate_runtime_read_context, list_snapshots},
    table_store::list_table_rows,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileCleanupRow {
    pub delete_file: DeleteFileRow,
    pub table_id: TableId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDataFileCleanupRow {
    pub data_file: DataFileRow,
    pub schedule_start_micros: i64,
    pub cleanup_kind: ScheduledDataFileCleanupKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledDataFileCleanupKind {
    UnreachableOnly,
    CompactionReplacement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDeleteFileCleanupRow {
    pub delete_file: DeleteFileRow,
    pub table_id: TableId,
    pub schedule_start_micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteFilePhysicalCleanupDecision {
    NotCleanupCandidate,
    CleanupCandidateStillNeededByRetainedSnapshot,
    SafeToRemove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct InlineTableCleanupId {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub begin_order: CatalogOrderId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineTableCleanupRow {
    pub id: InlineTableCleanupId,
    pub chunk_count: usize,
}

pub fn list_old_data_files_for_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    let snapshots = list_snapshots(kv, catalog)?;
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for row in list_scheduled_data_file_cleanup_rows(kv, catalog)? {
        if !scheduled_data_file_is_safe_for_physical_cleanup(kv, catalog, &row, &snapshots)? {
            continue;
        }
        seen.insert(row.data_file.data_file_id.0);
        rows.push(row.data_file);
    }
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::EndOrder),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let Some(EndOrderEntry::DataFile { data_file_id, .. }) =
            decode_end_order_entry(catalog, &item.key)?
        else {
            continue;
        };
        if !seen.insert(data_file_id.0) {
            continue;
        }
        let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
            continue;
        };
        let row = DataFileRow::decode(&value)?;
        if row_is_unreachable(&row, &snapshots) {
            rows.push(row);
        }
    }
    Ok(rows)
}

pub fn list_old_delete_files_for_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DeleteFileCleanupRow>> {
    let snapshots = list_snapshots(kv, catalog)?;
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for row in list_scheduled_delete_file_cleanup_rows(kv, catalog)? {
        if row.delete_file.validity.end_order.is_none() {
            continue;
        }
        if !delete_file_is_cleanup_candidate(kv, catalog, &row.delete_file, &snapshots)? {
            continue;
        }
        seen.insert(row.delete_file.delete_file_id.0);
        rows.push(DeleteFileCleanupRow {
            delete_file: row.delete_file,
            table_id: row.table_id,
        });
    }
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::EndOrder),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let Some(EndOrderEntry::DeleteFile {
            table_id,
            delete_file_id,
        }) = decode_end_order_entry(catalog, &item.key)?
        else {
            continue;
        };
        if !seen.insert(delete_file_id.0) {
            continue;
        }
        let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
            continue;
        };
        let row = DeleteFileRow::decode(&value)?;
        if row.validity.end_order.is_some()
            && delete_file_is_cleanup_candidate(kv, catalog, &row, &snapshots)?
        {
            rows.push(DeleteFileCleanupRow {
                delete_file: row,
                table_id,
            });
        }
    }
    Ok(rows)
}

pub fn list_old_inline_table_payloads_for_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<InlineTableCleanupRow>> {
    let snapshots = list_snapshots(kv, catalog)?;
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::EndOrder),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let Some(EndOrderEntry::InlineTable {
            table_id,
            schema_id,
            begin_order,
            end_order,
        }) = decode_end_order_entry(catalog, &item.key)?
        else {
            continue;
        };
        let id = InlineTableCleanupId {
            table_id,
            schema_id,
            begin_order,
        };
        if !seen.insert(id) {
            continue;
        }
        let mut chunks = inline_table_payload_chunks(kv, catalog, id)?;
        for chunk in &mut chunks {
            chunk.validity.end_order = Some(end_order);
        }
        if inline_chunks_are_unreachable(&chunks, &snapshots) {
            rows.push(InlineTableCleanupRow {
                id,
                chunk_count: chunks.len(),
            });
        }
    }
    Ok(rows)
}

pub fn remove_old_data_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    if data_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    let cleanup_rows = list_old_data_files_for_cleanup(kv, catalog)?;
    let snapshots = list_snapshots(kv, catalog)?;
    let requested: BTreeSet<u64> = data_file_ids.iter().map(|id| id.0).collect();
    let rows: Vec<_> = cleanup_rows
        .into_iter()
        .filter(|row| requested.contains(&row.data_file_id.0))
        .collect();
    let affected_tables = rows.iter().map(|row| row.table_id).collect::<BTreeSet<_>>();
    let mut batch = KvBatch::new();
    for row in &rows {
        let scheduled = is_scheduled_data_file_cleanup(kv, catalog, row.data_file_id)?;
        if scheduled {
            batch.delete(scheduled_data_file_cleanup_key(catalog, row.data_file_id));
        }
        if row_is_unreachable(row, &snapshots) {
            stage_remove_data_file_metadata(kv, &mut batch, catalog, row)?;
        }
    }
    for table_id in affected_tables {
        stage_remove_unreachable_table_metadata(kv, &mut batch, catalog, table_id, &snapshots)?;
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub fn remove_old_delete_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    delete_file_ids: &[DeleteFileId],
) -> CatalogResult<Vec<DeleteFileCleanupRow>> {
    if delete_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    let cleanup_rows = list_old_delete_files_for_cleanup(kv, catalog)?;
    let snapshots = list_snapshots(kv, catalog)?;
    let requested: BTreeSet<u64> = delete_file_ids.iter().map(|id| id.0).collect();
    let rows: Vec<_> = cleanup_rows
        .into_iter()
        .filter(|row| requested.contains(&row.delete_file.delete_file_id.0))
        .collect();
    let affected_tables = rows.iter().map(|row| row.table_id).collect::<BTreeSet<_>>();
    let mut batch = KvBatch::new();
    for row in &rows {
        if is_scheduled_delete_file_cleanup(kv, catalog, row.delete_file.delete_file_id)? {
            batch.delete(scheduled_delete_file_cleanup_key(
                catalog,
                row.delete_file.delete_file_id,
            ));
        }
        if delete_file_cleanup_is_allowed(kv, catalog, &row.delete_file, &snapshots)? {
            stage_remove_delete_file_metadata(&mut batch, catalog, row);
        }
    }
    for table_id in affected_tables {
        stage_remove_unreachable_table_metadata(kv, &mut batch, catalog, table_id, &snapshots)?;
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub fn remove_old_inline_table_payloads(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    inline_ids: &[InlineTableCleanupId],
) -> CatalogResult<Vec<InlineTableCleanupRow>> {
    if inline_ids.is_empty() {
        return Ok(Vec::new());
    }
    let cleanup_rows = list_old_inline_table_payloads_for_cleanup(kv, catalog)?;
    let requested: BTreeSet<_> = inline_ids.iter().copied().collect();
    let rows: Vec<_> = cleanup_rows
        .into_iter()
        .filter(|row| requested.contains(&row.id))
        .collect();
    let mut batch = KvBatch::new();
    for row in &rows {
        stage_remove_inline_table_payload(kv, &mut batch, catalog, row.id)?;
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(rows)
}

pub(crate) fn stage_scheduled_data_file_cleanup(
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
) {
    stage_scheduled_data_file_cleanup_with_kind(
        batch,
        catalog,
        data_file_id,
        ScheduledDataFileCleanupKind::UnreachableOnly,
    );
}

pub(crate) fn stage_scheduled_compacted_data_file_cleanup(
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
) {
    stage_scheduled_data_file_cleanup_with_kind(
        batch,
        catalog,
        data_file_id,
        ScheduledDataFileCleanupKind::CompactionReplacement,
    );
}

fn stage_scheduled_data_file_cleanup_with_kind(
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
    cleanup_kind: ScheduledDataFileCleanupKind,
) {
    batch.put(
        scheduled_data_file_cleanup_key(catalog, data_file_id),
        encode_scheduled_data_cleanup_value(cleanup_kind, current_timestamp_micros()),
    );
}

pub(crate) fn stage_scheduled_delete_file_cleanup(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    delete_file_id: DeleteFileId,
) {
    batch.put(
        scheduled_delete_file_cleanup_key(catalog, delete_file_id),
        encode_scheduled_delete_cleanup_value(table_id, current_timestamp_micros()),
    );
}

pub(crate) fn list_scheduled_data_file_cleanup_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<ScheduledDataFileCleanupRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &scheduled_data_file_cleanup_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let data_file_id = DataFileId(decode_cleanup_id(&item.key)?);
        if let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? {
            let (cleanup_kind, schedule_start_micros) =
                decode_scheduled_data_cleanup_value(&item.value)?;
            rows.push(ScheduledDataFileCleanupRow {
                data_file: DataFileRow::decode(&value)?,
                schedule_start_micros,
                cleanup_kind,
            });
        }
    }
    Ok(rows)
}

pub(crate) fn list_scheduled_delete_file_cleanup_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<ScheduledDeleteFileCleanupRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &scheduled_delete_file_cleanup_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let delete_file_id = DeleteFileId(decode_cleanup_id(&item.key)?);
        if let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? {
            let delete_file = DeleteFileRow::decode(&value)?;
            let (table_id, schedule_start_micros) =
                decode_scheduled_delete_cleanup_value(kv, catalog, &item.value, &delete_file)?;
            rows.push(ScheduledDeleteFileCleanupRow {
                schedule_start_micros,
                table_id,
                delete_file,
            });
        }
    }
    Ok(rows)
}

pub(crate) fn encode_scheduled_data_cleanup_value(
    cleanup_kind: ScheduledDataFileCleanupKind,
    schedule_start_micros: i64,
) -> Vec<u8> {
    let mut value = vec![1, cleanup_kind_tag(cleanup_kind)];
    value.extend_from_slice(&schedule_start_micros.to_be_bytes());
    value
}

fn decode_scheduled_data_cleanup_value(
    value: &[u8],
) -> CatalogResult<(ScheduledDataFileCleanupKind, i64)> {
    if value.is_empty() {
        return Ok((ScheduledDataFileCleanupKind::UnreachableOnly, 0));
    }
    if value.len() == 8 {
        return Ok((
            ScheduledDataFileCleanupKind::UnreachableOnly,
            i64::from_be_bytes(value.try_into().map_err(|_| {
                crate::CatalogError::Decode("scheduled cleanup timestamp is truncated".to_owned())
            })?),
        ));
    }
    if value.len() != 10 || value[0] != 1 {
        return Err(crate::CatalogError::Decode(format!(
            "scheduled data cleanup value has {} bytes",
            value.len()
        )));
    }
    Ok((
        cleanup_kind_from_tag(value[1])?,
        i64::from_be_bytes(value[2..10].try_into().map_err(|_| {
            crate::CatalogError::Decode("scheduled cleanup timestamp is truncated".to_owned())
        })?),
    ))
}

fn cleanup_kind_tag(cleanup_kind: ScheduledDataFileCleanupKind) -> u8 {
    match cleanup_kind {
        ScheduledDataFileCleanupKind::UnreachableOnly => 0,
        ScheduledDataFileCleanupKind::CompactionReplacement => 1,
    }
}

fn cleanup_kind_from_tag(tag: u8) -> CatalogResult<ScheduledDataFileCleanupKind> {
    match tag {
        0 => Ok(ScheduledDataFileCleanupKind::UnreachableOnly),
        1 => Ok(ScheduledDataFileCleanupKind::CompactionReplacement),
        other => Err(crate::CatalogError::Decode(format!(
            "scheduled data cleanup kind {other} is unknown"
        ))),
    }
}

pub(crate) fn encode_scheduled_delete_cleanup_value(
    table_id: TableId,
    schedule_start_micros: i64,
) -> Vec<u8> {
    let mut value = table_id.0.to_be_bytes().to_vec();
    value.extend_from_slice(&schedule_start_micros.to_be_bytes());
    value
}

fn decode_scheduled_delete_cleanup_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    scheduled_value: &[u8],
    delete_file: &DeleteFileRow,
) -> CatalogResult<(TableId, i64)> {
    match scheduled_value.len() {
        16 => Ok((
            TableId(u64::from_be_bytes(
                scheduled_value[0..8].try_into().map_err(|_| {
                    crate::CatalogError::Decode(
                        "scheduled delete cleanup table id is truncated".to_owned(),
                    )
                })?,
            )),
            i64::from_be_bytes(scheduled_value[8..16].try_into().map_err(|_| {
                crate::CatalogError::Decode(
                    "scheduled delete cleanup timestamp is truncated".to_owned(),
                )
            })?),
        )),
        8 => Ok((
            TableId(u64::from_be_bytes(scheduled_value.try_into().map_err(
                |_| {
                    crate::CatalogError::Decode(
                        "scheduled delete cleanup table id is truncated".to_owned(),
                    )
                },
            )?)),
            0,
        )),
        0 => Ok((
            scheduled_delete_cleanup_table_id(kv, catalog, delete_file)?,
            0,
        )),
        other => Err(crate::CatalogError::Decode(format!(
            "scheduled delete cleanup value has {other} bytes"
        ))),
    }
}

fn scheduled_delete_cleanup_table_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file: &DeleteFileRow,
) -> CatalogResult<TableId> {
    let Some(value) = kv.get(&data_file_key(catalog, delete_file.data_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file data file"));
    };
    Ok(DataFileRow::decode(&value)?.table_id)
}

fn is_scheduled_data_file_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<bool> {
    Ok(kv
        .get(&scheduled_data_file_cleanup_key(catalog, data_file_id))?
        .is_some())
}

fn is_scheduled_delete_file_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<bool> {
    Ok(kv
        .get(&scheduled_delete_file_cleanup_key(catalog, delete_file_id))?
        .is_some())
}

fn stage_remove_unreachable_table_metadata(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    snapshots: &[SnapshotRow],
) -> CatalogResult<()> {
    let table_rows = list_table_rows(kv, catalog)?
        .into_iter()
        .filter(|row| row.table_id == table_id)
        .collect::<Vec<_>>();
    if table_rows.is_empty()
        || table_rows.iter().any(|row| {
            snapshots
                .iter()
                .any(|snapshot| row.validity.is_visible_at(snapshot.order))
        })
    {
        return Ok(());
    }
    for row in &table_rows {
        batch.delete(table_visibility_key(
            catalog,
            row.validity.begin_order,
            row.table_id,
        ));
    }
    batch.delete(current_table_row_key(catalog, table_id));
    let table_prefix = table_object_prefix(catalog, table_id);
    for item in kv.scan_prefix(&table_prefix, RangeDirection::Forward, usize::MAX)? {
        batch.delete(item.key);
    }
    Ok(())
}

fn decode_cleanup_id(key: &[u8]) -> CatalogResult<u64> {
    let id_start = key.len().saturating_sub(8);
    if key.len() < 8 {
        return Err(crate::CatalogError::InvalidKey(
            "scheduled cleanup key is too short".to_owned(),
        ));
    }
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&key[id_start..]);
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn row_is_unreachable(row: &DataFileRow, snapshots: &[SnapshotRow]) -> bool {
    row.validity.end_order.is_some()
        && !snapshots
            .iter()
            .any(|snapshot| data_file_visible_at(row, snapshot.order))
}

pub(crate) fn data_file_is_safe_for_physical_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DataFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    if row.validity.end_order.is_none() {
        return Ok(false);
    }
    for snapshot in snapshots
        .iter()
        .filter(|snapshot| data_file_visible_at(row, snapshot.order))
    {
        if !has_covering_data_file(kv, catalog, row, snapshot.order)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn scheduled_data_file_is_safe_for_physical_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &ScheduledDataFileCleanupRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    if row.data_file.validity.end_order.is_none() {
        return Ok(false);
    }
    if matches!(
        row.cleanup_kind,
        ScheduledDataFileCleanupKind::CompactionReplacement
    ) {
        return Ok(true);
    }
    data_file_is_safe_for_physical_cleanup(kv, catalog, &row.data_file, snapshots)
}

fn has_covering_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DataFileRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::DataFile),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let candidate = DataFileRow::decode(&item.value)?;
        if candidate.data_file_id == row.data_file_id
            || candidate.validity.begin_order > snapshot_order
        {
            continue;
        }
        if data_file_covers(&candidate, row)
            || partition_replacement_covers(kv, catalog, &candidate, row, snapshot_order)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn data_file_covers(candidate: &DataFileRow, row: &DataFileRow) -> bool {
    if candidate.table_id != row.table_id
        || !candidate.row_id_start_known
        || !row.row_id_start_known
        || row.record_count == 0
    {
        return false;
    }
    let candidate_end = candidate
        .row_id_start
        .saturating_add(candidate.record_count);
    let row_end = row.row_id_start.saturating_add(row.record_count);
    candidate.row_id_start <= row.row_id_start && row_end <= candidate_end
}

fn partition_replacement_covers(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    candidate: &DataFileRow,
    row: &DataFileRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    if candidate.table_id != row.table_id
        || candidate.record_count == 0
        || candidate.record_count <= row.record_count
        || candidate.validity.begin_order > snapshot_order
    {
        return Ok(false);
    }
    let candidate_key = data_file_partition_key(kv, catalog, candidate.data_file_id)?;
    Ok(!candidate_key.is_empty()
        && candidate_key == data_file_partition_key(kv, catalog, row.data_file_id)?)
}

fn data_file_partition_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Vec<(u32, String)>> {
    let data_file_ids = BTreeSet::from([data_file_id]);
    let mut values = list_file_partition_values_for_data_files(kv, catalog, &data_file_ids)?
        .into_iter()
        .map(|row| (row.partition_key_index.0, row.partition_value))
        .collect::<Vec<_>>();
    values.sort();
    Ok(values)
}

pub(crate) fn delete_file_is_safe_for_physical_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    Ok(matches!(
        delete_file_physical_cleanup_decision(kv, catalog, row, snapshots)?,
        DeleteFilePhysicalCleanupDecision::SafeToRemove
    ))
}

pub(crate) fn delete_file_physical_cleanup_decision(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<DeleteFilePhysicalCleanupDecision> {
    if row.validity.end_order.is_none() {
        return Ok(DeleteFilePhysicalCleanupDecision::NotCleanupCandidate);
    }
    if delete_file_answers_any_retained_snapshot(kv, catalog, row, snapshots)? {
        return Ok(
            DeleteFilePhysicalCleanupDecision::CleanupCandidateStillNeededByRetainedSnapshot,
        );
    }
    Ok(DeleteFilePhysicalCleanupDecision::SafeToRemove)
}

fn delete_file_is_cleanup_candidate(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    if row.validity.end_order.is_none() {
        return Ok(false);
    }
    Ok(delete_file_is_unreachable(row, snapshots)
        || source_data_file_is_current(kv, catalog, row)?
        || source_data_file_has_compaction_replacement_cleanup(kv, catalog, row.data_file_id)?)
}

fn delete_file_is_unreachable(row: &DeleteFileRow, snapshots: &[SnapshotRow]) -> bool {
    row.validity.end_order.is_some()
        && !snapshots
            .iter()
            .any(|snapshot| row.validity.is_visible_at(snapshot.order))
}

fn source_data_file_is_current(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<bool> {
    let Some(value) = kv.get(&data_file_key(catalog, row.data_file_id))? else {
        return Ok(false);
    };
    let data_file = DataFileRow::decode(&value)?;
    Ok(kv
        .get(&current_data_file_key(
            catalog,
            data_file.table_id,
            row.data_file_id,
        ))?
        .is_some())
}

fn delete_file_cleanup_is_allowed(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    Ok(
        delete_file_is_safe_for_physical_cleanup(kv, catalog, row, snapshots)?
            || source_data_file_has_compaction_replacement_cleanup(kv, catalog, row.data_file_id)?,
    )
}

fn source_data_file_has_compaction_replacement_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<bool> {
    let Some(value) = kv.get(&scheduled_data_file_cleanup_key(catalog, data_file_id))? else {
        return Ok(false);
    };
    let (cleanup_kind, _) = decode_scheduled_data_cleanup_value(&value)?;
    Ok(matches!(
        cleanup_kind,
        ScheduledDataFileCleanupKind::CompactionReplacement
    ))
}

fn delete_file_answers_any_retained_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
    snapshots: &[SnapshotRow],
) -> CatalogResult<bool> {
    for timeline_order in delete_file_timeline_orders(kv, catalog, row)? {
        if snapshots
            .iter()
            .any(|snapshot| delete_file_can_answer_snapshot(row, timeline_order, snapshot.order))
        {
            return Ok(true);
        }
    }
    Ok(snapshots
        .iter()
        .any(|snapshot| row.validity.is_visible_at(snapshot.order)))
}

fn delete_file_timeline_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &DeleteFileRow,
) -> CatalogResult<Vec<CatalogOrderId>> {
    let mut orders = Vec::new();
    let prefix = delete_file_timeline_prefix(catalog, row.data_file_id);
    for item in kv.scan_range(
        &prefix,
        &prefix_end(&prefix),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let delete_file_id = match DeleteFileRow::decode(&item.value) {
            Ok(existing) => existing.delete_file_id,
            Err(_) => decode_delete_file_id(&item.value)?,
        };
        if delete_file_id == row.delete_file_id {
            orders.push(delete_file_timeline_order_from_key(
                catalog,
                row.data_file_id,
                &item.key,
                row.validity.begin_order.kind(),
            )?);
        }
    }
    Ok(orders)
}

fn decode_delete_file_id(value: &[u8]) -> CatalogResult<DeleteFileId> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        crate::CatalogError::Decode("delete file id value must be 8 bytes".to_owned())
    })?;
    Ok(DeleteFileId(u64::from_be_bytes(bytes)))
}

fn inline_chunks_are_unreachable(
    chunks: &[InlineTableChunkRow],
    snapshots: &[SnapshotRow],
) -> bool {
    !chunks.is_empty()
        && chunks.iter().all(|chunk| {
            chunk.validity.end_order.is_some()
                && !snapshots
                    .iter()
                    .any(|snapshot| chunk.validity.is_visible_at(snapshot.order))
        })
}

fn stage_remove_data_file_metadata(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DataFileRow,
) -> CatalogResult<()> {
    crate::immutable_file_metadata::remove_immutable_data_file_metadata(
        kv,
        catalog,
        row.data_file_id,
    );
    batch.delete(data_file_key(catalog, row.data_file_id));
    batch.delete(data_file_begin_key(
        catalog,
        row.table_id,
        row.validity.begin_order,
        row.data_file_id,
    ));
    if let Some(end_order) = row.validity.end_order {
        batch.delete(data_file_end_key(
            catalog,
            row.table_id,
            end_order,
            row.data_file_id,
        ));
    }
    batch.delete(current_data_file_key(
        catalog,
        row.table_id,
        row.data_file_id,
    ));
    stage_remove_file_column_stats(kv, batch, catalog, row.data_file_id)?;
    delete_partition_values_for_data_file(kv, batch, catalog, row.data_file_id)
}

fn stage_remove_inline_table_payload(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    id: InlineTableCleanupId,
) -> CatalogResult<()> {
    let chunks = inline_table_payload_chunks(kv, catalog, id)?;
    let mut end_order = None;
    for item in kv.scan_prefix(
        &inline_table_payload_prefix(catalog, id.table_id, id.schema_id, id.begin_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        batch.delete(item.key);
    }
    for chunk in chunks {
        end_order = chunk.validity.end_order;
    }
    if let Some(order) = end_order {
        batch.delete(inline_table_end_key(
            catalog,
            id.table_id,
            order,
            id.schema_id,
            id.begin_order,
        ));
    }
    Ok(())
}

fn stage_remove_delete_file_metadata(
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DeleteFileCleanupRow,
) {
    let delete_file = &row.delete_file;
    batch.delete(delete_file_key(catalog, delete_file.delete_file_id));
    batch.delete(delete_file_timeline_key(
        catalog,
        delete_file.data_file_id,
        delete_file.validity.begin_order,
        delete_file.delete_file_id,
    ));
    batch.delete(table_delete_file_change_key(
        catalog,
        row.table_id,
        delete_file.validity.begin_order,
        delete_file.delete_file_id,
    ));
    batch.delete(order_delete_file_change_key(
        catalog,
        delete_file.validity.begin_order,
        row.table_id,
        delete_file.delete_file_id,
    ));
    if let Some(end_order) = delete_file.validity.end_order {
        batch.delete(delete_file_end_key(
            catalog,
            row.table_id,
            end_order,
            delete_file.delete_file_id,
        ));
    }
}

fn inline_table_payload_chunks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    id: InlineTableCleanupId,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    kv.scan_prefix(
        &inline_table_payload_prefix(catalog, id.table_id, id.schema_id, id.begin_order),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| decode_inline_table_item(catalog, &item.key, &item.value))
    .collect()
}

fn stage_remove_file_column_stats(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    for item in kv.scan_prefix(
        &file_column_stats_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = crate::FileColumnStatsRow::decode(&item.value)?;
        batch.delete(item.key);
        batch.delete(file_column_stats_lookup_key(
            catalog,
            row.table_id,
            row.column_id,
            row.data_file_id,
        ));
    }
    remove_cached_file_column_stats_for_data_file(kv, catalog, data_file_id);
    Ok(())
}

fn file_column_stats_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::FileColumnStats);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndOrderEntry {
    DataFile {
        data_file_id: DataFileId,
    },
    DeleteFile {
        table_id: TableId,
        delete_file_id: DeleteFileId,
    },
    InlineTable {
        table_id: TableId,
        schema_id: SchemaId,
        begin_order: CatalogOrderId,
        end_order: CatalogOrderId,
    },
}

fn decode_end_order_entry(catalog: CatalogId, key: &[u8]) -> CatalogResult<Option<EndOrderEntry>> {
    let prefix = family_prefix(catalog, KeyFamily::EndOrder);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Ok(None);
    };
    let minimum_len = 8 + 1 + CatalogOrderId::LEN + 1 + 8;
    if tail.len() < minimum_len {
        return Err(crate::CatalogError::InvalidKey(format!(
            "end-order key tail is too short: {} bytes",
            tail.len()
        )));
    }
    let table_id = TableId(u64::from_be_bytes(tail[0..8].try_into().map_err(|_| {
        crate::CatalogError::InvalidKey("end-order table id is truncated".to_owned())
    })?));
    let order_start = 9;
    let object_start = order_start + CatalogOrderId::LEN + 1;
    if tail[8] != b'/' || tail[order_start + CatalogOrderId::LEN] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "end-order key separators are invalid".to_owned(),
        ));
    }
    let object = &tail[object_start..];
    if object.len() == 8 {
        return Ok(Some(EndOrderEntry::DataFile {
            data_file_id: DataFileId(u64::from_be_bytes(object.try_into().map_err(|_| {
                crate::CatalogError::InvalidKey("end-order data file id is truncated".to_owned())
            })?)),
        }));
    }
    if object.len() == 10 && object[0] == b'x' && object[1] == b'/' {
        return Ok(Some(EndOrderEntry::DeleteFile {
            table_id,
            delete_file_id: DeleteFileId(u64::from_be_bytes(object[2..].try_into().map_err(
                |_| {
                    crate::CatalogError::InvalidKey(
                        "end-order delete file id is truncated".to_owned(),
                    )
                },
            )?)),
        }));
    }
    let inline_len = 2 + 8 + 1 + CatalogOrderId::LEN;
    if object.len() == inline_len && object[0] == b'i' && object[1] == b'/' {
        let begin_start = 2 + 8 + 1;
        if object[10] != b'/' {
            return Err(crate::CatalogError::InvalidKey(
                "end-order inline table separators are invalid".to_owned(),
            ));
        }
        let begin_order = CatalogOrderId::uuid_v7(u128::from_be_bytes(
            object[begin_start..].try_into().map_err(|_| {
                crate::CatalogError::InvalidKey("end-order inline begin is truncated".to_owned())
            })?,
        ));
        return Ok(Some(EndOrderEntry::InlineTable {
            table_id,
            schema_id: SchemaId(u64::from_be_bytes(object[2..10].try_into().map_err(
                |_| {
                    crate::CatalogError::InvalidKey(
                        "end-order inline schema is truncated".to_owned(),
                    )
                },
            )?)),
            begin_order,
            end_order: CatalogOrderId::from_bytes(
                CatalogOrderId::uuid_v7(0).kind(),
                tail[order_start..order_start + CatalogOrderId::LEN]
                    .try_into()
                    .map_err(|_| {
                        crate::CatalogError::InvalidKey(
                            "end-order inline marker order is truncated".to_owned(),
                        )
                    })?,
            ),
        }));
    }
    Err(crate::CatalogError::InvalidKey(format!(
        "end-order object tail has unsupported shape: {} bytes",
        object.len()
    )))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::{
        CatalogId, CatalogOrderId, CatalogResult, DataFileId, DataFileRow, FakeOrderedCatalogKv,
        FilePartitionValueRow, OrderedCatalogKv, PartitionKeyIndex, RangeDirection, RangeItem,
        TableId, commit_append_data_files, register_file_partition_value,
    };

    use super::data_file_partition_key;

    #[test]
    fn given_cleanup_partition_key_loaded_twice_then_second_load_uses_partition_cache() {
        let catalog = CatalogId(131);
        let table = TableId(7);
        let data_file_id = DataFileId(11);
        let mut inner = FakeOrderedCatalogKv::new();
        commit_append_data_files(
            &mut inner,
            catalog,
            vec![DataFileRow::new(
                data_file_id,
                table,
                "main/table/file.parquet",
                10,
                128,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap();
        register_file_partition_value(
            &mut inner,
            catalog,
            FilePartitionValueRow::new(data_file_id, table, PartitionKeyIndex(0), "eu"),
        )
        .unwrap();
        let kv = PartitionValueScanCountingKv::new(inner, catalog);

        let first = data_file_partition_key(&kv, catalog, data_file_id).unwrap();
        let second = data_file_partition_key(&kv, catalog, data_file_id).unwrap();

        assert_eq!(first, vec![(0, "eu".to_owned())]);
        assert_eq!(first, second);
        assert_eq!(kv.partition_value_scans(), 1);
    }

    struct PartitionValueScanCountingKv {
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        partition_value_scans: Cell<usize>,
    }

    impl PartitionValueScanCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                catalog,
                partition_value_scans: Cell::new(0),
            }
        }

        fn partition_value_scans(&self) -> usize {
            self.partition_value_scans.get()
        }
    }

    impl OrderedCatalogKv for PartitionValueScanCountingKv {
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            if prefix.starts_with(&crate::keys::family_prefix(
                self.catalog,
                crate::keys::KeyFamily::FilePartitionValue,
            )) {
                self.partition_value_scans
                    .set(self.partition_value_scans.get().saturating_add(1));
            }
            OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
