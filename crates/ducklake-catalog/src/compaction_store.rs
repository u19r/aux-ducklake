use std::collections::BTreeSet;

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DataFileChangeKind, DataFileId,
    DataFileRow, DeleteFileId, FileColumnStatsRow, FilePartitionValueRow, KvBatch,
    MutableCatalogKv, OrderedCatalogKv, TableId, ValidityWindow,
    conflict::{
        list_data_file_changes_since_base, table_drop_conflict_since_base,
        table_schema_conflict_since_base, write_data_file_change,
    },
    data_file_store::{
        reject_current_data_file_row_id_overlaps_except, stage_append_data_file,
        stage_append_data_file_without_change,
    },
    delete_change_feed::list_table_deletion_scan_files,
    file_partitions::{remove_cached_file_partition_values, stage_file_partition_value},
    file_stats::{
        list_file_column_stats_for_data_file_ids, remove_cached_file_column_stats_for_data_file,
        stage_file_column_stats, stage_table_file_stats_versions_for_rows,
    },
    file_visibility::{
        current_delete_file_id, stage_expire_current_data_file, stage_expire_current_delete_file,
    },
    keys::data_file_key,
    list_inline_file_deletions_at,
    maintenance::{
        stage_scheduled_compacted_data_file_cleanup, stage_scheduled_delete_file_cleanup,
    },
    snapshot_operations::{SnapshotOperationKind, stage_snapshot_operation},
    store::{latest_snapshot, snapshot_row_for_next_sequence, stage_snapshot},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeAdjacentCompaction {
    pub source_file_ids: Vec<DataFileId>,
    pub new_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteDeleteCompaction {
    pub source_file_ids: Vec<DataFileId>,
    pub new_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
}

pub fn commit_merge_adjacent_data_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut compaction: MergeAdjacentCompaction,
) -> CatalogResult<MergeAdjacentCompaction> {
    if compaction.source_file_ids.is_empty() {
        return Err(CatalogError::InvalidMutation(
            "merge-adjacent compaction requires source data files".to_owned(),
        ));
    }
    let latest = latest_snapshot(kv, catalog)?;
    reject_empty_replacement_for_non_empty_sources(kv, catalog, &compaction)?;
    if let Some(snapshot) = latest.as_ref() {
        reject_active_inline_deletions_for_merge_sources(
            kv,
            catalog,
            &compaction.source_file_ids,
            snapshot.order,
        )?;
    }
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);

    let mut expired_sources = Vec::new();
    for source_file_id in &compaction.source_file_ids {
        expired_sources.push(expire_compacted_source_file(
            kv,
            &mut batch,
            catalog,
            *source_file_id,
            order,
            DeleteFilePolicy::Reject,
        )?);
    }
    derive_merge_replacement_row_ids(&mut compaction.new_files, &expired_sources)?;
    apply_merge_replacement_visibility(&mut compaction.new_files, &expired_sources, order);
    for new_file in &mut compaction.new_files {
        new_file.validity.end_order = None;
        stage_merge_replacement_data_file(kv, &mut batch, catalog, new_file, order)?;
    }
    stage_new_file_partition_values(
        &mut batch,
        catalog,
        &compaction.partition_values,
        &compaction.new_files,
    )?;
    let file_column_stats = merge_adjacent_file_column_stats(
        kv,
        catalog,
        &compaction.source_file_ids,
        &compaction.new_files,
        &compaction.file_column_stats,
    )?;
    for row in &file_column_stats {
        stage_file_column_stats(&mut batch, catalog, &row);
    }
    stage_table_file_stats_versions_for_rows(&mut batch, catalog, &file_column_stats, order);

    kv.commit(batch)?;
    for row in &compaction.partition_values {
        remove_cached_file_partition_values(kv, catalog, row.data_file_id);
    }
    for row in &file_column_stats {
        remove_cached_file_column_stats_for_data_file(kv, catalog, row.data_file_id);
    }
    Ok(compaction)
}

fn stage_merge_replacement_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &DataFileRow,
    change_order: CatalogOrderId,
) -> CatalogResult<()> {
    stage_append_data_file_without_change(kv, batch, catalog, row)?;
    write_data_file_change(
        batch,
        catalog,
        row.table_id,
        change_order,
        DataFileChangeKind::Added,
        row.data_file_id,
    );
    Ok(())
}

pub fn commit_merge_adjacent_data_files_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    compaction: MergeAdjacentCompaction,
) -> CatalogResult<MergeAdjacentCompaction> {
    let table_id = compaction_table_id(
        kv,
        catalog,
        &compaction.source_file_ids,
        &compaction.new_files,
    )?;
    reject_merge_source_conflicts_since_base(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        &compaction.source_file_ids,
    )?;
    commit_merge_adjacent_data_files(kv, catalog, compaction)
}

pub fn commit_rewrite_delete_data_files(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut compaction: RewriteDeleteCompaction,
) -> CatalogResult<RewriteDeleteCompaction> {
    if compaction.source_file_ids.is_empty() {
        return Err(CatalogError::InvalidMutation(
            "rewrite-delete compaction requires source data files".to_owned(),
        ));
    }

    let latest = latest_snapshot(kv, catalog)?;
    let table_id = compaction_table_id(
        kv,
        catalog,
        &compaction.source_file_ids,
        &compaction.new_files,
    )?;
    normalize_rewrite_replacement_row_ids(
        kv,
        catalog,
        &compaction.source_file_ids,
        &mut compaction.new_files,
    )?;
    let inline_deletions = latest
        .as_ref()
        .map(|snapshot| list_inline_file_deletions_at(kv, catalog, table_id, snapshot.order))
        .transpose()?
        .unwrap_or_default();
    let allowed_current_file_ids = compaction.source_file_ids.iter().copied().collect();
    reject_current_data_file_row_id_overlaps_except(
        kv,
        catalog,
        &compaction.new_files,
        &allowed_current_file_ids,
    )?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_snapshot_operation(
        &mut batch,
        catalog,
        order,
        SnapshotOperationKind::RewriteDelete,
        table_id,
    );

    for source_file_id in &compaction.source_file_ids {
        let expired_source = expire_compacted_source_file(
            kv,
            &mut batch,
            catalog,
            *source_file_id,
            order,
            DeleteFilePolicy::Allow,
        )?;
        if expired_source.delete_file_id.is_none()
            && inline_deletions
                .get(source_file_id)
                .is_none_or(|rows| rows.is_empty())
        {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} has no delete file or inline deletions to rewrite",
                source_file_id.0
            )));
        }
        if let Some(delete_file_id) = expired_source.delete_file_id {
            expire_compacted_delete_file(
                kv,
                &mut batch,
                catalog,
                &expired_source.data_file,
                delete_file_id,
                order,
            )?;
        }
    }
    for new_file in &mut compaction.new_files {
        new_file.validity = ValidityWindow::new(order, None);
        stage_append_data_file(kv, &mut batch, catalog, new_file)?;
    }
    let file_column_stats = rewrite_delete_file_column_stats(
        kv,
        catalog,
        &compaction.source_file_ids,
        &compaction.new_files,
        &compaction.file_column_stats,
    )?;
    for row in &file_column_stats {
        stage_file_column_stats(&mut batch, catalog, &row);
    }
    stage_table_file_stats_versions_for_rows(&mut batch, catalog, &file_column_stats, order);
    stage_new_file_partition_values(
        &mut batch,
        catalog,
        &compaction.partition_values,
        &compaction.new_files,
    )?;

    kv.commit(batch)?;
    for row in &compaction.partition_values {
        remove_cached_file_partition_values(kv, catalog, row.data_file_id);
    }
    for row in &file_column_stats {
        remove_cached_file_column_stats_for_data_file(kv, catalog, row.data_file_id);
    }
    Ok(compaction)
}

pub fn commit_rewrite_delete_data_files_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    compaction: RewriteDeleteCompaction,
) -> CatalogResult<RewriteDeleteCompaction> {
    let table_id = compaction_table_id(
        kv,
        catalog,
        &compaction.source_file_ids,
        &compaction.new_files,
    )?;
    reject_rewrite_source_conflicts_since_base(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        &compaction.source_file_ids,
    )?;
    commit_rewrite_delete_data_files(kv, catalog, compaction)
}

#[derive(Debug, Clone, Copy)]
enum DeleteFilePolicy {
    Reject,
    Allow,
}

struct ExpiredSourceFile {
    data_file: DataFileRow,
    delete_file_id: Option<DeleteFileId>,
}

fn expire_compacted_source_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
    end_order: CatalogOrderId,
    delete_policy: DeleteFilePolicy,
) -> CatalogResult<ExpiredSourceFile> {
    let delete_file_id = current_delete_file_id(kv, catalog, data_file_id)?;
    match (delete_policy, delete_file_id) {
        (DeleteFilePolicy::Reject, Some(_)) => {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} has delete files and cannot be merge-adjacent compacted",
                data_file_id.0
            )));
        }
        _ => {}
    }
    let data_file = stage_expire_current_data_file(kv, batch, catalog, data_file_id, end_order)?;
    stage_scheduled_compacted_data_file_cleanup(batch, catalog, data_file_id);
    Ok(ExpiredSourceFile {
        data_file,
        delete_file_id,
    })
}

fn expire_compacted_delete_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file: &DataFileRow,
    delete_file_id: DeleteFileId,
    end_order: CatalogOrderId,
) -> CatalogResult<()> {
    stage_expire_current_delete_file(kv, batch, catalog, data_file, delete_file_id, end_order)?;
    stage_scheduled_delete_file_cleanup(batch, catalog, data_file.table_id, delete_file_id);
    Ok(())
}

fn reject_empty_replacement_for_non_empty_sources(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    compaction: &MergeAdjacentCompaction,
) -> CatalogResult<()> {
    if !compaction.new_files.is_empty() {
        return Ok(());
    }
    for source_file_id in &compaction.source_file_ids {
        if load_data_file(kv, catalog, *source_file_id)?.record_count != 0 {
            return Err(CatalogError::InvalidMutation(
                "merge-adjacent compaction requires a replacement data file for non-empty sources"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn normalize_rewrite_replacement_row_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    new_files: &mut [DataFileRow],
) -> CatalogResult<()> {
    let sources = load_source_files(kv, catalog, source_file_ids)?;
    let source_start = min_known_source_row_id_start(&sources)?;
    derive_unknown_rewrite_replacement_row_ids(&sources, new_files)?;
    for new_file in new_files {
        if new_file.row_id_start_known && new_file.row_id_start < source_start {
            new_file.row_id_start = source_start.saturating_add(new_file.row_id_start);
        }
    }
    Ok(())
}

fn min_known_source_row_id_start(sources: &[DataFileRow]) -> CatalogResult<u64> {
    sources
        .iter()
        .filter(|source| source.row_id_start_known)
        .map(|source| source.row_id_start)
        .min()
        .ok_or_else(|| {
            CatalogError::InvalidMutation(
                "rewrite-delete compaction requires source row id metadata".to_owned(),
            )
        })
}

fn load_source_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    source_file_ids
        .iter()
        .map(|source_file_id| load_data_file(kv, catalog, *source_file_id))
        .collect::<CatalogResult<Vec<_>>>()
}

fn derive_unknown_rewrite_replacement_row_ids(
    sources: &[DataFileRow],
    new_files: &mut [DataFileRow],
) -> CatalogResult<()> {
    let unknown_record_count: u64 = new_files
        .iter()
        .filter(|file| !file.row_id_start_known)
        .map(|file| file.record_count)
        .sum();
    if unknown_record_count == 0 {
        return Ok(());
    }
    let source_end = max_known_source_row_id_end(sources)?;
    let mut next_start = source_end.saturating_sub(unknown_record_count);
    for new_file in new_files.iter_mut().filter(|file| !file.row_id_start_known) {
        new_file.row_id_start = next_start;
        new_file.row_id_start_known = true;
        next_start = next_start.saturating_add(new_file.record_count);
    }
    Ok(())
}

fn max_known_source_row_id_end(sources: &[DataFileRow]) -> CatalogResult<u64> {
    sources
        .iter()
        .filter(|source| source.row_id_start_known)
        .map(|source| source.row_id_start.saturating_add(source.record_count))
        .max()
        .ok_or_else(|| {
            CatalogError::InvalidMutation(
                "rewrite-delete compaction requires source row id metadata".to_owned(),
            )
        })
}

fn derive_single_source_rewrite_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    new_files: &[DataFileRow],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let ([source_id], [new_file]) = (source_file_ids, new_files) else {
        return Ok(Vec::new());
    };
    let rows = list_file_column_stats_for_data_file_ids(kv, catalog, source_file_ids)?
        .into_iter()
        .filter(|row| row.data_file_id == *source_id)
        .map(|mut row| {
            row.data_file_id = new_file.data_file_id;
            row.table_id = new_file.table_id;
            row.value_count = Some(new_file.record_count);
            row
        })
        .collect();
    Ok(rows)
}

pub(crate) fn rewrite_delete_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    new_files: &[DataFileRow],
    provided_stats: &[FileColumnStatsRow],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if !provided_stats.is_empty() {
        return Ok(provided_stats.to_vec());
    }
    derive_single_source_rewrite_stats(kv, catalog, source_file_ids, new_files)
}

pub(crate) fn merge_adjacent_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    new_files: &[DataFileRow],
    provided_stats: &[FileColumnStatsRow],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if !provided_stats.is_empty() {
        return Ok(provided_stats.to_vec());
    }
    derive_single_source_rewrite_stats(kv, catalog, source_file_ids, new_files)
}

fn reject_active_inline_deletions_for_merge_sources(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<()> {
    for source_file_id in source_file_ids {
        let source = load_data_file(kv, catalog, *source_file_id)?;
        if list_inline_file_deletions_at(kv, catalog, source.table_id, snapshot_order)?
            .get(source_file_id)
            .is_some_and(|row_ids| !row_ids.is_empty())
        {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} has active inline deletions and cannot be merge-adjacent compacted",
                source_file_id.0
            )));
        }
    }
    Ok(())
}

fn reject_merge_source_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<()> {
    if let Some(dropped_at) =
        table_drop_conflict_since_base(kv, catalog, table_id, base_order, through_order)?
    {
        return Err(CatalogError::TableLogicalConflict {
            table_id,
            dropped_at,
        });
    }

    let source_file_ids: BTreeSet<DataFileId> = source_file_ids.iter().copied().collect();
    let conflicting_changes: Vec<_> =
        list_data_file_changes_since_base(kv, catalog, base_order, through_order)?
            .into_iter()
            .filter(|change| {
                change.table_id == table_id
                    && change.kind == DataFileChangeKind::Removed
                    && source_file_ids.contains(&change.data_file_id)
            })
            .collect();
    if !conflicting_changes.is_empty() {
        return Err(CatalogError::LogicalConflict {
            table_id,
            conflicting_changes,
        });
    }

    if let Some(changed_at) =
        table_schema_conflict_since_base(kv, catalog, table_id, base_order, through_order)?
    {
        return Err(CatalogError::TableSchemaConflict {
            table_id,
            changed_at,
        });
    }
    Ok(())
}

fn reject_rewrite_source_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<()> {
    reject_merge_source_conflicts_since_base(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        source_file_ids,
    )?;
    let source_file_ids: BTreeSet<DataFileId> = source_file_ids.iter().copied().collect();
    let conflicting_changes =
        list_table_deletion_scan_files(kv, catalog, table_id, base_order, through_order)?
            .into_iter()
            .filter(|scan| {
                scan.snapshot_order > base_order
                    && source_file_ids.contains(&scan.data_file.data_file_id)
            })
            .map(|scan| crate::DataFileChange {
                table_id,
                order: scan.snapshot_order,
                kind: DataFileChangeKind::Removed,
                data_file_id: scan.data_file.data_file_id,
            })
            .collect::<Vec<_>>();
    if !conflicting_changes.is_empty() {
        return Err(CatalogError::LogicalConflict {
            table_id,
            conflicting_changes,
        });
    }
    Ok(())
}

fn compaction_table_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
    new_files: &[DataFileRow],
) -> CatalogResult<TableId> {
    let Some(first_source_id) = source_file_ids.first() else {
        return Err(CatalogError::InvalidMutation(
            "compaction conflict check requires source data files".to_owned(),
        ));
    };
    let table_id = load_data_file(kv, catalog, *first_source_id)?.table_id;
    for source_file_id in source_file_ids.iter().skip(1) {
        let source_table_id = load_data_file(kv, catalog, *source_file_id)?.table_id;
        if source_table_id != table_id {
            return Err(CatalogError::InvalidMutation(
                "compaction source files must belong to one table".to_owned(),
            ));
        }
    }
    for new_file in new_files {
        if new_file.table_id != table_id {
            return Err(CatalogError::InvalidMutation(
                "compaction replacement files must belong to the source table".to_owned(),
            ));
        }
    }
    Ok(table_id)
}

fn apply_merge_replacement_visibility(
    new_files: &mut [DataFileRow],
    expired_sources: &[ExpiredSourceFile],
    merge_order: CatalogOrderId,
) {
    let Some(first_source) = expired_sources.first() else {
        return;
    };
    let first_begin = first_source.data_file.validity.begin_order;
    if expired_sources
        .iter()
        .all(|source| source.data_file.validity.begin_order == first_begin)
    {
        for new_file in new_files {
            if has_explicit_merge_visibility(new_file) {
                continue;
            }
            new_file.validity.begin_order = merge_order;
            new_file.max_partial_order = None;
        }
        return;
    }
    let max_partial = expired_sources
        .iter()
        .map(|source| {
            source
                .data_file
                .max_partial_order
                .unwrap_or(source.data_file.validity.begin_order)
        })
        .max();
    for new_file in new_files {
        if has_explicit_merge_visibility(new_file) {
            continue;
        }
        new_file.validity.begin_order = first_begin;
        new_file.max_partial_order = if expired_sources.len() > 1 {
            max_partial
        } else {
            None
        };
    }
}

fn derive_merge_replacement_row_ids(
    new_files: &mut [DataFileRow],
    expired_sources: &[ExpiredSourceFile],
) -> CatalogResult<()> {
    if new_files.iter().all(|file| file.row_id_start_known) {
        return Ok(());
    }
    if new_files.len() != 1 {
        return Err(CatalogError::InvalidMutation(
            "merge-adjacent compaction replacements require row id metadata".to_owned(),
        ));
    }
    let row_id_start = min_known_source_row_id_start_from_expired(expired_sources)?;
    let replacement = &mut new_files[0];
    replacement.row_id_start = row_id_start;
    replacement.row_id_start_known = true;
    Ok(())
}

fn min_known_source_row_id_start_from_expired(
    expired_sources: &[ExpiredSourceFile],
) -> CatalogResult<u64> {
    expired_sources
        .iter()
        .filter(|source| source.data_file.row_id_start_known)
        .map(|source| source.data_file.row_id_start)
        .min()
        .ok_or_else(|| {
            CatalogError::InvalidMutation(
                "merge-adjacent compaction requires source row id metadata".to_owned(),
            )
        })
}

fn has_explicit_merge_visibility(file: &DataFileRow) -> bool {
    file.max_partial_order.is_some() || file.validity.begin_order != CatalogOrderId::uuid_v7(0)
}

fn stage_new_file_partition_values(
    batch: &mut KvBatch,
    catalog: CatalogId,
    partition_values: &[FilePartitionValueRow],
    new_files: &[DataFileRow],
) -> CatalogResult<()> {
    for row in partition_values {
        let Some(data_file) = new_files
            .iter()
            .find(|data_file| data_file.data_file_id == row.data_file_id)
        else {
            return Err(CatalogError::InvalidMutation(format!(
                "partition value references unknown data file {}",
                row.data_file_id.0
            )));
        };
        stage_file_partition_value(batch, catalog, row, data_file);
    }
    Ok(())
}

fn load_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

#[cfg(test)]
#[path = "compaction_store_tests.rs"]
mod compaction_store_tests;
