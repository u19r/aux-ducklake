use std::collections::{BTreeMap, BTreeSet};

use crate::conflict::write_data_file_change;
use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileRow,
    FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow, InlineTableFlush, KvBatch,
    MutableCatalogKv, TableId,
    conflict_watermarks::stage_max_file_id_watermark,
    data_file_store::reject_current_data_file_row_id_overlaps_except,
    data_file_store::{
        stage_append_data_file, stage_append_data_file_without_change, stage_expire_data_file,
        stage_register_delete_file, stage_register_delete_file_for_data_file,
    },
    data_mutation_intents::DeleteFileMaterialization,
    file_partitions::{remove_cached_file_partition_values, stage_file_partition_value},
    file_stats::{
        remove_cached_file_column_stats_for_data_file, stage_file_column_stats,
        stage_table_file_stats_versions_for_rows,
    },
    inline_change_feed::{InlineRowChangeKind, list_inline_row_changes},
    inline_data::{
        list_inline_file_deletion_rows_for_table_at, stage_flush_inline_table_payloads,
        stage_inline_file_deletion,
    },
    keys::{current_data_file_key, data_file_key},
    maintenance::stage_scheduled_data_file_cleanup,
    snapshot_by_ducklake_sequence,
    store::{
        invalidate_runtime_read_context, latest_snapshot, snapshot_row_for_next_sequence,
        stage_snapshot,
    },
};

#[derive(Debug, Default)]
pub struct DataMutationInput {
    pub data_files: Vec<DataFileRow>,
    pub delete_files: Vec<DeleteFileRow>,
    pub inline_flushes: Vec<InlineTableFlush>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub inline_file_deletions: Vec<InlineFileDeletionRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_file_ids: Vec<DataFileId>,
}

pub fn commit_data_mutation(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
    delete_files: Vec<DeleteFileRow>,
    inline_flushes: &[InlineTableFlush],
) -> CatalogResult<DataMutationCommit> {
    commit_data_mutation_with_file_partitions(
        kv,
        catalog,
        data_files,
        delete_files,
        inline_flushes,
        Vec::new(),
    )
}

pub fn commit_data_mutation_with_file_partitions(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
    delete_files: Vec<DeleteFileRow>,
    inline_flushes: &[InlineTableFlush],
    partition_values: Vec<FilePartitionValueRow>,
) -> CatalogResult<DataMutationCommit> {
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        kv,
        catalog,
        data_files,
        delete_files,
        inline_flushes,
        partition_values,
        Vec::new(),
    )
}

pub fn commit_data_mutation_with_file_partitions_and_inline_deletes(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    data_files: Vec<DataFileRow>,
    delete_files: Vec<DeleteFileRow>,
    inline_flushes: &[InlineTableFlush],
    partition_values: Vec<FilePartitionValueRow>,
    inline_file_deletions: Vec<InlineFileDeletionRow>,
) -> CatalogResult<DataMutationCommit> {
    commit_data_mutation_with_details(
        kv,
        catalog,
        DataMutationInput {
            data_files,
            delete_files,
            inline_flushes: inline_flushes.to_vec(),
            partition_values,
            inline_file_deletions,
            ..DataMutationInput::default()
        },
    )
}

pub fn commit_data_mutation_with_details(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    input: DataMutationInput,
) -> CatalogResult<DataMutationCommit> {
    let DataMutationInput {
        mut data_files,
        delete_files,
        inline_flushes,
        partition_values,
        mut inline_file_deletions,
        file_column_stats,
        dropped_data_file_ids,
    } = input;
    if data_files.is_empty()
        && delete_files.is_empty()
        && inline_flushes.is_empty()
        && partition_values.is_empty()
        && inline_file_deletions.is_empty()
        && file_column_stats.is_empty()
        && dropped_data_file_ids.is_empty()
    {
        return Ok(DataMutationCommit::default());
    }
    let mut delete_file_materializations = delete_files
        .into_iter()
        .map(DeleteFileMaterialization::historical_delete_file)
        .collect::<Vec<_>>();
    validate_partition_values(&data_files, &partition_values)?;
    validate_file_column_stats(&data_files, &file_column_stats)?;
    if inline_flushes.is_empty() {
        let overlappable_current_files = delete_file_materializations
            .iter()
            .map(DeleteFileMaterialization::data_file_id)
            .collect::<BTreeSet<_>>();
        reject_current_data_file_row_id_overlaps_except(
            kv,
            catalog,
            &data_files,
            &overlappable_current_files,
        )?;
    }

    let latest = latest_snapshot(kv, catalog)?;
    let order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest.clone(), order);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);

    for row in &mut data_files {
        let stores_existing_rows = row.max_partial_order.is_some();
        if stores_existing_rows {
            row.validity.end_order = None;
        } else {
            row.validity = crate::ValidityWindow::new(order, None);
        }
        if stores_existing_rows {
            stage_append_data_file_without_change(kv, &mut batch, catalog, row)?;
            write_data_file_change(
                &mut batch,
                catalog,
                row.table_id,
                order,
                crate::DataFileChangeKind::Added,
                row.data_file_id,
            );
        } else {
            stage_append_data_file(kv, &mut batch, catalog, row)?;
        }
    }
    for row in &partition_values {
        let data_file = data_files
            .iter()
            .find(|data_file| data_file.data_file_id == row.data_file_id)
            .ok_or_else(|| {
                crate::CatalogError::InvalidMutation(format!(
                    "partition value references unknown data file {}",
                    row.data_file_id.0
                ))
            })?;
        stage_file_partition_value(&mut batch, catalog, row, data_file);
    }
    for row in &file_column_stats {
        stage_file_column_stats(&mut batch, catalog, row);
    }
    stage_table_file_stats_versions_for_rows(&mut batch, catalog, &file_column_stats, order);
    for materialization in &mut delete_file_materializations {
        if materialization.row().validity.begin_order == crate::CatalogOrderId::uuid_v7(0) {
            materialization.row_mut().validity = crate::ValidityWindow::new(order, None);
        } else {
            materialization.row_mut().validity.end_order = None;
        }
        if materialization.row().validity.begin_order == order {
            let first_inline_row_delete = first_inline_row_delete_for_materialized_file(
                kv,
                catalog,
                &data_files,
                &inline_flushes,
                materialization.row(),
                latest.as_ref(),
            )?;
            if let Some(first_inline_delete) = first_inline_row_delete {
                materialization.row_mut().validity.begin_order = first_inline_delete;
            }
        }
        if let Some(data_file) = data_files
            .iter()
            .find(|data_file| data_file.data_file_id == materialization.data_file_id())
        {
            stage_register_delete_file_for_data_file(
                kv,
                &mut batch,
                catalog,
                data_file,
                materialization.row_mut(),
                order,
            )?;
        } else {
            stage_register_delete_file(kv, &mut batch, catalog, materialization.row_mut(), order)?;
        }
    }
    stage_close_materialized_inline_file_deletions(
        kv,
        &mut batch,
        catalog,
        &data_files,
        &delete_file_materializations,
        latest.as_ref(),
        order,
    )?;
    for flush in &inline_flushes {
        stage_flush_inline_table_payloads(kv, &mut batch, catalog, *flush, order)?;
    }
    for row in &mut inline_file_deletions {
        row.validity = crate::ValidityWindow::new(order, None);
        stage_inline_file_deletion(&mut batch, catalog, row);
    }
    if !inline_file_deletions.is_empty() {
        stage_max_file_id_watermark(kv, &mut batch, catalog, snapshot.sequence.0)?;
    }
    let dropped_data_files = load_current_dropped_data_files(kv, catalog, &dropped_data_file_ids)?;
    let dropped_data_file_count = dropped_data_files.len();
    for mut row in dropped_data_files {
        row.validity.end_order = Some(order);
        stage_expire_data_file(kv, &mut batch, catalog, &row, order)?;
        stage_scheduled_data_file_cleanup(&mut batch, catalog, row.data_file_id);
    }

    kv.commit(batch)?;
    for row in &partition_values {
        remove_cached_file_partition_values(kv, catalog, row.data_file_id);
    }
    let mut stats_file_ids = BTreeSet::new();
    for row in &file_column_stats {
        if stats_file_ids.insert(row.data_file_id) {
            remove_cached_file_column_stats_for_data_file(kv, catalog, row.data_file_id);
        }
    }
    invalidate_runtime_read_context(catalog);
    Ok(DataMutationCommit {
        data_files,
        delete_files: DeleteFileMaterialization::rows(&delete_file_materializations),
        partition_value_count: partition_values.len(),
        file_column_stats_count: file_column_stats.len(),
        flushed_inline_count: inline_flushes.len(),
        inline_file_deletion_count: inline_file_deletions.len(),
        dropped_data_file_count,
    })
}

fn first_inline_row_delete_for_materialized_file(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    appended_data_files: &[DataFileRow],
    inline_flushes: &[InlineTableFlush],
    delete_file: &DeleteFileRow,
    latest: Option<&crate::SnapshotRow>,
) -> CatalogResult<Option<crate::CatalogOrderId>> {
    let Some(latest) = latest else {
        return Ok(None);
    };
    let data_file = appended_or_committed_data_file(kv, catalog, appended_data_files, delete_file)?;
    let row_id_end = data_file
        .row_id_start
        .saturating_add(data_file.record_count);
    let mut first_delete: Option<crate::CatalogOrderId> = None;
    for row in
        list_inline_file_deletion_rows_for_table_at(kv, catalog, data_file.table_id, latest.order)?
    {
        if row.data_file_id != delete_file.data_file_id {
            continue;
        }
        first_delete = Some(first_delete.map_or(row.validity.begin_order, |order| {
            order.min(row.validity.begin_order)
        }));
    }
    for flush in inline_flushes {
        if flush.table_id != data_file.table_id {
            continue;
        }
        let Some(flush_snapshot) = snapshot_by_ducklake_sequence(
            kv,
            catalog,
            crate::DuckLakeSnapshotId(flush.flush_snapshot_sequence.0),
        )?
        else {
            continue;
        };
        for change in list_inline_row_changes(
            kv,
            catalog,
            flush.table_id,
            flush_snapshot.order,
            latest.order,
        )? {
            if change.schema_id != flush.schema_id
                || change.kind != InlineRowChangeKind::Deleted
                || change.row_id < data_file.row_id_start
                || change.row_id >= row_id_end
            {
                continue;
            }
            first_delete = Some(first_delete.map_or(change.order, |order| order.min(change.order)));
        }
    }
    Ok(first_delete)
}

fn stage_close_materialized_inline_file_deletions(
    kv: &impl crate::OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    appended_data_files: &[DataFileRow],
    delete_files: &[DeleteFileMaterialization],
    latest: Option<&crate::SnapshotRow>,
    end_order: crate::CatalogOrderId,
) -> CatalogResult<()> {
    let Some(latest) = latest else {
        return Ok(());
    };
    let mut materialized_data_files_by_table = BTreeMap::<TableId, BTreeSet<DataFileId>>::new();
    for materialization in delete_files {
        let data_file = appended_or_committed_data_file(
            kv,
            catalog,
            appended_data_files,
            materialization.row(),
        )?;
        materialized_data_files_by_table
            .entry(data_file.table_id)
            .or_default()
            .insert(data_file.data_file_id);
    }
    for (table_id, data_file_ids) in materialized_data_files_by_table {
        for mut row in
            list_inline_file_deletion_rows_for_table_at(kv, catalog, table_id, latest.order)?
        {
            if !data_file_ids.contains(&row.data_file_id) {
                continue;
            }
            row.validity.end_order = Some(end_order);
            batch.put(
                crate::keys::inline_file_deletion_key(
                    catalog,
                    row.table_id,
                    row.data_file_id,
                    row.validity.begin_order,
                    row.row_id,
                ),
                row.encode(),
            );
        }
    }
    Ok(())
}

fn appended_or_committed_data_file(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    appended_data_files: &[DataFileRow],
    delete_file: &DeleteFileRow,
) -> CatalogResult<DataFileRow> {
    if let Some(data_file) = appended_data_files
        .iter()
        .find(|data_file| data_file.data_file_id == delete_file.data_file_id)
    {
        return Ok(data_file.clone());
    }
    let Some(value) = kv.get(&crate::keys::data_file_key(
        catalog,
        delete_file.data_file_id,
    ))?
    else {
        return Err(CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

fn load_current_dropped_data_files(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    match data_file_ids {
        [] => return Ok(Vec::new()),
        [data_file_id] => {
            return load_current_dropped_data_file(kv, catalog, *data_file_id).map(|row| vec![row]);
        }
        _ => {}
    }
    let data_file_keys = data_file_ids
        .iter()
        .map(|data_file_id| data_file_key(catalog, *data_file_id))
        .collect::<Vec<_>>();
    let mut rows = Vec::with_capacity(data_file_ids.len());
    for (data_file_id, value) in data_file_ids
        .iter()
        .copied()
        .zip(kv.batch_get(&data_file_keys)?)
    {
        let Some(value) = value else {
            return Err(CatalogError::NotFound("data file"));
        };
        let row = DataFileRow::decode(&value)?;
        reject_loaded_data_file_id(data_file_id, row.data_file_id)?;
        reject_dropped_data_file_row_is_open(row.data_file_id, row.validity.end_order)?;
        rows.push(row);
    }

    let current_keys = rows
        .iter()
        .map(|row| current_data_file_key(catalog, row.table_id, row.data_file_id))
        .collect::<Vec<_>>();
    for (row, current) in rows.iter().zip(kv.batch_get(&current_keys)?) {
        if current.is_none() {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} is not current",
                row.data_file_id.0
            )));
        }
    }
    Ok(rows)
}

fn load_current_dropped_data_file(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(CatalogError::NotFound("data file"));
    };
    let row = DataFileRow::decode(&value)?;
    reject_loaded_data_file_id(data_file_id, row.data_file_id)?;
    reject_dropped_data_file_row_is_open(row.data_file_id, row.validity.end_order)?;
    if kv
        .get(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        ))?
        .is_none()
    {
        return Err(CatalogError::InvalidMutation(format!(
            "data file {} is not current",
            row.data_file_id.0
        )));
    }
    Ok(row)
}

fn reject_loaded_data_file_id(expected: DataFileId, actual: DataFileId) -> CatalogResult<()> {
    if actual != expected {
        return Err(CatalogError::Decode(format!(
            "data file key {} returned row {}",
            expected.0, actual.0
        )));
    }
    Ok(())
}

fn reject_dropped_data_file_row_is_open(
    data_file_id: DataFileId,
    end_order: Option<crate::CatalogOrderId>,
) -> CatalogResult<()> {
    if end_order.is_some() {
        return Err(CatalogError::InvalidMutation(format!(
            "data file {} is already closed",
            data_file_id.0
        )));
    }
    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DataMutationCommit {
    pub data_files: Vec<DataFileRow>,
    pub delete_files: Vec<DeleteFileRow>,
    pub partition_value_count: usize,
    pub file_column_stats_count: usize,
    pub flushed_inline_count: usize,
    pub inline_file_deletion_count: usize,
    pub dropped_data_file_count: usize,
}

fn validate_file_column_stats(
    data_files: &[DataFileRow],
    file_column_stats: &[FileColumnStatsRow],
) -> CatalogResult<()> {
    for row in file_column_stats {
        let table_id = appended_table_id(data_files, row.data_file_id).ok_or(
            CatalogError::NotFound("appended data file for file column stats"),
        )?;
        if table_id != row.table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "file column stats table {} does not match appended data file table {}",
                row.table_id.0, table_id.0
            )));
        }
    }
    Ok(())
}

fn validate_partition_values(
    data_files: &[DataFileRow],
    partition_values: &[FilePartitionValueRow],
) -> CatalogResult<()> {
    for row in partition_values {
        let table_id = appended_table_id(data_files, row.data_file_id).ok_or(
            CatalogError::NotFound("appended data file for partition value"),
        )?;
        if table_id != row.table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "partition table {} does not match appended data file table {}",
                row.table_id.0, table_id.0
            )));
        }
    }
    Ok(())
}

fn appended_table_id(data_files: &[DataFileRow], data_file_id: DataFileId) -> Option<TableId> {
    data_files
        .iter()
        .find(|row| row.data_file_id == data_file_id)
        .map(|row| row.table_id)
}
