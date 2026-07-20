use std::collections::{BTreeMap, BTreeSet};

use foundationdb::options::MutationType;

use crate::{
    CatalogError, CatalogOrderId, CatalogResult, CommitAttemptId, DataFileId, DataFileRow,
    DeleteFileRow, FdbOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow,
    InlineFileDeletionRow, SnapshotRow, TableId, ValidityWindow,
    conflict::commit_attempt_key,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_inline_flushes::{PreparedInlineFlush, estimate_prepared_inline_flush_bytes},
    fdb_versionstamp::{incomplete_order, versionstamped_value},
    file_partitions::encode_partition_lookup_value,
    inline_data::list_inline_file_deletion_rows_for_data_files_at,
    keys::{
        current_data_file_key, current_delete_file_key, data_file_key, delete_file_key,
        file_column_stats_key, file_column_stats_lookup_key, file_partition_value_key,
        inline_file_deletion_key, partition_value_lookup_key, snapshot_key, snapshot_timestamp_key,
    },
    kv::OrderedCatalogKv,
    snapshot_operations::SnapshotOperationKind,
};

use crate::fdb_data_mutations::*;
pub(super) struct MutationSizeEstimate<'a> {
    pub(super) catalog: crate::CatalogId,
    pub(super) snapshot: &'a SnapshotRow,
    pub(super) attempt_id: Option<CommitAttemptId>,
    pub(super) data_files: &'a [DataFileRow],
    pub(super) delete_files: &'a [DeleteFileMaterialization],
    pub(super) inline_flushes: &'a [PreparedInlineFlush],
    pub(super) partition_values: &'a [FilePartitionValueRow],
    pub(super) inline_file_deletions: &'a [InlineFileDeletionRow],
    pub(super) file_column_stats: &'a [FileColumnStatsRow],
    pub(super) dropped_data_file_ids: &'a [DataFileId],
    pub(super) expired_delete_files: &'a [FdbExpiredDeleteFile],
    pub(super) expired_inline_file_deletions: &'a [InlineFileDeletionRow],
    pub(super) snapshot_operations: &'a [(SnapshotOperationKind, crate::TableId)],
}

pub(super) fn reject_oversized_mutation(input: MutationSizeEstimate<'_>) -> CatalogResult<()> {
    let estimated_bytes = estimate_mutation_metadata_bytes(
        input.catalog,
        input.snapshot,
        input.attempt_id,
        input.data_files,
        input
            .delete_files
            .iter()
            .map(DeleteFileMaterialization::row),
        input.partition_values,
        input.file_column_stats,
    )
    .saturating_add(estimate_prepared_inline_flush_bytes(
        input.catalog,
        input.inline_flushes,
    ))
    .saturating_add(estimate_inline_file_deletion_bytes(
        input.catalog,
        input.inline_file_deletions,
    ))
    .saturating_add(input.dropped_data_file_ids.len().saturating_mul(256))
    .saturating_add(input.expired_delete_files.len().saturating_mul(256))
    .saturating_add(estimate_inline_file_deletion_bytes(
        input.catalog,
        input.expired_inline_file_deletions,
    ))
    .saturating_add(input.snapshot_operations.len().saturating_mul(64));
    reject_estimated_mutation(estimated_bytes)
}

pub(super) fn prepare_delete_file_for_versionstamped_commit(
    row: &mut DeleteFileRow,
    placeholder: CatalogOrderId,
) {
    if row.validity.begin_order == CatalogOrderId::uuid_v7(0) {
        row.validity = ValidityWindow::new(placeholder, None);
        return;
    }
    row.validity.end_order = None;
}

pub(super) fn complete_materialized_delete_file_visibility(
    delete_files: &mut [DeleteFileMaterialization],
    inline_file_deletions: &[InlineFileDeletionRow],
) {
    let mut visibility_by_data_file =
        BTreeMap::<DataFileId, (CatalogOrderId, CatalogOrderId)>::new();
    for row in inline_file_deletions {
        visibility_by_data_file
            .entry(row.data_file_id)
            .and_modify(|(min_order, max_order)| {
                *min_order = (*min_order).min(row.validity.begin_order);
                *max_order = (*max_order).max(row.validity.begin_order);
            })
            .or_insert((row.validity.begin_order, row.validity.begin_order));
    }
    for materialization in delete_files {
        if !materialization.materializes_inline_deletes() {
            continue;
        }
        let Some((begin_order, max_partial_order)) = visibility_by_data_file
            .get(&materialization.data_file_id())
            .copied()
        else {
            continue;
        };
        let row = materialization.row_mut();
        if row.validity.begin_order == incomplete_order() {
            row.validity.begin_order = begin_order;
        }
        if row.max_partial_order.is_none() || row.max_partial_order == Some(incomplete_order()) {
            row.max_partial_order = Some(max_partial_order);
        }
    }
}

pub(super) fn materialized_inline_file_deletions(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    data_file_context: &MutationDataFileContext,
    delete_files: &[DeleteFileMaterialization],
    latest: Option<&SnapshotRow>,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let Some(latest) = latest else {
        return Ok(Vec::new());
    };
    let mut materialized_data_files_by_table = BTreeMap::<TableId, BTreeSet<DataFileId>>::new();
    for materialization in delete_files {
        if !materialization.materializes_inline_deletes() {
            continue;
        }
        let data_file = data_file_context.get(materialization.data_file_id())?;
        materialized_data_files_by_table
            .entry(data_file.table_id)
            .or_default()
            .insert(data_file.data_file_id);
    }
    let mut rows = Vec::new();
    for (table_id, data_file_ids) in materialized_data_files_by_table {
        rows.extend(list_inline_file_deletion_rows_for_data_files_at(
            kv,
            catalog,
            table_id,
            latest.order,
            &data_file_ids,
        )?);
    }
    Ok(rows)
}

pub(super) fn stage_materialized_inline_file_deletion(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &InlineFileDeletionRow,
) -> CatalogResult<()> {
    let mut ended = row.clone();
    ended.validity.end_order = Some(incomplete_order());
    trx.atomic_op(
        &kv.namespaced_key(&inline_file_deletion_key(
            catalog,
            ended.table_id,
            ended.data_file_id,
            ended.validity.begin_order,
            ended.row_id,
        )),
        &versionstamped_value(
            &ended.encode(),
            InlineFileDeletionRow::END_ORDER_BYTES_OFFSET,
        )?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(super) fn delete_file_timeline_order_for_commit(
    proposed_data_file_ids: &ProposedDataFileTimelineLookup<'_>,
    row: &DeleteFileRow,
    placeholder: CatalogOrderId,
) -> CatalogOrderId {
    if proposed_data_file_ids.contains(&row.data_file_id) {
        return row.validity.begin_order;
    }
    placeholder
}

pub(super) enum ProposedDataFileTimelineLookup<'a> {
    Scan(&'a [DataFileRow]),
    Set(BTreeSet<DataFileId>),
}

impl ProposedDataFileTimelineLookup<'_> {
    pub(super) fn contains(&self, data_file_id: &DataFileId) -> bool {
        match self {
            Self::Scan(data_files) => data_files
                .iter()
                .any(|row| row.data_file_id == *data_file_id),
            Self::Set(data_file_ids) => data_file_ids.contains(data_file_id),
        }
    }

    #[cfg(test)]
    pub(super) fn uses_set(&self) -> bool {
        matches!(self, Self::Set(_))
    }
}

pub(super) fn proposed_data_file_ids_for_delete_timeline<'a>(
    data_files: &'a [DataFileRow],
    delete_files: &[DeleteFileMaterialization],
) -> ProposedDataFileTimelineLookup<'a> {
    if delete_files.len() <= 1 || data_files.len() <= 4 {
        return ProposedDataFileTimelineLookup::Scan(data_files);
    }
    ProposedDataFileTimelineLookup::Set(data_files.iter().map(|row| row.data_file_id).collect())
}

pub(super) fn estimate_inline_file_deletion_bytes(
    catalog: crate::CatalogId,
    inline_file_deletions: &[InlineFileDeletionRow],
) -> usize {
    inline_file_deletions
        .iter()
        .map(|row| {
            inline_file_deletion_key(
                catalog,
                row.table_id,
                row.data_file_id,
                row.validity.begin_order,
                row.row_id,
            )
            .len()
            .saturating_add(row.encode().len())
        })
        .sum()
}

pub(super) fn reject_estimated_mutation(estimated_bytes: usize) -> CatalogResult<()> {
    if estimated_bytes <= FdbOrderedCatalogKv::MAX_COMMIT_BYTES {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "foundationdb versionstamped data mutation is {estimated_bytes} bytes, over {} byte limit",
        FdbOrderedCatalogKv::MAX_COMMIT_BYTES
    )))
}

pub(super) fn estimate_mutation_metadata_bytes<'a>(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    attempt_id: Option<CommitAttemptId>,
    data_files: &[DataFileRow],
    delete_files: impl IntoIterator<Item = &'a DeleteFileRow>,
    partition_values: &[FilePartitionValueRow],
    file_column_stats: &[FileColumnStatsRow],
) -> usize {
    let mut bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    if let Some(attempt_id) = attempt_id {
        bytes = bytes.saturating_add(commit_attempt_key(catalog, attempt_id).len());
    }
    for row in data_files {
        let row_len = row.encode().len();
        bytes = bytes
            .saturating_add(data_file_key(catalog, row.data_file_id).len())
            .saturating_add(row_len)
            .saturating_add(current_data_file_key(catalog, row.table_id, row.data_file_id).len())
            .saturating_add(row_len);
    }
    for row in delete_files {
        let row_len = row.encode().len();
        bytes = bytes
            .saturating_add(delete_file_key(catalog, row.delete_file_id).len())
            .saturating_add(row_len)
            .saturating_add(current_delete_file_key(catalog, row.data_file_id).len())
            .saturating_add(row_len);
    }
    let data_file_lookup = PartitionEstimateDataFileLookup::new(data_files, partition_values.len());
    for row in partition_values {
        let encoded_len = row.encode().len();
        let lookup_value_len = data_file_lookup
            .get(row.data_file_id)
            .map_or(encoded_len, |data_file| {
                encode_partition_lookup_value(row, data_file).len()
            });
        bytes = bytes
            .saturating_add(
                file_partition_value_key(catalog, row.data_file_id, row.partition_key_index).len(),
            )
            .saturating_add(encoded_len)
            .saturating_add(
                partition_value_lookup_key(
                    catalog,
                    row.table_id,
                    row.partition_key_index,
                    &row.partition_value,
                    row.data_file_id,
                )
                .len(),
            )
            .saturating_add(lookup_value_len);
    }
    for row in file_column_stats {
        let encoded_len = row.encode().len();
        bytes = bytes
            .saturating_add(file_column_stats_key(catalog, row.data_file_id, row.column_id).len())
            .saturating_add(encoded_len)
            .saturating_add(
                file_column_stats_lookup_key(
                    catalog,
                    row.table_id,
                    row.column_id,
                    row.data_file_id,
                )
                .len(),
            )
            .saturating_add(encoded_len);
    }
    bytes
}

pub(super) enum PartitionEstimateDataFileLookup<'a> {
    Scan(&'a [DataFileRow]),
    Map(BTreeMap<DataFileId, &'a DataFileRow>),
}

impl<'a> PartitionEstimateDataFileLookup<'a> {
    pub(super) fn new(data_files: &'a [DataFileRow], partition_value_count: usize) -> Self {
        let lookup_work = data_files.len().saturating_mul(partition_value_count);
        if lookup_work <= PARTITION_ESTIMATE_LOOKUP_SCAN_MAX_PRODUCT {
            return Self::Scan(data_files);
        }
        let mut by_id = BTreeMap::new();
        for row in data_files {
            by_id.entry(row.data_file_id).or_insert(row);
        }
        Self::Map(by_id)
    }

    pub(super) fn get(&self, data_file_id: DataFileId) -> Option<&'a DataFileRow> {
        match self {
            Self::Scan(data_files) => data_files
                .iter()
                .find(|row| row.data_file_id == data_file_id),
            Self::Map(data_files) => data_files.get(&data_file_id).copied(),
        }
    }

    #[cfg(test)]
    pub(super) fn uses_map(&self) -> bool {
        matches!(self, Self::Map(_))
    }
}
