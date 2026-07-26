use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use foundationdb::options::ConflictRangeType;
use futures::{executor::block_on, future::try_join_all};

use crate::{
    CatalogError, CatalogResult, CommitAttemptId, CommitAttemptRow, DataFileId, DataFileRow,
    DataMutationCommit, DeleteFileId, DeleteFileRow, FdbOrderedCatalogKv, FilePartitionValueRow,
    InlineTableFlush, TableRow,
    conflict::{commit_attempt_key, load_commit_attempt},
    data_file_store::data_file_next_row_id,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_data_mutations::FdbAppendPartitionExpectation,
    fdb_runtime::map_fdb_error,
    keys::{
        current_data_file_key, current_data_file_prefix, current_delete_file_key,
        current_table_row_key, data_file_key, delete_file_key, prefix_end, table_next_row_id_key,
    },
    kv::OrderedCatalogKv,
};

#[cfg(test)]
use crate::store::latest_snapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MutationCommitAttempt {
    Done(DataMutationCommit),
    Retry(CatalogError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowIdOverlapPolicy {
    RejectCurrentOverlaps,
    TrustCompactionReplacementRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MutationFailureAction {
    RecoverMaybeCommitted,
    Retry,
    ReturnError,
}

pub(super) struct MutationDataFileContext {
    data_files: BTreeMap<DataFileId, DataFileRow>,
}

impl MutationDataFileContext {
    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: crate::CatalogId,
        proposed: &[DataFileRow],
        preloaded: &[DataFileRow],
        referenced_ids: BTreeSet<DataFileId>,
    ) -> CatalogResult<Self> {
        let data_files = proposed
            .iter()
            .map(|row| (row.data_file_id, row.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut context = Self { data_files };
        for row in preloaded {
            context
                .data_files
                .entry(row.data_file_id)
                .or_insert_with(|| row.clone());
        }
        context.load_missing(kv, catalog, referenced_ids)?;
        Ok(context)
    }

    pub(super) fn load_missing(
        &mut self,
        kv: &impl OrderedCatalogKv,
        catalog: crate::CatalogId,
        referenced_ids: BTreeSet<DataFileId>,
    ) -> CatalogResult<()> {
        let missing_ids = referenced_ids
            .into_iter()
            .filter(|data_file_id| !self.data_files.contains_key(data_file_id))
            .collect::<Vec<_>>();
        if missing_ids.is_empty() {
            return Ok(());
        }
        let keys = missing_ids
            .iter()
            .map(|data_file_id| data_file_key(catalog, *data_file_id))
            .collect::<Vec<_>>();
        for (data_file_id, value) in missing_ids.into_iter().zip(kv.batch_get(&keys)?) {
            let Some(value) = value else {
                return Err(CatalogError::NotFound("data file"));
            };
            let row = DataFileRow::decode(&value)?;
            if row.data_file_id != data_file_id {
                return Err(CatalogError::Decode(format!(
                    "data file key {} decoded as data file {}",
                    data_file_id.0, row.data_file_id.0
                )));
            }
            self.data_files.insert(data_file_id, row);
        }
        Ok(())
    }

    pub(super) fn get(&self, data_file_id: DataFileId) -> CatalogResult<&DataFileRow> {
        self.data_files
            .get(&data_file_id)
            .ok_or(CatalogError::NotFound("data file"))
    }
}

pub(super) fn mutation_data_file_reference_ids(
    partition_values: &[FilePartitionValueRow],
    delete_files: &[DeleteFileMaterialization],
    dropped_data_file_ids: &[DataFileId],
) -> BTreeSet<DataFileId> {
    partition_values
        .iter()
        .map(|row| row.data_file_id)
        .chain(
            delete_files
                .iter()
                .map(DeleteFileMaterialization::data_file_id),
        )
        .chain(dropped_data_file_ids.iter().copied())
        .collect()
}

pub(super) fn materialized_inline_delete_file_data_file_ids(
    delete_files: &[DeleteFileMaterialization],
) -> BTreeSet<DataFileId> {
    delete_files
        .iter()
        .filter(|materialization| materialization.materializes_inline_deletes())
        .map(DeleteFileMaterialization::data_file_id)
        .collect()
}

pub(super) fn reject_existing_file_ids(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    data_files: &[DataFileRow],
    delete_files: &[DeleteFileMaterialization],
) -> CatalogResult<()> {
    reject_duplicate_proposed_file_ids(
        data_files,
        delete_files
            .iter()
            .map(|materialization| materialization.row().delete_file_id),
    )?;
    let data_file_keys = data_files
        .iter()
        .map(|row| data_file_key(catalog, row.data_file_id))
        .collect::<Vec<_>>();
    let delete_file_keys = delete_files
        .iter()
        .map(|row| delete_file_key(catalog, row.row().delete_file_id))
        .collect::<Vec<_>>();
    let existing = transaction_batch_get(
        kv,
        trx,
        &data_file_keys
            .iter()
            .chain(&delete_file_keys)
            .cloned()
            .collect::<Vec<_>>(),
    )?;
    let (existing_data_files, existing_delete_files) = existing.split_at(data_files.len());
    for (row, existing) in data_files.iter().zip(existing_data_files) {
        if existing.is_some() {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: data file id {} already exists",
                row.data_file_id.0
            )));
        }
    }

    for (row, existing) in delete_files.iter().zip(existing_delete_files) {
        if existing.is_some() {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: delete file id {} already exists",
                row.row().delete_file_id.0
            )));
        }
    }
    Ok(())
}

pub(super) fn transaction_batch_get(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    keys: &[Vec<u8>],
) -> CatalogResult<Vec<Option<Vec<u8>>>> {
    let reads = keys.iter().map(|key| {
        let namespaced = kv.namespaced_key(key);
        async move {
            trx.get(&namespaced, false)
                .await
                .map_err(map_fdb_error)
                .map(|value| value.map(|bytes| bytes.deref().to_vec()))
        }
    });
    block_on(try_join_all(reads))
}

pub(super) fn stage_current_data_file_conflicts(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    data_files: &[DataFileRow],
) -> CatalogResult<()> {
    let table_ids = data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<BTreeSet<_>>();
    for table_id in table_ids {
        let prefix = kv.namespaced_key(&current_data_file_prefix(catalog, table_id));
        trx.add_conflict_range(&prefix, &prefix_end(&prefix), ConflictRangeType::Read)
            .map_err(map_fdb_error)?;
    }
    Ok(())
}

pub(super) fn reject_allocated_table_row_id_reuse_in_transaction(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    data_files: &[DataFileRow],
) -> CatalogResult<()> {
    let mut by_table = BTreeMap::<crate::TableId, Vec<&DataFileRow>>::new();
    for row in data_files
        .iter()
        .filter(|row| data_file_next_row_id(row).is_some())
    {
        by_table.entry(row.table_id).or_default().push(row);
    }
    let table_ids = by_table.keys().copied().collect::<Vec<_>>();
    let keys = table_ids
        .iter()
        .map(|table_id| table_next_row_id_key(catalog, *table_id))
        .collect::<Vec<_>>();
    let values = transaction_batch_get(kv, trx, &keys)?;
    for ((table_id, rows), value) in by_table.into_iter().zip(values) {
        let next_row_id = match value {
            Some(value) => u64::from_be_bytes(value.try_into().map_err(|_| {
                CatalogError::Decode(format!("table {} next row id must be 8 bytes", table_id.0))
            })?),
            None => 0,
        };
        for row in rows {
            if row.row_id_start < next_row_id {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict committing data mutation: data file {} row ids [{}..{}) reuse allocated row ids below table {} next row id {}",
                    row.data_file_id.0,
                    row.row_id_start,
                    row.row_id_start.saturating_add(row.record_count),
                    table_id.0,
                    next_row_id
                )));
            }
        }
    }
    Ok(())
}

pub(super) struct DataFileTargetValidation<'a> {
    pub(super) recovery_id: Option<CommitAttemptId>,
    pub(super) read_order: Option<crate::CatalogOrderId>,
    pub(super) data_file_context: &'a MutationDataFileContext,
    pub(super) proposed_data_files: &'a [DataFileRow],
    pub(super) append_partitions: &'a [FdbAppendPartitionExpectation],
    pub(super) delete_files: &'a [DeleteFileMaterialization],
    pub(super) dropped_data_file_ids: &'a [DataFileId],
}

pub(super) fn reject_stale_data_file_targets_in_transaction(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    input: DataFileTargetValidation<'_>,
) -> CatalogResult<bool> {
    let DataFileTargetValidation {
        recovery_id,
        read_order,
        data_file_context,
        proposed_data_files,
        append_partitions,
        delete_files,
        dropped_data_file_ids,
    } = input;
    let proposed_ids = proposed_data_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let target_ids = delete_files
        .iter()
        .map(DeleteFileMaterialization::data_file_id)
        .chain(dropped_data_file_ids.iter().copied())
        .filter(|data_file_id| !proposed_ids.contains(data_file_id))
        .collect::<BTreeSet<_>>();
    let active_targets = target_ids
        .iter()
        .map(|data_file_id| data_file_context.get(*data_file_id))
        .collect::<CatalogResult<Vec<_>>>()?
        .into_iter()
        .filter(|row| read_order.is_none_or(|read_order| row.validity.begin_order <= read_order))
        .collect::<Vec<_>>();
    let delete_target_ids = read_order
        .map(|_| {
            delete_files
                .iter()
                .map(DeleteFileMaterialization::data_file_id)
                .filter(|data_file_id| target_ids.contains(data_file_id))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let delete_target_ids = active_targets
        .iter()
        .map(|row| row.data_file_id)
        .filter(|data_file_id| delete_target_ids.contains(data_file_id))
        .collect::<Vec<_>>();
    let proposed_table_ids = proposed_data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<BTreeSet<_>>();
    let mut first_read_keys = recovery_id
        .map(|recovery_id| vec![commit_attempt_key(catalog, recovery_id)])
        .unwrap_or_default();
    first_read_keys.extend(
        active_targets
            .iter()
            .map(|row| current_data_file_key(catalog, row.table_id, row.data_file_id)),
    );
    first_read_keys.extend(
        delete_target_ids
            .iter()
            .map(|data_file_id| current_delete_file_key(catalog, *data_file_id)),
    );
    first_read_keys.extend(
        proposed_table_ids
            .iter()
            .map(|table_id| current_table_row_key(catalog, *table_id)),
    );
    let first_read_values = transaction_batch_get(kv, trx, &first_read_keys)?;
    let recovery_value_count = usize::from(recovery_id.is_some());
    let recovery_value = if recovery_value_count == 1 {
        first_read_values.first()
    } else {
        None
    };
    if let Some(Some(value)) = recovery_value {
        CommitAttemptRow::decode(value)?;
        return Ok(true);
    }
    let current_data_file_end = recovery_value_count.saturating_add(active_targets.len());
    let current_data_files = &first_read_values[recovery_value_count..current_data_file_end];
    for (row, current) in active_targets.iter().zip(current_data_files) {
        if current.is_none() {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: data file {} is no longer current",
                row.data_file_id.0
            )));
        }
    }
    let current_delete_file_end = current_data_file_end.saturating_add(delete_target_ids.len());
    for (table_id, current) in proposed_table_ids
        .iter()
        .zip(&first_read_values[current_delete_file_end..])
    {
        let Some(value) = current else {
            let message = if read_order.is_some() {
                format!(
                    "conflict committing data mutation: table {} was dropped after read snapshot",
                    table_id.0
                )
            } else {
                format!(
                    "conflict committing data mutation: table {} is not current",
                    table_id.0
                )
            };
            return Err(CatalogError::InvalidMutation(message));
        };
        let row = TableRow::decode(value)?;
        if row.table_id != *table_id {
            return Err(CatalogError::Decode(format!(
                "current table key {} decoded as table {}",
                table_id.0, row.table_id.0
            )));
        }
        for data_file in proposed_data_files
            .iter()
            .filter(|data_file| data_file.table_id == *table_id)
        {
            let Some(expectation) = append_partitions
                .iter()
                .find(|expectation| expectation.data_file_id == data_file.data_file_id)
            else {
                if read_order.is_some() {
                    return Err(CatalogError::InvalidMutation(format!(
                        "conflict committing data mutation: partition expectation is missing for data file {}",
                        data_file.data_file_id.0
                    )));
                }
                continue;
            };
            if !expectation.matches_current_table(&row) {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict committing data mutation: table {} partition metadata is stale",
                    table_id.0
                )));
            }
        }
    }
    let Some(read_order) = read_order else {
        return Ok(false);
    };
    let current_delete_files = &first_read_values[current_data_file_end..current_delete_file_end];
    let mut legacy_delete_ids = Vec::new();
    for (data_file_id, current) in delete_target_ids.iter().zip(current_delete_files) {
        let Some(value) = current else {
            continue;
        };
        match DeleteFileRow::decode(value) {
            Ok(row) => reject_current_delete_file_after_read(*data_file_id, read_order, &row)?,
            Err(_) if value.len() == 8 => {
                legacy_delete_ids.push((
                    *data_file_id,
                    DeleteFileId(u64::from_be_bytes(value.as_slice().try_into().map_err(
                        |_| {
                            CatalogError::Decode(
                                "current delete file pointer is truncated".to_owned(),
                            )
                        },
                    )?)),
                ));
            }
            Err(_) => {
                return Err(CatalogError::Decode(format!(
                    "current delete file pointer must be a row or 8-byte id, got {} bytes",
                    value.len()
                )));
            }
        }
    }
    if legacy_delete_ids.is_empty() {
        return Ok(false);
    }
    let keys = legacy_delete_ids
        .iter()
        .map(|(_, delete_file_id)| delete_file_key(catalog, *delete_file_id))
        .collect::<Vec<_>>();
    for ((data_file_id, _), value) in legacy_delete_ids
        .iter()
        .zip(transaction_batch_get(kv, trx, &keys)?)
    {
        let Some(value) = value else {
            return Err(CatalogError::NotFound("delete file"));
        };
        reject_current_delete_file_after_read(
            *data_file_id,
            read_order,
            &DeleteFileRow::decode(&value)?,
        )?;
    }
    Ok(false)
}

fn reject_current_delete_file_after_read(
    data_file_id: DataFileId,
    read_order: crate::CatalogOrderId,
    delete_file: &DeleteFileRow,
) -> CatalogResult<()> {
    if delete_file.validity.begin_order > read_order {
        return Err(CatalogError::InvalidMutation(format!(
            "conflict committing data mutation: data file {} was deleted from after read snapshot",
            data_file_id.0
        )));
    }
    Ok(())
}

pub(super) fn should_reject_current_row_id_overlaps(
    row_id_overlap_policy: RowIdOverlapPolicy,
    inline_flushes: &[InlineTableFlush],
) -> bool {
    matches!(
        row_id_overlap_policy,
        RowIdOverlapPolicy::RejectCurrentOverlaps
    ) && inline_flushes.is_empty()
}

pub(super) fn reject_duplicate_proposed_file_ids(
    data_files: &[DataFileRow],
    delete_file_ids_iter: impl IntoIterator<Item = DeleteFileId>,
) -> CatalogResult<()> {
    let mut data_file_ids = BTreeSet::new();
    for row in data_files {
        if !data_file_ids.insert(row.data_file_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: data file id {} is duplicated in the mutation",
                row.data_file_id.0
            )));
        }
    }

    let mut seen_delete_file_ids = BTreeSet::new();
    for delete_file_id in delete_file_ids_iter {
        if !seen_delete_file_ids.insert(delete_file_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: delete file id {} is duplicated in the mutation",
                delete_file_id.0
            )));
        }
    }
    Ok(())
}

pub(super) fn recover_committed_mutation(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    attempt_id: Option<CommitAttemptId>,
) -> CatalogResult<Option<DataMutationCommit>> {
    let Some(attempt_id) = attempt_id else {
        return Ok(None);
    };
    Ok(load_commit_attempt(kv, catalog, attempt_id)?.map(|_| DataMutationCommit::default()))
}

#[cfg(test)]
pub(super) fn mutation_snapshot_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    attempt_id: Option<CommitAttemptId>,
) -> CatalogResult<crate::RawSnapshotSequence> {
    let latest = latest_snapshot(kv, catalog)?;
    mutation_snapshot_sequence_from_latest(latest.as_ref(), attempt_id)
}

pub(super) fn mutation_snapshot_sequence_from_latest(
    latest: Option<&crate::SnapshotRow>,
    attempt_id: Option<CommitAttemptId>,
) -> CatalogResult<crate::RawSnapshotSequence> {
    if let Some(attempt_id) = attempt_id {
        let raw_sequence = u64::try_from(attempt_id.0).map_err(|_| {
            CatalogError::InvalidMutation(format!(
                "ducklake snapshot id {} is too large for aux catalog snapshots",
                attempt_id.0
            ))
        })?;
        let requested = crate::RawSnapshotSequence(raw_sequence);
        return Ok(latest.map_or(requested, |snapshot| {
            if requested >= snapshot.sequence {
                requested
            } else {
                snapshot.sequence.next()
            }
        }));
    }
    Ok(
        latest.map_or(crate::RawSnapshotSequence::initial(), |snapshot| {
            snapshot.sequence.next()
        }),
    )
}
