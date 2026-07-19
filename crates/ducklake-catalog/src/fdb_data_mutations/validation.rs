use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use foundationdb::options::ConflictRangeType;
use futures::{executor::block_on, future::try_join_all};

use crate::{
    CatalogError, CatalogResult, CommitAttemptId, DataFileId, DataFileRow, DataMutationCommit,
    DeleteFileId, FdbOrderedCatalogKv, FilePartitionValueRow, InlineTableFlush, TableId, TableRow,
    conflict::load_commit_attempt,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_runtime::map_fdb_error,
    keys::{
        current_data_file_key, current_data_file_prefix, current_table_row_key, data_file_key,
        delete_file_key, prefix_end,
    },
    kv::OrderedCatalogKv,
    list_tables_at,
    store::latest_snapshot,
};

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

pub(super) fn reject_dropping_non_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    rows: &[DataFileRow],
) -> CatalogResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    for row in rows {
        if row.validity.end_order.is_some() {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} is already closed",
                row.data_file_id.0
            )));
        }
    }
    let keys = rows
        .iter()
        .map(|row| current_data_file_key(catalog, row.table_id, row.data_file_id))
        .collect::<Vec<_>>();
    for (row, current) in rows.iter().zip(kv.batch_get(&keys)?) {
        if current.is_none() {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} is not current",
                row.data_file_id.0
            )));
        }
    }
    Ok(())
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
    for (row, existing) in data_files
        .iter()
        .zip(transaction_batch_get(kv, trx, &data_file_keys)?)
    {
        if existing.is_some() {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: data file id {} already exists",
                row.data_file_id.0
            )));
        }
    }

    let delete_file_keys = delete_files
        .iter()
        .map(|row| delete_file_key(catalog, row.row().delete_file_id))
        .collect::<Vec<_>>();
    for (row, existing) in
        delete_files
            .iter()
            .zip(transaction_batch_get(kv, trx, &delete_file_keys)?)
    {
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

pub(super) fn reject_missing_current_tables(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    data_files: &[DataFileRow],
) -> CatalogResult<()> {
    if data_files.is_empty() {
        return Ok(());
    }
    let table_ids = data_files
        .iter()
        .map(|file| file.table_id)
        .collect::<BTreeSet<_>>();
    let missing = missing_current_table_ids_from_index(kv, catalog, &table_ids)?;
    if missing.is_empty() {
        return Ok(());
    }

    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let current_tables = list_tables_at(kv, catalog, latest.order)?;
    for table_id in missing {
        if !current_tables
            .iter()
            .any(|table| table.table_id == table_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing data mutation: table {} is not current",
                table_id.0
            )));
        }
    }
    Ok(())
}

pub(super) fn missing_current_table_ids_from_index(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    table_ids: &BTreeSet<TableId>,
) -> CatalogResult<Vec<TableId>> {
    let keys = table_ids
        .iter()
        .map(|table_id| current_table_row_key(catalog, *table_id))
        .collect::<Vec<_>>();
    let mut missing = Vec::new();
    for (table_id, value) in table_ids.iter().zip(kv.batch_get(&keys)?) {
        let Some(value) = value else {
            missing.push(*table_id);
            continue;
        };
        let row = TableRow::decode(&value)?;
        if row.table_id != *table_id {
            return Err(CatalogError::Decode(format!(
                "current table key {} decoded as table {}",
                table_id.0, row.table_id.0
            )));
        }
    }
    Ok(missing)
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
