use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use foundationdb::options::{ConflictRangeType, MutationType};
use futures::{executor::block_on, future::try_join_all};

use crate::{
    CatalogError, CatalogOrderId, CatalogResult, CommitAttemptId, CommitAttemptRow, DataFileId,
    DataFileRow, DataMutationCommit, DeleteFileId, DeleteFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, FoundationDbErrorClass, InlineFileDeletionRow,
    InlineTableFlush, SnapshotRow, TableId, TableRow, ValidityWindow,
    conflict::{commit_attempt_key, load_commit_attempt},
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    data_file_store::reject_current_data_file_row_id_overlaps_except_with_latest_order,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_data_mutation_staging::{
        stage_catalog_file_stats_version, stage_data_file_without_watermark,
        stage_delete_file_without_watermark, stage_expired_data_file, stage_expired_delete_file,
        stage_file_column_stats, stage_file_partition_value, stage_inline_file_deletion,
        stage_snapshot, stage_snapshot_operation, stage_table_file_stats_version,
    },
    fdb_inline_flushes::{
        PreparedInlineFlush, estimate_prepared_inline_flush_bytes, prepare_inline_flushes,
        stage_prepared_inline_flush_versionstamped,
    },
    fdb_runtime::{classify_fdb_error, map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{committed_order, incomplete_order, versionstamped_value},
    file_partitions::{encode_partition_lookup_value, remove_cached_file_partition_values},
    file_stats::remove_cached_file_column_stats_for_data_file,
    inline_data::list_inline_file_deletion_rows_for_data_files_at,
    keys::{
        current_data_file_key, current_data_file_prefix, current_delete_file_key,
        current_table_row_key, data_file_key, delete_file_key, file_column_stats_key,
        file_column_stats_lookup_key, file_partition_value_key, inline_file_deletion_key,
        partition_value_lookup_key, prefix_end, scheduled_data_file_cleanup_key,
        scheduled_delete_file_cleanup_key, snapshot_key, snapshot_timestamp_key,
    },
    kv::OrderedCatalogKv,
    list_tables_at,
    maintenance::{
        ScheduledDataFileCleanupKind, encode_scheduled_data_cleanup_value,
        encode_scheduled_delete_cleanup_value,
    },
    rows::current_timestamp_micros,
    snapshot_operations::SnapshotOperationKind,
    store::latest_snapshot,
};

const MAX_MUTATION_COMMIT_RETRIES: usize = 3;
const PARTITION_ESTIMATE_LOOKUP_SCAN_MAX_PRODUCT: usize = 64;

#[derive(Debug, Default)]
struct FdbFileIdWatermark {
    candidate: Option<u64>,
}

impl FdbFileIdWatermark {
    fn observe(&mut self, value: u64) {
        self.candidate = Some(
            self.candidate
                .map_or(value, |candidate| candidate.max(value)),
        );
    }

    fn stage(
        self,
        kv: &FdbOrderedCatalogKv,
        trx: &foundationdb::Transaction,
        catalog: crate::CatalogId,
    ) {
        if let Some(candidate) = self.candidate {
            stage_fdb_max_file_id_watermark(kv, trx, catalog, candidate);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FdbExpiredDeleteFile {
    pub table_id: crate::TableId,
    pub delete_file: DeleteFileRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiredObjectCleanupPolicy {
    Schedule(ScheduledDataFileCleanupKind),
    Preserve,
}

impl FdbOrderedCatalogKv {
    pub fn commit_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_metadata(
            catalog,
            attempt_id,
            crate::SnapshotCommitMetadata::default(),
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            dropped_data_file_ids,
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_metadata(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
            catalog,
            attempt_id,
            commit_metadata,
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            Vec::new(),
            Vec::new(),
            dropped_data_file_ids,
        )
    }

    pub fn commit_data_mutation_versionstamped_with_inline_file_deletions(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
            catalog,
            attempt_id,
            crate::SnapshotCommitMetadata::default(),
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            inline_file_deletions,
            Vec::new(),
            dropped_data_file_ids,
        )
    }

    pub fn commit_data_mutation_versionstamped_with_inline_file_deletions_and_file_stats(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
            catalog,
            attempt_id,
            crate::SnapshotCommitMetadata::default(),
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_expired_delete_files(
            catalog,
            attempt_id,
            commit_metadata,
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
            Vec::new(),
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_expired_delete_files(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_file_ids: Vec<DataFileId>,
        expired_delete_files: Vec<FdbExpiredDeleteFile>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_expired_delete_files_and_row_id_policy(
            catalog,
            attempt_id,
            commit_metadata,
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
            expired_delete_files,
            Vec::new(),
            RowIdOverlapPolicy::RejectCurrentOverlaps,
            ExpiredObjectCleanupPolicy::Schedule(ScheduledDataFileCleanupKind::UnreachableOnly),
            Vec::new(),
        )
    }

    pub(crate) fn commit_compaction_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        partition_values: Vec<FilePartitionValueRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_files: Vec<DataFileRow>,
    ) -> CatalogResult<DataMutationCommit> {
        let dropped_data_file_ids = dropped_data_files
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>();
        self.commit_data_mutation_versionstamped_with_expired_delete_files_and_row_id_policy(
            catalog,
            attempt_id,
            commit_metadata,
            data_files,
            Vec::new(),
            Vec::new(),
            partition_values,
            Vec::new(),
            file_column_stats,
            dropped_data_file_ids,
            Vec::new(),
            Vec::new(),
            RowIdOverlapPolicy::TrustCompactionReplacementRows,
            ExpiredObjectCleanupPolicy::Schedule(
                ScheduledDataFileCleanupKind::CompactionReplacement,
            ),
            dropped_data_files,
        )
    }

    pub(crate) fn commit_rewrite_delete_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_files: Vec<DataFileRow>,
        expired_delete_files: Vec<FdbExpiredDeleteFile>,
        table_id: crate::TableId,
    ) -> CatalogResult<DataMutationCommit> {
        let dropped_data_file_ids = dropped_data_files
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>();
        self.commit_data_mutation_versionstamped_with_expired_delete_files_and_row_id_policy(
            catalog,
            attempt_id,
            commit_metadata,
            data_files,
            Vec::new(),
            Vec::new(),
            partition_values,
            inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
            expired_delete_files,
            vec![(SnapshotOperationKind::RewriteDelete, table_id)],
            RowIdOverlapPolicy::TrustCompactionReplacementRows,
            ExpiredObjectCleanupPolicy::Preserve,
            dropped_data_files,
        )
    }

    fn commit_data_mutation_versionstamped_with_expired_delete_files_and_row_id_policy(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: Vec<FileColumnStatsRow>,
        dropped_data_file_ids: Vec<DataFileId>,
        expired_delete_files: Vec<FdbExpiredDeleteFile>,
        snapshot_operations: Vec<(SnapshotOperationKind, crate::TableId)>,
        row_id_overlap_policy: RowIdOverlapPolicy,
        expired_object_cleanup_policy: ExpiredObjectCleanupPolicy,
        preloaded_data_files: Vec<DataFileRow>,
    ) -> CatalogResult<DataMutationCommit> {
        if data_files.is_empty()
            && delete_files.is_empty()
            && inline_flushes.is_empty()
            && partition_values.is_empty()
            && inline_file_deletions.is_empty()
            && file_column_stats.is_empty()
            && dropped_data_file_ids.is_empty()
            && expired_delete_files.is_empty()
        {
            return Ok(DataMutationCommit::default());
        }
        let mut last_retry_error = None;
        for attempt_index in 0..=MAX_MUTATION_COMMIT_RETRIES {
            match self.try_commit_data_mutation_versionstamped(
                catalog,
                attempt_id,
                &commit_metadata,
                data_files.clone(),
                delete_files.clone(),
                &inline_flushes,
                &partition_values,
                inline_file_deletions.clone(),
                &file_column_stats,
                &dropped_data_file_ids,
                &expired_delete_files,
                &snapshot_operations,
                row_id_overlap_policy,
                expired_object_cleanup_policy,
                &preloaded_data_files,
                attempt_index,
            )? {
                MutationCommitAttempt::Done(result) => return Ok(result),
                MutationCommitAttempt::Retry(error) => last_retry_error = Some(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb data mutation retry loop did not run".to_owned(),
            )
        }))
    }

    fn try_commit_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: &crate::SnapshotCommitMetadata,
        mut data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: &[InlineTableFlush],
        partition_values: &[FilePartitionValueRow],
        mut inline_file_deletions: Vec<InlineFileDeletionRow>,
        file_column_stats: &[FileColumnStatsRow],
        dropped_data_file_ids: &[DataFileId],
        expired_delete_files: &[FdbExpiredDeleteFile],
        snapshot_operations: &[(SnapshotOperationKind, crate::TableId)],
        row_id_overlap_policy: RowIdOverlapPolicy,
        expired_object_cleanup_policy: ExpiredObjectCleanupPolicy,
        preloaded_data_files: &[DataFileRow],
        attempt_index: usize,
    ) -> CatalogResult<MutationCommitAttempt> {
        let recovery_id = mutation_recovery_attempt_id(
            attempt_id,
            commit_metadata,
            &data_files,
            &delete_files,
            &inline_flushes,
            partition_values,
            &inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
            expired_delete_files,
            snapshot_operations,
        );
        if let Some(result) = recover_committed_mutation(self, catalog, recovery_id)? {
            return Ok(MutationCommitAttempt::Done(result));
        }
        let mut delete_file_materializations = delete_files
            .into_iter()
            .map(DeleteFileMaterialization::historical_delete_file)
            .collect::<Vec<_>>();
        reject_missing_current_tables(self, catalog, &data_files)?;
        let latest = latest_snapshot(self, catalog)?;
        if should_reject_current_row_id_overlaps(row_id_overlap_policy, &inline_flushes) {
            let dropped_data_file_id_set = dropped_data_file_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            reject_current_data_file_row_id_overlaps_except_with_latest_order(
                self,
                catalog,
                &data_files,
                &dropped_data_file_id_set,
                latest.as_ref().map(|row| row.order),
            )?;
        }

        let next_sequence = mutation_snapshot_sequence_from_latest(latest.as_ref(), attempt_id)?;
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence)
            .with_optional_commit_metadata(Some(commit_metadata));
        for row in &mut data_files {
            if row.max_partial_order.is_some() {
                row.validity.end_order = None;
            } else {
                row.validity = ValidityWindow::new(placeholder, None);
            }
        }
        for materialization in &mut delete_file_materializations {
            prepare_delete_file_for_versionstamped_commit(materialization.row_mut(), placeholder);
        }
        let mut data_file_context = MutationDataFileContext::load(
            self,
            catalog,
            &data_files,
            preloaded_data_files,
            materialized_inline_delete_file_data_file_ids(&delete_file_materializations),
        )?;
        let materialized_inline_file_deletions = materialized_inline_file_deletions(
            self,
            catalog,
            &data_file_context,
            &delete_file_materializations,
            latest.as_ref(),
        )?;
        complete_materialized_delete_file_visibility(
            &mut delete_file_materializations,
            &materialized_inline_file_deletions,
        );
        for row in &mut inline_file_deletions {
            row.validity = ValidityWindow::new(placeholder, None);
        }
        let prepared_inline_flushes = prepare_inline_flushes(self, catalog, &inline_flushes)?;
        reject_oversized_mutation(
            catalog,
            &snapshot,
            attempt_id,
            &data_files,
            &delete_file_materializations,
            &prepared_inline_flushes,
            partition_values,
            &inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
            expired_delete_files,
            &materialized_inline_file_deletions,
            snapshot_operations,
        )?;
        data_file_context.load_missing(
            self,
            catalog,
            mutation_data_file_reference_ids(
                partition_values,
                &delete_file_materializations,
                dropped_data_file_ids,
            ),
        )?;

        let trx = self.create_transaction()?;
        let mut staged_inline_flush_items = 0usize;
        for flush in &prepared_inline_flushes {
            match stage_prepared_inline_flush_versionstamped(self, &trx, catalog, flush) {
                Ok(staged) => {
                    staged_inline_flush_items =
                        staged_inline_flush_items.saturating_add(staged.staged_item_count());
                }
                Err(error) => {
                    if matches!(error, CatalogError::ConflictFenceChanged { .. }) {
                        return Ok(MutationCommitAttempt::Done(DataMutationCommit::default()));
                    }
                    return Err(error);
                }
            }
        }
        if !inline_flushes.is_empty() && staged_inline_flush_items == 0 {
            return Ok(MutationCommitAttempt::Done(DataMutationCommit::default()));
        }
        reject_existing_file_ids(
            self,
            &trx,
            catalog,
            &data_files,
            &delete_file_materializations,
        )?;
        stage_current_data_file_conflicts(self, &trx, catalog, &data_files)?;
        if let Some(recovery_id) = recovery_id {
            let attempt = CommitAttemptRow::new(recovery_id, placeholder);
            trx.atomic_op(
                &self.namespaced_key(&commit_attempt_key(catalog, recovery_id)),
                &versionstamped_value(
                    &attempt.encode(),
                    CommitAttemptRow::COMMIT_ORDER_BYTES_OFFSET,
                )?,
                MutationType::SetVersionstampedValue,
            );
        }
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        for (kind, table_id) in snapshot_operations {
            stage_snapshot_operation(self, &trx, catalog, placeholder, *kind, *table_id)?;
        }
        let mut file_watermark = FdbFileIdWatermark::default();
        for row in &data_files {
            stage_data_file_without_watermark(self, &trx, catalog, row)?;
            file_watermark.observe(row.data_file_id.0);
        }
        let proposed_data_file_ids =
            proposed_data_file_ids_for_delete_timeline(&data_files, &delete_file_materializations);
        for row in partition_values {
            let data_file = data_file_context.get(row.data_file_id)?;
            stage_file_partition_value(self, &trx, catalog, row, data_file);
        }
        for row in file_column_stats {
            stage_file_column_stats(self, &trx, catalog, row);
        }
        if !file_column_stats.is_empty() {
            stage_catalog_file_stats_version(self, &trx, catalog)?;
        }
        for table_id in file_column_stats
            .iter()
            .map(|row| row.table_id)
            .collect::<BTreeSet<_>>()
        {
            stage_table_file_stats_version(self, &trx, catalog, table_id)?;
        }
        for materialization in &delete_file_materializations {
            let row = materialization.row();
            let data_file = data_file_context.get(row.data_file_id)?;
            let timeline_order =
                delete_file_timeline_order_for_commit(&proposed_data_file_ids, row, placeholder);
            stage_delete_file_without_watermark(
                self,
                &trx,
                catalog,
                data_file,
                row,
                row.validity.begin_order,
                timeline_order,
                placeholder,
            )?;
            file_watermark.observe(row.delete_file_id.0);
        }
        for row in &materialized_inline_file_deletions {
            stage_materialized_inline_file_deletion(self, &trx, catalog, row)?;
        }
        for row in &inline_file_deletions {
            stage_inline_file_deletion(self, &trx, catalog, row)?;
        }
        if !inline_file_deletions.is_empty() {
            file_watermark.observe(snapshot.sequence.0);
        }
        file_watermark.stage(self, &trx, catalog);
        let cleanup_schedule_start_micros = current_timestamp_micros();
        let dropped_data_files = dropped_data_file_ids
            .iter()
            .map(|data_file_id| data_file_context.get(*data_file_id).cloned())
            .collect::<CatalogResult<Vec<_>>>()?;
        reject_dropping_non_current_data_files(self, catalog, &dropped_data_files)?;
        for mut row in dropped_data_files {
            let data_file_id = row.data_file_id;
            row.validity.end_order = Some(placeholder);
            stage_expired_data_file(self, &trx, catalog, &row)?;
            if let ExpiredObjectCleanupPolicy::Schedule(cleanup_kind) =
                expired_object_cleanup_policy
            {
                trx.set(
                    &self.namespaced_key(&scheduled_data_file_cleanup_key(catalog, data_file_id)),
                    &encode_scheduled_data_cleanup_value(
                        cleanup_kind,
                        cleanup_schedule_start_micros,
                    ),
                );
            }
        }
        for expired in expired_delete_files {
            let mut row = expired.delete_file.clone();
            row.validity.end_order = Some(placeholder);
            stage_expired_delete_file(self, &trx, catalog, expired.table_id, &row)?;
            if matches!(
                expired_object_cleanup_policy,
                ExpiredObjectCleanupPolicy::Schedule(_)
            ) {
                trx.set(
                    &self.namespaced_key(&scheduled_delete_file_cleanup_key(
                        catalog,
                        row.delete_file_id,
                    )),
                    &encode_scheduled_delete_cleanup_value(
                        expired.table_id,
                        cleanup_schedule_start_micros,
                    ),
                );
            }
        }

        let versionstamp = trx.get_versionstamp();
        if let Err(error) = block_on(trx.commit()) {
            let error_class = classify_fdb_error(*error);
            if !inline_flushes.is_empty()
                && error_class == FoundationDbErrorClass::RetryableNotCommitted
            {
                return Ok(MutationCommitAttempt::Done(DataMutationCommit::default()));
            }
            match mutation_failure_action(error_class, attempt_index) {
                MutationFailureAction::RecoverMaybeCommitted => {
                    if let Some(result) = recover_committed_mutation(self, catalog, recovery_id)? {
                        return Ok(MutationCommitAttempt::Done(result));
                    }
                }
                MutationFailureAction::Retry => {
                    return Ok(MutationCommitAttempt::Retry(map_fdb_commit_error(error)));
                }
                MutationFailureAction::ReturnError => {}
            }
            let catalog_error = map_fdb_commit_error(error);
            return Err(mutation_final_error(
                catalog_error,
                error_class,
                attempt_index,
            ));
        }

        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        for row in &mut data_files {
            if row.max_partial_order.is_some() {
                row.validity.end_order = None;
            } else {
                row.validity = ValidityWindow::new(order, None);
            }
        }
        for materialization in &mut delete_file_materializations {
            if materialization.row().validity.begin_order == placeholder {
                materialization.row_mut().validity = ValidityWindow::new(order, None);
            }
        }
        for row in &mut inline_file_deletions {
            row.validity = ValidityWindow::new(order, None);
        }
        for row in partition_values {
            remove_cached_file_partition_values(self, catalog, row.data_file_id);
        }
        let mut stats_file_ids = BTreeSet::new();
        for row in file_column_stats {
            if stats_file_ids.insert(row.data_file_id) {
                remove_cached_file_column_stats_for_data_file(self, catalog, row.data_file_id);
            }
        }
        Ok(MutationCommitAttempt::Done(DataMutationCommit {
            data_files,
            delete_files: DeleteFileMaterialization::rows(&delete_file_materializations),
            partition_value_count: partition_values.len(),
            file_column_stats_count: file_column_stats.len(),
            flushed_inline_count: inline_flushes.len(),
            inline_file_deletion_count: inline_file_deletions.len(),
            dropped_data_file_count: dropped_data_file_ids.len(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MutationCommitAttempt {
    Done(DataMutationCommit),
    Retry(CatalogError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowIdOverlapPolicy {
    RejectCurrentOverlaps,
    TrustCompactionReplacementRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationFailureAction {
    RecoverMaybeCommitted,
    Retry,
    ReturnError,
}

fn reject_dropping_non_current_data_files(
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

struct MutationDataFileContext {
    data_files: BTreeMap<DataFileId, DataFileRow>,
}

impl MutationDataFileContext {
    fn load(
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

    fn load_missing(
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

    fn get(&self, data_file_id: DataFileId) -> CatalogResult<&DataFileRow> {
        self.data_files
            .get(&data_file_id)
            .ok_or(CatalogError::NotFound("data file"))
    }
}

fn mutation_data_file_reference_ids(
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

fn materialized_inline_delete_file_data_file_ids(
    delete_files: &[DeleteFileMaterialization],
) -> BTreeSet<DataFileId> {
    delete_files
        .iter()
        .filter(|materialization| materialization.materializes_inline_deletes())
        .map(DeleteFileMaterialization::data_file_id)
        .collect()
}

fn reject_existing_file_ids(
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

fn transaction_batch_get(
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

fn stage_current_data_file_conflicts(
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

fn should_reject_current_row_id_overlaps(
    row_id_overlap_policy: RowIdOverlapPolicy,
    inline_flushes: &[InlineTableFlush],
) -> bool {
    matches!(
        row_id_overlap_policy,
        RowIdOverlapPolicy::RejectCurrentOverlaps
    ) && inline_flushes.is_empty()
}

fn reject_duplicate_proposed_file_ids(
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

fn recover_committed_mutation(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    attempt_id: Option<CommitAttemptId>,
) -> CatalogResult<Option<DataMutationCommit>> {
    let Some(attempt_id) = attempt_id else {
        return Ok(None);
    };
    Ok(load_commit_attempt(kv, catalog, attempt_id)?.map(|_| DataMutationCommit::default()))
}

fn reject_missing_current_tables(
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

fn missing_current_table_ids_from_index(
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
fn mutation_snapshot_sequence(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    attempt_id: Option<CommitAttemptId>,
) -> CatalogResult<crate::RawSnapshotSequence> {
    let latest = latest_snapshot(kv, catalog)?;
    mutation_snapshot_sequence_from_latest(latest.as_ref(), attempt_id)
}

fn mutation_snapshot_sequence_from_latest(
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

fn mutation_recovery_attempt_id(
    snapshot_id: Option<CommitAttemptId>,
    commit_metadata: &crate::SnapshotCommitMetadata,
    data_files: &[DataFileRow],
    delete_files: &[DeleteFileRow],
    inline_flushes: &[InlineTableFlush],
    partition_values: &[FilePartitionValueRow],
    inline_file_deletions: &[InlineFileDeletionRow],
    file_column_stats: &[FileColumnStatsRow],
    dropped_data_file_ids: &[DataFileId],
    expired_delete_files: &[FdbExpiredDeleteFile],
    snapshot_operations: &[(SnapshotOperationKind, crate::TableId)],
) -> Option<CommitAttemptId> {
    let snapshot_id = snapshot_id?;
    let mut hash = Fnv1a64::new();
    hash.write_u128(snapshot_id.0);
    hash.write_tag(1);
    hash.write_optional_string(commit_metadata.author.as_deref());
    hash.write_optional_string(commit_metadata.commit_message.as_deref());
    hash.write_optional_string(commit_metadata.commit_extra_info.as_deref());
    for row in data_files {
        hash.write_tag(2);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in delete_files {
        hash.write_tag(3);
        hash.write_bytes_with_len(&row.encode());
    }
    for flush in inline_flushes {
        hash.write_tag(4);
        hash.write_u64(flush.table_id.0);
        hash.write_u64(flush.schema_id.0);
        hash.write_u64(flush.flush_snapshot_sequence.0);
    }
    for row in partition_values {
        hash.write_tag(5);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in inline_file_deletions {
        hash.write_tag(6);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in file_column_stats {
        hash.write_tag(7);
        hash.write_bytes_with_len(&row.encode());
    }
    for id in dropped_data_file_ids {
        hash.write_tag(8);
        hash.write_u64(id.0);
    }
    for expired in expired_delete_files {
        hash.write_tag(9);
        hash.write_u64(expired.table_id.0);
        hash.write_bytes_with_len(&expired.delete_file.encode());
    }
    for (kind, table_id) in snapshot_operations {
        hash.write_tag(10);
        hash.write_tag(snapshot_operation_hash_tag(*kind));
        hash.write_u64(table_id.0);
    }
    Some(CommitAttemptId(
        (snapshot_id.0 << 64) ^ u128::from(hash.finish()),
    ))
}

fn snapshot_operation_hash_tag(kind: SnapshotOperationKind) -> u8 {
    match kind {
        SnapshotOperationKind::RewriteDelete => 1,
    }
}

struct Fnv1a64(u64);

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn finish(self) -> u64 {
        self.0
    }

    fn write_tag(&mut self, tag: u8) {
        self.write_bytes(&[tag]);
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_be_bytes());
    }

    fn write_u128(&mut self, value: u128) {
        self.write_bytes(&value.to_be_bytes());
    }

    fn write_optional_string(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.write_tag(1);
                self.write_bytes_with_len(value.as_bytes());
            }
            None => self.write_tag(0),
        }
    }

    fn write_bytes_with_len(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        self.write_bytes(bytes);
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

fn mutation_failure_action(
    error_class: FoundationDbErrorClass,
    attempt_index: usize,
) -> MutationFailureAction {
    match error_class {
        FoundationDbErrorClass::MaybeCommitted => MutationFailureAction::RecoverMaybeCommitted,
        FoundationDbErrorClass::RetryableNotCommitted
            if attempt_index < MAX_MUTATION_COMMIT_RETRIES =>
        {
            MutationFailureAction::Retry
        }
        FoundationDbErrorClass::RetryableNotCommitted
        | FoundationDbErrorClass::Retryable
        | FoundationDbErrorClass::NonRetryable => MutationFailureAction::ReturnError,
    }
}

fn mutation_final_error(
    error: CatalogError,
    error_class: FoundationDbErrorClass,
    attempt_index: usize,
) -> CatalogError {
    if error_class != FoundationDbErrorClass::RetryableNotCommitted
        || attempt_index < MAX_MUTATION_COMMIT_RETRIES
    {
        return error;
    }
    let CatalogError::FoundationDb {
        code,
        message,
        class,
    } = error
    else {
        return error;
    };
    CatalogError::FoundationDbRetryExhausted {
        operation: "data mutation commit",
        attempts: attempt_index.saturating_add(1),
        code,
        message,
        class,
    }
}

fn reject_oversized_mutation(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    attempt_id: Option<CommitAttemptId>,
    data_files: &[DataFileRow],
    delete_files: &[DeleteFileMaterialization],
    inline_flushes: &[PreparedInlineFlush],
    partition_values: &[FilePartitionValueRow],
    inline_file_deletions: &[InlineFileDeletionRow],
    file_column_stats: &[FileColumnStatsRow],
    dropped_data_file_ids: &[DataFileId],
    expired_delete_files: &[FdbExpiredDeleteFile],
    expired_inline_file_deletions: &[InlineFileDeletionRow],
    snapshot_operations: &[(SnapshotOperationKind, crate::TableId)],
) -> CatalogResult<()> {
    let estimated_bytes = estimate_mutation_metadata_bytes(
        catalog,
        snapshot,
        attempt_id,
        data_files,
        delete_files.iter().map(DeleteFileMaterialization::row),
        partition_values,
        file_column_stats,
    )
    .saturating_add(estimate_prepared_inline_flush_bytes(
        catalog,
        inline_flushes,
    ))
    .saturating_add(estimate_inline_file_deletion_bytes(
        catalog,
        inline_file_deletions,
    ))
    .saturating_add(dropped_data_file_ids.len().saturating_mul(256))
    .saturating_add(expired_delete_files.len().saturating_mul(256))
    .saturating_add(estimate_inline_file_deletion_bytes(
        catalog,
        expired_inline_file_deletions,
    ))
    .saturating_add(snapshot_operations.len().saturating_mul(64));
    reject_estimated_mutation(estimated_bytes)
}

fn prepare_delete_file_for_versionstamped_commit(
    row: &mut DeleteFileRow,
    placeholder: CatalogOrderId,
) {
    if row.validity.begin_order == CatalogOrderId::uuid_v7(0) {
        row.validity = ValidityWindow::new(placeholder, None);
        return;
    }
    row.validity.end_order = None;
}

fn complete_materialized_delete_file_visibility(
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

fn materialized_inline_file_deletions(
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

fn stage_materialized_inline_file_deletion(
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

fn delete_file_timeline_order_for_commit(
    proposed_data_file_ids: &ProposedDataFileTimelineLookup<'_>,
    row: &DeleteFileRow,
    placeholder: CatalogOrderId,
) -> CatalogOrderId {
    if proposed_data_file_ids.contains(&row.data_file_id) {
        return row.validity.begin_order;
    }
    placeholder
}

enum ProposedDataFileTimelineLookup<'a> {
    Scan(&'a [DataFileRow]),
    Set(BTreeSet<DataFileId>),
}

impl ProposedDataFileTimelineLookup<'_> {
    fn contains(&self, data_file_id: &DataFileId) -> bool {
        match self {
            Self::Scan(data_files) => data_files
                .iter()
                .any(|row| row.data_file_id == *data_file_id),
            Self::Set(data_file_ids) => data_file_ids.contains(data_file_id),
        }
    }

    #[cfg(test)]
    fn uses_set(&self) -> bool {
        matches!(self, Self::Set(_))
    }
}

fn proposed_data_file_ids_for_delete_timeline<'a>(
    data_files: &'a [DataFileRow],
    delete_files: &[DeleteFileMaterialization],
) -> ProposedDataFileTimelineLookup<'a> {
    if delete_files.len() <= 1 || data_files.len() <= 4 {
        return ProposedDataFileTimelineLookup::Scan(data_files);
    }
    ProposedDataFileTimelineLookup::Set(data_files.iter().map(|row| row.data_file_id).collect())
}

fn estimate_inline_file_deletion_bytes(
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

fn reject_estimated_mutation(estimated_bytes: usize) -> CatalogResult<()> {
    if estimated_bytes <= FdbOrderedCatalogKv::MAX_COMMIT_BYTES {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "foundationdb versionstamped data mutation is {estimated_bytes} bytes, over {} byte limit",
        FdbOrderedCatalogKv::MAX_COMMIT_BYTES
    )))
}

fn estimate_mutation_metadata_bytes<'a>(
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

enum PartitionEstimateDataFileLookup<'a> {
    Scan(&'a [DataFileRow]),
    Map(BTreeMap<DataFileId, &'a DataFileRow>),
}

impl<'a> PartitionEstimateDataFileLookup<'a> {
    fn new(data_files: &'a [DataFileRow], partition_value_count: usize) -> Self {
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

    fn get(&self, data_file_id: DataFileId) -> Option<&'a DataFileRow> {
        match self {
            Self::Scan(data_files) => data_files
                .iter()
                .find(|row| row.data_file_id == data_file_id),
            Self::Map(data_files) => data_files.get(&data_file_id).copied(),
        }
    }

    #[cfg(test)]
    fn uses_map(&self) -> bool {
        matches!(self, Self::Map(_))
    }
}

#[cfg(test)]
#[path = "fdb_data_mutations_tests.rs"]
mod tests;
