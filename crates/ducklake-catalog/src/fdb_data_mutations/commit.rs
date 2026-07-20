use std::{collections::BTreeSet, ops::Deref};

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogResult, CommitAttemptRow, DataMutationCommit, FdbOrderedCatalogKv,
    FoundationDbErrorClass, SnapshotRow, ValidityWindow,
    conflict::commit_attempt_key,
    data_file_store::reject_current_data_file_row_id_overlaps_except_with_latest_order,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_data_mutation_staging::{
        DeleteFileCommitOrders, stage_catalog_file_stats_version,
        stage_data_file_without_watermark, stage_delete_file_without_watermark,
        stage_expired_data_file, stage_expired_delete_file, stage_file_column_stats,
        stage_file_partition_value, stage_inline_file_deletion, stage_snapshot,
        stage_snapshot_operation, stage_table_file_stats_version,
    },
    fdb_inline_flushes::{prepare_inline_flushes, stage_prepared_inline_flush_versionstamped},
    fdb_runtime::{classify_fdb_error, map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{committed_order, incomplete_order, versionstamped_value},
    file_partitions::remove_cached_file_partition_values,
    file_stats::remove_cached_file_column_stats_for_data_file,
    keys::{scheduled_data_file_cleanup_key, scheduled_delete_file_cleanup_key},
    maintenance::{encode_scheduled_data_cleanup_value, encode_scheduled_delete_cleanup_value},
    rows::current_timestamp_micros,
    store::latest_snapshot,
};

use crate::fdb_data_mutations::*;
impl FdbOrderedCatalogKv {
    pub(super) fn commit_planned_data_mutation(
        &self,
        catalog: crate::CatalogId,
        plan: FdbMutationPlan,
    ) -> CatalogResult<DataMutationCommit> {
        if plan.mutation.is_empty() && plan.expired_delete_files.is_empty() {
            return Ok(DataMutationCommit::default());
        }
        let mut last_retry_error = None;
        for attempt_index in 0..=MAX_MUTATION_COMMIT_RETRIES {
            match self.try_commit_data_mutation_versionstamped(
                catalog,
                plan.clone(),
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

    pub(super) fn try_commit_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        plan: FdbMutationPlan,
        attempt_index: usize,
    ) -> CatalogResult<MutationCommitAttempt> {
        let recovery_id = mutation_recovery_attempt_id(
            plan.attempt_id,
            &plan.commit_metadata,
            &plan.mutation,
            &plan.expired_delete_files,
            &plan.snapshot_operations,
        );
        let FdbMutationPlan {
            attempt_id,
            commit_metadata,
            mutation,
            expired_delete_files,
            snapshot_operations,
            row_id_overlap_policy,
            expired_object_cleanup_policy,
            preloaded_data_files,
        } = plan;
        let FdbDataMutation {
            mut data_files,
            delete_files,
            inline_flushes,
            partition_values,
            mut inline_file_deletions,
            file_column_stats,
            dropped_data_file_ids,
        } = mutation;
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
            .with_optional_commit_metadata(Some(&commit_metadata));
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
            &preloaded_data_files,
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
        reject_oversized_mutation(MutationSizeEstimate {
            catalog,
            snapshot: &snapshot,
            attempt_id,
            data_files: &data_files,
            delete_files: &delete_file_materializations,
            inline_flushes: &prepared_inline_flushes,
            partition_values: &partition_values,
            inline_file_deletions: &inline_file_deletions,
            file_column_stats: &file_column_stats,
            dropped_data_file_ids: &dropped_data_file_ids,
            expired_delete_files: &expired_delete_files,
            expired_inline_file_deletions: &materialized_inline_file_deletions,
            snapshot_operations: &snapshot_operations,
        })?;
        data_file_context.load_missing(
            self,
            catalog,
            mutation_data_file_reference_ids(
                &partition_values,
                &delete_file_materializations,
                &dropped_data_file_ids,
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
            stage_snapshot_operation(self, &trx, catalog, placeholder, kind, table_id)?;
        }
        let mut file_watermark = FdbFileIdWatermark::default();
        for row in &data_files {
            stage_data_file_without_watermark(self, &trx, catalog, row)?;
            file_watermark.observe(row.data_file_id.0);
        }
        let proposed_data_file_ids =
            proposed_data_file_ids_for_delete_timeline(&data_files, &delete_file_materializations);
        for row in &partition_values {
            let data_file = data_file_context.get(row.data_file_id)?;
            stage_file_partition_value(self, &trx, catalog, row, data_file);
        }
        for row in &file_column_stats {
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
                DeleteFileCommitOrders {
                    begin: row.validity.begin_order,
                    timeline: timeline_order,
                    table_change: placeholder,
                },
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
        for row in &partition_values {
            remove_cached_file_partition_values(self, catalog, row.data_file_id);
        }
        let mut stats_file_ids = BTreeSet::new();
        for row in &file_column_stats {
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
