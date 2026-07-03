use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    AppendCommitResult, CatalogError, CatalogResult, CommitAttemptRow, DataCommitIntent,
    DataFileChangeKind, DataFileRow, FdbOrderedCatalogKv, FoundationDbErrorClass, ValidityWindow,
    conflict::{commit_attempt_key, load_commit_attempt, reject_conflicts_since_base},
    fdb_runtime::{classify_fdb_error, map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, data_file_begin_key_order_offset,
        estimate_versionstamped_append_commit_bytes, incomplete_order,
        snapshot_data_file_change_key_order_offset, table_data_file_change_key_order_offset,
        versionstamped_value,
    },
    keys::{
        current_data_file_key, data_file_begin_key, data_file_key, snapshot_data_file_change_key,
        table_data_file_change_key,
    },
};

const MAX_APPEND_COMMIT_RETRIES: usize = 3;

impl FdbOrderedCatalogKv {
    pub fn commit_append_data_file_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: crate::CommitAttemptId,
        base_order: crate::CatalogOrderId,
        through_order: crate::CatalogOrderId,
        row: DataFileRow,
    ) -> CatalogResult<AppendCommitResult> {
        let mut last_retry_error = None;
        for attempt_index in 0..=MAX_APPEND_COMMIT_RETRIES {
            match self.try_commit_append_data_file_versionstamped(
                catalog,
                attempt_id,
                base_order,
                through_order,
                row.clone(),
                attempt_index,
            )? {
                AppendCommitAttempt::Done(result) => return Ok(result),
                AppendCommitAttempt::Retry(error) => last_retry_error = Some(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb append commit retry loop did not run".to_owned(),
            )
        }))
    }

    fn try_commit_append_data_file_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: crate::CommitAttemptId,
        base_order: crate::CatalogOrderId,
        through_order: crate::CatalogOrderId,
        mut row: DataFileRow,
        attempt_index: usize,
    ) -> CatalogResult<AppendCommitAttempt> {
        let placeholder = incomplete_order();
        row.validity = ValidityWindow::new(placeholder, None);
        let attempt = CommitAttemptRow::new(attempt_id, placeholder);
        let estimated_bytes =
            estimate_versionstamped_append_commit_bytes(catalog, attempt_id, &attempt, &row);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped append commit is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        let attempt_key = self.namespaced_key(&commit_attempt_key(catalog, attempt_id));
        if let Some(bytes) = block_on(trx.get(&attempt_key, false)).map_err(map_fdb_error)? {
            let attempt = CommitAttemptRow::decode(bytes.deref())?;
            return Ok(AppendCommitAttempt::Done(
                AppendCommitResult::AlreadyCommitted {
                    commit_order: attempt.commit_order,
                },
            ));
        }

        reject_conflicts_since_base(
            self,
            catalog,
            row.table_id,
            base_order,
            through_order,
            DataCommitIntent::AppendFiles,
        )?;

        trx.atomic_op(
            &attempt_key,
            &versionstamped_value(
                &attempt.encode(),
                CommitAttemptRow::COMMIT_ORDER_BYTES_OFFSET,
            )?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.namespaced_key(&current_data_file_key(
                catalog,
                row.table_id,
                row.data_file_id,
            )),
            &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.namespaced_key(&data_file_key(catalog, row.data_file_id)),
            &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &data_file_begin_key(catalog, row.table_id, placeholder, row.data_file_id),
                data_file_begin_key_order_offset(catalog, row.table_id),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &table_data_file_change_key(
                    catalog,
                    row.table_id,
                    placeholder,
                    DataFileChangeKind::Added,
                    row.data_file_id,
                ),
                table_data_file_change_key_order_offset(catalog, row.table_id),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_data_file_change_key(
                    catalog,
                    row.table_id,
                    placeholder,
                    DataFileChangeKind::Added,
                    row.data_file_id,
                ),
                snapshot_data_file_change_key_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        let versionstamp = trx.get_versionstamp();
        if let Err(error) = block_on(trx.commit()) {
            let error_class = classify_fdb_error(*error);
            match append_commit_failure_action(
                error_class,
                attempt_index,
                MAX_APPEND_COMMIT_RETRIES,
            ) {
                AppendCommitFailureAction::RecoverMaybeCommitted => {
                    if let Some(result) = recover_maybe_committed_append(self, catalog, attempt_id)?
                    {
                        return Ok(AppendCommitAttempt::Done(result));
                    }
                }
                AppendCommitFailureAction::Retry => {
                    return Ok(AppendCommitAttempt::Retry(map_fdb_commit_error(error)));
                }
                AppendCommitFailureAction::ReturnError => {}
            }
            let catalog_error = map_fdb_commit_error(error);
            return Err(append_commit_final_error(
                catalog_error,
                error_class,
                attempt_index,
                MAX_APPEND_COMMIT_RETRIES,
            ));
        }
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        row.validity = ValidityWindow::new(order, None);
        Ok(AppendCommitAttempt::Done(AppendCommitResult::Committed(
            row,
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AppendCommitAttempt {
    Done(AppendCommitResult),
    Retry(CatalogError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendCommitFailureAction {
    RecoverMaybeCommitted,
    Retry,
    ReturnError,
}

fn append_commit_failure_action(
    error_class: FoundationDbErrorClass,
    attempt_index: usize,
    max_retries: usize,
) -> AppendCommitFailureAction {
    match error_class {
        FoundationDbErrorClass::MaybeCommitted => AppendCommitFailureAction::RecoverMaybeCommitted,
        FoundationDbErrorClass::RetryableNotCommitted if attempt_index < max_retries => {
            AppendCommitFailureAction::Retry
        }
        FoundationDbErrorClass::RetryableNotCommitted
        | FoundationDbErrorClass::Retryable
        | FoundationDbErrorClass::NonRetryable => AppendCommitFailureAction::ReturnError,
    }
}

fn append_commit_final_error(
    error: CatalogError,
    error_class: FoundationDbErrorClass,
    attempt_index: usize,
    max_retries: usize,
) -> CatalogError {
    if error_class != FoundationDbErrorClass::RetryableNotCommitted || attempt_index < max_retries {
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
        operation: "append commit",
        attempts: attempt_index.saturating_add(1),
        code,
        message,
        class,
    }
}

fn recover_maybe_committed_append(
    kv: &impl crate::OrderedCatalogKv,
    catalog: crate::CatalogId,
    attempt_id: crate::CommitAttemptId,
) -> CatalogResult<Option<AppendCommitResult>> {
    Ok(
        load_commit_attempt(kv, catalog, attempt_id)?.map(|attempt| {
            AppendCommitResult::AlreadyCommitted {
                commit_order: attempt.commit_order,
            }
        }),
    )
}

#[cfg(test)]
#[path = "fdb_append_commits_tests.rs"]
mod fdb_append_commits_tests;
