use crate::{
    CatalogId, CatalogResult, DataFileRow, KvBatch, MutableCatalogKv,
    conflict::{
        CommitAttemptDecision, DataCommitIntent, reject_conflicts_since_base, stage_commit_attempt,
    },
    data_file_store::stage_append_data_file,
    ids::{CatalogOrderId, CommitAttemptId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendCommitResult {
    Committed(DataFileRow),
    AlreadyCommitted { commit_order: CatalogOrderId },
}

pub fn commit_append_data_file(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    attempt_id: CommitAttemptId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    row: DataFileRow,
) -> CatalogResult<AppendCommitResult> {
    let mut batch = KvBatch::new();
    match stage_commit_attempt(
        kv,
        &mut batch,
        catalog,
        attempt_id,
        row.validity.begin_order,
    )? {
        CommitAttemptDecision::AlreadyCommitted(attempt) => {
            return Ok(AppendCommitResult::AlreadyCommitted {
                commit_order: attempt.commit_order,
            });
        }
        CommitAttemptDecision::FirstCommit(_) => {}
    }

    reject_conflicts_since_base(
        kv,
        catalog,
        row.table_id,
        base_order,
        through_order,
        DataCommitIntent::AppendFiles,
    )?;

    stage_append_data_file(kv, &mut batch, catalog, &row)?;
    kv.commit(batch)?;
    Ok(AppendCommitResult::Committed(row))
}
