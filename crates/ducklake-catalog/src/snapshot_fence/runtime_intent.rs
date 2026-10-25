use crate::{
    CatalogError, CatalogResult,
    runtime_commit_attempt_ops::{RuntimeCommitAttemptIntent, RuntimeInlineOperation},
};

impl RuntimeCommitAttemptIntent {
    pub(crate) fn validate_snapshot_fence(&self) -> CatalogResult<()> {
        if self.snapshot_fence.is_none() {
            return Ok(());
        }
        if !self.metadata_intents.is_empty()
            || !self.compaction_intents.is_empty()
            || self.inline_payloads.iter().any(|intent| {
                matches!(
                    intent.operation,
                    RuntimeInlineOperation::RegisterInlineTables
                )
            })
        {
            return Err(CatalogError::InvalidMutation(
                "snapshot-fenced transactions permit only atomic data and inline row mutations"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}
