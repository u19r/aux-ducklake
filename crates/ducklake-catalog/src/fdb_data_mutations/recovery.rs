use crate::{
    CatalogError, CommitAttemptId, FoundationDbErrorClass,
    snapshot_operations::SnapshotOperationKind,
};

use crate::fdb_data_mutations::*;
pub(super) fn mutation_recovery_attempt_id(
    snapshot_id: Option<CommitAttemptId>,
    commit_metadata: &crate::SnapshotCommitMetadata,
    mutation: &FdbDataMutation,
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
    for row in &mutation.data_files {
        hash.write_tag(2);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in &mutation.delete_files {
        hash.write_tag(3);
        hash.write_bytes_with_len(&row.encode());
    }
    for flush in &mutation.inline_flushes {
        hash.write_tag(4);
        hash.write_u64(flush.table_id.0);
        hash.write_u64(flush.schema_id.0);
        hash.write_u64(flush.flush_snapshot_sequence.0);
    }
    for row in &mutation.partition_values {
        hash.write_tag(5);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in &mutation.inline_file_deletions {
        hash.write_tag(6);
        hash.write_bytes_with_len(&row.encode());
    }
    for row in &mutation.file_column_stats {
        hash.write_tag(7);
        hash.write_bytes_with_len(&row.encode());
    }
    for id in &mutation.dropped_data_file_ids {
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

pub(super) fn snapshot_operation_hash_tag(kind: SnapshotOperationKind) -> u8 {
    match kind {
        SnapshotOperationKind::RewriteDelete => 1,
    }
}

pub(super) struct Fnv1a64(u64);

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

pub(super) fn mutation_failure_action(
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

pub(super) fn mutation_final_error(
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
