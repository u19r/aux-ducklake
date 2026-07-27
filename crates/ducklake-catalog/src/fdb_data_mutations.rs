use crate::fdb_inline_tables::{InlineTableDeletePayload, InlineTablePayload};
use crate::{
    CommitAttemptId, DataFileId, DataFileRow, DeleteFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow, InlineTableFlush,
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    maintenance::ScheduledDataFileCleanupKind, snapshot_operations::SnapshotOperationKind,
};

const MAX_MUTATION_COMMIT_RETRIES: usize = 3;

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

#[derive(Debug, Clone, Default)]
pub struct FdbDataMutation {
    pub data_files: Vec<DataFileRow>,
    pub delete_files: Vec<DeleteFileRow>,
    pub inline_flushes: Vec<InlineTableFlush>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub inline_file_deletions: Vec<InlineFileDeletionRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_file_ids: Vec<DataFileId>,
}

impl FdbDataMutation {
    #[must_use]
    pub fn new(
        data_files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        inline_flushes: Vec<InlineTableFlush>,
        partition_values: Vec<FilePartitionValueRow>,
        dropped_data_file_ids: Vec<DataFileId>,
    ) -> Self {
        Self {
            data_files,
            delete_files,
            inline_flushes,
            partition_values,
            dropped_data_file_ids,
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.data_files.is_empty()
            && self.delete_files.is_empty()
            && self.inline_flushes.is_empty()
            && self.partition_values.is_empty()
            && self.inline_file_deletions.is_empty()
            && self.file_column_stats.is_empty()
            && self.dropped_data_file_ids.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FdbMutationReadContext {
    pub(crate) order: Option<crate::CatalogOrderId>,
    pub(crate) data_files: Vec<DataFileRow>,
    pub(crate) append_partitions: Vec<FdbAppendPartitionExpectation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FdbAppendPartitionExpectation {
    pub(crate) data_file_id: DataFileId,
    pub(crate) table_id: crate::TableId,
    pub(crate) partition_table_id: Option<crate::TableId>,
    pub(crate) partition_id: Option<u64>,
    pub(crate) value_count: usize,
}

impl FdbAppendPartitionExpectation {
    pub(crate) fn matches_current_table(self, table: &crate::TableRow) -> bool {
        if table.table_id != self.table_id {
            return false;
        }
        match &table.partition {
            Some(partition) => {
                self.partition_table_id == Some(self.table_id)
                    && self.partition_id == Some(partition.partition_id)
                    && self.value_count == partition.fields.len()
            }
            None => {
                self.partition_table_id.is_none()
                    && self.partition_id.is_none()
                    && self.value_count == 0
            }
        }
    }
}

#[derive(Debug, Clone)]
struct FdbMutationPlan {
    attempt: FdbMutationAttempt,
    commit_metadata: crate::SnapshotCommitMetadata,
    mutation: FdbDataMutation,
    expired_delete_files: Vec<FdbExpiredDeleteFile>,
    snapshot_operations: Vec<(SnapshotOperationKind, crate::TableId)>,
    row_id_overlap_policy: RowIdOverlapPolicy,
    expired_object_cleanup_policy: ExpiredObjectCleanupPolicy,
    read_context: FdbMutationReadContext,
    inline_mutation: FdbInlineMutation,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct FdbMutationAttempt {
    pub(crate) proposed_snapshot: Option<CommitAttemptId>,
    pub(crate) recovery: Option<CommitAttemptId>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FdbInlineMutation {
    pub(crate) tables: Vec<crate::TableRow>,
    pub(crate) payloads: Vec<InlineTablePayload>,
    pub(crate) deletes: Vec<InlineTableDeletePayload>,
}

impl FdbInlineMutation {
    fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.payloads.is_empty() && self.deletes.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct FdbCompactionMutation {
    pub data_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_files: Vec<DataFileRow>,
}

#[derive(Default)]
pub(crate) struct FdbRewriteDeleteMutation {
    pub data_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub inline_file_deletions: Vec<InlineFileDeletionRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_files: Vec<DataFileRow>,
    pub expired_delete_files: Vec<FdbExpiredDeleteFile>,
    pub table_ids: Vec<crate::TableId>,
}

mod commit;
mod entrypoints;
mod recovery;
mod sizing;
mod validation;

use recovery::*;
use sizing::*;
use validation::*;
#[cfg(test)]
#[path = "fdb_data_mutations_tests.rs"]
mod tests;
