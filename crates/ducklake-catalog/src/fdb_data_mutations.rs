use crate::{
    CommitAttemptId, DataFileId, DataFileRow, DeleteFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow, InlineTableFlush,
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    maintenance::ScheduledDataFileCleanupKind, snapshot_operations::SnapshotOperationKind,
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

#[derive(Debug, Clone)]
struct FdbMutationPlan {
    attempt_id: Option<CommitAttemptId>,
    commit_metadata: crate::SnapshotCommitMetadata,
    mutation: FdbDataMutation,
    expired_delete_files: Vec<FdbExpiredDeleteFile>,
    snapshot_operations: Vec<(SnapshotOperationKind, crate::TableId)>,
    row_id_overlap_policy: RowIdOverlapPolicy,
    expired_object_cleanup_policy: ExpiredObjectCleanupPolicy,
    preloaded_data_files: Vec<DataFileRow>,
}

pub(crate) struct FdbCompactionMutation {
    pub data_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_files: Vec<DataFileRow>,
}

pub(crate) struct FdbRewriteDeleteMutation {
    pub data_files: Vec<DataFileRow>,
    pub partition_values: Vec<FilePartitionValueRow>,
    pub inline_file_deletions: Vec<InlineFileDeletionRow>,
    pub file_column_stats: Vec<FileColumnStatsRow>,
    pub dropped_data_files: Vec<DataFileRow>,
    pub expired_delete_files: Vec<FdbExpiredDeleteFile>,
    pub table_id: crate::TableId,
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
