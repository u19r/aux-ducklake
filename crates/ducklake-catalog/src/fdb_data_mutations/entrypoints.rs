use crate::{
    CatalogResult, CommitAttemptId, DataMutationCommit, FdbOrderedCatalogKv,
    maintenance::ScheduledDataFileCleanupKind, snapshot_operations::SnapshotOperationKind,
};

use crate::fdb_data_mutations::*;
impl FdbOrderedCatalogKv {
    pub fn commit_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        mutation: FdbDataMutation,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_metadata(
            catalog,
            attempt_id,
            crate::SnapshotCommitMetadata::default(),
            mutation,
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_metadata(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        mutation: FdbDataMutation,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
            catalog,
            attempt_id,
            commit_metadata,
            mutation,
        )
    }

    pub fn commit_data_mutation_versionstamped_with_inline_file_deletions(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        mutation: FdbDataMutation,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
            catalog,
            attempt_id,
            crate::SnapshotCommitMetadata::default(),
            mutation,
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_inline_file_deletions_and_stats(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        mutation: FdbDataMutation,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_data_mutation_versionstamped_with_expired_delete_files(
            catalog,
            attempt_id,
            commit_metadata,
            mutation,
            Vec::new(),
        )
    }

    pub(crate) fn commit_data_mutation_versionstamped_with_expired_delete_files(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        mutation: FdbDataMutation,
        expired_delete_files: Vec<FdbExpiredDeleteFile>,
    ) -> CatalogResult<DataMutationCommit> {
        self.commit_planned_data_mutation(
            catalog,
            FdbMutationPlan {
                attempt_id,
                commit_metadata,
                mutation,
                expired_delete_files,
                snapshot_operations: Vec::new(),
                row_id_overlap_policy: RowIdOverlapPolicy::RejectCurrentOverlaps,
                expired_object_cleanup_policy: ExpiredObjectCleanupPolicy::Schedule(
                    ScheduledDataFileCleanupKind::UnreachableOnly,
                ),
                preloaded_data_files: Vec::new(),
            },
        )
    }

    pub(crate) fn commit_compaction_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        compaction: FdbCompactionMutation,
    ) -> CatalogResult<DataMutationCommit> {
        let dropped_data_file_ids = compaction
            .dropped_data_files
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>();
        self.commit_planned_data_mutation(
            catalog,
            FdbMutationPlan {
                attempt_id,
                commit_metadata,
                mutation: FdbDataMutation {
                    data_files: compaction.data_files,
                    partition_values: compaction.partition_values,
                    file_column_stats: compaction.file_column_stats,
                    dropped_data_file_ids,
                    ..FdbDataMutation::default()
                },
                expired_delete_files: Vec::new(),
                snapshot_operations: Vec::new(),
                row_id_overlap_policy: RowIdOverlapPolicy::TrustCompactionReplacementRows,
                expired_object_cleanup_policy: ExpiredObjectCleanupPolicy::Schedule(
                    ScheduledDataFileCleanupKind::CompactionReplacement,
                ),
                preloaded_data_files: compaction.dropped_data_files,
            },
        )
    }

    pub(crate) fn commit_rewrite_delete_data_mutation_versionstamped(
        &self,
        catalog: crate::CatalogId,
        attempt_id: Option<CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        rewrite: FdbRewriteDeleteMutation,
    ) -> CatalogResult<DataMutationCommit> {
        let dropped_data_file_ids = rewrite
            .dropped_data_files
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>();
        self.commit_planned_data_mutation(
            catalog,
            FdbMutationPlan {
                attempt_id,
                commit_metadata,
                mutation: FdbDataMutation {
                    data_files: rewrite.data_files,
                    partition_values: rewrite.partition_values,
                    inline_file_deletions: rewrite.inline_file_deletions,
                    file_column_stats: rewrite.file_column_stats,
                    dropped_data_file_ids,
                    ..FdbDataMutation::default()
                },
                expired_delete_files: rewrite.expired_delete_files,
                snapshot_operations: vec![(SnapshotOperationKind::RewriteDelete, rewrite.table_id)],
                row_id_overlap_policy: RowIdOverlapPolicy::TrustCompactionReplacementRows,
                expired_object_cleanup_policy: ExpiredObjectCleanupPolicy::Preserve,
                preloaded_data_files: rewrite.dropped_data_files,
            },
        )
    }
}
