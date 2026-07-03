use crate::{CatalogOrderId, CommitAttemptId, DuckLakeSnapshotId};

#[derive(Clone, Copy)]
pub(crate) struct ReadSnapshot(DuckLakeSnapshotId);

impl ReadSnapshot {
    pub(crate) fn new(snapshot_id: DuckLakeSnapshotId) -> Self {
        Self(snapshot_id)
    }

    pub(crate) fn public_id(self) -> DuckLakeSnapshotId {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProposedCommitSnapshot(CommitAttemptId);

impl ProposedCommitSnapshot {
    pub(crate) fn new(snapshot_id: CommitAttemptId) -> Self {
        Self(snapshot_id)
    }

    pub(crate) fn commit_attempt_id(self) -> CommitAttemptId {
        self.0
    }
}

impl PartialEq<CommitAttemptId> for ProposedCommitSnapshot {
    fn eq(&self, other: &CommitAttemptId) -> bool {
        self.0 == *other
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SemanticDeleteCoverageBegin(DuckLakeSnapshotId);

impl SemanticDeleteCoverageBegin {
    pub(crate) fn new(snapshot_id: DuckLakeSnapshotId) -> Self {
        Self(snapshot_id)
    }

    pub(crate) fn public_id(self) -> DuckLakeSnapshotId {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotWatermarkCutoffOrder(CatalogOrderId);

impl SnapshotWatermarkCutoffOrder {
    pub(crate) fn new(order: CatalogOrderId) -> Self {
        Self(order)
    }

    pub(crate) fn catalog_order(self) -> CatalogOrderId {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotDataChangeOrder(CatalogOrderId);

impl SnapshotDataChangeOrder {
    pub(crate) fn new(order: CatalogOrderId) -> Self {
        Self(order)
    }

    pub(crate) fn catalog_order(self) -> CatalogOrderId {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ChangeFeedStartSnapshot(DuckLakeSnapshotId);

impl ChangeFeedStartSnapshot {
    pub(crate) fn new(snapshot_id: DuckLakeSnapshotId) -> Self {
        Self(snapshot_id)
    }

    pub(crate) fn public_id(self) -> DuckLakeSnapshotId {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ChangeFeedEndSnapshot(DuckLakeSnapshotId);

impl ChangeFeedEndSnapshot {
    pub(crate) fn new(snapshot_id: DuckLakeSnapshotId) -> Self {
        Self(snapshot_id)
    }

    pub(crate) fn public_id(self) -> DuckLakeSnapshotId {
        self.0
    }
}
