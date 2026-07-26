use crate::runtime_metrics::RuntimeMetricStage;
use crate::{
    CatalogError, CatalogResult, CommitAttemptId, DuckLakeSnapshotId, SnapshotCommitMetadata,
    runtime_object_ops::{CHANGE_VIEW_COMMENT, CREATE_VIEWS, DROP_VIEWS, RENAME_VIEWS},
    runtime_schema_change_payload::{
        ADD_COLUMNS, CHANGE_COLUMN_DEFAULTS, CHANGE_COLUMN_TYPES, CHANGE_COMMENTS,
        CHANGE_PARTITION_KEYS, CHANGE_SORT_KEYS, DROP_COLUMNS, RENAME_COLUMNS, RENAME_TABLES,
    },
    runtime_schema_ops::{CREATE_SCHEMAS, DROP_SCHEMAS},
    runtime_snapshot_range::ProposedCommitSnapshot,
    runtime_table_ops::{CREATE_TABLES, REPLACE_TABLES},
};

const COMMIT_ATTEMPT: &str = "CommitAttempt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommitAttemptIntent {
    pub(crate) read_snapshot: Option<DuckLakeSnapshotId>,
    pub(crate) proposed_commit_snapshot: ProposedCommitSnapshot,
    pub(crate) recovery_attempt_id: Option<CommitAttemptId>,
    pub(crate) commit_metadata: SnapshotCommitMetadata,
    pub(crate) metadata_intents: Vec<RuntimeMetadataIntent>,
    pub(crate) compaction_intents: Vec<RuntimeCompactionIntent>,
    pub(crate) data_mutation_payload: Vec<u8>,
    pub(crate) inline_payloads: Vec<RuntimeInlineIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeMetadataIntent {
    pub(crate) operation: RuntimeMetadataOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeMetadataOperation {
    CreateSchemas,
    DropSchemas,
    CreateTables,
    ReplaceTables,
    CreateViews,
    DropViews,
    RenameViews,
    AddColumns,
    RenameColumns,
    RenameTables,
    ChangeColumnDefaults,
    ChangeColumnTypes,
    ChangeComments,
    ChangeViewComment,
    DropColumns,
    ChangeSortKeys,
    ChangePartitionKeys,
}

impl RuntimeMetadataOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            CREATE_SCHEMAS => Ok(Self::CreateSchemas),
            DROP_SCHEMAS => Ok(Self::DropSchemas),
            CREATE_TABLES => Ok(Self::CreateTables),
            REPLACE_TABLES => Ok(Self::ReplaceTables),
            CREATE_VIEWS => Ok(Self::CreateViews),
            DROP_VIEWS => Ok(Self::DropViews),
            RENAME_VIEWS => Ok(Self::RenameViews),
            ADD_COLUMNS => Ok(Self::AddColumns),
            RENAME_COLUMNS => Ok(Self::RenameColumns),
            RENAME_TABLES => Ok(Self::RenameTables),
            CHANGE_COLUMN_DEFAULTS => Ok(Self::ChangeColumnDefaults),
            CHANGE_COLUMN_TYPES => Ok(Self::ChangeColumnTypes),
            CHANGE_COMMENTS => Ok(Self::ChangeComments),
            CHANGE_VIEW_COMMENT => Ok(Self::ChangeViewComment),
            DROP_COLUMNS => Ok(Self::DropColumns),
            CHANGE_SORT_KEYS => Ok(Self::ChangeSortKeys),
            CHANGE_PARTITION_KEYS => Ok(Self::ChangePartitionKeys),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support metadata operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CreateSchemas => CREATE_SCHEMAS,
            Self::DropSchemas => DROP_SCHEMAS,
            Self::CreateTables => CREATE_TABLES,
            Self::ReplaceTables => REPLACE_TABLES,
            Self::CreateViews => CREATE_VIEWS,
            Self::DropViews => DROP_VIEWS,
            Self::RenameViews => RENAME_VIEWS,
            Self::AddColumns => ADD_COLUMNS,
            Self::RenameColumns => RENAME_COLUMNS,
            Self::RenameTables => RENAME_TABLES,
            Self::ChangeColumnDefaults => CHANGE_COLUMN_DEFAULTS,
            Self::ChangeColumnTypes => CHANGE_COLUMN_TYPES,
            Self::ChangeComments => CHANGE_COMMENTS,
            Self::ChangeViewComment => CHANGE_VIEW_COMMENT,
            Self::DropColumns => DROP_COLUMNS,
            Self::ChangeSortKeys => CHANGE_SORT_KEYS,
            Self::ChangePartitionKeys => CHANGE_PARTITION_KEYS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeInlineIntent {
    pub(crate) operation: RuntimeInlineOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeInlineOperation {
    RegisterInlineTables,
    RegisterInlineRows,
    DeleteInlineRows,
}

impl RuntimeInlineOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            "RegisterInlineTables" => Ok(Self::RegisterInlineTables),
            "RegisterInlineRows" => Ok(Self::RegisterInlineRows),
            "DeleteInlineRows" => Ok(Self::DeleteInlineRows),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support inline operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::RegisterInlineTables => "RegisterInlineTables",
            Self::RegisterInlineRows => "RegisterInlineRows",
            Self::DeleteInlineRows => "DeleteInlineRows",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCompactionIntent {
    pub(crate) operation: RuntimeCompactionOperation,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeCompactionOperation {
    MergeAdjacentFiles,
    RewriteDeleteFiles,
}

impl RuntimeCompactionOperation {
    fn parse(operation: &str) -> CatalogResult<Self> {
        match operation {
            "MergeAdjacentFiles" => Ok(Self::MergeAdjacentFiles),
            "RewriteDeleteFiles" => Ok(Self::RewriteDeleteFiles),
            _ => Err(CatalogError::InvalidMutation(format!(
                "CommitAttempt does not support compaction operation {operation}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::MergeAdjacentFiles => "MergeAdjacentFiles",
            Self::RewriteDeleteFiles => "RewriteDeleteFiles",
        }
    }
}

mod metadata_conflicts;
mod orchestration;
mod parsing;
mod payload_remap;
mod table_assembly;
pub(crate) mod table_commit;

use metadata_conflicts::*;
pub(crate) use orchestration::*;
pub(crate) use parsing::*;
use payload_remap::*;
use table_assembly::*;
use table_commit::*;
#[cfg(test)]
#[path = "runtime_commit_attempt_ops_tests.rs"]
mod runtime_commit_attempt_ops_tests;
