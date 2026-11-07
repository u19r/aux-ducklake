use std::fmt;

use crate::{
    file_listing::DataFileChange,
    ids::{CatalogOrderId, CommitAttemptId, TableId},
};

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Backend(String),
    FoundationDb {
        code: i32,
        message: String,
        class: FoundationDbErrorClass,
    },
    FoundationDbRetryExhausted {
        operation: &'static str,
        attempts: usize,
        code: i32,
        message: String,
        class: FoundationDbErrorClass,
    },
    CommitAttemptOrderChanged {
        attempt_id: CommitAttemptId,
    },
    ConflictFenceChanged {
        fence: Vec<u8>,
    },
    Decode(String),
    InvalidMutation(String),
    InvalidKey(String),
    LogicalConflict {
        table_id: TableId,
        conflicting_changes: Vec<DataFileChange>,
    },
    TableLogicalConflict {
        table_id: TableId,
        dropped_at: CatalogOrderId,
    },
    TableSchemaConflict {
        table_id: TableId,
        changed_at: CatalogOrderId,
    },
    NotFound(&'static str),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(message) => write!(formatter, "backend failed: {message}"),
            Self::FoundationDb {
                code,
                message,
                class,
            } => write!(
                formatter,
                "foundationdb failed: code={code} class={class} message={message}"
            ),
            Self::FoundationDbRetryExhausted {
                operation,
                attempts,
                code,
                message,
                class,
            } => write!(
                formatter,
                "foundationdb retry budget exhausted for {operation} after {attempts} attempt(s): code={code} class={class} message={message}"
            ),
            Self::CommitAttemptOrderChanged { attempt_id } => {
                write!(
                    formatter,
                    "commit attempt {} already recorded a different order",
                    attempt_id.0
                )
            }
            Self::ConflictFenceChanged { fence } => {
                write!(formatter, "conflict fence changed: {}", hex(fence))
            }
            Self::Decode(message) => write!(formatter, "decode failed: {message}"),
            Self::InvalidMutation(message) => write!(formatter, "invalid mutation: {message}"),
            Self::InvalidKey(message) => write!(formatter, "invalid key: {message}"),
            Self::LogicalConflict {
                table_id,
                conflicting_changes,
            } => {
                write!(
                    formatter,
                    "logical conflict on table {} across {} change(s)",
                    table_id.0,
                    conflicting_changes.len()
                )?;
                for change in conflicting_changes {
                    write!(
                        formatter,
                        "; {:?} data_file={} at order={}",
                        change.kind, change.data_file_id.0, change.order
                    )?;
                }
                Ok(())
            }
            Self::TableLogicalConflict {
                table_id,
                dropped_at,
            } => write!(
                formatter,
                "logical conflict on table {}: table was dropped at order={}",
                table_id.0, dropped_at
            ),
            Self::TableSchemaConflict {
                table_id,
                changed_at,
            } => write!(
                formatter,
                "logical conflict on table {}: schema changed at order={}",
                table_id.0, changed_at
            ),
            Self::NotFound(name) => write!(formatter, "{name} not found"),
        }
    }
}

impl std::error::Error for CatalogError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoundationDbErrorClass {
    MaybeCommitted,
    RetryableNotCommitted,
    Retryable,
    NonRetryable,
}

impl fmt::Display for FoundationDbErrorClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaybeCommitted => write!(formatter, "maybe_committed"),
            Self::RetryableNotCommitted => write!(formatter, "retryable_not_committed"),
            Self::Retryable => write!(formatter, "retryable"),
            Self::NonRetryable => write!(formatter, "non_retryable"),
        }
    }
}

pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
