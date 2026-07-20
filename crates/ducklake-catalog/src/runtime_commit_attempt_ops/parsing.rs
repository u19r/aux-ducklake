use crate::{
    CatalogError, CatalogResult, CommitAttemptId, DuckLakeSnapshotId, SnapshotCommitMetadata,
    runtime_data_mutation_ops::{
        data_mutation_payload_values, proposed_commit_snapshot_covering_inline_flushes,
    },
    runtime_snapshot_range::ProposedCommitSnapshot,
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
};

use crate::runtime_commit_attempt_ops::*;
pub(crate) fn commit_attempt_intent(payload: &[u8]) -> CatalogResult<RuntimeCommitAttemptIntent> {
    let mut read_snapshot = None;
    let mut proposed_commit_snapshot = None;
    let mut metadata_intents = Vec::new();
    let mut compaction_intents = Vec::new();
    let mut data_mutation_payload = Vec::new();
    let mut inline_payloads = Vec::new();
    let mut commit_metadata = SnapshotCommitMetadata::default();
    let mut current_section: Option<RuntimeCommitAttemptSection> = None;

    for row in TabularPayload::new(COMMIT_ATTEMPT, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["read_snapshot", snapshot_id] => {
                read_snapshot = Some(DuckLakeSnapshotId(parse_u64_field(
                    COMMIT_ATTEMPT,
                    snapshot_id,
                    "read snapshot id",
                )?));
            }
            ["commit_snapshot", snapshot_id] => {
                proposed_commit_snapshot = Some(ProposedCommitSnapshot::new(CommitAttemptId(
                    parse_u64_field(COMMIT_ATTEMPT, snapshot_id, "commit snapshot id")?.into(),
                )));
            }
            ["commit_author", author] => {
                commit_metadata.author = Some((*author).to_owned());
            }
            ["commit_message", message] => {
                commit_metadata.commit_message = Some((*message).to_owned());
            }
            ["commit_extra_info", extra_info] => {
                commit_metadata.commit_extra_info = Some((*extra_info).to_owned());
            }
            ["metadata", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::metadata(operation)?);
            }
            ["data_mutation"] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::data_mutation());
            }
            ["inline", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::inline(operation)?);
            }
            ["compaction", operation] => {
                finish_section(
                    current_section.take(),
                    &mut metadata_intents,
                    &mut compaction_intents,
                    &mut data_mutation_payload,
                    &mut inline_payloads,
                )?;
                current_section = Some(RuntimeCommitAttemptSection::compaction(operation)?);
            }
            _ => {
                let Some(section) = current_section.as_mut() else {
                    return Err(CatalogError::Decode(format!(
                        "{COMMIT_ATTEMPT} payload row appears before an intent section"
                    )));
                };
                section.push_tabular_row(fields.to_vec());
            }
        }
    }
    finish_section(
        current_section,
        &mut metadata_intents,
        &mut compaction_intents,
        &mut data_mutation_payload,
        &mut inline_payloads,
    )?;
    let mut proposed_commit_snapshot = proposed_commit_snapshot.ok_or_else(|| {
        CatalogError::Decode(format!(
            "{COMMIT_ATTEMPT} payload requires commit_snapshot row"
        ))
    })?;
    if !data_mutation_payload.is_empty() {
        let data_mutation = data_mutation_payload_values(&data_mutation_payload)?;
        proposed_commit_snapshot = proposed_commit_snapshot_covering_inline_flushes(
            Some(proposed_commit_snapshot),
            &data_mutation.inline_flushes,
        )
        .ok_or_else(|| {
            CatalogError::Decode(
                "CommitAttempt proposed snapshot resolution unexpectedly returned no snapshot"
                    .to_owned(),
            )
        })?;
    }
    Ok(RuntimeCommitAttemptIntent {
        read_snapshot,
        proposed_commit_snapshot,
        commit_metadata,
        metadata_intents,
        compaction_intents,
        data_mutation_payload,
        inline_payloads,
    })
}

pub(super) enum RuntimeCommitAttemptSection {
    Metadata {
        operation: RuntimeMetadataOperation,
        payload: Vec<u8>,
    },
    Compaction {
        operation: RuntimeCompactionOperation,
        payload: Vec<u8>,
    },
    DataMutation {
        payload: Vec<u8>,
    },
    Inline {
        operation: RuntimeInlineOperation,
        payload: Vec<u8>,
    },
}

impl RuntimeCommitAttemptSection {
    fn metadata(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Metadata {
            operation: RuntimeMetadataOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn data_mutation() -> Self {
        Self::DataMutation {
            payload: Vec::new(),
        }
    }

    fn compaction(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Compaction {
            operation: RuntimeCompactionOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn inline(operation: &str) -> CatalogResult<Self> {
        Ok(Self::Inline {
            operation: RuntimeInlineOperation::parse(operation)?,
            payload: Vec::new(),
        })
    }

    fn push_tabular_row(&mut self, fields: Vec<&str>) {
        let payload = match self {
            Self::Metadata { payload, .. }
            | Self::Compaction { payload, .. }
            | Self::DataMutation { payload }
            | Self::Inline { payload, .. } => payload,
        };
        if !payload.is_empty() {
            payload.push(b'\n');
        }
        payload.extend(fields.join("\t").as_bytes());
    }
}

pub(super) fn finish_section(
    section: Option<RuntimeCommitAttemptSection>,
    metadata_intents: &mut Vec<RuntimeMetadataIntent>,
    compaction_intents: &mut Vec<RuntimeCompactionIntent>,
    data_mutation_payload: &mut Vec<u8>,
    inline_payloads: &mut Vec<RuntimeInlineIntent>,
) -> CatalogResult<()> {
    match section {
        None => Ok(()),
        Some(RuntimeCommitAttemptSection::Metadata { operation, payload }) => {
            metadata_intents.push(RuntimeMetadataIntent { operation, payload });
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::Compaction { operation, payload }) => {
            compaction_intents.push(RuntimeCompactionIntent { operation, payload });
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::DataMutation { payload }) => {
            if !data_mutation_payload.is_empty() {
                return Err(CatalogError::Decode(
                    "CommitAttempt payload contains multiple data_mutation sections".to_owned(),
                ));
            }
            *data_mutation_payload = payload;
            Ok(())
        }
        Some(RuntimeCommitAttemptSection::Inline { operation, payload }) => {
            inline_payloads.push(RuntimeInlineIntent { operation, payload });
            Ok(())
        }
    }
}
