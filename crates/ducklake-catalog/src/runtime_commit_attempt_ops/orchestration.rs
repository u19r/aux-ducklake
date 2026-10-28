use std::collections::BTreeMap;

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{RuntimeMetricStatus, record_runtime_request};
use crate::runtime_trace::RuntimeSpan;
use crate::{
    CatalogError, CatalogId, CatalogResult, TableId,
    runtime_commit_attempt_ops::*,
    runtime_compaction_ops::commit_compaction_intent,
    runtime_data_mutation_ops::commit_data_and_inline_mutation,
    runtime_inline_ops::{inline_delete_payload, inline_rows_payloads, register_inline_tables},
    runtime_mutable_catalog::{RuntimeMutableCatalog, open_runtime_catalog},
    runtime_protocol::RuntimeCatalogBackend,
};
pub(crate) fn commit_attempt(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
    snapshot_fence: Option<crate::SnapshotFence>,
) -> CatalogResult<Vec<u8>> {
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.ParseIntent");
    let mut intent = commit_attempt_intent(payload)?;
    intent.snapshot_fence = snapshot_fence;
    intent.validate_snapshot_fence()?;
    stage.finish(true);
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.OpenForMetadata");
    let mut kv = open_runtime_catalog(backend)?;
    let current = current_catalog_state(&kv, catalog, intent.recovery_attempt_id)?;
    stage.finish(true);
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.MetadataIntents");
    let metadata = commit_metadata_intents(&mut kv, catalog, &intent, &current)?;
    stage.finish(true);
    let schema_version = current.final_schema_version(metadata.public_schema_changed);
    let table_id_remaps = metadata.table_id_remaps();
    #[cfg_attr(
        not(feature = "foundationdb"),
        expect(
            clippy::drop_non_drop,
            reason = "release the catalog handle before later operations reopen it"
        )
    )]
    drop(kv);
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.PrepareInlineIntents");
    let pending_inline = prepare_inline_intents(backend, catalog, &intent, &table_id_remaps)?;
    stage.finish(true);
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.CompactionIntents");
    let compaction_output_bytes =
        commit_compaction_intents(backend, catalog, &intent, &table_id_remaps)?;
    stage.finish(true);
    let stage = RuntimeSpan::enter("method.runtime_commit_attempt.DataMutationIntent");
    let (data_mutation_payload_bytes, inline_output_bytes) =
        if intent.data_mutation_payload.is_empty() {
            (
                0,
                commit_pending_inline(backend, catalog, &intent, pending_inline)?,
            )
        } else {
            let inline_output_bytes = pending_inline.output_bytes();
            let payload = commit_data_mutation_intent(
                backend,
                catalog,
                &intent,
                &table_id_remaps,
                pending_inline,
            )?
            .ok_or(CatalogError::NotFound("data mutation payload"))?;
            (payload.len(), inline_output_bytes)
        };
    stage.finish(true);
    let mut output = format!(
        concat!(
            "commit_attempt_intent=true\n",
            "ducklake_schema_version={}\n",
            "metadata_intent_count={}\n",
            "inline_intent_count={}\n",
            "compaction_intent_count={}\n",
            "data_mutation_payload_bytes={}\n",
            "inline_output_bytes={}\n",
            "compaction_output_bytes={}\n",
            "changed_table_count={}\n",
            "created_table_count={}\n",
        ),
        schema_version,
        intent.metadata_intents.len(),
        intent.inline_payloads.len(),
        intent.compaction_intents.len(),
        data_mutation_payload_bytes,
        inline_output_bytes,
        compaction_output_bytes,
        metadata.changed_table_count,
        metadata.created_tables.len(),
    );
    for table in metadata.created_tables {
        output.push_str(&format!(
            "created_table\t{}\t{}\t{}\t{}\n",
            table.requested_table_id.0, table.table_id.0, table.schema_id.0, table.name
        ));
    }
    record_commit_attempt_child_metrics(backend, &intent)?;
    Ok(output.into_bytes())
}

#[cfg(feature = "runtime-metrics")]
pub(super) fn record_commit_attempt_child_metrics(
    backend: RuntimeCatalogBackend,
    intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<()> {
    for metadata in &intent.metadata_intents {
        record_runtime_request(backend, metadata.operation.name(), RuntimeMetricStatus::Ok);
    }
    for inline in &intent.inline_payloads {
        record_runtime_request(backend, inline.operation.name(), RuntimeMetricStatus::Ok);
    }
    for compaction in &intent.compaction_intents {
        record_runtime_request(
            backend,
            compaction.operation.name(),
            RuntimeMetricStatus::Ok,
        );
    }
    if !intent.data_mutation_payload.is_empty() {
        record_runtime_request(backend, "CommitDataMutation", RuntimeMetricStatus::Ok);
    }
    Ok(())
}

#[cfg(not(feature = "runtime-metrics"))]
pub(super) fn record_commit_attempt_child_metrics(
    _backend: RuntimeCatalogBackend,
    _intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<()> {
    Ok(())
}

pub(super) fn commit_compaction_intents(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<usize> {
    let mut output_bytes = 0;
    for compaction in &intent.compaction_intents {
        let operation = compaction.operation.name();
        let compaction_payload =
            remap_compaction_payload(compaction.operation, &compaction.payload, table_id_remaps)?;
        let output = commit_compaction_intent(
            backend,
            catalog,
            operation,
            &compaction_payload,
            intent.read_snapshot,
            intent.proposed_commit_snapshot,
            intent.commit_metadata.clone(),
        )?;
        output_bytes += output.len();
    }
    Ok(output_bytes)
}

pub(super) fn commit_data_mutation_intent(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
    pending_inline: PendingInlineMutations,
) -> CatalogResult<Option<Vec<u8>>> {
    if intent.data_mutation_payload.is_empty() {
        return Ok(None);
    }
    let data_mutation_payload =
        remap_data_mutation_payload(&intent.data_mutation_payload, table_id_remaps)?;
    let payload = payload_with_commit_header(
        intent,
        "CommitDataMutation",
        &data_mutation_payload,
        include_read_snapshot_for_storage_intents(intent),
        true,
    )?;
    let commit_snapshot = u64::try_from(intent.proposed_commit_snapshot.commit_attempt_id().0)
        .map_err(|_| CatalogError::Decode("commit snapshot exceeds u64".to_owned()))?;
    commit_data_and_inline_mutation(
        backend,
        catalog,
        &payload,
        pending_inline.rows,
        pending_inline.deletes,
        Some(crate::DuckLakeSnapshotId(commit_snapshot)),
    )
    .map(Some)
}

pub(super) struct PendingInlineMutations {
    rows: Vec<crate::runtime_inline_ops::RuntimeInlineRows>,
    deletes: Vec<crate::runtime_inline_ops::RuntimeInlineDelete>,
    registered_table_output_bytes: usize,
}

impl PendingInlineMutations {
    fn output_bytes(&self) -> usize {
        let mut output_bytes = self.registered_table_output_bytes;
        if !self.rows.is_empty() {
            output_bytes += format!("inline_chunk_count={}\n", self.rows.len()).len();
        }
        if !self.deletes.is_empty() {
            output_bytes += "deleted_inline_row_count=0\nrewritten_inline_payload_count=0\n".len();
        }
        output_bytes
    }
}

pub(super) fn prepare_inline_intents(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<PendingInlineMutations> {
    let mut output_bytes = 0;
    let mut row_requests = Vec::new();
    let mut delete_requests = Vec::new();
    for inline in &intent.inline_payloads {
        let operation = inline.operation.name();
        let inline_payload =
            remap_inline_payload(inline.operation, &inline.payload, table_id_remaps)?;
        let (include_read_snapshot, include_commit_metadata) = match inline.operation {
            RuntimeInlineOperation::RegisterInlineRows => {
                (include_read_snapshot_for_storage_intents(intent), true)
            }
            RuntimeInlineOperation::RegisterInlineTables
            | RuntimeInlineOperation::DeleteInlineRows => (false, false),
        };
        let payload = payload_with_commit_header(
            intent,
            operation,
            &inline_payload,
            include_read_snapshot,
            include_commit_metadata,
        )?;
        match inline.operation {
            RuntimeInlineOperation::RegisterInlineTables => {
                output_bytes += register_inline_tables(backend, catalog, &payload)?.len();
            }
            RuntimeInlineOperation::RegisterInlineRows => {
                row_requests.extend(inline_rows_payloads(&payload)?);
            }
            RuntimeInlineOperation::DeleteInlineRows => {
                delete_requests.push(inline_delete_payload(&payload)?);
            }
        }
    }
    Ok(PendingInlineMutations {
        rows: row_requests,
        deletes: delete_requests,
        registered_table_output_bytes: output_bytes,
    })
}

fn commit_pending_inline(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    pending: PendingInlineMutations,
) -> CatalogResult<usize> {
    let output_bytes = pending.output_bytes();
    if pending.rows.is_empty() && pending.deletes.is_empty() {
        return Ok(output_bytes);
    }
    let payload = payload_with_commit_header(
        intent,
        "CommitDataMutation",
        &[],
        include_read_snapshot_for_storage_intents(intent),
        true,
    )?;
    commit_data_and_inline_mutation(
        backend,
        catalog,
        &payload,
        pending.rows,
        pending.deletes,
        Some(crate::DuckLakeSnapshotId(commit_snapshot_u64(
            intent.proposed_commit_snapshot,
        )?)),
    )?;
    Ok(output_bytes)
}

pub(super) fn commit_metadata_intents(
    kv: &mut RuntimeMutableCatalog,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    current: &CurrentCatalogState,
) -> CatalogResult<CommitMetadataResult> {
    commit_metadata_intents_with_current_kv(kv, catalog, intent, current)
}

#[cfg(test)]
pub(super) fn commit_metadata_intents_with_kv(
    kv: &mut impl CommitAttemptTableReplacements,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
) -> CatalogResult<CommitMetadataResult> {
    let current = current_catalog_state(kv, catalog, intent.recovery_attempt_id)?;
    commit_metadata_intents_with_current_kv(kv, catalog, intent, &current)
}
