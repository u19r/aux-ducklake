use std::collections::BTreeMap;

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{
    RuntimeMetricStatus, record_runtime_method_elapsed, record_runtime_request,
};
use crate::{
    CatalogId, CatalogResult, TableId,
    runtime_compaction_ops::commit_compaction_intent,
    runtime_data_mutation_ops::commit_data_mutation,
    runtime_inline_ops::{delete_inline_rows, register_inline_rows, register_inline_tables},
    runtime_protocol::RuntimeCatalogBackend,
    runtime_schema_change_ops::{RuntimeMutableCatalog, open_runtime_catalog},
};

use crate::runtime_commit_attempt_ops::*;
pub(crate) fn commit_attempt(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let started = RuntimeMetricStage::start();
    let intent = commit_attempt_intent(payload)?;
    record_commit_attempt_stage("ParseIntent", started);
    let started = RuntimeMetricStage::start();
    let mut kv = open_runtime_catalog(backend)?;
    let current = current_catalog_state(&kv, catalog)?;
    record_commit_attempt_stage("OpenForMetadata", started);
    let started = RuntimeMetricStage::start();
    let metadata = commit_metadata_intents(&mut kv, catalog, &intent, &current)?;
    record_commit_attempt_stage("MetadataIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
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
    let started = RuntimeMetricStage::start();
    let inline_output_bytes = commit_inline_intents(backend, catalog, &intent, &table_id_remaps)?;
    record_commit_attempt_stage("InlineIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let started = RuntimeMetricStage::start();
    let compaction_output_bytes =
        commit_compaction_intents(backend, catalog, &intent, &table_id_remaps)?;
    record_commit_attempt_stage("CompactionIntents", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let started = RuntimeMetricStage::start();
    let data_mutation_payload_bytes =
        commit_data_mutation_intent(backend, catalog, &intent, &table_id_remaps)?
            .map_or(0, |payload| payload.len());
    record_commit_attempt_stage("DataMutationIntent", started);
    crate::store::invalidate_runtime_read_context(catalog);
    let mut output = format!(
        "commit_attempt_intent=true\nducklake_schema_version={schema_version}\nmetadata_intent_count={}\ninline_intent_count={}\ncompaction_intent_count={}\ndata_mutation_payload_bytes={}\ninline_output_bytes={inline_output_bytes}\ncompaction_output_bytes={compaction_output_bytes}\nchanged_table_count={}\ncreated_table_count={}\n",
        intent.metadata_intents.len(),
        intent.inline_payloads.len(),
        intent.compaction_intents.len(),
        data_mutation_payload_bytes,
        metadata.changed_table_count,
        metadata.created_tables.len(),
    );
    for table in metadata.created_tables {
        output.push_str(&format!(
            "created_table\t{}\t{}\t{}\t{}\n",
            table.requested_table_id.0,
            table.persisted.table_id.0,
            table.persisted.schema_id.0,
            table.persisted.name
        ));
    }
    record_commit_attempt_child_metrics(backend, &intent)?;
    Ok(output.into_bytes())
}

#[cfg(feature = "runtime-metrics")]
pub(super) fn record_commit_attempt_stage(stage: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(
        &format!("method.runtime_commit_attempt.{stage}"),
        started.elapsed_micros(),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
pub(super) fn record_commit_attempt_stage(_stage: &str, _started: RuntimeMetricStage) {}

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
    commit_data_mutation(backend, catalog, &payload).map(Some)
}

pub(super) fn commit_inline_intents(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    intent: &RuntimeCommitAttemptIntent,
    table_id_remaps: &BTreeMap<TableId, TableId>,
) -> CatalogResult<usize> {
    let mut output_bytes = 0;
    for inline in &intent.inline_payloads {
        let operation = inline.operation.name();
        let inline_payload =
            remap_inline_payload(inline.operation, &inline.payload, table_id_remaps)?;
        let payload = payload_with_commit_header(
            intent,
            operation,
            &inline_payload,
            include_read_snapshot_for_storage_intents(intent),
            true,
        )?;
        let output = match inline.operation {
            RuntimeInlineOperation::RegisterInlineTables => {
                register_inline_tables(backend, catalog, &payload)?
            }
            RuntimeInlineOperation::RegisterInlineRows => {
                register_inline_rows(backend, catalog, &payload)?
            }
            RuntimeInlineOperation::DeleteInlineRows => {
                delete_inline_rows(backend, catalog, &payload)?
            }
        };
        output_bytes += output.len();
    }
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
    let current = current_catalog_state(kv, catalog)?;
    commit_metadata_intents_with_current_kv(kv, catalog, intent, &current)
}
