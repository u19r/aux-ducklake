use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use foundationdb::{
    RangeOption,
    options::{ConflictRangeType, MutationType},
};
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DataFileRow, DuckLakeSnapshotId,
    FdbOrderedCatalogKv, InlineRowChangeKind, InlineTableChunkRow, InlineTableDeleteCommit,
    InlineTablePayloadCommit, KvBatch, OrderedCatalogKv, RawSnapshotSequence, SchemaId,
    SnapshotRow, TableId, TableRow, ValidityWindow,
    conflict_watermarks::{stage_fdb_max_catalog_id_watermark, stage_fdb_max_file_id_watermark},
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_tables::{
        stage_current_table_row, stage_table_visibility_begin, stage_table_visibility_end,
    },
    fdb_versionstamp::{
        committed_order, incomplete_order, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, table_object_key_order_offset, versionstamped_value,
    },
    inline_change_feed::{inline_payload_rows, stage_inline_row_changes_for_payload},
    inline_data::{
        InlineCurrentRow, inline_table_chunk_key, inline_table_chunks, inline_table_payload_prefix,
        validate_inline_table_rows_fit_fdb,
    },
    keys::{
        current_table_row_key, inline_current_row_key, inline_live_row_key, inline_next_row_id_key,
        inline_table_change_key, inline_table_change_prefix, prefix_end, snapshot_key,
        snapshot_prefix, snapshot_timestamp_key, table_inline_row_change_key,
        table_inline_row_change_prefix, table_object_key, table_schema_kind_inline_row_change_key,
        table_schema_kind_inline_row_change_prefix, table_visibility_key,
    },
    rows::STORED_ORDER_LEN,
    store::{latest_snapshot, stage_fdb_snapshot_indexes},
    table_store::load_current_table_row,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct InlineTableCommitContext<'a> {
    pub commit_snapshot: Option<DuckLakeSnapshotId>,
    pub read_snapshot: Option<DuckLakeSnapshotId>,
    pub commit_metadata: Option<&'a crate::SnapshotCommitMetadata>,
}

#[derive(Clone, Debug)]
pub(crate) struct InlineTablePayload {
    pub(crate) table_id: TableId,
    pub(crate) schema_id: SchemaId,
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct InlineTableDeletePayload {
    pub(crate) table_id: TableId,
    pub(crate) schema_id: SchemaId,
    pub(crate) row_ids: Vec<u64>,
}

pub(crate) struct InlineTableMutationCommit {
    pub(crate) rows: Vec<InlineTableChunkRow>,
}

impl FdbOrderedCatalogKv {
    pub fn register_inline_table_payload_versionstamped(
        &self,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        payload: Vec<u8>,
    ) -> CatalogResult<Vec<InlineTableChunkRow>> {
        commit_inline_table_payload(
            self,
            catalog,
            None,
            table_id,
            schema_id,
            payload,
            InlineTableCommitContext::default(),
        )
    }

    pub fn register_inline_table_payload_with_table_versionstamped(
        &self,
        catalog: CatalogId,
        table: TableRow,
        schema_id: SchemaId,
        payload: Vec<u8>,
    ) -> CatalogResult<Vec<InlineTableChunkRow>> {
        self.register_inline_table_payload_with_table_at_snapshot_versionstamped(
            catalog,
            table,
            schema_id,
            payload,
            InlineTableCommitContext::default(),
        )
    }

    pub fn register_inline_table_payload_with_table_at_snapshot_versionstamped(
        &self,
        catalog: CatalogId,
        table: TableRow,
        schema_id: SchemaId,
        payload: Vec<u8>,
        context: InlineTableCommitContext<'_>,
    ) -> CatalogResult<Vec<InlineTableChunkRow>> {
        let table_id = table.table_id;
        commit_inline_table_payloads(
            self,
            catalog,
            vec![table],
            vec![InlineTablePayload {
                table_id,
                schema_id,
                payload,
            }],
            context,
        )
    }

    pub(crate) fn commit_inline_table_mutations_at_snapshot_versionstamped(
        &self,
        catalog: CatalogId,
        tables: Vec<TableRow>,
        payloads: Vec<InlineTablePayload>,
        deletes: Vec<InlineTableDeletePayload>,
        context: InlineTableCommitContext<'_>,
    ) -> CatalogResult<InlineTableMutationCommit> {
        commit_inline_table_mutations(self, catalog, tables, payloads, deletes, context)
    }

    pub fn route_inline_table_payload_or_data_file_versionstamped(
        &self,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        payload: Vec<u8>,
        fallback_file: DataFileRow,
    ) -> CatalogResult<InlineTablePayloadCommit> {
        if fallback_file.table_id != table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "inline fallback file table {} does not match inline table {}",
                fallback_file.table_id.0, table_id.0
            )));
        }
        if validate_inline_table_rows_fit_fdb(&payload).is_ok() {
            return self
                .register_inline_table_payload_versionstamped(catalog, table_id, schema_id, payload)
                .map(InlineTablePayloadCommit::Inlined);
        }
        self.append_data_files_versionstamped(catalog, vec![fallback_file])
            .map(InlineTablePayloadCommit::FileBacked)
    }

    pub fn commit_delete_inline_table_rows_versionstamped(
        &self,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        deleted_row_ids: &[u64],
        commit_snapshot: Option<DuckLakeSnapshotId>,
    ) -> CatalogResult<InlineTableDeleteCommit> {
        commit_delete_inline_table_rows(
            self,
            catalog,
            table_id,
            schema_id,
            deleted_row_ids,
            commit_snapshot,
        )
    }
}

fn commit_inline_table_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table: Option<TableRow>,
    table_id: TableId,
    schema_id: SchemaId,
    payload: Vec<u8>,
    context: InlineTableCommitContext<'_>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    commit_inline_table_payloads(
        kv,
        catalog,
        table.into_iter().collect(),
        vec![InlineTablePayload {
            table_id,
            schema_id,
            payload,
        }],
        context,
    )
}

pub(crate) struct PreparedInlinePayload {
    table_id: TableId,
    schema_id: SchemaId,
    rows: Vec<InlineTableChunkRow>,
    row_changes: Vec<VersionstampedInlineChangeKey>,
    current_rows: BTreeMap<u64, Vec<u8>>,
}

pub(crate) struct PreparedInlineDelete {
    table_id: TableId,
    schema_id: SchemaId,
    row_ids: Vec<u64>,
}

fn prepare_inline_delete(
    live_rows: &BTreeMap<(TableId, SchemaId, u64), Option<RawSnapshotSequence>>,
    target_sequence: RawSnapshotSequence,
    delete: InlineTableDeletePayload,
) -> CatalogResult<PreparedInlineDelete> {
    let row_ids = delete
        .row_ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|row_id| {
            live_rows
                .get(&(delete.table_id, delete.schema_id, *row_id))
                .and_then(|sequence| *sequence)
                .is_some_and(|sequence| sequence < target_sequence)
        })
        .collect();
    Ok(PreparedInlineDelete {
        table_id: delete.table_id,
        schema_id: delete.schema_id,
        row_ids,
    })
}

fn commit_inline_table_payloads(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    tables: Vec<TableRow>,
    payloads: Vec<InlineTablePayload>,
    context: InlineTableCommitContext<'_>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    commit_inline_table_mutations(kv, catalog, tables, payloads, Vec::new(), context)
        .map(|commit| commit.rows)
}

fn commit_inline_table_mutations(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    tables: Vec<TableRow>,
    payloads: Vec<InlineTablePayload>,
    deletes: Vec<InlineTableDeletePayload>,
    context: InlineTableCommitContext<'_>,
) -> CatalogResult<InlineTableMutationCommit> {
    let prepared = prepare_inline_table_mutation(kv, catalog, tables, payloads, deletes, context)?;
    validate_prepared_inline_mutation_size(catalog, &prepared)?;
    let trx = kv.create_transaction()?;
    add_snapshot_prefix_conflict(kv, &trx, catalog)?;
    stage_snapshot(kv, &trx, catalog, &prepared.snapshot)?;
    stage_prepared_inline_mutation(kv, &trx, catalog, &prepared)?;
    commit_prepared_inline_mutation(trx, prepared)
}

pub(crate) struct PreparedInlineMutation {
    pub(crate) snapshot: SnapshotRow,
    pub(crate) replacements: Vec<(TableRow, TableRow)>,
    pub(crate) payloads: Vec<PreparedInlinePayload>,
    pub(crate) deletes: Vec<PreparedInlineDelete>,
}

pub(crate) fn prepare_inline_table_mutation(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    tables: Vec<TableRow>,
    payloads: Vec<InlineTablePayload>,
    deletes: Vec<InlineTableDeletePayload>,
    context: InlineTableCommitContext<'_>,
) -> CatalogResult<PreparedInlineMutation> {
    for payload in &payloads {
        validate_inline_table_rows_fit_fdb(&payload.payload)?;
    }
    let latest = latest_snapshot(kv, catalog)?;
    validate_inline_commit_snapshot(latest.as_ref(), context)?;
    let sequence = match (latest.as_ref(), context.commit_snapshot) {
        (_, Some(commit_snapshot)) => crate::RawSnapshotSequence(commit_snapshot.0),
        (Some(snapshot), None) => snapshot.sequence.next(),
        (None, None) => crate::RawSnapshotSequence::initial(),
    };
    let placeholder = incomplete_order();
    let snapshot = SnapshotRow::new(placeholder, sequence)
        .with_optional_commit_metadata(context.commit_metadata);
    let replacements = tables
        .into_iter()
        .map(|table| {
            prepare_table_replacement(kv, catalog, latest.as_ref(), placeholder, Some(table))
        })
        .collect::<CatalogResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let payloads = payloads
        .into_iter()
        .map(|payload| prepare_inline_payload(catalog, placeholder, payload))
        .collect::<CatalogResult<Vec<_>>>()?;
    let live_rows = load_inline_live_rows(kv, catalog, &payloads, &deletes)?;
    reject_existing_inline_row_ids(&payloads, &live_rows)?;
    let deletes = deletes
        .into_iter()
        .map(|delete| prepare_inline_delete(&live_rows, sequence, delete))
        .collect::<CatalogResult<Vec<_>>>()?;
    Ok(PreparedInlineMutation {
        snapshot,
        replacements,
        payloads,
        deletes,
    })
}

fn load_inline_live_rows(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    payloads: &[PreparedInlinePayload],
    deletes: &[InlineTableDeletePayload],
) -> CatalogResult<BTreeMap<(TableId, SchemaId, u64), Option<RawSnapshotSequence>>> {
    let mut identities = BTreeSet::new();
    for payload in payloads {
        for row_id in payload
            .row_changes
            .iter()
            .filter_map(|change| change.row_id)
        {
            if !identities.insert((payload.table_id, payload.schema_id, row_id)) {
                return Err(CatalogError::InvalidMutation(format!(
                    "inline row id {row_id} is inserted more than once for table {} schema {}",
                    payload.table_id.0, payload.schema_id.0
                )));
            }
        }
    }
    for delete in deletes {
        identities.extend(
            delete
                .row_ids
                .iter()
                .map(|row_id| (delete.table_id, delete.schema_id, *row_id)),
        );
    }
    let keys = identities
        .iter()
        .map(|(table_id, schema_id, row_id)| {
            inline_live_row_key(catalog, *table_id, *schema_id, *row_id)
        })
        .collect::<Vec<_>>();
    identities
        .into_iter()
        .zip(kv.batch_get(&keys)?)
        .map(|(identity, value)| {
            let sequence = value
                .map(|value| {
                    value
                        .as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map(RawSnapshotSequence)
                        .map_err(|_| {
                            CatalogError::Decode(
                                "inline live-row sequence must contain eight bytes".to_owned(),
                            )
                        })
                })
                .transpose()?;
            Ok((identity, sequence))
        })
        .collect()
}

fn reject_existing_inline_row_ids(
    payloads: &[PreparedInlinePayload],
    live_rows: &BTreeMap<(TableId, SchemaId, u64), Option<RawSnapshotSequence>>,
) -> CatalogResult<()> {
    for payload in payloads {
        for row_id in payload
            .row_changes
            .iter()
            .filter_map(|change| change.row_id)
        {
            if live_rows
                .get(&(payload.table_id, payload.schema_id, row_id))
                .and_then(|sequence| *sequence)
                .is_some()
            {
                return Err(CatalogError::InvalidMutation(format!(
                    "inline row id {row_id} is already live for table {} schema {}",
                    payload.table_id.0, payload.schema_id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_inline_commit_snapshot(
    latest: Option<&SnapshotRow>,
    context: InlineTableCommitContext<'_>,
) -> CatalogResult<()> {
    let Some(commit_snapshot) = context.commit_snapshot else {
        return Ok(());
    };
    let latest_sequence = latest
        .ok_or(CatalogError::NotFound("catalog snapshot"))?
        .sequence;
    let latest_commit = DuckLakeSnapshotId(latest_sequence.0);
    let next_commit = DuckLakeSnapshotId(latest_sequence.next().0);
    let valid = if context.read_snapshot.is_some() {
        commit_snapshot == next_commit
    } else {
        commit_snapshot == latest_commit || commit_snapshot == next_commit
    };
    if valid {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "conflict committing inline rows: proposed commit snapshot {} does not match latest \
         DuckLake snapshot {} or next DuckLake snapshot {}",
        commit_snapshot.0, latest_commit.0, next_commit.0
    )))
}

fn prepare_inline_payload(
    catalog: CatalogId,
    order: CatalogOrderId,
    payload: InlineTablePayload,
) -> CatalogResult<PreparedInlinePayload> {
    let current_rows = inline_payload_rows(&payload.payload)?.into_iter().collect();
    Ok(PreparedInlinePayload {
        table_id: payload.table_id,
        schema_id: payload.schema_id,
        rows: inline_table_chunks(
            payload.table_id,
            payload.schema_id,
            order,
            payload.payload.clone(),
        )?,
        row_changes: staged_inline_change_keys(
            catalog,
            payload.table_id,
            payload.schema_id,
            &payload.payload,
        )?,
        current_rows,
    })
}

pub(crate) fn prepared_inline_mutation_size(
    catalog: CatalogId,
    prepared: &PreparedInlineMutation,
) -> usize {
    estimate_inline_payload_bytes(
        catalog,
        &prepared.snapshot,
        &prepared.replacements,
        &prepared.payloads,
    )
    .saturating_add(
        prepared
            .deletes
            .iter()
            .map(|delete| {
                estimate_inline_delete_change_bytes(
                    catalog,
                    prepared.snapshot.order,
                    delete.table_id,
                    delete.schema_id,
                    &delete.row_ids,
                )
            })
            .sum::<usize>(),
    )
}

pub(crate) fn validate_prepared_inline_mutation_size(
    catalog: CatalogId,
    prepared: &PreparedInlineMutation,
) -> CatalogResult<()> {
    let estimated_bytes = prepared_inline_mutation_size(catalog, prepared);
    if estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES {
        return Err(CatalogError::InvalidMutation(format!(
            "foundationdb versionstamped inline payload is {estimated_bytes} bytes, over {} byte limit",
            FdbOrderedCatalogKv::MAX_COMMIT_BYTES
        )));
    }
    Ok(())
}

pub(crate) fn stage_prepared_inline_mutation(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineMutation,
) -> CatalogResult<()> {
    for replacement in &prepared.replacements {
        stage_inline_table_replacement(kv, trx, catalog, replacement)?;
    }
    for payload in &prepared.payloads {
        stage_inline_payload(
            kv,
            trx,
            catalog,
            prepared.snapshot.order,
            prepared.snapshot.sequence,
            payload,
        )?;
    }
    for delete in &prepared.deletes {
        stage_inline_delete(kv, trx, catalog, prepared.snapshot.order, delete, true)?;
    }
    stage_fdb_max_file_id_watermark(kv, trx, catalog, prepared.snapshot.sequence.0);
    Ok(())
}

fn stage_inline_table_replacement(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    replacement: &(TableRow, TableRow),
) -> CatalogResult<()> {
    let (previous, next) = replacement;
    trx.atomic_op(
        &kv.namespaced_key(&table_object_key(
            catalog,
            previous.table_id,
            previous.validity.begin_order,
        )),
        &versionstamped_value(&previous.encode(), TableRow::END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &table_object_key(catalog, next.table_id, next.validity.begin_order),
            table_object_key_order_offset(catalog, next.table_id),
        )?,
        &next.encode(),
        MutationType::SetVersionstampedKey,
    );
    stage_current_table_row(kv, trx, catalog, next)?;
    stage_table_visibility_end(kv, trx, catalog, previous)?;
    stage_table_visibility_begin(kv, trx, catalog, next)?;
    stage_fdb_max_catalog_id_watermark(kv, trx, catalog, next.table_id.0);
    Ok(())
}

fn stage_inline_payload(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    order: CatalogOrderId,
    sequence: RawSnapshotSequence,
    payload: &PreparedInlinePayload,
) -> CatalogResult<()> {
    for row in &payload.rows {
        let key = inline_table_chunk_key(
            catalog,
            payload.table_id,
            payload.schema_id,
            order,
            row.chunk_index,
        );
        trx.atomic_op(
            &kv.versionstamped_key(
                &key,
                inline_table_chunk_key_order_offset(catalog, payload.table_id, payload.schema_id),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
    }
    for key in &payload.row_changes {
        trx.atomic_op(
            &kv.versionstamped_key(&key.key, key.order_offset)?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        if let Some(row_id) = key.row_id {
            trx.set(
                &kv.namespaced_key(&inline_live_row_key(
                    catalog,
                    payload.table_id,
                    payload.schema_id,
                    row_id,
                )),
                &sequence.0.to_be_bytes(),
            );
            let row_payload = payload.current_rows.get(&row_id).ok_or_else(|| {
                CatalogError::InvalidMutation(format!(
                    "inline row id {row_id} is missing its current-row payload"
                ))
            })?;
            let current_row = InlineCurrentRow::new(order, sequence, row_payload.clone()).encode();
            trx.atomic_op(
                &kv.namespaced_key(&inline_current_row_key(
                    catalog,
                    payload.table_id,
                    payload.schema_id,
                    row_id,
                )),
                &versionstamped_value(&current_row, InlineCurrentRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            trx.atomic_op(
                &kv.namespaced_key(&inline_next_row_id_key(
                    catalog,
                    payload.table_id,
                    payload.schema_id,
                )),
                &row_id.saturating_add(1).to_be_bytes(),
                MutationType::ByteMax,
            );
        }
    }
    Ok(())
}

fn commit_prepared_inline_mutation(
    trx: foundationdb::Transaction,
    prepared: PreparedInlineMutation,
) -> CatalogResult<InlineTableMutationCommit> {
    let versionstamp = trx.get_versionstamp();
    block_on(trx.commit()).map_err(map_fdb_commit_error)?;
    let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
    let rows = prepared
        .payloads
        .into_iter()
        .flat_map(|payload| payload.rows)
        .map(|mut row| {
            row.validity = ValidityWindow::new(order, None);
            row
        })
        .collect();
    Ok(InlineTableMutationCommit { rows })
}

fn stage_inline_delete(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    order: CatalogOrderId,
    delete: &PreparedInlineDelete,
    versionstamped: bool,
) -> CatalogResult<()> {
    if delete.row_ids.is_empty() {
        return Ok(());
    }
    let table_key = inline_table_change_key(
        catalog,
        order,
        InlineRowChangeKind::Deleted,
        delete.table_id,
    );
    stage_inline_change_key(
        kv,
        trx,
        &table_key,
        inline_table_change_prefix(catalog).len(),
        versionstamped,
    )?;
    for row_id in &delete.row_ids {
        let row_key = table_inline_row_change_key(
            catalog,
            delete.table_id,
            order,
            InlineRowChangeKind::Deleted,
            delete.schema_id,
            *row_id,
        );
        stage_inline_change_key(
            kv,
            trx,
            &row_key,
            table_inline_row_change_prefix(catalog, delete.table_id).len(),
            versionstamped,
        )?;
        let schema_key = table_schema_kind_inline_row_change_key(
            catalog,
            delete.table_id,
            delete.schema_id,
            InlineRowChangeKind::Deleted,
            order,
            *row_id,
        );
        stage_inline_change_key(
            kv,
            trx,
            &schema_key,
            table_schema_kind_inline_row_change_prefix(
                catalog,
                delete.table_id,
                delete.schema_id,
                InlineRowChangeKind::Deleted,
            )
            .len(),
            versionstamped,
        )?;
        trx.clear(&kv.namespaced_key(&inline_live_row_key(
            catalog,
            delete.table_id,
            delete.schema_id,
            *row_id,
        )));
        trx.clear(&kv.namespaced_key(&inline_current_row_key(
            catalog,
            delete.table_id,
            delete.schema_id,
            *row_id,
        )));
    }
    Ok(())
}

fn stage_inline_change_key(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    key: &[u8],
    order_offset: usize,
    versionstamped: bool,
) -> CatalogResult<()> {
    if versionstamped {
        trx.atomic_op(
            &kv.versionstamped_key(key, order_offset)?,
            &[],
            MutationType::SetVersionstampedKey,
        );
    } else {
        trx.set(&kv.namespaced_key(key), &[]);
    }
    Ok(())
}

fn add_snapshot_prefix_conflict(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
) -> CatalogResult<()> {
    let prefix = kv.namespaced_key(&snapshot_prefix(catalog));
    let mut range = RangeOption::from(prefix.clone()..prefix_end(&prefix));
    range.limit = Some(1);
    block_on(trx.get_range(&range, 1, false)).map_err(map_fdb_error)?;
    trx.add_conflict_range(&prefix, &prefix_end(&prefix), ConflictRangeType::Read)
        .map_err(map_fdb_error)?;
    trx.add_conflict_range(&prefix, &prefix_end(&prefix), ConflictRangeType::Write)
        .map_err(map_fdb_error)
}

fn stage_snapshot(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_key(catalog, snapshot.order),
            snapshot_key_order_offset(catalog),
        )?,
        &snapshot.encode(),
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
        )?,
        &snapshot.sequence.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    stage_fdb_snapshot_indexes(kv, trx, catalog, snapshot)?;
    Ok(())
}

fn commit_delete_inline_table_rows(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    deleted_row_ids: &[u64],
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<InlineTableDeleteCommit> {
    let deleted = deleted_row_ids.iter().copied().collect::<BTreeSet<_>>();
    if deleted.is_empty() {
        return Ok(InlineTableDeleteCommit {
            deleted_row_count: 0,
            rewritten_payload_count: 0,
        });
    }

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("snapshot"))?;
    let target = fdb_inline_delete_target(kv, catalog, &latest, commit_snapshot)?;
    let order = target.snapshot.order;
    let delete = InlineTableDeletePayload {
        table_id,
        schema_id,
        row_ids: deleted.into_iter().collect(),
    };
    let live_rows = load_inline_live_rows(kv, catalog, &[], std::slice::from_ref(&delete))?;
    let prepared = prepare_inline_delete(&live_rows, target.snapshot.sequence, delete)?;

    if prepared.row_ids.is_empty() {
        return Ok(InlineTableDeleteCommit {
            deleted_row_count: 0,
            rewritten_payload_count: 0,
        });
    }

    let rewritten_payload_count = 0;
    let estimated_bytes = estimate_inline_delete_bytes(
        catalog,
        &target.snapshot,
        table_id,
        schema_id,
        &prepared.row_ids,
    );
    if estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES {
        return Err(CatalogError::InvalidMutation(format!(
            "foundationdb versionstamped inline delete is {estimated_bytes} bytes, over {} byte limit",
            FdbOrderedCatalogKv::MAX_COMMIT_BYTES
        )));
    }

    let trx = kv.create_transaction()?;
    if target.stage_snapshot {
        stage_snapshot(kv, &trx, catalog, &target.snapshot)?;
    }
    stage_inline_delete(kv, &trx, catalog, order, &prepared, target.stage_snapshot)?;

    block_on(trx.commit()).map_err(map_fdb_commit_error)?;
    Ok(InlineTableDeleteCommit {
        deleted_row_count: prepared.row_ids.len(),
        rewritten_payload_count,
    })
}

struct FdbInlineDeleteTarget {
    snapshot: SnapshotRow,
    stage_snapshot: bool,
}

fn fdb_inline_delete_target(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    latest: &SnapshotRow,
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<FdbInlineDeleteTarget> {
    if let Some(commit_snapshot) = commit_snapshot
        && let Some(snapshot) = crate::snapshot_by_ducklake_sequence(kv, catalog, commit_snapshot)?
    {
        return Ok(FdbInlineDeleteTarget {
            snapshot,
            stage_snapshot: false,
        });
    }
    Ok(FdbInlineDeleteTarget {
        snapshot: SnapshotRow::new(incomplete_order(), latest.sequence.next()),
        stage_snapshot: true,
    })
}

fn prepare_table_replacement(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    latest: Option<&SnapshotRow>,
    placeholder: crate::CatalogOrderId,
    table: Option<TableRow>,
) -> CatalogResult<Option<(TableRow, TableRow)>> {
    let Some(mut next) = table else {
        return Ok(None);
    };
    let _ = latest.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut previous = load_current_table_row(kv, catalog, next.table_id)?
        .ok_or(CatalogError::NotFound("table"))?;
    previous.validity.end_order = Some(placeholder);
    next.validity = ValidityWindow::new(placeholder, None);
    Ok(Some((previous, next)))
}

struct VersionstampedInlineChangeKey {
    key: Vec<u8>,
    order_offset: usize,
    row_id: Option<u64>,
}

fn staged_inline_change_keys(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    payload: &[u8],
) -> CatalogResult<Vec<VersionstampedInlineChangeKey>> {
    let mut batch = KvBatch::new();
    stage_inline_row_changes_for_payload(
        &mut batch,
        catalog,
        table_id,
        schema_id,
        incomplete_order(),
        InlineRowChangeKind::Inserted,
        payload,
    )?;
    batch
        .writes()
        .iter()
        .map(|(key, _)| {
            let order_offset = inline_change_order_offset(catalog, table_id, key)?;
            let row_id = key
                .starts_with(&table_inline_row_change_prefix(catalog, table_id))
                .then(|| {
                    key.get(key.len().saturating_sub(8)..)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u64::from_be_bytes)
                        .ok_or_else(|| {
                            CatalogError::InvalidKey(
                                "inline row-change key is missing its row id".to_owned(),
                            )
                        })
                })
                .transpose()?;
            Ok(VersionstampedInlineChangeKey {
                key: key.clone(),
                order_offset,
                row_id,
            })
        })
        .collect()
}

fn inline_change_order_offset(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
) -> CatalogResult<usize> {
    let row_prefix = table_inline_row_change_prefix(catalog, table_id);
    if key.starts_with(&row_prefix) {
        return Ok(row_prefix.len());
    }
    let table_prefix = inline_table_change_prefix(catalog);
    if key.starts_with(&table_prefix) {
        return Ok(table_prefix.len());
    }
    Err(CatalogError::InvalidKey(
        "inline change key has unknown family".to_owned(),
    ))
}

fn inline_table_chunk_key_order_offset(
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
) -> usize {
    inline_table_payload_prefix(catalog, table_id, schema_id, incomplete_order())
        .len()
        .saturating_sub(crate::CatalogOrderId::LEN + 1)
}

fn estimate_inline_payload_bytes(
    catalog: CatalogId,
    snapshot: &SnapshotRow,
    replacements: &[(TableRow, TableRow)],
    payloads: &[PreparedInlinePayload],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let table_bytes = replacements
        .iter()
        .map(|(previous, next)| {
            let previous_len = previous.encode().len();
            let next_len = next.encode().len();
            table_object_key(catalog, previous.table_id, previous.validity.begin_order)
                .len()
                .saturating_add(previous_len)
                .saturating_add(table_object_key(catalog, next.table_id, snapshot.order).len())
                .saturating_add(next_len)
                .saturating_add(current_table_row_key(catalog, next.table_id).len())
                .saturating_add(next_len)
                .saturating_add(
                    table_visibility_key(catalog, previous.validity.begin_order, previous.table_id)
                        .len(),
                )
                .saturating_add(previous_len)
                .saturating_add(table_visibility_key(catalog, snapshot.order, next.table_id).len())
                .saturating_add(next_len)
        })
        .sum::<usize>();
    let chunk_bytes = payloads
        .iter()
        .flat_map(|payload| &payload.rows)
        .map(|row| {
            inline_table_chunk_key(
                catalog,
                row.table_id,
                row.schema_id,
                snapshot.order,
                row.chunk_index,
            )
            .len()
            .saturating_add(row.encode().len())
        })
        .sum::<usize>();
    let change_bytes = payloads
        .iter()
        .flat_map(|payload| {
            payload.row_changes.iter().map(|change| {
                change.key.len()
                    + change.row_id.map_or(0, |row_id| {
                        let current_value_len =
                            payload.current_rows.get(&row_id).map_or(0, |row| {
                                InlineCurrentRow::new(
                                    snapshot.order,
                                    snapshot.sequence,
                                    row.clone(),
                                )
                                .encode()
                                .len()
                            });
                        inline_live_row_key(catalog, payload.table_id, payload.schema_id, row_id)
                            .len()
                            .saturating_add(8)
                            .saturating_add(
                                inline_current_row_key(
                                    catalog,
                                    payload.table_id,
                                    payload.schema_id,
                                    row_id,
                                )
                                .len(),
                            )
                            .saturating_add(current_value_len)
                            .saturating_add(
                                inline_next_row_id_key(
                                    catalog,
                                    payload.table_id,
                                    payload.schema_id,
                                )
                                .len()
                                .saturating_add(8),
                            )
                    })
            })
        })
        .sum::<usize>();
    let row_count = payloads
        .iter()
        .map(|payload| payload.rows.len())
        .sum::<usize>();
    snapshot_bytes
        .saturating_add(table_bytes)
        .saturating_add(chunk_bytes)
        .saturating_add(change_bytes)
        .saturating_add(row_count.saturating_mul(STORED_ORDER_LEN))
}

fn estimate_inline_delete_bytes(
    catalog: CatalogId,
    snapshot: &SnapshotRow,
    table_id: TableId,
    schema_id: SchemaId,
    deleted_rows: &[u64],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    snapshot_bytes.saturating_add(estimate_inline_delete_change_bytes(
        catalog,
        snapshot.order,
        table_id,
        schema_id,
        deleted_rows,
    ))
}

fn estimate_inline_delete_change_bytes(
    catalog: CatalogId,
    order: CatalogOrderId,
    table_id: TableId,
    schema_id: SchemaId,
    deleted_rows: &[u64],
) -> usize {
    deleted_rows
        .iter()
        .map(|row_id| {
            table_inline_row_change_key(
                catalog,
                table_id,
                order,
                InlineRowChangeKind::Deleted,
                schema_id,
                *row_id,
            )
            .len()
                + table_schema_kind_inline_row_change_key(
                    catalog,
                    table_id,
                    schema_id,
                    InlineRowChangeKind::Deleted,
                    order,
                    *row_id,
                )
                .len()
                + inline_live_row_key(catalog, table_id, schema_id, *row_id).len()
                + inline_current_row_key(catalog, table_id, schema_id, *row_id).len()
        })
        .sum::<usize>()
        .saturating_add(
            inline_table_change_key(catalog, order, InlineRowChangeKind::Deleted, table_id).len(),
        )
}

#[cfg(test)]
#[path = "fdb_inline_tables_tests.rs"]
mod tests;
