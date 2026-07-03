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
    CatalogError, CatalogId, CatalogResult, DataFileRow, DuckLakeSnapshotId, FdbOrderedCatalogKv,
    InlineRowChangeKind, InlineTableChunkRow, InlineTableDeleteCommit, InlineTablePayloadCommit,
    KvBatch, SchemaId, SnapshotRow, TableId, TableRow, ValidityWindow,
    conflict_watermarks::{stage_fdb_max_catalog_id_watermark, stage_fdb_max_file_id_watermark},
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_tables::{
        stage_current_table_row, stage_table_visibility_begin, stage_table_visibility_end,
    },
    fdb_versionstamp::{
        committed_order, incomplete_order, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, table_object_key_order_offset, versionstamped_value,
    },
    inline_change_feed::{
        list_inline_deleted_row_changes_for_schema, stage_inline_row_changes_for_payload,
    },
    inline_data::{
        assemble_inline_payload, filter_deleted_rows, inline_table_chunk_key, inline_table_chunks,
        inline_table_payload_prefix, validate_inline_table_rows_fit_fdb,
        visible_inline_payload_chunks,
    },
    keys::{
        current_table_row_key, inline_table_change_key, inline_table_change_prefix, prefix_end,
        snapshot_key, snapshot_prefix, snapshot_timestamp_key, table_inline_row_change_key,
        table_inline_row_change_prefix, table_object_key, table_schema_kind_inline_row_change_key,
        table_schema_kind_inline_row_change_prefix, table_visibility_key,
    },
    public_snapshot_sequence_for_order,
    rows::STORED_ORDER_LEN,
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
    table_store::load_current_table_row,
};

impl FdbOrderedCatalogKv {
    pub fn register_inline_table_payload_versionstamped(
        &self,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        payload: Vec<u8>,
    ) -> CatalogResult<Vec<InlineTableChunkRow>> {
        commit_inline_table_payload(
            self, catalog, None, table_id, schema_id, payload, None, None, None,
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
            catalog, table, schema_id, payload, None, None, None,
        )
    }

    pub fn register_inline_table_payload_with_table_at_snapshot_versionstamped(
        &self,
        catalog: CatalogId,
        table: TableRow,
        schema_id: SchemaId,
        payload: Vec<u8>,
        commit_snapshot: Option<DuckLakeSnapshotId>,
        read_snapshot: Option<DuckLakeSnapshotId>,
        commit_metadata: Option<&crate::SnapshotCommitMetadata>,
    ) -> CatalogResult<Vec<InlineTableChunkRow>> {
        let table_id = table.table_id;
        commit_inline_table_payload(
            self,
            catalog,
            Some(table),
            table_id,
            schema_id,
            payload,
            commit_snapshot,
            read_snapshot,
            commit_metadata,
        )
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
    commit_snapshot: Option<DuckLakeSnapshotId>,
    read_snapshot: Option<DuckLakeSnapshotId>,
    commit_metadata: Option<&crate::SnapshotCommitMetadata>,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    validate_inline_table_rows_fit_fdb(&payload)?;
    let latest = latest_snapshot(kv, catalog)?;
    if let Some(commit_snapshot) = commit_snapshot {
        let latest_sequence = latest
            .as_ref()
            .ok_or(CatalogError::NotFound("catalog snapshot"))?
            .sequence;
        let latest_commit = DuckLakeSnapshotId(latest_sequence.0);
        let next_commit = DuckLakeSnapshotId(latest_sequence.next().0);
        let valid_commit_snapshot = if read_snapshot.is_some() {
            commit_snapshot == next_commit
        } else {
            commit_snapshot == latest_commit || commit_snapshot == next_commit
        };
        if !valid_commit_snapshot {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict committing inline rows: proposed commit snapshot {} does not match latest DuckLake snapshot {} or next DuckLake snapshot {}",
                commit_snapshot.0, latest_commit.0, next_commit.0
            )));
        }
    }
    let next_sequence = match (latest.as_ref(), commit_snapshot) {
        (_, Some(commit_snapshot)) => crate::RawSnapshotSequence(commit_snapshot.0),
        (Some(snapshot), None) => snapshot.sequence.next(),
        (None, None) => crate::RawSnapshotSequence::initial(),
    };
    let placeholder = incomplete_order();
    let snapshot =
        SnapshotRow::new(placeholder, next_sequence).with_optional_commit_metadata(commit_metadata);
    let rows = inline_table_chunks(table_id, schema_id, placeholder, payload.clone())?;
    let row_changes = staged_inline_change_keys(catalog, table_id, schema_id, &payload)?;
    let replacement = prepare_table_replacement(kv, catalog, latest.as_ref(), placeholder, table)?;
    let estimated_bytes = estimate_inline_payload_bytes(
        catalog,
        &snapshot,
        replacement.as_ref(),
        &rows,
        &row_changes,
    );
    if estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES {
        return Err(CatalogError::InvalidMutation(format!(
            "foundationdb versionstamped inline payload is {estimated_bytes} bytes, over {} byte limit",
            FdbOrderedCatalogKv::MAX_COMMIT_BYTES
        )));
    }

    let trx = kv.create_transaction()?;
    add_snapshot_prefix_conflict(kv, &trx, catalog)?;
    stage_snapshot(kv, &trx, catalog, &snapshot)?;
    if let Some((previous, next)) = &replacement {
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
                &table_object_key(catalog, next.table_id, placeholder),
                table_object_key_order_offset(catalog, next.table_id),
            )?,
            &next.encode(),
            MutationType::SetVersionstampedKey,
        );
        stage_current_table_row(kv, &trx, catalog, next)?;
        stage_table_visibility_end(kv, &trx, catalog, previous)?;
        stage_table_visibility_begin(kv, &trx, catalog, next)?;
        stage_fdb_max_catalog_id_watermark(kv, &trx, catalog, next.table_id.0);
    }
    for row in &rows {
        trx.atomic_op(
            &kv.versionstamped_key(
                &inline_table_chunk_key(catalog, table_id, schema_id, placeholder, row.chunk_index),
                inline_table_chunk_key_order_offset(catalog, table_id, schema_id),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
    }
    for key in &row_changes {
        trx.atomic_op(
            &kv.versionstamped_key(&key.key, key.order_offset)?,
            &[],
            MutationType::SetVersionstampedKey,
        );
    }
    stage_fdb_max_file_id_watermark(kv, &trx, catalog, snapshot.sequence.0);

    let versionstamp = trx.get_versionstamp();
    block_on(trx.commit()).map_err(map_fdb_commit_error)?;
    let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
    Ok(rows
        .into_iter()
        .map(|mut row| {
            row.validity = ValidityWindow::new(order, None);
            row
        })
        .collect())
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
    stage_fdb_latest_snapshot_value(kv, trx, catalog, snapshot)?;
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
    let visible_payloads =
        visible_inline_payload_chunks(kv, catalog, table_id, schema_id, latest.order)?;
    let existing_deletions =
        ExistingInlineDeletions::load(kv, catalog, table_id, schema_id, latest.order)?;
    let mut deleted_rows = Vec::new();
    let mut hidden_versions_by_row_id = BTreeMap::<u64, usize>::new();

    for (begin_order, chunks) in visible_payloads {
        let begin_snapshot = public_snapshot_sequence_for_order(kv, catalog, begin_order)?
            .ok_or_else(|| CatalogError::NotFound("inline payload snapshot"))?
            .0;
        if begin_snapshot >= target.snapshot.sequence.0 {
            continue;
        }
        let payload = assemble_inline_payload(chunks.clone())?;
        let filtered = filter_deleted_rows(&payload, &deleted)?;
        if filtered.deleted_row_ids.is_empty() {
            continue;
        }
        for row_id in filtered.deleted_row_ids {
            if existing_deletions.hides_row_version_before(
                row_id,
                begin_snapshot,
                target.snapshot.sequence.0,
            ) {
                continue;
            }
            let hidden_versions = hidden_versions_by_row_id.entry(row_id).or_default();
            *hidden_versions += 1;
            if *hidden_versions > 1 {
                return Err(CatalogError::InvalidMutation(format!(
                    "inline row id {row_id} matches multiple live payload versions for table {} schema {}",
                    table_id.0, schema_id.0
                )));
            }
            deleted_rows.push(row_id);
        }
    }

    if deleted_rows.is_empty() {
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
        &deleted_rows,
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
    if !deleted_rows.is_empty() {
        let table_change_key =
            inline_table_change_key(catalog, order, InlineRowChangeKind::Deleted, table_id);
        if target.stage_snapshot {
            trx.atomic_op(
                &kv.versionstamped_key(
                    &table_change_key,
                    inline_table_change_prefix(catalog).len(),
                )?,
                &[],
                MutationType::SetVersionstampedKey,
            );
        } else {
            trx.set(&kv.namespaced_key(&table_change_key), &[]);
        }
    }
    for row_id in &deleted_rows {
        let key = table_inline_row_change_key(
            catalog,
            table_id,
            order,
            InlineRowChangeKind::Deleted,
            schema_id,
            *row_id,
        );
        let schema_kind_key = table_schema_kind_inline_row_change_key(
            catalog,
            table_id,
            schema_id,
            InlineRowChangeKind::Deleted,
            order,
            *row_id,
        );
        if target.stage_snapshot {
            trx.atomic_op(
                &kv.versionstamped_key(
                    &key,
                    table_inline_row_change_prefix(catalog, table_id).len(),
                )?,
                &[],
                MutationType::SetVersionstampedKey,
            );
            trx.atomic_op(
                &kv.versionstamped_key(
                    &schema_kind_key,
                    table_schema_kind_inline_row_change_prefix(
                        catalog,
                        table_id,
                        schema_id,
                        InlineRowChangeKind::Deleted,
                    )
                    .len(),
                )?,
                &[],
                MutationType::SetVersionstampedKey,
            );
        } else {
            trx.set(&kv.namespaced_key(&key), &[]);
            trx.set(&kv.namespaced_key(&schema_kind_key), &[]);
        }
    }

    block_on(trx.commit()).map_err(map_fdb_commit_error)?;
    Ok(InlineTableDeleteCommit {
        deleted_row_count: deleted_rows.len(),
        rewritten_payload_count,
    })
}

struct ExistingInlineDeletions {
    by_row_id: BTreeMap<u64, BTreeSet<u64>>,
}

impl ExistingInlineDeletions {
    fn load(
        kv: &FdbOrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        latest_order: crate::CatalogOrderId,
    ) -> CatalogResult<Self> {
        let start_order =
            crate::CatalogOrderId::from_bytes(latest_order.kind(), [0; crate::CatalogOrderId::LEN]);
        let mut by_row_id = BTreeMap::<u64, BTreeSet<u64>>::new();
        for change in list_inline_deleted_row_changes_for_schema(
            kv,
            catalog,
            table_id,
            schema_id,
            start_order,
            latest_order,
        )? {
            let Some(delete_snapshot) =
                public_snapshot_sequence_for_order(kv, catalog, change.order)?
            else {
                continue;
            };
            by_row_id
                .entry(change.row_id)
                .or_default()
                .insert(delete_snapshot.0);
        }
        Ok(Self { by_row_id })
    }

    fn hides_row_version_before(
        &self,
        row_id: u64,
        begin_snapshot: u64,
        before_snapshot: u64,
    ) -> bool {
        self.by_row_id.get(&row_id).is_some_and(|delete_snapshots| {
            delete_snapshots
                .range((begin_snapshot + 1)..before_snapshot)
                .next()
                .is_some()
        })
    }
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
    if let Some(commit_snapshot) = commit_snapshot {
        if let Some(snapshot) = crate::snapshot_by_ducklake_sequence(kv, catalog, commit_snapshot)?
        {
            return Ok(FdbInlineDeleteTarget {
                snapshot,
                stage_snapshot: false,
            });
        }
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
            Ok(VersionstampedInlineChangeKey {
                key: key.clone(),
                order_offset,
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
    replacement: Option<&(TableRow, TableRow)>,
    rows: &[InlineTableChunkRow],
    row_changes: &[VersionstampedInlineChangeKey],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let table_bytes = replacement.map_or(0, |(previous, next)| {
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
    });
    let chunk_bytes = rows
        .iter()
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
    let change_bytes = row_changes
        .iter()
        .map(|change| change.key.len())
        .sum::<usize>();
    snapshot_bytes
        .saturating_add(table_bytes)
        .saturating_add(chunk_bytes)
        .saturating_add(change_bytes)
        .saturating_add(rows.len().saturating_mul(STORED_ORDER_LEN))
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
    let change_bytes = deleted_rows
        .iter()
        .map(|row_id| {
            table_inline_row_change_key(
                catalog,
                table_id,
                snapshot.order,
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
                    snapshot.order,
                    *row_id,
                )
                .len()
        })
        .sum::<usize>()
        .saturating_add(
            inline_table_change_key(
                catalog,
                snapshot.order,
                InlineRowChangeKind::Deleted,
                table_id,
            )
            .len(),
        );
    snapshot_bytes.saturating_add(change_bytes)
}

#[cfg(test)]
#[path = "fdb_inline_tables_tests.rs"]
mod tests;
