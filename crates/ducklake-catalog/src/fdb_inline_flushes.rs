use std::ops::Deref;

use foundationdb::options::{ConflictRangeType, MutationType};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, FdbOrderedCatalogKv,
    InlineFileDeletionRow, InlineTableFlush,
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    fdb_runtime::map_fdb_error,
    fdb_versionstamp::{incomplete_order, versionstamped_value},
    inline_data::{
        flush_inline_table_payload_rows_at_snapshot_order, inline_table_payload_prefix,
        list_inline_current_rows, list_inline_file_deletion_rows_for_table_at,
    },
    keys::{
        InlineFlushWatermarkKey, KeyFamily, family_prefix, inline_current_row_key,
        inline_file_deletion_key, prefix_end,
    },
    kv::OrderedCatalogKv,
    object_keys::conflict_fence_key,
    snapshot_operations::{
        SnapshotOperationKind, snapshot_operation_key, snapshot_operation_order_offset,
    },
    store::snapshot_by_raw_sequence,
};

pub(crate) fn stage_prepared_inline_flushes(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &[PreparedInlineFlush],
) -> CatalogResult<usize> {
    let mut fenced = std::collections::BTreeSet::new();
    let mut staged_items = 0_usize;
    for flush in prepared {
        if fenced.insert(flush.flush.table_id) {
            stage_inline_flush_fence(kv, trx, catalog, flush)?;
        }
        let staged = stage_inline_flush(kv, trx, catalog, flush)?;
        staged_items = staged_items.saturating_add(staged);
    }
    Ok(staged_items)
}

fn stage_inline_flush(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<usize> {
    stage_file_deletion_ends(kv, trx, catalog, prepared)?;
    if prepared.flush.flush_inline_data {
        stage_payload_watermark(kv, trx, catalog, prepared)?;
        clear_observed_current_rows(kv, trx, catalog, prepared)?;
        trx.atomic_op(
            &kv.versionstamped_key(
                &snapshot_operation_key(
                    catalog,
                    crate::fdb_versionstamp::incomplete_order(),
                    SnapshotOperationKind::InlineFlush,
                    prepared.flush.table_id,
                ),
                snapshot_operation_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
    } else {
        protect_file_deletion_range(kv, trx, catalog, prepared)?;
    }
    stage_fdb_max_file_id_watermark(kv, trx, catalog, prepared.flush.flush_snapshot_sequence.0);
    Ok(prepared
        .table_chunk_count
        .saturating_add(prepared.inline_file_deletions.len()))
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedInlineFlush {
    flush: InlineTableFlush,
    fence_value: Option<Vec<u8>>,
    table_chunk_count: usize,
    inline_file_deletions: Vec<InlineFileDeletionRow>,
    min_begin_order: Option<CatalogOrderId>,
    max_begin_order: Option<CatalogOrderId>,
    current_row_ids: Vec<u64>,
}

pub(crate) fn prepare_inline_flushes(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    flushes: &[InlineTableFlush],
) -> CatalogResult<Vec<PreparedInlineFlush>> {
    // Coalesce data and file-deletion flushes independently. Their source
    // frontiers can differ even when they commit at the same catalog order.
    let mut coalesced = std::collections::BTreeMap::new();
    for flush in flushes {
        coalesced
            .entry((flush.table_id, flush.schema_id, flush.flush_inline_data))
            .and_modify(|existing: &mut InlineTableFlush| {
                if flush.flush_snapshot_sequence > existing.flush_snapshot_sequence {
                    *existing = *flush;
                }
            })
            .or_insert(*flush);
    }
    coalesced
        .values()
        .map(|flush| prepare_inline_flush(kv, catalog, *flush))
        .collect()
}

fn prepare_inline_flush(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    flush: InlineTableFlush,
) -> CatalogResult<PreparedInlineFlush> {
    // Capture the generation before loading observations. The native attempt
    // must reject them if another flush completes before this attempt starts.
    let fence_value = kv.get(&inline_flush_fence_key(catalog, flush))?;
    let Some(flush_snapshot) =
        snapshot_by_raw_sequence(kv, catalog, flush.flush_snapshot_sequence)?
    else {
        return Err(CatalogError::InvalidMutation(format!(
            "inline flush references missing snapshot {}",
            flush.flush_snapshot_sequence
        )));
    };
    let inline_file_deletions = list_inline_file_deletion_rows_for_table_at(
        kv,
        catalog,
        flush.table_id,
        flush_snapshot.order,
    )?;
    if !flush.flush_inline_data {
        return Ok(PreparedInlineFlush {
            flush,
            fence_value,
            table_chunk_count: 0,
            inline_file_deletions,
            min_begin_order: None,
            max_begin_order: None,
            current_row_ids: Vec::new(),
        });
    }
    let flushed = flush_inline_table_payload_rows_at_snapshot_order(
        kv,
        catalog,
        flush,
        flush_snapshot.order,
        incomplete_order(),
    )?;
    let min_begin_order = flushed.iter().map(|row| row.validity.begin_order).min();
    let max_begin_order = flushed.iter().map(|row| row.validity.begin_order).max();
    let current_row_ids = list_inline_current_rows(kv, catalog, flush.table_id, flush.schema_id)?
        .into_iter()
        .filter_map(|(row_id, row)| (row.begin_order <= flush_snapshot.order).then_some(row_id))
        .collect();
    Ok(PreparedInlineFlush {
        flush,
        fence_value,
        table_chunk_count: flushed.len(),
        inline_file_deletions,
        min_begin_order,
        max_begin_order,
        current_row_ids,
    })
}

fn stage_inline_flush_fence(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<()> {
    let key = inline_flush_fence_key(catalog, prepared.flush);
    let namespaced = kv.namespaced_key(&key);
    let value = crate::fdb_runtime::wait_for_fdb("wait.fdb.get", trx.get(&namespaced, false))
        .map_err(map_fdb_error)?
        .map(|value| value.deref().to_vec());
    if value != prepared.fence_value {
        return Err(CatalogError::ConflictFenceChanged { fence: key });
    }
    let current = value
        .as_deref()
        .map(decode_flush_fence)
        .transpose()?
        .unwrap_or(0);
    trx.set(&namespaced, &current.saturating_add(1).to_be_bytes());
    Ok(())
}

fn inline_flush_fence_key(catalog: CatalogId, flush: InlineTableFlush) -> Vec<u8> {
    // Inlined file deletions belong to the table, across all inline schemas.
    let mut scope = b"inline-table-flush/".to_vec();
    scope.extend_from_slice(&flush.table_id.0.to_be_bytes());
    conflict_fence_key(catalog, &scope)
}

fn decode_flush_fence(bytes: &[u8]) -> CatalogResult<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CatalogError::Decode("inline flush fence is not 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(array))
}

fn inline_flush_watermark_order_offset(catalog: CatalogId) -> usize {
    family_prefix(catalog, KeyFamily::InlineFlushWatermark)
        .len()
        .saturating_add(8 + 1)
}

fn stage_file_deletion_ends(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<()> {
    for row in &prepared.inline_file_deletions {
        let mut ended = row.clone();
        ended.validity.end_order = Some(incomplete_order());
        trx.atomic_op(
            &kv.namespaced_key(&inline_file_deletion_key(
                catalog,
                ended.table_id,
                ended.data_file_id,
                ended.validity.begin_order,
                ended.row_id,
            )),
            &versionstamped_value(
                &ended.encode(),
                crate::inline_data::InlineFileDeletionRow::END_ORDER_BYTES_OFFSET,
            )?,
            MutationType::SetVersionstampedValue,
        );
    }
    Ok(())
}

fn stage_payload_watermark(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<()> {
    if let Some(max_begin_order) = prepared.max_begin_order {
        trx.atomic_op(
            &kv.versionstamped_key(
                &InlineFlushWatermarkKey {
                    table_id: prepared.flush.table_id,
                    end_order: incomplete_order(),
                    schema_id: prepared.flush.schema_id,
                    max_begin_order,
                }
                .encode(catalog),
                inline_flush_watermark_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
    }
    // Protect only the begin-order range this flush covers. A concurrent
    // insert appends a later begin order and must not conflict with a
    // flush that cannot see it.
    if let (Some(min_begin_order), Some(max_begin_order)) =
        (prepared.min_begin_order, prepared.max_begin_order)
    {
        let chunk_start = kv.namespaced_key(&inline_table_payload_prefix(
            catalog,
            prepared.flush.table_id,
            prepared.flush.schema_id,
            min_begin_order,
        ));
        let chunk_end = prefix_end(&kv.namespaced_key(&inline_table_payload_prefix(
            catalog,
            prepared.flush.table_id,
            prepared.flush.schema_id,
            max_begin_order,
        )));
        trx.add_conflict_range(&chunk_start, &chunk_end, ConflictRangeType::Read)
            .map_err(map_fdb_error)?;
    }
    Ok(())
}

fn clear_observed_current_rows(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<()> {
    // Clear exactly the observed IDs. Contiguous IDs share one mutation;
    // gaps and any later allocated IDs remain outside these ranges. The
    // read conflict covers only the cleared range, not the whole prefix.
    let row_key = |row_id| {
        kv.namespaced_key(&inline_current_row_key(
            catalog,
            prepared.flush.table_id,
            prepared.flush.schema_id,
            row_id,
        ))
    };
    for rows in prepared
        .current_row_ids
        .chunk_by(|left, right| left.checked_add(1) == Some(*right))
    {
        let start = row_key(rows[0]);
        let mut end = row_key(rows[rows.len() - 1]);
        end.push(0);
        trx.add_conflict_range(&start, &end, ConflictRangeType::Read)
            .map_err(map_fdb_error)?;
        trx.clear_range(&start, &end);
    }
    Ok(())
}

fn protect_file_deletion_range(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<()> {
    // The flush only ends inlined file-deletion rows. Protect the range so
    // a concurrent delete cannot leave an ended row with a stale value.
    let deletion_prefix = kv.namespaced_key(&crate::keys::inline_file_deletion_table_prefix(
        catalog,
        prepared.flush.table_id,
    ));
    trx.add_conflict_range(
        &deletion_prefix,
        &prefix_end(&deletion_prefix),
        ConflictRangeType::Read,
    )
    .map_err(map_fdb_error)?;
    trx.add_conflict_range(
        &deletion_prefix,
        &prefix_end(&deletion_prefix),
        ConflictRangeType::Write,
    )
    .map_err(map_fdb_error)?;
    Ok(())
}
