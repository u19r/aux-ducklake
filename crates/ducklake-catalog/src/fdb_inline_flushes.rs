use std::ops::Deref;

use foundationdb::options::{ConflictRangeType, MutationType};
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, FdbOrderedCatalogKv, InlineFileDeletionRow,
    InlineTableChunkRow, InlineTableFlush,
    conflict_watermarks::stage_fdb_max_file_id_watermark,
    fdb_runtime::map_fdb_error,
    fdb_versionstamp::{incomplete_order, versionstamped_value},
    inline_data::{
        flush_inline_table_payload_rows_at_snapshot_order, inline_table_chunk_key,
        list_inline_current_rows, list_inline_file_deletion_rows_for_table_at,
    },
    keys::{
        KeyFamily, family_prefix, inline_current_row_key, inline_current_row_prefix,
        inline_file_deletion_key, inline_table_end_key, prefix_end,
    },
    object_keys::conflict_fence_key,
    snapshot_operations::{
        SnapshotOperationKind, snapshot_operation_key, snapshot_operation_order_offset,
    },
    store::snapshot_by_raw_sequence,
};

pub(crate) fn stage_prepared_inline_flush_versionstamped(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    prepared: &PreparedInlineFlush,
) -> CatalogResult<StagedInlineFlush> {
    stage_inline_flush_fence(kv, trx, catalog, prepared.flush)?;
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
    for row in &prepared.table_rows {
        stage_inline_chunk_conflicts(kv, trx, catalog, row)?;
        if row.chunk_index == 0 {
            trx.atomic_op(
                &kv.versionstamped_key(
                    &inline_table_end_key(
                        catalog,
                        prepared.flush.table_id,
                        incomplete_order(),
                        prepared.flush.schema_id,
                        row.validity.begin_order,
                    ),
                    inline_table_end_key_order_offset(catalog),
                )?,
                &row.validity.begin_order.as_bytes(),
                MutationType::SetVersionstampedKey,
            );
        }
    }
    let current_prefix = kv.namespaced_key(&inline_current_row_prefix(
        catalog,
        prepared.flush.table_id,
        prepared.flush.schema_id,
    ));
    trx.add_conflict_range(
        &current_prefix,
        &prefix_end(&current_prefix),
        ConflictRangeType::Read,
    )
    .map_err(map_fdb_error)?;
    for row_id in &prepared.current_row_ids {
        trx.clear(&kv.namespaced_key(&inline_current_row_key(
            catalog,
            prepared.flush.table_id,
            prepared.flush.schema_id,
            *row_id,
        )));
    }
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
    stage_fdb_max_file_id_watermark(kv, trx, catalog, prepared.flush.flush_snapshot_sequence.0);
    Ok(StagedInlineFlush {
        table_chunk_count: prepared.table_rows.len(),
        inline_file_deletion_count: prepared.inline_file_deletions.len(),
    })
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedInlineFlush {
    flush: InlineTableFlush,
    inline_file_deletions: Vec<InlineFileDeletionRow>,
    table_rows: Vec<InlineTableChunkRow>,
    current_row_ids: Vec<u64>,
}

pub(crate) fn prepare_inline_flushes(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    flushes: &[InlineTableFlush],
) -> CatalogResult<Vec<PreparedInlineFlush>> {
    flushes
        .iter()
        .map(|flush| prepare_inline_flush(kv, catalog, *flush))
        .collect()
}

fn prepare_inline_flush(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    flush: InlineTableFlush,
) -> CatalogResult<PreparedInlineFlush> {
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
    let table_rows = flush_inline_table_payload_rows_at_snapshot_order(
        kv,
        catalog,
        flush,
        flush_snapshot.order,
        incomplete_order(),
    )?;
    let current_row_ids = list_inline_current_rows(kv, catalog, flush.table_id, flush.schema_id)?
        .into_iter()
        .filter_map(|(row_id, row)| (row.begin_order <= flush_snapshot.order).then_some(row_id))
        .collect();
    Ok(PreparedInlineFlush {
        flush,
        inline_file_deletions,
        table_rows,
        current_row_ids,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagedInlineFlush {
    pub(crate) table_chunk_count: usize,
    pub(crate) inline_file_deletion_count: usize,
}

impl StagedInlineFlush {
    #[must_use]
    pub(crate) const fn staged_item_count(self) -> usize {
        self.table_chunk_count
            .saturating_add(self.inline_file_deletion_count)
    }
}

fn stage_inline_flush_fence(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    flush: InlineTableFlush,
) -> CatalogResult<()> {
    let key = inline_flush_fence_key(catalog, flush);
    let namespaced = kv.namespaced_key(&key);
    let current = block_on(trx.get(&namespaced, false))
        .map_err(map_fdb_error)?
        .map(|value| decode_flush_fence(value.deref()))
        .transpose()?
        .unwrap_or(0);
    trx.set(&namespaced, &current.saturating_add(1).to_be_bytes());
    Ok(())
}

fn inline_flush_fence_key(catalog: CatalogId, flush: InlineTableFlush) -> Vec<u8> {
    let mut scope = b"inline-flush/".to_vec();
    scope.extend_from_slice(&flush.table_id.0.to_be_bytes());
    scope.push(b'/');
    scope.extend_from_slice(&flush.schema_id.0.to_be_bytes());
    conflict_fence_key(catalog, &scope)
}

fn decode_flush_fence(bytes: &[u8]) -> CatalogResult<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CatalogError::Decode("inline flush fence is not 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(array))
}

fn stage_inline_chunk_conflicts(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    row: &InlineTableChunkRow,
) -> CatalogResult<()> {
    let key = inline_table_chunk_key(
        catalog,
        row.table_id,
        row.schema_id,
        row.validity.begin_order,
        row.chunk_index,
    );
    let namespaced = kv.namespaced_key(&key);
    trx.add_conflict_range(
        &namespaced,
        &prefix_end(&namespaced),
        ConflictRangeType::Read,
    )
    .map_err(map_fdb_error)?;
    trx.add_conflict_range(
        &namespaced,
        &prefix_end(&namespaced),
        ConflictRangeType::Write,
    )
    .map_err(map_fdb_error)?;
    Ok(())
}

fn inline_table_end_key_order_offset(catalog: CatalogId) -> usize {
    family_prefix(catalog, KeyFamily::EndOrder)
        .len()
        .saturating_add(8 + 1)
}
