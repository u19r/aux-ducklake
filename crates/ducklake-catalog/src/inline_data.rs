mod inline_deletions;
mod inline_file_deletes;
mod inline_row_deletes;
mod inline_tables;

use crate::{
    CatalogError, CatalogResult, ValidityWindow,
    ids::CatalogOrderId,
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
};

pub use inline_deletions::{
    InlineDeletionChunkRow, load_inline_deletion_payload_at, register_inline_deletion_payload,
};
#[cfg(feature = "foundationdb")]
pub(crate) use inline_file_deletes::list_inline_file_deletion_rows_for_data_files_at;
pub use inline_file_deletes::{
    InlineFileDeletionRow, commit_inline_file_deletions, list_inline_file_deletions_at,
    list_inline_file_deletions_between,
};
pub(crate) use inline_file_deletes::{
    inline_file_deletion_begin_order, inline_file_deletion_changed_table_ids_at,
    list_inline_file_deletion_rows_for_table_at, list_inline_file_deletions_for_data_files_at,
    stage_flush_inline_file_deletions, stage_inline_file_deletion,
};
pub use inline_row_deletes::{
    InlineTableDeleteCommit, commit_delete_inline_table_rows,
    commit_delete_inline_table_rows_at_snapshot,
};
#[cfg(feature = "foundationdb")]
pub(crate) use inline_row_deletes::{filter_deleted_rows, visible_inline_payload_chunks};
#[cfg(not(test))]
pub(crate) use inline_tables::invalidate_inline_table_payload_read_context;
pub use inline_tables::{
    InlineTableChunkRow, InlineTableFlush, InlineTablePayloadCommit, InlineTablePayloadRow,
    list_inline_table_payloads_at, load_inline_table_payload_at, register_inline_table_payload,
    register_inline_table_payload_with_table, register_inline_table_payload_with_table_at_snapshot,
    route_inline_table_payload_or_data_file,
};
pub(crate) use inline_tables::{
    assemble_inline_payload, decode_inline_table_item, inline_chunk_visible_at,
    inline_table_all_end_orders, inline_table_end_orders_at, inline_table_flushes_ending_at,
    inline_table_payload_prefix, inline_table_prefix, list_unflushed_inline_table_payloads_at,
    list_unflushed_inline_table_payloads_at_with_end_orders, stage_flush_inline_table_payloads,
};
#[cfg(feature = "foundationdb")]
pub(crate) use inline_tables::{
    flush_inline_table_payload_rows_at_snapshot_order, inline_table_chunk_key, inline_table_chunks,
};

pub const INLINE_PAYLOAD_LIMIT_BYTES: usize = 90 * 1024;
pub const INLINE_CHUNK_BYTES: usize = 16 * 1024;

pub(crate) fn validate_inline_payload_size(payload_len: usize) -> CatalogResult<()> {
    if payload_len > INLINE_PAYLOAD_LIMIT_BYTES {
        return Err(CatalogError::InvalidMutation(format!(
            "inline payload is {payload_len} bytes, over {INLINE_PAYLOAD_LIMIT_BYTES} byte limit"
        )));
    }
    Ok(())
}

pub(crate) fn validate_inline_table_rows_fit_fdb(payload: &[u8]) -> CatalogResult<()> {
    for row in payload.split(|byte| *byte == b'\n') {
        if row.is_empty() {
            continue;
        }
        let row_len = row.len() + 1;
        if row_len > INLINE_PAYLOAD_LIMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "inline row is {row_len} bytes, over {INLINE_PAYLOAD_LIMIT_BYTES} byte limit"
            )));
        }
    }
    Ok(())
}

pub(crate) fn chunk_count(payload_len: usize) -> CatalogResult<u32> {
    let count = if payload_len == 0 {
        1
    } else {
        payload_len.saturating_add(INLINE_CHUNK_BYTES - 1) / INLINE_CHUNK_BYTES
    };
    u32::try_from(count)
        .map_err(|_| CatalogError::InvalidMutation("inline payload has too many chunks".to_owned()))
}

pub(crate) fn chunk_bounds(payload_len: usize, chunk_index: u32) -> (usize, usize) {
    let start = chunk_index as usize * INLINE_CHUNK_BYTES;
    let end = payload_len.min(start.saturating_add(INLINE_CHUNK_BYTES));
    (start, end)
}

pub(crate) fn encode_inline_end_order(out: &mut Vec<u8>, validity: ValidityWindow) {
    match validity.end_order {
        Some(order) => {
            out.push(1);
            encode_stored_order(out, order);
        }
        None => {
            out.push(0);
            out.resize(out.len() + STORED_ORDER_LEN, 0);
        }
    }
}

pub(crate) fn decode_inline_end_order(
    flag: u8,
    bytes: &[u8],
) -> CatalogResult<Option<CatalogOrderId>> {
    match flag {
        0 => Ok(None),
        1 => decode_stored_order(bytes, "inline end order").map(Some),
        other => Err(CatalogError::Decode(format!(
            "unsupported inline end-order flag {other}"
        ))),
    }
}

pub(crate) fn validate_contiguous_chunks(
    observed_len: usize,
    chunk_count: u32,
) -> CatalogResult<()> {
    if observed_len == chunk_count as usize {
        return Ok(());
    }
    Err(CatalogError::Decode(format!(
        "inline payload expected {chunk_count} chunk(s), got {observed_len}"
    )))
}
