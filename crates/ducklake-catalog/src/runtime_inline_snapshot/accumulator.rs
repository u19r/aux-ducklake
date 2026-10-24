use super::capture::{CapturedInlineRows, ensure_inline_capture_bound};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogError, CatalogOrderId, CatalogResult, RawSnapshotSequence,
    inline_data::{INLINE_PAYLOAD_LIMIT_BYTES, InlineCurrentRow, InlineTableChunkRow},
};

/// The bounded state used while folding the append-only inline history. It is
/// intentionally keyed by row id rather than by history order: an overwrite
/// replaces the previous payload in place and a delete removes it, so history
/// churn does not grow the retained state.
pub(super) struct InlineCurrentAccumulator {
    current: BTreeMap<u64, (CatalogOrderId, Vec<u8>)>,
    rows_by_begin: BTreeMap<CatalogOrderId, BTreeSet<u64>>,
    current_payload_bytes: usize,
    next_row_id: u64,
    max_rows: usize,
    max_payload_bytes: usize,
    pub(super) max_history_metadata_bytes: usize,
}

impl InlineCurrentAccumulator {
    fn new(max_rows: usize, max_payload_bytes: usize, max_history_metadata_bytes: usize) -> Self {
        Self {
            current: BTreeMap::new(),
            rows_by_begin: BTreeMap::new(),
            current_payload_bytes: 0,
            next_row_id: 0,
            max_rows,
            max_payload_bytes,
            max_history_metadata_bytes,
        }
    }

    pub(super) fn insert_payload_row(
        &mut self,
        begin_order: CatalogOrderId,
        row_id: u64,
        payload: Vec<u8>,
    ) -> CatalogResult<()> {
        if payload.len().saturating_add(1) > INLINE_PAYLOAD_LIMIT_BYTES {
            return Err(CatalogError::Backend(format!(
                "inline read snapshot row payload is {} bytes, over {} byte limit",
                payload.len().saturating_add(1),
                INLINE_PAYLOAD_LIMIT_BYTES
            )));
        }
        let previous_payload_bytes = self
            .current
            .get(&row_id)
            .map_or(0, |(_, value)| value.len());
        let next_row_count = self
            .current
            .len()
            .saturating_add(usize::from(!self.current.contains_key(&row_id)));
        let next_payload_bytes = self
            .current_payload_bytes
            .checked_sub(previous_payload_bytes)
            .and_then(|bytes| bytes.checked_add(payload.len()))
            .ok_or_else(|| {
                CatalogError::Backend(
                    "inline read snapshot payload byte count overflowed".to_owned(),
                )
            })?;
        ensure_inline_capture_bound(
            next_row_count,
            next_payload_bytes,
            self.max_rows,
            self.max_payload_bytes,
        )?;

        self.remove_row(row_id);
        self.current.insert(row_id, (begin_order, payload));
        self.rows_by_begin
            .entry(begin_order)
            .or_default()
            .insert(row_id);
        self.current_payload_bytes = next_payload_bytes;
        self.next_row_id = self.next_row_id.max(row_id.saturating_add(1));
        Ok(())
    }

    pub(super) fn remove_deleted(&mut self, row_id: u64, delete_order: CatalogOrderId) {
        if self
            .current
            .get(&row_id)
            .is_some_and(|(begin_order, _)| *begin_order < delete_order)
        {
            self.remove_row(row_id);
        }
    }

    fn remove_row(&mut self, row_id: u64) {
        if let Some((begin_order, payload)) = self.current.remove(&row_id) {
            self.current_payload_bytes = self.current_payload_bytes.saturating_sub(payload.len());
            if let Some(row_ids) = self.rows_by_begin.get_mut(&begin_order) {
                row_ids.remove(&row_id);
                if row_ids.is_empty() {
                    self.rows_by_begin.remove(&begin_order);
                }
            }
        }
    }
}

pub(super) struct InlinePayloadGroup {
    pub(super) begin_order: CatalogOrderId,
    end_order: Option<CatalogOrderId>,
    chunk_count: u32,
    next_chunk_index: u32,
    line: Vec<u8>,
    pub(super) visible: bool,
}

impl InlinePayloadGroup {
    pub(super) fn new(
        row: &InlineTableChunkRow,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        if row.chunk_count == 0 {
            return Err(CatalogError::Decode(
                "inline payload chunk count cannot be zero".to_owned(),
            ));
        }
        Ok(Self {
            begin_order: row.validity.begin_order,
            end_order: row.validity.end_order,
            chunk_count: row.chunk_count,
            next_chunk_index: 0,
            line: Vec::new(),
            visible: row.validity.begin_order <= snapshot_order
                && row
                    .validity
                    .end_order
                    .is_none_or(|end_order| snapshot_order < end_order),
        })
    }

    pub(super) fn push(
        &mut self,
        row: InlineTableChunkRow,
        accumulator: &mut InlineCurrentAccumulator,
    ) -> CatalogResult<()> {
        if row.validity.begin_order != self.begin_order
            || row.chunk_count != self.chunk_count
            || row.chunk_index != self.next_chunk_index
            || row.validity.end_order != self.end_order
        {
            return Err(CatalogError::Decode(
                "inline payload chunks are not contiguous".to_owned(),
            ));
        }
        self.next_chunk_index = self.next_chunk_index.saturating_add(1);
        if !self.visible {
            return Ok(());
        }
        for byte in row.payload {
            if self.line.len() >= INLINE_PAYLOAD_LIMIT_BYTES {
                return Err(CatalogError::Backend(format!(
                    "inline read snapshot row payload is over {} byte limit",
                    INLINE_PAYLOAD_LIMIT_BYTES
                )));
            }
            self.line.push(byte);
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line);
                consume_inline_payload_lines(self.begin_order, &line, accumulator)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        accumulator: &mut InlineCurrentAccumulator,
    ) -> CatalogResult<()> {
        if self.next_chunk_index != self.chunk_count {
            return Err(CatalogError::Decode(
                "inline payload chunks are not contiguous".to_owned(),
            ));
        }
        if self.visible && !self.line.is_empty() {
            let line = std::mem::take(&mut self.line);
            consume_inline_payload_lines(self.begin_order, &line, accumulator)?;
        }
        Ok(())
    }
}

pub(super) fn consume_inline_payload_lines(
    begin_order: CatalogOrderId,
    bytes: &[u8],
    accumulator: &mut InlineCurrentAccumulator,
) -> CatalogResult<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| CatalogError::Decode(format!("inline payload is not utf-8: {error}")))?;
    for line in text.lines().filter(|line| !line.is_empty()) {
        let row_id = crate::inline_change_feed::inline_row_id(line)?;
        accumulator.insert_payload_row(begin_order, row_id, line.as_bytes().to_vec())?;
    }
    Ok(())
}

pub(super) fn reconstruct_inline_current_rows_streaming<History>(
    history_stream: History,
    snapshot_order: CatalogOrderId,
    snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    current_rows: &BTreeMap<u64, (CatalogOrderId, RawSnapshotSequence)>,
    max_rows: usize,
    max_payload_bytes: usize,
    max_history_metadata_bytes: usize,
) -> CatalogResult<CapturedInlineRows>
where
    History: FnOnce(&mut InlineCurrentAccumulator) -> CatalogResult<()>,
{
    let mut accumulator =
        InlineCurrentAccumulator::new(max_rows, max_payload_bytes, max_history_metadata_bytes);
    history_stream(&mut accumulator)?;
    accumulator.finish_with_snapshot_order(snapshot_order, snapshot_sequences, current_rows)
}

impl InlineCurrentAccumulator {
    fn finish_with_snapshot_order(
        self,
        snapshot_order: CatalogOrderId,
        snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
        current_rows: &BTreeMap<u64, (CatalogOrderId, RawSnapshotSequence)>,
    ) -> CatalogResult<CapturedInlineRows> {
        let mut rows = Vec::with_capacity(self.current.len());
        for (row_id, (begin_order, payload)) in self.current {
            let sequence = snapshot_sequences
                .get(&begin_order)
                .copied()
                .or_else(|| {
                    current_rows
                        .get(&row_id)
                        .and_then(|(current_begin, sequence)| {
                            (*current_begin == begin_order && *current_begin <= snapshot_order)
                                .then_some(sequence.0)
                        })
                })
                .ok_or_else(|| {
                    CatalogError::Decode(format!(
                        "inline payload begin order {begin_order} has no canonical snapshot \
                         sequence"
                    ))
                })?;
            rows.push((
                row_id,
                InlineCurrentRow::new(begin_order, RawSnapshotSequence(sequence), payload),
            ));
        }
        Ok(CapturedInlineRows {
            rows,
            next_row_id: self.next_row_id,
        })
    }
}
