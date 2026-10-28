use super::{
    accumulator::{InlineCurrentAccumulator, InlinePayloadGroup},
    capture::CAPTURE_ALLOCATION_OVERHEAD_BYTES,
};
use std::collections::{BTreeSet, VecDeque};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, SchemaId, TableId,
    inline_change_feed::{InlineRowChange, InlineRowChangeKind},
    inline_data::{
        InlineTableChunkRow, decode_inline_end_marker, decode_inline_table_item,
        inline_table_prefix,
    },
    keys::{KeyFamily, family_prefix, prefix_end, table_schema_kind_inline_row_change_prefix},
};

const INLINE_HISTORY_PAGE_SIZE: usize = 256;
// End-order keys are metadata only, but a BTreeSet entry has allocator/node
// overhead in addition to the order bytes. Charge a conservative fixed amount
// so pre-indexing ended groups cannot grow beyond the capture budget.
const INLINE_END_MARKER_ENTRY_BYTES: usize =
    std::mem::size_of::<CatalogOrderId>() + CAPTURE_ALLOCATION_OVERHEAD_BYTES * 2;

struct InlineHistoryRangeCursor<'a> {
    kv: &'a dyn crate::OrderedCatalogKv,
    start: Vec<u8>,
    end: Vec<u8>,
    page: VecDeque<crate::RangeItem>,
    finished: bool,
}

impl<'a> InlineHistoryRangeCursor<'a> {
    fn new(kv: &'a dyn crate::OrderedCatalogKv, prefix: Vec<u8>) -> Self {
        Self {
            kv,
            end: prefix_end(&prefix),
            start: prefix,
            page: VecDeque::new(),
            finished: false,
        }
    }

    fn next(&mut self) -> CatalogResult<Option<crate::RangeItem>> {
        if self.page.is_empty() && !self.finished {
            if self.start >= self.end {
                self.finished = true;
                return Ok(None);
            }
            let page = self.kv.scan_range(
                &self.start,
                &self.end,
                crate::RangeDirection::Forward,
                INLINE_HISTORY_PAGE_SIZE,
            )?;
            if page.is_empty() {
                self.finished = true;
                return Ok(None);
            }
            if page.len() > INLINE_HISTORY_PAGE_SIZE {
                return Err(CatalogError::InvalidMutation(
                    "inline history page returned more items than requested".to_owned(),
                ));
            }
            let last_key = page.last().map(|item| item.key.clone()).ok_or_else(|| {
                CatalogError::InvalidMutation("inline history page was empty".to_owned())
            })?;
            self.start = key_after_inline_history_item(&last_key);
            self.page = page.into_iter().collect();
        }
        Ok(self.page.pop_front())
    }
}

fn key_after_inline_history_item(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len().saturating_add(1));
    next.extend_from_slice(key);
    next.push(0);
    next
}

#[derive(Clone, Copy)]
struct InlineEndMarker {
    end_order: CatalogOrderId,
    schema_id: SchemaId,
    begin_order: CatalogOrderId,
}

fn next_inline_payload_chunk(
    cursor: &mut InlineHistoryRangeCursor<'_>,
    catalog: CatalogId,
) -> CatalogResult<Option<InlineTableChunkRow>> {
    cursor.next()?.map_or(Ok(None), |item| {
        decode_inline_table_item(catalog, &item.key, &item.value).map(Some)
    })
}

fn next_inline_end_marker(
    cursor: &mut InlineHistoryRangeCursor<'_>,
    prefix: &[u8],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<InlineEndMarker>> {
    loop {
        let Some(item) = cursor.next()? else {
            return Ok(None);
        };
        let Some((end_order, schema_id, begin_order)) =
            decode_inline_end_marker(prefix, &item.key, snapshot_order)?
        else {
            // The EndOrder family is shared by data-file, delete-file, and
            // inline markers. Keep advancing until the inline marker stream is
            // exhausted; an unrelated sibling must not hide later inline rows.
            continue;
        };
        return Ok(Some(InlineEndMarker {
            end_order,
            schema_id,
            begin_order,
        }));
    }
}

pub(super) struct InlineHistoryRequest {
    pub(super) catalog: CatalogId,
    pub(super) table: TableId,
    pub(super) schema: SchemaId,
    pub(super) snapshot: CatalogOrderId,
}

impl InlineHistoryRequest {
    pub(super) fn stream(
        &self,
        kv: &impl crate::OrderedCatalogKv,
        accumulator: &mut InlineCurrentAccumulator,
    ) -> CatalogResult<()> {
        let mut payload = InlineHistoryRangeCursor::new(
            kv,
            inline_table_prefix(self.catalog, self.table, self.schema),
        );
        payload.end = crate::keys::prefix_end(&crate::inline_data::inline_table_payload_prefix(
            self.catalog,
            self.table,
            self.schema,
            self.snapshot,
        ));
        let mut deletions = InlineHistoryRangeCursor::new(kv, self.deletion_prefix());
        deletions.end = crate::keys::table_schema_kind_inline_row_change_scan_end(
            self.catalog,
            self.table,
            self.schema,
            InlineRowChangeKind::Deleted,
            self.snapshot,
        );
        let ended = self.load_ended_groups(kv, accumulator.max_history_metadata_bytes)?;
        self.fold_history(&mut payload, &mut deletions, ended, accumulator)
    }

    fn deletion_prefix(&self) -> Vec<u8> {
        table_schema_kind_inline_row_change_prefix(
            self.catalog,
            self.table,
            self.schema,
            InlineRowChangeKind::Deleted,
        )
    }

    fn load_ended_groups(
        &self,
        kv: &impl crate::OrderedCatalogKv,
        max_bytes: usize,
    ) -> CatalogResult<BTreeSet<CatalogOrderId>> {
        let mut prefix = family_prefix(self.catalog, KeyFamily::EndOrder);
        prefix.extend_from_slice(&self.table.0.to_be_bytes());
        prefix.push(b'/');
        let mut cursor = InlineHistoryRangeCursor::new(kv, prefix.clone());
        let mut through_snapshot = prefix.clone();
        through_snapshot.extend_from_slice(&self.snapshot.as_bytes());
        cursor.end = prefix_end(&through_snapshot);
        let mut ended = BTreeSet::new();
        let mut bytes = 0usize;
        while let Some(marker) = next_inline_end_marker(&mut cursor, &prefix, self.snapshot)? {
            if marker.end_order > self.snapshot {
                break;
            }
            if marker.schema_id != self.schema || !ended.insert(marker.begin_order) {
                continue;
            }
            bytes = bytes
                .checked_add(INLINE_END_MARKER_ENTRY_BYTES)
                .ok_or_else(|| {
                    CatalogError::Backend(
                        "inline end-marker metadata byte count overflowed".to_owned(),
                    )
                })?;
            if bytes > max_bytes {
                return Err(CatalogError::Backend(format!(
                    "inline end-marker metadata is {bytes} bytes, over history budget {max_bytes}"
                )));
            }
        }
        Ok(ended)
    }

    fn next_deletion(
        &self,
        cursor: &mut InlineHistoryRangeCursor<'_>,
        prefix: &[u8],
    ) -> CatalogResult<Option<InlineRowChange>> {
        cursor.next()?.map_or(Ok(None), |item| {
            crate::inline_change_feed::decode_schema_kind_inline_row_change_key(
                self.table,
                self.schema,
                InlineRowChangeKind::Deleted,
                prefix,
                &item.key,
                self.snapshot.kind(),
            )
            .map(Some)
        })
    }

    fn fold_history(
        &self,
        payload_cursor: &mut InlineHistoryRangeCursor<'_>,
        deletion_cursor: &mut InlineHistoryRangeCursor<'_>,
        mut ended: BTreeSet<CatalogOrderId>,
        accumulator: &mut InlineCurrentAccumulator,
    ) -> CatalogResult<()> {
        let mut payload = next_inline_payload_chunk(payload_cursor, self.catalog)?;
        let deletion_prefix = self.deletion_prefix();
        let mut deletion = self.next_deletion(deletion_cursor, &deletion_prefix)?;
        loop {
            let payload_order = payload
                .as_ref()
                .map(|row| row.validity.begin_order)
                .filter(|order| *order <= self.snapshot);
            let deletion_order = deletion
                .as_ref()
                .map(|change| change.order)
                .filter(|order| *order <= self.snapshot);
            let Some(order) = [payload_order, deletion_order].into_iter().flatten().min() else {
                break;
            };
            if deletion_order == Some(order) {
                let row = deletion.take().ok_or_else(|| {
                    CatalogError::Backend("inline deletion cursor lost its item".to_owned())
                })?;
                accumulator.remove_deleted(row.row_id, row.order);
                deletion = self.next_deletion(deletion_cursor, &deletion_prefix)?;
            } else {
                let first = payload.take().ok_or_else(|| {
                    CatalogError::Backend("inline payload cursor lost its item".to_owned())
                })?;
                payload =
                    self.consume_payload_group(first, payload_cursor, &mut ended, accumulator)?;
            }
        }
        Ok(())
    }

    fn consume_payload_group(
        &self,
        first: InlineTableChunkRow,
        cursor: &mut InlineHistoryRangeCursor<'_>,
        ended: &mut BTreeSet<CatalogOrderId>,
        accumulator: &mut InlineCurrentAccumulator,
    ) -> CatalogResult<Option<InlineTableChunkRow>> {
        let mut group = InlinePayloadGroup::new(&first, self.snapshot)?;
        if ended.remove(&group.begin_order) {
            group.visible = false;
        }
        group.push(first, accumulator)?;
        let next = loop {
            let Some(row) = next_inline_payload_chunk(cursor, self.catalog)? else {
                break None;
            };
            if row.validity.begin_order != group.begin_order {
                break Some(row);
            }
            group.push(row, accumulator)?;
        };
        group.finish(accumulator)?;
        Ok(next)
    }
}
