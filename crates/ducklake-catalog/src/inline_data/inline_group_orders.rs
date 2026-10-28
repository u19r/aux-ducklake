use std::collections::BTreeMap;

use super::{
    INLINE_PAYLOAD_LIMIT_BYTES, InlineTableChunkRow, decode_inline_table_item,
    inline_chunk_visible_at, inline_table_prefix, validate_contiguous_chunks,
};
use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, RangeDirection,
    SchemaId, TableId,
};

pub(crate) struct InlineGroupOrderRequest<'a> {
    pub(crate) catalog: CatalogId,
    pub(crate) table: TableId,
    pub(crate) schema: SchemaId,
    pub(crate) snapshot: CatalogOrderId,
    pub(crate) end_orders: &'a BTreeMap<CatalogOrderId, CatalogOrderId>,
}

impl InlineGroupOrderRequest<'_> {
    pub(crate) fn fetch(&self, kv: &impl OrderedCatalogKv) -> CatalogResult<Vec<CatalogOrderId>> {
        let prefix = inline_table_prefix(self.catalog, self.table, self.schema);
        let mut visible = BTreeMap::<CatalogOrderId, Vec<InlineTableChunkRow>>::new();
        for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
            let row = decode_inline_table_item(self.catalog, &item.key, &item.value)?;
            if inline_chunk_visible_at(&row, self.end_orders, self.snapshot) {
                visible
                    .entry(row.validity.begin_order)
                    .or_default()
                    .push(row);
            }
        }
        let mut orders = Vec::new();
        for (order, mut rows) in visible {
            rows.sort_by_key(|row| row.chunk_index);
            validate_payload_chunks(&rows)?;
            if !self.end_orders.contains_key(&order) {
                orders.push(order);
            }
        }
        Ok(orders)
    }
}

pub(crate) fn assemble_inline_payload(
    mut rows: Vec<InlineTableChunkRow>,
) -> CatalogResult<Vec<u8>> {
    rows.sort_by_key(|row| row.chunk_index);
    validate_payload_chunks(&rows)?;
    let mut payload = Vec::with_capacity(rows.iter().map(|row| row.payload.len()).sum());
    for row in rows {
        payload.extend_from_slice(&row.payload);
    }
    Ok(payload)
}

fn validate_payload_chunks(rows: &[InlineTableChunkRow]) -> CatalogResult<()> {
    let first = rows
        .first()
        .ok_or_else(|| CatalogError::Decode("inline payload has no chunks".to_owned()))?;
    validate_contiguous_chunks(rows.len(), first.chunk_count)?;
    let mut line_bytes = 0usize;
    for (index, row) in rows.iter().enumerate() {
        if row.chunk_count != first.chunk_count || row.chunk_index != index as u32 {
            return Err(CatalogError::Decode(
                "inline payload chunks are not contiguous".to_owned(),
            ));
        }
        for fragment in row.payload.split_inclusive(|byte| *byte == b'\n') {
            line_bytes = line_bytes.saturating_add(fragment.len());
            if fragment.last() == Some(&b'\n') {
                validate_row_bytes(line_bytes)?;
                line_bytes = 0;
            }
        }
    }
    if line_bytes > 0 {
        validate_row_bytes(line_bytes.saturating_add(1))?;
    }
    Ok(())
}

fn validate_row_bytes(bytes: usize) -> CatalogResult<()> {
    if bytes > INLINE_PAYLOAD_LIMIT_BYTES {
        return Err(CatalogError::InvalidMutation(format!(
            "inline row is {bytes} bytes, over {} byte limit",
            INLINE_PAYLOAD_LIMIT_BYTES,
        )));
    }
    Ok(())
}
