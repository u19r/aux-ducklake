use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, TableId, ValidityWindow,
    ids::{CatalogOrderId, DataFileId},
    inline_data::{
        chunk_bounds, chunk_count, decode_inline_end_order, encode_inline_end_order,
        validate_contiguous_chunks, validate_inline_payload_size,
    },
    keys::{KeyFamily, family_prefix},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    store::{latest_snapshot, snapshot_row_for_next_sequence, stage_snapshot},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineDeletionChunkRow {
    pub table_id: TableId,
    pub data_file_id: DataFileId,
    pub row_id: u64,
    pub validity: ValidityWindow,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub payload: Vec<u8>,
}

impl InlineDeletionChunkRow {
    const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        table_id: TableId,
        data_file_id: DataFileId,
        row_id: u64,
        validity: ValidityWindow,
        chunk_index: u32,
        chunk_count: u32,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            table_id,
            data_file_id,
            row_id,
            validity,
            chunk_index,
            chunk_count,
            payload,
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(1 + 8 + 8 + 8 + STORED_ORDER_LEN * 2 + 10 + self.payload.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.row_id.to_be_bytes());
        encode_stored_order(&mut out, self.validity.begin_order);
        encode_inline_end_order(&mut out, self.validity);
        out.extend_from_slice(&self.chunk_index.to_be_bytes());
        out.extend_from_slice(&self.chunk_count.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        const HEADER_LEN: usize =
            1 + 8 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN + 4 + 4 + 4;
        if bytes.len() < HEADER_LEN {
            return Err(CatalogError::Decode(format!(
                "inline deletion chunk row is too short: {} bytes",
                bytes.len()
            )));
        }
        if bytes[0] != Self::VERSION {
            return Err(CatalogError::Decode(format!(
                "unsupported inline deletion chunk row version {}",
                bytes[0]
            )));
        }
        let table_start = 1;
        let data_file_start = table_start + 8;
        let row_id_start = data_file_start + 8;
        let begin_start = row_id_start + 8;
        let end_flag = begin_start + STORED_ORDER_LEN;
        let end_start = end_flag + 1;
        let chunk_index_start = end_start + STORED_ORDER_LEN;
        let chunk_count_start = chunk_index_start + 4;
        let payload_len_start = chunk_count_start + 4;
        let payload_start = payload_len_start + 4;
        let payload_len =
            u32::from_be_bytes(bytes[payload_len_start..payload_start].try_into().map_err(
                |_| CatalogError::Decode("inline deletion payload length is truncated".to_owned()),
            )?) as usize;
        let payload_end = payload_start.saturating_add(payload_len);
        if bytes.len() != payload_end {
            return Err(CatalogError::Decode(format!(
                "inline deletion chunk payload must be {payload_len} bytes, got {}",
                bytes.len().saturating_sub(payload_start)
            )));
        }
        Ok(Self {
            table_id: TableId(u64::from_be_bytes(
                bytes[table_start..data_file_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("inline table id is truncated".to_owned()))?,
            )),
            data_file_id: DataFileId(u64::from_be_bytes(
                bytes[data_file_start..row_id_start]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode("inline data file id is truncated".to_owned())
                    })?,
            )),
            row_id: u64::from_be_bytes(
                bytes[row_id_start..begin_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("inline row id is truncated".to_owned()))?,
            ),
            validity: ValidityWindow::new(
                decode_stored_order(&bytes[begin_start..end_flag], "inline deletion begin order")?,
                decode_inline_end_order(bytes[end_flag], &bytes[end_start..chunk_index_start])?,
            ),
            chunk_index: u32::from_be_bytes(
                bytes[chunk_index_start..chunk_count_start]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode("inline chunk index is truncated".to_owned())
                    })?,
            ),
            chunk_count: u32::from_be_bytes(
                bytes[chunk_count_start..payload_len_start]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode("inline chunk count is truncated".to_owned())
                    })?,
            ),
            payload: bytes[payload_start..payload_end].to_vec(),
        })
    }
}

pub fn register_inline_deletion_payload(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    row_id: u64,
    payload: Vec<u8>,
) -> CatalogResult<Vec<InlineDeletionChunkRow>> {
    validate_inline_payload_size(payload.len())?;
    let latest = latest_snapshot(kv, catalog)?;
    let begin_order = kv.generated_order_id()?;
    let snapshot = snapshot_row_for_next_sequence(latest, begin_order);
    let rows = inline_deletion_chunks(table_id, data_file_id, row_id, begin_order, payload)?;
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    for row in &rows {
        batch.put(
            inline_deletion_chunk_key(
                catalog,
                table_id,
                data_file_id,
                row_id,
                begin_order,
                row.chunk_index,
            ),
            row.encode(),
        );
    }
    kv.commit(batch)?;
    Ok(rows)
}

pub fn load_inline_deletion_payload_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    row_id: u64,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<Vec<u8>>> {
    let mut visible = BTreeMap::<CatalogOrderId, Vec<InlineDeletionChunkRow>>::new();
    for item in kv.scan_prefix(
        &inline_deletion_prefix(catalog, table_id, data_file_id, row_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = InlineDeletionChunkRow::decode(&item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            visible
                .entry(row.validity.begin_order)
                .or_default()
                .push(row);
        }
    }
    let Some((_, rows)) = visible.into_iter().next_back() else {
        return Ok(None);
    };
    assemble_inline_deletion_payload(rows).map(Some)
}

fn inline_deletion_chunks(
    table_id: TableId,
    data_file_id: DataFileId,
    row_id: u64,
    begin_order: CatalogOrderId,
    payload: Vec<u8>,
) -> CatalogResult<Vec<InlineDeletionChunkRow>> {
    let chunk_count = chunk_count(payload.len())?;
    let validity = ValidityWindow::new(begin_order, None);
    let mut rows = Vec::with_capacity(chunk_count as usize);
    for chunk_index in 0..chunk_count {
        let (start, end) = chunk_bounds(payload.len(), chunk_index);
        rows.push(InlineDeletionChunkRow::new(
            table_id,
            data_file_id,
            row_id,
            validity,
            chunk_index,
            chunk_count,
            payload[start..end].to_vec(),
        ));
    }
    Ok(rows)
}

fn assemble_inline_deletion_payload(
    mut rows: Vec<InlineDeletionChunkRow>,
) -> CatalogResult<Vec<u8>> {
    if rows.is_empty() {
        return Err(CatalogError::Decode(
            "inline deletion payload has no chunks".to_owned(),
        ));
    }
    rows.sort_by_key(|row| row.chunk_index);
    let chunk_count = rows[0].chunk_count;
    validate_contiguous_chunks(rows.len(), chunk_count)?;
    let mut payload = Vec::new();
    for (expected_index, row) in rows.into_iter().enumerate() {
        if row.chunk_count != chunk_count || row.chunk_index != expected_index as u32 {
            return Err(CatalogError::Decode(
                "inline deletion payload chunks are not contiguous".to_owned(),
            ));
        }
        payload.extend_from_slice(&row.payload);
    }
    validate_inline_payload_size(payload.len())?;
    Ok(payload)
}

fn inline_deletion_prefix(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    row_id: u64,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::InlineDeletion);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&row_id.to_be_bytes());
    key.push(b'/');
    key
}

fn inline_deletion_chunk_key(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    row_id: u64,
    begin_order: CatalogOrderId,
    chunk_index: u32,
) -> Vec<u8> {
    let mut key = inline_deletion_prefix(catalog, table_id, data_file_id, row_id);
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&chunk_index.to_be_bytes());
    key
}
