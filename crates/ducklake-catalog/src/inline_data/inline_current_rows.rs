use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SchemaId, TableId,
    keys::{inline_current_row_prefix, inline_next_row_id_key},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
};

const VERSION: u8 = 1;
const HEADER_LEN: usize = 1 + STORED_ORDER_LEN + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InlineCurrentRow {
    pub(crate) begin_order: CatalogOrderId,
    pub(crate) begin_sequence: RawSnapshotSequence,
    pub(crate) payload: Vec<u8>,
}

impl InlineCurrentRow {
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 2;

    pub(crate) fn new(
        begin_order: CatalogOrderId,
        begin_sequence: RawSnapshotSequence,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            begin_order,
            begin_sequence,
            payload,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(VERSION);
        encode_stored_order(&mut out, self.begin_order);
        out.extend_from_slice(&self.begin_sequence.0.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(CatalogError::Decode(format!(
                "inline current row is truncated: expected at least {HEADER_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        if bytes[0] != VERSION {
            return Err(CatalogError::Decode(format!(
                "unsupported inline current-row version {}",
                bytes[0]
            )));
        }
        Ok(Self {
            begin_order: decode_stored_order(
                &bytes[1..1 + STORED_ORDER_LEN],
                "inline current-row begin order",
            )?,
            begin_sequence: RawSnapshotSequence(u64::from_be_bytes(
                bytes[1 + STORED_ORDER_LEN..HEADER_LEN]
                    .try_into()
                    .map_err(|_| {
                        CatalogError::Decode(
                            "inline current-row begin sequence is truncated".to_owned(),
                        )
                    })?,
            )),
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

pub(crate) fn list_inline_current_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
) -> CatalogResult<Vec<(u64, InlineCurrentRow)>> {
    let prefix = inline_current_row_prefix(catalog, table_id, schema_id);
    kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)?
        .into_iter()
        .map(|item| {
            let row_id = item
                .key
                .strip_prefix(prefix.as_slice())
                .and_then(|suffix| suffix.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| {
                    CatalogError::InvalidKey(
                        "inline current-row key must end with an eight-byte row id".to_owned(),
                    )
                })?;
            Ok((row_id, InlineCurrentRow::decode(&item.value)?))
        })
        .collect()
}

pub(crate) fn load_inline_next_row_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
) -> CatalogResult<u64> {
    let Some(value) = kv.get(&inline_next_row_id_key(catalog, table_id, schema_id))? else {
        return Ok(0);
    };
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        CatalogError::Decode("inline next-row-id watermark must contain eight bytes".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}
