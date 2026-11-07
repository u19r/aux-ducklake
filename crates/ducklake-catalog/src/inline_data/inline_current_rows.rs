use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, RangeDirection,
    RawSnapshotSequence, SchemaId, TableId,
    keys::{inline_current_row_prefix, inline_next_row_id_key, prefix_end},
    kv::scan_range_pages,
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

    pub(crate) fn decode_metadata(
        bytes: &[u8],
    ) -> CatalogResult<(CatalogOrderId, RawSnapshotSequence)> {
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
        let begin_order = decode_stored_order(
            &bytes[1..1 + STORED_ORDER_LEN],
            "inline current-row begin order",
        )?;
        let begin_sequence = RawSnapshotSequence(u64::from_be_bytes(
            bytes[1 + STORED_ORDER_LEN..HEADER_LEN]
                .try_into()
                .map_err(|_| {
                    CatalogError::Decode(
                        "inline current-row begin sequence is truncated".to_owned(),
                    )
                })?,
        ));
        Ok((begin_order, begin_sequence))
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

/// Read at most the number of current rows that the runtime capture budget can
/// account for. A bounded scan asks for one extra item so a truncated range is
/// reported as an error instead of being mistaken for a complete snapshot.
pub(crate) fn list_inline_current_rows_bounded(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    max_rows: usize,
    max_payload_bytes: usize,
) -> CatalogResult<Vec<(u64, InlineCurrentRow)>> {
    let prefix = inline_current_row_prefix(catalog, table_id, schema_id);
    let mut payload_bytes = 0usize;
    let mut rows = Vec::new();
    scan_range_pages(
        kv,
        &prefix,
        &prefix_end(&prefix),
        RangeDirection::Forward,
        Some(max_rows),
        |item| {
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
            if rows.len() >= max_rows {
                return Err(CatalogError::Backend(format!(
                    "inline read snapshot has more than {max_rows} current rows"
                )));
            }
            if item.value.len() > HEADER_LEN.saturating_add(super::INLINE_PAYLOAD_LIMIT_BYTES) {
                return Err(CatalogError::Backend(format!(
                    "inline current-row value is {} bytes, over the canonical {} byte row limit",
                    item.value.len(),
                    HEADER_LEN.saturating_add(super::INLINE_PAYLOAD_LIMIT_BYTES),
                )));
            }
            let encoded_payload_bytes = item.value.len().saturating_sub(HEADER_LEN);
            let next_payload_bytes = payload_bytes
                .checked_add(encoded_payload_bytes)
                .ok_or_else(|| {
                    CatalogError::Backend(
                        "inline read snapshot payload byte count overflowed".to_owned(),
                    )
                })?;
            if next_payload_bytes > max_payload_bytes {
                return Err(CatalogError::Backend(format!(
                    "inline read snapshot payload is {next_payload_bytes} bytes, over capture budget {max_payload_bytes}"
                )));
            }
            let row = InlineCurrentRow::decode(&item.value)?;
            payload_bytes = next_payload_bytes;
            rows.push((row_id, row));
            Ok(())
        },
    )?;
    Ok(rows)
}

/// Read only the order and sequence metadata needed to reconstruct historical
/// inline rows. This keeps the current-row fallback bounded without retaining a
/// second copy of every row payload beside the reconstructed view.
pub(crate) fn list_inline_current_row_metadata_bounded(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    max_rows: usize,
) -> CatalogResult<std::collections::BTreeMap<u64, (CatalogOrderId, RawSnapshotSequence)>> {
    let prefix = inline_current_row_prefix(catalog, table_id, schema_id);
    let mut rows = std::collections::BTreeMap::new();
    scan_range_pages(
        kv,
        &prefix,
        &prefix_end(&prefix),
        RangeDirection::Forward,
        Some(max_rows),
        |item| {
            if rows.len() >= max_rows {
                return Err(CatalogError::Backend(format!(
                    "inline read snapshot has more than {max_rows} current rows"
                )));
            }
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
            let metadata = InlineCurrentRow::decode_metadata(&item.value)?;
            rows.insert(row_id, metadata);
            Ok(())
        },
    )?;
    Ok(rows)
}

pub(crate) fn load_inline_next_row_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
) -> CatalogResult<u64> {
    let value = kv.get(&inline_next_row_id_key(catalog, table_id, schema_id))?;
    decode_inline_next_row_id(value.as_deref())
}

pub(crate) fn decode_inline_next_row_id(value: Option<&[u8]>) -> CatalogResult<u64> {
    let Some(value) = value else {
        return Ok(0);
    };
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        CatalogError::Decode("inline next-row-id watermark must contain eight bytes".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}
