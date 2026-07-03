use crate::{
    CatalogError, CatalogResult, ValidityWindow,
    ids::{CatalogOrderId, SchemaId},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRow {
    pub schema_id: SchemaId,
    pub uuid: String,
    pub name: String,
    pub path: String,
    pub validity: ValidityWindow,
}

impl SchemaRow {
    const VERSION: u8 = 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(
        schema_id: SchemaId,
        uuid: impl Into<String>,
        name: impl Into<String>,
        path: impl Into<String>,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            schema_id,
            uuid: uuid.into(),
            name: name.into(),
            path: path.into(),
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            1 + 8 + STORED_ORDER_LEN * 2 + 1 + self.uuid.len() + self.name.len() + self.path.len(),
        );
        out.push(Self::VERSION);
        out.extend_from_slice(&self.schema_id.0.to_be_bytes());
        encode_stored_order(&mut out, self.validity.begin_order);
        match self.validity.end_order {
            Some(end_order) => {
                out.push(1);
                encode_stored_order(&mut out, end_order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        encode_string(&mut out, &self.uuid);
        encode_string(&mut out, &self.name);
        encode_string(&mut out, &self.path);
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(Self::VERSION) => {}
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported schema row version {other}"
                )));
            }
            None => return Err(CatalogError::Decode("empty schema row".to_owned())),
        }

        let mut cursor = 1;
        let schema_id = SchemaId(decode_fixed_u64(bytes, &mut cursor, "schema_id")?);
        let begin_order = decode_stored_order(
            read_bytes(bytes, &mut cursor, STORED_ORDER_LEN)?,
            "begin_order",
        )?;
        let has_end = *read_bytes(bytes, &mut cursor, 1)?
            .first()
            .ok_or_else(|| CatalogError::Decode("missing schema end marker".to_owned()))?;
        let decoded_end = decode_stored_order(
            read_bytes(bytes, &mut cursor, STORED_ORDER_LEN)?,
            "end_order",
        )?;
        let end_order = match has_end {
            0 => None,
            1 => Some(decoded_end),
            other => {
                return Err(CatalogError::Decode(format!(
                    "invalid schema end marker {other}"
                )));
            }
        };
        let uuid = decode_string(bytes, &mut cursor, "schema uuid")?;
        let name = decode_string(bytes, &mut cursor, "schema name")?;
        let path = decode_string(bytes, &mut cursor, "schema path")?;
        Ok(Self {
            schema_id,
            uuid,
            name,
            path,
            validity: ValidityWindow::new(begin_order, end_order),
        })
    }
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn decode_string(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<String> {
    let len = decode_fixed_u32(bytes, cursor, field)? as usize;
    let raw = read_bytes(bytes, cursor, len)?;
    String::from_utf8(raw.to_vec())
        .map_err(|error| CatalogError::Decode(format!("invalid utf-8 in {field}: {error}")))
}

fn decode_fixed_u64(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<u64> {
    let raw = read_bytes(bytes, cursor, 8)?;
    Ok(u64::from_be_bytes(raw.try_into().map_err(|_| {
        CatalogError::Decode(format!("invalid {field} length"))
    })?))
}

fn decode_fixed_u32(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<u32> {
    let raw = read_bytes(bytes, cursor, 4)?;
    Ok(u32::from_be_bytes(raw.try_into().map_err(|_| {
        CatalogError::Decode(format!("invalid {field} length"))
    })?))
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> CatalogResult<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| CatalogError::Decode("schema row cursor overflow".to_owned()))?;
    if end > bytes.len() {
        return Err(CatalogError::Decode("truncated schema row".to_owned()));
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}
