use crate::{
    CatalogError, CatalogResult, ValidityWindow,
    ids::{CatalogOrderId, SchemaId, TableId},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRow {
    pub view_id: TableId,
    pub schema_id: SchemaId,
    pub uuid: String,
    pub name: String,
    pub dialect: String,
    pub sql: String,
    pub column_aliases: Vec<String>,
    pub comment: Option<String>,
    pub validity: ValidityWindow,
}

impl ViewRow {
    const VERSION: u8 = 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(
        view_id: TableId,
        schema_id: SchemaId,
        uuid: impl Into<String>,
        name: impl Into<String>,
        dialect: impl Into<String>,
        sql: impl Into<String>,
        column_aliases: Vec<String>,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            view_id,
            schema_id,
            uuid: uuid.into(),
            name: name.into(),
            dialect: dialect.into(),
            sql: sql.into(),
            column_aliases,
            comment: None,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn with_comment(mut self, comment: Option<impl Into<String>>) -> Self {
        self.comment = comment.map(Into::into);
        self
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(Self::VERSION);
        out.extend_from_slice(&self.view_id.0.to_be_bytes());
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
        encode_string(&mut out, &self.dialect);
        encode_string(&mut out, &self.sql);
        encode_optional_string(&mut out, self.comment.as_deref());
        out.extend_from_slice(&(self.column_aliases.len() as u32).to_be_bytes());
        for alias in &self.column_aliases {
            encode_string(&mut out, alias);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(Self::VERSION) => {}
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported view row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "view row is too short: 0 bytes".to_owned(),
                ));
            }
        }
        let minimum_len = 1 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "view row is too short: {} bytes",
                bytes.len()
            )));
        }
        let view_start = 1;
        let schema_start = view_start + 8;
        let begin_start = schema_start + 8;
        let end_present_index = begin_start + STORED_ORDER_LEN;
        let end_start = end_present_index + 1;
        let mut offset = end_start + STORED_ORDER_LEN;
        let view_id = TableId(decode_fixed_u64(
            bytes,
            view_start,
            schema_start,
            "view id",
        )?);
        let schema_id = SchemaId(decode_fixed_u64(
            bytes,
            schema_start,
            begin_start,
            "schema id",
        )?);
        let begin_order =
            decode_stored_order(&bytes[begin_start..end_present_index], "begin_order")?;
        let end_order = match bytes[end_present_index] {
            0 => None,
            1 => Some(decode_stored_order(&bytes[end_start..offset], "end_order")?),
            other => {
                return Err(CatalogError::Decode(format!(
                    "invalid view end-order marker {other}"
                )));
            }
        };
        let uuid = decode_string(bytes, &mut offset, "view uuid")?;
        let name = decode_string(bytes, &mut offset, "view name")?;
        let dialect = decode_string(bytes, &mut offset, "view dialect")?;
        let sql = decode_string(bytes, &mut offset, "view sql")?;
        let comment = decode_optional_string(bytes, &mut offset, "view comment")?;
        let alias_count = decode_u32(bytes, &mut offset, "view alias count")? as usize;
        let mut column_aliases = Vec::with_capacity(alias_count);
        for _ in 0..alias_count {
            column_aliases.push(decode_string(bytes, &mut offset, "view alias")?);
        }
        Ok(Self {
            view_id,
            schema_id,
            uuid,
            name,
            dialect,
            sql,
            column_aliases,
            comment,
            validity: ValidityWindow::new(begin_order, end_order),
        })
    }
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn encode_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            encode_string(out, value);
        }
        None => out.push(0),
    }
}

fn decode_string(bytes: &[u8], offset: &mut usize, field: &str) -> CatalogResult<String> {
    let len = decode_u32(bytes, offset, field)? as usize;
    let end = offset.saturating_add(len);
    if end > bytes.len() {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let value = std::str::from_utf8(&bytes[*offset..end])
        .map_err(|err| CatalogError::Decode(format!("{field} is not utf8: {err}")))?
        .to_owned();
    *offset = end;
    Ok(value)
}

fn decode_optional_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> CatalogResult<Option<String>> {
    let present = decode_bool(bytes, offset, field)?;
    if present {
        decode_string(bytes, offset, field).map(Some)
    } else {
        Ok(None)
    }
}

fn decode_u32(bytes: &[u8], offset: &mut usize, field: &str) -> CatalogResult<u32> {
    let end = offset.saturating_add(4);
    if end > bytes.len() {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let value = u32::from_be_bytes(
        bytes[*offset..end]
            .try_into()
            .map_err(|_| CatalogError::Decode(format!("{field} is truncated")))?,
    );
    *offset = end;
    Ok(value)
}

fn decode_bool(bytes: &[u8], offset: &mut usize, field: &str) -> CatalogResult<bool> {
    if *offset >= bytes.len() {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let value = match bytes[*offset] {
        0 => false,
        1 => true,
        other => {
            return Err(CatalogError::Decode(format!(
                "{field} has invalid bool marker {other}"
            )));
        }
    };
    *offset += 1;
    Ok(value)
}

fn decode_fixed_u64(bytes: &[u8], start: usize, end: usize, field: &str) -> CatalogResult<u64> {
    Ok(u64::from_be_bytes(bytes[start..end].try_into().map_err(
        |_| CatalogError::Decode(format!("{field} is truncated")),
    )?))
}
