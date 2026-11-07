use crate::{
    CatalogError, CatalogResult, ValidityWindow,
    ids::{CatalogOrderId, MacroId, SchemaId},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroParameterRow {
    pub parameter_name: String,
    pub parameter_type: String,
    pub default_value: String,
    pub default_value_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroImplementationRow {
    pub dialect: String,
    pub sql: String,
    pub macro_type: String,
    pub parameters: Vec<MacroParameterRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroRow {
    pub macro_id: MacroId,
    pub schema_id: SchemaId,
    pub name: String,
    pub implementations: Vec<MacroImplementationRow>,
    pub validity: ValidityWindow,
}

impl MacroRow {
    const VERSION: u8 = 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(
        macro_id: MacroId,
        schema_id: SchemaId,
        name: impl Into<String>,
        implementations: Vec<MacroImplementationRow>,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            macro_id,
            schema_id,
            name: name.into(),
            implementations,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(Self::VERSION);
        out.extend_from_slice(&self.macro_id.0.to_be_bytes());
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
        encode_string(&mut out, &self.name);
        out.extend_from_slice(&(self.implementations.len() as u32).to_be_bytes());
        for implementation in &self.implementations {
            encode_string(&mut out, &implementation.dialect);
            encode_string(&mut out, &implementation.sql);
            encode_string(&mut out, &implementation.macro_type);
            out.extend_from_slice(&(implementation.parameters.len() as u32).to_be_bytes());
            for parameter in &implementation.parameters {
                encode_string(&mut out, &parameter.parameter_name);
                encode_string(&mut out, &parameter.parameter_type);
                encode_string(&mut out, &parameter.default_value);
                encode_string(&mut out, &parameter.default_value_type);
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(Self::VERSION) => {}
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported macro row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "macro row is too short: 0 bytes".to_owned(),
                ));
            }
        }
        let minimum_len = 1 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "macro row is too short: {} bytes",
                bytes.len()
            )));
        }
        let macro_start = 1;
        let schema_start = macro_start + 8;
        let begin_start = schema_start + 8;
        let end_present_index = begin_start + STORED_ORDER_LEN;
        let end_start = end_present_index + 1;
        let mut offset = end_start + STORED_ORDER_LEN;
        let macro_id = MacroId(decode_fixed_u64(
            bytes,
            macro_start,
            schema_start,
            "macro id",
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
                    "invalid macro end-order marker {other}"
                )));
            }
        };
        let name = decode_string(bytes, &mut offset, "macro name")?;
        let implementation_count = decode_u32(bytes, &mut offset, "macro implementation count")?;
        let mut implementations = Vec::with_capacity(implementation_count as usize);
        for _ in 0..implementation_count {
            implementations.push(decode_implementation(bytes, &mut offset)?);
        }
        Ok(Self {
            macro_id,
            schema_id,
            name,
            implementations,
            validity: ValidityWindow::new(begin_order, end_order),
        })
    }
}

fn decode_implementation(
    bytes: &[u8],
    offset: &mut usize,
) -> CatalogResult<MacroImplementationRow> {
    let dialect = decode_string(bytes, offset, "macro dialect")?;
    let sql = decode_string(bytes, offset, "macro sql")?;
    let macro_type = decode_string(bytes, offset, "macro type")?;
    let parameter_count = decode_u32(bytes, offset, "macro parameter count")?;
    let mut parameters = Vec::with_capacity(parameter_count as usize);
    for _ in 0..parameter_count {
        parameters.push(MacroParameterRow {
            parameter_name: decode_string(bytes, offset, "macro parameter name")?,
            parameter_type: decode_string(bytes, offset, "macro parameter type")?,
            default_value: decode_string(bytes, offset, "macro parameter default value")?,
            default_value_type: decode_string(bytes, offset, "macro parameter default type")?,
        });
    }
    Ok(MacroImplementationRow {
        dialect,
        sql,
        macro_type,
        parameters,
    })
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
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

fn decode_fixed_u64(bytes: &[u8], start: usize, end: usize, field: &str) -> CatalogResult<u64> {
    Ok(u64::from_be_bytes(bytes[start..end].try_into().map_err(
        |_| CatalogError::Decode(format!("{field} is truncated")),
    )?))
}
