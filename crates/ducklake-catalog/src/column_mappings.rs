use crate::{
    CatalogId, CatalogResult, ColumnId, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, TableId,
    keys::{KeyFamily, family_prefix},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMappingRow {
    pub mapping_id: u64,
    pub table_id: TableId,
    pub mapping_type: String,
    pub columns: Vec<NameMappingColumnRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameMappingColumnRow {
    pub column_id: ColumnId,
    pub source_name: String,
    pub target_field_id: u64,
    pub parent_column: Option<u64>,
    pub is_partition: bool,
}

impl ColumnMappingRow {
    #[must_use]
    pub fn new(mapping_id: u64, table_id: TableId, mapping_type: impl Into<String>) -> Self {
        Self {
            mapping_id,
            table_id,
            mapping_type: mapping_type.into(),
            columns: Vec::new(),
        }
    }
}

pub fn put_column_mappings(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    rows: Vec<ColumnMappingRow>,
) -> CatalogResult<()> {
    let mut batch = KvBatch::default();
    for row in rows {
        batch.put(
            column_mapping_key(catalog, row.mapping_id),
            encode_mapping(&row),
        );
    }
    kv.commit(batch)
}

pub fn list_column_mappings(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    start_from: Option<u64>,
) -> CatalogResult<Vec<ColumnMappingRow>> {
    let mut rows = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::ColumnMapping),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| ColumnMappingRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.mapping_id);
    if let Some(start) = start_from {
        rows.retain(|row| row.mapping_id >= start);
    }
    Ok(rows)
}

fn column_mapping_key(catalog: CatalogId, mapping_id: u64) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::ColumnMapping);
    key.extend_from_slice(&mapping_id.to_be_bytes());
    key
}

fn encode_mapping(row: &ColumnMappingRow) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(1);
    out.extend_from_slice(&row.mapping_id.to_be_bytes());
    out.extend_from_slice(&row.table_id.0.to_be_bytes());
    encode_string(&mut out, &row.mapping_type);
    out.extend_from_slice(&(row.columns.len() as u32).to_be_bytes());
    for column in &row.columns {
        out.extend_from_slice(&column.column_id.0.to_be_bytes());
        encode_string(&mut out, &column.source_name);
        out.extend_from_slice(&column.target_field_id.to_be_bytes());
        match column.parent_column {
            Some(parent) => {
                out.push(1);
                out.extend_from_slice(&parent.to_be_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0_u64.to_be_bytes());
            }
        }
        out.push(u8::from(column.is_partition));
    }
    out
}

impl ColumnMappingRow {
    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(1) => {}
            Some(other) => {
                return Err(crate::CatalogError::Decode(format!(
                    "unsupported column mapping row version {other}"
                )));
            }
            None => {
                return Err(crate::CatalogError::Decode(
                    "empty column mapping row".to_owned(),
                ));
            }
        }
        let mut cursor = 1;
        let mapping_id = decode_u64(bytes, &mut cursor, "mapping id")?;
        let table_id = TableId(decode_u64(bytes, &mut cursor, "mapping table id")?);
        let mapping_type = decode_string(bytes, &mut cursor, "mapping type")?;
        let column_count = decode_u32(bytes, &mut cursor, "mapping column count")?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let column_id = ColumnId(decode_u64(bytes, &mut cursor, "mapping column id")?);
            let source_name = decode_string(bytes, &mut cursor, "mapping source name")?;
            let target_field_id = decode_u64(bytes, &mut cursor, "mapping target field id")?;
            let has_parent = decode_byte(bytes, &mut cursor, "mapping parent marker")?;
            let parent_value = decode_u64(bytes, &mut cursor, "mapping parent column")?;
            let is_partition = decode_byte(bytes, &mut cursor, "mapping partition marker")?;
            columns.push(NameMappingColumnRow {
                column_id,
                source_name,
                target_field_id,
                parent_column: match has_parent {
                    0 => None,
                    1 => Some(parent_value),
                    other => {
                        return Err(crate::CatalogError::Decode(format!(
                            "invalid mapping parent marker {other}"
                        )));
                    }
                },
                is_partition: match is_partition {
                    0 => false,
                    1 => true,
                    other => {
                        return Err(crate::CatalogError::Decode(format!(
                            "invalid mapping partition marker {other}"
                        )));
                    }
                },
            });
        }
        Ok(Self {
            mapping_id,
            table_id,
            mapping_type,
            columns,
        })
    }
}

fn encode_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn decode_string(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<String> {
    let len = decode_u32(bytes, cursor, field)? as usize;
    let raw = read_bytes(bytes, cursor, len)?;
    String::from_utf8(raw.to_vec())
        .map_err(|error| crate::CatalogError::Decode(format!("invalid utf-8 in {field}: {error}")))
}

fn decode_u64(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<u64> {
    let raw = read_bytes(bytes, cursor, 8)?;
    Ok(u64::from_be_bytes(raw.try_into().map_err(|_| {
        crate::CatalogError::Decode(format!("invalid {field} length"))
    })?))
}

fn decode_u32(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<u32> {
    let raw = read_bytes(bytes, cursor, 4)?;
    Ok(u32::from_be_bytes(raw.try_into().map_err(|_| {
        crate::CatalogError::Decode(format!("invalid {field} length"))
    })?))
}

fn decode_byte(bytes: &[u8], cursor: &mut usize, field: &str) -> CatalogResult<u8> {
    Ok(*read_bytes(bytes, cursor, 1)?
        .first()
        .ok_or_else(|| crate::CatalogError::Decode(format!("missing {field}")))?)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> CatalogResult<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| crate::CatalogError::Decode("column mapping cursor overflow".to_owned()))?;
    if end > bytes.len() {
        return Err(crate::CatalogError::Decode(
            "truncated column mapping row".to_owned(),
        ));
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

#[cfg(test)]
#[path = "column_mappings_tests.rs"]
mod column_mappings_tests;
