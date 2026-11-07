use crate::{
    CatalogError, CatalogResult, ValidityWindow,
    ids::{CatalogOrderId, ColumnId, SchemaId, TableId},
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    table_partition_rows::{TablePartitionFieldRow, TablePartitionRow},
    table_sort_rows::{TableSortFieldRow, TableSortRow},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnRow {
    pub column_id: ColumnId,
    pub name: String,
    pub column_type: String,
    pub nulls_allowed: bool,
    pub parent_id: Option<ColumnId>,
    pub comment: Option<String>,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub default_value_type: String,
    pub created_with_table: bool,
}

impl TableColumnRow {
    #[must_use]
    pub fn new(
        column_id: ColumnId,
        name: impl Into<String>,
        column_type: impl Into<String>,
        nulls_allowed: bool,
        parent_id: Option<ColumnId>,
    ) -> Self {
        Self {
            column_id,
            name: name.into(),
            column_type: column_type.into(),
            nulls_allowed,
            parent_id,
            comment: None,
            initial_default: None,
            default_value: None,
            default_value_type: "literal".to_owned(),
            created_with_table: true,
        }
    }

    #[must_use]
    pub fn with_comment(mut self, comment: Option<impl Into<String>>) -> Self {
        self.comment = comment.map(Into::into);
        self
    }

    #[must_use]
    pub fn with_default_metadata(
        mut self,
        initial_default: Option<impl Into<String>>,
        default_value: Option<impl Into<String>>,
        default_value_type: impl Into<String>,
    ) -> Self {
        self.initial_default = initial_default.map(Into::into);
        self.default_value = default_value.map(Into::into);
        self.default_value_type = default_value_type.into();
        self
    }

    #[must_use]
    pub fn with_created_with_table(mut self, created_with_table: bool) -> Self {
        self.created_with_table = created_with_table;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinedTableRow {
    pub table_name: String,
    pub schema_version: u64,
}

impl InlinedTableRow {
    #[must_use]
    pub fn new(table_name: impl Into<String>, schema_version: u64) -> Self {
        Self {
            table_name: table_name.into(),
            schema_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub uuid: String,
    pub name: String,
    pub path: String,
    pub comment: Option<String>,
    pub columns: Vec<TableColumnRow>,
    pub inlined_data_tables: Vec<InlinedTableRow>,
    pub partition: Option<TablePartitionRow>,
    pub sort: Option<TableSortRow>,
    pub validity: ValidityWindow,
}

impl TableRow {
    const VERSION: u8 = 8;
    #[cfg(feature = "foundationdb")]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 1;
    #[cfg(feature = "foundationdb")]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(table_id: TableId, name: impl Into<String>, begin_order: CatalogOrderId) -> Self {
        Self {
            table_id,
            schema_id: SchemaId(0),
            uuid: String::new(),
            name: name.into(),
            path: String::new(),
            comment: None,
            columns: Vec::new(),
            inlined_data_tables: Vec::new(),
            partition: None,
            sort: None,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn with_catalog_metadata(
        table_id: TableId,
        schema_id: SchemaId,
        uuid: impl Into<String>,
        name: impl Into<String>,
        path: impl Into<String>,
        columns: Vec<TableColumnRow>,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            table_id,
            schema_id,
            uuid: uuid.into(),
            name: name.into(),
            path: path.into(),
            comment: None,
            columns,
            inlined_data_tables: Vec::new(),
            partition: None,
            sort: None,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub(crate) fn same_user_visible_schema_as(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.validity = right.validity;
        left.inlined_data_tables.clear();
        right.inlined_data_tables.clear();
        left == right
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            1 + 8
                + 8
                + STORED_ORDER_LEN * 2
                + 1
                + self.uuid.len()
                + self.name.len()
                + self.path.len(),
        );
        out.push(Self::VERSION);
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
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
        encode_optional_string(&mut out, self.comment.as_deref());
        out.extend_from_slice(&(self.columns.len() as u32).to_be_bytes());
        for column in &self.columns {
            out.extend_from_slice(&column.column_id.0.to_be_bytes());
            out.push(u8::from(column.nulls_allowed));
            match column.parent_id {
                Some(parent_id) => {
                    out.push(1);
                    out.extend_from_slice(&parent_id.0.to_be_bytes());
                }
                None => {
                    out.push(0);
                    out.extend_from_slice(&0_u64.to_be_bytes());
                }
            }
            encode_string(&mut out, &column.name);
            encode_string(&mut out, &column.column_type);
            encode_optional_string(&mut out, column.comment.as_deref());
            encode_optional_string(&mut out, column.initial_default.as_deref());
            encode_optional_string(&mut out, column.default_value.as_deref());
            encode_string(&mut out, &column.default_value_type);
            out.push(u8::from(column.created_with_table));
        }
        out.extend_from_slice(&(self.inlined_data_tables.len() as u32).to_be_bytes());
        for inlined_table in &self.inlined_data_tables {
            encode_string(&mut out, &inlined_table.table_name);
            out.extend_from_slice(&inlined_table.schema_version.to_be_bytes());
        }
        match &self.partition {
            Some(partition) => {
                out.push(1);
                out.extend_from_slice(&partition.partition_id.to_be_bytes());
                out.extend_from_slice(&(partition.fields.len() as u32).to_be_bytes());
                for field in &partition.fields {
                    out.extend_from_slice(&field.partition_key_index.to_be_bytes());
                    out.extend_from_slice(&field.column_id.0.to_be_bytes());
                    encode_string(&mut out, &field.transform);
                }
            }
            None => out.push(0),
        }
        match &self.sort {
            Some(sort) => {
                out.push(1);
                out.extend_from_slice(&sort.sort_id.to_be_bytes());
                out.extend_from_slice(&(sort.fields.len() as u32).to_be_bytes());
                for field in &sort.fields {
                    out.extend_from_slice(&field.sort_key_index.to_be_bytes());
                    encode_string(&mut out, &field.expression);
                    encode_string(&mut out, &field.dialect);
                    encode_string(&mut out, &field.sort_direction);
                    encode_string(&mut out, &field.null_order);
                }
            }
            None => out.push(0),
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(Self::VERSION) => {}
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported table row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "table row is too short: 0 bytes".to_owned(),
                ));
            }
        }
        let minimum_len = 1 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "table row is too short: {} bytes",
                bytes.len()
            )));
        }
        let table_start = 1;
        let schema_start = table_start + 8;
        let begin_start = schema_start + 8;
        let end_present_index = begin_start + STORED_ORDER_LEN;
        let end_start = end_present_index + 1;
        let mut offset = end_start + STORED_ORDER_LEN;
        let table_id = TableId(u64::from_be_bytes(
            bytes[table_start..schema_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("table id is truncated".to_owned()))?,
        ));
        let schema_id = SchemaId(u64::from_be_bytes(
            bytes[schema_start..begin_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("schema id is truncated".to_owned()))?,
        ));
        let begin_order =
            decode_stored_order(&bytes[begin_start..end_present_index], "begin_order")?;
        let end_order = match bytes[end_present_index] {
            0 => None,
            1 => Some(decode_stored_order(&bytes[end_start..offset], "end_order")?),
            other => {
                return Err(CatalogError::Decode(format!(
                    "invalid table end-order marker {other}"
                )));
            }
        };
        let uuid = decode_string(bytes, &mut offset, "table uuid")?;
        let name = decode_string(bytes, &mut offset, "table name")?;
        let path = decode_string(bytes, &mut offset, "table path")?;
        let comment = decode_optional_string(bytes, &mut offset, "table comment")?;
        let column_count = decode_u32(bytes, &mut offset, "column count")? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column_id = ColumnId(decode_u64(bytes, &mut offset, "column id")?);
            let nulls_allowed = decode_bool(bytes, &mut offset, "column nulls_allowed")?;
            let has_parent = decode_bool(bytes, &mut offset, "column parent marker")?;
            let parent_value = decode_u64(bytes, &mut offset, "column parent id")?;
            let parent_id = has_parent.then_some(ColumnId(parent_value));
            let column_name = decode_string(bytes, &mut offset, "column name")?;
            let column_type = decode_string(bytes, &mut offset, "column type")?;
            let comment = decode_optional_string(bytes, &mut offset, "column comment")?;
            let initial_default =
                decode_optional_string(bytes, &mut offset, "column initial default")?;
            let default_value = decode_optional_string(bytes, &mut offset, "column default value")?;
            let default_value_type =
                decode_string(bytes, &mut offset, "column default value type")?;
            let created_with_table = decode_bool(bytes, &mut offset, "column created with table")?;
            columns.push(
                TableColumnRow::new(
                    column_id,
                    column_name,
                    column_type,
                    nulls_allowed,
                    parent_id,
                )
                .with_default_metadata(initial_default, default_value, default_value_type)
                .with_comment(comment)
                .with_created_with_table(created_with_table),
            );
        }
        let inlined_table_count = decode_u32(bytes, &mut offset, "inlined table count")? as usize;
        let mut inlined_data_tables = Vec::with_capacity(inlined_table_count);
        for _ in 0..inlined_table_count {
            let table_name = decode_string(bytes, &mut offset, "inlined table name")?;
            let schema_version = decode_u64(bytes, &mut offset, "inlined table schema version")?;
            inlined_data_tables.push(InlinedTableRow::new(table_name, schema_version));
        }
        let partition = if decode_bool(bytes, &mut offset, "table partition marker")? {
            let partition_id = decode_u64(bytes, &mut offset, "table partition id")?;
            let field_count =
                decode_u32(bytes, &mut offset, "table partition field count")? as usize;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(TablePartitionFieldRow::new(
                    decode_u64(bytes, &mut offset, "table partition key index")?,
                    ColumnId(decode_u64(bytes, &mut offset, "table partition column id")?),
                    decode_string(bytes, &mut offset, "table partition transform")?,
                ));
            }
            Some(TablePartitionRow::new(partition_id, fields))
        } else {
            None
        };
        let sort = if decode_bool(bytes, &mut offset, "table sort marker")? {
            let sort_id = decode_u64(bytes, &mut offset, "table sort id")?;
            let field_count = decode_u32(bytes, &mut offset, "table sort field count")? as usize;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(TableSortFieldRow::new(
                    decode_u64(bytes, &mut offset, "table sort key index")?,
                    decode_string(bytes, &mut offset, "table sort expression")?,
                    decode_string(bytes, &mut offset, "table sort dialect")?,
                    decode_string(bytes, &mut offset, "table sort direction")?,
                    decode_string(bytes, &mut offset, "table sort null order")?,
                ));
            }
            Some(TableSortRow::new(sort_id, fields))
        } else {
            None
        };
        Ok(Self {
            table_id,
            schema_id,
            uuid,
            name,
            path,
            comment,
            columns,
            inlined_data_tables,
            partition,
            sort,
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

fn decode_u64(bytes: &[u8], offset: &mut usize, field: &str) -> CatalogResult<u64> {
    let end = offset.saturating_add(8);
    if end > bytes.len() {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let value = u64::from_be_bytes(
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
