use crate::{CatalogError, CatalogResult, ColumnId, TableColumnRow, TableRow};

pub fn inline_columns_payload(table: &TableRow) -> CatalogResult<String> {
    let mut out = String::new();
    for column in table
        .columns
        .iter()
        .filter(|column| column.parent_id.is_none())
    {
        out.push_str(&format!(
            "column\t{}\t{}\t{}\t{}\n",
            column.column_id.0,
            column.name,
            inline_column_type(column, &table.columns)?,
            column.nulls_allowed
        ));
    }
    Ok(out)
}

fn inline_column_type(
    column: &TableColumnRow,
    columns: &[TableColumnRow],
) -> CatalogResult<String> {
    let children = child_columns(columns, column.column_id);
    if children.is_empty() {
        return Ok(inline_leaf_type(&column.column_type).to_owned());
    }
    match column.column_type.to_ascii_lowercase().as_str() {
        "struct" => struct_type(&children, columns),
        "list" => list_type(&children, columns),
        "map" => map_type(&children, columns),
        _ => Err(CatalogError::Decode(format!(
            "column {} has children but non-nested type {}",
            column.column_id.0, column.column_type
        ))),
    }
}

fn struct_type(children: &[&TableColumnRow], columns: &[TableColumnRow]) -> CatalogResult<String> {
    let mut fields = Vec::with_capacity(children.len());
    for child in children {
        fields.push(format!(
            "{} {}",
            sql_identifier(&child.name),
            inline_column_type(child, columns)?
        ));
    }
    Ok(format!("STRUCT({})", fields.join(", ")))
}

fn list_type(children: &[&TableColumnRow], columns: &[TableColumnRow]) -> CatalogResult<String> {
    if children.len() != 1 {
        return Err(CatalogError::Decode(format!(
            "list column has {} child columns",
            children.len()
        )));
    }
    Ok(format!("{}[]", inline_column_type(children[0], columns)?))
}

fn map_type(children: &[&TableColumnRow], columns: &[TableColumnRow]) -> CatalogResult<String> {
    if children.len() != 2 {
        return Err(CatalogError::Decode(format!(
            "map column has {} child columns",
            children.len()
        )));
    }
    Ok(format!(
        "MAP({}, {})",
        inline_column_type(children[0], columns)?,
        inline_column_type(children[1], columns)?
    ))
}

fn inline_leaf_type(column_type: &str) -> &str {
    match column_type.to_ascii_lowercase().as_str() {
        "boolean" => "BOOLEAN",
        "int8" => "TINYINT",
        "int16" => "SMALLINT",
        "int32" => "INTEGER",
        "int64" => "BIGINT",
        "int128" => "HUGEINT",
        "uint8" => "UTINYINT",
        "uint16" => "USMALLINT",
        "uint32" => "UINTEGER",
        "uint64" => "UBIGINT",
        "uint128" => "UHUGEINT",
        "float32" => "FLOAT",
        "float64" => "DOUBLE",
        "varchar" => "VARCHAR",
        "blob" => "BLOB",
        "time_ns" => "TIME_NS",
        "timestamp_us" => "TIMESTAMP",
        "timestamp_ms" => "TIMESTAMP_MS",
        "timestamp_ns" => "TIMESTAMP_NS",
        "timestamp_s" => "TIMESTAMP_S",
        "timestamptz" => "TIMESTAMPTZ",
        "timestamptz_ns" => "TIMESTAMPTZ_NS",
        "timetz" => "TIMETZ",
        _ => column_type,
    }
}

fn child_columns(columns: &[TableColumnRow], parent_id: ColumnId) -> Vec<&TableColumnRow> {
    columns
        .iter()
        .filter(|column| column.parent_id == Some(parent_id))
        .collect()
}

fn sql_identifier(name: &str) -> String {
    if is_simple_identifier(name) {
        return name.to_owned();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn is_simple_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[cfg(test)]
#[path = "inline_column_types_tests.rs"]
mod inline_column_types_tests;
