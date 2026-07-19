use crate::{
    CatalogResult, ColumnId, ColumnMappingRow, MetadataSettingRow, MetadataSettingScope,
    NameMappingColumnRow, TableId,
    runtime_payload::{payload_string_value, payload_u64_value},
    runtime_protocol::RuntimeCatalogBackend,
};

pub(super) fn metadata_operation_payload(
    operation: &str,
    backend: RuntimeCatalogBackend,
    payload_len: usize,
    fields: &[&str],
) -> Vec<u8> {
    let mut payload = format!(
        "runtime_ffi=ok\noperation={operation}\nbackend={}\npayload_bytes={payload_len}\n",
        backend.as_str()
    );
    for field in fields {
        payload.push_str(field);
        payload.push('\n');
    }
    payload.into_bytes()
}

pub(super) fn config_option_payload(payload: &[u8]) -> CatalogResult<MetadataSettingRow> {
    let key = payload_string_value(payload, "key", "SetConfigOption missing key")?;
    let value = payload_string_value(payload, "value", "SetConfigOption missing value")?;
    let scope = crate::runtime_payload::payload_str_value(
        payload,
        "scope",
        "SetConfigOption missing scope",
    )?;
    reject_tabular_field(&key, "config option key")?;
    reject_tabular_field(&value, "config option value")?;
    match scope {
        "global" => Ok(MetadataSettingRow::global(key, value)),
        "schema" => Ok(MetadataSettingRow::schema(
            key,
            value,
            payload_u64_value(
                payload,
                "scope_id",
                "SetConfigOption missing schema scope_id",
            )?,
        )),
        "table" => Ok(MetadataSettingRow::table(
            key,
            value,
            payload_u64_value(
                payload,
                "scope_id",
                "SetConfigOption missing table scope_id",
            )?,
        )),
        _ => Err(crate::CatalogError::Decode(format!(
            "SetConfigOption has unsupported scope {scope}"
        ))),
    }
}

pub(super) fn config_options_payload(rows: &[MetadataSettingRow]) -> String {
    let mut out = format!("config_option_count={}\n", rows.len());
    for row in rows {
        let (scope, scope_id) = match row.scope {
            MetadataSettingScope::Global => ("global", String::new()),
            MetadataSettingScope::Schema(id) => ("schema", id.to_string()),
            MetadataSettingScope::Table(id) => ("table", id.to_string()),
        };
        out.push_str(&format!(
            "config_option\t{}\t{}\t{}\t{}\n",
            row.key, row.value, scope, scope_id
        ));
    }
    out
}

pub(super) fn column_mapping_payload(payload: &[u8]) -> CatalogResult<Vec<ColumnMappingRow>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("column mapping payload is not utf-8: {error}"))
    })?;
    let mut rows = Vec::new();
    for line in payload.lines() {
        if line.is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["mapping", mapping_id, table_id, mapping_type] => {
                rows.push(ColumnMappingRow::new(
                    parse_u64(mapping_id, "mapping id")?,
                    TableId(parse_u64(table_id, "mapping table id")?),
                    *mapping_type,
                ));
            }
            [
                "mapping_column",
                mapping_id,
                column_id,
                source_name,
                target_field_id,
                parent_column,
                is_partition,
            ] => {
                let mapping_id = parse_u64(mapping_id, "mapping column mapping id")?;
                let Some(row) = rows.iter_mut().find(|row| row.mapping_id == mapping_id) else {
                    return Err(crate::CatalogError::Decode(format!(
                        "mapping column references unknown mapping {mapping_id}"
                    )));
                };
                row.columns.push(NameMappingColumnRow {
                    column_id: ColumnId(parse_u64(column_id, "mapping column id")?),
                    source_name: (*source_name).to_owned(),
                    target_field_id: parse_u64(target_field_id, "mapping target field id")?,
                    parent_column: optional_u64(parent_column, "mapping parent column")?,
                    is_partition: parse_bool(is_partition, "mapping partition flag")?,
                });
            }
            _ => {
                return Err(crate::CatalogError::Decode(format!(
                    "invalid column mapping payload line: {line}"
                )));
            }
        }
    }
    Ok(rows)
}

pub(super) fn column_mappings_payload(rows: &[ColumnMappingRow]) -> String {
    let mut out = format!("column_mapping_count={}\n", rows.len());
    for row in rows {
        out.push_str(&format!(
            "mapping\t{}\t{}\t{}\n",
            row.mapping_id, row.table_id.0, row.mapping_type
        ));
        for column in &row.columns {
            out.push_str(&format!(
                "mapping_column\t{}\t{}\t{}\t{}\t{}\t{}\n",
                row.mapping_id,
                column.column_id.0,
                column.source_name,
                column.target_field_id,
                column
                    .parent_column
                    .map_or(String::new(), |id| id.to_string()),
                column.is_partition
            ));
        }
    }
    out
}

pub(super) fn optional_payload_u64(payload: &[u8], key: &str) -> CatalogResult<Option<u64>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("runtime payload is not utf-8: {error}"))
    })?;
    let prefix = format!("{key}=");
    for line in payload.lines() {
        let Some(value) = line.strip_prefix(&prefix) else {
            continue;
        };
        if value.is_empty() {
            return Ok(None);
        }
        return Ok(Some(parse_u64(value, key)?));
    }
    Ok(None)
}

pub(super) fn optional_u64(value: &str, field: &str) -> CatalogResult<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_u64(value, field)?))
}

pub(super) fn optional_u64_sql(value: Option<u64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

pub(super) fn optional_sql_string(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_string)
}

pub(super) fn optional_encryption_key_sql(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_owned()
    } else {
        sql_string(value)
    }
}

pub(super) fn null_if_empty(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_owned()
    } else {
        value.to_owned()
    }
}

pub(super) fn sql_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            escaped.push('\'');
        }
        escaped.push(ch);
    }
    escaped.push('\'');
    escaped
}

pub(super) fn parse_bool(value: &str, field: &str) -> CatalogResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(crate::CatalogError::Decode(format!(
            "invalid {field} {value}"
        ))),
    }
}

pub(super) fn parse_u64(value: &str, field: &str) -> CatalogResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| crate::CatalogError::Decode(format!("invalid {field} {value}: {error}")))
}

pub(super) fn reject_tabular_field(value: &str, name: &str) -> CatalogResult<()> {
    if value.contains('\t') || value.contains('\n') {
        return Err(crate::CatalogError::Decode(format!(
            "{name} cannot contain tabs or newlines"
        )));
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(super) fn open_foundationdb_catalog() -> CatalogResult<crate::FdbOrderedCatalogKv> {
    crate::runtime_foundationdb::open_foundationdb_catalog()
}

#[cfg(not(feature = "foundationdb"))]
pub(super) fn open_foundationdb_catalog() -> CatalogResult<crate::FakeOrderedCatalogKv> {
    Err(crate::CatalogError::Backend(
        "foundationdb feature is not enabled".to_owned(),
    ))
}
