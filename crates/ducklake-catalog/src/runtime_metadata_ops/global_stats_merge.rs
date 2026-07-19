use std::collections::BTreeSet;

use crate::{CatalogResult, DataFileId, TableId, runtime_payload::optional_payload_str_value};

#[cfg(feature = "foundationdb")]
use crate::runtime_metadata_ops::*;

#[cfg(feature = "foundationdb")]
pub(super) fn global_stats_file_rows_from_payload(
    payload: &[u8],
) -> CatalogResult<Vec<GlobalStatsFileRow>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
    })?;
    let mut rows = Vec::new();
    for line in payload.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.first().copied() != Some("file") {
            continue;
        }
        let data_file_id = fields
            .get(1)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "data file id"))
            .map(DataFileId)?;
        let table_id = fields
            .get(2)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file table id"))
            .map(TableId)?;
        let record_count = fields
            .get(4)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file record count"))?;
        let file_size_bytes = fields
            .get(5)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file size bytes"))?;
        let row_id_start = fields
            .get(6)
            .filter(|value| !value.is_empty())
            .map(|value| parse_global_stats_u64(value, "row id start"))
            .transpose()?;
        let has_deletions = fields.get(8).is_some_and(|value| !value.is_empty())
            || fields.get(13).is_some_and(|value| !value.is_empty());
        rows.push(GlobalStatsFileRow {
            data_file_id,
            table_id,
            record_count,
            file_size_bytes,
            row_id_start,
            has_deletions,
        });
    }
    Ok(rows)
}

#[cfg(feature = "foundationdb")]
pub(super) fn can_recompute_exact_inline_stats(
    is_rewrite_snapshot: bool,
    rows: &[GlobalStatsFileRow],
) -> bool {
    is_rewrite_snapshot && rows.iter().all(|row| !row.has_deletions)
}

#[cfg(feature = "foundationdb")]
pub(super) fn parse_global_stats_u64(value: &str, field_name: &str) -> CatalogResult<u64> {
    value.parse::<u64>().map_err(|error| {
        crate::CatalogError::Decode(format!(
            "invalid global stats {field_name} '{value}': {error}"
        ))
    })
}

#[cfg(feature = "foundationdb")]
pub(super) fn parse_global_stats_bool(value: &str, field_name: &str) -> CatalogResult<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(crate::CatalogError::Decode(format!(
            "invalid global stats {field_name} '{value}'"
        ))),
    }
}

pub(super) fn global_stats_value_less_than(left: &str, right: &str) -> bool {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left < right,
        _ => left < right,
    }
}

pub(super) fn global_stats_value_greater_than(left: &str, right: &str) -> bool {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left > right,
        _ => left > right,
    }
}

pub(super) fn merge_global_extra_stats(current: &str, incoming: &str) -> String {
    if current.is_empty() {
        return strip_sql_string_quotes(incoming).to_owned();
    }
    if current.contains("\"bbox\"")
        && incoming.contains("\"bbox\"")
        && let (Some(current_geo), Some(incoming_geo)) = (
            GeoExtraStats::parse(current),
            GeoExtraStats::parse(incoming),
        )
    {
        return current_geo.merged(&incoming_geo).to_json();
    }
    strip_sql_string_quotes(incoming).to_owned()
}

pub(super) fn strip_sql_string_quotes(value: &str) -> &str {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(value)
}

#[derive(Default)]
pub(super) struct GeoExtraStats {
    xmin: Option<f64>,
    xmax: Option<f64>,
    ymin: Option<f64>,
    ymax: Option<f64>,
    zmin: Option<f64>,
    zmax: Option<f64>,
    mmin: Option<f64>,
    mmax: Option<f64>,
    types: BTreeSet<String>,
}

impl GeoExtraStats {
    fn parse(value: &str) -> Option<Self> {
        let value = strip_sql_string_quotes(value);
        Some(Self {
            xmin: parse_json_number_field(value, "xmin"),
            xmax: parse_json_number_field(value, "xmax"),
            ymin: parse_json_number_field(value, "ymin"),
            ymax: parse_json_number_field(value, "ymax"),
            zmin: parse_json_number_field(value, "zmin"),
            zmax: parse_json_number_field(value, "zmax"),
            mmin: parse_json_number_field(value, "mmin"),
            mmax: parse_json_number_field(value, "mmax"),
            types: parse_json_string_array_field(value, "types")?,
        })
    }

    fn merged(&self, incoming: &Self) -> Self {
        let mut types = self.types.clone();
        types.extend(incoming.types.iter().cloned());
        Self {
            xmin: merge_optional_f64_min(self.xmin, incoming.xmin),
            xmax: merge_optional_f64_max(self.xmax, incoming.xmax),
            ymin: merge_optional_f64_min(self.ymin, incoming.ymin),
            ymax: merge_optional_f64_max(self.ymax, incoming.ymax),
            zmin: merge_optional_f64_min(self.zmin, incoming.zmin),
            zmax: merge_optional_f64_max(self.zmax, incoming.zmax),
            mmin: merge_optional_f64_min(self.mmin, incoming.mmin),
            mmax: merge_optional_f64_max(self.mmax, incoming.mmax),
            types,
        }
    }

    fn to_json(&self) -> String {
        format!(
            "{{\"bbox\": {{\"xmin\": {}, \"xmax\": {}, \"ymin\": {}, \"ymax\": {}, \"zmin\": {}, \"zmax\": {}, \"mmin\": {}, \"mmax\": {}}}, \"types\": [{}]}}",
            json_number_or_null(self.xmin),
            json_number_or_null(self.xmax),
            json_number_or_null(self.ymin),
            json_number_or_null(self.ymax),
            json_number_or_null(self.zmin),
            json_number_or_null(self.zmax),
            json_number_or_null(self.mmin),
            json_number_or_null(self.mmax),
            self.types
                .iter()
                .map(|value| format!("\"{}\"", value.replace('"', "\\\"")))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub(super) fn parse_json_number_field(value: &str, field: &str) -> Option<f64> {
    let field_marker = format!("\"{field}\"");
    let start = value.find(&field_marker)?;
    let after_colon =
        value[start + field_marker.len()..].find(':')? + start + field_marker.len() + 1;
    let rest = value[after_colon..].trim_start();
    if rest.starts_with("null") {
        return None;
    }
    let end = rest
        .find(|ch: char| !(ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+' | 'e' | 'E')))
        .unwrap_or(rest.len());
    rest[..end].parse::<f64>().ok()
}

pub(super) fn parse_json_string_array_field(value: &str, field: &str) -> Option<BTreeSet<String>> {
    let field_marker = format!("\"{field}\"");
    let start = value.find(&field_marker)?;
    let after_colon =
        value[start + field_marker.len()..].find(':')? + start + field_marker.len() + 1;
    let array_start = value[after_colon..].find('[')? + after_colon + 1;
    let array_end = value[array_start..].find(']')? + array_start;
    let mut result = BTreeSet::new();
    let mut rest = &value[array_start..array_end];
    while let Some(start_quote) = rest.find('"') {
        rest = &rest[start_quote + 1..];
        let end_quote = rest.find('"')?;
        result.insert(rest[..end_quote].to_owned());
        rest = &rest[end_quote + 1..];
    }
    Some(result)
}

pub(super) fn merge_optional_f64_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn merge_optional_f64_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn json_number_or_null(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:?}"))
        .unwrap_or_else(|| "null".to_owned())
}

pub(super) fn optional_metadata_payload_bool(
    payload: &[u8],
    key: &str,
    default_value: bool,
) -> CatalogResult<bool> {
    match optional_payload_str_value(payload, key)? {
        None => Ok(default_value),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(value) => Err(crate::CatalogError::Decode(format!(
            "ListGlobalStatsInputsForSnapshot payload has invalid {key} {value}"
        ))),
    }
}

pub(super) fn collect_data_file_ids_from_payload(
    payload: &[u8],
    data_file_ids: &mut BTreeSet<DataFileId>,
) -> CatalogResult<()> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
    })?;
    for line in payload.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.first().copied() != Some("file") {
            continue;
        }
        let data_file_id = fields
            .get(1)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))?
            .parse::<u64>()
            .map_err(|error| {
                crate::CatalogError::Decode(format!("invalid file id in {line}: {error}"))
            })?;
        data_file_ids.insert(DataFileId(data_file_id));
    }
    Ok(())
}

pub(super) fn append_table_inline_stats(
    out: &mut String,
    table_id: TableId,
    payload: &[u8],
) -> CatalogResult<()> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global inline stats payload is not utf-8: {error}"))
    })?;
    for line in payload.lines() {
        let Some(rest) = line.strip_prefix("inline_table_stats\t") else {
            if let Some(rest) = line.strip_prefix("inline_column_stats\t") {
                out.push_str(&format!(
                    "table_inline_column_stats\t{}\t{rest}\n",
                    table_id.0
                ));
            } else if let Some(rest) = line.strip_prefix("inline_aggregate_stats\t") {
                out.push_str(&format!("table_inline_stats\t{}\t{rest}\n", table_id.0));
            } else if let Some(rest) = line.strip_prefix("inline_aggregate_column_stats\t") {
                out.push_str(&format!(
                    "table_inline_column_stats\t{}\t{rest}\n",
                    table_id.0
                ));
            }
            continue;
        };
        out.push_str(&format!("table_inline_stats\t{}\t{rest}\n", table_id.0));
    }
    Ok(())
}
