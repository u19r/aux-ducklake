use crate::{CatalogOrderId, CatalogResult, TableRow};

use crate::runtime_inline_rows::*;

pub(super) struct GlobalInlineStatsInput<'a> {
    pub(super) payload: &'a [u8],
    pub(super) begin_order: CatalogOrderId,
    pub(super) include_deleted: bool,
    pub(super) include_materialized: bool,
    pub(super) stats_mode: InlineStatsMode,
    pub(super) materialized_files: &'a InlineMaterializedFiles,
    pub(super) deleted_rows: Option<&'a RawInlineDeletionIndex>,
}

pub(super) fn accumulate_global_inline_stats(
    input: GlobalInlineStatsInput<'_>,
    catalog_stats: &mut InlineCatalogStats,
) -> CatalogResult<()> {
    let rows = std::str::from_utf8(input.payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        catalog_stats.observe_row_id(row_id);
        if !input.include_materialized
            && input
                .materialized_files
                .materializes(row_id, input.begin_order)
        {
            continue;
        }
        if matches!(input.stats_mode, InlineStatsMode::ExactVisible)
            && !input.include_deleted
            && input
                .deleted_rows
                .is_some_and(|index| index.hides_row_version(row_id, input.begin_order))
        {
            continue;
        }
        catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
    }
    Ok(())
}

pub(super) struct InlineFlushRow {
    pub(super) row_id: u64,
    pub(super) begin_snapshot: u64,
    pub(super) end_snapshot: Option<u64>,
    pub(super) values: Vec<String>,
}

pub(super) struct InlineFlushPartitionFilter {
    predicates: Vec<InlineFlushPartitionPredicate>,
}

pub(super) struct InlineFlushPartitionPredicate {
    column_index: usize,
    transform: InlineFlushPartitionTransform,
    value: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) enum InlineFlushPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
}

impl InlineFlushPartitionFilter {
    pub(super) fn parse(filter: Option<&str>, schema_table: &TableRow) -> CatalogResult<Self> {
        let Some(filter) = filter.map(str::trim).filter(|filter| !filter.is_empty()) else {
            return Ok(Self {
                predicates: Vec::new(),
            });
        };
        let mut predicates = Vec::new();
        for clause in filter
            .split(" AND ")
            .map(str::trim)
            .filter(|clause| !clause.is_empty())
        {
            predicates.push(InlineFlushPartitionPredicate::parse(clause, schema_table)?);
        }
        Ok(Self { predicates })
    }

    pub(super) fn matches(&self, row: &InlineFlushRow) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.matches(row))
    }
}

impl InlineFlushPartitionPredicate {
    fn parse(clause: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        if let Some(left) = clause.strip_suffix(" IS NULL") {
            let (column_index, transform) = parse_partition_left(left.trim(), schema_table)?;
            return Ok(Self {
                column_index,
                transform,
                value: None,
            });
        }
        let Some((left, right)) = clause.split_once(" = ") else {
            return Err(crate::CatalogError::Decode(format!(
                "unsupported inline flush partition filter: {clause}"
            )));
        };
        let (column_index, transform) = parse_partition_left(left.trim(), schema_table)?;
        Ok(Self {
            column_index,
            transform,
            value: Some(unquote_sql_partition_value(right.trim())),
        })
    }

    fn matches(&self, row: &InlineFlushRow) -> bool {
        let value = row
            .values
            .get(self.column_index)
            .and_then(|encoded| decode_inline_partition_value(encoded));
        let value = match (&self.transform, value) {
            (_, None) => None,
            (InlineFlushPartitionTransform::Identity, Some(value)) => Some(value),
            (InlineFlushPartitionTransform::Year, Some(value)) => date_part(&value, 0, 4),
            (InlineFlushPartitionTransform::Month, Some(value)) => date_part(&value, 5, 7),
            (InlineFlushPartitionTransform::Day, Some(value)) => date_part(&value, 8, 10),
            (InlineFlushPartitionTransform::Hour, Some(value)) => date_part(&value, 11, 13),
        };
        value == self.value
    }
}

pub(super) fn parse_partition_left(
    left: &str,
    schema_table: &TableRow,
) -> CatalogResult<(usize, InlineFlushPartitionTransform)> {
    let left = unwrap_partition_cast(left.trim());
    for (prefix, transform) in [
        ("year(", InlineFlushPartitionTransform::Year),
        ("month(", InlineFlushPartitionTransform::Month),
        ("day(", InlineFlushPartitionTransform::Day),
        ("hour(", InlineFlushPartitionTransform::Hour),
    ] {
        if let Some(column) = left
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(')'))
        {
            return Ok((partition_column_index(schema_table, column)?, transform));
        }
    }
    Ok((
        partition_column_index(schema_table, left)?,
        InlineFlushPartitionTransform::Identity,
    ))
}

pub(super) fn unwrap_partition_cast(left: &str) -> &str {
    let Some(inner) = left
        .strip_prefix("CAST(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return left;
    };
    inner
        .split_once(" AS ")
        .map(|(column, _)| column.trim())
        .unwrap_or(left)
}

pub(super) fn partition_column_index(
    schema_table: &TableRow,
    column_name: &str,
) -> CatalogResult<usize> {
    let column_name = unquote_identifier(unwrap_partition_cast(column_name.trim()));
    schema_table
        .columns
        .iter()
        .filter(|column| column.parent_id.is_none())
        .position(|column| column.name.eq_ignore_ascii_case(&column_name))
        .ok_or_else(|| {
            crate::CatalogError::Decode(format!(
                "inline flush partition filter references unknown column {column_name}"
            ))
        })
}

pub(super) fn unquote_identifier(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return value[1..value.len() - 1].replace("\"\"", "\"");
    }
    value.to_owned()
}

pub(super) fn unquote_sql_partition_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return value[1..value.len() - 1].replace("''", "'");
    }
    value.to_owned()
}

pub(super) fn date_part(value: &str, start: usize, end: usize) -> Option<String> {
    value.get(start..end).map(|part| part.to_owned())
}

pub(super) fn decode_inline_partition_value(encoded: &str) -> Option<String> {
    if encoded == "n:" {
        return None;
    }
    if encoded == "b:1" {
        return Some("true".to_owned());
    }
    if encoded == "b:0" {
        return Some("false".to_owned());
    }
    if let Some(value) = encoded.strip_prefix("i:") {
        return Some(value.to_owned());
    }
    if let Some(value) = encoded
        .strip_prefix("s:")
        .or_else(|| encoded.strip_prefix("v:"))
    {
        return hex_decode(value).ok();
    }
    None
}

pub(super) fn collect_inline_flush_rows(
    payload: &[u8],
    begin_snapshot: u64,
    deleted_rows: &InlineDeletionIndex,
    out: &mut Vec<InlineFlushRow>,
) -> CatalogResult<()> {
    let rows = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        out.push(InlineFlushRow {
            row_id,
            begin_snapshot,
            end_snapshot: deleted_rows.end_snapshot_for(row_id, begin_snapshot),
            values: fields[2..]
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        });
    }
    Ok(())
}
