use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
};

use crate::{CatalogResult, TableRow};

use crate::runtime_inline_rows::*;

pub(super) struct InlineCatalogStats {
    columns: Vec<InlineStatsColumn>,
    missing_current_column_ids: Vec<u64>,
    record_count: u64,
    next_row_id: u64,
    stats: BTreeMap<u64, LiveColumnStats>,
}

pub(super) struct InlineStatsColumn {
    column_id: u64,
    pub(super) column_type: String,
    pub(super) children: Vec<InlineStatsChildColumn>,
}

pub(super) struct InlineStatsChildColumn {
    pub(super) column_id: u64,
    pub(super) name: String,
}

impl InlineCatalogStats {
    pub(super) fn for_inline_schema(schema_table: &TableRow, current_table: &TableRow) -> Self {
        let columns = schema_table
            .columns
            .iter()
            .filter(|column| column.parent_id.is_none())
            .map(|column| InlineStatsColumn {
                column_id: column.column_id.0,
                column_type: column.column_type.clone(),
                children: schema_table
                    .columns
                    .iter()
                    .filter(|child| child.parent_id == Some(column.column_id))
                    .map(|child| InlineStatsChildColumn {
                        column_id: child.column_id.0,
                        name: child.name.clone(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let schema_column_ids = schema_table
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<BTreeSet<_>>();
        let missing_current_column_ids = current_table
            .columns
            .iter()
            .filter(|column| !schema_column_ids.contains(&column.column_id))
            .map(|column| column.column_id.0)
            .collect::<Vec<_>>();
        Self {
            columns,
            missing_current_column_ids,
            record_count: 0,
            next_row_id: 0,
            stats: BTreeMap::new(),
        }
    }

    pub(super) fn observe_row_id(&mut self, row_id: u64) {
        self.next_row_id = self.next_row_id.max(row_id + 1);
    }

    pub(super) fn accumulate_visible_row(
        &mut self,
        row_id: u64,
        encoded_values: &[&str],
    ) -> CatalogResult<()> {
        if encoded_values.len() != self.columns.len() {
            return Err(crate::CatalogError::Decode(format!(
                "inline row has {} values for {} columns",
                encoded_values.len(),
                self.columns.len()
            )));
        }
        self.record_count += 1;
        self.observe_row_id(row_id);
        for (column, encoded_value) in self.columns.iter().zip(encoded_values.iter()) {
            self.stats
                .entry(column.column_id)
                .or_default()
                .accumulate_encoded(encoded_value, &column.column_type)?;
            accumulate_child_inline_stats(&mut self.stats, column, encoded_value)?;
        }
        Ok(())
    }

    pub(super) fn clear_min_max(&mut self) {
        for stats in self.stats.values_mut() {
            stats.clear_min_max();
        }
    }

    pub(super) fn append_to(&self, out: &mut String) -> CatalogResult<()> {
        writeln!(
            out,
            "inline_table_stats\t{}\t{}",
            self.record_count, self.next_row_id
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render inline table stats: {error}"))
        })?;
        for (column_id, stats) in &self.stats {
            writeln!(
                out,
                "inline_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                column_id,
                stats.has_contains_null,
                stats.contains_null,
                stats.min_value.is_some(),
                stats.min_value.as_deref().unwrap_or_default(),
                stats.max_value.is_some(),
                stats.max_value.as_deref().unwrap_or_default()
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!("failed to render inline stats: {error}"))
            })?;
            if let Some(extra_stats) = &stats.extra_stats {
                writeln!(
                    out,
                    "inline_column_extra_stats\t{}\t{}",
                    column_id, extra_stats
                )
                .map_err(|error| {
                    crate::CatalogError::Decode(format!(
                        "failed to render inline extra stats: {error}"
                    ))
                })?;
            }
        }
        if self.record_count > 0 {
            for column_id in &self.missing_current_column_ids {
                writeln!(
                    out,
                    "inline_column_stats\t{column_id}\ttrue\ttrue\tfalse\t\tfalse\t"
                )
                .map_err(|error| {
                    crate::CatalogError::Decode(format!(
                        "failed to render missing inline stats: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    pub(super) fn append_aggregate_to(&self, out: &mut String) -> CatalogResult<()> {
        writeln!(out, "inline_aggregate_stats\t{}", self.record_count).map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render inline aggregate stats: {error}"))
        })?;
        writeln!(out, "inline_aggregate_next_row_id\t{}", self.next_row_id).map_err(|error| {
            crate::CatalogError::Decode(format!(
                "failed to render inline aggregate next row id: {error}"
            ))
        })?;
        for column in &self.columns {
            let stats = self.stats.get(&column.column_id);
            writeln!(
                out,
                "inline_aggregate_column_stats\t{}\t{}\t{}\t{}\t{}\t{}",
                column.column_id,
                stats.map(|stats| stats.non_null_count).unwrap_or(0),
                stats.and_then(|stats| stats.min_value.as_deref()).is_some(),
                stats
                    .and_then(|stats| stats.min_value.as_deref())
                    .unwrap_or_default(),
                stats.and_then(|stats| stats.max_value.as_deref()).is_some(),
                stats
                    .and_then(|stats| stats.max_value.as_deref())
                    .unwrap_or_default()
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render inline aggregate column stats: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct LiveColumnStats {
    non_null_count: u64,
    pub(super) has_contains_null: bool,
    pub(super) contains_null: bool,
    pub(super) min_value: Option<String>,
    pub(super) max_value: Option<String>,
    pub(super) extra_stats: Option<InlineGeometryStats>,
}

impl LiveColumnStats {
    pub(super) fn accumulate_encoded(
        &mut self,
        encoded: &str,
        column_type: &str,
    ) -> CatalogResult<()> {
        self.has_contains_null = true;
        let value = match decode_inline_stats_value(encoded)? {
            InlineStatsValue::Null => {
                self.accumulate_null();
                return Ok(());
            }
            InlineStatsValue::Opaque => {
                self.accumulate_opaque();
                return Ok(());
            }
            InlineStatsValue::Comparable(value) => value,
        };
        if column_type.eq_ignore_ascii_case("geometry") {
            self.accumulate_geometry(value);
            return Ok(());
        }
        self.accumulate_comparable(value);
        Ok(())
    }

    pub(super) fn accumulate_null(&mut self) {
        self.has_contains_null = true;
        self.contains_null = true;
    }

    fn accumulate_opaque(&mut self) {
        self.has_contains_null = true;
        self.non_null_count += 1;
    }

    pub(super) fn accumulate_comparable(&mut self, value: String) {
        self.has_contains_null = true;
        self.non_null_count += 1;
        if self
            .min_value
            .as_deref()
            .is_none_or(|current| stats_value_cmp(&value, current).is_lt())
        {
            self.min_value = Some(value.clone());
        }
        if self
            .max_value
            .as_deref()
            .is_none_or(|current| stats_value_cmp(&value, current).is_gt())
        {
            self.max_value = Some(value);
        }
    }

    fn accumulate_geometry(&mut self, value: String) {
        self.has_contains_null = true;
        self.non_null_count += 1;
        if let Some(stats) = InlineGeometryStats::parse_wkt(&value) {
            match &mut self.extra_stats {
                Some(current) => current.merge(stats),
                None => self.extra_stats = Some(stats),
            }
        }
    }

    fn clear_min_max(&mut self) {
        self.min_value = None;
        self.max_value = None;
    }
}
