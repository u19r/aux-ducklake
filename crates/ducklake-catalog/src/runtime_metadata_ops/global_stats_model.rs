#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "foundationdb")]
use crate::{CatalogResult, ColumnId, DataFileId, TableId};
use crate::{
    FileColumnStatsRow,
    runtime_metadata_ops::*,
    typed_scalar::{
        CatalogScalarType, compare_typed_scalars, normalize_catalog_scalar_type,
        typed_extrema_are_invalid,
    },
};

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy)]
pub(super) struct GlobalStatsFileRow {
    pub(super) data_file_id: DataFileId,
    pub(super) record_count: u64,
    pub(super) file_size_bytes: u64,
    pub(super) row_id_start: Option<u64>,
    pub(super) has_deletions: bool,
}

#[derive(Clone, Default)]
pub(super) struct GlobalColumnStats {
    pub(super) scalar_type: CatalogScalarType,
    pub(super) has_contains_null: bool,
    pub(super) contains_null: bool,
    pub(super) has_min: bool,
    pub(super) min_value: String,
    pub(super) has_max: bool,
    pub(super) max_value: String,
    pub(super) has_extra_stats: bool,
    pub(super) extra_stats: String,
    pub(super) invalid_extrema: bool,
}

pub(super) struct InlineStatsMerge<'a> {
    pub(super) has_contains_null: bool,
    pub(super) contains_null: bool,
    pub(super) has_non_null_values: bool,
    pub(super) min_value: Option<&'a str>,
    pub(super) max_value: Option<&'a str>,
}

#[cfg(feature = "foundationdb")]
pub(super) struct GlobalTableStats {
    table_id: TableId,
    leaf_column_ids: BTreeSet<ColumnId>,
    column_types: BTreeMap<ColumnId, CatalogScalarType>,
    column_defaults: BTreeMap<ColumnId, (Option<String>, bool)>,
    file_columns: BTreeMap<DataFileId, BTreeSet<ColumnId>>,
    live_files: BTreeSet<DataFileId>,
    record_count: u64,
    next_row_id: u64,
    table_size_bytes: u64,
    columns: BTreeMap<ColumnId, GlobalColumnStats>,
}

#[cfg(feature = "foundationdb")]
impl GlobalTableStats {
    pub(super) fn new(table: &crate::TableRow, allocated_next_row_id: u64) -> Self {
        let leaf_column_ids = leaf_column_ids(table).into_iter().collect::<BTreeSet<_>>();
        let column_types = table
            .columns
            .iter()
            .filter(|column| leaf_column_ids.contains(&column.column_id))
            .map(|column| {
                (
                    column.column_id,
                    normalize_catalog_scalar_type(&column.column_type),
                )
            })
            .collect();
        Self {
            table_id: table.table_id,
            leaf_column_ids,
            column_types,
            column_defaults: table
                .columns
                .iter()
                .map(|column| {
                    (
                        column.column_id,
                        (
                            column.initial_default.clone(),
                            column.parent_id.is_some() || column.created_with_table,
                        ),
                    )
                })
                .collect(),
            file_columns: BTreeMap::new(),
            live_files: BTreeSet::new(),
            record_count: 0,
            next_row_id: allocated_next_row_id,
            table_size_bytes: 0,
            columns: BTreeMap::new(),
        }
    }

    fn stats_for_column(&mut self, column_id: ColumnId) -> &mut GlobalColumnStats {
        let scalar_type = self
            .column_types
            .get(&column_id)
            .copied()
            .unwrap_or_default();
        self.columns
            .entry(column_id)
            .or_insert_with(|| GlobalColumnStats::with_type(scalar_type))
    }

    pub(super) fn accumulate_file(&mut self, row: &GlobalStatsFileRow) {
        self.live_files.insert(row.data_file_id);
        self.record_count = self.record_count.saturating_add(row.record_count);
        if let Some(row_id_start) = row.row_id_start {
            self.next_row_id = self
                .next_row_id
                .max(row_id_start.saturating_add(row.record_count));
        } else {
            self.next_row_id = self.next_row_id.saturating_add(row.record_count);
        }
        self.table_size_bytes = self.table_size_bytes.saturating_add(row.file_size_bytes);
    }

    pub(super) fn accumulate_file_column_stats(&mut self, row: FileColumnStatsRow) {
        if !self.live_files.contains(&row.data_file_id) {
            return;
        }
        self.file_columns
            .entry(row.data_file_id)
            .or_default()
            .insert(row.column_id);
        self.stats_for_column(row.column_id).merge_file(row);
    }

    pub(super) fn accumulate_inline_payload(&mut self, payload: &[u8]) -> CatalogResult<()> {
        let payload = std::str::from_utf8(payload).map_err(|error| {
            crate::CatalogError::Decode(format!(
                "global inline stats payload is not utf-8: {error}"
            ))
        })?;
        let mut current_inline_record_count = 0;
        let mut observed_columns = BTreeSet::new();
        for line in payload.lines() {
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["inline_table_stats", record_count, next_row_id] => {
                    current_inline_record_count =
                        parse_global_stats_u64(record_count, "inline record count")?;
                    self.record_count = self.record_count.saturating_add(parse_global_stats_u64(
                        record_count,
                        "inline record count",
                    )?);
                    self.next_row_id = self
                        .next_row_id
                        .max(parse_global_stats_u64(next_row_id, "inline next row id")?);
                }
                ["inline_aggregate_stats", record_count] => {
                    current_inline_record_count =
                        parse_global_stats_u64(record_count, "inline aggregate record count")?;
                    self.record_count = self
                        .record_count
                        .saturating_add(current_inline_record_count);
                }
                ["inline_aggregate_next_row_id", next_row_id] => {
                    self.next_row_id = self.next_row_id.max(parse_global_stats_u64(
                        next_row_id,
                        "inline aggregate next row id",
                    )?);
                }
                [
                    "inline_column_stats",
                    column_id,
                    has_contains_null,
                    contains_null,
                    has_min,
                    min_value,
                    has_max,
                    max_value,
                ] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline column stats column id",
                    )?);
                    let has_contains_null = parse_global_stats_bool(
                        has_contains_null,
                        "inline column stats has_contains_null",
                    )?;
                    let contains_null = parse_global_stats_bool(
                        contains_null,
                        "inline column stats contains_null",
                    )?;
                    observed_columns.insert(column_id);
                    self.stats_for_column(column_id)
                        .merge_inline(InlineStatsMerge {
                            has_contains_null,
                            contains_null,
                            has_non_null_values: has_contains_null && !contains_null,
                            min_value: parse_global_stats_bool(
                                has_min,
                                "inline column stats has_min",
                            )?
                            .then_some(min_value),
                            max_value: parse_global_stats_bool(
                                has_max,
                                "inline column stats has_max",
                            )?
                            .then_some(max_value),
                        });
                }
                [
                    "inline_aggregate_column_stats",
                    column_id,
                    non_null_count,
                    has_min,
                    min_value,
                    has_max,
                    max_value,
                ] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline aggregate column stats column id",
                    )?);
                    let non_null_count = parse_global_stats_u64(
                        non_null_count,
                        "inline aggregate column stats non-null count",
                    )?;
                    observed_columns.insert(column_id);
                    self.stats_for_column(column_id)
                        .merge_inline(InlineStatsMerge {
                            has_contains_null: true,
                            contains_null: non_null_count < current_inline_record_count,
                            has_non_null_values: non_null_count > 0,
                            min_value: parse_global_stats_bool(
                                has_min,
                                "inline aggregate column stats has_min",
                            )?
                            .then_some(min_value),
                            max_value: parse_global_stats_bool(
                                has_max,
                                "inline aggregate column stats has_max",
                            )?
                            .then_some(max_value),
                        });
                }
                ["inline_column_extra_stats", column_id, extra_stats] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline column extra stats column id",
                    )?);
                    self.stats_for_column(column_id)
                        .merge_extra_stats(extra_stats);
                }
                _ => {}
            }
        }
        if current_inline_record_count > 0 {
            self.merge_missing_inline_defaults(&observed_columns);
        }
        Ok(())
    }

    fn merge_missing_inline_defaults(&mut self, observed: &BTreeSet<ColumnId>) {
        let missing = self
            .leaf_column_ids
            .difference(observed)
            .copied()
            .collect::<Vec<_>>();
        for column in missing {
            let (default, is_unknown) = self
                .column_defaults
                .get(&column)
                .cloned()
                .unwrap_or_default();
            let bound = if is_unknown { None } else { default.as_deref() };
            self.stats_for_column(column)
                .merge_inline(InlineStatsMerge {
                    has_contains_null: true,
                    contains_null: is_unknown || default.is_none(),
                    has_non_null_values: is_unknown || default.is_some(),
                    min_value: bound,
                    max_value: bound,
                });
        }
    }

    fn fill_missing_file_columns(&self, columns: &mut BTreeMap<ColumnId, GlobalColumnStats>) {
        let observed_column_ids = columns.keys().copied().collect::<Vec<_>>();
        for file_id in &self.live_files {
            let present_columns = self.file_columns.get(file_id);
            for column_id in &observed_column_ids {
                if self.leaf_column_ids.contains(column_id)
                    && present_columns.is_none_or(|columns| !columns.contains(column_id))
                {
                    let scalar_type = self
                        .column_types
                        .get(column_id)
                        .copied()
                        .unwrap_or_default();
                    columns
                        .entry(*column_id)
                        .or_insert_with(|| GlobalColumnStats::with_type(scalar_type))
                        .mark_unknown();
                }
            }
        }
    }

    pub(super) fn append_to(&self, out: &mut String) -> CatalogResult<()> {
        use std::fmt::Write as _;

        writeln!(
            out,
            "global_table_stats\t{}\t{}\t{}\t{}",
            self.table_id.0, self.record_count, self.next_row_id, self.table_size_bytes
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render global table stats: {error}"))
        })?;

        let mut columns = self.columns.clone();
        self.fill_missing_file_columns(&mut columns);
        for (column_id, stats) in columns {
            if !self.leaf_column_ids.contains(&column_id) {
                continue;
            }
            writeln!(
                out,
                "global_table_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                self.table_id.0,
                column_id.0,
                stats.has_contains_null,
                stats.contains_null,
                stats.has_min,
                stats.min_value,
                stats.has_max,
                stats.max_value,
                stats.has_extra_stats,
                stats.extra_stats
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render global table column stats: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

#[cfg(feature = "foundationdb")]
pub(super) fn leaf_column_ids(table: &crate::TableRow) -> Vec<ColumnId> {
    let parent_column_ids = table
        .columns
        .iter()
        .filter_map(|column| column.parent_id)
        .collect::<BTreeSet<_>>();
    table
        .columns
        .iter()
        .filter(|column| !parent_column_ids.contains(&column.column_id))
        .map(|column| column.column_id)
        .collect()
}

impl GlobalColumnStats {
    pub(super) fn with_type(scalar_type: CatalogScalarType) -> Self {
        Self {
            scalar_type,
            ..Self::default()
        }
    }

    fn merge_file(&mut self, row: FileColumnStatsRow) {
        self.has_contains_null = true;
        if row.null_count > 0 {
            self.contains_null = true;
        }
        if typed_extrema_are_invalid(
            self.scalar_type,
            row.value_count,
            row.null_count,
            row.min_value.as_deref(),
            row.max_value.as_deref(),
        ) {
            self.clear_extrema();
            self.invalid_extrema = true;
        } else if !self.invalid_extrema {
            if let Some(min_value) = row.min_value
                && (!self.has_min
                    || compare_typed_scalars(self.scalar_type, &min_value, &self.min_value)
                        .is_some_and(|ordering| ordering.is_lt()))
            {
                self.has_min = true;
                self.min_value = min_value;
            }
            if let Some(max_value) = row.max_value
                && (!self.has_max
                    || compare_typed_scalars(self.scalar_type, &max_value, &self.max_value)
                        .is_some_and(|ordering| ordering.is_gt()))
            {
                self.has_max = true;
                self.max_value = max_value;
            }
        }
        if let Some(extra_stats) = row.extra_stats
            && !extra_stats.is_empty()
        {
            self.merge_extra_stats(&extra_stats);
        }
    }

    fn merge_extra_stats(&mut self, extra_stats: &str) {
        if extra_stats.is_empty() {
            return;
        }
        self.has_extra_stats = true;
        self.extra_stats = merge_global_extra_stats(&self.extra_stats, extra_stats);
    }

    pub(super) fn merge_inline(&mut self, inline: InlineStatsMerge<'_>) {
        if inline.has_contains_null {
            self.has_contains_null = true;
            self.contains_null = self.contains_null || inline.contains_null;
        }
        let bounds = match (inline.min_value, inline.max_value) {
            (Some(min_value), Some(max_value)) => Some((min_value, max_value)),
            (None, None) => None,
            _ => {
                self.clear_extrema();
                self.invalid_extrema = true;
                return;
            }
        };
        let bounds_are_invalid = bounds.is_some_and(|(min_value, max_value)| {
            compare_typed_scalars(self.scalar_type, min_value, min_value).is_none()
                || compare_typed_scalars(self.scalar_type, max_value, max_value).is_none()
                || compare_typed_scalars(self.scalar_type, min_value, max_value)
                    .is_none_or(|ordering| ordering.is_gt())
        });
        if !self.invalid_extrema && bounds_are_invalid {
            self.clear_extrema();
            self.invalid_extrema = true;
        } else if let Some((min_value, max_value)) = bounds
            && !self.invalid_extrema
        {
            if !self.has_min
                || compare_typed_scalars(self.scalar_type, min_value, &self.min_value)
                    .is_some_and(|ordering| ordering.is_lt())
            {
                self.has_min = true;
                self.min_value = min_value.to_owned();
            }
            if !self.has_max
                || compare_typed_scalars(self.scalar_type, max_value, &self.max_value)
                    .is_some_and(|ordering| ordering.is_gt())
            {
                self.has_max = true;
                self.max_value = max_value.to_owned();
            }
        } else if bounds.is_none() {
            self.clear_extrema();
            if inline.has_non_null_values {
                self.invalid_extrema = true;
            }
        }
    }

    fn mark_unknown(&mut self) {
        self.has_contains_null = true;
        self.contains_null = true;
        self.clear_extrema();
        self.invalid_extrema = true;
    }

    fn clear_extrema(&mut self) {
        self.has_min = false;
        self.min_value.clear();
        self.has_max = false;
        self.max_value.clear();
    }
}
