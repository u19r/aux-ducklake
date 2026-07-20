#[cfg(feature = "foundationdb")]
use std::collections::{BTreeMap, BTreeSet};

use crate::FileColumnStatsRow;
#[cfg(feature = "foundationdb")]
use crate::{CatalogResult, ColumnId, DataFileId, TableId};

use crate::runtime_metadata_ops::*;

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy)]
pub(super) struct GlobalStatsFileRow {
    pub(super) data_file_id: DataFileId,
    pub(super) table_id: TableId,
    pub(super) record_count: u64,
    pub(super) file_size_bytes: u64,
    pub(super) row_id_start: Option<u64>,
    pub(super) has_deletions: bool,
}

#[derive(Clone, Default)]
pub(super) struct GlobalColumnStats {
    pub(super) has_contains_null: bool,
    pub(super) contains_null: bool,
    pub(super) has_min: bool,
    pub(super) min_value: String,
    pub(super) has_max: bool,
    pub(super) max_value: String,
    pub(super) has_extra_stats: bool,
    pub(super) extra_stats: String,
}

#[cfg(feature = "foundationdb")]
pub(super) struct GlobalTableStats {
    table_id: TableId,
    leaf_column_ids: BTreeSet<ColumnId>,
    file_columns: BTreeMap<DataFileId, BTreeSet<ColumnId>>,
    live_files: BTreeSet<DataFileId>,
    record_count: u64,
    next_row_id: u64,
    table_size_bytes: u64,
    columns: BTreeMap<ColumnId, GlobalColumnStats>,
}

#[cfg(feature = "foundationdb")]
impl GlobalTableStats {
    pub(super) fn new(table: &crate::TableRow) -> Self {
        let parent_column_ids = table
            .columns
            .iter()
            .filter_map(|column| column.parent_id)
            .collect::<BTreeSet<_>>();
        Self {
            table_id: table.table_id,
            leaf_column_ids: table
                .columns
                .iter()
                .filter(|column| !parent_column_ids.contains(&column.column_id))
                .map(|column| column.column_id)
                .collect(),
            file_columns: BTreeMap::new(),
            live_files: BTreeSet::new(),
            record_count: 0,
            next_row_id: 0,
            table_size_bytes: 0,
            columns: BTreeMap::new(),
        }
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
        self.columns
            .entry(row.column_id)
            .or_default()
            .merge_file(row);
    }

    pub(super) fn accumulate_inline_payload(&mut self, payload: &[u8]) -> CatalogResult<()> {
        let payload = std::str::from_utf8(payload).map_err(|error| {
            crate::CatalogError::Decode(format!(
                "global inline stats payload is not utf-8: {error}"
            ))
        })?;
        let mut current_inline_record_count = 0;
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
                    self.columns.entry(column_id).or_default().merge_inline(
                        parse_global_stats_bool(
                            has_contains_null,
                            "inline column stats has_contains_null",
                        )?,
                        parse_global_stats_bool(
                            contains_null,
                            "inline column stats contains_null",
                        )?,
                        parse_global_stats_bool(has_min, "inline column stats has_min")?,
                        min_value,
                        parse_global_stats_bool(has_max, "inline column stats has_max")?,
                        max_value,
                    );
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
                    self.columns.entry(column_id).or_default().merge_inline(
                        true,
                        non_null_count < current_inline_record_count,
                        parse_global_stats_bool(has_min, "inline aggregate column stats has_min")?,
                        min_value,
                        parse_global_stats_bool(has_max, "inline aggregate column stats has_max")?,
                        max_value,
                    );
                }
                ["inline_column_extra_stats", column_id, extra_stats] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline column extra stats column id",
                    )?);
                    self.columns
                        .entry(column_id)
                        .or_default()
                        .merge_extra_stats(extra_stats);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn fill_missing_file_columns(&self, columns: &mut BTreeMap<ColumnId, GlobalColumnStats>) {
        let observed_column_ids = columns.keys().copied().collect::<Vec<_>>();
        for file_id in &self.live_files {
            let present_columns = self.file_columns.get(file_id);
            for column_id in &observed_column_ids {
                if self.leaf_column_ids.contains(column_id)
                    && present_columns.is_none_or(|columns| !columns.contains(column_id))
                {
                    columns.entry(*column_id).or_default().mark_contains_null();
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

impl GlobalColumnStats {
    fn merge_file(&mut self, row: FileColumnStatsRow) {
        self.has_contains_null = true;
        if row.null_count > 0 {
            self.contains_null = true;
        }
        if let Some(min_value) = row.min_value
            && (!self.has_min || global_stats_value_less_than(&min_value, &self.min_value))
        {
            self.has_min = true;
            self.min_value = min_value;
        }
        if let Some(max_value) = row.max_value
            && (!self.has_max || global_stats_value_greater_than(&max_value, &self.max_value))
        {
            self.has_max = true;
            self.max_value = max_value;
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

    pub(super) fn merge_inline(
        &mut self,
        has_contains_null: bool,
        contains_null: bool,
        has_min: bool,
        min_value: &str,
        has_max: bool,
        max_value: &str,
    ) {
        if has_contains_null {
            self.has_contains_null = true;
            self.contains_null = self.contains_null || contains_null;
        }
        if has_min && (!self.has_min || global_stats_value_less_than(min_value, &self.min_value)) {
            self.has_min = true;
            self.min_value = min_value.to_owned();
        }
        if has_max && (!self.has_max || global_stats_value_greater_than(max_value, &self.max_value))
        {
            self.has_max = true;
            self.max_value = max_value.to_owned();
        }
        if !has_min && !has_max {
            self.has_min = false;
            self.min_value.clear();
            self.has_max = false;
            self.max_value.clear();
        }
    }

    fn mark_contains_null(&mut self) {
        self.has_contains_null = true;
        self.contains_null = true;
    }
}
