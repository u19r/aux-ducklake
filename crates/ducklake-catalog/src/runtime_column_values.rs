use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogId, CatalogResult, ColumnId, DataFileRow, DuckLakeSnapshotId, FileColumnStatsRow,
    OrderedCatalogKv, TableId, TableRow,
    file_stats::list_file_column_stats_for_data_files,
    list_data_files_at, load_table_at,
    runtime_inline_rows::{ReadInlineRowsPayload, read_inline_rows_aggregate_stats_payload},
    runtime_snapshot_range::ReadSnapshot,
    runtime_snapshots::SnapshotReadContext,
};

pub(super) struct ColumnDefaultHistory {
    pub(super) birth: crate::CatalogOrderId,
    pub(super) has_nonnull_initial: bool,
}

pub(crate) struct ColumnValuesRequest {
    pub(crate) table_id: TableId,
    pub(crate) snapshot_id: DuckLakeSnapshotId,
    pub(crate) columns: BTreeSet<ColumnId>,
}

// Unknown sources retain unknown bounds; only proven NULL-only candidates
// return.
pub(crate) fn list_null_only_columns(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    request: ColumnValuesRequest,
) -> CatalogResult<BTreeSet<ColumnId>> {
    let context = SnapshotReadContext::for_ducklake_snapshot(kv, catalog, request.snapshot_id)?;
    let snapshot = context
        .ducklake_snapshot(request.snapshot_id)
        .ok_or(crate::CatalogError::NotFound("column values snapshot"))?;
    let Some(mut table) = load_table_at(kv, catalog, request.table_id, snapshot.order)? else {
        return Ok(BTreeSet::new());
    };
    table
        .columns
        .retain(|column| request.columns.contains(&column.column_id));
    let columns = table
        .columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let files = list_data_files_at(kv, catalog, request.table_id, snapshot.order)?;
    let stats = list_file_column_stats_for_data_files(
        kv,
        catalog,
        &files,
        &BTreeMap::from([(request.table_id, columns.clone())]),
    )?;
    let null_defaults = load_null_default_orders(kv, catalog, &table)?;
    let mut possible =
        find_file_columns_with_possible_values(&files, &stats, &columns, &null_defaults);
    for inline in &table.inlined_data_tables {
        let payload = load_inline_counts(kv, catalog, &inline.table_name, request.snapshot_id)?;
        possible.extend(find_inline_columns_with_possible_values(
            &table,
            &payload,
            &null_defaults,
        )?);
    }
    Ok(columns
        .into_iter()
        .filter(|column| !possible.contains(column))
        .collect())
}

fn load_inline_counts(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_name: &str,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Vec<u8>> {
    read_inline_rows_aggregate_stats_payload(
        kv,
        catalog,
        ReadInlineRowsPayload {
            table_name: table_name.to_owned(),
            snapshot: Some(ReadSnapshot::new(snapshot_id)),
            include_flushed: false,
            include_deleted: false,
        },
    )
}

pub(super) fn find_file_columns_with_possible_values(
    files: &[DataFileRow],
    stats: &[FileColumnStatsRow],
    columns: &[ColumnId],
    null_defaults: &BTreeMap<ColumnId, ColumnDefaultHistory>,
) -> BTreeSet<ColumnId> {
    let counts = stats
        .iter()
        .map(|row| ((row.data_file_id, row.column_id), row.value_count))
        .collect::<BTreeMap<_, _>>();
    columns
        .iter()
        .copied()
        .filter(|column| {
            files.iter().any(|file| {
                if file.record_count == 0 {
                    return false;
                }
                let file_end = file
                    .max_partial_order
                    .unwrap_or(file.validity.begin_order)
                    .max(file.validity.begin_order);
                let default = null_defaults.get(column);
                if default
                    .is_some_and(|default| default.has_nonnull_initial && file_end < default.birth)
                {
                    return true;
                }
                match counts.get(&(file.data_file_id, *column)) {
                    Some(count) => *count != Some(0),
                    None => default.is_none_or(|default| {
                        default.has_nonnull_initial || file_end >= default.birth
                    }),
                }
            })
        })
        .collect()
}

pub(super) fn find_inline_columns_with_possible_values(
    table: &TableRow,
    payload: &[u8],
    null_defaults: &BTreeMap<ColumnId, ColumnDefaultHistory>,
) -> CatalogResult<BTreeSet<ColumnId>> {
    let values = InlineValueCounts::try_from(payload)?;
    if values.rows == 0 {
        return Ok(BTreeSet::new());
    }
    Ok(table
        .columns
        .iter()
        .filter(|column| {
            column.parent_id.is_some()
                || match values.counts.get(&column.column_id) {
                    Some(count) => *count > 0,
                    None => {
                        column.created_with_table
                            || null_defaults
                                .get(&column.column_id)
                                .is_none_or(|default| default.has_nonnull_initial)
                    }
                }
        })
        .map(|column| column.column_id)
        .collect())
}

struct InlineValueCounts {
    rows: u64,
    counts: BTreeMap<ColumnId, u64>,
}

impl TryFrom<&[u8]> for InlineValueCounts {
    type Error = crate::CatalogError;

    fn try_from(payload: &[u8]) -> CatalogResult<Self> {
        let text = std::str::from_utf8(payload)
            .map_err(|error| crate::CatalogError::Decode(error.to_string()))?;
        let mut rows = None;
        let mut counts = BTreeMap::new();
        for line in text.lines() {
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["inline_aggregate_stats", count] => rows = Some(parse_count(count)?),
                [
                    "inline_aggregate_next_row_id" | "inline_active_delete_count",
                    _,
                ] => {}
                ["inline_aggregate_column_stats", column, count, _, _, _, _] => {
                    counts.insert(ColumnId(parse_count(column)?), parse_count(count)?);
                }
                _ => {
                    return Err(crate::CatalogError::Decode(
                        "invalid inline aggregate response".to_owned(),
                    ));
                }
            }
        }
        Ok(Self {
            rows: rows.ok_or(crate::CatalogError::NotFound("inline aggregate row count"))?,
            counts,
        })
    }
}

fn load_null_default_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table: &TableRow,
) -> CatalogResult<BTreeMap<ColumnId, ColumnDefaultHistory>> {
    let mut rows = crate::table_store::list_table_rows_for_table(kv, catalog, table.table_id)?;
    rows.retain(|row| row.validity.begin_order <= table.validity.begin_order);
    rows.sort_by_key(|row| row.validity.begin_order);
    let mut first_orders = BTreeMap::<ColumnId, ColumnDefaultHistory>::new();
    for row in rows {
        for column in row.columns {
            let history = first_orders
                .entry(column.column_id)
                .or_insert(ColumnDefaultHistory {
                    birth: row.validity.begin_order,
                    has_nonnull_initial: false,
                });
            history.has_nonnull_initial |= column.initial_default.is_some();
        }
    }
    Ok(first_orders)
}

fn parse_count(value: &str) -> CatalogResult<u64> {
    value
        .parse()
        .map_err(|error| crate::CatalogError::Decode(format!("invalid aggregate count: {error}")))
}
