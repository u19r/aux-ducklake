use std::collections::BTreeSet;

use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, TableColumnRow, TableId,
    ids::CatalogOrderId,
    store::latest_snapshot,
    table_columns::{commit_column_version, same_default_metadata},
    table_store::{load_current_table_row, reject_table_conflicts_since_base},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefaultChange {
    pub table_id: TableId,
    pub column: TableColumnRow,
}

impl ColumnDefaultChange {
    #[must_use]
    pub fn new(table_id: TableId, column: TableColumnRow) -> Self {
        Self { table_id, column }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedColumnDefault {
    pub table_id: TableId,
    pub previous: TableColumnRow,
    pub changed: TableColumnRow,
}

pub fn commit_change_table_column_defaults(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    changes: &[ColumnDefaultChange],
) -> CatalogResult<Vec<ChangedColumnDefault>> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let table_id = reject_column_default_change_batch_shape(changes)?;
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut next_columns = previous.columns.clone();
    let mut changed = Vec::with_capacity(changes.len());
    for change in changes {
        let existing = replace_default(&mut next_columns, change, table_id)?;
        changed.push(ChangedColumnDefault {
            table_id,
            previous: existing,
            changed: change.column.clone(),
        });
    }

    commit_column_version(
        kv,
        catalog,
        table_id,
        latest.sequence,
        previous,
        next_columns,
    )?;
    Ok(changed)
}

pub fn commit_change_table_column_defaults_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    changes: &[ColumnDefaultChange],
) -> CatalogResult<Vec<ChangedColumnDefault>> {
    let table_id = reject_column_default_change_batch_shape(changes)?;
    reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    commit_change_table_column_defaults(kv, catalog, changes)
}

fn replace_default(
    columns: &mut [TableColumnRow],
    change: &ColumnDefaultChange,
    table_id: TableId,
) -> CatalogResult<TableColumnRow> {
    let Some(column_index) = columns
        .iter()
        .position(|column| column.column_id == change.column.column_id)
    else {
        return Err(CatalogError::NotFound("column"));
    };
    let existing = columns[column_index].clone();
    reject_non_default_column_change(&existing, &change.column, table_id)?;
    columns[column_index].initial_default = change.column.initial_default.clone();
    columns[column_index].default_value = change.column.default_value.clone();
    columns[column_index].default_value_type = change.column.default_value_type.clone();
    Ok(existing)
}

fn reject_column_default_change_batch_shape(
    changes: &[ColumnDefaultChange],
) -> CatalogResult<TableId> {
    let first = changes.first().ok_or_else(|| {
        CatalogError::InvalidMutation("empty column default change batch".to_owned())
    })?;
    let mut unique = BTreeSet::new();
    for change in changes {
        if change.table_id != first.table_id {
            return Err(CatalogError::InvalidMutation(
                "column default change only supports one table per operation".to_owned(),
            ));
        }
        if !unique.insert(change.column.column_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} is listed more than once for default change",
                change.column.column_id.0
            )));
        }
    }
    Ok(first.table_id)
}

fn reject_non_default_column_change(
    existing: &TableColumnRow,
    replacement: &TableColumnRow,
    table_id: TableId,
) -> CatalogResult<()> {
    if !existing.name.eq_ignore_ascii_case(&replacement.name)
        || existing.column_type != replacement.column_type
        || existing.nulls_allowed != replacement.nulls_allowed
        || existing.parent_id != replacement.parent_id
    {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} changes more than its default",
            existing.column_id.0, table_id.0
        )));
    }
    if same_default_metadata(existing, replacement) {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} default is unchanged",
            existing.column_id.0, table_id.0
        )));
    }
    Ok(())
}
