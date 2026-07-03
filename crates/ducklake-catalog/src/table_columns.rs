use std::collections::BTreeSet;

use crate::{
    CatalogError, CatalogId, CatalogResult, ColumnId, MutableCatalogKv, OrderedCatalogKv,
    TableColumnRow, TableId, TableVersionReplacement,
    ids::CatalogOrderId,
    store::latest_snapshot,
    table_store::{load_current_table_row, reject_table_conflicts_since_base},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRename {
    pub table_id: TableId,
    pub column: TableColumnRow,
}

impl ColumnRename {
    #[must_use]
    pub fn new(table_id: TableId, column: TableColumnRow) -> Self {
        Self { table_id, column }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenamedColumn {
    pub table_id: TableId,
    pub previous: TableColumnRow,
    pub renamed: TableColumnRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDrop {
    pub table_id: TableId,
    pub column_id: ColumnId,
}

impl ColumnDrop {
    #[must_use]
    pub fn new(table_id: TableId, column_id: ColumnId) -> Self {
        Self {
            table_id,
            column_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedColumn {
    pub table_id: TableId,
    pub column: TableColumnRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTypeChange {
    pub table_id: TableId,
    pub column: TableColumnRow,
}

impl ColumnTypeChange {
    #[must_use]
    pub fn new(table_id: TableId, column: TableColumnRow) -> Self {
        Self { table_id, column }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedColumnType {
    pub table_id: TableId,
    pub previous: TableColumnRow,
    pub changed: TableColumnRow,
}

pub fn commit_rename_table_columns(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    renames: &[ColumnRename],
) -> CatalogResult<Vec<RenamedColumn>> {
    if renames.is_empty() {
        return Ok(Vec::new());
    }
    let table_id = reject_column_rename_batch_shape(renames)?;
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut next_columns = previous.columns.clone();
    let mut renamed = Vec::with_capacity(renames.len());
    for rename in renames {
        let Some(column_index) = next_columns
            .iter()
            .position(|column| column.column_id == rename.column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        let existing = next_columns[column_index].clone();
        reject_non_rename_column_change(&existing, &rename.column, table_id)?;
        reject_column_name_conflict(&next_columns, &existing, &rename.column.name, table_id)?;
        next_columns[column_index].name = rename.column.name.clone();
        renamed.push(RenamedColumn {
            table_id,
            previous: existing,
            renamed: next_columns[column_index].clone(),
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
    Ok(renamed)
}

pub fn commit_rename_table_columns_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    renames: &[ColumnRename],
) -> CatalogResult<Vec<RenamedColumn>> {
    let table_id = reject_column_rename_batch_shape(renames)?;
    reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    commit_rename_table_columns(kv, catalog, renames)
}

pub fn normalize_column_renames_to_current_shape(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    renames: &[ColumnRename],
) -> CatalogResult<Vec<ColumnRename>> {
    let table_id = reject_column_rename_batch_shape(renames)?;
    let _ = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let table =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut normalized = Vec::with_capacity(renames.len());
    for rename in renames {
        let Some(current) = table
            .columns
            .iter()
            .find(|column| column.column_id == rename.column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        let mut renamed = current.clone();
        renamed.name = rename.column.name.clone();
        normalized.push(ColumnRename::new(table_id, renamed));
    }
    Ok(normalized)
}

pub fn commit_drop_table_columns(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    drops: &[ColumnDrop],
) -> CatalogResult<Vec<DroppedColumn>> {
    if drops.is_empty() {
        return Ok(Vec::new());
    }
    let table_id = reject_column_drop_batch_shape(drops)?;
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut next_columns = previous.columns.clone();
    let mut dropped = Vec::with_capacity(drops.len());
    for drop in drops {
        if !next_columns
            .iter()
            .any(|column| column.column_id == drop.column_id)
        {
            return Err(CatalogError::NotFound("column"));
        }
        let dropped_ids = column_and_descendant_ids(&next_columns, drop.column_id);
        let mut retained = Vec::with_capacity(next_columns.len().saturating_sub(dropped_ids.len()));
        for column in next_columns {
            if dropped_ids.contains(&column.column_id) {
                dropped.push(DroppedColumn { table_id, column });
            } else {
                retained.push(column);
            }
        }
        next_columns = retained;
    }

    commit_column_version(
        kv,
        catalog,
        table_id,
        latest.sequence,
        previous,
        next_columns,
    )?;
    Ok(dropped)
}

pub fn commit_drop_table_columns_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    drops: &[ColumnDrop],
) -> CatalogResult<Vec<DroppedColumn>> {
    let table_id = reject_column_drop_batch_shape(drops)?;
    reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    commit_drop_table_columns(kv, catalog, drops)
}

pub fn commit_change_table_column_types(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    changes: &[ColumnTypeChange],
) -> CatalogResult<Vec<ChangedColumnType>> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let table_id = reject_column_type_change_batch_shape(changes)?;
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut next_columns = previous.columns.clone();
    let mut changed = Vec::with_capacity(changes.len());
    for change in changes {
        let Some(column_index) = next_columns
            .iter()
            .position(|column| column.column_id == change.column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        let existing = next_columns[column_index].clone();
        reject_parent_column_type_change(&next_columns, &existing, table_id)?;
        reject_non_type_column_identity_change(&existing, &change.column, table_id)?;
        next_columns[column_index] = type_change_replacement(&existing, &change.column);
        changed.push(ChangedColumnType {
            table_id,
            previous: existing,
            changed: next_columns[column_index].clone(),
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

pub fn commit_change_table_column_types_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    changes: &[ColumnTypeChange],
) -> CatalogResult<Vec<ChangedColumnType>> {
    let table_id = reject_column_type_change_batch_shape(changes)?;
    reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    commit_change_table_column_types(kv, catalog, changes)
}

pub(crate) fn commit_column_version(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    previous_sequence: crate::RawSnapshotSequence,
    previous: crate::TableRow,
    next_columns: Vec<TableColumnRow>,
) -> CatalogResult<()> {
    let mut next = previous.clone();
    next.columns = next_columns;
    kv.commit_table_replacements(
        catalog,
        previous_sequence,
        vec![TableVersionReplacement::new(table_id, previous, next)],
    )
}

fn reject_column_rename_batch_shape(renames: &[ColumnRename]) -> CatalogResult<TableId> {
    let first = renames
        .first()
        .ok_or_else(|| CatalogError::InvalidMutation("empty column rename batch".to_owned()))?;
    for (index, rename) in renames.iter().enumerate() {
        if rename.table_id != first.table_id {
            return Err(CatalogError::InvalidMutation(
                "column rename only supports one table per operation".to_owned(),
            ));
        }
        if renames[..index]
            .iter()
            .any(|previous| previous.column.column_id == rename.column.column_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} is listed more than once for rename",
                rename.column.column_id.0
            )));
        }
    }
    Ok(first.table_id)
}

fn reject_column_drop_batch_shape(drops: &[ColumnDrop]) -> CatalogResult<TableId> {
    let first = drops
        .first()
        .ok_or_else(|| CatalogError::InvalidMutation("empty column drop batch".to_owned()))?;
    for (index, drop) in drops.iter().enumerate() {
        if drop.table_id != first.table_id {
            return Err(CatalogError::InvalidMutation(
                "column drop only supports one table per operation".to_owned(),
            ));
        }
        if drops[..index]
            .iter()
            .any(|previous| previous.column_id == drop.column_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} is listed more than once for drop",
                drop.column_id.0
            )));
        }
    }
    Ok(first.table_id)
}

fn reject_column_type_change_batch_shape(changes: &[ColumnTypeChange]) -> CatalogResult<TableId> {
    let first = changes.first().ok_or_else(|| {
        CatalogError::InvalidMutation("empty column type change batch".to_owned())
    })?;
    for (index, change) in changes.iter().enumerate() {
        if change.table_id != first.table_id {
            return Err(CatalogError::InvalidMutation(
                "column type change only supports one table per operation".to_owned(),
            ));
        }
        if changes[..index]
            .iter()
            .any(|previous| previous.column.column_id == change.column.column_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} is listed more than once for type change",
                change.column.column_id.0
            )));
        }
    }
    Ok(first.table_id)
}

fn reject_non_rename_column_change(
    existing: &TableColumnRow,
    replacement: &TableColumnRow,
    table_id: TableId,
) -> CatalogResult<()> {
    if existing.name.eq_ignore_ascii_case(&replacement.name) {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} is not renamed",
            existing.column_id.0, table_id.0
        )));
    }
    if existing.column_type != replacement.column_type
        || existing.nulls_allowed != replacement.nulls_allowed
        || existing.parent_id != replacement.parent_id
    {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} changes more than its name",
            existing.column_id.0, table_id.0
        )));
    }
    Ok(())
}

fn reject_non_type_column_identity_change(
    existing: &TableColumnRow,
    replacement: &TableColumnRow,
    table_id: TableId,
) -> CatalogResult<()> {
    if !existing.name.eq_ignore_ascii_case(&replacement.name)
        || existing.parent_id != replacement.parent_id
    {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} changes identity metadata during type change",
            existing.column_id.0, table_id.0
        )));
    }
    if existing.parent_id.is_none() && existing.nulls_allowed != replacement.nulls_allowed {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} changes top-level nullability during type change",
            existing.column_id.0, table_id.0
        )));
    }
    if existing.column_type == replacement.column_type {
        return Err(CatalogError::InvalidMutation(format!(
            "column {} on table {} type is unchanged",
            existing.column_id.0, table_id.0
        )));
    }
    Ok(())
}

pub(crate) fn same_default_metadata(
    existing: &TableColumnRow,
    replacement: &TableColumnRow,
) -> bool {
    existing.initial_default == replacement.initial_default
        && existing.default_value == replacement.default_value
        && existing.default_value_type == replacement.default_value_type
}

fn type_change_replacement(
    existing: &TableColumnRow,
    replacement: &TableColumnRow,
) -> TableColumnRow {
    let mut next = existing.clone();
    next.column_type = replacement.column_type.clone();
    next.nulls_allowed = replacement.nulls_allowed;
    next.initial_default = replacement.initial_default.clone();
    next.default_value = replacement.default_value.clone();
    next.default_value_type = replacement.default_value_type.clone();
    next
}

fn reject_column_name_conflict(
    columns: &[TableColumnRow],
    existing: &TableColumnRow,
    new_name: &str,
    table_id: TableId,
) -> CatalogResult<()> {
    if columns.iter().any(|column| {
        column.parent_id == existing.parent_id
            && column.column_id != existing.column_id
            && column.name.eq_ignore_ascii_case(new_name)
    }) {
        return Err(CatalogError::InvalidMutation(format!(
            "column name {new_name} already exists on table {}",
            table_id.0
        )));
    }
    Ok(())
}

fn reject_parent_column_type_change(
    columns: &[TableColumnRow],
    existing: &TableColumnRow,
    table_id: TableId,
) -> CatalogResult<()> {
    if columns
        .iter()
        .any(|column| column.parent_id == Some(existing.column_id))
    {
        return Err(CatalogError::InvalidMutation(format!(
            "parent column type change is not implemented for column {} on table {}",
            existing.column_id.0, table_id.0
        )));
    }
    Ok(())
}

fn column_and_descendant_ids(columns: &[TableColumnRow], root: ColumnId) -> BTreeSet<ColumnId> {
    let mut ids = BTreeSet::from([root]);
    loop {
        let len_before = ids.len();
        for column in columns {
            if column
                .parent_id
                .is_some_and(|parent_id| ids.contains(&parent_id))
            {
                ids.insert(column.column_id);
            }
        }
        if ids.len() == len_before {
            return ids;
        }
    }
}

#[cfg(test)]
#[path = "table_columns_tests.rs"]
mod table_columns_tests;
