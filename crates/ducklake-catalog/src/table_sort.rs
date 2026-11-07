use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, TableId, TableSortRow,
    ids::CatalogOrderId,
    store::latest_snapshot,
    table_store::{load_current_table_row, reject_table_conflicts_since_base},
    table_version_commit::commit_replaced_table_version,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSortChange {
    pub table_id: TableId,
    pub sort: Option<TableSortRow>,
}

impl TableSortChange {
    #[must_use]
    pub fn new(table_id: TableId, sort: Option<TableSortRow>) -> Self {
        Self { table_id, sort }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedTableSort {
    pub table_id: TableId,
    pub previous: Option<TableSortRow>,
    pub changed: Option<TableSortRow>,
}

pub fn commit_change_table_sort(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    change: &TableSortChange,
) -> CatalogResult<Option<ChangedTableSort>> {
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous = load_current_table_row(kv, catalog, change.table_id)?
        .ok_or(CatalogError::NotFound("table"))?;
    if previous.sort == change.sort {
        return Ok(None);
    }

    let mut next = previous.clone();
    next.sort = change.sort.clone();
    commit_replaced_table_version(
        kv,
        catalog,
        change.table_id,
        latest.sequence,
        previous.clone(),
        next,
    )?;
    Ok(Some(ChangedTableSort {
        table_id: change.table_id,
        previous: previous.sort,
        changed: change.sort.clone(),
    }))
}

pub fn commit_change_table_sort_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    change: &TableSortChange,
) -> CatalogResult<Option<ChangedTableSort>> {
    reject_table_conflicts_since_base(kv, catalog, change.table_id, base_order, through_order)?;
    commit_change_table_sort(kv, catalog, change)
}
