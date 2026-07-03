use crate::{
    CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, TableId, TableRow,
    runtime_snapshots::snapshot_schema_versions_by_order, store::list_snapshots,
    table_store::list_table_rows,
};

pub fn table_row_for_inline_schema(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_version: u64,
) -> CatalogResult<TableRow> {
    let table_rows = list_table_rows(kv, catalog)?;
    if let Some(table) = earliest_table_with_inline_schema(&table_rows, table_id, schema_version) {
        return Ok(table);
    }
    let order = begin_order_for_table_schema(kv, catalog, table_id, schema_version, &table_rows)?;
    table_visible_at(&table_rows, table_id, order)
        .ok_or(crate::CatalogError::NotFound("inline table schema version"))
}

fn earliest_table_with_inline_schema(
    table_rows: &[TableRow],
    table_id: TableId,
    schema_version: u64,
) -> Option<TableRow> {
    table_rows
        .iter()
        .find(|row| {
            row.table_id == table_id
                && row
                    .inlined_data_tables
                    .iter()
                    .any(|inlined| inlined.schema_version == schema_version)
        })
        .cloned()
}

fn begin_order_for_table_schema(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_version: u64,
    table_rows: &[TableRow],
) -> CatalogResult<CatalogOrderId> {
    let schema_versions = snapshot_schema_versions_by_order(kv, catalog)?;
    for snapshot in list_snapshots(kv, catalog)? {
        if schema_versions.get(&snapshot.order).copied() != Some(schema_version) {
            continue;
        }
        let Some(table) = table_visible_at(table_rows, table_id, snapshot.order) else {
            continue;
        };
        if table.validity.begin_order <= snapshot.order {
            return Ok(snapshot.order);
        }
    }
    Err(crate::CatalogError::NotFound("inline table schema version"))
}

fn table_visible_at(
    table_rows: &[TableRow],
    table_id: TableId,
    order: CatalogOrderId,
) -> Option<TableRow> {
    table_rows
        .iter()
        .find(|row| row.table_id == table_id && row.validity.is_visible_at(order))
        .cloned()
}

#[cfg(test)]
#[path = "inline_read_schema_tests.rs"]
mod inline_read_schema_tests;
