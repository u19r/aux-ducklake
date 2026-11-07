use crate::{
    CatalogId, CatalogResult, OrderedCatalogKv, TableId, TableRow,
    runtime_snapshots::{SchemaVersionSnapshotScope, snapshot_schema_versions_by_order},
    table_store::list_table_rows_for_table,
};

pub fn table_row_for_inline_schema(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_version: u64,
) -> CatalogResult<TableRow> {
    let table_rows = list_table_rows_for_table(kv, catalog, table_id)?;
    if let Some(table) = earliest_table_with_inline_schema(&table_rows, table_id, schema_version) {
        return Ok(table);
    }
    find_table_for_schema(kv, catalog, schema_version, &table_rows)
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

fn find_table_for_schema(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    schema_version: u64,
    table_rows: &[TableRow],
) -> CatalogResult<TableRow> {
    let schema_versions =
        snapshot_schema_versions_by_order(kv, catalog, SchemaVersionSnapshotScope::Active)?;
    for (order, version) in schema_versions.as_ref() {
        if *version != schema_version {
            continue;
        }
        if let Some(table) = table_rows
            .iter()
            .find(|row| row.validity.is_visible_at(*order))
        {
            return Ok(table.clone());
        }
    }
    Err(crate::CatalogError::NotFound("inline table schema version"))
}

#[cfg(test)]
#[path = "inline_read_schema_tests.rs"]
mod inline_read_schema_tests;
