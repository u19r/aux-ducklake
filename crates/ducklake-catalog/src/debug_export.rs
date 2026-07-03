use crate::{
    CatalogId, CatalogResult, DataFileRow, DeleteFileRow, InlineDeletionChunkRow,
    InlineTableChunkRow, OrderedCatalogKv, RangeDirection, SnapshotRow, TableColumnRow, TableId,
    TableRow,
    inline_data::decode_inline_table_item,
    keys::{KeyFamily, family_prefix},
    list_current_data_files, list_snapshots,
    table_store::list_table_rows,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogDebugRow {
    Snapshot(SnapshotRow),
    Table(TableRow),
    Column {
        table_id: TableId,
        table: TableRow,
        column: TableColumnRow,
    },
    DataFile(DataFileRow),
    DeleteFile(DeleteFileRow),
}

pub fn list_catalog_debug_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<CatalogDebugRow>> {
    let mut rows = Vec::new();
    rows.extend(
        list_snapshots(kv, catalog)?
            .into_iter()
            .map(CatalogDebugRow::Snapshot),
    );
    rows.extend(list_table_debug_rows(kv, catalog, limit)?);
    rows.extend(list_data_file_debug_rows(kv, catalog, limit)?);
    rows.extend(list_delete_file_debug_rows(kv, catalog, limit)?);
    rows.truncate(limit);
    Ok(rows)
}

pub fn list_inline_table_debug_chunks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineTable),
        RangeDirection::Forward,
        limit,
    )?
    .into_iter()
    .map(|item| decode_inline_table_item(catalog, &item.key, &item.value))
    .collect()
}

pub fn list_inline_deletion_debug_chunks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<InlineDeletionChunkRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineDeletion),
        RangeDirection::Forward,
        limit,
    )?
    .into_iter()
    .map(|item| InlineDeletionChunkRow::decode(&item.value))
    .collect()
}

fn list_table_debug_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<CatalogDebugRow>> {
    let mut rows = Vec::new();
    for table in list_table_rows(kv, catalog)? {
        rows.push(CatalogDebugRow::Table(table.clone()));
        rows.extend(
            table
                .columns
                .iter()
                .cloned()
                .map(|column| CatalogDebugRow::Column {
                    table_id: table.table_id,
                    table: table.clone(),
                    column,
                }),
        );
        if rows.len() >= limit {
            break;
        }
    }
    rows.truncate(limit);
    Ok(rows)
}

fn list_data_file_debug_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<CatalogDebugRow>> {
    let mut rows = Vec::new();
    for table in list_table_rows(kv, catalog)? {
        for file in list_current_data_files(kv, catalog, table.table_id)? {
            rows.push(CatalogDebugRow::DataFile(file));
            if rows.len() >= limit {
                return Ok(rows);
            }
        }
    }
    Ok(rows)
}

fn list_delete_file_debug_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    limit: usize,
) -> CatalogResult<Vec<CatalogDebugRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::DeleteFile),
        RangeDirection::Forward,
        limit,
    )?
    .into_iter()
    .map(|item| DeleteFileRow::decode(&item.value).map(CatalogDebugRow::DeleteFile))
    .collect()
}
