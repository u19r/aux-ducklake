use crate::{
    CatalogId, CatalogResult, DataFileRow, DeleteFileRow, OrderedCatalogKv, RangeDirection,
    TableId,
    keys::{KeyFamily, family_prefix},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnownCleanupFileRow {
    Data(DataFileRow),
    Delete {
        delete_file: DeleteFileRow,
        table_id: TableId,
    },
}

pub fn list_known_files_for_orphan_cleanup(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<KnownCleanupFileRow>> {
    let data_files = list_all_data_file_rows(kv, catalog)?;
    let delete_files = list_all_delete_file_rows(kv, catalog)?;
    let mut rows = Vec::with_capacity(data_files.len() + delete_files.len());
    for data_file in &data_files {
        rows.push(KnownCleanupFileRow::Data(data_file.clone()));
    }
    for delete_file in delete_files {
        let Some(data_file) = data_files
            .iter()
            .find(|row| row.data_file_id == delete_file.data_file_id)
        else {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "delete file {} references missing data file {}",
                delete_file.delete_file_id.0, delete_file.data_file_id.0
            )));
        };
        rows.push(KnownCleanupFileRow::Delete {
            delete_file,
            table_id: data_file.table_id,
        });
    }
    Ok(rows)
}

fn list_all_data_file_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DataFileRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::DataFile),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| DataFileRow::decode(&item.value))
    .collect()
}

fn list_all_delete_file_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<DeleteFileRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::DeleteFile),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| DeleteFileRow::decode(&item.value))
    .collect()
}
