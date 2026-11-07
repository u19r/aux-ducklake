use crate::{
    CatalogId, CatalogResult, KnownCleanupFileRow, OrderedCatalogKv,
    list_known_files_for_orphan_cleanup, list_old_data_files_for_cleanup,
    list_old_delete_files_for_cleanup,
    maintenance::{
        delete_file_is_safe_for_physical_cleanup, list_scheduled_data_file_cleanup_rows,
        list_scheduled_delete_file_cleanup_rows, scheduled_data_file_is_safe_for_physical_cleanup,
    },
    store::list_snapshots,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OldFilesCleanupRequest {
    pub cleanup_all: bool,
    pub schedule_before_micros: Option<i64>,
}

pub(crate) fn old_files_cleanup_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    request: OldFilesCleanupRequest,
) -> CatalogResult<Vec<u8>> {
    let data_files = cleanup_data_files(kv, catalog, request)?;
    let delete_files = cleanup_delete_files(kv, catalog, request)?;
    let mut out = format!(
        "cleanup_file_count={}\n",
        data_files.len() + delete_files.len()
    );
    for file in data_files {
        out.push_str(&format!(
            "cleanup_file\tdata\t{}\t{}\t{}\n",
            file.data_file_id.0, file.table_id.0, file.path
        ));
    }
    for file in delete_files {
        out.push_str(&format!(
            "cleanup_file\tdelete\t{}\t{}\t{}\n",
            file.delete_file.delete_file_id.0, file.table_id.0, file.delete_file.path
        ));
    }
    Ok(out.into_bytes())
}

fn cleanup_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    request: OldFilesCleanupRequest,
) -> CatalogResult<Vec<crate::DataFileRow>> {
    if request.cleanup_all {
        return list_old_data_files_for_cleanup(kv, catalog);
    }
    let Some(before) = request.schedule_before_micros else {
        return Ok(Vec::new());
    };
    let snapshots = list_snapshots(kv, catalog)?;
    let mut rows = Vec::new();
    for row in list_scheduled_data_file_cleanup_rows(kv, catalog)? {
        if row.schedule_start_micros < before
            && scheduled_data_file_is_safe_for_physical_cleanup(kv, catalog, &row, &snapshots)?
        {
            rows.push(row.data_file);
        }
    }
    Ok(rows)
}

fn cleanup_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    request: OldFilesCleanupRequest,
) -> CatalogResult<Vec<crate::DeleteFileCleanupRow>> {
    if request.cleanup_all {
        let snapshots = list_snapshots(kv, catalog)?;
        let mut rows = Vec::new();
        for row in list_old_delete_files_for_cleanup(kv, catalog)? {
            if delete_file_is_safe_for_physical_cleanup(kv, catalog, &row.delete_file, &snapshots)?
            {
                rows.push(row);
            }
        }
        return Ok(rows);
    }
    let Some(before) = request.schedule_before_micros else {
        return Ok(Vec::new());
    };
    let snapshots = list_snapshots(kv, catalog)?;
    let mut rows = Vec::new();
    for row in list_scheduled_delete_file_cleanup_rows(kv, catalog)? {
        if row.schedule_start_micros < before
            && delete_file_is_safe_for_physical_cleanup(kv, catalog, &row.delete_file, &snapshots)?
        {
            rows.push(crate::DeleteFileCleanupRow {
                delete_file: row.delete_file,
                table_id: row.table_id,
            });
        }
    }
    Ok(rows)
}

pub(crate) fn known_files_cleanup_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let files = list_known_files_for_orphan_cleanup(kv, catalog)?;
    let mut out = format!("known_file_count={}\n", files.len());
    for file in files {
        match file {
            KnownCleanupFileRow::Data(row) => {
                out.push_str(&format!(
                    "known_file\tdata\t{}\t{}\t{}\n",
                    row.data_file_id.0, row.table_id.0, row.path
                ));
            }
            KnownCleanupFileRow::Delete {
                delete_file,
                table_id,
            } => {
                out.push_str(&format!(
                    "known_file\tdelete\t{}\t{}\t{}\n",
                    delete_file.delete_file_id.0, table_id.0, delete_file.path
                ));
            }
        }
    }
    Ok(out.into_bytes())
}

#[cfg(test)]
#[path = "runtime_cleanup_tests.rs"]
mod runtime_cleanup_tests;
