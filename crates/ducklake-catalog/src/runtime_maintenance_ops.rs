use crate::{
    CatalogId, CatalogResult, DataFileId, DeleteFileId,
    runtime_protocol::RuntimeCatalogBackend,
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
};

const REMOVE_CLEANUP_FILES: &str = "RemoveCleanupFiles";

#[derive(Debug, Default)]
struct CleanupRemovalRequest {
    data_file_ids: Vec<DataFileId>,
    delete_file_ids: Vec<DeleteFileId>,
}

pub(crate) fn remove_cleanup_files(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let request = cleanup_removal_payload_values(payload)?;
    let (data_files, delete_files) = { remove_foundationdb_cleanup_files(catalog, &request)? };
    let mut output = format!(
        "removed_cleanup_file_count={}\n",
        data_files.len() + delete_files.len()
    );
    for file in data_files {
        output.push_str(&format!(
            "removed_cleanup_file\tdata\t{}\n",
            file.data_file_id.0
        ));
    }
    for file in delete_files {
        output.push_str(&format!(
            "removed_cleanup_file\tdelete\t{}\n",
            file.delete_file.delete_file_id.0
        ));
    }
    Ok(output.into_bytes())
}

#[cfg(feature = "foundationdb")]
fn remove_foundationdb_cleanup_files(
    catalog: CatalogId,
    request: &CleanupRemovalRequest,
) -> CatalogResult<(Vec<crate::DataFileRow>, Vec<crate::DeleteFileCleanupRow>)> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    Ok((
        kv.remove_old_data_files_checked(catalog, &request.data_file_ids)?,
        kv.remove_old_delete_files_checked(catalog, &request.delete_file_ids)?,
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn remove_foundationdb_cleanup_files(
    _catalog: CatalogId,
    _request: &CleanupRemovalRequest,
) -> CatalogResult<(Vec<crate::DataFileRow>, Vec<crate::DeleteFileCleanupRow>)> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

fn cleanup_removal_payload_values(payload: &[u8]) -> CatalogResult<CleanupRemovalRequest> {
    let mut request = CleanupRemovalRequest::default();
    for row in TabularPayload::new(REMOVE_CLEANUP_FILES, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["cleanup_file", "data", id] => request.data_file_ids.push(DataFileId(
                parse_u64_field(REMOVE_CLEANUP_FILES, id, "cleanup data file id")?,
            )),
            ["cleanup_file", "delete", id] => request.delete_file_ids.push(DeleteFileId(
                parse_u64_field(REMOVE_CLEANUP_FILES, id, "cleanup delete file id")?,
            )),
            _ => return Err(row.invalid()),
        }
    }
    Ok(request)
}
