use super::{file_column_stats_payload, open_foundationdb_catalog, parse_u64};
use crate::{
    CatalogId, CatalogResult, ColumnId, DataFileId, runtime_protocol::RuntimeCatalogBackend,
};
use std::collections::BTreeSet;

pub(crate) fn list_current_metadata_file_column_stats_rows_for_data_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (files, columns) = requested_file_statistics(payload)?;
    let kv = open_foundationdb_catalog()?;
    let rows = crate::file_stats::list_file_column_stats_for_data_file_columns(
        &kv, catalog, &files, &columns,
    )?;
    file_column_stats_payload(&rows)
}

pub(super) fn requested_file_statistics(
    payload: &[u8],
) -> CatalogResult<(Vec<DataFileId>, BTreeSet<ColumnId>)> {
    let mut files = Vec::new();
    let mut columns = BTreeSet::new();
    for row in
        crate::runtime_tabular_payload::TabularPayload::new("file statistics request", payload)?
    {
        let row = row?;
        let fields = row.fields();
        let fields = fields.as_slice();
        if fields.len() != 2 {
            return Err(row.invalid());
        }
        let value = parse_u64(fields[1], "file statistics identifier")?;
        match fields[0] {
            "data_file_id" => files.push(DataFileId(value)),
            "column_id" => {
                columns.insert(ColumnId(value));
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok((files, columns))
}
