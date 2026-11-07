#[cfg(not(test))]
use crate::latest_snapshot;
use crate::runtime_protocol::RuntimePayload;
use crate::{
    CatalogId, CatalogResult, DataFileRow, list_data_files, list_snapshots,
    runtime_metadata_ops::*, runtime_protocol::RuntimeCatalogBackend,
};
use std::fmt::Write as _;

pub(crate) fn list_data_file_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let (rows, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        (
            list_data_files(&kv, catalog)?,
            list_snapshots(&kv, catalog)?,
        )
    };
    Ok(data_file_rows_payload(&rows, &snapshots).into_bytes())
}

pub(crate) fn list_current_metadata_data_file_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    #[cfg(test)]
    let _ = backend;
    #[cfg(not(test))]
    if backend == RuntimeCatalogBackend::FoundationDb {
        return cached_foundationdb_current_metadata_data_file_rows(catalog);
    }
    let (rows, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        (
            list_data_files(&kv, catalog)?,
            list_snapshots(&kv, catalog)?,
        )
    };
    let rows = current_metadata_data_file_rows(rows);
    Ok(data_file_rows_payload(&rows, &snapshots)
        .into_bytes()
        .into())
}

pub(crate) fn list_current_metadata_data_file_rows_for_data_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let data_file_ids = data_file_ids_payload_values(payload)?;
    let (rows, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        let rows = list_current_data_files_for_data_file_ids(&kv, catalog, &data_file_ids)?;
        let snapshots = list_snapshots_for_orders(&kv, catalog, data_file_snapshot_orders(&rows))?;
        (rows, snapshots)
    };
    Ok(data_file_rows_payload(&rows, &snapshots).into_bytes())
}

#[cfg(not(test))]
pub(super) fn cached_foundationdb_current_metadata_data_file_rows(
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"data_file_count=0\n".to_vec().into());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::CurrentDataFiles,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload.into());
    }
    let context =
        crate::runtime_read_context::CatalogCurrentFilesContext::for_current_files(&kv, catalog)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    let rows = context.current_data_files();
    let payload = data_file_rows_payload(&rows, &snapshots).into_bytes();
    let payload = std::sync::Arc::new(payload);
    cache.insert(key, payload.clone());
    Ok(payload.into())
}

pub(super) fn current_metadata_data_file_rows(mut rows: Vec<DataFileRow>) -> Vec<DataFileRow> {
    rows.retain(|row| row.validity.end_order.is_none());
    rows
}

pub(super) fn data_file_rows_payload(
    rows: &[DataFileRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let mut out = format!("data_file_count={}\n", rows.len());
    for row in rows {
        let begin_snapshot = snapshot_sequence_for_order(snapshots, row.validity.begin_order)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let end_snapshot =
            snapshot_sequence_for_optional_end_order(snapshots, row.validity.end_order);
        let partial_max = row
            .max_partial_order
            .and_then(|order| snapshot_sequence_for_order(snapshots, order))
            .map(|value| value.to_string())
            .unwrap_or_default();
        let footer_size = row
            .footer_size
            .map(|value| value.to_string())
            .unwrap_or_default();
        let row_id_start = if row.row_id_start_known {
            row.row_id_start.to_string()
        } else {
            Default::default()
        };
        write!(
            out,
            "data_file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.data_file_id.0,
            row.table_id.0,
            row.path,
            row.record_count,
            row.file_size_bytes,
            row_id_start,
            row.mapping_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
            begin_snapshot,
            end_snapshot,
            partial_max,
            footer_size,
            row.encryption_key
        )
        .expect("writing to String cannot fail");
    }
    out
}

pub(super) fn append_data_file_mirror_inserts(
    out: &mut String,
    rows: &[DataFileRow],
    snapshots: &[crate::SnapshotRow],
    partition_ids: &std::collections::BTreeMap<crate::DataFileId, u64>,
) {
    append_mirror_values(
        out,
        "ducklake_data_file",
        rows.iter().map(|row| {
            render_data_file_mirror_row(
                row,
                snapshots,
                partition_ids.get(&row.data_file_id).copied(),
            )
        }),
    );
}

fn render_data_file_mirror_row(
    row: &DataFileRow,
    snapshots: &[crate::SnapshotRow],
    partition_id: Option<u64>,
) -> Vec<String> {
    let begin_snapshot = snapshot_sequence_for_order(snapshots, row.validity.begin_order)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_owned());
    let end_snapshot = null_if_empty(&snapshot_sequence_for_optional_end_order(
        snapshots,
        row.validity.end_order,
    ));
    let partial_max = row
        .max_partial_order
        .and_then(|order| snapshot_sequence_for_order(snapshots, order))
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_owned());
    let partition_id = optional_u64_sql(partition_id);
    vec![
        row.data_file_id.0.to_string(),
        row.table_id.0.to_string(),
        begin_snapshot,
        end_snapshot,
        row.data_file_id.0.to_string(),
        sql_string(&row.path),
        "false".to_owned(),
        "'parquet'".to_owned(),
        row.record_count.to_string(),
        row.file_size_bytes.to_string(),
        optional_u64_sql(row.footer_size),
        optional_u64_sql(row.row_id_start_known.then_some(row.row_id_start)),
        partition_id,
        optional_encryption_key_sql(&row.encryption_key),
        optional_u64_sql(row.mapping_id),
        partial_max,
        "NULL".to_owned(),
    ]
}

pub(super) fn data_file_snapshot_orders(
    rows: &[DataFileRow],
) -> std::collections::BTreeSet<crate::CatalogOrderId> {
    rows.iter()
        .flat_map(|row| {
            [
                Some(row.validity.begin_order),
                row.validity.end_order,
                row.max_partial_order,
            ]
        })
        .flatten()
        .collect()
}
