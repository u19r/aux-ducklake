use crate::{
    CatalogId, CatalogResult, DataFileRow, list_data_files, list_snapshots,
    runtime_protocol::RuntimeCatalogBackend,
};

#[cfg(not(test))]
use crate::latest_snapshot;

use crate::runtime_metadata_ops::*;

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
) -> CatalogResult<Vec<u8>> {
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
    Ok(data_file_rows_payload(&rows, &snapshots).into_bytes())
}

pub(crate) fn render_current_metadata_data_file_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let (rows, snapshots) = (
        list_data_files(&kv, catalog)?,
        list_snapshots(&kv, catalog)?,
    );
    let rows = current_metadata_data_file_rows(rows);
    Ok(data_file_mirror_sql(&rows, &snapshots).into_bytes())
}

pub(crate) fn list_current_metadata_data_file_rows_for_data_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let data_file_ids = data_file_ids_payload_values(payload)?;
    let (rows, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        (
            list_current_data_files_for_data_file_ids(&kv, catalog, &data_file_ids)?,
            list_snapshots(&kv, catalog)?,
        )
    };
    Ok(data_file_rows_payload(&rows, &snapshots).into_bytes())
}

#[cfg(not(test))]
pub(super) fn cached_foundationdb_current_metadata_data_file_rows(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"data_file_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::CurrentDataFiles,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload);
    }
    let context =
        crate::runtime_read_context::CatalogCurrentFilesContext::for_current_files(&kv, catalog)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    let rows = context.current_data_files().to_vec();
    let payload = data_file_rows_payload(&rows, &snapshots).into_bytes();
    cache.insert(key, payload.clone());
    Ok(payload)
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
        out.push_str(&format!(
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
        ));
    }
    out
}

pub(super) fn data_file_mirror_sql(
    rows: &[DataFileRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_data_file;\n".to_owned();
    append_data_file_mirror_inserts(&mut out, rows, snapshots);
    out
}

pub(super) fn append_data_file_mirror_inserts(
    out: &mut String,
    rows: &[DataFileRow],
    snapshots: &[crate::SnapshotRow],
) {
    for row in rows {
        let begin_snapshot = snapshot_sequence_for_order(snapshots, row.validity.begin_order)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NULL".to_owned());
        let end_snapshot =
            snapshot_sequence_for_optional_end_order(snapshots, row.validity.end_order);
        let end_snapshot = null_if_empty(&end_snapshot);
        let partial_max = row
            .max_partial_order
            .and_then(|order| snapshot_sequence_for_order(snapshots, order))
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NULL".to_owned());
        let footer_size = optional_u64_sql(row.footer_size);
        let row_id_start = if row.row_id_start_known {
            row.row_id_start.to_string()
        } else {
            "NULL".to_owned()
        };
        let mapping_id = optional_u64_sql(row.mapping_id);
        let partition_id = format!(
            "(CASE WHEN EXISTS (SELECT 1 FROM {{METADATA_CATALOG}}.ducklake_file_partition_value WHERE data_file_id = {}) \
THEN (SELECT partition_id FROM {{METADATA_CATALOG}}.ducklake_partition_info WHERE table_id = {} AND end_snapshot IS NULL ORDER BY partition_id DESC LIMIT 1) ELSE NULL END)",
            row.data_file_id.0, row.table_id.0
        );
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_data_file VALUES ({}, {}, {}, {}, {}, {}, false, 'parquet', {}, {}, {}, {}, {}, {}, {}, {});\n",
            row.data_file_id.0,
            row.table_id.0,
            begin_snapshot,
            end_snapshot,
            row.data_file_id.0,
            sql_string(&row.path),
            row.record_count,
            row.file_size_bytes,
            footer_size,
            row_id_start,
            partition_id,
            optional_encryption_key_sql(&row.encryption_key),
            mapping_id,
            partial_max
        ));
    }
}
