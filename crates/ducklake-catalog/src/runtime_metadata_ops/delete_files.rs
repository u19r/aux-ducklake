use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv, RangeDirection,
    keys::{delete_file_key, delete_file_timeline_prefix},
    list_snapshots,
    runtime_protocol::RuntimeCatalogBackend,
};

#[cfg(not(test))]
use crate::latest_snapshot;

use crate::runtime_metadata_ops::*;

pub(crate) fn list_delete_file_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    #[cfg(test)]
    let _ = backend;
    #[cfg(not(test))]
    if backend == RuntimeCatalogBackend::FoundationDb {
        return cached_foundationdb_delete_file_rows(catalog);
    }
    let (rows, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        (
            list_delete_files(&kv, catalog)?,
            list_snapshots(&kv, catalog)?,
        )
    };
    Ok(delete_file_rows_payload(&rows, &snapshots).into_bytes())
}

pub(crate) fn render_delete_file_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let (rows, snapshots) = (
        list_delete_files(&kv, catalog)?,
        list_snapshots(&kv, catalog)?,
    );
    Ok(delete_file_mirror_sql(&rows, &snapshots).into_bytes())
}

pub(crate) fn render_scheduled_cleanup_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let (rows, snapshots) = (
        list_delete_files(&kv, catalog)?,
        list_snapshots(&kv, catalog)?,
    );
    Ok(scheduled_cleanup_mirror_sql(&rows, &snapshots).into_bytes())
}

pub(crate) fn render_bounded_delete_file_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let delete_file_ids = delete_file_ids_payload_values(payload)?;
    let kv = open_foundationdb_catalog()?;
    let rows = list_delete_files_for_delete_file_ids(&kv, catalog, &delete_file_ids)?;
    let semantic_begin_orders = semantic_delete_begin_orders_for_rows(&kv, catalog, &rows)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    Ok(
        bounded_delete_file_mirror_sql(&delete_file_ids, &rows, &semantic_begin_orders, &snapshots)
            .into_bytes(),
    )
}

pub(crate) fn list_delete_file_rows_for_delete_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let delete_file_ids = delete_file_ids_payload_values(payload)?;
    let (rows, semantic_begin_orders, snapshots) = {
        let kv = open_foundationdb_catalog()?;
        let rows = list_delete_files_for_delete_file_ids(&kv, catalog, &delete_file_ids)?;
        (
            rows.clone(),
            semantic_delete_begin_orders_for_rows(&kv, catalog, &rows)?,
            list_snapshots(&kv, catalog)?,
        )
    };
    Ok(
        delete_file_rows_payload_with_semantic_begin(&rows, &semantic_begin_orders, &snapshots)
            .into_bytes(),
    )
}

#[cfg(not(test))]
pub(super) fn cached_foundationdb_delete_file_rows(catalog: CatalogId) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"delete_file_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::DeleteFiles,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload);
    }
    let rows = list_delete_files(&kv, catalog)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    let payload = delete_file_rows_payload(&rows, &snapshots).into_bytes();
    cache.insert(key, payload.clone());
    Ok(payload)
}

pub(super) fn delete_file_rows_payload(
    rows: &[DeleteFileRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let semantic_begin_orders = semantic_delete_begin_orders_from_rows(rows);
    delete_file_rows_payload_with_semantic_begin(rows, &semantic_begin_orders, snapshots)
}

pub(super) fn delete_file_rows_payload_with_semantic_begin(
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
    snapshots: &[crate::SnapshotRow],
) -> String {
    let mut out = format!("delete_file_count={}\n", rows.len());
    for row in rows {
        let begin_order = semantic_begin_orders
            .get(&row.data_file_id)
            .copied()
            .unwrap_or(row.validity.begin_order);
        let begin_snapshot = snapshot_sequence_for_order(snapshots, begin_order)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let end_snapshot =
            snapshot_sequence_for_optional_end_order(snapshots, row.validity.end_order);
        out.push_str(&format!(
            "delete_file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.delete_file_id.0,
            row.data_file_id.0,
            row.path,
            row.record_count,
            row.file_size_bytes,
            begin_snapshot,
            end_snapshot,
            row.encryption_key
        ));
    }
    out
}

pub(super) fn delete_file_mirror_sql(
    rows: &[DeleteFileRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let semantic_begin_orders = semantic_delete_begin_orders_from_rows(rows);
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_delete_file;\n".to_owned();
    append_delete_file_mirror_inserts(&mut out, rows, &semantic_begin_orders, snapshots);
    out
}

pub(super) fn append_delete_file_mirror_inserts(
    out: &mut String,
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
    snapshots: &[crate::SnapshotRow],
) {
    for row in rows {
        let begin_order = semantic_begin_orders
            .get(&row.data_file_id)
            .copied()
            .unwrap_or(row.validity.begin_order);
        let begin_snapshot = snapshot_sequence_for_order(snapshots, begin_order)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NULL".to_owned());
        let end_snapshot = null_if_empty(&snapshot_sequence_for_optional_end_order(
            snapshots,
            row.validity.end_order,
        ));
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_delete_file VALUES ({}, (SELECT table_id FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id = {}), {}, {}, {}, {}, false, 'parquet', {}, {}, 0, {}, NULL);\n",
            row.delete_file_id.0,
            row.data_file_id.0,
            begin_snapshot,
            end_snapshot,
            row.data_file_id.0,
            sql_string(&row.path),
            row.record_count,
            row.file_size_bytes,
            optional_encryption_key_sql(&row.encryption_key)
        ));
    }
}

pub(super) fn bounded_append_mirror_sql(
    data_file_ids: &[DataFileId],
    partition_rows: Vec<FilePartitionValueRow>,
    data_files: &[DataFileRow],
    file_stats: &[FileColumnStatsRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let ids = data_file_ids_sql(data_file_ids);
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_file_partition_value WHERE data_file_id IN ({ids});\n\
DELETE FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id IN ({ids});\n\
DELETE FROM {{METADATA_CATALOG}}.ducklake_file_column_stats WHERE data_file_id IN ({ids});\n"
    );
    append_file_partition_values_mirror_inserts(&mut out, partition_rows);
    append_data_file_mirror_inserts(&mut out, data_files, snapshots);
    append_file_column_stats_mirror_inserts(&mut out, file_stats.to_vec());
    out
}

pub(super) fn bounded_delete_file_mirror_sql(
    delete_file_ids: &[DeleteFileId],
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
    snapshots: &[crate::SnapshotRow],
) -> String {
    let ids = delete_file_ids_sql(delete_file_ids);
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_delete_file WHERE delete_file_id IN ({ids});\n"
    );
    append_delete_file_mirror_inserts(&mut out, rows, semantic_begin_orders, snapshots);
    out
}

pub(super) fn scheduled_cleanup_mirror_sql(
    rows: &[DeleteFileRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let mut out =
        "DELETE FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion;\n".to_owned();
    for row in rows {
        let end_snapshot =
            snapshot_sequence_for_optional_end_order(snapshots, row.validity.end_order);
        if end_snapshot.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_files_scheduled_for_deletion VALUES ({}, {}, false, NOW());\n",
            row.delete_file_id.0,
            sql_string(&row.path)
        ));
    }
    out
}

pub(super) fn semantic_delete_begin_orders_from_rows(
    rows: &[DeleteFileRow],
) -> BTreeMap<DataFileId, crate::CatalogOrderId> {
    let mut begin_orders = BTreeMap::new();
    for row in rows {
        begin_orders
            .entry(row.data_file_id)
            .and_modify(|begin_order| {
                if row.validity.begin_order < *begin_order {
                    *begin_order = row.validity.begin_order;
                }
            })
            .or_insert(row.validity.begin_order);
    }
    begin_orders
}

pub(super) fn semantic_delete_begin_orders_for_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DeleteFileRow],
) -> CatalogResult<BTreeMap<DataFileId, crate::CatalogOrderId>> {
    let data_file_ids = rows
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let mut begin_orders = BTreeMap::new();
    for data_file_id in data_file_ids {
        for item in kv.scan_prefix(
            &delete_file_timeline_prefix(catalog, data_file_id),
            RangeDirection::Forward,
            usize::MAX,
        )? {
            let row = delete_file_from_timeline_value(kv, catalog, &item.value)?;
            begin_orders
                .entry(data_file_id)
                .and_modify(|begin_order| {
                    if row.validity.begin_order < *begin_order {
                        *begin_order = row.validity.begin_order;
                    }
                })
                .or_insert(row.validity.begin_order);
        }
    }
    Ok(begin_orders)
}

pub(super) fn delete_file_from_timeline_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    value: &[u8],
) -> CatalogResult<DeleteFileRow> {
    if let Ok(row) = DeleteFileRow::decode(value) {
        return Ok(row);
    }
    if value.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            value.len()
        )));
    }
    let id_bytes: [u8; 8] = value.try_into().map_err(|_| {
        crate::CatalogError::Decode("delete file id pointer must be 8 bytes".to_owned())
    })?;
    let delete_file_id = DeleteFileId(u64::from_be_bytes(id_bytes));
    let Some(row) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&row)
}
