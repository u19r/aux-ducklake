use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv, RangeDirection,
    keys::{delete_file_key, delete_file_timeline_prefix},
    list_snapshots,
    maintenance::{
        ScheduledDataFileCleanupRow, ScheduledDeleteFileCleanupRow,
        load_scheduled_data_file_cleanup_rows, load_scheduled_delete_file_cleanup_rows,
    },
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

pub(crate) fn render_bounded_scheduled_cleanup_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let data_file_ids = unique_data_file_ids(&data_file_ids_payload_values(payload)?);
    let kv = open_foundationdb_catalog()?;
    Ok(
        bounded_scheduled_cleanup_mirror_sql_for_catalog(&kv, catalog, &data_file_ids)?
            .into_bytes(),
    )
}

pub(super) fn bounded_scheduled_cleanup_mirror_sql_for_catalog(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<String> {
    let mut delete_file_ids = BTreeSet::new();
    for data_file_id in data_file_ids {
        for item in kv.scan_prefix(
            &delete_file_timeline_prefix(catalog, *data_file_id),
            RangeDirection::Forward,
            usize::MAX,
        )? {
            delete_file_ids
                .insert(delete_file_from_timeline_value(kv, catalog, &item.value)?.delete_file_id);
        }
    }
    let delete_file_ids = delete_file_ids.into_iter().collect::<Vec<_>>();
    let data_files = load_scheduled_data_file_cleanup_rows(kv, catalog, data_file_ids)?;
    let delete_files = load_scheduled_delete_file_cleanup_rows(kv, catalog, &delete_file_ids)?;
    Ok(scheduled_cleanup_delta_sql(
        data_file_ids,
        &delete_file_ids,
        &data_files,
        &delete_files,
    ))
}

pub(crate) fn render_bounded_delete_file_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (explicit_delete_file_ids, mut data_file_ids) =
        bounded_delete_file_mirror_payload_values(payload)?;
    let kv = open_foundationdb_catalog()?;
    let explicit_rows =
        list_delete_files_for_delete_file_ids(&kv, catalog, &explicit_delete_file_ids)?;
    data_file_ids.extend(explicit_rows.iter().map(|row| row.data_file_id));
    let data_file_ids = unique_data_file_ids(&data_file_ids);
    let rows = list_delete_files_for_data_file_ids(&kv, catalog, &data_file_ids)?;
    let delete_file_ids = unique_delete_file_ids(
        &explicit_delete_file_ids
            .into_iter()
            .chain(rows.iter().map(|row| row.delete_file_id))
            .collect::<Vec<_>>(),
    );
    let semantic_begin_orders = semantic_delete_begin_orders_from_rows(&rows);
    let snapshots = list_snapshots_for_orders(
        &kv,
        catalog,
        snapshot_orders_for_delete_files(&rows, &semantic_begin_orders),
    )?;
    Ok(bounded_delete_file_mirror_sql(
        &delete_file_ids,
        &data_file_ids,
        &rows,
        &semantic_begin_orders,
        &snapshots,
    )
    .into_bytes())
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
        let semantic_begin_orders = semantic_delete_begin_orders_for_rows(&kv, catalog, &rows)?;
        let snapshots = list_snapshots_for_orders(
            &kv,
            catalog,
            snapshot_orders_for_delete_files(&rows, &semantic_begin_orders),
        )?;
        (rows.clone(), semantic_begin_orders, snapshots)
    };
    Ok(
        delete_file_rows_payload_with_semantic_begin(&rows, &semantic_begin_orders, &snapshots)
            .into_bytes(),
    )
}

fn snapshot_orders_for_delete_files(
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
) -> BTreeSet<crate::CatalogOrderId> {
    semantic_begin_orders
        .values()
        .copied()
        .chain(rows.iter().filter_map(|row| row.validity.end_order))
        .collect()
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
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_delete_file VALUES ({}, (SELECT table_id FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id = {}), {}, {}, {}, {}, false, 'parquet', {}, {}, 0, {}, NULL, NULL);\n",
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
    affected_table_ids: &[crate::TableId],
    partition_rows: Vec<FilePartitionValueRow>,
    data_files: &[DataFileRow],
    file_stats: &[FileColumnStatsRow],
    snapshots: &[crate::SnapshotRow],
) -> String {
    let ids = data_file_ids_sql(data_file_ids);
    let table_ids = table_ids_sql(affected_table_ids);
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_file_partition_value WHERE data_file_id IN ({ids});\n\
DELETE FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id IN ({ids});\n\
DELETE FROM {{METADATA_CATALOG}}.ducklake_file_column_stats WHERE data_file_id IN ({ids});\n"
    );
    append_file_partition_values_mirror_inserts(&mut out, partition_rows);
    append_data_file_mirror_inserts(&mut out, data_files, snapshots);
    append_file_column_stats_mirror_inserts(&mut out, file_stats.to_vec());
    out.push_str(&format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_table_column_stats \
WHERE table_id IN ({table_ids});\n\
INSERT INTO {{METADATA_CATALOG}}.ducklake_table_column_stats
SELECT stats.table_id, stats.column_id, max(stats.null_count > 0), NULL, \
min(stats.min_value), max(stats.max_value), NULL
FROM {{METADATA_CATALOG}}.ducklake_file_column_stats stats
JOIN {{METADATA_CATALOG}}.ducklake_data_file data USING (data_file_id)
WHERE data.end_snapshot IS NULL AND stats.table_id IN ({table_ids})
GROUP BY stats.table_id, stats.column_id;\n"
    ));
    out
}

pub(super) fn bounded_delete_file_mirror_sql(
    delete_file_ids: &[DeleteFileId],
    data_file_ids: &[DataFileId],
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
    snapshots: &[crate::SnapshotRow],
) -> String {
    let delete_ids = delete_file_ids_sql(delete_file_ids);
    let data_ids = data_file_ids_sql(data_file_ids);
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_delete_file \
WHERE delete_file_id IN ({delete_ids}) OR data_file_id IN ({data_ids});\n"
    );
    append_delete_file_mirror_inserts(&mut out, rows, semantic_begin_orders, snapshots);
    out
}

pub(super) fn scheduled_cleanup_delta_sql(
    data_file_ids: &[DataFileId],
    delete_file_ids: &[DeleteFileId],
    data_files: &[ScheduledDataFileCleanupRow],
    delete_files: &[ScheduledDeleteFileCleanupRow],
) -> String {
    let ids = data_file_ids
        .iter()
        .map(|id| id.0)
        .chain(delete_file_ids.iter().map(|id| id.0))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = if ids.is_empty() {
        String::new()
    } else {
        format!(
            "DELETE FROM {{METADATA_CATALOG}}.ducklake_files_scheduled_for_deletion WHERE data_file_id IN ({ids});\n"
        )
    };
    append_scheduled_cleanup_inserts(&mut out, data_files, delete_files);
    out
}

fn append_scheduled_cleanup_inserts(
    out: &mut String,
    data_files: &[ScheduledDataFileCleanupRow],
    delete_files: &[ScheduledDeleteFileCleanupRow],
) {
    for row in data_files {
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_files_scheduled_for_deletion VALUES ({}, {}, false, make_timestamptz({}));\n",
            row.data_file.data_file_id.0,
            sql_string(&row.data_file.path),
            row.schedule_start_micros
        ));
    }
    for row in delete_files {
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_files_scheduled_for_deletion VALUES ({}, {}, false, make_timestamptz({}));\n",
            row.delete_file.delete_file_id.0,
            sql_string(&row.delete_file.path),
            row.schedule_start_micros
        ));
    }
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
