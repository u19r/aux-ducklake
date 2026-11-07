use crate::runtime_protocol::RuntimePayload;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

#[cfg(not(test))]
use crate::latest_snapshot;
use crate::{
    CatalogId, CatalogResult, DataFileId, DeleteFileId, DeleteFileRow, OrderedCatalogKv,
    list_snapshots,
    maintenance::{ScheduledDataFileCleanupRow, ScheduledDeleteFileCleanupRow},
    runtime_metadata_ops::*,
    runtime_protocol::RuntimeCatalogBackend,
};

pub(crate) fn list_delete_file_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
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
    Ok(delete_file_rows_payload(&rows, &snapshots)
        .into_bytes()
        .into())
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

pub(super) fn snapshot_orders_for_delete_files(
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
pub(super) fn cached_foundationdb_delete_file_rows(
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"delete_file_count=0\n".to_vec().into());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::DeleteFiles,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload.into());
    }
    let rows = list_delete_files(&kv, catalog)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    let payload = delete_file_rows_payload(&rows, &snapshots).into_bytes();
    let payload = std::sync::Arc::new(payload);
    cache.insert(key, payload.clone());
    Ok(payload.into())
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
        write!(
            out,
            "delete_file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.delete_file_id.0,
            row.data_file_id.0,
            row.path,
            row.record_count,
            row.file_size_bytes,
            begin_snapshot,
            end_snapshot,
            row.encryption_key
        )
        .expect("writing to String cannot fail");
    }
    out
}

pub(super) fn append_delete_file_mirror_inserts(
    out: &mut String,
    rows: &[DeleteFileRow],
    semantic_begin_orders: &BTreeMap<DataFileId, crate::CatalogOrderId>,
    snapshots: &[crate::SnapshotRow],
) {
    let values = rows.iter().map(|row| {
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
        vec![
            row.delete_file_id.0.to_string(),
            "NULL".to_owned(),
            begin_snapshot,
            end_snapshot,
            row.data_file_id.0.to_string(),
            sql_string(&row.path),
            "false".to_owned(),
            "'parquet'".to_owned(),
            row.record_count.to_string(),
            row.file_size_bytes.to_string(),
            "0".to_owned(),
            optional_encryption_key_sql(&row.encryption_key),
            "NULL".to_owned(),
            "NULL".to_owned(),
        ]
    });
    let Some(select) = render_mirror_select(values) else {
        return;
    };
    write!(
        out,
        "INSERT INTO {{METADATA_CATALOG}}.ducklake_delete_file SELECT delta.* REPLACE \
         (file.table_id AS table_id) FROM ({select}) AS delta(delete_file_id, table_id, \
         begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, \
         delete_count, file_size_bytes, footer_size, encryption_key, partial_max, \
         row_group_count) LEFT JOIN {{METADATA_CATALOG}}.ducklake_data_file file USING \
         (data_file_id);\n"
    )
    .expect("writing to String cannot fail");
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
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_delete_file WHERE delete_file_id IN \
         ({delete_ids}) OR data_file_id IN ({data_ids});\n"
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
            "DELETE FROM {{METADATA_CATALOG}}.ducklake_files_scheduled_for_deletion WHERE \
             data_file_id IN ({ids});\n"
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
    let data_values = data_files.iter().map(|row| {
        vec![
            row.data_file.data_file_id.0.to_string(),
            sql_string(&row.data_file.path),
            "false".to_owned(),
            format!("make_timestamptz({})", row.schedule_start_micros),
        ]
    });
    let delete_values = delete_files.iter().map(|row| {
        vec![
            row.delete_file.delete_file_id.0.to_string(),
            sql_string(&row.delete_file.path),
            "false".to_owned(),
            format!("make_timestamptz({})", row.schedule_start_micros),
        ]
    });
    append_mirror_values(
        out,
        "ducklake_files_scheduled_for_deletion",
        data_values.chain(delete_values),
    );
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
    let data_file_ids = rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>();
    let history = list_delete_files_for_data_file_ids(kv, catalog, &data_file_ids)?;
    Ok(semantic_delete_begin_orders_from_rows(&history))
}
