use std::collections::BTreeSet;

use crate::{
    CatalogId, CatalogResult, list_column_mappings, list_file_column_stats, list_snapshots,
    put_column_mappings, runtime_protocol::RuntimeCatalogBackend,
};

#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, TableId, runtime_catalog_snapshot::conflict_snapshot_payload_for_row,
    runtime_payload::payload_u64_value, runtime_snapshots::snapshot_changes_after_payload,
    store::latest_snapshot_uncached,
};

use crate::runtime_metadata_ops::*;

pub(crate) fn commit_column_mappings(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let rows = column_mapping_payload(payload)?;
    let count = rows.len();
    {
        let mut kv = open_foundationdb_catalog()?;
        put_column_mappings(&mut kv, catalog, rows)?;
    }
    Ok(format!(
        "runtime_ffi=ok\noperation=CommitColumnMappings\nbackend={}\ncolumn_mapping_count={count}\n",
        backend.as_str()
    )
    .into_bytes())
}

pub(crate) fn list_column_mapping_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let start_from = optional_payload_u64(payload, "start_from")?;
    let rows = {
        let kv = open_foundationdb_catalog()?;
        list_column_mappings(&kv, catalog, start_from)?
    };
    Ok(column_mappings_payload(&rows).into_bytes())
}

pub(crate) fn list_file_partition_value_rows_for_data_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let data_file_ids = data_file_ids_payload_values(payload)?;
    let rows = {
        let kv = open_foundationdb_catalog()?;
        let data_file_ids = data_file_ids.into_iter().collect::<BTreeSet<_>>();
        crate::file_partitions::list_file_partition_values_for_data_files(
            &kv,
            catalog,
            &data_file_ids,
        )?
    };
    file_partition_values_payload(rows)
}

pub(crate) fn list_current_metadata_file_partition_value_rows(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let rows = {
        let kv = open_foundationdb_catalog()?;
        current_metadata_file_partition_values(&kv, catalog)?
    };
    file_partition_values_payload(rows)
}

pub(crate) fn render_current_metadata_file_partition_value_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let rows = current_metadata_file_partition_values(&kv, catalog)?;
    Ok(file_partition_values_mirror_sql(rows).into_bytes())
}

pub(crate) fn list_file_column_stats_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    #[cfg(test)]
    let _ = backend;
    #[cfg(not(test))]
    if backend == RuntimeCatalogBackend::FoundationDb {
        return cached_foundationdb_file_column_stats_rows(catalog);
    }
    let rows = {
        let kv = open_foundationdb_catalog()?;
        list_file_column_stats(&kv, catalog)?
    };
    file_column_stats_payload(rows)
}

pub(crate) fn render_current_metadata_file_column_stats_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let rows = current_metadata_file_column_stats(&kv, catalog)?;
    Ok(file_column_stats_mirror_sql(rows).into_bytes())
}

pub(crate) fn render_bounded_append_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (data_file_ids, partition_rows) = bounded_append_mirror_payload_values(payload)?;
    let kv = open_foundationdb_catalog()?;
    let data_files = list_current_data_files_for_data_file_ids(&kv, catalog, &data_file_ids)?;
    let file_stats = list_file_column_stats_for_data_file_ids(&kv, catalog, &data_file_ids)?;
    let snapshots = list_snapshots(&kv, catalog)?;
    Ok(bounded_append_mirror_sql(
        &data_file_ids,
        partition_rows,
        &data_files,
        &file_stats,
        &snapshots,
    )
    .into_bytes())
}

#[cfg(not(test))]
pub(super) fn cached_foundationdb_file_column_stats_rows(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let key = VersionedFileColumnStatsPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        version: crate::OrderedCatalogKv::get(
            &kv,
            &crate::keys::catalog_file_stats_version_key(catalog),
        )?,
    };
    let cache = file_column_stats_payload_cache();
    if let Some(payload) = cache.get_ref(&key) {
        return Ok(payload);
    }
    let payload = file_column_stats_payload(list_file_column_stats(&kv, catalog)?)?;
    cache.insert(key, payload.clone());
    Ok(payload)
}

pub(crate) fn list_current_metadata_file_column_stats_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    #[cfg(test)]
    let _ = backend;
    #[cfg(not(test))]
    if backend == RuntimeCatalogBackend::FoundationDb {
        return cached_foundationdb_current_metadata_file_column_stats_rows(catalog);
    }
    let rows = {
        let kv = open_foundationdb_catalog()?;
        current_metadata_file_column_stats(&kv, catalog)?
    };
    file_column_stats_payload(rows)
}

pub(crate) fn list_current_metadata_file_column_stats_rows_for_data_file_ids(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let data_file_ids = data_file_ids_payload_values(payload)?;
    let rows = {
        let kv = open_foundationdb_catalog()?;
        list_file_column_stats_for_data_file_ids(&kv, catalog, &data_file_ids)?
    };
    file_column_stats_payload(rows)
}

pub(crate) fn list_global_stats_inputs_for_snapshot(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(feature = "foundationdb"))]
    {
        let _ = catalog;
        let _ = payload;
        Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ))
    }

    #[cfg(feature = "foundationdb")]
    {
        let snapshot_id = payload_u64_value(
            payload,
            "snapshot_id",
            "ListGlobalStatsInputsForSnapshot missing snapshot_id",
        )?;
        let include_inline_stats =
            optional_metadata_payload_bool(payload, "include_inline_stats", true)?;
        let include_file_column_stats =
            optional_metadata_payload_bool(payload, "include_file_column_stats", true)?;
        let requested_table_id = optional_payload_u64(payload, "table_id")?.map(TableId);
        let kv = open_foundationdb_catalog()?;
        global_stats_inputs_for_snapshot_payload(
            &kv,
            catalog,
            snapshot_id,
            include_inline_stats,
            include_file_column_stats,
            requested_table_id,
        )
    }
}

pub(crate) fn list_global_stats_for_snapshot(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(feature = "foundationdb"))]
    {
        let _ = catalog;
        let _ = payload;
        Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ))
    }

    #[cfg(feature = "foundationdb")]
    {
        let snapshot_id = payload_u64_value(
            payload,
            "snapshot_id",
            "ListGlobalStatsForSnapshot missing snapshot_id",
        )?;
        let requested_table_id = optional_payload_u64(payload, "table_id")?.map(TableId);
        let kv = open_foundationdb_catalog()?;
        global_stats_for_snapshot_payload(&kv, catalog, snapshot_id, requested_table_id)
    }
}

pub(crate) fn list_snapshot_stats_and_changes_inputs(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(feature = "foundationdb"))]
    {
        let _ = catalog;
        let _ = payload;
        Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ))
    }

    #[cfg(feature = "foundationdb")]
    {
        let base_snapshot_id = payload_u64_value(
            payload,
            "snapshot_id",
            "ListSnapshotStatsAndChangesInputs missing snapshot_id",
        )?;
        let kv = open_foundationdb_catalog()?;
        let Some(latest) = latest_snapshot_uncached(&kv, catalog).row? else {
            return Ok(b"catalog_snapshot_exists=false\n".to_vec());
        };
        let latest_snapshot_id = latest.sequence.0;
        let mut out = String::from_utf8(conflict_snapshot_payload_for_row(&kv, catalog, latest)?)
            .map_err(|error| {
            crate::CatalogError::Decode(format!("conflict snapshot payload is not utf-8: {error}"))
        })?;
        out.push_str(
            std::str::from_utf8(&snapshot_changes_after_payload(
                &kv,
                catalog,
                DuckLakeSnapshotId(base_snapshot_id),
            )?)
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "snapshot changes payload is not utf-8: {error}"
                ))
            })?,
        );
        out.push_str(
            std::str::from_utf8(&global_stats_for_snapshot_payload(
                &kv,
                catalog,
                latest_snapshot_id,
                None,
            )?)
            .map_err(|error| {
                crate::CatalogError::Decode(format!("global stats payload is not utf-8: {error}"))
            })?,
        );
        Ok(out.into_bytes())
    }
}
