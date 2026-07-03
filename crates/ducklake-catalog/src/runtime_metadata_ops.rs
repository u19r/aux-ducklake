use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogResult, ColumnId, ColumnMappingRow, DataFileId, DataFileRow, DeleteFileId,
    DeleteFileRow, FileColumnStatsRow, FilePartitionValueRow, MetadataSettingRow,
    MetadataSettingScope, NameMappingColumnRow, OrderedCatalogKv, RangeDirection, SnapshotRow,
    TableId,
    keys::{KeyFamily, data_file_key, delete_file_key, delete_file_timeline_prefix, family_prefix},
    list_all_snapshots, list_column_mappings, list_data_files, list_file_column_stats,
    list_metadata_settings, list_snapshots, put_column_mappings,
    runtime_foundationdb::{
        runtime_foundationdb_initialize_ducklake, runtime_foundationdb_metadata_exists,
        runtime_foundationdb_touch_catalog,
    },
    runtime_payload::{
        optional_payload_str_value, optional_payload_string_value, payload_string_value,
        payload_u64_value,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_read_context::{
        CatalogCurrentFileColumnStatsContext, CatalogCurrentFilePartitionValuesContext,
    },
    runtime_snapshots::snapshot_schema_versions_by_order,
    set_metadata_setting,
    table_store::list_table_rows,
};

#[cfg(feature = "foundationdb")]
use crate::runtime_file_listing::{ListDataFilesAtPayload, foundationdb_data_files_at_payload};
#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, FdbOrderedCatalogKv, list_tables_at,
    runtime_catalog_snapshot::conflict_snapshot_payload_for_row,
    runtime_inline_rows::{
        ReadInlineRowsPayload, read_inline_rows_aggregate_stats_payload,
        read_inline_rows_global_stats_payload,
    },
    runtime_snapshot_range::ReadSnapshot,
    runtime_snapshots::snapshot_changes_after_payload,
    store::latest_snapshot_uncached,
};

#[cfg(not(test))]
use crate::latest_snapshot;

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataPayloadCacheKey {
    catalog: CatalogId,
    latest_order: crate::CatalogOrderId,
    operation: MetadataPayloadOperation,
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MetadataPayloadOperation {
    CurrentDataFiles,
    CurrentFileColumnStats,
    DeleteFiles,
}

#[cfg(not(test))]
static METADATA_PAYLOAD_CACHE: OnceLock<BoundedCache<MetadataPayloadCacheKey, Vec<u8>>> =
    OnceLock::new();

#[cfg(not(test))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct VersionedFileColumnStatsPayloadCacheKey {
    catalog: CatalogId,
    version: Option<Vec<u8>>,
}

#[cfg(not(test))]
static FILE_COLUMN_STATS_PAYLOAD_CACHE: OnceLock<
    BoundedCache<VersionedFileColumnStatsPayloadCacheKey, Vec<u8>>,
> = OnceLock::new();

#[cfg(not(test))]
fn metadata_payload_cache() -> &'static BoundedCache<MetadataPayloadCacheKey, Vec<u8>> {
    static_bounded_cache(&METADATA_PAYLOAD_CACHE, 128)
}

#[cfg(not(test))]
fn file_column_stats_payload_cache()
-> &'static BoundedCache<VersionedFileColumnStatsPayloadCacheKey, Vec<u8>> {
    static_bounded_cache(&FILE_COLUMN_STATS_PAYLOAD_CACHE, 64)
}

pub(crate) fn attach_metadata(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let catalog = attach_metadata_catalog_id(backend, catalog, payload)?;
    runtime_foundationdb_touch_catalog(catalog)?;
    Ok(metadata_operation_payload(
        "AttachMetadata",
        backend,
        payload.len(),
        &[
            "metadata_attached=true",
            &format!("runtime_catalog_id={}", catalog.0),
        ],
    ))
}

pub(crate) fn resolve_catalog_id(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let catalog = attach_metadata_catalog_id(backend, catalog, payload)?;
    Ok(metadata_operation_payload(
        "ResolveCatalogId",
        backend,
        payload.len(),
        &[&format!("runtime_catalog_id={}", catalog.0)],
    ))
}

pub(crate) fn metadata_exists(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    {
        runtime_foundationdb_metadata_exists(catalog)?;
    }
    Ok(b"metadata_exists=false\n".to_vec())
}

fn attach_metadata_catalog_id(
    _backend: RuntimeCatalogBackend,
    fallback: CatalogId,
    payload: &[u8],
) -> CatalogResult<CatalogId> {
    if payload.is_empty() {
        return Ok(fallback);
    }
    {
        if let Some(identity) = optional_payload_string_value(payload, "catalog_identity")? {
            return Ok(CatalogId(runtime_catalog_hash([identity.as_str()])));
        }
        let metadata_path = optional_payload_string_value(payload, "metadata_path")?;
        let metadata_database = optional_payload_string_value(payload, "metadata_database")?;
        let metadata_schema = optional_payload_string_value(payload, "metadata_schema")?;
        if metadata_path.is_none() && metadata_database.is_none() && metadata_schema.is_none() {
            return Ok(fallback);
        }
        let metadata_path = metadata_path.unwrap_or_default();
        let metadata_database = metadata_database.unwrap_or_default();
        let metadata_schema = metadata_schema.unwrap_or_default();
        Ok(CatalogId(runtime_catalog_hash([
            metadata_path.as_str(),
            metadata_database.as_str(),
            metadata_schema.as_str(),
        ])))
    }
}

fn runtime_catalog_hash<const N: usize>(fields: [&str; N]) -> u64 {
    const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;
    let mut hash = FNV_OFFSET;
    for field in fields {
        for byte in field.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= u64::from(b'\n');
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    if hash < 2 { hash + 2 } else { hash }
}

pub(crate) fn initialize_ducklake(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let order = runtime_foundationdb_initialize_ducklake(catalog)?;
    Ok(format!(
        "runtime_ffi=ok\noperation=InitializeDuckLake\nbackend={}\ncatalog_snapshot_order={order}\n",
        backend.as_str()
    )
    .into_bytes())
}

pub(crate) fn commit_metadata_batch(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    runtime_foundationdb_touch_catalog(catalog)?;
    Ok(format!(
        "runtime_ffi=ok\noperation=CommitMetadataBatch\nbackend={}\nmetadata_batch_committed=true\n",
        backend.as_str()
    )
    .into_bytes())
}

pub(crate) fn set_config_option(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let row = config_option_payload(payload)?;
    {
        let mut kv = open_foundationdb_catalog()?;
        set_metadata_setting(&mut kv, catalog, row)?;
    }
    Ok(format!(
        "runtime_ffi=ok\noperation=SetConfigOption\nbackend={}\nconfig_option_set=true\n",
        backend.as_str()
    )
    .into_bytes())
}

pub(crate) fn list_config_options(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let rows = {
        let kv = open_foundationdb_catalog()?;
        list_metadata_settings(&kv, catalog)?
    };
    Ok(config_options_payload(&rows).into_bytes())
}

pub(crate) fn lookup_begin_snapshot_for_schema_version(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_id = TableId(payload_u64_value(
        payload,
        "table_id",
        "GetBeginSnapshotForSchemaVersion missing table_id",
    )?);
    let schema_version = payload_u64_value(
        payload,
        "schema_version",
        "GetBeginSnapshotForSchemaVersion missing schema_version",
    )?;
    let begin_snapshot = {
        let kv = open_foundationdb_catalog()?;
        begin_snapshot_for_schema_version(&kv, catalog, table_id, schema_version)?
    };
    Ok(format!("begin_snapshot_id={begin_snapshot}\n").into_bytes())
}

fn begin_snapshot_for_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_version: u64,
) -> CatalogResult<crate::DuckLakeSnapshotId> {
    let snapshots = list_all_snapshots(kv, catalog)?;
    let table_rows = list_table_rows(kv, catalog)?;
    let schema_versions = snapshot_schema_versions_by_order(kv, catalog)?;
    for snapshot in &snapshots {
        if schema_versions.get(&snapshot.order).copied() != Some(schema_version) {
            continue;
        }
        let Some(table) = table_rows
            .iter()
            .find(|row| row.table_id == table_id && row.validity.is_visible_at(snapshot.order))
        else {
            continue;
        };
        if table.validity.begin_order <= snapshot.order {
            return Ok(crate::DuckLakeSnapshotId(snapshot.sequence.0));
        }
    }
    for row in &table_rows {
        if row.table_id == table_id
            && row
                .inlined_data_tables
                .iter()
                .any(|inlined| inlined.schema_version == schema_version)
        {
            let Some(snapshot_id) =
                snapshot_sequence_for_order(&snapshots, row.validity.begin_order)
            else {
                return Err(crate::CatalogError::Decode(format!(
                    "no snapshot contains inline schema version {schema_version} for table {}",
                    table_id.0
                )));
            };
            return Ok(crate::DuckLakeSnapshotId(snapshot_id));
        }
    }
    Err(crate::CatalogError::NotFound("table schema version"))
}

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
fn cached_foundationdb_file_column_stats_rows(catalog: CatalogId) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let key = VersionedFileColumnStatsPayloadCacheKey {
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
        return Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ));
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
        return Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ));
    }

    #[cfg(feature = "foundationdb")]
    {
        let snapshot_id = payload_u64_value(
            payload,
            "snapshot_id",
            "ListGlobalStatsForSnapshot missing snapshot_id",
        )?;
        let requested_table_id = optional_payload_u64(payload, "table_id")?.map(TableId);
        let exact_inline_stats =
            optional_payload_str_value(payload, "inline_stats_mode")? == Some("exact_visible");
        let kv = open_foundationdb_catalog()?;
        global_stats_for_snapshot_payload(
            &kv,
            catalog,
            snapshot_id,
            requested_table_id,
            exact_inline_stats,
        )
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
        return Err(crate::CatalogError::Backend(
            "FoundationDB runtime is not enabled".to_owned(),
        ));
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
                false,
            )?)
            .map_err(|error| {
                crate::CatalogError::Decode(format!("global stats payload is not utf-8: {error}"))
            })?,
        );
        Ok(out.into_bytes())
    }
}

#[cfg(feature = "foundationdb")]
fn global_stats_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    requested_table_id: Option<TableId>,
    exact_inline_stats: bool,
) -> CatalogResult<Vec<u8>> {
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = list_tables_at(kv, catalog, snapshot.order)?
        .into_iter()
        .filter(|table| {
            requested_table_id
                .map(|table_id| table.table_id == table_id)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let mut stats_by_table = tables
        .iter()
        .map(|table| (table.table_id, GlobalTableStats::new(table)))
        .collect::<BTreeMap<_, _>>();
    let mut data_file_ids = BTreeSet::new();

    for table in &tables {
        let table_files = foundationdb_data_files_at_payload(
            kv,
            catalog,
            ListDataFilesAtPayload {
                snapshot_id: resolved_snapshot_id,
                table_id: table.table_id,
            },
        )?;
        for row in global_stats_file_rows_from_payload(&table_files)? {
            data_file_ids.insert(row.data_file_id);
            if let Some(stats) = stats_by_table.get_mut(&row.table_id) {
                stats.accumulate_file(&row);
            }
        }

        for inlined_table in &table.inlined_data_tables {
            let payload = ReadInlineRowsPayload {
                table_name: inlined_table.table_name.clone(),
                snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(resolved_snapshot_id))),
                include_flushed: false,
                include_deleted: false,
            };
            let inline_stats = if exact_inline_stats {
                read_inline_rows_aggregate_stats_payload(kv, catalog, payload)?
            } else {
                read_inline_rows_global_stats_payload(kv, catalog, payload)?
            };
            if let Some(stats) = stats_by_table.get_mut(&table.table_id) {
                stats.accumulate_inline_payload(&inline_stats)?;
            }
        }
    }

    let file_stats = list_file_column_stats_for_data_file_ids(
        kv,
        catalog,
        &data_file_ids.into_iter().collect::<Vec<_>>(),
    )?;
    for row in file_stats {
        if let Some(stats) = stats_by_table.get_mut(&row.table_id) {
            stats.accumulate_file_column_stats(row);
        }
    }

    let mut out = format!("global_stats_snapshot={snapshot_id}\n");
    for table in &tables {
        if let Some(stats) = stats_by_table.get(&table.table_id) {
            stats.append_to(&mut out)?;
        }
    }
    Ok(out.into_bytes())
}

#[cfg(feature = "foundationdb")]
fn global_stats_inputs_for_snapshot_payload(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
    include_inline_stats: bool,
    include_file_column_stats: bool,
    requested_table_id: Option<TableId>,
) -> CatalogResult<Vec<u8>> {
    let snapshot = stats_snapshot_for_request(kv, catalog, snapshot_id)?;
    let resolved_snapshot_id = snapshot.sequence.0;
    let tables = list_tables_at(kv, catalog, snapshot.order)?
        .into_iter()
        .filter(|table| {
            requested_table_id
                .map(|table_id| table.table_id == table_id)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let mut out = format!("global_stats_input_snapshot={snapshot_id}\n");
    let mut data_file_ids = BTreeSet::new();

    for table in &tables {
        let table_files = foundationdb_data_files_at_payload(
            kv,
            catalog,
            ListDataFilesAtPayload {
                snapshot_id: resolved_snapshot_id,
                table_id: table.table_id,
            },
        )?;
        collect_data_file_ids_from_payload(&table_files, &mut data_file_ids)?;
        out.push_str(std::str::from_utf8(&table_files).map_err(|error| {
            crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
        })?);

        if include_inline_stats {
            for inlined_table in &table.inlined_data_tables {
                let inline_stats = read_inline_rows_global_stats_payload(
                    kv,
                    catalog,
                    ReadInlineRowsPayload {
                        table_name: inlined_table.table_name.clone(),
                        snapshot: Some(ReadSnapshot::new(DuckLakeSnapshotId(resolved_snapshot_id))),
                        include_flushed: false,
                        include_deleted: false,
                    },
                )?;
                append_table_inline_stats(&mut out, table.table_id, &inline_stats)?;
            }
        }
    }

    if include_file_column_stats {
        let file_stats = list_file_column_stats_for_data_file_ids(
            kv,
            catalog,
            &data_file_ids.into_iter().collect::<Vec<_>>(),
        )?;
        out.push_str(
            std::str::from_utf8(&file_column_stats_payload(file_stats)?).map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "global stats file-column payload is not utf-8: {error}"
                ))
            })?,
        );
    }
    Ok(out.into_bytes())
}

fn stats_snapshot_for_request(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: u64,
) -> CatalogResult<SnapshotRow> {
    let Some(latest) = crate::latest_snapshot(kv, catalog)? else {
        return Err(crate::CatalogError::Decode(format!(
            "snapshot {snapshot_id} does not exist"
        )));
    };
    if snapshot_id <= latest.sequence.0 {
        return Ok(latest);
    }
    if let Some(snapshot) = list_all_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence.0 == snapshot_id)
        .max_by_key(|snapshot| snapshot.order)
    {
        return Ok(snapshot);
    }
    Ok(latest)
}

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy)]
struct GlobalStatsFileRow {
    data_file_id: DataFileId,
    table_id: TableId,
    record_count: u64,
    file_size_bytes: u64,
    row_id_start: Option<u64>,
}

#[derive(Clone, Default)]
struct GlobalColumnStats {
    has_contains_null: bool,
    contains_null: bool,
    has_min: bool,
    min_value: String,
    has_max: bool,
    max_value: String,
    has_extra_stats: bool,
    extra_stats: String,
}

#[cfg(feature = "foundationdb")]
struct GlobalTableStats {
    table_id: TableId,
    leaf_column_ids: BTreeSet<ColumnId>,
    file_columns: BTreeMap<DataFileId, BTreeSet<ColumnId>>,
    live_files: BTreeSet<DataFileId>,
    record_count: u64,
    next_row_id: u64,
    table_size_bytes: u64,
    columns: BTreeMap<ColumnId, GlobalColumnStats>,
}

#[cfg(feature = "foundationdb")]
impl GlobalTableStats {
    fn new(table: &crate::TableRow) -> Self {
        let parent_column_ids = table
            .columns
            .iter()
            .filter_map(|column| column.parent_id)
            .collect::<BTreeSet<_>>();
        Self {
            table_id: table.table_id,
            leaf_column_ids: table
                .columns
                .iter()
                .filter(|column| !parent_column_ids.contains(&column.column_id))
                .map(|column| column.column_id)
                .collect(),
            file_columns: BTreeMap::new(),
            live_files: BTreeSet::new(),
            record_count: 0,
            next_row_id: 0,
            table_size_bytes: 0,
            columns: BTreeMap::new(),
        }
    }

    fn accumulate_file(&mut self, row: &GlobalStatsFileRow) {
        self.live_files.insert(row.data_file_id);
        self.record_count = self.record_count.saturating_add(row.record_count);
        if let Some(row_id_start) = row.row_id_start {
            self.next_row_id = self
                .next_row_id
                .max(row_id_start.saturating_add(row.record_count));
        } else {
            self.next_row_id = self.next_row_id.saturating_add(row.record_count);
        }
        self.table_size_bytes = self.table_size_bytes.saturating_add(row.file_size_bytes);
    }

    fn accumulate_file_column_stats(&mut self, row: FileColumnStatsRow) {
        if !self.live_files.contains(&row.data_file_id) {
            return;
        }
        self.file_columns
            .entry(row.data_file_id)
            .or_default()
            .insert(row.column_id);
        self.columns
            .entry(row.column_id)
            .or_default()
            .merge_file(row);
    }

    fn accumulate_inline_payload(&mut self, payload: &[u8]) -> CatalogResult<()> {
        let payload = std::str::from_utf8(payload).map_err(|error| {
            crate::CatalogError::Decode(format!(
                "global inline stats payload is not utf-8: {error}"
            ))
        })?;
        let mut current_inline_record_count = 0;
        for line in payload.lines() {
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["inline_table_stats", record_count, next_row_id] => {
                    current_inline_record_count =
                        parse_global_stats_u64(record_count, "inline record count")?;
                    self.record_count = self.record_count.saturating_add(parse_global_stats_u64(
                        record_count,
                        "inline record count",
                    )?);
                    self.next_row_id = self
                        .next_row_id
                        .max(parse_global_stats_u64(next_row_id, "inline next row id")?);
                }
                ["inline_aggregate_stats", record_count] => {
                    current_inline_record_count =
                        parse_global_stats_u64(record_count, "inline aggregate record count")?;
                    self.record_count = self
                        .record_count
                        .saturating_add(current_inline_record_count);
                }
                [
                    "inline_column_stats",
                    column_id,
                    has_contains_null,
                    contains_null,
                    has_min,
                    min_value,
                    has_max,
                    max_value,
                ] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline column stats column id",
                    )?);
                    self.columns.entry(column_id).or_default().merge_inline(
                        parse_global_stats_bool(
                            has_contains_null,
                            "inline column stats has_contains_null",
                        )?,
                        parse_global_stats_bool(
                            contains_null,
                            "inline column stats contains_null",
                        )?,
                        parse_global_stats_bool(has_min, "inline column stats has_min")?,
                        min_value,
                        parse_global_stats_bool(has_max, "inline column stats has_max")?,
                        max_value,
                    );
                }
                [
                    "inline_aggregate_column_stats",
                    column_id,
                    non_null_count,
                    has_min,
                    min_value,
                    has_max,
                    max_value,
                ] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline aggregate column stats column id",
                    )?);
                    let non_null_count = parse_global_stats_u64(
                        non_null_count,
                        "inline aggregate column stats non-null count",
                    )?;
                    self.columns.entry(column_id).or_default().merge_inline(
                        true,
                        non_null_count < current_inline_record_count,
                        parse_global_stats_bool(has_min, "inline aggregate column stats has_min")?,
                        min_value,
                        parse_global_stats_bool(has_max, "inline aggregate column stats has_max")?,
                        max_value,
                    );
                }
                ["inline_column_extra_stats", column_id, extra_stats] => {
                    let column_id = ColumnId(parse_global_stats_u64(
                        column_id,
                        "inline column extra stats column id",
                    )?);
                    self.columns
                        .entry(column_id)
                        .or_default()
                        .merge_extra_stats(extra_stats);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn fill_missing_file_columns(&self, columns: &mut BTreeMap<ColumnId, GlobalColumnStats>) {
        let observed_column_ids = columns.keys().copied().collect::<Vec<_>>();
        for file_id in &self.live_files {
            let present_columns = self.file_columns.get(file_id);
            for column_id in &observed_column_ids {
                if self.leaf_column_ids.contains(column_id)
                    && present_columns.is_none_or(|columns| !columns.contains(column_id))
                {
                    columns.entry(*column_id).or_default().mark_contains_null();
                }
            }
        }
    }

    fn append_to(&self, out: &mut String) -> CatalogResult<()> {
        use std::fmt::Write as _;

        writeln!(
            out,
            "global_table_stats\t{}\t{}\t{}\t{}",
            self.table_id.0, self.record_count, self.next_row_id, self.table_size_bytes
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("failed to render global table stats: {error}"))
        })?;

        let mut columns = self.columns.clone();
        self.fill_missing_file_columns(&mut columns);
        for (column_id, stats) in columns {
            if !self.leaf_column_ids.contains(&column_id) {
                continue;
            }
            writeln!(
                out,
                "global_table_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                self.table_id.0,
                column_id.0,
                stats.has_contains_null,
                stats.contains_null,
                stats.has_min,
                stats.min_value,
                stats.has_max,
                stats.max_value,
                stats.has_extra_stats,
                stats.extra_stats
            )
            .map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render global table column stats: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

impl GlobalColumnStats {
    fn merge_file(&mut self, row: FileColumnStatsRow) {
        self.has_contains_null = true;
        if row.null_count > 0 {
            self.contains_null = true;
        }
        if let Some(min_value) = row.min_value
            && (!self.has_min || global_stats_value_less_than(&min_value, &self.min_value))
        {
            self.has_min = true;
            self.min_value = min_value;
        }
        if let Some(max_value) = row.max_value
            && (!self.has_max || global_stats_value_greater_than(&max_value, &self.max_value))
        {
            self.has_max = true;
            self.max_value = max_value;
        }
        if let Some(extra_stats) = row.extra_stats
            && !extra_stats.is_empty()
        {
            self.merge_extra_stats(&extra_stats);
        }
    }

    fn merge_extra_stats(&mut self, extra_stats: &str) {
        if extra_stats.is_empty() {
            return;
        }
        self.has_extra_stats = true;
        self.extra_stats = merge_global_extra_stats(&self.extra_stats, extra_stats);
    }

    fn merge_inline(
        &mut self,
        has_contains_null: bool,
        contains_null: bool,
        has_min: bool,
        min_value: &str,
        has_max: bool,
        max_value: &str,
    ) {
        if has_contains_null {
            self.has_contains_null = true;
            self.contains_null = self.contains_null || contains_null;
        }
        if has_min && (!self.has_min || global_stats_value_less_than(min_value, &self.min_value)) {
            self.has_min = true;
            self.min_value = min_value.to_owned();
        }
        if has_max && (!self.has_max || global_stats_value_greater_than(max_value, &self.max_value))
        {
            self.has_max = true;
            self.max_value = max_value.to_owned();
        }
        if !has_min && !has_max {
            self.has_min = false;
            self.min_value.clear();
            self.has_max = false;
            self.max_value.clear();
        }
    }

    fn mark_contains_null(&mut self) {
        self.has_contains_null = true;
        self.contains_null = true;
    }
}

#[cfg(feature = "foundationdb")]
fn global_stats_file_rows_from_payload(payload: &[u8]) -> CatalogResult<Vec<GlobalStatsFileRow>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
    })?;
    let mut rows = Vec::new();
    for line in payload.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.first().copied() != Some("file") {
            continue;
        }
        let data_file_id = fields
            .get(1)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "data file id"))
            .map(DataFileId)?;
        let table_id = fields
            .get(2)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file table id"))
            .map(TableId)?;
        let record_count = fields
            .get(4)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file record count"))?;
        let file_size_bytes = fields
            .get(5)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))
            .and_then(|value| parse_global_stats_u64(value, "file size bytes"))?;
        let row_id_start = fields
            .get(6)
            .filter(|value| !value.is_empty())
            .map(|value| parse_global_stats_u64(value, "row id start"))
            .transpose()?;
        rows.push(GlobalStatsFileRow {
            data_file_id,
            table_id,
            record_count,
            file_size_bytes,
            row_id_start,
        });
    }
    Ok(rows)
}

#[cfg(feature = "foundationdb")]
fn parse_global_stats_u64(value: &str, field_name: &str) -> CatalogResult<u64> {
    value.parse::<u64>().map_err(|error| {
        crate::CatalogError::Decode(format!(
            "invalid global stats {field_name} '{value}': {error}"
        ))
    })
}

#[cfg(feature = "foundationdb")]
fn parse_global_stats_bool(value: &str, field_name: &str) -> CatalogResult<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(crate::CatalogError::Decode(format!(
            "invalid global stats {field_name} '{value}'"
        ))),
    }
}

fn global_stats_value_less_than(left: &str, right: &str) -> bool {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left < right,
        _ => left < right,
    }
}

fn global_stats_value_greater_than(left: &str, right: &str) -> bool {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left > right,
        _ => left > right,
    }
}

fn merge_global_extra_stats(current: &str, incoming: &str) -> String {
    if current.is_empty() {
        return strip_sql_string_quotes(incoming).to_owned();
    }
    if current.contains("\"bbox\"")
        && incoming.contains("\"bbox\"")
        && let (Some(current_geo), Some(incoming_geo)) = (
            GeoExtraStats::parse(current),
            GeoExtraStats::parse(incoming),
        )
    {
        return current_geo.merged(&incoming_geo).to_json();
    }
    strip_sql_string_quotes(incoming).to_owned()
}

fn strip_sql_string_quotes(value: &str) -> &str {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(value)
}

#[derive(Default)]
struct GeoExtraStats {
    xmin: Option<f64>,
    xmax: Option<f64>,
    ymin: Option<f64>,
    ymax: Option<f64>,
    zmin: Option<f64>,
    zmax: Option<f64>,
    mmin: Option<f64>,
    mmax: Option<f64>,
    types: BTreeSet<String>,
}

impl GeoExtraStats {
    fn parse(value: &str) -> Option<Self> {
        let value = strip_sql_string_quotes(value);
        Some(Self {
            xmin: parse_json_number_field(value, "xmin"),
            xmax: parse_json_number_field(value, "xmax"),
            ymin: parse_json_number_field(value, "ymin"),
            ymax: parse_json_number_field(value, "ymax"),
            zmin: parse_json_number_field(value, "zmin"),
            zmax: parse_json_number_field(value, "zmax"),
            mmin: parse_json_number_field(value, "mmin"),
            mmax: parse_json_number_field(value, "mmax"),
            types: parse_json_string_array_field(value, "types")?,
        })
    }

    fn merged(&self, incoming: &Self) -> Self {
        let mut types = self.types.clone();
        types.extend(incoming.types.iter().cloned());
        Self {
            xmin: merge_optional_f64_min(self.xmin, incoming.xmin),
            xmax: merge_optional_f64_max(self.xmax, incoming.xmax),
            ymin: merge_optional_f64_min(self.ymin, incoming.ymin),
            ymax: merge_optional_f64_max(self.ymax, incoming.ymax),
            zmin: merge_optional_f64_min(self.zmin, incoming.zmin),
            zmax: merge_optional_f64_max(self.zmax, incoming.zmax),
            mmin: merge_optional_f64_min(self.mmin, incoming.mmin),
            mmax: merge_optional_f64_max(self.mmax, incoming.mmax),
            types,
        }
    }

    fn to_json(&self) -> String {
        format!(
            "{{\"bbox\": {{\"xmin\": {}, \"xmax\": {}, \"ymin\": {}, \"ymax\": {}, \"zmin\": {}, \"zmax\": {}, \"mmin\": {}, \"mmax\": {}}}, \"types\": [{}]}}",
            json_number_or_null(self.xmin),
            json_number_or_null(self.xmax),
            json_number_or_null(self.ymin),
            json_number_or_null(self.ymax),
            json_number_or_null(self.zmin),
            json_number_or_null(self.zmax),
            json_number_or_null(self.mmin),
            json_number_or_null(self.mmax),
            self.types
                .iter()
                .map(|value| format!("\"{}\"", value.replace('"', "\\\"")))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn parse_json_number_field(value: &str, field: &str) -> Option<f64> {
    let field_marker = format!("\"{field}\"");
    let start = value.find(&field_marker)?;
    let after_colon =
        value[start + field_marker.len()..].find(':')? + start + field_marker.len() + 1;
    let rest = value[after_colon..].trim_start();
    if rest.starts_with("null") {
        return None;
    }
    let end = rest
        .find(|ch: char| !(ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+' | 'e' | 'E')))
        .unwrap_or(rest.len());
    rest[..end].parse::<f64>().ok()
}

fn parse_json_string_array_field(value: &str, field: &str) -> Option<BTreeSet<String>> {
    let field_marker = format!("\"{field}\"");
    let start = value.find(&field_marker)?;
    let after_colon =
        value[start + field_marker.len()..].find(':')? + start + field_marker.len() + 1;
    let array_start = value[after_colon..].find('[')? + after_colon + 1;
    let array_end = value[array_start..].find(']')? + array_start;
    let mut result = BTreeSet::new();
    let mut rest = &value[array_start..array_end];
    while let Some(start_quote) = rest.find('"') {
        rest = &rest[start_quote + 1..];
        let Some(end_quote) = rest.find('"') else {
            return None;
        };
        result.insert(rest[..end_quote].to_owned());
        rest = &rest[end_quote + 1..];
    }
    Some(result)
}

fn merge_optional_f64_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_optional_f64_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn json_number_or_null(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

fn optional_metadata_payload_bool(
    payload: &[u8],
    key: &str,
    default_value: bool,
) -> CatalogResult<bool> {
    match optional_payload_str_value(payload, key)? {
        None => Ok(default_value),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(value) => Err(crate::CatalogError::Decode(format!(
            "ListGlobalStatsInputsForSnapshot payload has invalid {key} {value}"
        ))),
    }
}

fn collect_data_file_ids_from_payload(
    payload: &[u8],
    data_file_ids: &mut BTreeSet<DataFileId>,
) -> CatalogResult<()> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global stats file payload is not utf-8: {error}"))
    })?;
    for line in payload.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.first().copied() != Some("file") {
            continue;
        }
        let data_file_id = fields
            .get(1)
            .ok_or_else(|| crate::CatalogError::Decode(format!("invalid file row: {line}")))?
            .parse::<u64>()
            .map_err(|error| {
                crate::CatalogError::Decode(format!("invalid file id in {line}: {error}"))
            })?;
        data_file_ids.insert(DataFileId(data_file_id));
    }
    Ok(())
}

fn append_table_inline_stats(
    out: &mut String,
    table_id: TableId,
    payload: &[u8],
) -> CatalogResult<()> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("global inline stats payload is not utf-8: {error}"))
    })?;
    for line in payload.lines() {
        let Some(rest) = line.strip_prefix("inline_table_stats\t") else {
            if let Some(rest) = line.strip_prefix("inline_column_stats\t") {
                out.push_str(&format!(
                    "table_inline_column_stats\t{}\t{rest}\n",
                    table_id.0
                ));
            } else if let Some(rest) = line.strip_prefix("inline_aggregate_stats\t") {
                out.push_str(&format!("table_inline_stats\t{}\t{rest}\n", table_id.0));
            } else if let Some(rest) = line.strip_prefix("inline_aggregate_column_stats\t") {
                out.push_str(&format!(
                    "table_inline_column_stats\t{}\t{rest}\n",
                    table_id.0
                ));
            }
            continue;
        };
        out.push_str(&format!("table_inline_stats\t{}\t{rest}\n", table_id.0));
    }
    Ok(())
}

#[cfg(not(test))]
fn cached_foundationdb_current_metadata_file_column_stats_rows(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let context =
        CatalogCurrentFileColumnStatsContext::for_current_file_column_stats(&kv, catalog)?;
    let Some(latest) = context.latest() else {
        return Ok(b"file_column_stats_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::CurrentFileColumnStats,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload);
    }
    let rows = context.current_file_column_stats().to_vec();
    let payload = file_column_stats_payload(rows)?;
    cache.insert(key, payload.clone());
    Ok(payload)
}

fn current_metadata_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    Ok(
        CatalogCurrentFilePartitionValuesContext::for_current_file_partition_values(kv, catalog)?
            .current_file_partition_values()
            .to_vec(),
    )
}

fn current_metadata_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    Ok(
        CatalogCurrentFileColumnStatsContext::for_current_file_column_stats(kv, catalog)?
            .current_file_column_stats()
            .to_vec(),
    )
}

fn file_partition_values_payload(rows: Vec<FilePartitionValueRow>) -> CatalogResult<Vec<u8>> {
    let mut out = format!("file_partition_value_count={}\n", rows.len());
    for row in rows {
        out.push_str(&format!(
            "file_partition_value\t{}\t{}\t{}\t{}\n",
            row.data_file_id.0, row.table_id.0, row.partition_key_index.0, row.partition_value
        ));
    }
    Ok(out.into_bytes())
}

fn file_partition_values_mirror_sql(rows: Vec<FilePartitionValueRow>) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_file_partition_value;\n".to_owned();
    append_file_partition_values_mirror_inserts(&mut out, rows);
    out
}

fn append_file_partition_values_mirror_inserts(out: &mut String, rows: Vec<FilePartitionValueRow>) {
    for row in rows {
        let partition_value = if row.partition_value == "__HIVE_DEFAULT_PARTITION__" {
            "NULL".to_owned()
        } else {
            sql_string(&row.partition_value)
        };
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_file_partition_value VALUES ({}, {}, {}, {});\n",
            row.data_file_id.0, row.table_id.0, row.partition_key_index.0, partition_value
        ));
    }
}

fn file_column_stats_payload(rows: Vec<FileColumnStatsRow>) -> CatalogResult<Vec<u8>> {
    let mut out = format!("file_column_stats_count={}\n", rows.len());
    for row in rows {
        out.push_str(&format!(
            "file_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.data_file_id.0,
            row.table_id.0,
            row.column_id.0,
            row.value_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.null_count,
            row.min_value.unwrap_or_default(),
            row.max_value.unwrap_or_default()
        ));
        if let Some(extra_stats) = row.extra_stats {
            out.push('\t');
            out.push_str(&extra_stats);
        }
        out.push('\n');
    }
    Ok(out.into_bytes())
}

fn file_column_stats_mirror_sql(rows: Vec<FileColumnStatsRow>) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_file_column_stats;\n".to_owned();
    append_file_column_stats_mirror_inserts(&mut out, rows);
    out
}

fn append_file_column_stats_mirror_inserts(out: &mut String, rows: Vec<FileColumnStatsRow>) {
    for row in rows {
        out.push_str(&format!(
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_file_column_stats VALUES ({}, {}, {}, NULL, {}, {}, {}, {}, NULL, {});\n",
            row.data_file_id.0,
            row.table_id.0,
            row.column_id.0,
            optional_u64_sql(row.value_count),
            row.null_count,
            optional_sql_string(row.min_value.as_deref()),
            optional_sql_string(row.max_value.as_deref()),
            optional_sql_string(row.extra_stats.as_deref())
        ));
    }
}

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
fn cached_foundationdb_current_metadata_data_file_rows(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"data_file_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
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

fn current_metadata_data_file_rows(mut rows: Vec<DataFileRow>) -> Vec<DataFileRow> {
    rows.retain(|row| row.validity.end_order.is_none());
    rows
}

fn data_file_rows_payload(rows: &[DataFileRow], snapshots: &[crate::SnapshotRow]) -> String {
    let mut out = format!("data_file_count={}\n", rows.len());
    for row in rows {
        let begin_snapshot = snapshot_sequence_for_order(&snapshots, row.validity.begin_order)
            .map(|value| value.to_string())
            .unwrap_or_default();
        let end_snapshot =
            snapshot_sequence_for_optional_end_order(&snapshots, row.validity.end_order);
        let partial_max = row
            .max_partial_order
            .and_then(|order| snapshot_sequence_for_order(&snapshots, order))
            .map(|value| value.to_string())
            .unwrap_or_default();
        let footer_size = row
            .footer_size
            .map(|value| value.to_string())
            .unwrap_or_default();
        let row_id_start = row
            .row_id_start_known
            .then(|| row.row_id_start.to_string())
            .unwrap_or_default();
        out.push_str(&format!(
            "data_file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
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
            footer_size
        ));
    }
    out
}

fn data_file_mirror_sql(rows: &[DataFileRow], snapshots: &[crate::SnapshotRow]) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_data_file;\n".to_owned();
    append_data_file_mirror_inserts(&mut out, rows, snapshots);
    out
}

fn append_data_file_mirror_inserts(
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
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_data_file VALUES ({}, {}, {}, {}, {}, {}, false, 'parquet', {}, {}, {}, {}, {}, NULL, {}, {});\n",
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
            mapping_id,
            partial_max
        ));
    }
}

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
fn cached_foundationdb_delete_file_rows(catalog: CatalogId) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let Some(latest) = latest_snapshot(&kv, catalog)? else {
        return Ok(b"delete_file_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
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

fn delete_file_rows_payload(rows: &[DeleteFileRow], snapshots: &[crate::SnapshotRow]) -> String {
    let semantic_begin_orders = semantic_delete_begin_orders_from_rows(rows);
    delete_file_rows_payload_with_semantic_begin(rows, &semantic_begin_orders, snapshots)
}

fn delete_file_rows_payload_with_semantic_begin(
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
            "delete_file\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.delete_file_id.0,
            row.data_file_id.0,
            row.path,
            row.record_count,
            row.file_size_bytes,
            begin_snapshot,
            end_snapshot
        ));
    }
    out
}

fn delete_file_mirror_sql(rows: &[DeleteFileRow], snapshots: &[crate::SnapshotRow]) -> String {
    let semantic_begin_orders = semantic_delete_begin_orders_from_rows(rows);
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_delete_file;\n".to_owned();
    append_delete_file_mirror_inserts(&mut out, rows, &semantic_begin_orders, snapshots);
    out
}

fn append_delete_file_mirror_inserts(
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
            "INSERT INTO {{METADATA_CATALOG}}.ducklake_delete_file VALUES ({}, (SELECT table_id FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id = {}), {}, {}, {}, {}, false, 'parquet', {}, {}, 0, NULL, NULL);\n",
            row.delete_file_id.0,
            row.data_file_id.0,
            begin_snapshot,
            end_snapshot,
            row.data_file_id.0,
            sql_string(&row.path),
            row.record_count,
            row.file_size_bytes
        ));
    }
}

fn bounded_append_mirror_sql(
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

fn bounded_delete_file_mirror_sql(
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

fn scheduled_cleanup_mirror_sql(
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

fn semantic_delete_begin_orders_from_rows(
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

fn semantic_delete_begin_orders_for_rows(
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

fn delete_file_from_timeline_value(
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

fn list_delete_files(
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

fn list_current_data_files_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    let data_file_ids = unique_data_file_ids(data_file_ids);
    let mut rows = kv
        .batch_get(
            &data_file_ids
                .iter()
                .map(|data_file_id| data_file_key(catalog, *data_file_id))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .flatten()
        .map(|value| DataFileRow::decode(&value))
        .filter(|row| {
            row.as_ref()
                .map(|row| row.validity.end_order.is_none())
                .unwrap_or(true)
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.data_file_id.0);
    Ok(rows)
}

fn list_delete_files_for_delete_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_ids: &[DeleteFileId],
) -> CatalogResult<Vec<DeleteFileRow>> {
    let delete_file_ids = unique_delete_file_ids(delete_file_ids);
    let mut rows = kv
        .batch_get(
            &delete_file_ids
                .iter()
                .map(|delete_file_id| delete_file_key(catalog, *delete_file_id))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .flatten()
        .map(|value| DeleteFileRow::decode(&value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.delete_file_id.0);
    Ok(rows)
}

fn list_file_column_stats_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if data_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    let requested_ids = data_file_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut rows = current_metadata_file_column_stats(kv, catalog)?
        .into_iter()
        .filter(|row| requested_ids.contains(&row.data_file_id))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    Ok(rows)
}

fn unique_data_file_ids(data_file_ids: &[DataFileId]) -> Vec<DataFileId> {
    data_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn unique_delete_file_ids(delete_file_ids: &[DeleteFileId]) -> Vec<DeleteFileId> {
    delete_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn data_file_ids_payload_values(payload: &[u8]) -> CatalogResult<Vec<DataFileId>> {
    parse_id_payload(payload, "data_file_id").map(|ids| ids.into_iter().map(DataFileId).collect())
}

fn delete_file_ids_payload_values(payload: &[u8]) -> CatalogResult<Vec<DeleteFileId>> {
    parse_id_payload(payload, "delete_file_id")
        .map(|ids| ids.into_iter().map(DeleteFileId).collect())
}

fn bounded_append_mirror_payload_values(
    payload: &[u8],
) -> CatalogResult<(Vec<DataFileId>, Vec<FilePartitionValueRow>)> {
    let input = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("invalid bounded append mirror payload: {error}"))
    })?;
    let mut data_file_ids = Vec::new();
    let mut partition_rows = Vec::new();
    for line in input.lines().filter(|line| !line.is_empty()) {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["data_file_id", value] => {
                data_file_ids.push(DataFileId(parse_u64(value, "data file id")?));
            }
            [
                "file_partition",
                data_file_id,
                table_id,
                partition_key_index,
                partition_value,
            ] => {
                partition_rows.push(FilePartitionValueRow::new(
                    DataFileId(parse_u64(data_file_id, "partition value data file id")?),
                    TableId(parse_u64(table_id, "partition value table id")?),
                    crate::PartitionKeyIndex(
                        parse_u64(partition_key_index, "partition key index")?
                            .try_into()
                            .map_err(|_| {
                                crate::CatalogError::Decode(format!(
                                    "partition key index is out of range: {partition_key_index}"
                                ))
                            })?,
                    ),
                    *partition_value,
                ));
            }
            _ => {}
        }
    }
    data_file_ids.sort_unstable();
    data_file_ids.dedup();
    Ok((data_file_ids, partition_rows))
}

fn data_file_ids_sql(ids: &[DataFileId]) -> String {
    let mut values = ids.iter().map(|id| id.0).collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    id_sql(values)
}

fn delete_file_ids_sql(ids: &[DeleteFileId]) -> String {
    let mut values = ids.iter().map(|id| id.0).collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    id_sql(values)
}

fn id_sql(values: Vec<u64>) -> String {
    if values.is_empty() {
        return "NULL".to_owned();
    }
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_id_payload(payload: &[u8], row_label: &str) -> CatalogResult<Vec<u64>> {
    let input = std::str::from_utf8(payload)
        .map_err(|error| crate::CatalogError::Decode(format!("invalid id payload: {error}")))?;
    let mut ids = Vec::new();
    for line in input.lines().filter(|line| !line.is_empty()) {
        let Some((label, value)) = line.split_once('\t') else {
            return Err(crate::CatalogError::Decode(format!(
                "invalid id payload row: {line}"
            )));
        };
        if label != row_label {
            return Err(crate::CatalogError::Decode(format!(
                "expected {row_label} row, found {label}"
            )));
        }
        ids.push(value.parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid {row_label} value {value}: {error}"))
        })?);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn snapshot_sequence_for_order(
    snapshots: &[crate::SnapshotRow],
    order: crate::CatalogOrderId,
) -> Option<u64> {
    snapshots
        .iter()
        .find(|snapshot| snapshot.order == order)
        .map(|snapshot| snapshot.sequence.0)
}

fn snapshot_sequence_for_optional_end_order(
    snapshots: &[crate::SnapshotRow],
    order: Option<crate::CatalogOrderId>,
) -> String {
    match order {
        Some(order) => snapshot_sequence_for_order(snapshots, order)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "0".to_owned()),
        None => String::new(),
    }
}

fn metadata_operation_payload(
    operation: &str,
    backend: RuntimeCatalogBackend,
    payload_len: usize,
    fields: &[&str],
) -> Vec<u8> {
    let mut payload = format!(
        "runtime_ffi=ok\noperation={operation}\nbackend={}\npayload_bytes={payload_len}\n",
        backend.as_str()
    );
    for field in fields {
        payload.push_str(field);
        payload.push('\n');
    }
    payload.into_bytes()
}

fn config_option_payload(payload: &[u8]) -> CatalogResult<MetadataSettingRow> {
    let key = payload_string_value(payload, "key", "SetConfigOption missing key")?;
    let value = payload_string_value(payload, "value", "SetConfigOption missing value")?;
    let scope = crate::runtime_payload::payload_str_value(
        payload,
        "scope",
        "SetConfigOption missing scope",
    )?;
    reject_tabular_field(&key, "config option key")?;
    reject_tabular_field(&value, "config option value")?;
    match scope {
        "global" => Ok(MetadataSettingRow::global(key, value)),
        "schema" => Ok(MetadataSettingRow::schema(
            key,
            value,
            payload_u64_value(
                payload,
                "scope_id",
                "SetConfigOption missing schema scope_id",
            )?,
        )),
        "table" => Ok(MetadataSettingRow::table(
            key,
            value,
            payload_u64_value(
                payload,
                "scope_id",
                "SetConfigOption missing table scope_id",
            )?,
        )),
        _ => Err(crate::CatalogError::Decode(format!(
            "SetConfigOption has unsupported scope {scope}"
        ))),
    }
}

fn config_options_payload(rows: &[MetadataSettingRow]) -> String {
    let mut out = format!("config_option_count={}\n", rows.len());
    for row in rows {
        let (scope, scope_id) = match row.scope {
            MetadataSettingScope::Global => ("global", String::new()),
            MetadataSettingScope::Schema(id) => ("schema", id.to_string()),
            MetadataSettingScope::Table(id) => ("table", id.to_string()),
        };
        out.push_str(&format!(
            "config_option\t{}\t{}\t{}\t{}\n",
            row.key, row.value, scope, scope_id
        ));
    }
    out
}

fn column_mapping_payload(payload: &[u8]) -> CatalogResult<Vec<ColumnMappingRow>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("column mapping payload is not utf-8: {error}"))
    })?;
    let mut rows = Vec::new();
    for line in payload.lines() {
        if line.is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["mapping", mapping_id, table_id, mapping_type] => {
                rows.push(ColumnMappingRow::new(
                    parse_u64(mapping_id, "mapping id")?,
                    TableId(parse_u64(table_id, "mapping table id")?),
                    *mapping_type,
                ));
            }
            [
                "mapping_column",
                mapping_id,
                column_id,
                source_name,
                target_field_id,
                parent_column,
                is_partition,
            ] => {
                let mapping_id = parse_u64(mapping_id, "mapping column mapping id")?;
                let Some(row) = rows.iter_mut().find(|row| row.mapping_id == mapping_id) else {
                    return Err(crate::CatalogError::Decode(format!(
                        "mapping column references unknown mapping {mapping_id}"
                    )));
                };
                row.columns.push(NameMappingColumnRow {
                    column_id: ColumnId(parse_u64(column_id, "mapping column id")?),
                    source_name: (*source_name).to_owned(),
                    target_field_id: parse_u64(target_field_id, "mapping target field id")?,
                    parent_column: optional_u64(parent_column, "mapping parent column")?,
                    is_partition: parse_bool(is_partition, "mapping partition flag")?,
                });
            }
            _ => {
                return Err(crate::CatalogError::Decode(format!(
                    "invalid column mapping payload line: {line}"
                )));
            }
        }
    }
    Ok(rows)
}

fn column_mappings_payload(rows: &[ColumnMappingRow]) -> String {
    let mut out = format!("column_mapping_count={}\n", rows.len());
    for row in rows {
        out.push_str(&format!(
            "mapping\t{}\t{}\t{}\n",
            row.mapping_id, row.table_id.0, row.mapping_type
        ));
        for column in &row.columns {
            out.push_str(&format!(
                "mapping_column\t{}\t{}\t{}\t{}\t{}\t{}\n",
                row.mapping_id,
                column.column_id.0,
                column.source_name,
                column.target_field_id,
                column
                    .parent_column
                    .map_or(String::new(), |id| id.to_string()),
                column.is_partition
            ));
        }
    }
    out
}

fn optional_payload_u64(payload: &[u8], key: &str) -> CatalogResult<Option<u64>> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("runtime payload is not utf-8: {error}"))
    })?;
    let prefix = format!("{key}=");
    for line in payload.lines() {
        let Some(value) = line.strip_prefix(&prefix) else {
            continue;
        };
        if value.is_empty() {
            return Ok(None);
        }
        return Ok(Some(parse_u64(value, key)?));
    }
    Ok(None)
}

fn optional_u64(value: &str, field: &str) -> CatalogResult<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_u64(value, field)?))
}

fn optional_u64_sql(value: Option<u64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

fn optional_sql_string(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_string)
}

fn null_if_empty(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_owned()
    } else {
        value.to_owned()
    }
}

fn sql_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            escaped.push('\'');
        }
        escaped.push(ch);
    }
    escaped.push('\'');
    escaped
}

fn parse_bool(value: &str, field: &str) -> CatalogResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(crate::CatalogError::Decode(format!(
            "invalid {field} {value}"
        ))),
    }
}

fn parse_u64(value: &str, field: &str) -> CatalogResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| crate::CatalogError::Decode(format!("invalid {field} {value}: {error}")))
}

fn reject_tabular_field(value: &str, name: &str) -> CatalogResult<()> {
    if value.contains('\t') || value.contains('\n') {
        return Err(crate::CatalogError::Decode(format!(
            "{name} cannot contain tabs or newlines"
        )));
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
fn open_foundationdb_catalog() -> CatalogResult<crate::FdbOrderedCatalogKv> {
    crate::runtime_foundationdb::open_foundationdb_catalog()
}

#[cfg(not(feature = "foundationdb"))]
fn open_foundationdb_catalog() -> CatalogResult<crate::FakeOrderedCatalogKv> {
    Err(crate::CatalogError::Backend(
        "foundationdb feature is not enabled".to_owned(),
    ))
}

#[cfg(test)]
#[path = "runtime_metadata_ops_tests.rs"]
mod runtime_metadata_ops_tests;
