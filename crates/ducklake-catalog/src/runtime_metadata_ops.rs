#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogResult, MetadataSettingRow, OrderedCatalogKv, TableId, list_all_snapshots,
    list_metadata_settings,
    runtime_foundationdb::{
        runtime_foundationdb_initialize_ducklake, runtime_foundationdb_metadata_exists,
        runtime_foundationdb_touch_catalog,
    },
    runtime_payload::{optional_payload_string_value, payload_string_value, payload_u64_value},
    runtime_protocol::RuntimeCatalogBackend,
    runtime_snapshots::snapshot_schema_versions_by_order,
    set_metadata_setting,
    table_store::list_table_rows,
};

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataPayloadCacheKey {
    namespace: CatalogCacheNamespace,
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
    namespace: CatalogCacheNamespace,
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
    let exists = runtime_foundationdb_metadata_exists(catalog)?;
    Ok(format!("metadata_exists={exists}\n").into_bytes())
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
        // DuckLake fills an omitted metadata schema from the attached catalog's
        // default after AttachMetadata. Treat the aux_catalog default as `main`
        // so that the manager replacement during initialization keeps the same
        // FoundationDB catalog identity.
        let metadata_schema = metadata_schema
            .filter(|schema| !schema.is_empty())
            .unwrap_or_else(|| "main".to_owned());
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
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let metadata = initialization_metadata_settings(payload)?;
    let order = runtime_foundationdb_initialize_ducklake(catalog, &metadata)?;
    Ok(format!(
        "runtime_ffi=ok\noperation=InitializeDuckLake\nbackend={}\ncatalog_snapshot_order={order}\n",
        backend.as_str()
    )
    .into_bytes())
}

fn initialization_metadata_settings(payload: &[u8]) -> CatalogResult<Vec<MetadataSettingRow>> {
    let encrypted = payload_string_value(
        payload,
        "encrypted",
        "InitializeDuckLake missing encrypted setting",
    )?;
    if !matches!(encrypted.as_str(), "true" | "false") {
        return Err(crate::CatalogError::Decode(format!(
            "InitializeDuckLake encrypted setting must be true or false, got {encrypted}"
        )));
    }
    Ok(vec![
        MetadataSettingRow::global(
            "version",
            payload_string_value(
                payload,
                "version",
                "InitializeDuckLake missing version setting",
            )?,
        ),
        MetadataSettingRow::global(
            "created_by",
            payload_string_value(
                payload,
                "created_by",
                "InitializeDuckLake missing created_by setting",
            )?,
        ),
        MetadataSettingRow::global(
            "data_path",
            payload_string_value(
                payload,
                "data_path",
                "InitializeDuckLake missing data_path setting",
            )?,
        ),
        MetadataSettingRow::global("encrypted", encrypted),
    ])
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

mod current_stats;
mod data_files;
mod delete_files;
mod file_stats;
mod global_stats_merge;
mod global_stats_model;
mod global_stats_requests;
mod metadata_queries;
mod payload_parser;

use current_stats::*;
pub(crate) use data_files::*;
pub(crate) use delete_files::*;
pub(crate) use file_stats::{
    commit_column_mappings, list_column_mapping_rows, list_current_metadata_file_column_stats_rows,
    list_current_metadata_file_column_stats_rows_for_data_file_ids,
    list_current_metadata_file_partition_value_rows, list_file_column_stats_rows,
    list_file_partition_value_rows_for_data_file_ids, list_global_stats_for_snapshot,
    list_global_stats_inputs_for_snapshot, list_snapshot_stats_and_changes_inputs,
    render_bounded_append_mirror_sql, render_current_metadata_file_column_stats_mirror_sql,
    render_current_metadata_file_partition_value_mirror_sql,
};
use global_stats_merge::*;
#[cfg(any(test, feature = "foundationdb"))]
use global_stats_model::*;
#[cfg(any(test, feature = "foundationdb"))]
use global_stats_requests::*;
use metadata_queries::*;
use payload_parser::*;

#[cfg(test)]
#[path = "runtime_metadata_ops_tests.rs"]
mod runtime_metadata_ops_tests;
