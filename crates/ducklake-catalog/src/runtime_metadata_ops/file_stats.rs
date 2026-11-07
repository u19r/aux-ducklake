use crate::runtime_protocol::RuntimePayload;
use std::collections::{BTreeMap, BTreeSet};

use crate::typed_scalar::{
    CatalogScalarType, compare_typed_scalars, normalize_catalog_scalar_type,
    typed_extrema_are_invalid,
};
use crate::{
    CatalogId, CatalogResult, ColumnId, DataFileId, DataFileRow, FileColumnStatsRow,
    PartitionKeyIndex, TableId, TableRow, list_column_mappings, list_file_column_stats,
    put_column_mappings, runtime_protocol::RuntimeCatalogBackend,
};

#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, runtime_catalog_snapshot::conflict_snapshot_payload_for_row,
    runtime_payload::payload_u64_value, runtime_snapshots::snapshot_changes_after_payload,
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
    let (data_file_ids, partition_key_indices) = partition_value_request(payload)?;
    let rows = {
        let kv = open_foundationdb_catalog()?;
        crate::file_partitions::list_file_partition_values_for_data_files_and_keys(
            &kv,
            catalog,
            &data_file_ids,
            &partition_key_indices,
        )?
    };
    file_partition_values_payload(rows)
}

pub(super) fn partition_value_request(
    payload: &[u8],
) -> CatalogResult<(BTreeSet<DataFileId>, BTreeSet<PartitionKeyIndex>)> {
    let payload = std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("invalid file partition value request: {error}"))
    })?;
    let mut data_file_ids = BTreeSet::new();
    let mut partition_key_indices = BTreeSet::new();
    for line in payload.lines().filter(|line| !line.is_empty()) {
        let Some((label, value)) = line.split_once('\t') else {
            return Err(crate::CatalogError::Decode(format!(
                "invalid file partition value request row: {line}"
            )));
        };
        match label {
            "data_file_id" => {
                data_file_ids.insert(DataFileId(parse_u64(value, "data file id")?));
            }
            "partition_key_index" => {
                let value = parse_u64(value, "partition key index")?;
                partition_key_indices.insert(PartitionKeyIndex(u32::try_from(value).map_err(
                    |_| {
                        crate::CatalogError::Decode(format!(
                            "partition key index {value} exceeds u32"
                        ))
                    },
                )?));
            }
            _ => {
                return Err(crate::CatalogError::Decode(format!(
                    "unexpected file partition value request row: {line}"
                )));
            }
        }
    }
    Ok((data_file_ids, partition_key_indices))
}

pub(crate) fn render_bounded_partition_info_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_ids = parse_id_payload(payload, "table_id")?
        .into_iter()
        .map(TableId)
        .collect::<Vec<_>>();
    let requested = table_ids.iter().copied().collect::<BTreeSet<_>>();
    let kv = open_foundationdb_catalog()?;
    let tables = crate::table_store::load_current_table_rows(&kv, catalog, &requested)?;
    Ok(bounded_partition_info_mirror_sql(&table_ids, &tables).into_bytes())
}

pub(super) fn bounded_partition_info_mirror_sql(
    table_ids: &[TableId],
    tables: &[crate::TableRow],
) -> String {
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_partition_info WHERE table_id IN ({});\n",
        table_ids_sql(table_ids)
    );
    for table in tables {
        if let Some(partition) = &table.partition {
            out.push_str(&format!(
                "INSERT INTO {{METADATA_CATALOG}}.ducklake_partition_info VALUES ({}, {}, NULL, NULL);\n",
                partition.partition_id, table.table_id.0
            ));
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableColumnStatsMirrorRow {
    pub(crate) table_id: TableId,
    pub(crate) column_id: ColumnId,
    pub(crate) contains_null: bool,
    pub(crate) min_value: Option<String>,
    pub(crate) max_value: Option<String>,
}

pub(crate) fn recompute_table_column_stats(
    tables: &[TableRow],
    current_data_files: &[DataFileRow],
    file_stats: &[FileColumnStatsRow],
) -> Vec<TableColumnStatsMirrorRow> {
    let current_file_ids = current_data_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let column_types = tables
        .iter()
        .flat_map(|table| {
            table.columns.iter().map(|column| {
                (
                    (table.table_id, column.column_id),
                    normalize_catalog_scalar_type(&column.column_type),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let mut accumulators = BTreeMap::<(TableId, ColumnId), MirrorColumnStats>::new();
    for row in file_stats
        .iter()
        .filter(|row| current_file_ids.contains(&row.data_file_id))
    {
        let key = (row.table_id, row.column_id);
        let scalar_type = column_types.get(&key).copied().unwrap_or_default();
        accumulators
            .entry(key)
            .or_insert_with(|| MirrorColumnStats::new(scalar_type))
            .merge(row);
    }
    accumulators
        .into_iter()
        .map(|((table_id, column_id), stats)| TableColumnStatsMirrorRow {
            table_id,
            column_id,
            contains_null: stats.contains_null,
            min_value: if stats.invalid_extrema {
                None
            } else {
                stats.min_value
            },
            max_value: if stats.invalid_extrema {
                None
            } else {
                stats.max_value
            },
        })
        .collect()
}

struct MirrorColumnStats {
    scalar_type: CatalogScalarType,
    contains_null: bool,
    min_value: Option<String>,
    max_value: Option<String>,
    invalid_extrema: bool,
}

impl MirrorColumnStats {
    fn new(scalar_type: CatalogScalarType) -> Self {
        Self {
            scalar_type,
            contains_null: false,
            min_value: None,
            max_value: None,
            invalid_extrema: false,
        }
    }

    fn merge(&mut self, row: &FileColumnStatsRow) {
        self.contains_null |= row.null_count > 0;
        if self.invalid_extrema {
            return;
        }
        if typed_extrema_are_invalid(
            self.scalar_type,
            row.value_count,
            row.null_count,
            row.min_value.as_deref(),
            row.max_value.as_deref(),
        ) {
            self.min_value = None;
            self.max_value = None;
            self.invalid_extrema = true;
            return;
        }
        if let Some(value) = row.min_value.as_ref()
            && self.min_value.as_deref().is_none_or(|current| {
                compare_typed_scalars(self.scalar_type, value, current)
                    .is_some_and(|ordering| ordering.is_lt())
            })
        {
            self.min_value = Some(value.clone());
        }
        if let Some(value) = row.max_value.as_ref()
            && self.max_value.as_deref().is_none_or(|current| {
                compare_typed_scalars(self.scalar_type, value, current)
                    .is_some_and(|ordering| ordering.is_gt())
            })
        {
            self.max_value = Some(value.clone());
        }
    }
}

pub(crate) fn list_file_column_stats_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
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
    file_column_stats_payload(&rows).map(Into::into)
}

#[cfg(not(test))]
pub(super) fn cached_foundationdb_file_column_stats_rows(
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    let kv = open_foundationdb_catalog()?;
    let key = VersionedFileColumnStatsPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        version: crate::catalog_request_read_set::fetch_value(
            &kv,
            catalog,
            &crate::keys::catalog_file_stats_version_key(catalog),
        )?,
    };
    let cache = file_column_stats_payload_cache();
    if let Some(payload) = cache.get_ref(&key) {
        return Ok(payload.into());
    }
    let payload = file_column_stats_payload(&list_file_column_stats(&kv, catalog)?)?;
    let payload = std::sync::Arc::new(payload);
    cache.insert(key, payload.clone());
    Ok(payload.into())
}

pub(crate) fn list_current_metadata_file_column_stats_rows(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
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
    file_column_stats_payload(&rows).map(Into::into)
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
        let requested_table_ids = payload_u64_values(payload, "table_id")?
            .into_iter()
            .map(TableId)
            .collect();
        let kv = open_foundationdb_catalog()?;
        global_stats_inputs_for_snapshot_payload(
            &kv,
            catalog,
            snapshot_id,
            include_inline_stats,
            include_file_column_stats,
            &requested_table_ids,
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
        let requested_table_ids = payload_u64_values(payload, "table_id")?
            .into_iter()
            .map(TableId)
            .collect();
        let kv = open_foundationdb_catalog()?;
        global_stats_for_snapshot_payload(&kv, catalog, snapshot_id, &requested_table_ids)
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
        let read_kv = crate::runtime_file_listing::runtime_read_snapshot_kv(&kv)?;
        let Some(latest) = crate::store::prepared_snapshot_read(
            &read_kv,
            catalog,
            crate::catalog_request_read_set::CatalogHeaderProjection::Conflict,
        )
        .row?
        else {
            return Ok(b"catalog_snapshot_exists=false\n".to_vec());
        };
        let latest_snapshot_id = latest.sequence.0;
        let mut out = String::from_utf8(
            conflict_snapshot_payload_for_row(&read_kv, catalog, latest)?.into_vec(),
        )
        .map_err(|error| {
            crate::CatalogError::Decode(format!("conflict snapshot payload is not utf-8: {error}"))
        })?;
        out.push_str(
            std::str::from_utf8(&snapshot_changes_after_payload(
                &read_kv,
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
                &read_kv,
                catalog,
                latest_snapshot_id,
                &BTreeSet::new(),
            )?)
            .map_err(|error| {
                crate::CatalogError::Decode(format!("global stats payload is not utf-8: {error}"))
            })?,
        );
        Ok(out.into_bytes())
    }
}
