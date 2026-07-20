use crate::{
    CatalogId, CatalogResult, FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv,
    runtime_read_context::{
        CatalogCurrentFileColumnStatsContext, CatalogCurrentFilePartitionValuesContext,
    },
};

use crate::runtime_metadata_ops::*;

#[cfg(not(test))]
pub(super) fn cached_foundationdb_current_metadata_file_column_stats_rows(
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    let context =
        CatalogCurrentFileColumnStatsContext::for_current_file_column_stats(&kv, catalog)?;
    let Some(latest) = context.latest() else {
        return Ok(b"file_column_stats_count=0\n".to_vec());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
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

pub(super) fn current_metadata_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    Ok(
        CatalogCurrentFilePartitionValuesContext::for_current_file_partition_values(kv, catalog)?
            .current_file_partition_values()
            .to_vec(),
    )
}

pub(super) fn current_metadata_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    Ok(
        CatalogCurrentFileColumnStatsContext::for_current_file_column_stats(kv, catalog)?
            .current_file_column_stats()
            .to_vec(),
    )
}

pub(super) fn file_partition_values_payload(
    rows: Vec<FilePartitionValueRow>,
) -> CatalogResult<Vec<u8>> {
    let mut out = format!("file_partition_value_count={}\n", rows.len());
    for row in rows {
        out.push_str(&format!(
            "file_partition_value\t{}\t{}\t{}\t{}\n",
            row.data_file_id.0, row.table_id.0, row.partition_key_index.0, row.partition_value
        ));
    }
    Ok(out.into_bytes())
}

pub(super) fn file_partition_values_mirror_sql(rows: Vec<FilePartitionValueRow>) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_file_partition_value;\n".to_owned();
    append_file_partition_values_mirror_inserts(&mut out, rows);
    out
}

pub(super) fn append_file_partition_values_mirror_inserts(
    out: &mut String,
    rows: Vec<FilePartitionValueRow>,
) {
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

pub(super) fn file_column_stats_payload(rows: Vec<FileColumnStatsRow>) -> CatalogResult<Vec<u8>> {
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

pub(super) fn file_column_stats_mirror_sql(rows: Vec<FileColumnStatsRow>) -> String {
    let mut out = "DELETE FROM {METADATA_CATALOG}.ducklake_file_column_stats;\n".to_owned();
    append_file_column_stats_mirror_inserts(&mut out, rows);
    out
}

pub(super) fn append_file_column_stats_mirror_inserts(
    out: &mut String,
    rows: Vec<FileColumnStatsRow>,
) {
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
