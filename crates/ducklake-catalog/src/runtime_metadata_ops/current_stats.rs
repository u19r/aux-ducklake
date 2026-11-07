#[cfg(not(test))]
use crate::runtime_protocol::RuntimePayload;
use crate::{
    CatalogId, CatalogResult, FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv,
    runtime_metadata_ops::*, runtime_read_context::CatalogCurrentFileColumnStatsContext,
};
use std::fmt::Write as _;

#[cfg(not(test))]
pub(super) fn cached_foundationdb_current_metadata_file_column_stats_rows(
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    let kv = open_foundationdb_catalog()?;
    let context =
        CatalogCurrentFileColumnStatsContext::for_current_file_column_stats(&kv, catalog)?;
    let Some(latest) = context.latest() else {
        return Ok(b"file_column_stats_count=0\n".to_vec().into());
    };
    let key = MetadataPayloadCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        latest_order: latest.order,
        operation: MetadataPayloadOperation::CurrentFileColumnStats,
    };
    let cache = metadata_payload_cache();
    if let Some(payload) = cache.get(key) {
        return Ok(payload.into());
    }
    let rows = context.current_file_column_stats();
    let payload = file_column_stats_payload(&rows)?;
    let payload = std::sync::Arc::new(payload);
    cache.insert(key, payload.clone());
    Ok(payload.into())
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
        write!(
            out,
            "file_partition_value\t{}\t{}\t{}\t{}\n",
            row.data_file_id.0, row.table_id.0, row.partition_key_index.0, row.partition_value
        )
        .expect("writing to String cannot fail");
    }
    Ok(out.into_bytes())
}

pub(super) fn append_file_partition_values_mirror_inserts(
    out: &mut String,
    rows: Vec<FilePartitionValueRow>,
) {
    let values = rows.into_iter().map(|row| {
        let partition_value = if row.partition_value == "__HIVE_DEFAULT_PARTITION__" {
            "NULL".to_owned()
        } else {
            sql_string(&row.partition_value)
        };
        vec![
            row.data_file_id.0.to_string(),
            row.table_id.0.to_string(),
            row.partition_key_index.0.to_string(),
            partition_value,
        ]
    });
    append_mirror_values(out, "ducklake_file_partition_value", values);
}

pub(super) fn file_column_stats_payload(rows: &[FileColumnStatsRow]) -> CatalogResult<Vec<u8>> {
    let mut out = format!("file_column_stats_count={}\n", rows.len());
    for row in rows {
        write!(
            out,
            "file_column_stats\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.data_file_id.0,
            row.table_id.0,
            row.column_id.0,
            row.value_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.null_count,
            row.min_value.as_deref().unwrap_or_default(),
            row.max_value.as_deref().unwrap_or_default()
        )
        .expect("writing to String cannot fail");
        if let Some(extra_stats) = &row.extra_stats {
            out.push('\t');
            out.push_str(&extra_stats);
        }
        out.push('\n');
    }
    Ok(out.into_bytes())
}

pub(super) fn append_file_column_stats_mirror_inserts(
    out: &mut String,
    rows: Vec<FileColumnStatsRow>,
) {
    let values = rows.into_iter().map(|row| {
        vec![
            row.data_file_id.0.to_string(),
            row.table_id.0.to_string(),
            row.column_id.0.to_string(),
            "NULL".to_owned(),
            optional_u64_sql(row.value_count),
            row.null_count.to_string(),
            optional_sql_string(row.min_value.as_deref()),
            optional_sql_string(row.max_value.as_deref()),
            "NULL".to_owned(),
            optional_sql_string(row.extra_stats.as_deref()),
        ]
    });
    append_mirror_values(
        out,
        "ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, \
         value_count, null_count, min_value, max_value, contains_nan, extra_stats)",
        values,
    );
}

pub(super) fn append_mirror_values(
    out: &mut String,
    table: &'static str,
    values: impl Iterator<Item = Vec<String>>,
) {
    if let Some(select) = render_mirror_select(values) {
        write!(out, "INSERT INTO {{METADATA_CATALOG}}.{table} {select};\n")
            .expect("writing to String cannot fail");
    }
}

pub(super) fn render_mirror_select(
    mut values: impl Iterator<Item = Vec<String>>,
) -> Option<String> {
    let mut columns = values.next()?;
    for row in values {
        assert_eq!(
            row.len(),
            columns.len(),
            "mirror row width must match its table"
        );
        for (column, value) in columns.iter_mut().zip(row) {
            column.push_str(", ");
            column.push_str(&value);
        }
    }
    let mut out = String::from("SELECT ");
    for (index, column) in columns.iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        write!(out, "unnest([{column}])").expect("writing to String cannot fail");
    }
    Some(out)
}
