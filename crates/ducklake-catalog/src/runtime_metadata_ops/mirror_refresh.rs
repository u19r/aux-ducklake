use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FileColumnStatsRow, FilePartitionValueRow, OrderedCatalogKv, TableRow,
    file_stats::list_file_column_stats_for_data_file_ids,
    maintenance::{load_scheduled_data_file_cleanup_rows, load_scheduled_delete_file_cleanup_rows},
    runtime_protocol::RuntimeCatalogBackend,
};

pub(crate) fn render_bounded_mutation_mirror_sql(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let request = MutationMirrorRequest::parse(payload)?;
    let kv = open_foundationdb_catalog()?;
    #[cfg(feature = "foundationdb")]
    let kv = kv.with_read_version()?;
    request.fetch_sql(&kv, catalog)
}

#[cfg(test)]
pub(super) fn render_mutation_mirror_sql_for_catalog(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    MutationMirrorRequest::parse(payload)?.fetch_sql(kv, catalog)
}

#[derive(Default)]
struct MutationMirrorRequest {
    append_data_file_ids: BTreeSet<DataFileId>,
    delete_data_file_ids: BTreeSet<DataFileId>,
    cleanup_data_file_ids: BTreeSet<DataFileId>,
    explicit_delete_file_ids: BTreeSet<DeleteFileId>,
    partitions: Vec<crate::FilePartitionValueRow>,
}

enum MirrorFileRole {
    Append = 1,
    Delete = 2,
    Cleanup = 4,
}

impl MutationMirrorRequest {
    fn fetch_sql(self, kv: &impl OrderedCatalogKv, catalog: CatalogId) -> CatalogResult<Vec<u8>> {
        let history = MutationMirrorHistory::fetch(kv, catalog, &self)?;
        let append_ids = self.append_data_file_ids.into_iter().collect::<Vec<_>>();
        let mut out = if append_ids.is_empty() {
            Vec::new()
        } else {
            bounded_append_mirror_sql_for_catalog(kv, catalog, &append_ids, self.partitions)?
        };
        out.extend_from_slice(history.render_delete_sql(kv, catalog)?.as_bytes());
        out.extend_from_slice(history.render_cleanup_sql(kv, catalog)?.as_bytes());
        Ok(out)
    }

    fn parse(payload: &[u8]) -> CatalogResult<Self> {
        let input = std::str::from_utf8(payload).map_err(|error| {
            CatalogError::Decode(format!("invalid mutation mirror payload: {error}"))
        })?;
        let mut request = Self::default();
        for line in input.lines().filter(|line| !line.is_empty()) {
            request.parse_row(&line.split('\t').collect::<Vec<_>>())?;
        }
        if request
            .partitions
            .iter()
            .any(|row| !request.append_data_file_ids.contains(&row.data_file_id))
        {
            return Err(CatalogError::Decode(
                "mirror partition has no append target".to_owned(),
            ));
        }
        Ok(request)
    }

    fn parse_row(&mut self, fields: &[&str]) -> CatalogResult<()> {
        match fields {
            ["file", id, roles] => self.insert_file_roles(id, roles),
            ["delete_file_id", id] => {
                self.explicit_delete_file_ids
                    .insert(DeleteFileId(parse_u64(id, "delete file id")?));
                Ok(())
            }
            ["file_partition", ..] => {
                self.partitions.push(parse_mirror_partition(fields)?);
                Ok(())
            }
            _ => Err(CatalogError::Decode(
                "invalid mutation mirror row".to_owned(),
            )),
        }
    }

    fn insert_file_roles(&mut self, id: &str, roles: &str) -> CatalogResult<()> {
        let id = DataFileId(parse_u64(id, "data file id")?);
        let roles = parse_u64(roles, "mirror file roles")?;
        if !(1..=7).contains(&roles) {
            return Err(CatalogError::Decode("invalid mirror file roles".to_owned()));
        }
        for (role, files) in [
            (MirrorFileRole::Append, &mut self.append_data_file_ids),
            (MirrorFileRole::Delete, &mut self.delete_data_file_ids),
            (MirrorFileRole::Cleanup, &mut self.cleanup_data_file_ids),
        ] {
            if roles & role as u64 != 0 {
                files.insert(id);
            }
        }
        Ok(())
    }
}

fn parse_mirror_partition(fields: &[&str]) -> CatalogResult<crate::FilePartitionValueRow> {
    let ["file_partition", file, table, key, value] = fields else {
        return Err(CatalogError::Decode(
            "invalid mirror partition row".to_owned(),
        ));
    };
    let index = parse_u64(key, "partition key index")?
        .try_into()
        .map_err(|_| CatalogError::Decode("mirror partition key index exceeds u32".to_owned()))?;
    Ok(crate::FilePartitionValueRow::new(
        DataFileId(parse_u64(file, "partition data file id")?),
        crate::TableId(parse_u64(table, "partition table id")?),
        crate::PartitionKeyIndex(index),
        *value,
    ))
}

struct MutationMirrorHistory {
    explicit_delete_file_ids: Vec<DeleteFileId>,
    delete_data_file_ids: Vec<DataFileId>,
    cleanup_data_file_ids: Vec<DataFileId>,
    rows: Vec<DeleteFileRow>,
}

impl MutationMirrorHistory {
    fn fetch(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        request: &MutationMirrorRequest,
    ) -> CatalogResult<Self> {
        let explicit_delete_file_ids = request
            .explicit_delete_file_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut delete_data_file_ids = request
            .delete_data_file_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        if !explicit_delete_file_ids.is_empty() {
            let explicit =
                list_delete_files_for_delete_file_ids(kv, catalog, &explicit_delete_file_ids)?;
            delete_data_file_ids.extend(explicit.iter().map(|row| row.data_file_id));
        }
        let delete_data_file_ids = unique_data_file_ids(&delete_data_file_ids);
        let cleanup_data_file_ids = request
            .cleanup_data_file_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let history_ids = delete_data_file_ids
            .iter()
            .chain(&cleanup_data_file_ids)
            .copied()
            .collect::<Vec<_>>();
        let rows = list_delete_files_for_data_file_ids(kv, catalog, &history_ids)?;
        Ok(Self {
            explicit_delete_file_ids,
            delete_data_file_ids,
            cleanup_data_file_ids,
            rows,
        })
    }

    fn render_delete_sql(
        &self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<String> {
        if self.delete_data_file_ids.is_empty() && self.explicit_delete_file_ids.is_empty() {
            return Ok(String::new());
        }
        let rows = self
            .rows
            .iter()
            .filter(|row| {
                self.delete_data_file_ids
                    .binary_search(&row.data_file_id)
                    .is_ok()
            })
            .cloned()
            .collect::<Vec<_>>();
        let delete_file_ids = unique_delete_file_ids(
            &self
                .explicit_delete_file_ids
                .iter()
                .copied()
                .chain(rows.iter().map(|row| row.delete_file_id))
                .collect::<Vec<_>>(),
        );
        let semantic_begin_orders = semantic_delete_begin_orders_from_rows(&rows);
        let snapshots = list_snapshots_for_orders(
            kv,
            catalog,
            snapshot_orders_for_delete_files(&rows, &semantic_begin_orders),
        )?;
        Ok(bounded_delete_file_mirror_sql(
            &delete_file_ids,
            &self.delete_data_file_ids,
            &rows,
            &semantic_begin_orders,
            &snapshots,
        ))
    }

    fn render_cleanup_sql(
        &self,
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
    ) -> CatalogResult<String> {
        if self.cleanup_data_file_ids.is_empty() {
            return Ok(String::new());
        }
        let delete_file_ids = self
            .rows
            .iter()
            .filter(|row| {
                self.cleanup_data_file_ids
                    .binary_search(&row.data_file_id)
                    .is_ok()
            })
            .map(|row| row.delete_file_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let data_files =
            load_scheduled_data_file_cleanup_rows(kv, catalog, &self.cleanup_data_file_ids)?;
        let delete_files = load_scheduled_delete_file_cleanup_rows(kv, catalog, &delete_file_ids)?;
        Ok(scheduled_cleanup_delta_sql(
            &self.cleanup_data_file_ids,
            &delete_file_ids,
            &data_files,
            &delete_files,
        ))
    }
}

pub(super) fn bounded_append_mirror_sql_for_catalog(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
    mut partition_rows: Vec<crate::FilePartitionValueRow>,
) -> CatalogResult<Vec<u8>> {
    let changed_data_files = list_data_files_for_data_file_ids(kv, catalog, data_file_ids)?;
    let affected_table_ids = changed_data_files
        .iter()
        .map(|row| row.table_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let data_files = changed_data_files
        .into_iter()
        .filter(|row| row.validity.end_order.is_none())
        .collect::<Vec<_>>();
    let current_data_file_ids = data_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<Vec<_>>();
    let file_stats = list_file_column_stats_for_data_file_ids(kv, catalog, &current_data_file_ids)?;
    let tables = crate::table_store::load_current_table_rows(
        kv,
        catalog,
        &affected_table_ids.iter().copied().collect(),
    )?;
    let table_column_stats = current_table_column_stats(kv, catalog, &tables)?;
    let partition_ids = resolve_mirror_partition_ids(&tables, &data_files, &mut partition_rows);
    let snapshots = list_snapshots_for_orders(kv, catalog, data_file_snapshot_orders(&data_files))?;
    Ok(bounded_append_mirror_sql(
        data_file_ids,
        &affected_table_ids,
        partition_rows,
        AppendMirrorRows {
            data_files: &data_files,
            file_stats: &file_stats,
            table_column_stats: &table_column_stats,
            snapshots: &snapshots,
            partition_ids,
        },
    )
    .into_bytes())
}

fn resolve_mirror_partition_ids(
    tables: &[TableRow],
    data_files: &[DataFileRow],
    partition_rows: &mut [crate::FilePartitionValueRow],
) -> BTreeMap<DataFileId, u64> {
    let table_by_file = data_files
        .iter()
        .map(|file| (file.data_file_id, file.table_id))
        .collect::<BTreeMap<_, _>>();
    let partition_by_table = tables
        .iter()
        .filter_map(|table| {
            table
                .partition
                .as_ref()
                .map(|partition| (table.table_id, partition.partition_id))
        })
        .collect::<BTreeMap<_, _>>();
    partition_rows
        .iter_mut()
        .filter_map(|row| {
            row.table_id = *table_by_file.get(&row.data_file_id)?;
            partition_by_table
                .get(&row.table_id)
                .map(|id| (row.data_file_id, *id))
        })
        .collect()
}

fn current_table_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    tables: &[TableRow],
) -> CatalogResult<Vec<TableColumnStatsMirrorRow>> {
    let mut files = Vec::new();
    for table in tables {
        files.extend(crate::data_file_store::scan_current_data_files(
            kv,
            catalog,
            table.table_id,
        )?);
    }
    let file_ids = files
        .iter()
        .map(|file| file.data_file_id)
        .collect::<Vec<_>>();
    let stats = list_file_column_stats_for_data_file_ids(kv, catalog, &file_ids)?;
    Ok(file_stats::recompute_table_column_stats(
        tables, &files, &stats,
    ))
}

pub(super) struct AppendMirrorRows<'a> {
    pub(super) data_files: &'a [DataFileRow],
    pub(super) file_stats: &'a [FileColumnStatsRow],
    pub(super) table_column_stats: &'a [TableColumnStatsMirrorRow],
    pub(super) snapshots: &'a [crate::SnapshotRow],
    pub(super) partition_ids: BTreeMap<DataFileId, u64>,
}

pub(super) fn bounded_append_mirror_sql(
    data_file_ids: &[DataFileId],
    affected_table_ids: &[crate::TableId],
    partition_rows: Vec<FilePartitionValueRow>,
    rows: AppendMirrorRows<'_>,
) -> String {
    let ids = data_file_ids_sql(data_file_ids);
    let table_ids = table_ids_sql(affected_table_ids);
    let mut out = format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_file_partition_value WHERE data_file_id IN \
         ({ids});\nDELETE FROM {{METADATA_CATALOG}}.ducklake_data_file WHERE data_file_id IN \
         ({ids});\nDELETE FROM {{METADATA_CATALOG}}.ducklake_file_column_stats WHERE data_file_id \
         IN ({ids});\n"
    );
    append_file_partition_values_mirror_inserts(&mut out, partition_rows);
    append_data_file_mirror_inserts(
        &mut out,
        rows.data_files,
        rows.snapshots,
        &rows.partition_ids,
    );
    append_file_column_stats_mirror_inserts(&mut out, rows.file_stats.to_vec());
    out.push_str(&format!(
        "DELETE FROM {{METADATA_CATALOG}}.ducklake_table_column_stats WHERE table_id IN \
         ({table_ids});\n"
    ));
    append_table_column_stats_mirror_inserts(&mut out, rows.table_column_stats);
    out
}

fn append_table_column_stats_mirror_inserts(out: &mut String, rows: &[TableColumnStatsMirrorRow]) {
    let values = rows.iter().map(|row| {
        vec![
            row.table_id.0.to_string(),
            row.column_id.0.to_string(),
            row.contains_null.to_string(),
            "NULL".to_owned(),
            optional_sql_string(row.min_value.as_deref()),
            optional_sql_string(row.max_value.as_deref()),
            "NULL".to_owned(),
        ]
    });
    append_mirror_values(
        out,
        "ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, \
         min_value, max_value, extra_stats)",
        values,
    );
}
