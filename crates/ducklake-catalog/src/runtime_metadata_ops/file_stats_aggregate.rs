use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

use super::{GlobalStatsFileRow, GlobalTableStats, leaf_column_ids};
use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, CatalogResult,
    OrderedCatalogKv, TableId, TableRow,
    bounded_cache::{BoundedCache, static_bounded_cache},
    conflict_watermarks::decode_table_next_row_id,
    keys::{
        current_table_row_key, file_column_stats_key, table_file_stats_version_key,
        table_next_row_id_key,
    },
};

#[derive(Clone, Default)]
pub(super) struct FileStatsReadStamp {
    pub(super) allocated_next_row_id: u64,
    version: Option<Vec<u8>>,
}

pub(super) struct FileStatsAggregateInput<'a> {
    pub(super) table: &'a TableRow,
    pub(super) files: &'a [GlobalStatsFileRow],
    pub(super) stamp: FileStatsReadStamp,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FileStatsAggregateKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table: TableId,
    stats_version: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FileStatsColumnIdentity {
    id: crate::ColumnId,
    column_type: String,
    parent: Option<crate::ColumnId>,
    initial_default: Option<String>,
    created_with_table: bool,
}

impl FileStatsColumnIdentity {
    fn matches(&self, column: &crate::TableColumnRow) -> bool {
        self.id == column.column_id
            && self.column_type == column.column_type
            && self.parent == column.parent_id
            && self.initial_default == column.initial_default
            && self.created_with_table == column.created_with_table
    }
}

impl From<&crate::TableColumnRow> for FileStatsColumnIdentity {
    fn from(column: &crate::TableColumnRow) -> Self {
        Self {
            id: column.column_id,
            column_type: column.column_type.clone(),
            parent: column.parent_id,
            initial_default: column.initial_default.clone(),
            created_with_table: column.created_with_table,
        }
    }
}

struct FileStatsAggregate {
    columns: Vec<FileStatsColumnIdentity>,
    files: Vec<GlobalStatsFileRow>,
    stats: GlobalTableStats,
    unknown_row_count: u64,
}

impl FileStatsAggregate {
    fn matches(&self, table: &TableRow, files: &[GlobalStatsFileRow]) -> bool {
        self.files == files
            && self.columns.len() == table.columns.len()
            && self
                .columns
                .iter()
                .zip(&table.columns)
                .all(|(cached, column)| cached.matches(column))
    }

    fn stats_for_allocator(&self, allocated_next_row_id: u64) -> GlobalTableStats {
        let mut stats = self.stats.clone();
        stats.include_allocated_rows(allocated_next_row_id, self.unknown_row_count);
        stats
    }
}

static FILE_STATS_AGGREGATES: OnceLock<
    BoundedCache<FileStatsAggregateKey, std::sync::Arc<FileStatsAggregate>>,
> = OnceLock::new();

pub(super) struct StatsTableInputs {
    pub(super) tables: Vec<TableRow>,
    pub(super) stamps: BTreeMap<TableId, FileStatsReadStamp>,
}

impl StatsTableInputs {
    pub(super) fn fetch(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        snapshot_order: CatalogOrderId,
        table_ids: &BTreeSet<TableId>,
    ) -> CatalogResult<Self> {
        if table_ids.is_empty() {
            let tables = crate::list_tables_at(kv, catalog, snapshot_order)?;
            let stamps = fetch_file_stats_read_stamps(kv, catalog, &tables)?;
            return Ok(Self { tables, stamps });
        }
        let ids = table_ids.iter().copied().collect::<Vec<_>>();
        let keys = ids
            .iter()
            .flat_map(|id| {
                [
                    current_table_row_key(catalog, *id),
                    table_next_row_id_key(catalog, *id),
                    table_file_stats_version_key(catalog, *id),
                ]
            })
            .collect::<Vec<_>>();
        let values = kv.batch_get(&keys)?;
        Self::parse_values(catalog, &ids, values)
    }

    fn parse_values(
        catalog: CatalogId,
        ids: &[TableId],
        values: Vec<Option<Vec<u8>>>,
    ) -> CatalogResult<Self> {
        if values.len() != ids.len() * 3 {
            return Err(CatalogError::Backend(
                "statistics table batch has wrong key count".to_owned(),
            ));
        }
        let mut values = values.into_iter();
        let mut tables = Vec::with_capacity(ids.len());
        let mut stamps = BTreeMap::new();
        for id in ids {
            if let Some(value) = values.next().expect("validated triples") {
                tables.push(crate::table_store::decode_current_table_row(*id, &value)?);
            }
            let allocated = values.next().expect("validated triples");
            let version = values.next().expect("validated triples");
            stamps.insert(
                *id,
                FileStatsReadStamp {
                    allocated_next_row_id: decode_table_next_row_id(
                        catalog,
                        *id,
                        allocated.as_deref(),
                    )?,
                    version,
                },
            );
        }
        Ok(Self { tables, stamps })
    }
}

pub(super) fn fetch_file_stats_read_stamps(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    tables: &[TableRow],
) -> CatalogResult<BTreeMap<TableId, FileStatsReadStamp>> {
    let keys = tables
        .iter()
        .flat_map(|table| {
            [
                table_next_row_id_key(catalog, table.table_id),
                table_file_stats_version_key(catalog, table.table_id),
            ]
        })
        .collect::<Vec<_>>();
    let values = kv.batch_get(&keys)?;
    let ids = tables
        .iter()
        .map(|table| table.table_id)
        .collect::<Vec<_>>();
    FileStatsReadStamp::decode_many(catalog, &ids, values)
}

impl FileStatsReadStamp {
    fn decode_many(
        catalog: CatalogId,
        ids: &[TableId],
        values: Vec<Option<Vec<u8>>>,
    ) -> CatalogResult<BTreeMap<TableId, Self>> {
        if values.len() != ids.len() * 2 {
            return Err(CatalogError::Backend(
                "table statistics stamp batch has wrong key count".to_owned(),
            ));
        }
        let mut values = values.into_iter();
        ids.iter()
            .map(|id| {
                let allocated = values.next().expect("validated pairs");
                let version = values.next().expect("validated pairs");
                Ok((
                    *id,
                    Self {
                        allocated_next_row_id: decode_table_next_row_id(
                            catalog,
                            *id,
                            allocated.as_deref(),
                        )?,
                        version,
                    },
                ))
            })
            .collect()
    }
}

pub(super) fn fetch_file_stats_aggregate(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    input: FileStatsAggregateInput<'_>,
) -> CatalogResult<GlobalTableStats> {
    let key = FileStatsAggregateKey {
        namespace: kv.catalog_cache_namespace().without_read_context(),
        catalog,
        table: input.table.table_id,
        stats_version: input.stamp.version,
    };
    let cache = static_bounded_cache(&FILE_STATS_AGGREGATES, 1024);
    if let Some(stats) = cache.get_ref(&key)
        && stats.matches(input.table, input.files)
    {
        crate::runtime_metrics::record_runtime_measurement("stats.file_aggregate.hit", 1, 0);
        return Ok(stats.stats_for_allocator(input.stamp.allocated_next_row_id));
    }
    let aggregate = std::sync::Arc::new(fetch_file_stats_reduction(
        kv,
        catalog,
        input.table,
        input.files,
    )?);
    let stats = aggregate.stats_for_allocator(input.stamp.allocated_next_row_id);
    cache.insert(key, aggregate);
    Ok(stats)
}

fn fetch_file_stats_reduction(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table: &TableRow,
    files: &[GlobalStatsFileRow],
) -> CatalogResult<FileStatsAggregate> {
    let mut stats = GlobalTableStats::new(table, 0);
    for file in files {
        stats.accumulate_file(file);
    }
    let columns = leaf_column_ids(table);
    let file_ids = files
        .iter()
        .map(|file| file.data_file_id)
        .collect::<Vec<_>>();
    visit_aggregate_source_rows(kv, catalog, (&file_ids, &columns), |row| {
        if row.table_id != table.table_id {
            return Err(CatalogError::Decode(
                "file statistics belong to another table".to_owned(),
            ));
        }
        stats.accumulate_file_column_stats_ref(row);
        Ok(())
    })?;
    stats.finish_file_coverage();
    Ok(FileStatsAggregate {
        columns: table
            .columns
            .iter()
            .map(FileStatsColumnIdentity::from)
            .collect(),
        files: files.to_vec(),
        stats,
        unknown_row_count: files
            .iter()
            .filter(|file| file.row_id_start.is_none())
            .fold(0_u64, |count, file| count.saturating_add(file.record_count)),
    })
}

#[cfg(test)]
pub(super) fn fetch_aggregate_source_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    files: &[crate::DataFileId],
    columns: &BTreeSet<crate::ColumnId>,
) -> CatalogResult<Vec<crate::FileColumnStatsRow>> {
    let mut rows = Vec::new();
    visit_aggregate_source_rows(kv, catalog, (files, columns), |row| {
        rows.push(row.into());
        Ok(())
    })?;
    Ok(rows)
}

fn visit_aggregate_source_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source: (
        &[crate::DataFileId],
        &std::collections::BTreeSet<crate::ColumnId>,
    ),
    visit: impl FnMut(crate::file_stats::FileColumnStatsRef<'_>) -> CatalogResult<()>,
) -> CatalogResult<()> {
    let (files, columns) = source;
    if columns.len() > 1
        && columns.last().unwrap().0 - columns.first().unwrap().0 == (columns.len() - 1) as u64
    {
        return visit_contiguous_aggregate_rows(kv, catalog, (files, columns), visit);
    }
    let identities = files
        .iter()
        .flat_map(|file| columns.iter().map(move |column| (*file, *column)))
        .collect::<Vec<_>>();
    let keys = identities
        .iter()
        .map(|(file, column)| file_column_stats_key(catalog, *file, *column))
        .collect::<Vec<_>>();
    let values = kv.batch_get(&keys)?;
    #[cfg(feature = "runtime-metrics")]
    record_aggregate_source_usage(values.iter().flatten().map(Vec::as_slice));
    visit_aggregate_source_values(identities, values, visit)
}

fn visit_contiguous_aggregate_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source: (&[crate::DataFileId], &BTreeSet<crate::ColumnId>),
    mut visit: impl FnMut(crate::file_stats::FileColumnStatsRef<'_>) -> CatalogResult<()>,
) -> CatalogResult<()> {
    let (files, columns) = source;
    let ranges = files
        .iter()
        .map(|file| crate::kv::CatalogReadRange {
            start: file_column_stats_key(catalog, *file, *columns.first().unwrap()),
            end: crate::keys::prefix_end(&file_column_stats_key(
                catalog,
                *file,
                *columns.last().unwrap(),
            )),
            direction: crate::RangeDirection::Forward,
            limit: columns.len(),
        })
        .collect::<Vec<_>>();
    let reads = kv.read_set(&[], &ranges)?;
    if reads.ranges.len() != files.len() {
        return Err(CatalogError::Backend(
            "file aggregate range batch has wrong file count".to_owned(),
        ));
    }
    #[cfg(feature = "runtime-metrics")]
    record_aggregate_source_usage(
        reads
            .ranges
            .iter()
            .flatten()
            .map(|item| item.value.as_slice()),
    );
    for (file, items) in files.iter().zip(reads.ranges) {
        for item in items {
            visit(decode_aggregate_range_row(catalog, *file, columns, &item)?)?;
        }
    }
    Ok(())
}

fn decode_aggregate_range_row<'a>(
    catalog: CatalogId,
    file: crate::DataFileId,
    columns: &std::collections::BTreeSet<crate::ColumnId>,
    item: &'a crate::RangeItem,
) -> CatalogResult<crate::file_stats::FileColumnStatsRef<'a>> {
    let row = crate::file_stats::FileColumnStatsRef::decode(&item.value)?;
    if row.data_file_id != file
        || !columns.contains(&row.column_id)
        || item.key != file_column_stats_key(catalog, file, row.column_id)
    {
        return Err(CatalogError::Decode(
            "file aggregate source identity does not match its key".to_owned(),
        ));
    }
    Ok(row)
}

#[cfg(feature = "runtime-metrics")]
fn record_aggregate_source_usage<'a>(values: impl Iterator<Item = &'a [u8]>) {
    let (count, bytes) = values.fold((0, 0), |(count, bytes), value| {
        (count + 1, bytes + value.len() as u64)
    });
    crate::runtime_metrics::record_runtime_measurement(
        "stats.file_aggregate.source_rows",
        count,
        bytes,
    );
}

fn visit_aggregate_source_values(
    identities: Vec<(crate::DataFileId, crate::ColumnId)>,
    values: Vec<Option<Vec<u8>>>,
    mut visit: impl FnMut(crate::file_stats::FileColumnStatsRef<'_>) -> CatalogResult<()>,
) -> CatalogResult<()> {
    if values.len() != identities.len() {
        return Err(CatalogError::Backend(
            "file aggregate source batch has wrong key count".to_owned(),
        ));
    }
    for ((file, column), value) in identities.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let row = crate::file_stats::FileColumnStatsRef::decode(&value)?;
        if row.data_file_id != file || row.column_id != column {
            return Err(CatalogError::Decode(
                "file aggregate source identity does not match its key".to_owned(),
            ));
        }
        visit(row)?;
    }
    Ok(())
}
