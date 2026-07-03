use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, OnceLock},
};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::{record_runtime_measurement, record_runtime_method_elapsed};
use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, CatalogResult, ColumnId,
    DataFileId, DataFileRow, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection, TableId,
    immutable_file_metadata::immutable_data_file_metadata_batch,
    keys::{
        KeyFamily, catalog_file_stats_version_key, family_prefix,
        file_column_stats_data_file_prefix, file_column_stats_key, file_column_stats_lookup_key,
        file_column_stats_lookup_prefix, prefix_end, table_file_stats_version_key,
    },
    lru_cache::LruCache,
};

const FILE_COLUMN_STATS_ROW_CACHE_CAPACITY: usize = 131_072;
const DENSE_FILE_COLUMN_STATS_SCAN_MIN_FILES: usize = 2;
const DENSE_FILE_COLUMN_STATS_SCAN_MAX_GAP_FACTOR: u64 = 8;
const FULL_FILE_STATS_LOAD_MIN_COLUMNS: usize = 8;
const FULL_FILE_STATS_LOAD_MIN_FILES: usize = 64;

static FILE_COLUMN_STATS_ROW_CACHE: OnceLock<
    Mutex<LruCache<FileColumnStatsCacheKey, FileColumnStatsRow>>,
> = OnceLock::new();
static FILE_COLUMN_STATS_FILE_CACHE: OnceLock<
    Mutex<LruCache<FileColumnStatsFileCacheKey, Vec<FileColumnStatsRow>>>,
> = OnceLock::new();

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_method_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_method_stage(_operation: &str, _started: RuntimeMetricStage) {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileColumnStatsRow {
    pub data_file_id: DataFileId,
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub value_count: Option<u64>,
    pub null_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub extra_stats: Option<String>,
}

impl FileColumnStatsRow {
    const VERSION: u8 = 3;

    #[must_use]
    pub fn new(
        data_file_id: DataFileId,
        table_id: TableId,
        column_id: ColumnId,
        null_count: u64,
        min_value: Option<String>,
        max_value: Option<String>,
    ) -> Self {
        Self {
            data_file_id,
            table_id,
            column_id,
            value_count: None,
            null_count,
            min_value,
            max_value,
            extra_stats: None,
        }
    }

    #[must_use]
    pub fn with_value_count(mut self, value_count: Option<u64>) -> Self {
        self.value_count = value_count;
        self
    }

    #[must_use]
    pub fn with_extra_stats(mut self, extra_stats: Option<String>) -> Self {
        self.extra_stats = extra_stats;
        self
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(Self::VERSION);
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.column_id.0.to_be_bytes());
        match self.value_count {
            Some(value_count) => {
                out.push(1);
                out.extend_from_slice(&value_count.to_be_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0_u64.to_be_bytes());
            }
        }
        out.extend_from_slice(&self.null_count.to_be_bytes());
        encode_optional_string(&mut out, self.min_value.as_deref());
        encode_optional_string(&mut out, self.max_value.as_deref());
        encode_optional_string(&mut out, self.extra_stats.as_deref());
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let version = bytes
            .first()
            .copied()
            .ok_or_else(|| CatalogError::Decode("file column stats row is empty".to_owned()))?;
        let fixed_len = match version {
            1 => 1 + 8 + 8 + 8 + 8,
            2 | 3 => 1 + 8 + 8 + 8 + 9 + 8,
            other => {
                return Err(CatalogError::Decode(format!(
                    "unsupported file column stats row version {other}"
                )));
            }
        };
        if bytes.len() < fixed_len {
            return Err(CatalogError::Decode(format!(
                "file column stats row is too short: {} bytes",
                bytes.len()
            )));
        }
        let data_file_start = 1;
        let table_start = data_file_start + 8;
        let column_start = table_start + 8;
        let value_count_start = column_start + 8;
        let (value_count, null_count_start) = if version >= 2 {
            (
                decode_optional_u64(bytes, value_count_start, "value count")?,
                value_count_start + 9,
            )
        } else {
            (None, value_count_start)
        };
        let values_start = null_count_start + 8;
        let data_file_id = DataFileId(decode_u64(
            &bytes[data_file_start..table_start],
            "data file id",
        )?);
        let table_id = TableId(decode_u64(&bytes[table_start..column_start], "table id")?);
        let column_id = ColumnId(decode_u64(
            &bytes[column_start..value_count_start],
            "column id",
        )?);
        let null_count = decode_u64(&bytes[null_count_start..values_start], "null count")?;
        let (min_value, offset) = decode_optional_string(bytes, values_start, "min value")?;
        let (max_value, offset) = decode_optional_string(bytes, offset, "max value")?;
        let (extra_stats, offset) = if version >= 3 {
            decode_optional_string(bytes, offset, "extra stats")?
        } else {
            (None, offset)
        };
        if offset != bytes.len() {
            return Err(CatalogError::Decode(format!(
                "file column stats row has {} trailing bytes",
                bytes.len() - offset
            )));
        }
        Ok(Self {
            data_file_id,
            table_id,
            column_id,
            value_count,
            null_count,
            min_value,
            max_value,
            extra_stats,
        })
    }
}

pub fn register_file_column_stats(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    row: FileColumnStatsRow,
) -> CatalogResult<FileColumnStatsRow> {
    register_file_column_stats_batch(kv, catalog, vec![row])?
        .pop()
        .ok_or_else(|| CatalogError::Decode("file column stats batch returned no rows".to_owned()))
}

pub fn register_file_column_stats_batch(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    rows: Vec<FileColumnStatsRow>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if rows.is_empty() {
        return Ok(rows);
    }
    let mut batch = KvBatch::new();
    let version = kv.generated_order_id()?;
    let table_ids = stats_data_file_table_ids(kv, catalog, &rows)?;
    for row in &rows {
        validate_file_column_stats_row(&table_ids, row)?;
        stage_file_column_stats(&mut batch, catalog, row);
    }
    stage_table_file_stats_versions_for_rows(&mut batch, catalog, &rows, version);
    kv.commit(batch)?;
    let mut invalidated_files = BTreeSet::new();
    for row in &rows {
        remove_cached_file_column_stats(kv, catalog, row.data_file_id, row.column_id);
        if invalidated_files.insert(row.data_file_id) {
            remove_cached_file_column_stats_for_data_file(kv, catalog, row.data_file_id);
        }
    }
    Ok(rows)
}

fn stats_data_file_table_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[FileColumnStatsRow],
) -> CatalogResult<BTreeMap<DataFileId, TableId>> {
    immutable_data_file_metadata_batch(kv, catalog, rows.iter().map(|row| row.data_file_id)).map(
        |metadata| {
            metadata
                .into_iter()
                .map(|(data_file_id, metadata)| (data_file_id, metadata.table_id))
                .collect()
        },
    )
}

fn validate_file_column_stats_row(
    table_ids: &BTreeMap<DataFileId, TableId>,
    row: &FileColumnStatsRow,
) -> CatalogResult<()> {
    let Some(table_id) = table_ids.get(&row.data_file_id) else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    if *table_id != row.table_id {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "stats table {} does not match data file table {}",
            row.table_id.0, table_id.0
        )));
    }
    Ok(())
}

pub(crate) fn stage_file_column_stats(
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &FileColumnStatsRow,
) {
    let encoded = row.encode();
    batch.put(
        file_column_stats_key(catalog, row.data_file_id, row.column_id),
        encoded.clone(),
    );
    batch.put(
        file_column_stats_lookup_key(catalog, row.table_id, row.column_id, row.data_file_id),
        encoded,
    );
}

pub(crate) fn stage_table_file_stats_versions_for_rows(
    batch: &mut KvBatch,
    catalog: CatalogId,
    rows: &[FileColumnStatsRow],
    version: CatalogOrderId,
) {
    if rows.is_empty() {
        return;
    }
    stage_catalog_file_stats_version(batch, catalog, version);
    let mut table_ids = BTreeSet::new();
    for row in rows {
        table_ids.insert(row.table_id);
    }
    for table_id in table_ids {
        stage_table_file_stats_version(batch, catalog, table_id, version);
    }
}

pub(crate) fn stage_catalog_file_stats_version(
    batch: &mut KvBatch,
    catalog: CatalogId,
    version: CatalogOrderId,
) {
    batch.put(
        catalog_file_stats_version_key(catalog),
        version.as_bytes().to_vec(),
    );
}

pub(crate) fn stage_table_file_stats_version(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    version: CatalogOrderId,
) {
    batch.put(
        table_file_stats_version_key(catalog, table_id),
        version.as_bytes().to_vec(),
    );
}

pub fn list_file_column_stats_for_table_column(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    column_id: ColumnId,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &file_column_stats_lookup_prefix(catalog, table_id, column_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        rows.push(FileColumnStatsRow::decode(&item.value)?);
    }
    record_runtime_method_stage(
        "method.file_stats.list_file_column_stats_for_table_column",
        started,
    );
    Ok(rows)
}

pub fn list_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::FileColumnStats),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| FileColumnStatsRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    record_runtime_method_stage("method.file_stats.list_file_column_stats", started);
    Ok(rows)
}

pub(crate) fn list_file_column_stats_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &[DataFileId],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    let mut missing = Vec::new();
    let mut requested = BTreeSet::new();
    for data_file_id in data_file_ids {
        if !requested.insert(*data_file_id) {
            continue;
        }
        let key = file_column_stats_file_cache_key(kv, catalog, *data_file_id);
        if let Some(cached) = cached_file_column_stats_for_data_file(&key) {
            rows.extend(cached);
        } else {
            missing.push(key);
        }
    }
    rows.extend(load_missing_file_column_stats_for_data_file_ids(
        kv, catalog, missing,
    )?);
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    record_runtime_method_stage(
        "method.file_stats.list_file_column_stats_for_data_file_ids",
        started,
    );
    Ok(rows)
}

fn load_missing_file_column_stats_for_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    mut missing: Vec<FileColumnStatsFileCacheKey>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    missing.sort();
    if dense_file_column_stats_scan_is_worthwhile(&missing) {
        return load_dense_file_column_stats_uncached(kv, catalog, missing);
    }

    let mut rows = Vec::new();
    for key in missing {
        let file_rows =
            load_file_column_stats_for_data_file_uncached(kv, catalog, key.data_file_id)?;
        insert_file_column_stats_rows(kv, catalog, &file_rows);
        insert_file_column_stats_for_data_file(key, file_rows.clone());
        rows.extend(file_rows);
    }
    Ok(rows)
}

fn dense_file_column_stats_scan_is_worthwhile(keys: &[FileColumnStatsFileCacheKey]) -> bool {
    if keys.len() < DENSE_FILE_COLUMN_STATS_SCAN_MIN_FILES {
        return false;
    }
    let Some(first) = keys.first() else {
        return false;
    };
    let Some(last) = keys.last() else {
        return false;
    };
    let span = last
        .data_file_id
        .0
        .saturating_sub(first.data_file_id.0)
        .saturating_add(1);
    let max_dense_span =
        (keys.len() as u64).saturating_mul(DENSE_FILE_COLUMN_STATS_SCAN_MAX_GAP_FACTOR);
    span <= max_dense_span
}

fn load_dense_file_column_stats_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    missing: Vec<FileColumnStatsFileCacheKey>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let (Some(first), Some(last)) = (missing.first(), missing.last()) else {
        return Ok(Vec::new());
    };
    let requested_ids = missing
        .iter()
        .map(|key| key.data_file_id)
        .collect::<BTreeSet<_>>();
    let start = file_column_stats_data_file_prefix(catalog, first.data_file_id);
    let end = prefix_end(&file_column_stats_data_file_prefix(
        catalog,
        last.data_file_id,
    ));
    let mut rows_by_file = requested_ids
        .iter()
        .copied()
        .map(|data_file_id| (data_file_id, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for item in kv.scan_range(&start, &end, RangeDirection::Forward, usize::MAX)? {
        let key_data_file_id = file_column_stats_data_file_id_from_key(catalog, &item.key)?;
        if !requested_ids.contains(&key_data_file_id) {
            continue;
        }
        let row = FileColumnStatsRow::decode(&item.value)?;
        validate_file_column_stats_data_file_row(catalog, key_data_file_id, &row)?;
        rows_by_file.entry(key_data_file_id).or_default().push(row);
    }

    let mut rows = Vec::new();
    for key in missing {
        let mut file_rows = rows_by_file.remove(&key.data_file_id).unwrap_or_default();
        file_rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
        insert_file_column_stats_rows(kv, catalog, &file_rows);
        insert_file_column_stats_for_data_file(key, file_rows.clone());
        rows.extend(file_rows);
    }
    Ok(rows)
}

fn load_file_column_stats_for_data_file_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &file_column_stats_data_file_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = FileColumnStatsRow::decode(&item.value)?;
        validate_file_column_stats_data_file_row(catalog, data_file_id, &row)?;
        rows.push(row);
    }
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    Ok(rows)
}

fn insert_file_column_stats_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[FileColumnStatsRow],
) {
    for row in rows {
        insert_file_column_stats(
            file_column_stats_cache_key_for_row(kv, catalog, row),
            row.clone(),
        );
    }
}

pub(crate) fn list_file_column_stats_for_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    files: &[DataFileRow],
    columns_by_table: &BTreeMap<TableId, Vec<ColumnId>>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    let mut requested_keys = Vec::new();
    let mut requested = BTreeSet::new();
    #[cfg(feature = "runtime-metrics")]
    let mut touched_tables = BTreeSet::new();
    for file in files {
        let Some(columns) = columns_by_table.get(&file.table_id) else {
            continue;
        };
        #[cfg(feature = "runtime-metrics")]
        touched_tables.insert(file.table_id);
        for column_id in columns {
            let cache_key = file_column_stats_cache_key(kv, catalog, file.data_file_id, *column_id);
            if !requested.insert(cache_key.clone()) {
                continue;
            }
            requested_keys.push(cache_key);
        }
    }
    let (mut cached_rows, missing) = cached_file_column_stats_rows(requested_keys);
    rows.append(&mut cached_rows);
    #[cfg(feature = "runtime-metrics")]
    let missing_count = missing.len();
    rows.extend(load_missing_file_column_stats_for_data_file_columns(
        kv, catalog, missing,
    )?);
    #[cfg(feature = "runtime-metrics")]
    {
        record_runtime_measurement(
            "measure.file_stats.current_files",
            u64::try_from(files.len()).unwrap_or(u64::MAX),
            0,
        );
        record_runtime_measurement(
            "measure.file_stats.current_tables",
            u64::try_from(touched_tables.len()).unwrap_or(u64::MAX),
            0,
        );
        record_runtime_measurement(
            "measure.file_stats.requested_keys",
            u64::try_from(requested.len()).unwrap_or(u64::MAX),
            0,
        );
        record_runtime_measurement(
            "measure.file_stats.cache_misses",
            u64::try_from(missing_count).unwrap_or(u64::MAX),
            0,
        );
        record_runtime_measurement(
            "measure.file_stats.returned_rows",
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
            0,
        );
    }
    rows.sort_by_key(|row| (row.table_id.0, row.data_file_id.0, row.column_id.0));
    record_runtime_method_stage(
        "method.file_stats.list_file_column_stats_for_data_files",
        started,
    );
    Ok(rows)
}

fn load_missing_file_column_stats_for_data_file_columns(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    missing: Vec<FileColumnStatsCacheKey>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    if missing.len()
        < FULL_FILE_STATS_LOAD_MIN_FILES.saturating_mul(FULL_FILE_STATS_LOAD_MIN_COLUMNS)
    {
        return load_exact_missing_file_column_stats(kv, catalog, missing);
    }

    let mut rows = Vec::new();
    let mut missing_by_file =
        BTreeMap::<FileColumnStatsFileCacheKey, Vec<FileColumnStatsCacheKey>>::new();
    for key in missing {
        let file_key = file_column_stats_file_cache_key(kv, catalog, key.data_file_id);
        missing_by_file.entry(file_key).or_default().push(key);
    }

    let mut exact_missing = Vec::new();
    let mut full_file_missing = Vec::new();
    let mut requested_columns_by_file = BTreeMap::<DataFileId, BTreeSet<ColumnId>>::new();
    for (file_key, keys) in missing_by_file {
        let requested_columns = keys
            .iter()
            .map(|key| key.column_id)
            .collect::<BTreeSet<_>>();
        if let Some(cached_rows) = cached_file_column_stats_for_data_file(&file_key) {
            for row in cached_rows {
                if requested_columns.contains(&row.column_id) {
                    insert_file_column_stats(
                        file_column_stats_cache_key_for_row(kv, catalog, &row),
                        row.clone(),
                    );
                    rows.push(row);
                }
            }
        } else if requested_columns.len() >= FULL_FILE_STATS_LOAD_MIN_COLUMNS {
            requested_columns_by_file.insert(file_key.data_file_id, requested_columns);
            full_file_missing.push(file_key);
        } else {
            exact_missing.extend(keys);
        }
    }

    if full_file_missing.len() >= FULL_FILE_STATS_LOAD_MIN_FILES
        && dense_file_column_stats_scan_is_worthwhile(&full_file_missing)
    {
        for row in load_dense_file_column_stats_uncached(kv, catalog, full_file_missing)? {
            if requested_columns_by_file
                .get(&row.data_file_id)
                .is_some_and(|columns| columns.contains(&row.column_id))
            {
                rows.push(row);
            }
        }
    } else {
        for file_key in full_file_missing {
            let Some(columns) = requested_columns_by_file.remove(&file_key.data_file_id) else {
                continue;
            };
            exact_missing.extend(
                columns
                    .into_iter()
                    .map(|column_id| FileColumnStatsCacheKey {
                        namespace: file_key.namespace,
                        catalog: file_key.catalog,
                        data_file_id: file_key.data_file_id,
                        column_id,
                    }),
            );
        }
    }

    rows.extend(load_exact_missing_file_column_stats(
        kv,
        catalog,
        exact_missing,
    )?);
    Ok(rows)
}

fn load_exact_missing_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    missing: Vec<FileColumnStatsCacheKey>,
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    let missing_keys = missing
        .iter()
        .map(|key| file_column_stats_key(catalog, key.data_file_id, key.column_id))
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for (cache_key, value) in missing.into_iter().zip(kv.batch_get(&missing_keys)?) {
        let Some(value) = value else {
            continue;
        };
        let row = FileColumnStatsRow::decode(&value)?;
        validate_file_column_stats_cache_row(&cache_key, &row)?;
        insert_file_column_stats(cache_key, row.clone());
        rows.push(row);
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FileColumnStatsCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    data_file_id: DataFileId,
    column_id: ColumnId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FileColumnStatsFileCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    data_file_id: DataFileId,
}

fn file_column_stats_cache_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    column_id: ColumnId,
) -> FileColumnStatsCacheKey {
    FileColumnStatsCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        data_file_id,
        column_id,
    }
}

fn file_column_stats_cache_key_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: &FileColumnStatsRow,
) -> FileColumnStatsCacheKey {
    file_column_stats_cache_key(kv, catalog, row.data_file_id, row.column_id)
}

fn file_column_stats_file_cache_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> FileColumnStatsFileCacheKey {
    FileColumnStatsFileCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        data_file_id,
    }
}

fn cached_file_column_stats_rows(
    keys: Vec<FileColumnStatsCacheKey>,
) -> (Vec<FileColumnStatsRow>, Vec<FileColumnStatsCacheKey>) {
    if keys.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let Ok(mut cache) = file_column_stats_row_cache().lock() else {
        return (Vec::new(), keys);
    };
    let mut rows = Vec::new();
    let mut missing = Vec::new();
    for key in keys {
        if let Some(row) = cache.get(&key) {
            rows.push(row);
        } else {
            missing.push(key);
        }
    }
    (rows, missing)
}

fn insert_file_column_stats(key: FileColumnStatsCacheKey, row: FileColumnStatsRow) {
    let Ok(mut cache) = file_column_stats_row_cache().lock() else {
        return;
    };
    cache.insert(key, row);
}

fn cached_file_column_stats_for_data_file(
    key: &FileColumnStatsFileCacheKey,
) -> Option<Vec<FileColumnStatsRow>> {
    let Ok(mut cache) = file_column_stats_file_cache().lock() else {
        return None;
    };
    cache.get(key)
}

fn insert_file_column_stats_for_data_file(
    key: FileColumnStatsFileCacheKey,
    rows: Vec<FileColumnStatsRow>,
) {
    let Ok(mut cache) = file_column_stats_file_cache().lock() else {
        return;
    };
    cache.insert(key, rows);
}

fn remove_cached_file_column_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    column_id: ColumnId,
) {
    let key = file_column_stats_cache_key(kv, catalog, data_file_id, column_id);
    if let Some(cache) = FILE_COLUMN_STATS_ROW_CACHE.get()
        && let Ok(mut cache) = cache.lock()
    {
        cache.remove(&key);
    }
}

pub(crate) fn remove_cached_file_column_stats_for_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) {
    let Some(cache) = FILE_COLUMN_STATS_FILE_CACHE.get() else {
        return;
    };
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    cache.remove(&file_column_stats_file_cache_key(kv, catalog, data_file_id));
}

fn file_column_stats_row_cache()
-> &'static Mutex<LruCache<FileColumnStatsCacheKey, FileColumnStatsRow>> {
    FILE_COLUMN_STATS_ROW_CACHE
        .get_or_init(|| Mutex::new(LruCache::new(FILE_COLUMN_STATS_ROW_CACHE_CAPACITY)))
}

fn file_column_stats_file_cache()
-> &'static Mutex<LruCache<FileColumnStatsFileCacheKey, Vec<FileColumnStatsRow>>> {
    FILE_COLUMN_STATS_FILE_CACHE
        .get_or_init(|| Mutex::new(LruCache::new(FILE_COLUMN_STATS_ROW_CACHE_CAPACITY)))
}

fn validate_file_column_stats_cache_row(
    key: &FileColumnStatsCacheKey,
    row: &FileColumnStatsRow,
) -> CatalogResult<()> {
    if row.data_file_id != key.data_file_id || row.column_id != key.column_id {
        return Err(CatalogError::Decode(format!(
            "file column stats key ({}, {}) contained row ({}, {})",
            key.data_file_id.0, key.column_id.0, row.data_file_id.0, row.column_id.0
        )));
    }
    Ok(())
}

fn validate_file_column_stats_data_file_row(
    catalog: CatalogId,
    data_file_id: DataFileId,
    row: &FileColumnStatsRow,
) -> CatalogResult<()> {
    if row.data_file_id != data_file_id {
        return Err(CatalogError::Decode(format!(
            "file column stats prefix for catalog {} data file {} decoded data file {}",
            catalog.0, data_file_id.0, row.data_file_id.0
        )));
    }
    Ok(())
}

fn file_column_stats_data_file_id_from_key(
    catalog: CatalogId,
    key: &[u8],
) -> CatalogResult<DataFileId> {
    let prefix = family_prefix(catalog, KeyFamily::FileColumnStats);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "file column stats key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = 8 + 1 + 8;
    if tail.len() != minimum_len || tail[8] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "file column stats key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; 8] = tail[..8].try_into().map_err(|_| {
        CatalogError::InvalidKey("file column stats data file id is truncated".to_owned())
    })?;
    Ok(DataFileId(u64::from_be_bytes(bytes)))
}

fn encode_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            let bytes = value.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0_u32.to_be_bytes());
        }
    }
}

fn decode_optional_string(
    bytes: &[u8],
    offset: usize,
    field: &str,
) -> CatalogResult<(Option<String>, usize)> {
    if bytes.len() < offset + 5 {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let present = bytes[offset];
    let len_start = offset + 1;
    let value_start = len_start + 4;
    let len = u32::from_be_bytes(
        bytes[len_start..value_start]
            .try_into()
            .map_err(|_| CatalogError::Decode(format!("{field} length is truncated")))?,
    ) as usize;
    let value_end = value_start + len;
    if bytes.len() < value_end {
        return Err(CatalogError::Decode(format!("{field} bytes are truncated")));
    }
    let value = match present {
        0 if len == 0 => None,
        0 => {
            return Err(CatalogError::Decode(format!(
                "{field} absent marker has nonzero length"
            )));
        }
        1 => Some(
            std::str::from_utf8(&bytes[value_start..value_end])
                .map_err(|err| CatalogError::Decode(format!("{field} is not utf8: {err}")))?
                .to_owned(),
        ),
        other => {
            return Err(CatalogError::Decode(format!(
                "{field} marker is invalid: {other}"
            )));
        }
    };
    Ok((value, value_end))
}

fn decode_optional_u64(bytes: &[u8], offset: usize, field: &str) -> CatalogResult<Option<u64>> {
    if bytes.len() < offset + 9 {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let present = bytes[offset];
    let value = decode_u64(&bytes[offset + 1..offset + 9], field)?;
    match present {
        0 if value == 0 => Ok(None),
        0 => Err(CatalogError::Decode(format!(
            "{field} absent marker has nonzero value"
        ))),
        1 => Ok(Some(value)),
        other => Err(CatalogError::Decode(format!(
            "{field} marker is invalid: {other}"
        ))),
    }
}

#[cfg(test)]
#[path = "file_stats_tests.rs"]
mod file_stats_tests;

fn decode_u64(bytes: &[u8], field: &str) -> CatalogResult<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        CatalogError::Decode(format!("{field} is truncated"))
    })?))
}
