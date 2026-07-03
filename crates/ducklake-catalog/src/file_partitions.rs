use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Mutex, OnceLock},
};

#[cfg(feature = "foundationdb")]
use crate::RangeItem;
use crate::{
    AttachedDataFile, CatalogCacheNamespace, CatalogError, CatalogId, CatalogResult, DataFileId,
    DataFileRow, KvBatch, MutableCatalogKv, OrderedCatalogKv, PartitionKeyIndex, RangeDirection,
    TableId,
    data_file_store::{
        attach_current_delete_files, attach_delete_files_at, list_current_data_files,
        list_data_files_at,
    },
    ids::CatalogOrderId,
    keys::{
        KeyFamily, current_data_file_key, data_file_key, family_prefix, file_partition_value_key,
        file_partition_value_prefix, partition_value_lookup_key, partition_value_lookup_prefix,
        prefix_end,
    },
    lru_cache::LruCache,
};

const FILE_PARTITION_VALUES_CACHE_CAPACITY: usize = 131_072;
const DENSE_PARTITION_VALUE_SCAN_MIN_FILES: usize = 2;
const DENSE_PARTITION_VALUE_SCAN_MAX_GAP_FACTOR: u64 = 8;

static FILE_PARTITION_VALUES_CACHE: OnceLock<
    Mutex<LruCache<FilePartitionValuesCacheKey, Vec<FilePartitionValueRow>>>,
> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePartitionValueRow {
    pub data_file_id: DataFileId,
    pub table_id: TableId,
    pub partition_key_index: PartitionKeyIndex,
    pub partition_value: String,
}

impl FilePartitionValueRow {
    const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        data_file_id: DataFileId,
        table_id: TableId,
        partition_key_index: PartitionKeyIndex,
        partition_value: impl Into<String>,
    ) -> Self {
        Self {
            data_file_id,
            table_id,
            partition_key_index,
            partition_value: partition_value.into(),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let value_bytes = self.partition_value.as_bytes();
        let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + 4 + value_bytes.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.partition_key_index.0.to_be_bytes());
        out.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(value_bytes);
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let fixed_len = 1 + 8 + 8 + 4 + 4;
        if bytes.len() < fixed_len {
            return Err(CatalogError::Decode(format!(
                "file partition value row is too short: {} bytes",
                bytes.len()
            )));
        }
        if bytes[0] != Self::VERSION {
            return Err(CatalogError::Decode(format!(
                "unsupported file partition value row version {}",
                bytes[0]
            )));
        }
        let data_file_start = 1;
        let table_start = data_file_start + 8;
        let partition_index_start = table_start + 8;
        let value_len_start = partition_index_start + 4;
        let value_start = value_len_start + 4;
        let data_file_id = DataFileId(decode_u64(
            &bytes[data_file_start..table_start],
            "data file id",
        )?);
        let table_id = TableId(decode_u64(
            &bytes[table_start..partition_index_start],
            "table id",
        )?);
        let partition_key_index = PartitionKeyIndex(decode_u32(
            &bytes[partition_index_start..value_len_start],
            "partition key index",
        )?);
        let value_len = decode_u32(
            &bytes[value_len_start..value_start],
            "partition value length",
        )? as usize;
        let value_end = value_start + value_len;
        if bytes.len() != value_end {
            return Err(CatalogError::Decode(format!(
                "file partition value row expected {value_end} bytes, got {}",
                bytes.len()
            )));
        }
        let partition_value = std::str::from_utf8(&bytes[value_start..value_end])
            .map_err(|err| CatalogError::Decode(format!("partition value is not utf8: {err}")))?
            .to_owned();
        Ok(Self {
            data_file_id,
            table_id,
            partition_key_index,
            partition_value,
        })
    }
}

pub fn register_file_partition_value(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    row: FilePartitionValueRow,
) -> CatalogResult<FilePartitionValueRow> {
    let Some(value) = kv.get(&data_file_key(catalog, row.data_file_id))? else {
        return Err(CatalogError::NotFound("data file"));
    };
    let data_file = DataFileRow::decode(&value)?;
    if data_file.table_id != row.table_id {
        return Err(CatalogError::InvalidMutation(format!(
            "partition table {} does not match data file table {}",
            row.table_id.0, data_file.table_id.0
        )));
    }

    let primary_key = file_partition_value_key(catalog, row.data_file_id, row.partition_key_index);
    let mut batch = KvBatch::new();
    if let Some(existing) = kv.get(&primary_key)? {
        let existing = FilePartitionValueRow::decode(&existing)?;
        batch.delete(partition_lookup_key(catalog, &existing));
    }
    stage_file_partition_value(&mut batch, catalog, &row, &data_file);
    kv.commit(batch)?;
    remove_cached_file_partition_values(kv, catalog, row.data_file_id);
    Ok(row)
}

pub fn list_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let mut rows = kv
        .scan_prefix(
            &crate::keys::family_prefix(catalog, crate::keys::KeyFamily::FilePartitionValue),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| FilePartitionValueRow::decode(&item.value))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| {
        (
            row.table_id.0,
            row.data_file_id.0,
            row.partition_key_index.0,
            row.partition_value.clone(),
        )
    });
    Ok(rows)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn list_partition_lookup_values_for_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let prefix = partition_key_lookup_prefix(catalog, table_id, partition_key_index);
    let mut rows = kv
        .scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)?
        .into_iter()
        .map(|item| partition_lookup_key_row(&item, &prefix, table_id, partition_key_index))
        .collect::<CatalogResult<Vec<_>>>()?;
    rows.sort_by_key(|row| {
        (
            row.table_id.0,
            row.data_file_id.0,
            row.partition_key_index.0,
            row.partition_value.clone(),
        )
    });
    Ok(rows)
}

#[cfg(feature = "foundationdb")]
fn partition_lookup_key_row(
    item: &RangeItem,
    prefix: &[u8],
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
) -> CatalogResult<FilePartitionValueRow> {
    let Some(suffix) = item.key.strip_prefix(prefix) else {
        return Err(CatalogError::Decode(
            "partition lookup key does not match requested prefix".to_owned(),
        ));
    };
    let (partition_value, tail) = take_len_prefixed(suffix, "partition lookup key value")?;
    if tail.len() != 9 || tail[0] != b'/' {
        return Err(CatalogError::Decode(format!(
            "partition lookup key has invalid data file suffix length {}",
            tail.len()
        )));
    }
    let partition_value = String::from_utf8(partition_value.to_vec())
        .map_err(|_| CatalogError::Decode("partition lookup key value is not utf-8".to_owned()))?;
    Ok(FilePartitionValueRow::new(
        decode_data_file_id(&tail[1..])?,
        table_id,
        partition_key_index,
        partition_value,
    ))
}

pub(crate) fn list_file_partition_values_for_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let mut rows = Vec::new();
    let mut missing = Vec::new();
    for data_file_id in data_file_ids {
        let key = file_partition_values_cache_key(kv, catalog, *data_file_id);
        if let Some(cached) = cached_file_partition_values(&key) {
            rows.extend(cached);
        } else {
            missing.push(key);
        }
    }
    rows.extend(load_missing_file_partition_values(kv, catalog, missing)?);
    rows.sort_by_key(|row| {
        (
            row.table_id.0,
            row.data_file_id.0,
            row.partition_key_index.0,
            row.partition_value.clone(),
        )
    });
    Ok(rows)
}

fn load_missing_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    mut missing: Vec<FilePartitionValuesCacheKey>,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    missing.sort();
    if dense_partition_value_scan_is_worthwhile(&missing) {
        return load_dense_file_partition_values_uncached(kv, catalog, missing);
    }

    let mut rows = Vec::new();
    for key in missing {
        let file_rows = load_file_partition_values_uncached(kv, catalog, key.data_file_id)?;
        insert_file_partition_values(key, file_rows.clone());
        rows.extend(file_rows);
    }
    Ok(rows)
}

fn dense_partition_value_scan_is_worthwhile(keys: &[FilePartitionValuesCacheKey]) -> bool {
    if keys.len() < DENSE_PARTITION_VALUE_SCAN_MIN_FILES {
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
        (keys.len() as u64).saturating_mul(DENSE_PARTITION_VALUE_SCAN_MAX_GAP_FACTOR);
    span <= max_dense_span
}

fn load_dense_file_partition_values_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    missing: Vec<FilePartitionValuesCacheKey>,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let (Some(first), Some(last)) = (missing.first(), missing.last()) else {
        return Ok(Vec::new());
    };
    let requested_ids = missing
        .iter()
        .map(|key| key.data_file_id)
        .collect::<BTreeSet<_>>();
    let start = file_partition_value_prefix(catalog, first.data_file_id);
    let end = prefix_end(&file_partition_value_prefix(catalog, last.data_file_id));
    let mut rows_by_file = requested_ids
        .iter()
        .copied()
        .map(|data_file_id| (data_file_id, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for item in kv.scan_range(&start, &end, RangeDirection::Forward, usize::MAX)? {
        let row = FilePartitionValueRow::decode(&item.value)?;
        if requested_ids.contains(&row.data_file_id) {
            rows_by_file.entry(row.data_file_id).or_default().push(row);
        }
    }

    let mut rows = Vec::new();
    for key in missing {
        let mut file_rows = rows_by_file.remove(&key.data_file_id).unwrap_or_default();
        file_rows.sort_by_key(|row| {
            (
                row.table_id.0,
                row.data_file_id.0,
                row.partition_key_index.0,
                row.partition_value.clone(),
            )
        });
        insert_file_partition_values(key, file_rows.clone());
        rows.extend(file_rows);
    }
    Ok(rows)
}

fn load_file_partition_values_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &file_partition_value_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = FilePartitionValueRow::decode(&item.value)?;
        if row.data_file_id != data_file_id {
            return Err(CatalogError::Decode(format!(
                "file partition value prefix for catalog {} data file {} decoded data file {}",
                catalog.0, data_file_id.0, row.data_file_id.0
            )));
        }
        rows.push(row);
    }
    rows.sort_by_key(|row| {
        (
            row.table_id.0,
            row.data_file_id.0,
            row.partition_key_index.0,
            row.partition_value.clone(),
        )
    });
    Ok(rows)
}

pub(crate) fn remove_cached_file_partition_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) {
    let Some(cache) = FILE_PARTITION_VALUES_CACHE.get() else {
        return;
    };
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    cache.remove(&file_partition_values_cache_key(kv, catalog, data_file_id));
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilePartitionValuesCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    data_file_id: DataFileId,
}

fn file_partition_values_cache_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> FilePartitionValuesCacheKey {
    FilePartitionValuesCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        data_file_id,
    }
}

fn cached_file_partition_values(
    key: &FilePartitionValuesCacheKey,
) -> Option<Vec<FilePartitionValueRow>> {
    let Ok(mut cache) = file_partition_values_cache().lock() else {
        return None;
    };
    cache.get(key)
}

fn insert_file_partition_values(
    key: FilePartitionValuesCacheKey,
    rows: Vec<FilePartitionValueRow>,
) {
    let Ok(mut cache) = file_partition_values_cache().lock() else {
        return;
    };
    cache.insert(key, rows);
}

fn file_partition_values_cache()
-> &'static Mutex<LruCache<FilePartitionValuesCacheKey, Vec<FilePartitionValueRow>>> {
    FILE_PARTITION_VALUES_CACHE
        .get_or_init(|| Mutex::new(LruCache::new(FILE_PARTITION_VALUES_CACHE_CAPACITY)))
}

pub fn list_current_data_files_by_partition_value(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<Vec<DataFileRow>> {
    partition_value_lookup_current_data_files(
        kv,
        catalog,
        table_id,
        partition_key_index,
        partition_value,
    )
}

pub fn list_current_data_files_by_partition_value_with_deletes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let rows = list_current_data_files_by_partition_value(
        kv,
        catalog,
        table_id,
        partition_key_index,
        partition_value,
    )?;
    attach_current_delete_files(kv, catalog, rows)
}

pub(crate) fn list_current_data_files_for_partition_scan(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<Vec<DataFileRow>> {
    let matched = partition_value_lookup_current_data_file_ids(
        kv,
        catalog,
        table_id,
        partition_key_index,
        partition_value,
    )?;
    let partitioned = partitioned_data_file_ids(kv, catalog, table_id, partition_key_index)?;
    Ok(current_data_files_by_id(kv, catalog, table_id)?
        .into_values()
        .filter(|data_file| {
            matched.contains(&data_file.data_file_id)
                || !partitioned.contains(&data_file.data_file_id)
        })
        .collect())
}

pub fn list_current_data_files_for_partition_scan_with_deletes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let rows = list_current_data_files_for_partition_scan(
        kv,
        catalog,
        table_id,
        partition_key_index,
        partition_value,
    )?;
    attach_current_delete_files(kv, catalog, rows)
}

pub fn list_data_files_for_partition_scan_at_with_deletes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    let filter =
        PartitionScanFilter::load(kv, catalog, table_id, partition_key_index, partition_value)?;
    let rows = list_data_files_at(kv, catalog, table_id, snapshot_order)?
        .into_iter()
        .filter(|data_file| filter.includes(data_file.data_file_id))
        .collect();
    attach_delete_files_at(kv, catalog, rows, snapshot_order)
}

pub(crate) fn delete_partition_values_for_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    for item in kv.scan_prefix(
        &file_partition_value_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = FilePartitionValueRow::decode(&item.value)?;
        batch.delete(item.key);
        batch.delete(partition_lookup_key(catalog, &row));
    }
    remove_cached_file_partition_values(kv, catalog, data_file_id);
    Ok(())
}

pub(crate) fn delete_partition_lookups_for_data_file(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<()> {
    for item in kv.scan_prefix(
        &file_partition_value_prefix(catalog, data_file_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = FilePartitionValueRow::decode(&item.value)?;
        batch.delete(partition_lookup_key(catalog, &row));
    }
    Ok(())
}

pub(crate) fn stage_file_partition_value(
    batch: &mut KvBatch,
    catalog: CatalogId,
    row: &FilePartitionValueRow,
    data_file: &DataFileRow,
) {
    batch.put(
        file_partition_value_key(catalog, row.data_file_id, row.partition_key_index),
        row.encode(),
    );
    batch.put(
        partition_lookup_key(catalog, row),
        encode_partition_lookup_value(row, data_file),
    );
}

fn current_data_files_by_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<BTreeMap<DataFileId, DataFileRow>> {
    Ok(list_current_data_files(kv, catalog, table_id)?
        .into_iter()
        .map(|row| (row.data_file_id, row))
        .collect())
}

fn partition_value_lookup_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<HashSet<DataFileId>> {
    let prefix =
        partition_value_lookup_prefix(catalog, table_id, partition_key_index, partition_value);
    kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)?
        .into_iter()
        .map(|item| partition_lookup_data_file_id(&item.value))
        .collect()
}

pub(crate) struct PartitionScanFilter {
    matched: HashSet<DataFileId>,
    partitioned: HashSet<DataFileId>,
}

impl PartitionScanFilter {
    pub(crate) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        partition_key_index: PartitionKeyIndex,
        partition_value: &str,
    ) -> CatalogResult<Self> {
        Ok(Self {
            matched: partition_value_lookup_ids(
                kv,
                catalog,
                table_id,
                partition_key_index,
                partition_value,
            )?,
            partitioned: partitioned_data_file_ids(kv, catalog, table_id, partition_key_index)?,
        })
    }

    pub(crate) fn includes(&self, data_file_id: DataFileId) -> bool {
        self.matched.contains(&data_file_id) || !self.partitioned.contains(&data_file_id)
    }
}

fn partition_value_lookup_current_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<Vec<DataFileRow>> {
    let prefix =
        partition_value_lookup_prefix(catalog, table_id, partition_key_index, partition_value);
    let mut rows = Vec::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        match partition_lookup_data_file(&item.value)? {
            PartitionLookupDataFile::Inline(row) => {
                if row.validity.end_order.is_none() {
                    rows.push(row);
                }
            }
            PartitionLookupDataFile::Pointer(data_file_id) => {
                let Some(value) =
                    kv.get(&current_data_file_key(catalog, table_id, data_file_id))?
                else {
                    continue;
                };
                let row = DataFileRow::decode(&value)?;
                if row.validity.end_order.is_none() {
                    rows.push(row);
                }
            }
        }
    }
    Ok(rows)
}

fn partition_value_lookup_current_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> CatalogResult<HashSet<DataFileId>> {
    Ok(partition_value_lookup_current_data_files(
        kv,
        catalog,
        table_id,
        partition_key_index,
        partition_value,
    )?
    .into_iter()
    .map(|row| row.data_file_id)
    .collect())
}

fn partitioned_data_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
) -> CatalogResult<HashSet<DataFileId>> {
    kv.scan_prefix(
        &partition_key_lookup_prefix(catalog, table_id, partition_key_index),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| partition_lookup_data_file_id(&item.value))
    .collect()
}

fn partition_lookup_data_file_id(value: &[u8]) -> CatalogResult<DataFileId> {
    match partition_lookup_data_file(value)? {
        PartitionLookupDataFile::Inline(row) => Ok(row.data_file_id),
        PartitionLookupDataFile::Pointer(id) => Ok(id),
    }
}

enum PartitionLookupDataFile {
    Inline(DataFileRow),
    Pointer(DataFileId),
}

fn partition_lookup_data_file(value: &[u8]) -> CatalogResult<PartitionLookupDataFile> {
    match PartitionLookupValue::decode(value)? {
        PartitionLookupValue::WithDataFile(row) => Ok(PartitionLookupDataFile::Inline(row)),
        PartitionLookupValue::PartitionOnly(row) => {
            Ok(PartitionLookupDataFile::Pointer(row.data_file_id))
        }
        PartitionLookupValue::Pointer(id) => Ok(PartitionLookupDataFile::Pointer(id)),
    }
}

enum PartitionLookupValue {
    WithDataFile(DataFileRow),
    PartitionOnly(FilePartitionValueRow),
    Pointer(DataFileId),
}

impl PartitionLookupValue {
    const WITH_DATA_FILE_VERSION: u8 = 2;

    fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        if bytes.first() == Some(&Self::WITH_DATA_FILE_VERSION) {
            return decode_partition_lookup_value(bytes).map(Self::WithDataFile);
        }
        if let Ok(row) = FilePartitionValueRow::decode(bytes) {
            return Ok(Self::PartitionOnly(row));
        }
        decode_data_file_id(bytes).map(Self::Pointer)
    }
}

pub(crate) fn encode_partition_lookup_value(
    row: &FilePartitionValueRow,
    data_file: &DataFileRow,
) -> Vec<u8> {
    let partition = row.encode();
    let data_file = data_file.encode();
    let mut out = Vec::with_capacity(
        1usize
            .saturating_add(4)
            .saturating_add(partition.len())
            .saturating_add(4)
            .saturating_add(data_file.len()),
    );
    out.push(PartitionLookupValue::WITH_DATA_FILE_VERSION);
    out.extend_from_slice(&(partition.len() as u32).to_be_bytes());
    out.extend_from_slice(&partition);
    out.extend_from_slice(&(data_file.len() as u32).to_be_bytes());
    out.extend_from_slice(&data_file);
    out
}

fn decode_partition_lookup_value(bytes: &[u8]) -> CatalogResult<DataFileRow> {
    let Some((&version, tail)) = bytes.split_first() else {
        return Err(CatalogError::Decode(
            "partition lookup value is empty".to_owned(),
        ));
    };
    if version != PartitionLookupValue::WITH_DATA_FILE_VERSION {
        return Err(CatalogError::Decode(format!(
            "unsupported partition lookup value version {version}"
        )));
    }
    let (partition_len, tail) = take_len_prefixed(tail, "partition lookup partition row")?;
    FilePartitionValueRow::decode(partition_len)?;
    let (data_file_len, tail) = take_len_prefixed(tail, "partition lookup data file row")?;
    if !tail.is_empty() {
        return Err(CatalogError::Decode(format!(
            "partition lookup value has {} trailing bytes",
            tail.len()
        )));
    }
    DataFileRow::decode(data_file_len)
}

fn take_len_prefixed<'a>(bytes: &'a [u8], field: &str) -> CatalogResult<(&'a [u8], &'a [u8])> {
    let Some(len_bytes) = bytes.get(..4) else {
        return Err(CatalogError::Decode(format!("{field} length is truncated")));
    };
    let len = decode_u32(len_bytes, field)? as usize;
    let value_start = 4;
    let value_end = value_start + len;
    let Some(value) = bytes.get(value_start..value_end) else {
        return Err(CatalogError::Decode(format!(
            "{field} expected {value_end} bytes, got {}",
            bytes.len()
        )));
    };
    Ok((value, &bytes[value_end..]))
}

fn partition_key_lookup_prefix(
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::PartitionValueLookup);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&partition_key_index.0.to_be_bytes());
    key.push(b'/');
    key
}

fn partition_lookup_key(catalog: CatalogId, row: &FilePartitionValueRow) -> Vec<u8> {
    partition_value_lookup_key(
        catalog,
        row.table_id,
        row.partition_key_index,
        &row.partition_value,
        row.data_file_id,
    )
}

fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    if bytes.len() != 8 {
        return Err(CatalogError::Decode(format!(
            "data file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DataFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| CatalogError::Decode("data file id pointer is truncated".to_owned()),
    )?)))
}

fn decode_u64(bytes: &[u8], field: &str) -> CatalogResult<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        CatalogError::Decode(format!("{field} is truncated"))
    })?))
}

fn decode_u32(bytes: &[u8], field: &str) -> CatalogResult<u32> {
    Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| {
        CatalogError::Decode(format!("{field} is truncated"))
    })?))
}

#[cfg(test)]
#[path = "file_partitions_tests.rs"]
mod file_partitions_tests;
