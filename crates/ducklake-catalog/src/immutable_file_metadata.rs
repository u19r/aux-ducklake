use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, OnceLock},
};

use crate::{
    CatalogCacheNamespace, CatalogError, CatalogId, CatalogOrderId, CatalogResult, DataFileId,
    DataFileRow, OrderedCatalogKv, TableId, keys::data_file_key, lru_cache::LruCache,
};

const IMMUTABLE_DATA_FILE_METADATA_CACHE_CAPACITY: usize = 8192;

static IMMUTABLE_DATA_FILE_METADATA_CACHE: OnceLock<
    Mutex<LruCache<ImmutableDataFileMetadataCacheKey, ImmutableDataFileMetadata>>,
> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImmutableDataFileMetadata {
    pub(crate) data_file_id: DataFileId,
    pub(crate) table_id: TableId,
    pub(crate) path: String,
    pub(crate) record_count: u64,
    pub(crate) file_size_bytes: u64,
    pub(crate) footer_size: Option<u64>,
    pub(crate) row_id_start: u64,
    pub(crate) row_id_start_known: bool,
    pub(crate) mapping_id: Option<u64>,
    pub(crate) max_partial_order: Option<CatalogOrderId>,
}

impl From<&DataFileRow> for ImmutableDataFileMetadata {
    fn from(row: &DataFileRow) -> Self {
        Self {
            data_file_id: row.data_file_id,
            table_id: row.table_id,
            path: row.path.clone(),
            record_count: row.record_count,
            file_size_bytes: row.file_size_bytes,
            footer_size: row.footer_size,
            row_id_start: row.row_id_start,
            row_id_start_known: row.row_id_start_known,
            mapping_id: row.mapping_id,
            max_partial_order: row.max_partial_order,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImmutableDataFileMetadataCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    data_file_id: DataFileId,
}

pub(crate) fn immutable_data_file_metadata_batch(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_ids: impl IntoIterator<Item = DataFileId>,
) -> CatalogResult<BTreeMap<DataFileId, ImmutableDataFileMetadata>> {
    let mut metadata_by_file = BTreeMap::new();
    let mut missing = Vec::new();
    for data_file_id in data_file_ids.into_iter().collect::<BTreeSet<_>>() {
        let key = cache_key(kv, catalog, data_file_id);
        if let Some(metadata) = cached_immutable_data_file_metadata(&key) {
            metadata_by_file.insert(data_file_id, metadata);
        } else {
            missing.push((data_file_id, key));
        }
    }

    if missing.is_empty() {
        return Ok(metadata_by_file);
    }

    let keys = missing
        .iter()
        .map(|(data_file_id, _)| data_file_key(catalog, *data_file_id))
        .collect::<Vec<_>>();
    for ((requested_id, cache_key), value) in missing.into_iter().zip(kv.batch_get(&keys)?) {
        let Some(value) = value else {
            return Err(CatalogError::NotFound("data file"));
        };
        let data_file = DataFileRow::decode(&value)?;
        if data_file.data_file_id != requested_id {
            return Err(CatalogError::Decode(format!(
                "data file key {} contained row {}",
                requested_id.0, data_file.data_file_id.0
            )));
        }
        let metadata = ImmutableDataFileMetadata::from(&data_file);
        insert_immutable_data_file_metadata(cache_key, metadata.clone());
        metadata_by_file.insert(requested_id, metadata);
    }

    Ok(metadata_by_file)
}

pub(crate) fn remove_immutable_data_file_metadata(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) {
    let key = cache_key(kv, catalog, data_file_id);
    let Some(cache) = IMMUTABLE_DATA_FILE_METADATA_CACHE.get() else {
        return;
    };
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    cache.remove(&key);
}

fn cache_key(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> ImmutableDataFileMetadataCacheKey {
    ImmutableDataFileMetadataCacheKey {
        namespace: kv.catalog_cache_namespace(),
        catalog,
        data_file_id,
    }
}

fn cached_immutable_data_file_metadata(
    key: &ImmutableDataFileMetadataCacheKey,
) -> Option<ImmutableDataFileMetadata> {
    let cache = immutable_data_file_metadata_cache();
    let Ok(mut cache) = cache.lock() else {
        return None;
    };
    cache.get(key)
}

fn insert_immutable_data_file_metadata(
    key: ImmutableDataFileMetadataCacheKey,
    metadata: ImmutableDataFileMetadata,
) {
    let cache = immutable_data_file_metadata_cache();
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    cache.insert(key, metadata);
}

fn immutable_data_file_metadata_cache()
-> &'static Mutex<LruCache<ImmutableDataFileMetadataCacheKey, ImmutableDataFileMetadata>> {
    IMMUTABLE_DATA_FILE_METADATA_CACHE
        .get_or_init(|| Mutex::new(LruCache::new(IMMUTABLE_DATA_FILE_METADATA_CACHE_CAPACITY)))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::{CatalogOrderId, FakeOrderedCatalogKv, KvBatch, RangeDirection, RangeItem};

    use super::*;

    #[test]
    fn given_same_metadata_loaded_twice_when_cached_then_second_load_skips_kv_get() {
        let catalog = CatalogId(901);
        let data_file_id = DataFileId(902);
        let key = data_file_key(catalog, data_file_id);
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            key.clone(),
            DataFileRow::new(
                data_file_id,
                TableId(903),
                "main/cached.parquet",
                10,
                128,
                CatalogOrderId::uuid_v7(1),
            )
            .encode(),
        );
        inner.commit(batch).unwrap();
        let kv = CountingGetKv::new(inner, key);

        let first = immutable_data_file_metadata_batch(&kv, catalog, [data_file_id]).unwrap();
        let second = immutable_data_file_metadata_batch(&kv, catalog, [data_file_id]).unwrap();

        assert_eq!(first, second);
        assert_eq!(kv.data_file_gets(), 1);
    }

    #[test]
    fn given_metadata_removed_when_loaded_again_then_storage_is_checked() {
        let catalog = CatalogId(904);
        let data_file_id = DataFileId(905);
        let key = data_file_key(catalog, data_file_id);
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        batch.put(
            key.clone(),
            DataFileRow::new(
                data_file_id,
                TableId(906),
                "main/remove.parquet",
                10,
                128,
                CatalogOrderId::uuid_v7(1),
            )
            .encode(),
        );
        inner.commit(batch).unwrap();
        let kv = CountingGetKv::new(inner, key);

        immutable_data_file_metadata_batch(&kv, catalog, [data_file_id]).unwrap();
        remove_immutable_data_file_metadata(&kv, catalog, data_file_id);
        immutable_data_file_metadata_batch(&kv, catalog, [data_file_id]).unwrap();

        assert_eq!(kv.data_file_gets(), 2);
    }

    #[test]
    fn given_multiple_missing_metadata_rows_when_loaded_then_batch_get_is_used_once() {
        let catalog = CatalogId(907);
        let first = DataFileId(908);
        let second = DataFileId(909);
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        for data_file_id in [first, second] {
            batch.put(
                data_file_key(catalog, data_file_id),
                DataFileRow::new(
                    data_file_id,
                    TableId(910),
                    format!("main/{data_file_id:?}.parquet"),
                    10,
                    128,
                    CatalogOrderId::uuid_v7(1),
                )
                .encode(),
            );
        }
        inner.commit(batch).unwrap();
        let kv = CountingGetKv::new(inner, data_file_key(catalog, first));

        let loaded = immutable_data_file_metadata_batch(&kv, catalog, [first, second]).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(kv.batch_gets(), 1);
    }

    #[test]
    fn given_cache_exceeds_capacity_when_entry_is_reused_then_lru_entry_is_evicted() {
        let mut cache = LruCache::new(2);
        let namespace = CatalogCacheNamespace::process_local(1);
        let first = test_key(namespace, 1);
        let second = test_key(namespace, 2);
        let third = test_key(namespace, 3);

        cache.insert(first.clone(), test_metadata(1));
        cache.insert(second.clone(), test_metadata(2));
        assert!(cache.get(&first).is_some());
        cache.insert(third.clone(), test_metadata(3));

        assert!(cache.get(&first).is_some());
        assert!(cache.get(&second).is_none());
        assert!(cache.get(&third).is_some());
    }

    fn test_key(
        namespace: CatalogCacheNamespace,
        data_file_id: u64,
    ) -> ImmutableDataFileMetadataCacheKey {
        ImmutableDataFileMetadataCacheKey {
            namespace,
            catalog: CatalogId(900),
            data_file_id: DataFileId(data_file_id),
        }
    }

    fn test_metadata(data_file_id: u64) -> ImmutableDataFileMetadata {
        ImmutableDataFileMetadata {
            data_file_id: DataFileId(data_file_id),
            table_id: TableId(900),
            path: format!("main/{data_file_id}.parquet"),
            record_count: 1,
            file_size_bytes: 1,
            footer_size: None,
            row_id_start: 0,
            row_id_start_known: false,
            mapping_id: None,
            max_partial_order: None,
        }
    }

    struct CountingGetKv {
        inner: FakeOrderedCatalogKv,
        counted_key: Vec<u8>,
        data_file_gets: Cell<usize>,
        batch_gets: Cell<usize>,
    }

    impl CountingGetKv {
        fn new(inner: FakeOrderedCatalogKv, counted_key: Vec<u8>) -> Self {
            Self {
                inner,
                counted_key,
                data_file_gets: Cell::new(0),
                batch_gets: Cell::new(0),
            }
        }

        fn data_file_gets(&self) -> usize {
            self.data_file_gets.get()
        }

        fn batch_gets(&self) -> usize {
            self.batch_gets.get()
        }
    }

    impl OrderedCatalogKv for CountingGetKv {
        fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            if key == self.counted_key {
                self.data_file_gets
                    .set(self.data_file_gets.get().saturating_add(1));
            }
            Ok(self.inner.get(key))
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
            self.batch_gets.set(self.batch_gets.get().saturating_add(1));
            keys.iter().map(|key| self.get(key)).collect()
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            Ok(self.inner.scan_prefix(prefix, direction, limit))
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> CatalogResult<Vec<RangeItem>> {
            Ok(self.inner.scan_range(start, end, direction, limit))
        }

        fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
            Ok(self.inner.read_conflict_fence(key))
        }
    }
}
