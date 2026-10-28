#[cfg(feature = "foundationdb")]
use crate::FdbOrderedCatalogKv;
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, RangeItem, RawSnapshotSequence, TableVersionReplacement,
    runtime_protocol::RuntimeCatalogBackend,
};

pub(crate) enum RuntimeMutableCatalog {
    #[cfg(feature = "foundationdb")]
    FoundationDb(FdbOrderedCatalogKv),
    #[cfg(not(feature = "foundationdb"))]
    Unavailable,
}

// Preserve backend batching: the trait defaults perform serial reads.
impl OrderedCatalogKv for RuntimeMutableCatalog {
    fn catalog_cache_namespace(&self) -> crate::CatalogCacheNamespace {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.catalog_cache_namespace(),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => {
                crate::CatalogCacheNamespace::process_local(self as *const Self as usize)
            }
        }
    }

    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = key;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.get(key),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = keys;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.batch_get(keys),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_index_rows(
        &self,
        join: &crate::kv::CatalogIndexJoin,
    ) -> CatalogResult<Vec<crate::kv::CatalogIndexRow>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = join;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_index_rows(join),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn read_set(
        &self,
        keys: &[Vec<u8>],
        ranges: &[crate::kv::CatalogReadRange],
    ) -> CatalogResult<crate::kv::CatalogReadResult> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (keys, ranges);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.read_set(keys, ranges),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (prefix, direction, limit);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_prefix(prefix, direction, limit),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_prefixes(
        &self,
        prefixes: &[Vec<u8>],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<Vec<RangeItem>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (prefixes, direction, limit);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_prefixes(prefixes, direction, limit),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (start, end, direction, limit);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_range(start, end, direction, limit),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = key;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.read_conflict_fence(key),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }
}

impl MutableCatalogKv for RuntimeMutableCatalog {
    fn generated_order_id(&mut self) -> CatalogResult<CatalogOrderId> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.generated_order_id(),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn commit(&mut self, batch: KvBatch) -> CatalogResult<()> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = batch;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.commit(batch),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn commit_table_replacements(
        &mut self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (catalog, previous_sequence, replacements);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => {
                kv.commit_table_replacements(catalog, previous_sequence, replacements)
            }
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_unavailable() -> crate::CatalogError {
    crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    )
}

pub(crate) fn open_runtime_catalog(
    _backend: RuntimeCatalogBackend,
) -> CatalogResult<RuntimeMutableCatalog> {
    #[cfg(feature = "foundationdb")]
    {
        crate::runtime_foundationdb::open_foundationdb_catalog()
            .map(RuntimeMutableCatalog::FoundationDb)
    }
    #[cfg(not(feature = "foundationdb"))]
    {
        Err(foundationdb_runtime_unavailable())
    }
}
