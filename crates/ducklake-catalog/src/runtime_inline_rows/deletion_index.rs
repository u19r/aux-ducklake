#[cfg(not(test))]
use std::sync::OnceLock;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, SchemaId,
    inline_change_feed::list_inline_deleted_row_changes_for_schema, runtime_inline_rows::*,
};

#[derive(Clone)]
pub(super) struct InlineDeletionIndex {
    deleted_at: BTreeMap<u64, BTreeSet<u64>>,
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct InlineDeletionIndexCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: crate::TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
pub(super) static INLINE_DELETION_INDEX_CACHE: OnceLock<
    BoundedCache<InlineDeletionIndexCacheKey, InlineDeletionIndex>,
> = OnceLock::new();

#[cfg(not(test))]
pub(super) fn inline_deletion_index_cache()
-> &'static BoundedCache<InlineDeletionIndexCacheKey, InlineDeletionIndex> {
    static_bounded_cache(&INLINE_DELETION_INDEX_CACHE, 512)
}

impl InlineDeletionIndex {
    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
        snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    ) -> CatalogResult<Self> {
        #[cfg(not(test))]
        {
            let key = InlineDeletionIndexCacheKey {
                namespace: kv.catalog_cache_namespace(),
                catalog,
                table_id,
                schema_id,
                snapshot_order,
            };
            let cache = inline_deletion_index_cache();
            if let Some(index) = cache.get(key) {
                return Ok(index);
            }
            let index = Self::load_uncached(
                kv,
                catalog,
                table_id,
                schema_id,
                snapshot_order,
                snapshot_sequences,
            )?;
            cache.insert(key, index.clone());
            Ok(index)
        }
        #[cfg(test)]
        {
            Self::load_uncached(
                kv,
                catalog,
                table_id,
                schema_id,
                snapshot_order,
                snapshot_sequences,
            )
        }
    }

    fn load_uncached(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
        snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    ) -> CatalogResult<Self> {
        let start_order =
            CatalogOrderId::from_bytes(snapshot_order.kind(), [0; CatalogOrderId::LEN]);
        let read_snapshot = snapshot_sequences
            .get(&snapshot_order)
            .copied()
            .ok_or_else(|| missing_snapshot_order(snapshot_order))?;
        let mut deleted_at = BTreeMap::<u64, BTreeSet<u64>>::new();
        for change in list_inline_deleted_row_changes_for_schema(
            kv,
            catalog,
            table_id,
            schema_id,
            start_order,
            snapshot_order,
        )? {
            let delete_snapshot = snapshot_sequences
                .get(&change.order)
                .copied()
                .ok_or_else(|| missing_snapshot_order(change.order))?;
            if delete_snapshot > read_snapshot {
                continue;
            }
            deleted_at
                .entry(change.row_id)
                .or_default()
                .insert(delete_snapshot);
        }
        Ok(Self { deleted_at })
    }

    pub(super) fn hides_row_version(&self, row_id: u64, begin_snapshot: u64) -> bool {
        self.end_snapshot_for(row_id, begin_snapshot).is_some()
    }

    pub(super) fn end_snapshot_for(&self, row_id: u64, begin_snapshot: u64) -> Option<u64> {
        self.deleted_at.get(&row_id).and_then(|delete_snapshots| {
            delete_snapshots
                .range((begin_snapshot + 1)..)
                .next()
                .copied()
        })
    }
}

pub(super) struct RawInlineDeletionIndex {
    deleted_at: BTreeMap<u64, BTreeSet<CatalogOrderId>>,
}

impl RawInlineDeletionIndex {
    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let start_order =
            CatalogOrderId::from_bytes(snapshot_order.kind(), [0; CatalogOrderId::LEN]);
        let mut deleted_at = BTreeMap::<u64, BTreeSet<CatalogOrderId>>::new();
        for change in list_inline_deleted_row_changes_for_schema(
            kv,
            catalog,
            table_id,
            schema_id,
            start_order,
            snapshot_order,
        )? {
            deleted_at
                .entry(change.row_id)
                .or_default()
                .insert(change.order);
        }
        Ok(Self { deleted_at })
    }

    pub(super) fn hides_row_version(&self, row_id: u64, begin_order: CatalogOrderId) -> bool {
        self.deleted_at.get(&row_id).is_some_and(|delete_orders| {
            delete_orders
                .range((Bound::Excluded(begin_order), Bound::Unbounded))
                .next()
                .is_some()
        })
    }

    pub(super) fn has_deletions(&self) -> bool {
        !self.deleted_at.is_empty()
    }
}
