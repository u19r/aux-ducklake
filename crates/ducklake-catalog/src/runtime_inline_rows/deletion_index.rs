use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use super::missing_snapshot_order;
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, SchemaId,
    inline_change_feed::list_inline_deleted_row_changes_for_schema,
};

#[derive(Clone)]
pub(super) struct InlineDeletionIndex {
    deleted_at: BTreeMap<u64, BTreeSet<u64>>,
}

impl InlineDeletionIndex {
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

    pub(super) fn orders(&self) -> impl Iterator<Item = CatalogOrderId> + '_ {
        self.deleted_at
            .values()
            .flat_map(|orders| orders.iter().copied())
    }

    pub(super) fn resolve_sequences(
        self,
        sequences: &BTreeMap<CatalogOrderId, u64>,
        read_snapshot: u64,
    ) -> CatalogResult<InlineDeletionIndex> {
        let mut deleted_at = BTreeMap::new();
        for (row_id, orders) in self.deleted_at {
            let mut snapshots = BTreeSet::new();
            for order in orders {
                let sequence = sequences
                    .get(&order)
                    .copied()
                    .ok_or_else(|| missing_snapshot_order(order))?;
                if sequence <= read_snapshot {
                    snapshots.insert(sequence);
                }
            }
            deleted_at.insert(row_id, snapshots);
        }
        Ok(InlineDeletionIndex { deleted_at })
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
