#[cfg(not(test))]
use std::sync::OnceLock;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    ops::Bound,
};

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, SchemaId,
    inline_change_feed::list_inline_deleted_row_changes_for_schema,
};

use crate::runtime_inline_rows::*;

pub(crate) fn inline_flush_delete_positions_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: InlineFlushDeletePositionsPayload,
) -> CatalogResult<Vec<u8>> {
    let snapshot = inline_read_snapshot(
        kv,
        catalog,
        Some(payload.snapshot),
        InlineStatsRequest::Conservative,
    )?
    .ok_or_else(|| missing_snapshot(payload.snapshot.public_id()))?;
    let context = InlineTableSnapshotContext::load_for_table_name(
        kv,
        catalog,
        &payload.table_name,
        snapshot,
    )?;
    let payloads = context.payloads(kv, catalog, false)?;
    let render_context = context.render_context(kv, catalog)?;
    let mut rows = Vec::new();
    for inline_payload in payloads {
        let begin_snapshot = render_context
            .snapshot_sequence(inline_payload.begin_order)
            .ok_or_else(|| missing_snapshot_order(inline_payload.begin_order))?;
        collect_inline_flush_rows(
            &inline_payload.payload,
            begin_snapshot,
            &render_context.deleted_rows,
            &mut rows,
        )?;
    }
    let filter = InlineFlushPartitionFilter::parse(
        payload.partition_filter.as_deref(),
        &context.schema_table,
    )?;
    rows.retain(|row| filter.matches(row));
    let file_order =
        InlineFlushFileOrder::parse(payload.file_order.as_deref(), &context.schema_table)?;
    rows.sort_by(|left, right| file_order.compare(left, right));
    let position_start = payload.position_start.unwrap_or(0);
    let position_end = payload.position_end.unwrap_or(u64::MAX);
    let delete_count = rows
        .iter()
        .enumerate()
        .filter(|(position, row)| {
            row.end_snapshot.is_some()
                && position_in_flush_window(*position, position_start, position_end)
        })
        .count();
    if delete_count == 0 {
        return Ok(b"delete_position_count=0\n".to_vec());
    }

    let mut out = format!("delete_position_count={delete_count}\n");
    for (position, row) in rows.iter().enumerate() {
        if !position_in_flush_window(position, position_start, position_end) {
            continue;
        }
        if let Some(end_snapshot) = row.end_snapshot {
            writeln!(out, "delete_position\t{end_snapshot}\t{position}").map_err(|error| {
                crate::CatalogError::Decode(format!(
                    "failed to render inline delete position: {error}"
                ))
            })?;
        }
    }
    Ok(out.into_bytes())
}

pub(super) fn position_in_flush_window(position: usize, start: u64, end: u64) -> bool {
    let Ok(position) = u64::try_from(position) else {
        return false;
    };
    start <= position && position < end
}

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
