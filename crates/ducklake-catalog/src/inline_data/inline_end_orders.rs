use super::inline_tables::{InlineTableChunkRow, record_inline_tables_stage};
#[cfg(not(test))]
use crate::{
    CatalogCacheNamespace,
    bounded_cache::{BoundedCache, static_bounded_cache},
};
use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, OrderedCatalogKv, RangeDirection,
    SchemaId, TableId,
    keys::{KeyFamily, family_prefix},
    runtime_metrics::RuntimeMetricStage,
};
use std::collections::BTreeMap;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
pub(super) fn invalidate_inline_end_orders(catalog: CatalogId) {
    if let Some(cache) = INLINE_TABLE_END_ORDERS_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineTableEndOrdersCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

/// Per-row end markers plus range watermarks for one inline table/schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InlineTableEndOrders {
    rows: BTreeMap<CatalogOrderId, CatalogOrderId>,
    watermarks: Vec<InlineFlushWatermark>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineFlushWatermark {
    max_begin_order: CatalogOrderId,
    end_order: CatalogOrderId,
}

impl InlineTableEndOrders {
    /// Earliest end order that hides a chunk beginning at `begin_order`.
    pub(crate) fn end_order_for(&self, begin_order: CatalogOrderId) -> Option<CatalogOrderId> {
        let row = self.rows.get(&begin_order).copied();
        let watermark = self
            .watermarks
            .iter()
            .filter(|watermark| begin_order <= watermark.max_begin_order)
            .map(|watermark| watermark.end_order)
            .min();
        match (row, watermark) {
            (Some(row), Some(watermark)) => Some(row.min(watermark)),
            (Some(row), None) => Some(row),
            (None, Some(watermark)) => Some(watermark),
            (None, None) => None,
        }
    }
}

#[cfg(not(test))]
static INLINE_TABLE_END_ORDERS_CACHE: OnceLock<
    BoundedCache<InlineTableEndOrdersCacheKey, InlineTableEndOrders>,
> = OnceLock::new();

#[cfg(not(test))]
fn inline_table_end_orders_cache()
-> &'static BoundedCache<InlineTableEndOrdersCacheKey, InlineTableEndOrders> {
    static_bounded_cache(&INLINE_TABLE_END_ORDERS_CACHE, 256)
}

pub(crate) fn inline_table_end_orders_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<InlineTableEndOrders> {
    inline_table_end_orders(kv, catalog, table_id, schema_id, snapshot_order, true)
}

pub(crate) fn inline_table_all_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    order_kind_source: CatalogOrderId,
) -> CatalogResult<InlineTableEndOrders> {
    inline_table_end_orders(kv, catalog, table_id, schema_id, order_kind_source, false)
}

pub(super) fn inline_table_end_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    only_ended_at_or_before_snapshot: bool,
) -> CatalogResult<InlineTableEndOrders> {
    // Later processes can add end markers beyond this source snapshot. Only
    // snapshot-bounded history is immutable and safe to cache by that order.
    if !only_ended_at_or_before_snapshot {
        return inline_table_end_orders_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            false,
        );
    }
    #[cfg(test)]
    {
        inline_table_end_orders_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            only_ended_at_or_before_snapshot,
        )
    }
    #[cfg(not(test))]
    {
        let key = InlineTableEndOrdersCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            table_id,
            schema_id,
            snapshot_order,
        };
        let cache = inline_table_end_orders_cache();
        if let Some(end_orders) = cache.get(key) {
            return Ok(end_orders);
        }
        let end_orders = inline_table_end_orders_uncached(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            only_ended_at_or_before_snapshot,
        )?;
        cache.insert(key, end_orders.clone());
        Ok(end_orders)
    }
}

fn inline_table_end_orders_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
    only_ended_at_or_before_snapshot: bool,
) -> CatalogResult<InlineTableEndOrders> {
    let started = RuntimeMetricStage::start();
    let mut prefix = family_prefix(catalog, KeyFamily::EndOrder);
    prefix.extend_from_slice(&table_id.0.to_be_bytes());
    prefix.push(b'/');
    let mut end_orders = InlineTableEndOrders::default();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let Some((end_order, marker_schema_id, begin_order)) =
            decode_inline_end_marker(&prefix, &item.key, snapshot_order)?
        else {
            continue;
        };
        if marker_schema_id == schema_id
            && (!only_ended_at_or_before_snapshot || end_order <= snapshot_order)
        {
            end_orders.rows.insert(begin_order, end_order);
        }
    }

    let watermark_family = family_prefix(catalog, KeyFamily::InlineFlushWatermark);
    let mut watermark_prefix = watermark_family.clone();
    watermark_prefix.extend_from_slice(&table_id.0.to_be_bytes());
    watermark_prefix.push(b'/');
    for item in kv.scan_prefix(&watermark_prefix, RangeDirection::Forward, usize::MAX)? {
        let Some((_table_id, end_order, marker_schema_id, max_begin_order)) =
            decode_inline_flush_watermark(&watermark_family, &item.key, snapshot_order)?
        else {
            continue;
        };
        if marker_schema_id == schema_id
            && (!only_ended_at_or_before_snapshot || end_order <= snapshot_order)
        {
            end_orders.watermarks.push(InlineFlushWatermark {
                max_begin_order,
                end_order,
            });
        }
    }
    record_inline_tables_stage("method.inline_tables.inline_table_end_orders", started);
    Ok(end_orders)
}

pub(crate) fn inline_chunk_visible_at(
    row: &InlineTableChunkRow,
    end_orders: &InlineTableEndOrders,
    snapshot_order: CatalogOrderId,
) -> bool {
    let end_order = row
        .validity
        .end_order
        .or_else(|| end_orders.end_order_for(row.validity.begin_order));
    row.validity.begin_order <= snapshot_order
        && end_order.is_none_or(|end_order| snapshot_order < end_order)
}

/// Decode one flush watermark key. The key tail is
/// `end_order / schema_id / max_begin_order`.
pub(crate) fn decode_inline_flush_watermark(
    prefix: &[u8],
    key: &[u8],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<(TableId, CatalogOrderId, SchemaId, CatalogOrderId)>> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Ok(None);
    };
    let expected_len = 8 + 1 + CatalogOrderId::LEN + 1 + 8 + 1 + CatalogOrderId::LEN;
    if tail.len() != expected_len || tail[8] != b'/' {
        return Ok(None);
    }
    let table_id = TableId(u64::from_be_bytes(tail[..8].try_into().map_err(|_| {
        CatalogError::InvalidKey("inline watermark table is truncated".to_owned())
    })?));
    let order_start = 9;
    let order_end = order_start + CatalogOrderId::LEN;
    if tail[order_end] != b'/' || tail[order_end + 1 + 8] != b'/' {
        return Ok(None);
    }
    let end_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[order_start..order_end].try_into().map_err(|_| {
            CatalogError::InvalidKey("inline watermark order is truncated".to_owned())
        })?,
    );
    let schema_start = order_end + 1;
    let schema_id = SchemaId(u64::from_be_bytes(
        tail[schema_start..schema_start + 8]
            .try_into()
            .map_err(|_| {
                CatalogError::InvalidKey("inline watermark schema is truncated".to_owned())
            })?,
    ));
    let max_start = schema_start + 8 + 1;
    let max_begin_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[max_start..].try_into().map_err(|_| {
            CatalogError::InvalidKey("inline watermark begin is truncated".to_owned())
        })?,
    );
    Ok(Some((table_id, end_order, schema_id, max_begin_order)))
}

pub(crate) fn decode_inline_end_marker(
    prefix: &[u8],
    key: &[u8],
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<(CatalogOrderId, SchemaId, CatalogOrderId)>> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Ok(None);
    };
    let expected_len = CatalogOrderId::LEN + 1 + 2 + 8 + 1 + CatalogOrderId::LEN;
    if tail.len() != expected_len {
        return Ok(None);
    }
    let object_start = CatalogOrderId::LEN + 1;
    if tail[CatalogOrderId::LEN] != b'/'
        || tail[object_start] != b'i'
        || tail[object_start + 1] != b'/'
        || tail[object_start + 10] != b'/'
    {
        return Ok(None);
    }
    let end_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[..CatalogOrderId::LEN]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline end order is truncated".to_owned()))?,
    );
    let schema_start = object_start + 2;
    let begin_start = schema_start + 8 + 1;
    let schema_id = SchemaId(u64::from_be_bytes(
        tail[schema_start..schema_start + 8]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline end schema is truncated".to_owned()))?,
    ));
    let begin_order = CatalogOrderId::from_bytes(
        snapshot_order.kind(),
        tail[begin_start..]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("inline begin order is truncated".to_owned()))?,
    );
    Ok(Some((end_order, schema_id, begin_order)))
}
