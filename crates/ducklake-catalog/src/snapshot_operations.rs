use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::{
    CatalogCacheNamespace,
    bounded_cache::{BoundedCache, static_bounded_cache},
};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, KvBatch,
    OrderedCatalogKv, RangeDirection, TableId,
    keys::{KeyFamily, family_prefix},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SnapshotOperationKind {
    InlineFlush,
    RewriteDelete,
}

impl SnapshotOperationKind {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::InlineFlush => b'F',
            Self::RewriteDelete => b'R',
        }
    }

    fn from_code(code: u8) -> CatalogResult<Self> {
        match code {
            b'F' => Ok(Self::InlineFlush),
            b'R' => Ok(Self::RewriteDelete),
            _ => Err(CatalogError::InvalidKey(format!(
                "unknown snapshot operation byte 0x{code:02x}"
            ))),
        }
    }
}

#[cfg(feature = "foundationdb")]
pub(crate) fn snapshot_operation_order_offset(catalog: CatalogId) -> usize {
    snapshot_operation_prefix(catalog).len()
}

pub(crate) fn snapshot_operation_key(
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
    table_id: TableId,
) -> Vec<u8> {
    let mut key = snapshot_operation_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key.push(b'/');
    key.push(kind.code());
    key.push(b'/');
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key
}

pub(crate) fn snapshot_operation_prefix(catalog: CatalogId) -> Vec<u8> {
    family_prefix(catalog, KeyFamily::SnapshotOperation)
}

pub(crate) fn snapshot_operation_scan_start(catalog: CatalogId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = snapshot_operation_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key
}

pub(crate) fn snapshot_operation_scan_end(catalog: CatalogId, order: CatalogOrderId) -> Vec<u8> {
    let mut key = snapshot_operation_prefix(catalog);
    key.extend_from_slice(&order.as_bytes());
    key.push(0xff);
    key
}

pub(crate) fn stage_snapshot_operation(
    batch: &mut KvBatch,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
    table_id: TableId,
) {
    batch.put(
        snapshot_operation_key(catalog, order, kind, table_id),
        Vec::new(),
    );
}

pub(crate) fn snapshot_operation_table_ids_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
) -> CatalogResult<BTreeSet<TableId>> {
    #[cfg(not(test))]
    {
        let key = SnapshotOperationCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            order,
            kind,
        };
        let cache = snapshot_operation_cache();
        if let Some(table_ids) = cache.get(key) {
            return Ok(table_ids);
        }
        let table_ids = snapshot_operation_table_ids_at_uncached(kv, catalog, order, kind)?;
        cache.insert(key, table_ids.clone());
        Ok(table_ids)
    }
    #[cfg(test)]
    {
        snapshot_operation_table_ids_at_uncached(kv, catalog, order, kind)
    }
}

pub(crate) fn snapshot_operations_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order_kind: CatalogOrderKind,
) -> CatalogResult<BTreeMap<(CatalogOrderId, SnapshotOperationKind), BTreeSet<TableId>>> {
    let prefix = snapshot_operation_prefix(catalog);
    let mut operations =
        BTreeMap::<(CatalogOrderId, SnapshotOperationKind), BTreeSet<TableId>>::new();
    for item in kv.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)? {
        let (order, kind, table_id) =
            decode_snapshot_operation_key(&prefix, &item.key, order_kind)?;
        operations
            .entry((order, kind))
            .or_default()
            .insert(table_id);
    }
    Ok(operations)
}

fn snapshot_operation_table_ids_at_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
) -> CatalogResult<BTreeSet<TableId>> {
    let prefix = snapshot_operation_prefix(catalog);
    let mut table_ids = BTreeSet::new();
    for item in kv.scan_range(
        &snapshot_operation_scan_start(catalog, order),
        &snapshot_operation_scan_end(catalog, order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let (operation_order, operation_kind, table_id) =
            decode_snapshot_operation_key(&prefix, &item.key, order.kind())?;
        if operation_order == order && operation_kind == kind {
            table_ids.insert(table_id);
        }
    }
    Ok(table_ids)
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SnapshotOperationCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: SnapshotOperationKind,
}

#[cfg(not(test))]
static SNAPSHOT_OPERATION_CACHE: OnceLock<
    BoundedCache<SnapshotOperationCacheKey, BTreeSet<TableId>>,
> = OnceLock::new();

#[cfg(not(test))]
fn snapshot_operation_cache() -> &'static BoundedCache<SnapshotOperationCacheKey, BTreeSet<TableId>>
{
    static_bounded_cache(&SNAPSHOT_OPERATION_CACHE, 256)
}

fn decode_snapshot_operation_key(
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<(CatalogOrderId, SnapshotOperationKind, TableId)> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(CatalogError::InvalidKey(
            "snapshot operation key has wrong prefix".to_owned(),
        ));
    };
    let order_end = CatalogOrderId::LEN;
    let kind_start = order_end + 1;
    let kind_end = kind_start + 1;
    let table_start = kind_end + 1;
    let expected_len = table_start + 8;
    if tail.len() != expected_len {
        return Err(CatalogError::InvalidKey(format!(
            "snapshot operation key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    if tail[order_end] != b'/' || tail[kind_end] != b'/' {
        return Err(CatalogError::InvalidKey(
            "snapshot operation key separators are invalid".to_owned(),
        ));
    }
    let mut order_bytes = [0; CatalogOrderId::LEN];
    order_bytes.copy_from_slice(&tail[..order_end]);
    let mut table_bytes = [0; 8];
    table_bytes.copy_from_slice(&tail[table_start..expected_len]);
    Ok((
        CatalogOrderId::from_bytes(order_kind, order_bytes),
        SnapshotOperationKind::from_code(tail[kind_start])?,
        TableId(u64::from_be_bytes(table_bytes)),
    ))
}
