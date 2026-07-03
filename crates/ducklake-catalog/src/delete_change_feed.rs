#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogId, CatalogResult, DataFileChangeKind, DataFileId, DataFileRow, DeleteFileChange,
    DeleteFileId, DeleteFileRow, DeleteScanFile, OrderedCatalogKv, RangeDirection, TableId,
    data_file_changes::list_data_file_changes,
    ids::{CatalogOrderId, CatalogOrderKind, incomplete_fdb_order},
    inline_data::list_inline_file_deletions_between,
    keys::{
        data_file_key, delete_file_key, delete_file_timeline_prefix, delete_file_timeline_scan_end,
        order_delete_file_change_prefix, order_delete_file_change_scan_end,
        order_delete_file_change_scan_start,
    },
};
use std::collections::BTreeMap;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
static ORDER_DELETE_FILE_CHANGE_CACHE: OnceLock<
    BoundedCache<OrderDeleteFileChangeCacheKey, Vec<DeleteFileChange>>,
> = OnceLock::new();

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OrderDeleteFileChangeCacheKey {
    catalog: CatalogId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
}

#[cfg(not(test))]
pub(crate) fn invalidate_delete_change_feed_context(catalog: CatalogId) {
    if let Some(cache) = ORDER_DELETE_FILE_CHANGE_CACHE.get() {
        cache.retain(|key, _| key.catalog != catalog);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn invalidate_delete_change_feed_context(_catalog: CatalogId) {}

pub fn list_table_deletion_scan_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteScanFile>> {
    if end_order < start_order {
        return Err(crate::CatalogError::InvalidMutation(
            "deletion change-feed end order cannot precede start order".to_owned(),
        ));
    }
    let mut scans = list_partial_delete_scan_files(kv, catalog, table_id, start_order, end_order)?;
    scans.extend(list_full_delete_scan_files(
        kv,
        catalog,
        table_id,
        start_order,
        end_order,
    )?);
    merge_inline_file_deletion_scans(kv, catalog, table_id, start_order, end_order, &mut scans)?;
    scans.sort_by_key(|scan| {
        (
            scan.snapshot_order,
            scan.data_file.data_file_id.0,
            scan.delete_file
                .as_ref()
                .map_or(0, |delete_file| delete_file.delete_file_id.0),
        )
    });
    Ok(scans)
}

fn merge_inline_file_deletion_scans(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
    scans: &mut Vec<DeleteScanFile>,
) -> CatalogResult<()> {
    let mut groups = BTreeMap::<(CatalogOrderId, DataFileId), BTreeMap<u64, CatalogOrderId>>::new();
    for row in list_inline_file_deletions_between(kv, catalog, table_id, start_order, end_order)? {
        groups
            .entry((row.validity.begin_order, row.data_file_id))
            .or_default()
            .insert(row.row_id, row.validity.begin_order);
    }
    for ((snapshot_order, data_file_id), inline_file_deletions) in groups {
        if let Some(scan) = scans.iter_mut().find(|scan| {
            scan.snapshot_order == snapshot_order && scan.data_file.data_file_id == data_file_id
        }) {
            scan.inline_file_deletions.extend(inline_file_deletions);
            continue;
        }
        let data_file = load_data_file(kv, catalog, data_file_id)?;
        if data_file.table_id != table_id {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "inline file deletions for data file {} belong to table {} but were indexed for table {}",
                data_file_id.0, data_file.table_id.0, table_id.0
            )));
        }
        scans.push(DeleteScanFile::inline(
            data_file,
            snapshot_order,
            inline_file_deletions,
        ));
    }
    Ok(())
}

fn list_partial_delete_scan_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteScanFile>> {
    let mut scans = Vec::new();
    for change in list_delete_file_changes(kv, catalog, table_id, start_order, end_order)? {
        let delete_file = load_delete_file(kv, catalog, change.delete_file_id)?;
        let data_file = load_data_file(kv, catalog, delete_file.data_file_id)?;
        if data_file.table_id != table_id || change.table_id != table_id {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "delete file {} belongs to table {} but was indexed for table {}",
                delete_file.delete_file_id.0, data_file.table_id.0, table_id.0
            )));
        }
        let previous_delete_file = load_previous_delete_file_before(
            kv,
            catalog,
            data_file.data_file_id,
            change.order,
            Some(delete_file.delete_file_id),
        )?;
        scans.push(DeleteScanFile::partial(
            data_file,
            delete_file,
            previous_delete_file,
            change.order,
        ));
    }
    Ok(scans)
}

fn list_full_delete_scan_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteScanFile>> {
    let mut scans = Vec::new();
    for change in list_data_file_changes(kv, catalog, table_id, start_order, end_order)? {
        if change.kind != DataFileChangeKind::Removed {
            continue;
        }
        let data_file = load_data_file(kv, catalog, change.data_file_id)?;
        let previous_delete_file =
            load_previous_delete_file_before(kv, catalog, change.data_file_id, change.order, None)?;
        scans.push(DeleteScanFile::full(
            data_file,
            previous_delete_file,
            change.order,
        ));
    }
    Ok(scans)
}

fn list_delete_file_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteFileChange>> {
    Ok(
        list_order_delete_file_changes_between(kv, catalog, start_order, end_order)?
            .into_iter()
            .filter(|change| change.table_id == table_id)
            .collect(),
    )
}

fn list_order_delete_file_changes_between(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteFileChange>> {
    #[cfg(not(test))]
    {
        let key = OrderDeleteFileChangeCacheKey {
            catalog,
            start_order,
            end_order,
        };
        let cache = static_bounded_cache(&ORDER_DELETE_FILE_CHANGE_CACHE, 512);
        if let Some(changes) = cache.get(key) {
            return Ok(changes);
        }
        let changes =
            list_order_delete_file_changes_between_uncached(kv, catalog, start_order, end_order)?;
        cache.insert(key, changes.clone());
        Ok(changes)
    }
    #[cfg(test)]
    {
        list_order_delete_file_changes_between_uncached(kv, catalog, start_order, end_order)
    }
}

fn list_order_delete_file_changes_between_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DeleteFileChange>> {
    let prefix = order_delete_file_change_prefix(catalog);
    let order_kind = if start_order.kind() == end_order.kind() {
        end_order.kind()
    } else {
        CatalogOrderKind::UuidV7
    };
    let mut changes = Vec::new();
    for item in kv.scan_range(
        &order_delete_file_change_scan_start(catalog, start_order),
        &order_delete_file_change_scan_end(catalog, end_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        changes.push(decode_order_delete_file_change_key(
            &prefix, &item.key, order_kind,
        )?);
    }
    Ok(changes)
}

fn load_previous_delete_file_before(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    order: CatalogOrderId,
    skip_delete_file_id: Option<DeleteFileId>,
) -> CatalogResult<Option<DeleteFileRow>> {
    for item in kv.scan_range(
        &delete_file_timeline_prefix(catalog, data_file_id),
        &delete_file_timeline_scan_end(catalog, data_file_id, order),
        RangeDirection::Reverse,
        usize::MAX,
    )? {
        let timeline_order =
            decode_delete_file_timeline_order(catalog, data_file_id, &item.key, order.kind())?;
        let mut row = match DeleteFileRow::decode(&item.value) {
            Ok(row) => row,
            Err(_) => load_delete_file(kv, catalog, decode_delete_file_id(&item.value)?)?,
        };
        if row.validity.begin_order == incomplete_fdb_order() {
            row.validity.begin_order = timeline_order;
        }
        if Some(row.delete_file_id) == skip_delete_file_id {
            continue;
        }
        if order <= timeline_order {
            continue;
        }
        if !delete_file_existed_before_change(&row, order) {
            continue;
        }
        return Ok(Some(row));
    }
    Ok(None)
}

fn delete_file_existed_before_change(row: &DeleteFileRow, order: CatalogOrderId) -> bool {
    row.validity
        .end_order
        .is_some_and(|end_order| end_order <= order)
        || row.validity.begin_order < order
}

fn load_data_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
) -> CatalogResult<DataFileRow> {
    let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
        return Err(crate::CatalogError::NotFound("data file"));
    };
    DataFileRow::decode(&value)
}

fn load_delete_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> CatalogResult<DeleteFileRow> {
    let Some(value) = kv.get(&delete_file_key(catalog, delete_file_id))? else {
        return Err(crate::CatalogError::NotFound("delete file"));
    };
    DeleteFileRow::decode(&value)
}

fn decode_delete_file_id(bytes: &[u8]) -> CatalogResult<DeleteFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "delete file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DeleteFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("delete file id pointer is truncated".to_owned()),
    )?)))
}

fn decode_delete_file_timeline_order(
    catalog: CatalogId,
    data_file_id: DataFileId,
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = delete_file_timeline_prefix(catalog, data_file_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(crate::CatalogError::InvalidKey(
            "delete-file timeline key has wrong prefix".to_owned(),
        ));
    };
    if tail.len() != CatalogOrderId::LEN + 1 + 8 {
        return Err(crate::CatalogError::InvalidKey(format!(
            "delete-file timeline key tail must be {} bytes, got {}",
            CatalogOrderId::LEN + 1 + 8,
            tail.len()
        )));
    }
    if tail[CatalogOrderId::LEN] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "delete-file timeline key separator is invalid".to_owned(),
        ));
    }
    Ok(CatalogOrderId::from_bytes(
        order_kind,
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("delete-file timeline order is truncated".to_owned())
        })?,
    ))
}

fn decode_order_delete_file_change_key(
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<DeleteFileChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(crate::CatalogError::InvalidKey(
            "order delete-file change key has wrong prefix".to_owned(),
        ));
    };
    let expected_len = CatalogOrderId::LEN + 1 + 8 + 1 + 8;
    if tail.len() != expected_len {
        return Err(crate::CatalogError::InvalidKey(format!(
            "order delete-file change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    let order_end = CatalogOrderId::LEN;
    let table_start = order_end + 1;
    let table_end = table_start + 8;
    let delete_file_id_start = table_end + 1;
    if tail[order_end] != b'/' || tail[table_end] != b'/' {
        return Err(crate::CatalogError::InvalidKey(
            "order delete-file change key separator is invalid".to_owned(),
        ));
    }
    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("delete-file change order is truncated".to_owned())
        })?,
    );
    let table_id = TableId(u64::from_be_bytes(
        tail[table_start..table_end].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("delete-file change table id is truncated".to_owned())
        })?,
    ));
    let delete_file_id = DeleteFileId(u64::from_be_bytes(
        tail[delete_file_id_start..expected_len]
            .try_into()
            .map_err(|_| {
                crate::CatalogError::InvalidKey("delete file id is truncated".to_owned())
            })?,
    ));
    Ok(DeleteFileChange::new(table_id, order, delete_file_id))
}
