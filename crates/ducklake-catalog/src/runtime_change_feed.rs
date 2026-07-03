use crate::{
    CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataFileChangeKind, DataFileId,
    DataFileRow, DeleteFileRow, DeleteScanFile, DuckLakeSnapshotId, OrderedCatalogKv,
    RangeDirection, TableId,
    inline_data::inline_table_flushes_ending_at,
    keys::{data_file_begin_prefix, data_file_begin_scan_end, data_file_key},
    list_data_file_changes, list_data_files, list_snapshots, list_table_deletion_scan_files,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot},
    runtime_snapshots::{public_snapshot_order_span, public_snapshot_sequences_by_order},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
pub(crate) struct ChangeFeedPayload {
    pub(crate) table_id: TableId,
    pub(crate) start_snapshot: ChangeFeedStartSnapshot,
    pub(crate) end_snapshot: ChangeFeedEndSnapshot,
}

pub(crate) fn data_file_changes_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    let (start_order, end_order) = snapshot_orders(kv, catalog, payload)?;
    let snapshot_sequences = snapshot_ids_by_order(kv, catalog)?;
    let start_snapshot_id = payload.start_snapshot.public_id().0;
    let end_snapshot_id = payload.end_snapshot.public_id().0;
    let mut rows = Vec::new();
    for file in insertion_files(kv, catalog, payload.table_id, start_order, end_order)? {
        let sequence = snapshot_sequences
            .get(&file.validity.begin_order)
            .copied()
            .ok_or_else(|| missing_snapshot_order(file.validity.begin_order))?;
        let max_partial_sequence = file
            .max_partial_order
            .map(|order| {
                snapshot_sequences
                    .get(&order)
                    .copied()
                    .ok_or_else(|| missing_snapshot_order(order))
            })
            .transpose()?;
        let snapshot_filter_min = (sequence < start_snapshot_id).then_some(start_snapshot_id);
        let snapshot_filter_max = max_partial_sequence
            .and_then(|sequence| (sequence > end_snapshot_id).then_some(end_snapshot_id));
        let mut row = format!(
            "change_file\tadded\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            sequence,
            file.data_file_id.0,
            file.table_id.0,
            file.path,
            file.record_count,
            file.file_size_bytes,
            file.row_id_start
        );
        if let Some(mapping_id) = file.mapping_id {
            row.push('\t');
            row.push_str(&mapping_id.to_string());
        }
        if let Some(max_partial_sequence) = max_partial_sequence {
            if file.mapping_id.is_none() {
                row.push('\t');
            }
            row.push('\t');
            row.push_str(&max_partial_sequence.to_string());
        }
        if snapshot_filter_min.is_some() || snapshot_filter_max.is_some() {
            if file.mapping_id.is_none() && max_partial_sequence.is_none() {
                row.push('\t');
            }
            if max_partial_sequence.is_none() {
                row.push('\t');
            }
            row.push('\t');
            if let Some(snapshot_filter_min) = snapshot_filter_min {
                row.push_str(&snapshot_filter_min.to_string());
            }
            row.push('\t');
            if let Some(snapshot_filter_max) = snapshot_filter_max {
                row.push_str(&snapshot_filter_max.to_string());
            }
        }
        row.push('\n');
        rows.push(row);
    }
    let mut out = format!("change_count={}\n", rows.len());
    for row in rows {
        out.push_str(&row);
    }
    Ok(out.into_bytes())
}

pub fn insertion_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    if end_order < start_order {
        return Err(crate::CatalogError::InvalidMutation(
            "change-feed end order cannot precede start order".to_owned(),
        ));
    }
    let mut rows = BTreeMap::new();
    for item in kv.scan_range(
        &data_file_begin_prefix(catalog, table_id),
        &data_file_begin_scan_end(catalog, table_id, end_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_begin_index_item(kv, catalog, table_id, &item.key, &item.value)?;
        if row.max_partial_order.is_none()
            && is_flushed_inline_file(kv, catalog, table_id, row.validity.begin_order)?
        {
            continue;
        }
        if insertion_file_overlaps_window(&row, start_order, end_order) {
            rows.insert(row.data_file_id, row);
        }
    }
    for row in list_data_files(kv, catalog)?
        .into_iter()
        .filter(|row| row.table_id == table_id && row.max_partial_order.is_some())
    {
        if insertion_file_overlaps_window(&row, start_order, end_order) {
            rows.insert(row.data_file_id, row);
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|file| (file.validity.begin_order, file.data_file_id));
    Ok(without_compacted_sources(rows))
}

fn data_file_from_begin_index_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(mut row) => {
            row.validity.begin_order = data_file_begin_order_from_key(
                catalog,
                table_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            Ok(row)
        }
        Err(_) => {
            let data_file_id = decode_data_file_id(value)?;
            let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
                return Err(crate::CatalogError::NotFound("data file"));
            };
            DataFileRow::decode(&value)
        }
    }
}

fn data_file_begin_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = data_file_begin_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(crate::CatalogError::InvalidKey(
            "data file begin key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(crate::CatalogError::InvalidKey(format!(
            "data file begin key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            crate::CatalogError::InvalidKey("data file begin order is truncated".to_owned())
        })?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

fn is_flushed_inline_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
) -> CatalogResult<bool> {
    Ok(inline_table_flushes_ending_at(kv, catalog, order)?.contains(&table_id))
}

fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    if bytes.len() != 8 {
        return Err(crate::CatalogError::Decode(format!(
            "data file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DataFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| crate::CatalogError::Decode("data file id pointer is truncated".to_owned()),
    )?)))
}

fn insertion_file_overlaps_window(
    file: &DataFileRow,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> bool {
    let span_start = file
        .max_partial_order
        .map_or(file.validity.begin_order, |max_partial_order| {
            file.validity.begin_order.min(max_partial_order)
        });
    let span_end = file
        .max_partial_order
        .map_or(file.validity.begin_order, |max_partial_order| {
            file.validity.begin_order.max(max_partial_order)
        });
    span_start <= end_order && span_end >= start_order
}

fn without_compacted_sources(rows: Vec<DataFileRow>) -> Vec<DataFileRow> {
    let compacted_ranges = rows
        .iter()
        .filter_map(|file| {
            if file.validity.end_order.is_some() {
                return None;
            }
            file.max_partial_order
                .map(|max_order| (file.data_file_id, file.validity.begin_order, max_order))
        })
        .collect::<Vec<_>>();
    rows.into_iter()
        .filter(|file| {
            !compacted_ranges
                .iter()
                .any(|(replacement_id, begin_order, max_order)| {
                    *replacement_id != file.data_file_id
                        && *begin_order <= file.validity.begin_order
                        && file.validity.begin_order <= *max_order
                })
        })
        .collect()
}

pub fn user_visible_data_file_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<crate::DataFileChange>> {
    let changes = list_data_file_changes(kv, catalog, table_id, start_order, end_order)?;
    let mut added_orders = BTreeSet::new();
    let mut removed_orders = BTreeSet::new();
    for change in &changes {
        match change.kind {
            DataFileChangeKind::Added => {
                added_orders.insert(change.order);
            }
            DataFileChangeKind::Removed => {
                removed_orders.insert(change.order);
            }
        }
    }
    let rewrite_orders = added_orders
        .intersection(&removed_orders)
        .copied()
        .collect::<BTreeSet<_>>();
    Ok(changes
        .into_iter()
        .filter(|change| !rewrite_orders.contains(&change.order))
        .collect())
}

pub(crate) fn table_deletions_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ChangeFeedPayload,
) -> CatalogResult<Vec<u8>> {
    let (start_order, end_order) = snapshot_orders(kv, catalog, payload)?;
    let snapshot_sequences = snapshot_ids_by_order(kv, catalog)?;
    let scans =
        list_table_deletion_scan_files(kv, catalog, payload.table_id, start_order, end_order)?;
    let mut out = format!("deletion_scan_count={}\n", scans.len());
    for scan in scans {
        push_delete_scan(&mut out, &snapshot_sequences, scan)?;
    }
    Ok(out.into_bytes())
}

fn snapshot_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: ChangeFeedPayload,
) -> CatalogResult<(CatalogOrderId, CatalogOrderId)> {
    let start_order = change_feed_start_order(kv, catalog, payload.start_snapshot)?;
    let end_order = change_feed_end_order(kv, catalog, payload.end_snapshot)?;
    Ok((start_order, end_order))
}

fn change_feed_start_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedStartSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((start_order, _)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(start_order);
    }
    public_snapshot_order_span(kv, catalog, snapshot_id)?
        .map(|(start, _)| start)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

fn change_feed_end_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedEndSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((_, end_order)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(end_order);
    }
    public_snapshot_order_span(kv, catalog, snapshot_id)?
        .map(|(_, end)| end)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

fn ducklake_snapshot_order_span(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<(CatalogOrderId, CatalogOrderId)>> {
    let orders = list_snapshots(kv, catalog)?
        .into_iter()
        .filter(|snapshot| snapshot.sequence.0 == snapshot_id.0)
        .map(|snapshot| snapshot.order)
        .collect::<Vec<_>>();
    let Some(start) = orders.iter().min().copied() else {
        return Ok(None);
    };
    let end = orders.iter().max().copied().unwrap_or(start);
    Ok(Some((start, end)))
}

fn push_delete_scan(
    out: &mut String,
    snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    scan: DeleteScanFile,
) -> CatalogResult<()> {
    let delete_file = display_delete_file(scan.delete_file.as_ref());
    let previous_delete_file = display_delete_file(scan.previous_delete_file.as_ref());
    let sequence = snapshot_sequences
        .get(&scan.snapshot_order)
        .copied()
        .ok_or_else(|| missing_snapshot_order(scan.snapshot_order))?;
    let inline_file_deletions =
        display_inline_file_deletions(snapshot_sequences, &scan.inline_file_deletions)?;
    out.push_str(&format!(
        "delete_scan\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        if scan.full_file_delete {
            "full"
        } else {
            "partial"
        },
        sequence,
        scan.data_file.data_file_id.0,
        scan.data_file.table_id.0,
        scan.data_file.path,
        scan.data_file.record_count,
        scan.data_file.file_size_bytes,
        scan.data_file.row_id_start,
        scan.data_file
            .mapping_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        delete_file.0,
        delete_file.1,
        delete_file.2,
        delete_file.3,
        previous_delete_file.0,
        previous_delete_file.1,
        previous_delete_file.2,
        previous_delete_file.3,
        inline_file_deletions
    ));
    Ok(())
}

fn display_inline_file_deletions(
    snapshot_sequences: &BTreeMap<CatalogOrderId, u64>,
    inline_file_deletions: &BTreeMap<u64, CatalogOrderId>,
) -> CatalogResult<String> {
    let mut parts = Vec::new();
    for (row_id, order) in inline_file_deletions {
        let sequence = snapshot_sequences
            .get(order)
            .copied()
            .ok_or_else(|| missing_snapshot_order(*order))?;
        parts.push(format!("{row_id}:{sequence}"));
    }
    Ok(parts.join(","))
}

fn display_delete_file(row: Option<&DeleteFileRow>) -> (String, String, String, String) {
    row.map_or_else(
        || (String::new(), String::new(), String::new(), String::new()),
        |row| {
            (
                row.delete_file_id.0.to_string(),
                row.path.clone(),
                row.record_count.to_string(),
                row.file_size_bytes.to_string(),
            )
        },
    )
}

fn public_sequences_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    public_snapshot_sequences_by_order(kv, catalog)
}

fn snapshot_ids_by_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<BTreeMap<CatalogOrderId, u64>> {
    let mut ids = public_sequences_by_order(kv, catalog)?;
    for snapshot in list_snapshots(kv, catalog)? {
        ids.insert(snapshot.order, snapshot.sequence.0);
    }
    Ok(ids)
}

fn missing_snapshot(snapshot_id: DuckLakeSnapshotId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot {snapshot_id} does not exist"))
}

fn missing_snapshot_order(order: CatalogOrderId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot order {order} does not exist"))
}

#[cfg(test)]
#[path = "runtime_change_feed_tests.rs"]
mod runtime_change_feed_tests;
