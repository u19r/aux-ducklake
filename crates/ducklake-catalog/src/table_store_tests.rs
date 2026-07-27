use std::{cell::RefCell, collections::BTreeSet};

use crate::{
    CatalogId, CatalogOrderId, CatalogResult, FakeOrderedCatalogKv, KvBatch, OrderedCatalogKv,
    RangeDirection, RangeItem, RawSnapshotSequence, SnapshotRow, TableId, TableRow,
    keys::{
        current_table_row_key, table_object_key, table_object_prefix, table_object_scan_prefix,
        table_visibility_prefix,
    },
    store::stage_snapshot,
    table_store::{
        list_table_rows_for_table, list_tables_at, load_current_table_row, load_current_table_rows,
        stage_current_table_row, stage_table_visibility_row,
    },
};

struct ScanRecordingKv {
    inner: FakeOrderedCatalogKv,
    get_keys: RefCell<Vec<Vec<u8>>>,
    scanned_prefixes: RefCell<Vec<Vec<u8>>>,
    range_starts: RefCell<Vec<Vec<u8>>>,
}

#[test]
fn given_one_table_requested_when_loading_history_then_only_that_table_prefix_is_scanned() {
    let catalog = CatalogId(5);
    let requested = TableId(7);
    let other = TableId(8);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for (table_id, order) in [
        (requested, CatalogOrderId::uuid_v7(10)),
        (requested, CatalogOrderId::uuid_v7(20)),
        (other, CatalogOrderId::uuid_v7(30)),
    ] {
        batch.put(
            table_object_key(catalog, table_id, order),
            TableRow::new(table_id, format!("table_{}", table_id.0), order).encode(),
        );
    }
    kv.commit(batch).unwrap();
    let recording = ScanRecordingKv::new(kv);

    let rows = list_table_rows_for_table(&recording, catalog, requested).unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.table_id == requested));
    assert_eq!(
        recording.scanned_prefixes.into_inner(),
        vec![table_object_prefix(catalog, requested)]
    );
}

impl ScanRecordingKv {
    fn new(inner: FakeOrderedCatalogKv) -> Self {
        Self {
            inner,
            get_keys: RefCell::new(Vec::new()),
            scanned_prefixes: RefCell::new(Vec::new()),
            range_starts: RefCell::new(Vec::new()),
        }
    }
}

impl OrderedCatalogKv for ScanRecordingKv {
    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        self.get_keys.borrow_mut().push(key.to_vec());
        Ok(self.inner.get(key))
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        self.scanned_prefixes.borrow_mut().push(prefix.to_vec());
        Ok(self.inner.scan_prefix(prefix, direction, limit))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        self.range_starts.borrow_mut().push(start.to_vec());
        Ok(self.inner.scan_range(start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.read_conflict_fence(key))
    }
}

#[test]
fn given_current_table_row_when_loading_current_table_then_history_is_not_scanned() {
    let catalog = CatalogId(3);
    let latest_order = CatalogOrderId::uuid_v7(10);
    let table_id = TableId(7);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    let snapshot = SnapshotRow::new(latest_order, RawSnapshotSequence(3));
    let mut current = TableRow::new(table_id, "current", latest_order);
    current.validity = crate::ValidityWindow::new(latest_order, None);

    stage_snapshot(&mut batch, catalog, &snapshot);
    batch.put(
        table_object_key(catalog, current.table_id, current.validity.begin_order),
        current.encode(),
    );
    stage_current_table_row(&mut batch, catalog, &current);
    kv.commit(batch).unwrap();

    let recording = ScanRecordingKv::new(kv);
    let table = load_current_table_row(&recording, catalog, table_id)
        .unwrap()
        .unwrap();

    assert_eq!(table.table_id, table_id);
    assert!(
        recording
            .get_keys
            .borrow()
            .contains(&current_table_row_key(catalog, table_id)),
        "current table loading should use the current table row key"
    );
    assert!(
        recording.range_starts.borrow().is_empty(),
        "current table loading should not scan table history"
    );
}

#[test]
fn given_requested_current_tables_when_batch_loading_then_only_requested_keys_are_read() {
    let catalog = CatalogId(4);
    let order = CatalogOrderId::uuid_v7(10);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for table_id in [TableId(7), TableId(8), TableId(9)] {
        stage_current_table_row(
            &mut batch,
            catalog,
            &TableRow::new(table_id, format!("table_{}", table_id.0), order),
        );
    }
    kv.commit(batch).unwrap();
    let recording = ScanRecordingKv::new(kv);

    let rows = load_current_table_rows(
        &recording,
        catalog,
        &BTreeSet::from([TableId(7), TableId(9)]),
    )
    .unwrap();

    assert_eq!(
        rows.iter().map(|row| row.table_id).collect::<Vec<_>>(),
        vec![TableId(7), TableId(9)]
    );
    assert_eq!(
        recording.get_keys.into_inner(),
        vec![
            current_table_row_key(catalog, TableId(7)),
            current_table_row_key(catalog, TableId(9)),
        ]
    );
    assert!(recording.scanned_prefixes.into_inner().is_empty());
    assert!(recording.range_starts.into_inner().is_empty());
}

#[test]
fn given_table_visibility_index_when_listing_historical_tables_then_history_is_not_scanned() {
    let catalog = CatalogId(2);
    let order_1 = CatalogOrderId::uuid_v7(10);
    let order_2 = CatalogOrderId::uuid_v7(20);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    let mut previous = TableRow::new(TableId(7), "previous", order_1);
    previous.validity = crate::ValidityWindow::new(order_1, Some(order_2));
    let current = TableRow::new(TableId(7), "current", order_2);

    batch.put(
        table_object_key(catalog, previous.table_id, previous.validity.begin_order),
        previous.encode(),
    );
    batch.put(
        table_object_key(catalog, current.table_id, current.validity.begin_order),
        current.encode(),
    );
    stage_table_visibility_row(&mut batch, catalog, &previous);
    stage_table_visibility_row(&mut batch, catalog, &current);
    kv.commit(batch).unwrap();

    let recording = ScanRecordingKv::new(kv);
    let tables = list_tables_at(&recording, catalog, order_1).unwrap();

    assert_eq!(
        tables
            .iter()
            .map(|table| (table.table_id, table.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(TableId(7), "previous")]
    );
    assert!(
        !recording
            .scanned_prefixes
            .borrow()
            .contains(&table_object_scan_prefix(catalog)),
        "historical table listing should use the visibility index, not scan table history"
    );
    assert!(
        recording
            .range_starts
            .borrow()
            .contains(&table_visibility_prefix(catalog)),
        "historical table listing should scan the visibility index"
    );
}

#[test]
fn given_current_table_index_when_listing_latest_tables_then_history_is_not_scanned() {
    let catalog = CatalogId(1);
    let latest_order = CatalogOrderId::uuid_v7(10);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    let snapshot = SnapshotRow::new(latest_order, RawSnapshotSequence(3));
    let mut current = TableRow::new(TableId(7), "current", latest_order);
    current.validity = crate::ValidityWindow::new(latest_order, None);

    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_current_table_row(&mut batch, catalog, &current);
    kv.commit(batch).unwrap();

    let recording = ScanRecordingKv::new(kv);
    let tables = list_tables_at(&recording, catalog, latest_order).unwrap();

    assert_eq!(
        tables
            .iter()
            .map(|table| table.table_id)
            .collect::<Vec<_>>(),
        vec![TableId(7)]
    );
    assert!(
        !recording
            .scanned_prefixes
            .borrow()
            .contains(&table_object_scan_prefix(catalog)),
        "latest table listing should use the current table index, not scan table history"
    );
}
