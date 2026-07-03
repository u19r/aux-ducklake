use std::cell::RefCell;

use crate::{
    CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv, KvBatch,
    OrderedCatalogKv, RangeDirection, RangeItem, SchemaId, TableId,
    data_file_store::stage_append_data_file_without_change,
    inline_change_feed::{
        InlineRowChangeKind, insertion_data_files_through,
        list_inline_deleted_row_changes_for_schema, stage_inline_row_change,
    },
    keys::{
        KeyFamily, family_prefix, inline_table_change_key, table_schema_kind_inline_row_change_key,
        table_schema_kind_inline_row_change_scan_start,
    },
};

struct ScanRecordingKv {
    inner: FakeOrderedCatalogKv,
    scanned_prefixes: RefCell<Vec<Vec<u8>>>,
    scanned_ranges: RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl ScanRecordingKv {
    fn new(inner: FakeOrderedCatalogKv) -> Self {
        Self {
            inner,
            scanned_prefixes: RefCell::new(Vec::new()),
            scanned_ranges: RefCell::new(Vec::new()),
        }
    }

    fn scanned_prefixes(&self) -> Vec<Vec<u8>> {
        self.scanned_prefixes.borrow().clone()
    }

    fn scanned_ranges(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scanned_ranges.borrow().clone()
    }
}

impl OrderedCatalogKv for ScanRecordingKv {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.get(key))
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        self.scanned_prefixes.borrow_mut().push(prefix.to_vec());
        Ok(self.inner.scan_prefix(prefix, direction, limit))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        self.scanned_ranges
            .borrow_mut()
            .push((start.to_vec(), end.to_vec()));
        Ok(self.inner.scan_range(start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.read_conflict_fence(key))
    }
}

#[test]
fn given_partial_files_when_listing_insertions_then_scan_is_table_bounded() {
    let catalog = CatalogId(42);
    let wanted_table = TableId(7);
    let other_table = TableId(8);
    let snapshot_order = CatalogOrderId::uuid_v7(7);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();

    stage_append_data_file_without_change(
        &kv,
        &mut batch,
        catalog,
        &partial_file(1, wanted_table, 10, 5),
    )
    .unwrap();
    stage_append_data_file_without_change(
        &kv,
        &mut batch,
        catalog,
        &partial_file(2, other_table, 10, 5),
    )
    .unwrap();
    kv.commit(batch).unwrap();

    let recording = ScanRecordingKv::new(kv);
    let rows =
        insertion_data_files_through(&recording, catalog, wanted_table, snapshot_order).unwrap();

    assert_eq!(
        rows.iter().map(|row| row.data_file_id).collect::<Vec<_>>(),
        vec![DataFileId(1)]
    );
    assert!(
        !recording
            .scanned_prefixes()
            .contains(&family_prefix(catalog, KeyFamily::DataFile)),
        "insertion data-file lookup must not scan every data file in the catalog"
    );
}

#[test]
fn given_schema_kind_inline_row_changes_when_listing_deleted_rows_then_scan_is_schema_kind_bounded()
{
    let catalog = CatalogId(42);
    let table = TableId(7);
    let wanted_schema = SchemaId(11);
    let other_schema = SchemaId(12);
    let start_order = CatalogOrderId::uuid_v7(10);
    let wanted_order = CatalogOrderId::uuid_v7(20);
    let end_order = CatalogOrderId::uuid_v7(30);
    let later_order = CatalogOrderId::uuid_v7(40);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();

    stage_inline_row_change(
        &mut batch,
        catalog,
        table,
        wanted_schema,
        wanted_order,
        InlineRowChangeKind::Inserted,
        1,
    );
    stage_inline_row_change(
        &mut batch,
        catalog,
        table,
        other_schema,
        wanted_order,
        InlineRowChangeKind::Deleted,
        2,
    );
    stage_inline_row_change(
        &mut batch,
        catalog,
        table,
        wanted_schema,
        wanted_order,
        InlineRowChangeKind::Deleted,
        3,
    );
    stage_inline_row_change(
        &mut batch,
        catalog,
        table,
        wanted_schema,
        later_order,
        InlineRowChangeKind::Deleted,
        4,
    );
    kv.commit(batch).unwrap();
    assert_eq!(
        kv.get(&inline_table_change_key(
            catalog,
            wanted_order,
            InlineRowChangeKind::Inserted,
            table,
        )),
        Some(Vec::new()),
        "insert changes must write the compact table-level change index"
    );
    assert_eq!(
        kv.get(&table_schema_kind_inline_row_change_key(
            catalog,
            table,
            wanted_schema,
            InlineRowChangeKind::Inserted,
            wanted_order,
            1,
        )),
        None,
        "insert changes must not write the deleted-row visibility index"
    );

    let recording = ScanRecordingKv::new(kv);
    let rows = list_inline_deleted_row_changes_for_schema(
        &recording,
        catalog,
        table,
        wanted_schema,
        start_order,
        end_order,
    )
    .unwrap();

    assert_eq!(
        rows.iter().map(|row| row.row_id).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(recording.scanned_ranges().len(), 1);
    assert!(
        recording.scanned_ranges()[0].0.starts_with(
            &table_schema_kind_inline_row_change_scan_start(
                catalog,
                table,
                wanted_schema,
                InlineRowChangeKind::Deleted,
                start_order,
            )
        ),
        "deleted inline row lookup must use the schema/kind bounded index"
    );
}

fn partial_file(
    id: u64,
    table_id: TableId,
    begin_order: u128,
    max_partial_order: u128,
) -> DataFileRow {
    let mut row = DataFileRow::new(
        DataFileId(id),
        table_id,
        format!("table-{}/file-{id}.parquet", table_id.0),
        10,
        128,
        CatalogOrderId::uuid_v7(begin_order),
    );
    row.max_partial_order = Some(CatalogOrderId::uuid_v7(max_partial_order));
    row
}
