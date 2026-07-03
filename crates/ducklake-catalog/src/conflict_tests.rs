#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::{
        CatalogError, CatalogId, ColumnId, DataCommitIntent, DataFileChangeKind, DataFileId,
        FakeOrderedCatalogKv, InlinedTableRow, KvBatch, MutableCatalogKv, OrderedCatalogKv,
        RangeDirection, RangeItem, TableColumnRow, TableId, TableRow, TableVersionReplacement,
        commit_append_table_columns_with_conflict_check, commit_create_table_row,
        initialize_catalog_if_absent,
        keys::{
            snapshot_data_file_change_prefix, table_data_file_change_prefix, table_object_prefix,
        },
        latest_snapshot,
    };

    use super::super::{
        list_data_conflicts_since_base, reject_conflicts_since_base,
        reject_table_metadata_conflicts_since_base, table_schema_conflict_since_base,
        write_data_file_change,
    };

    #[test]
    fn given_no_order_window_when_rejecting_conflicts_then_storage_is_not_read() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let order = crate::CatalogOrderId::uuid_v7(10);
        let kv = ReadCountingKv::default();

        reject_conflicts_since_base(
            &kv,
            catalog,
            table_id,
            order,
            order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )
        .unwrap();

        assert_eq!(kv.read_count(), 0);
    }

    #[test]
    fn given_no_order_window_when_rejecting_table_metadata_conflicts_then_storage_is_not_read() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let order = crate::CatalogOrderId::uuid_v7(10);
        let kv = ReadCountingKv::default();

        reject_table_metadata_conflicts_since_base(&kv, catalog, table_id, order, order).unwrap();

        assert_eq!(kv.read_count(), 0);
    }

    #[test]
    fn given_reversed_table_metadata_conflict_window_then_storage_is_not_read() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let base_order = crate::CatalogOrderId::uuid_v7(20);
        let through_order = crate::CatalogOrderId::uuid_v7(10);
        let kv = ReadCountingKv::default();

        let err = reject_table_metadata_conflicts_since_base(
            &kv,
            catalog,
            table_id,
            base_order,
            through_order,
        )
        .unwrap_err();

        assert!(matches!(err, CatalogError::InvalidMutation(_)));
        assert_eq!(kv.read_count(), 0);
    }

    #[test]
    fn given_table_conflicts_when_listing_data_conflicts_then_catalog_change_index_is_not_scanned()
    {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let unrelated_table = TableId(11);
        let base_order = crate::CatalogOrderId::uuid_v7(10);
        let target_order = crate::CatalogOrderId::uuid_v7(20);
        let unrelated_order = crate::CatalogOrderId::uuid_v7(21);
        let through_order = crate::CatalogOrderId::uuid_v7(30);
        let mut inner = FakeOrderedCatalogKv::new();
        let mut batch = KvBatch::new();
        write_data_file_change(
            &mut batch,
            catalog,
            table_id,
            target_order,
            DataFileChangeKind::Removed,
            DataFileId(1),
        );
        write_data_file_change(
            &mut batch,
            catalog,
            unrelated_table,
            unrelated_order,
            DataFileChangeKind::Removed,
            DataFileId(2),
        );
        inner.commit(batch).unwrap();
        let kv = ConflictChangeScanCountingKv::new(inner, catalog, table_id);

        let conflicts = list_data_conflicts_since_base(
            &kv,
            catalog,
            table_id,
            base_order,
            through_order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )
        .unwrap();

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].table_id, table_id);
        assert_eq!(conflicts[0].data_file_id, DataFileId(1));
        assert_eq!(kv.table_change_scans(), 1);
        assert_eq!(kv.snapshot_change_scans(), 0);
    }

    #[test]
    fn given_unchanged_table_when_rejecting_conflicts_then_base_table_is_loaded_once() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let created = commit_create_table_row(&mut inner, catalog, test_table(table_id)).unwrap();
        let base_order = created.validity.begin_order;
        let mut unrelated_table = test_table(TableId(11));
        unrelated_table.name = "unrelated".to_owned();
        unrelated_table.path = "unrelated".to_owned();
        commit_create_table_row(&mut inner, catalog, unrelated_table).unwrap();
        let through_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = ConflictChangeScanCountingKv::new(inner, catalog, table_id);

        reject_conflicts_since_base(
            &kv,
            catalog,
            table_id,
            base_order,
            through_order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )
        .unwrap();

        assert_eq!(kv.table_object_scans(), 1);
        assert_eq!(kv.table_change_scans(), 1);
        assert_eq!(kv.snapshot_change_scans(), 0);
    }

    #[test]
    fn given_schema_changed_when_rejecting_conflicts_then_through_table_is_reused() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut inner = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut inner, catalog).unwrap();
        let created = commit_create_table_row(&mut inner, catalog, test_table(table_id)).unwrap();
        let base_order = created.validity.begin_order;
        commit_append_table_columns_with_conflict_check(
            &mut inner,
            catalog,
            table_id,
            base_order,
            base_order,
            vec![
                TableColumnRow::new(ColumnId(2), "k", "INTEGER", true, None)
                    .with_created_with_table(false),
            ],
        )
        .unwrap();
        let through_order = latest_snapshot(&inner, catalog).unwrap().unwrap().order;
        let kv = ConflictChangeScanCountingKv::new(inner, catalog, table_id);

        let err = reject_conflicts_since_base(
            &kv,
            catalog,
            table_id,
            base_order,
            through_order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            CatalogError::TableSchemaConflict {
                table_id: conflict_table,
                ..
            } if conflict_table == table_id
        ));
        assert_eq!(kv.table_object_scans(), 2);
        assert_eq!(kv.table_change_scans(), 1);
        assert_eq!(kv.snapshot_change_scans(), 0);
    }

    #[test]
    fn given_inline_registry_changed_when_checking_schema_conflicts_then_no_conflict_is_reported() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(&mut kv, catalog, test_table(table_id)).unwrap();
        let base_order = created.validity.begin_order;

        let mut with_inline_table = created.clone();
        with_inline_table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_10_1", 1));
        let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
        kv.commit_table_replacements(
            catalog,
            latest.sequence,
            vec![TableVersionReplacement::new(
                table_id,
                created,
                with_inline_table,
            )],
        )
        .unwrap();

        let through_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
        let conflict =
            table_schema_conflict_since_base(&kv, catalog, table_id, base_order, through_order)
                .unwrap();
        assert_eq!(conflict, None);

        commit_append_table_columns_with_conflict_check(
            &mut kv,
            catalog,
            table_id,
            base_order,
            through_order,
            vec![
                TableColumnRow::new(ColumnId(2), "k", "INTEGER", true, None)
                    .with_created_with_table(false),
            ],
        )
        .unwrap();
    }

    #[test]
    fn given_column_schema_changed_when_checking_schema_conflicts_then_conflict_is_reported() {
        let catalog = CatalogId(1);
        let table_id = TableId(10);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        let created = commit_create_table_row(&mut kv, catalog, test_table(table_id)).unwrap();
        let base_order = created.validity.begin_order;

        commit_append_table_columns_with_conflict_check(
            &mut kv,
            catalog,
            table_id,
            base_order,
            base_order,
            vec![
                TableColumnRow::new(ColumnId(2), "k", "INTEGER", true, None)
                    .with_created_with_table(false),
            ],
        )
        .unwrap();

        let through_order = latest_snapshot(&kv, catalog).unwrap().unwrap().order;
        let conflict =
            table_schema_conflict_since_base(&kv, catalog, table_id, base_order, through_order)
                .unwrap();
        assert!(conflict.is_some());
    }

    fn test_table(table_id: TableId) -> TableRow {
        TableRow::with_catalog_metadata(
            table_id,
            crate::SchemaId(0),
            "table-uuid",
            "test",
            "test",
            vec![
                TableColumnRow::new(ColumnId(0), "i", "INTEGER", true, None),
                TableColumnRow::new(ColumnId(1), "j", "INTEGER", true, None),
            ],
            crate::CatalogOrderId::uuid_v7(0),
        )
    }

    #[derive(Default)]
    struct ReadCountingKv {
        reads: Cell<usize>,
    }

    impl ReadCountingKv {
        fn read_count(&self) -> usize {
            self.reads.get()
        }

        fn record_read(&self) {
            self.reads.set(self.reads.get().saturating_add(1));
        }
    }

    impl OrderedCatalogKv for ReadCountingKv {
        fn get(&self, _key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            self.record_read();
            Ok(None)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            self.record_read();
            Ok(vec![None; keys.len()])
        }

        fn scan_prefix(
            &self,
            _prefix: &[u8],
            _direction: RangeDirection,
            _limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            self.record_read();
            Ok(Vec::new())
        }

        fn scan_range(
            &self,
            _start: &[u8],
            _end: &[u8],
            _direction: RangeDirection,
            _limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            self.record_read();
            Ok(Vec::new())
        }

        fn read_conflict_fence(&self, _key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            self.record_read();
            Ok(None)
        }
    }

    struct ConflictChangeScanCountingKv {
        inner: FakeOrderedCatalogKv,
        table_change_prefix: Vec<u8>,
        snapshot_change_prefix: Vec<u8>,
        table_object_prefix: Vec<u8>,
        table_change_scans: Cell<usize>,
        snapshot_change_scans: Cell<usize>,
        table_object_scans: Cell<usize>,
    }

    impl ConflictChangeScanCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId, table_id: TableId) -> Self {
            Self {
                inner,
                table_change_prefix: table_data_file_change_prefix(catalog, table_id),
                snapshot_change_prefix: snapshot_data_file_change_prefix(catalog),
                table_object_prefix: table_object_prefix(catalog, table_id),
                table_change_scans: Cell::new(0),
                snapshot_change_scans: Cell::new(0),
                table_object_scans: Cell::new(0),
            }
        }

        fn table_change_scans(&self) -> usize {
            self.table_change_scans.get()
        }

        fn snapshot_change_scans(&self) -> usize {
            self.snapshot_change_scans.get()
        }

        fn table_object_scans(&self) -> usize {
            self.table_object_scans.get()
        }
    }

    impl OrderedCatalogKv for ConflictChangeScanCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::get(&self.inner, key)
        }

        fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
            OrderedCatalogKv::batch_get(&self.inner, keys)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<RangeItem>> {
            if start.starts_with(&self.table_change_prefix) {
                self.table_change_scans
                    .set(self.table_change_scans.get().saturating_add(1));
            }
            if start.starts_with(&self.snapshot_change_prefix) {
                self.snapshot_change_scans
                    .set(self.snapshot_change_scans.get().saturating_add(1));
            }
            if start.starts_with(&self.table_object_prefix) {
                self.table_object_scans
                    .set(self.table_object_scans.get().saturating_add(1));
            }
            OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
