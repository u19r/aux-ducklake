use std::{cell::Cell, collections::BTreeMap};

use crate::{
    CatalogId, CatalogOrderId, CatalogResult, ColumnId, DataFileId, DataFileRow,
    FakeOrderedCatalogKv, FileColumnStatsRow, OrderedCatalogKv, RangeDirection, RangeItem,
    SchemaId, TableId, commit_append_data_files, initialize_catalog_if_absent,
    keys::{
        KeyFamily, catalog_file_stats_version_key, family_prefix,
        file_column_stats_data_file_prefix, file_column_stats_key, table_file_stats_version_key,
    },
    register_file_column_stats, register_inline_table_payload,
};

#[test]
fn given_file_column_stats_write_when_committed_then_table_file_stats_version_changes() {
    let catalog = CatalogId(41);
    let table = TableId(7);
    let file = DataFileId(11);
    let mut kv = FakeOrderedCatalogKv::new();

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            file,
            table,
            "main/data.parquet",
            10,
            512,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    let version_key = table_file_stats_version_key(catalog, table);
    let catalog_version_key = catalog_file_stats_version_key(catalog);
    assert_eq!(kv.get(&version_key), None);
    assert_eq!(kv.get(&catalog_version_key), None);

    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            ColumnId(1),
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    let first_version = kv.get(&version_key).unwrap();
    let first_catalog_version = kv.get(&catalog_version_key).unwrap();

    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            ColumnId(1),
            0,
            Some("2".to_owned()),
            Some("10".to_owned()),
        ),
    )
    .unwrap();

    assert_ne!(kv.get(&version_key).unwrap(), first_version);
    assert_ne!(kv.get(&catalog_version_key).unwrap(), first_catalog_version);
}

#[test]
fn given_inline_row_write_when_committed_then_table_file_stats_version_does_not_change() {
    let catalog = CatalogId(42);
    let table = TableId(8);
    let file = DataFileId(12);
    let schema = SchemaId(1);
    let mut kv = FakeOrderedCatalogKv::new();

    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            file,
            table,
            "main/data.parquet",
            10,
            512,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            ColumnId(1),
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    let version_key = table_file_stats_version_key(catalog, table);
    let catalog_version_key = catalog_file_stats_version_key(catalog);
    let stats_version = kv.get(&version_key).unwrap();
    let catalog_stats_version = kv.get(&catalog_version_key).unwrap();

    register_inline_table_payload(&mut kv, catalog, table, schema, b"row\t1\ti:1\n".to_vec())
        .unwrap();

    assert_eq!(kv.get(&version_key).unwrap(), stats_version);
    assert_eq!(kv.get(&catalog_version_key).unwrap(), catalog_stats_version);
}

#[test]
fn given_same_file_column_stats_loaded_twice_when_cached_then_second_load_skips_batch_get() {
    let catalog = CatalogId(43);
    let table = TableId(9);
    let file = DataFileId(13);
    let column = ColumnId(1);
    let mut inner = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_append_data_files(&mut inner, catalog, vec![test_data_file(file, table)]).unwrap();
    register_file_column_stats(
        &mut inner,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    let kv = CountingStatsKv::new(inner, file_column_stats_key(catalog, file, column));
    let files = vec![test_data_file(file, table)];
    let columns_by_table = columns_by_table(table, [column]);

    let first =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();
    let second =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();

    assert_eq!(first, second);
    assert_eq!(kv.stats_batch_gets(), 1);
}

#[test]
fn given_multiple_file_column_stats_loaded_twice_when_cached_then_second_load_skips_batch_get() {
    let catalog = CatalogId(49);
    let table = TableId(15);
    let file = DataFileId(19);
    let columns = [ColumnId(1), ColumnId(2)];
    let mut inner = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_append_data_files(&mut inner, catalog, vec![test_data_file(file, table)]).unwrap();
    for column in columns {
        register_file_column_stats(
            &mut inner,
            catalog,
            FileColumnStatsRow::new(
                file,
                table,
                column,
                0,
                Some(column.0.to_string()),
                Some(column.0.to_string()),
            ),
        )
        .unwrap();
    }
    let kv = CountingStatsKv::new(inner, file_column_stats_key(catalog, file, columns[0]));
    let files = vec![test_data_file(file, table)];
    let columns_by_table = columns_by_table(table, columns);

    let first =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();
    let second =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), 2);
    assert_eq!(kv.stats_batch_gets(), 1);
}

#[test]
fn given_stats_are_created_after_missing_lookup_when_loaded_again_then_new_stats_are_returned() {
    let catalog = CatalogId(44);
    let table = TableId(10);
    let file = DataFileId(14);
    let column = ColumnId(1);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![test_data_file(file, table)]).unwrap();
    let files = vec![test_data_file(file, table)];
    let columns_by_table = columns_by_table(table, [column]);

    let before =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    let after =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();

    assert!(before.is_empty());
    assert_eq!(after.len(), 1);
}

#[test]
fn given_cached_stats_are_registered_again_when_loaded_then_overwritten_stats_are_returned() {
    let catalog = CatalogId(45);
    let table = TableId(11);
    let file = DataFileId(15);
    let column = ColumnId(1);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![test_data_file(file, table)]).unwrap();
    let files = vec![test_data_file(file, table)];
    let columns_by_table = columns_by_table(table, [column]);
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table).unwrap();

    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("2".to_owned()),
            Some("10".to_owned()),
        ),
    )
    .unwrap();
    let rows =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].min_value.as_deref(), Some("2"));
    assert_eq!(rows[0].max_value.as_deref(), Some("10"));
}

#[test]
fn given_stats_for_source_file_ids_loaded_twice_when_cached_then_second_load_skips_prefix_scans() {
    let catalog = CatalogId(46);
    let table = TableId(12);
    let files = [DataFileId(16), DataFileId(17)];
    let mut inner = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_append_data_files(
        &mut inner,
        catalog,
        files
            .into_iter()
            .map(|file| test_data_file(file, table))
            .collect(),
    )
    .unwrap();
    for file in files {
        register_file_column_stats(
            &mut inner,
            catalog,
            FileColumnStatsRow::new(
                file,
                table,
                ColumnId(1),
                0,
                Some(file.0.to_string()),
                Some(file.0.to_string()),
            ),
        )
        .unwrap();
    }
    let kv = SourceStatsScanCountingKv::new(inner, catalog);

    let first = super::list_file_column_stats_for_data_file_ids(&kv, catalog, &files).unwrap();
    let second = super::list_file_column_stats_for_data_file_ids(&kv, catalog, &files).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), 2);
    assert_eq!(kv.source_stats_range_scans(), 1);
    assert_eq!(kv.source_stats_prefix_scans(), 0);
}

#[test]
fn given_sparse_source_stats_when_loaded_then_exact_prefix_scans_are_used() {
    let catalog = CatalogId(56);
    let table = TableId(22);
    let files = [DataFileId(16), DataFileId(10_000)];
    let mut inner = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_append_data_files(
        &mut inner,
        catalog,
        files
            .into_iter()
            .map(|file| test_data_file(file, table))
            .collect(),
    )
    .unwrap();
    for file in files {
        register_file_column_stats(
            &mut inner,
            catalog,
            FileColumnStatsRow::new(
                file,
                table,
                ColumnId(1),
                0,
                Some(file.0.to_string()),
                Some(file.0.to_string()),
            ),
        )
        .unwrap();
    }
    let kv = SourceStatsScanCountingKv::new(inner, catalog);

    let rows = super::list_file_column_stats_for_data_file_ids(&kv, catalog, &files).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(kv.source_stats_prefix_scans(), 2);
    assert_eq!(kv.source_stats_range_scans(), 0);
}

#[test]
fn given_wide_dense_file_stats_request_when_loaded_then_range_scan_replaces_batch_get() {
    let catalog = CatalogId(57);
    let table = TableId(23);
    let files = (16..80).map(DataFileId).collect::<Vec<_>>();
    let columns = [
        ColumnId(1),
        ColumnId(2),
        ColumnId(3),
        ColumnId(4),
        ColumnId(5),
        ColumnId(6),
        ColumnId(7),
        ColumnId(8),
    ];
    let mut inner = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_append_data_files(
        &mut inner,
        catalog,
        files
            .iter()
            .copied()
            .map(|file| test_data_file(file, table))
            .collect(),
    )
    .unwrap();
    for file in &files {
        for column in columns {
            register_file_column_stats(
                &mut inner,
                catalog,
                FileColumnStatsRow::new(
                    *file,
                    table,
                    column,
                    0,
                    Some(format!("{}-{}", file.0, column.0)),
                    Some(format!("{}-{}", file.0, column.0)),
                ),
            )
            .unwrap();
        }
    }
    let kv = SourceStatsScanCountingKv::new(inner, catalog);
    let files = files
        .into_iter()
        .map(|file| test_data_file(file, table))
        .collect::<Vec<_>>();
    let columns_by_table = columns_by_table(table, columns);

    let rows =
        super::list_file_column_stats_for_data_files(&kv, catalog, &files, &columns_by_table)
            .unwrap();

    assert_eq!(rows.len(), 512);
    assert_eq!(kv.source_stats_range_scans(), 1);
    assert_eq!(kv.source_stats_prefix_scans(), 0);
    assert_eq!(kv.stats_batch_gets(), 0);
}

#[test]
fn given_file_column_stats_key_when_decoded_then_data_file_id_is_validated() {
    let catalog = CatalogId(57);
    let file = DataFileId(23);
    let column = ColumnId(5);
    let key = file_column_stats_key(catalog, file, column);
    let truncated = file_column_stats_data_file_prefix(catalog, file);

    let decoded = super::file_column_stats_data_file_id_from_key(catalog, &key).unwrap();

    assert_eq!(decoded, file);
    assert!(super::file_column_stats_data_file_id_from_key(catalog, &truncated).is_err());
}

#[test]
fn given_empty_source_stats_cache_when_stats_are_registered_then_next_load_sees_new_row() {
    let catalog = CatalogId(47);
    let table = TableId(13);
    let file = DataFileId(18);
    let column = ColumnId(1);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![test_data_file(file, table)]).unwrap();

    let before = super::list_file_column_stats_for_data_file_ids(&kv, catalog, &[file]).unwrap();
    register_file_column_stats(
        &mut kv,
        catalog,
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("1".to_owned()),
            Some("9".to_owned()),
        ),
    )
    .unwrap();
    let after = super::list_file_column_stats_for_data_file_ids(&kv, catalog, &[file]).unwrap();

    assert!(before.is_empty());
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].data_file_id, file);
}

fn test_data_file(data_file_id: DataFileId, table_id: TableId) -> DataFileRow {
    DataFileRow::new(
        data_file_id,
        table_id,
        format!("main/{}.parquet", data_file_id.0),
        10,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}

fn columns_by_table(
    table_id: TableId,
    columns: impl IntoIterator<Item = ColumnId>,
) -> BTreeMap<TableId, Vec<ColumnId>> {
    BTreeMap::from([(table_id, columns.into_iter().collect())])
}

struct CountingStatsKv {
    inner: FakeOrderedCatalogKv,
    counted_key: Vec<u8>,
    stats_batch_gets: Cell<usize>,
}

struct SourceStatsScanCountingKv {
    inner: FakeOrderedCatalogKv,
    catalog: CatalogId,
    stats_batch_gets: Cell<usize>,
    source_stats_prefix_scans: Cell<usize>,
    source_stats_range_scans: Cell<usize>,
}

impl SourceStatsScanCountingKv {
    fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
        Self {
            inner,
            catalog,
            stats_batch_gets: Cell::new(0),
            source_stats_prefix_scans: Cell::new(0),
            source_stats_range_scans: Cell::new(0),
        }
    }

    fn stats_batch_gets(&self) -> usize {
        self.stats_batch_gets.get()
    }

    fn source_stats_prefix_scans(&self) -> usize {
        self.source_stats_prefix_scans.get()
    }

    fn source_stats_range_scans(&self) -> usize {
        self.source_stats_range_scans.get()
    }
}

impl OrderedCatalogKv for SourceStatsScanCountingKv {
    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.get(key))
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        if keys
            .iter()
            .any(|key| key.starts_with(&family_prefix(self.catalog, KeyFamily::FileColumnStats)))
        {
            self.stats_batch_gets
                .set(self.stats_batch_gets.get().saturating_add(1));
        }
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        for data_file_id in [DataFileId(16), DataFileId(17), DataFileId(10_000)] {
            if prefix == file_column_stats_data_file_prefix(self.catalog, data_file_id).as_slice() {
                self.source_stats_prefix_scans
                    .set(self.source_stats_prefix_scans.get().saturating_add(1));
            }
        }
        Ok(self.inner.scan_prefix(prefix, direction, limit))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        if start.starts_with(&file_column_stats_data_file_prefix(
            self.catalog,
            DataFileId(16),
        )) {
            self.source_stats_range_scans
                .set(self.source_stats_range_scans.get().saturating_add(1));
        }
        Ok(self.inner.scan_range(start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.read_conflict_fence(key))
    }
}

impl CountingStatsKv {
    fn new(inner: FakeOrderedCatalogKv, counted_key: Vec<u8>) -> Self {
        Self {
            inner,
            counted_key,
            stats_batch_gets: Cell::new(0),
        }
    }

    fn stats_batch_gets(&self) -> usize {
        self.stats_batch_gets.get()
    }
}

impl OrderedCatalogKv for CountingStatsKv {
    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.get(key))
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        if keys.iter().any(|key| key == &self.counted_key) {
            self.stats_batch_gets
                .set(self.stats_batch_gets.get().saturating_add(1));
        }
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        Ok(self.inner.scan_prefix(prefix, direction, limit))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        Ok(self.inner.scan_range(start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.read_conflict_fence(key))
    }
}
