use std::time::Instant;

use foundationdb::{RangeOption, tuple};
use futures::executor::block_on;

use crate::{
    CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FdbOrderedCatalogKv, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection,
    keys::{self, prefix_end},
    maintenance::{
        ScheduledDataFileCleanupKind, encode_scheduled_data_cleanup_value,
        encode_scheduled_delete_cleanup_value, list_scheduled_data_file_cleanup_rows,
        list_scheduled_delete_file_cleanup_rows,
    },
};

pub(crate) fn check_mapped_cleanup_rows(kv: &mut FdbOrderedCatalogKv) {
    let catalog = CatalogId(7_607);
    let order = CatalogOrderId::uuid_v7(1);
    let mut batch = KvBatch::new();
    for id in 0..513 {
        batch.put(
            keys::scheduled_data_file_cleanup_key(catalog, DataFileId(id)),
            encode_scheduled_data_cleanup_value(
                ScheduledDataFileCleanupKind::CompactionReplacement,
                id as i64,
            ),
        );
        if id < 512 {
            let path = if id == 256 {
                "x".repeat(70_000)
            } else {
                format!("file-{id}.parquet")
            };
            let row = DataFileRow::new(DataFileId(id), crate::TableId(1), path, 2, 128, order);
            batch.put(keys::data_file_key(catalog, row.data_file_id), row.encode());
        }
    }
    for id in 700..703 {
        batch.put(
            keys::scheduled_delete_file_cleanup_key(catalog, DeleteFileId(id)),
            encode_scheduled_delete_cleanup_value(crate::TableId(1), id as i64),
        );
        if id < 702 {
            let row = DeleteFileRow::new(
                DeleteFileId(id),
                DataFileId(1),
                format!("delete-{id}.parquet"),
                1,
                128,
                order,
            );
            batch.put(
                keys::delete_file_key(catalog, row.delete_file_id),
                row.encode(),
            );
        }
    }
    kv.commit(batch).unwrap();
    let primary_prefix = keys::family_prefix(catalog, keys::KeyFamily::DataFile);
    let ordinary = kv
        .scan_prefix(&primary_prefix, RangeDirection::Forward, usize::MAX)
        .unwrap();
    assert_eq!(ordinary.len(), 512);
    assert_eq!(
        DataFileRow::decode(&ordinary[256].value)
            .unwrap()
            .path
            .len(),
        70_000
    );
    let reverse = kv
        .scan_prefix(&primary_prefix, RangeDirection::Reverse, 2)
        .unwrap();
    assert_eq!(
        reverse
            .iter()
            .map(|row| DataFileRow::decode(&row.value).unwrap().data_file_id.0)
            .collect::<Vec<_>>(),
        vec![511, 510]
    );
    let ids = (0..(2 * crate::constants::FDB_PREFIX_SCAN_CONCURRENCY as u64 + 2))
        .rev()
        .collect::<Vec<_>>();
    let prefixes = ids
        .iter()
        .map(|id| keys::data_file_key(catalog, DataFileId(*id)))
        .collect::<Vec<_>>();
    let pages = kv
        .scan_prefixes(&prefixes, RangeDirection::Forward, 1)
        .unwrap();
    let ranges = prefixes
        .iter()
        .map(|prefix| crate::kv::CatalogReadRange {
            start: prefix.clone(),
            end: prefix_end(prefix),
            direction: RangeDirection::Forward,
            limit: 1,
        })
        .collect::<Vec<_>>();
    let mixed_pages = kv.read_set(&[], &ranges).unwrap().ranges;
    for pages in [pages, mixed_pages] {
        assert_eq!(pages.len(), ids.len());
        for (id, rows) in ids.iter().zip(pages) {
            assert_eq!(rows.len(), usize::from(*id < 512));
            if let Some(row) = rows.first() {
                let file = DataFileRow::decode(&row.value).unwrap();
                assert_eq!(file.data_file_id.0, *id);
                if *id == 256 {
                    assert_eq!(file.path.len(), 70_000);
                }
            }
        }
    }
    let started = Instant::now();
    for _ in 0..20 {
        let rows = list_scheduled_data_file_cleanup_rows(kv, catalog).unwrap();
        assert_eq!(rows.len(), 512);
        assert_eq!(
            rows.iter()
                .map(|row| row.data_file.data_file_id.0)
                .collect::<Vec<_>>(),
            (0..512).collect::<Vec<_>>()
        );
        assert_eq!(rows[256].data_file.path.len(), 70_000);
        assert_eq!(rows[511].schedule_start_micros, 511);
    }
    eprintln!(
        "mapped_cleanup_probe iterations=20 index_rows=513 primary_rows=512 elapsed_us={}",
        started.elapsed().as_micros()
    );
    crate::runtime_metrics::flush_runtime_metrics();
    let deletes = list_scheduled_delete_file_cleanup_rows(kv, catalog).unwrap();
    assert_eq!(
        deletes
            .iter()
            .map(|row| row.delete_file.delete_file_id.0)
            .collect::<Vec<_>>(),
        vec![700, 701]
    );
    assert!(deletes.iter().all(|row| row.table_id == crate::TableId(1)));
    let pinned = kv.with_read_version().unwrap();
    let mut replacement = DataFileRow::new(
        DataFileId(0),
        crate::TableId(1),
        "changed.parquet",
        2,
        128,
        order,
    );
    let mut batch = KvBatch::new();
    batch.put(
        keys::data_file_key(catalog, DataFileId(0)),
        replacement.encode(),
    );
    kv.commit(batch).unwrap();
    assert_eq!(
        list_scheduled_data_file_cleanup_rows(&pinned, catalog).unwrap()[0]
            .data_file
            .path,
        "file-0.parquet"
    );
    assert_eq!(
        list_scheduled_data_file_cleanup_rows(kv, catalog).unwrap()[0]
            .data_file
            .path,
        "changed.parquet"
    );
    if std::env::var("AUX_DUCKLAKE_TEST_MAPPED_NATIVE").as_deref() == Ok("1") {
        check_native_mapped_isolation(kv, catalog, &mut replacement);
        check_namespace_copy_and_cleanup(kv, catalog);
    }
}

fn check_native_mapped_isolation(
    kv: &mut FdbOrderedCatalogKv,
    catalog: CatalogId,
    row: &mut DataFileRow,
) {
    let prefix = kv.namespaced_key(&keys::scheduled_data_file_cleanup_prefix(catalog));
    let mut range = RangeOption::from(prefix.clone()..prefix_end(&prefix));
    range.limit = Some(1);
    let mapper = tuple::pack(&(kv.key_prefix(), "{V[0]}", "{V[1]}", "{...}"));
    let primary = kv.namespaced_key(&keys::data_file_key(catalog, row.data_file_id));
    let transaction = kv.create_transaction().unwrap();
    let mapped = block_on(transaction.get_mapped_range(&range, &mapper, 1, false)).unwrap();
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].key_values().len(), 1);
    assert_eq!(mapped[0].key_values()[0].key(), primary);
    assert_eq!(mapped[0].key_values()[0].value(), row.encode());
    row.path = "conflicting.parquet".to_owned();
    let mut batch = KvBatch::new();
    batch.put(keys::data_file_key(catalog, row.data_file_id), row.encode());
    kv.commit(batch).unwrap();
    transaction.set(
        &kv.namespaced_key(b"mapped-conflict-probe"),
        b"must-conflict",
    );
    let error = block_on(transaction.commit()).unwrap_err();
    assert_eq!(error.code(), 1020);
    let transaction = kv.create_transaction().unwrap();
    transaction.set(&primary, &row.encode());
    assert!(block_on(transaction.get_mapped_range(&range, &mapper, 1, false)).is_err());
    let transaction = kv.create_transaction().unwrap();
    assert!(block_on(transaction.get_mapped_range(&range, &mapper, 1, true)).is_err());
}

fn check_namespace_copy_and_cleanup(kv: &FdbOrderedCatalogKv, catalog: CatalogId) {
    use futures::TryStreamExt;
    let db = crate::fdb_runtime::shared_foundationdb_database(
        std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE")
            .ok()
            .as_deref(),
    )
    .unwrap();
    let destination = format!(
        "{}-copy/",
        String::from_utf8_lossy(kv.key_prefix()).trim_end_matches('/')
    );
    let (source, end) = crate::foundationdb_prefix_bounds(kv.key_prefix()).unwrap();
    let (target, _) = crate::foundationdb_prefix_bounds(destination.as_bytes()).unwrap();
    let transaction = db.create_trx().unwrap();
    let rows = block_on(
        transaction
            .get_ranges_keyvalues(RangeOption::from(source.clone()..end), false)
            .try_collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(!rows.is_empty());
    for row in rows {
        let mut key = target.clone();
        key.extend_from_slice(row.key().strip_prefix(source.as_slice()).unwrap());
        transaction.set(&key, row.value());
    }
    block_on(transaction.commit()).unwrap();
    let clone = FdbOrderedCatalogKv::open_default_with_prefix(destination.as_bytes()).unwrap();
    assert_eq!(
        list_scheduled_data_file_cleanup_rows(&clone, catalog).unwrap(),
        list_scheduled_data_file_cleanup_rows(kv, catalog).unwrap()
    );
    let child_prefix = format!("{destination}child/");
    let mut child = FdbOrderedCatalogKv::open_default_with_prefix(child_prefix.as_bytes()).unwrap();
    let mut batch = KvBatch::new();
    batch.put(b"child".to_vec(), b"exists".to_vec());
    child.commit(batch).unwrap();
    crate::clear_foundationdb_prefix(&db, destination.as_bytes()).unwrap();
    assert_eq!(
        crate::OrderedCatalogKv::get(&child, b"child").unwrap(),
        None
    );
    assert!(
        !list_scheduled_data_file_cleanup_rows(kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_scheduled_data_file_cleanup_rows(&clone, catalog)
            .unwrap()
            .is_empty()
    );
}
