#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        CatalogCacheNamespace, CatalogError, CatalogId, FakeOrderedCatalogKv, KvBatch,
        RangeDirection, RawSnapshotSequence, SnapshotRow,
        keys::{latest_snapshot_row_key, snapshot_prefix},
    };

    use super::super::{
        expire_snapshots, latest_snapshot, latest_snapshot_value, list_snapshots,
        snapshot_by_raw_sequence, stage_snapshot,
    };

    #[test]
    fn given_staged_snapshots_when_loading_latest_then_maintained_latest_row_is_used() {
        let catalog = CatalogId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        let snapshot_one = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(1));
        let snapshot_two = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(2));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot_one);
        stage_snapshot(&mut batch, catalog, &snapshot_two);
        kv.commit(batch).unwrap();

        assert!(
            crate::OrderedCatalogKv::get(&kv, &latest_snapshot_row_key(catalog))
                .unwrap()
                .is_some(),
            "snapshot commits must maintain the bounded latest-snapshot lookup row"
        );

        let kv = LatestSnapshotScanRejectingKv::new(kv, catalog);
        assert_eq!(
            latest_snapshot(&kv, catalog).unwrap(),
            Some(snapshot_two),
            "latest snapshot should not need a reverse scan over snapshot history"
        );
    }

    #[test]
    fn given_many_snapshots_when_loading_raw_sequence_then_history_is_not_scanned() {
        let catalog = CatalogId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        let first = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(1));
        let helper = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(1));
        let latest = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(2));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &first);
        stage_snapshot(&mut batch, catalog, &helper);
        stage_snapshot(&mut batch, catalog, &latest);
        kv.commit(batch).unwrap();

        let kv = LatestSnapshotScanRejectingKv::new(kv, catalog);
        assert_eq!(
            snapshot_by_raw_sequence(&kv, catalog, first.sequence).unwrap(),
            Some(helper.clone()),
            "the exact index must retain the newest helper snapshot for a raw sequence"
        );

        let mut kv = kv.inner;
        assert_eq!(
            expire_snapshots(&mut kv, catalog, &[first.sequence]).unwrap(),
            vec![first.clone(), helper.clone()]
        );
        assert!(
            list_snapshots(&kv, catalog)
                .unwrap()
                .iter()
                .all(|snapshot| snapshot.sequence != first.sequence)
        );
        assert_eq!(
            snapshot_by_raw_sequence(&kv, catalog, first.sequence).unwrap(),
            Some(helper),
            "expiry must retain raw history for internal snapshot resolution"
        );
    }

    #[test]
    fn given_one_runtime_request_when_loading_latest_repeatedly_then_storage_is_read_once() {
        let catalog = CatalogId(0);
        let mut inner = FakeOrderedCatalogKv::new();
        let snapshot = SnapshotRow::new(inner.generated_order_id(), RawSnapshotSequence(1));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot);
        inner.commit(batch).unwrap();
        let kv = LatestSnapshotGetCountingKv::new(inner, catalog);

        {
            let _request = super::super::begin_runtime_read_request(None);
            assert_eq!(
                latest_snapshot(&kv, catalog).unwrap(),
                Some(snapshot.clone())
            );
            assert_eq!(
                latest_snapshot(&kv, catalog).unwrap(),
                Some(snapshot.clone())
            );
            assert_eq!(kv.latest_get_count.get(), 1);
        }

        let _next_request = super::super::begin_runtime_read_request(None);
        assert_eq!(latest_snapshot(&kv, catalog).unwrap(), Some(snapshot));
        assert_eq!(
            kv.latest_get_count.get(),
            2,
            "request-local values must not be reused by a later FFI request"
        );
    }

    #[test]
    fn given_one_commit_read_context_when_requests_repeat_then_latest_snapshot_is_shared() {
        let catalog = CatalogId(0);
        let mut inner = FakeOrderedCatalogKv::new();
        let snapshot = SnapshotRow::new(inner.generated_order_id(), RawSnapshotSequence(1));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot);
        inner.commit(batch).unwrap();
        let kv = LatestSnapshotGetCountingKv::new(inner, catalog);

        {
            let _first_request = super::super::begin_runtime_read_request(Some(9_001));
            assert_eq!(
                latest_snapshot(&kv, catalog).unwrap(),
                Some(snapshot.clone())
            );
        }
        {
            let _second_request = super::super::begin_runtime_read_request(Some(9_001));
            assert_eq!(
                latest_snapshot(&kv, catalog).unwrap(),
                Some(snapshot.clone())
            );
        }
        assert_eq!(kv.latest_get_count.get(), 1);

        let _different_context = super::super::begin_runtime_read_request(Some(9_002));
        assert_eq!(latest_snapshot(&kv, catalog).unwrap(), Some(snapshot));
        assert_eq!(kv.latest_get_count.get(), 2);
    }

    #[test]
    fn given_context_cached_on_one_thread_when_worker_continues_then_global_snapshot_is_reused() {
        let catalog = CatalogId(0);
        let mut inner = FakeOrderedCatalogKv::new();
        let snapshot = SnapshotRow::new(inner.generated_order_id(), RawSnapshotSequence(1));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot);
        inner.commit(batch).unwrap();
        let kv = ThreadSafeLatestSnapshotGetCountingKv::new(inner, catalog);
        let context_id = 9_004_001;

        {
            let _request = super::super::begin_runtime_read_request(Some(context_id));
            assert_eq!(
                latest_snapshot(&kv, catalog).unwrap(),
                Some(snapshot.clone())
            );
        }

        let worker_kv = kv.clone();
        let worker_snapshot = std::thread::spawn(move || {
            let _request = super::super::begin_runtime_read_request(Some(context_id));
            latest_snapshot(&worker_kv, catalog)
        })
        .join()
        .unwrap()
        .unwrap();

        assert_eq!(worker_snapshot, Some(snapshot));
        assert_eq!(kv.latest_get_count.load(Ordering::Relaxed), 1);
        super::super::end_runtime_read_context(kv.catalog_cache_namespace, catalog, context_id);
    }

    #[test]
    fn given_concurrent_first_context_reads_then_first_snapshot_wins_for_both_workers() {
        let catalog = CatalogId(0);
        let mut inner = FakeOrderedCatalogKv::new();
        let first = SnapshotRow::new(inner.generated_order_id(), RawSnapshotSequence(1));
        let second = SnapshotRow::new(inner.generated_order_id(), RawSnapshotSequence(2));
        let context_id = 9_004_002;
        let kv = BarrierLatestSnapshotKv::new(inner, catalog, &first, &second);

        let first_kv = kv.clone();
        let first_worker = std::thread::spawn(move || {
            let _request = super::super::begin_runtime_read_request(Some(context_id));
            latest_snapshot(&first_kv, catalog)
        });
        let second_kv = kv.clone();
        let second_worker = std::thread::spawn(move || {
            let _request = super::super::begin_runtime_read_request(Some(context_id));
            latest_snapshot(&second_kv, catalog)
        });

        let first_result = first_worker.join().unwrap().unwrap();
        let second_result = second_worker.join().unwrap().unwrap();
        assert_eq!(first_result, second_result);
        assert!(first_result == Some(first) || first_result == Some(second));
        assert_eq!(kv.latest_get_count.load(Ordering::Relaxed), 2);

        super::super::end_runtime_read_context(kv.catalog_cache_namespace, catalog, context_id);
    }

    #[test]
    fn given_snapshot_already_expired_when_expiring_again_then_missing_non_latest_is_ignored() {
        let catalog = CatalogId(0);
        let mut kv = FakeOrderedCatalogKv::new();
        let snapshot_one = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(1));
        let snapshot_two = SnapshotRow::new(kv.generated_order_id(), RawSnapshotSequence(2));
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot_one);
        stage_snapshot(&mut batch, catalog, &snapshot_two);
        kv.commit(batch).unwrap();

        let first_expire = expire_snapshots(&mut kv, catalog, &[snapshot_one.sequence]).unwrap();
        assert_eq!(first_expire, vec![snapshot_one.clone()]);

        let second_expire = expire_snapshots(&mut kv, catalog, &[snapshot_one.sequence]).unwrap();
        assert!(second_expire.is_empty());
        assert_eq!(
            latest_snapshot(&kv, catalog).unwrap(),
            Some(snapshot_two.clone())
        );

        let latest_error = expire_snapshots(&mut kv, catalog, &[snapshot_two.sequence])
            .expect_err("latest snapshot must not be expired");
        assert!(matches!(latest_error, CatalogError::InvalidMutation(_)));
    }

    struct LatestSnapshotScanRejectingKv {
        inner: FakeOrderedCatalogKv,
        snapshot_prefix: Vec<u8>,
    }

    struct LatestSnapshotGetCountingKv {
        inner: FakeOrderedCatalogKv,
        latest_key: Vec<u8>,
        latest_get_count: Cell<usize>,
    }

    #[derive(Clone)]
    struct BarrierLatestSnapshotKv {
        inner: Arc<Mutex<FakeOrderedCatalogKv>>,
        latest_key: Vec<u8>,
        first_value: Vec<u8>,
        second_value: Vec<u8>,
        barrier: Arc<Barrier>,
        latest_get_count: Arc<AtomicUsize>,
        catalog_cache_namespace: CatalogCacheNamespace,
    }

    impl BarrierLatestSnapshotKv {
        fn new(
            inner: FakeOrderedCatalogKv,
            catalog: CatalogId,
            first: &SnapshotRow,
            second: &SnapshotRow,
        ) -> Self {
            Self {
                inner: Arc::new(Mutex::new(inner)),
                latest_key: latest_snapshot_row_key(catalog),
                first_value: latest_snapshot_value(first),
                second_value: latest_snapshot_value(second),
                barrier: Arc::new(Barrier::new(2)),
                latest_get_count: Arc::new(AtomicUsize::new(0)),
                catalog_cache_namespace: CatalogCacheNamespace::process_local(0x9_004_002),
            }
        }

        fn with_inner<T>(
            &self,
            read: impl FnOnce(&FakeOrderedCatalogKv) -> T,
        ) -> crate::CatalogResult<T> {
            let inner = self.inner.lock().map_err(|_| {
                crate::CatalogError::Backend("barrier test catalog mutex was poisoned".to_owned())
            })?;
            Ok(read(&inner))
        }
    }

    impl crate::OrderedCatalogKv for BarrierLatestSnapshotKv {
        fn catalog_cache_namespace(&self) -> CatalogCacheNamespace {
            self.catalog_cache_namespace
        }

        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if key == self.latest_key {
                let index = self.latest_get_count.fetch_add(1, Ordering::Relaxed);
                self.barrier.wait();
                return Ok(Some(if index == 0 {
                    self.first_value.clone()
                } else {
                    self.second_value.clone()
                }));
            }
            self.with_inner(|inner| crate::OrderedCatalogKv::get(inner, key))?
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            self.with_inner(|inner| {
                crate::OrderedCatalogKv::scan_prefix(inner, prefix, direction, limit)
            })?
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            self.with_inner(|inner| {
                crate::OrderedCatalogKv::scan_range(inner, start, end, direction, limit)
            })?
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            self.with_inner(|inner| crate::OrderedCatalogKv::read_conflict_fence(inner, key))?
        }
    }

    #[derive(Clone)]
    struct ThreadSafeLatestSnapshotGetCountingKv {
        inner: Arc<Mutex<FakeOrderedCatalogKv>>,
        latest_key: Vec<u8>,
        latest_get_count: Arc<AtomicUsize>,
        catalog_cache_namespace: CatalogCacheNamespace,
    }

    impl ThreadSafeLatestSnapshotGetCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            let catalog_cache_namespace = crate::OrderedCatalogKv::catalog_cache_namespace(&inner);
            Self {
                inner: Arc::new(Mutex::new(inner)),
                latest_key: latest_snapshot_row_key(catalog),
                latest_get_count: Arc::new(AtomicUsize::new(0)),
                catalog_cache_namespace,
            }
        }

        fn with_inner<T>(
            &self,
            read: impl FnOnce(&FakeOrderedCatalogKv) -> T,
        ) -> crate::CatalogResult<T> {
            let inner = self.inner.lock().map_err(|_| {
                crate::CatalogError::Backend(
                    "thread-safe test catalog mutex was poisoned".to_owned(),
                )
            })?;
            Ok(read(&inner))
        }
    }

    impl crate::OrderedCatalogKv for ThreadSafeLatestSnapshotGetCountingKv {
        fn catalog_cache_namespace(&self) -> CatalogCacheNamespace {
            self.catalog_cache_namespace
        }

        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if key == self.latest_key {
                self.latest_get_count.fetch_add(1, Ordering::Relaxed);
            }
            self.with_inner(|inner| crate::OrderedCatalogKv::get(inner, key))?
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            self.with_inner(|inner| {
                crate::OrderedCatalogKv::scan_prefix(inner, prefix, direction, limit)
            })?
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            self.with_inner(|inner| {
                crate::OrderedCatalogKv::scan_range(inner, start, end, direction, limit)
            })?
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            self.with_inner(|inner| crate::OrderedCatalogKv::read_conflict_fence(inner, key))?
        }
    }

    impl LatestSnapshotGetCountingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                latest_key: latest_snapshot_row_key(catalog),
                latest_get_count: Cell::new(0),
            }
        }
    }

    impl crate::OrderedCatalogKv for LatestSnapshotGetCountingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            if key == self.latest_key {
                self.latest_get_count
                    .set(self.latest_get_count.get().saturating_add(1));
            }
            crate::OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            crate::OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            crate::OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }

    impl LatestSnapshotScanRejectingKv {
        fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
            Self {
                inner,
                snapshot_prefix: snapshot_prefix(catalog),
            }
        }
    }

    impl crate::OrderedCatalogKv for LatestSnapshotScanRejectingKv {
        fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::get(&self.inner, key)
        }

        fn scan_prefix(
            &self,
            prefix: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            assert_ne!(
                prefix,
                self.snapshot_prefix.as_slice(),
                "exact snapshot lookups should not scan snapshot history"
            );
            crate::OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
        }

        fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            direction: crate::RangeDirection,
            limit: usize,
        ) -> crate::CatalogResult<Vec<crate::RangeItem>> {
            crate::OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
        }

        fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
            crate::OrderedCatalogKv::read_conflict_fence(&self.inner, key)
        }
    }
}
