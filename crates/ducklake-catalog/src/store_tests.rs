#[cfg(test)]
mod tests {
    use crate::{
        CatalogError, CatalogId, FakeOrderedCatalogKv, KvBatch, RawSnapshotSequence, SnapshotRow,
        keys::{latest_snapshot_row_key, snapshot_prefix},
    };

    use super::super::{expire_snapshots, latest_snapshot, stage_snapshot};

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
                "latest lookup should use the maintained latest row"
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
