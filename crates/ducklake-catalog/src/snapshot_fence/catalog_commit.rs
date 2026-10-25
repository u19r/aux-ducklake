use foundationdb::{RangeOption, options::ConflictRangeType};
use futures::executor::block_on;

use super::SnapshotFence;
use crate::{
    CatalogError, CatalogId, CatalogResult, FdbOrderedCatalogKv,
    fdb_runtime::map_fdb_error,
    keys::{prefix_end, snapshot_key, snapshot_prefix},
};

impl SnapshotFence {
    pub(crate) fn stage(
        self,
        kv: &FdbOrderedCatalogKv,
        transaction: &foundationdb::Transaction,
        catalog: CatalogId,
    ) -> CatalogResult<()> {
        let start = kv.namespaced_key(&snapshot_prefix(catalog));
        let end = prefix_end(&start);
        transaction
            .add_conflict_range(&start, &end, ConflictRangeType::Read)
            .map_err(map_fdb_error)?;
        let mut range = RangeOption::from((start.as_slice(), end.as_slice()));
        range.limit = Some(1);
        range.reverse = true;
        let rows = block_on(transaction.get_range(&range, 1, false)).map_err(map_fdb_error)?;
        let expected = kv.namespaced_key(&snapshot_key(catalog, self.order()));
        if rows.first().is_none_or(|row| row.key() != expected) {
            return Err(CatalogError::InvalidMutation(
                "conflict committing data mutation: transaction snapshot fence changed".to_owned(),
            ));
        }
        Ok(())
    }
}
