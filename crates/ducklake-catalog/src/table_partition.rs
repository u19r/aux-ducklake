use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, RawSnapshotSequence, TableId,
    TablePartitionRow,
    ids::CatalogOrderId,
    store::latest_snapshot,
    table_store::{load_current_table_row, reject_table_conflicts_since_base},
    table_version_commit::commit_replaced_table_version,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePartitionChange {
    pub table_id: TableId,
    pub partition: Option<TablePartitionRow>,
}

impl TablePartitionChange {
    #[must_use]
    pub fn new(table_id: TableId, partition: Option<TablePartitionRow>) -> Self {
        Self {
            table_id,
            partition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedTablePartition {
    pub table_id: TableId,
    pub previous: Option<TablePartitionRow>,
    pub changed: Option<TablePartitionRow>,
}

pub fn commit_change_table_partition(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    change: &TablePartitionChange,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Option<ChangedTablePartition>> {
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous = load_current_table_row(kv, catalog, change.table_id)?
        .ok_or(CatalogError::NotFound("table"))?;
    if previous.partition == change.partition {
        return Ok(None);
    }

    let mut next = previous.clone();
    next.partition = change.partition.clone();
    commit_replaced_table_version(
        kv,
        catalog,
        change.table_id,
        commit_raw_snapshot
            .map(|sequence| RawSnapshotSequence(sequence.0.saturating_sub(1)))
            .unwrap_or(latest.sequence),
        previous.clone(),
        next,
    )?;
    Ok(Some(ChangedTablePartition {
        table_id: change.table_id,
        previous: previous.partition,
        changed: change.partition.clone(),
    }))
}

pub fn commit_change_table_partition_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    change: &TablePartitionChange,
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Option<ChangedTablePartition>> {
    reject_table_conflicts_since_base(kv, catalog, change.table_id, base_order, through_order)?;
    commit_change_table_partition(kv, catalog, change, commit_raw_snapshot)
}
