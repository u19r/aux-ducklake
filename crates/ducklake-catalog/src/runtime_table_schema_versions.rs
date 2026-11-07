use crate::{
    CatalogError, CatalogId, CatalogResult, DuckLakeSnapshotId, OrderedCatalogKv, TableId,
    runtime_snapshots::snapshot_by_ducklake_sequence, schema_version_state::load_schema_version_at,
    table_store::list_table_rows_for_table,
};

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TableSchemaVersions {
    pub(super) creation: Option<u64>,
    pub(super) last_change: Option<u64>,
}

impl TableSchemaVersions {
    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        snapshot_id: DuckLakeSnapshotId,
    ) -> CatalogResult<Self> {
        let snapshot = snapshot_by_ducklake_sequence(kv, catalog, snapshot_id)?
            .ok_or(CatalogError::NotFound("table schema snapshot"))?;
        let mut rows = list_table_rows_for_table(kv, catalog, table_id)?;
        if !rows
            .iter()
            .any(|row| row.validity.is_visible_at(snapshot.order))
        {
            return Ok(Self::default());
        }
        rows.retain(|row| row.validity.begin_order <= snapshot.order);
        rows.sort_by_key(|row| row.validity.begin_order);
        let Some(first) = rows.first() else {
            return Ok(Self::default());
        };
        let last_change = rows
            .windows(2)
            .rev()
            .find(|pair| has_schema_change(&pair[0], &pair[1]))
            .map_or(first.validity.begin_order, |pair| {
                pair[1].validity.begin_order
            });
        Ok(Self {
            creation: Some(load_schema_version_at(
                kv,
                catalog,
                first.validity.begin_order,
            )?),
            last_change: Some(load_schema_version_at(kv, catalog, last_change)?),
        })
    }
}

fn has_schema_change(previous: &crate::TableRow, current: &crate::TableRow) -> bool {
    previous.partition != current.partition
        || previous.columns.len() != current.columns.len()
        || previous
            .columns
            .iter()
            .zip(&current.columns)
            .any(|(left, right)| {
                left.column_id != right.column_id
                    || left.name != right.name
                    || left.column_type != right.column_type
                    || left.nulls_allowed != right.nulls_allowed
                    || left.parent_id != right.parent_id
                    || left.initial_default != right.initial_default
                    || left.default_value != right.default_value
                    || left.default_value_type != right.default_value_type
            })
}
