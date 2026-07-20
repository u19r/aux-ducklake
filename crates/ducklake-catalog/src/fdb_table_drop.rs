use std::{collections::HashSet, ops::Deref};

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, CommitAttemptId, CommitAttemptRow, DataFileRow,
    DeleteFileRow, DroppedTable, FdbOrderedCatalogKv, OrderedCatalogKv, SnapshotRow, TableId,
    TableRow,
    conflict::{commit_attempt_key, load_commit_attempt},
    data_file_store::{current_delete_file_from_index_value, list_current_data_files},
    fdb_data_mutation_staging::{stage_expired_data_file, stage_expired_delete_file},
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_tables::{
        stage_current_table_name, stage_current_table_row, stage_remove_current_table_name,
        stage_remove_current_table_row, stage_table_visibility_begin, stage_table_visibility_end,
    },
    fdb_versionstamp::{
        committed_order, estimate_versionstamped_expire_bytes,
        estimate_versionstamped_table_create_bytes, incomplete_order, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, table_object_key_order_offset, versionstamped_value,
    },
    keys::{
        current_delete_file_key, current_table_name_key, current_table_row_key, snapshot_key,
        snapshot_timestamp_key, table_object_key, table_visibility_key,
    },
    schema_version_state::stage_fdb_next_schema_version,
    store::stage_fdb_latest_snapshot_value,
    table_store::{list_tables_at, load_current_table_row, load_table_at},
};

impl FdbOrderedCatalogKv {
    pub fn drop_tables_versionstamped(
        &self,
        catalog: CatalogId,
        table_ids: &[TableId],
    ) -> CatalogResult<Vec<DroppedTable>> {
        self.drop_tables_versionstamped_at(catalog, table_ids, None)
    }

    pub fn drop_tables_versionstamped_at(
        &self,
        catalog: CatalogId,
        table_ids: &[TableId],
        commit_raw_snapshot: Option<crate::RawSnapshotSequence>,
    ) -> CatalogResult<Vec<DroppedTable>> {
        if table_ids.is_empty() {
            return Ok(Vec::new());
        }
        reject_duplicate_table_ids(table_ids)?;
        let latest = crate::latest_snapshot(self, catalog)?
            .ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let placeholder = incomplete_order();
        let sequence = commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next());
        let snapshot = SnapshotRow::new(placeholder, sequence);
        let mut drops = Vec::with_capacity(table_ids.len());

        for table_id in table_ids {
            let table = load_current_table_row(self, catalog, *table_id)?
                .ok_or(CatalogError::NotFound("table"))?;
            drops.push(prepare_table_drop(self, catalog, table, placeholder)?);
        }
        let estimated_bytes = estimate_drop_bytes(catalog, &snapshot, &drops);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped table drop is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for drop in &drops {
            stage_drop(self, &trx, catalog, drop)?;
        }

        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        Ok(drops
            .into_iter()
            .map(|drop| drop.into_dropped_table(order))
            .collect())
    }

    pub fn replace_tables_versionstamped(
        &self,
        catalog: CatalogId,
        table_ids: &[TableId],
        tables: Vec<TableRow>,
        commit_raw_snapshot: Option<crate::RawSnapshotSequence>,
    ) -> CatalogResult<Vec<TableRow>> {
        self.replace_tables_versionstamped_recoverable(
            catalog,
            table_ids,
            tables,
            commit_raw_snapshot,
            None,
        )
    }

    pub fn replace_tables_versionstamped_recoverable(
        &self,
        catalog: CatalogId,
        table_ids: &[TableId],
        tables: Vec<TableRow>,
        commit_raw_snapshot: Option<crate::RawSnapshotSequence>,
        recovery_id: Option<CommitAttemptId>,
    ) -> CatalogResult<Vec<TableRow>> {
        if table_ids.is_empty() && tables.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(created) = recover_committed_replacement(self, catalog, recovery_id, &tables)? {
            return Ok(created);
        }
        reject_duplicate_table_ids(table_ids)?;
        let latest = crate::latest_snapshot(self, catalog)?
            .ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let next_sequence = commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next());
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        let mut drops = Vec::with_capacity(table_ids.len());

        for table_id in table_ids {
            let table = load_current_table_row(self, catalog, *table_id)?
                .ok_or(CatalogError::NotFound("table"))?;
            drops.push(prepare_table_drop(self, catalog, table, placeholder)?);
        }
        reject_replacement_create_conflicts(self, catalog, latest.order, &drops, &tables)?;

        let created = tables
            .into_iter()
            .map(|mut table| {
                table.validity = crate::ValidityWindow::new(placeholder, None);
                table
            })
            .collect::<Vec<_>>();
        let estimated_bytes = estimate_drop_bytes(catalog, &snapshot, &drops).saturating_add(
            created
                .iter()
                .map(|table| estimate_versionstamped_table_create_bytes(catalog, &snapshot, table))
                .sum::<usize>(),
        );
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped table replacement is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        if let Some(recovery_id) = recovery_id {
            let attempt = CommitAttemptRow::new(recovery_id, placeholder);
            trx.atomic_op(
                &self.namespaced_key(&commit_attempt_key(catalog, recovery_id)),
                &versionstamped_value(
                    &attempt.encode(),
                    CommitAttemptRow::COMMIT_ORDER_BYTES_OFFSET,
                )?,
                MutationType::SetVersionstampedValue,
            );
        }
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for drop in &drops {
            stage_drop(self, &trx, catalog, drop)?;
        }
        for table in &created {
            trx.atomic_op(
                &self.versionstamped_key(
                    &table_object_key(catalog, table.table_id, placeholder),
                    table_object_key_order_offset(catalog, table.table_id),
                )?,
                &versionstamped_value(&table.encode(), TableRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedKey,
            );
            stage_current_table_name(self, &trx, catalog, table);
            stage_current_table_row(self, &trx, catalog, table)?;
            stage_table_visibility_begin(self, &trx, catalog, table)?;
        }

        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        Ok(created
            .into_iter()
            .map(|mut table| {
                table.validity = crate::ValidityWindow::new(order, None);
                table
            })
            .collect())
    }
}

fn recover_committed_replacement(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    recovery_id: Option<CommitAttemptId>,
    tables: &[TableRow],
) -> CatalogResult<Option<Vec<TableRow>>> {
    let Some(recovery_id) = recovery_id else {
        return Ok(None);
    };
    let Some(attempt) = load_commit_attempt(kv, catalog, recovery_id)? else {
        return Ok(None);
    };
    tables
        .iter()
        .map(|table| {
            load_table_at(kv, catalog, table.table_id, attempt.commit_order)?
                .ok_or(CatalogError::NotFound("committed replacement table"))
        })
        .collect::<CatalogResult<Vec<_>>>()
        .map(Some)
}

fn reject_replacement_create_conflicts(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    latest_order: crate::CatalogOrderId,
    drops: &[PreparedTableDrop],
    tables: &[TableRow],
) -> CatalogResult<()> {
    let current = list_tables_at(kv, catalog, latest_order)?;
    for table in tables {
        for existing in &current {
            if drops
                .iter()
                .any(|drop| drop.table.table_id == existing.table_id)
            {
                continue;
            }
            if existing.table_id == table.table_id {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: table id {} already exists",
                    table.name, table.table_id.0
                )));
            }
            if existing.schema_id == table.schema_id
                && existing.name.eq_ignore_ascii_case(&table.name)
            {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: table name already exists in schema {}",
                    table.name, table.schema_id.0
                )));
            }
        }
    }
    Ok(())
}

struct PreparedTableDrop {
    table: TableRow,
    data_files: Vec<DataFileRow>,
    delete_files: Vec<DeleteFileRow>,
}

impl PreparedTableDrop {
    fn new(
        mut table: TableRow,
        files: Vec<DataFileRow>,
        delete_files: Vec<DeleteFileRow>,
        placeholder: crate::CatalogOrderId,
    ) -> Self {
        table.validity.end_order = Some(placeholder);
        let mut data_files = Vec::with_capacity(files.len());
        for file in files {
            let mut data_file = file;
            data_file.validity.end_order = Some(placeholder);
            data_files.push(data_file);
        }
        let delete_files = delete_files
            .into_iter()
            .map(|mut delete_file| {
                delete_file.validity.end_order = Some(placeholder);
                delete_file
            })
            .collect();
        Self {
            table,
            data_files,
            delete_files,
        }
    }

    fn into_dropped_table(mut self, order: crate::CatalogOrderId) -> DroppedTable {
        self.table.validity.end_order = Some(order);
        DroppedTable {
            table: self.table,
            expired_data_file_count: self.data_files.len(),
            expired_delete_file_count: self.delete_files.len(),
        }
    }
}

fn prepare_table_drop(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table: TableRow,
    placeholder: crate::CatalogOrderId,
) -> CatalogResult<PreparedTableDrop> {
    let data_files = list_current_data_files(kv, catalog, table.table_id)?;
    let delete_files = current_delete_files_for_data_files(kv, catalog, &data_files)?;
    Ok(PreparedTableDrop::new(
        table,
        data_files,
        delete_files,
        placeholder,
    ))
}

fn current_delete_files_for_data_files(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    data_files: &[DataFileRow],
) -> CatalogResult<Vec<DeleteFileRow>> {
    if data_files.is_empty() {
        return Ok(Vec::new());
    }
    let keys = data_files
        .iter()
        .map(|file| current_delete_file_key(catalog, file.data_file_id))
        .collect::<Vec<_>>();
    kv.batch_get(&keys)?
        .into_iter()
        .flatten()
        .map(|value| current_delete_file_from_index_value(kv, catalog, &value))
        .collect()
}

fn stage_snapshot(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_key(catalog, snapshot.order),
            snapshot_key_order_offset(catalog),
        )?,
        &snapshot.encode(),
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
        )?,
        &snapshot.sequence.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    stage_fdb_latest_snapshot_value(kv, trx, catalog, snapshot)?;
    Ok(())
}

fn stage_drop(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    drop: &PreparedTableDrop,
) -> CatalogResult<()> {
    for row in &drop.data_files {
        stage_expired_data_file(kv, trx, catalog, row)?;
    }
    for row in &drop.delete_files {
        stage_expired_delete_file(kv, trx, catalog, drop.table.table_id, row)?;
    }
    trx.atomic_op(
        &kv.namespaced_key(&table_object_key(
            catalog,
            drop.table.table_id,
            drop.table.validity.begin_order,
        )),
        &versionstamped_value(&drop.table.encode(), TableRow::END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    stage_table_visibility_end(kv, trx, catalog, &drop.table)?;
    stage_remove_current_table_name(kv, trx, catalog, &drop.table);
    stage_remove_current_table_row(kv, trx, catalog, drop.table.table_id);
    Ok(())
}

fn estimate_drop_bytes(
    catalog: CatalogId,
    snapshot: &SnapshotRow,
    drops: &[PreparedTableDrop],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    drops.iter().fold(snapshot_bytes, |bytes, drop| {
        bytes
            .saturating_add(table_drop_bytes(catalog, &drop.table))
            .saturating_add(
                drop.data_files
                    .iter()
                    .map(|row| estimate_versionstamped_expire_bytes(catalog, row))
                    .sum::<usize>(),
            )
            .saturating_add(
                drop.delete_files
                    .iter()
                    .map(|row| estimate_delete_file_expire_bytes(catalog, drop.table.table_id, row))
                    .sum::<usize>(),
            )
    })
}

fn table_drop_bytes(catalog: CatalogId, table: &TableRow) -> usize {
    let row_len = table.encode().len();
    table_object_key(catalog, table.table_id, table.validity.begin_order)
        .len()
        .saturating_add(row_len)
        .saturating_add(
            table_visibility_key(catalog, table.validity.begin_order, table.table_id).len(),
        )
        .saturating_add(row_len)
        .saturating_add(current_table_name_key(catalog, table.schema_id, &table.name).len())
        .saturating_add(current_table_row_key(catalog, table.table_id).len())
}

fn estimate_delete_file_expire_bytes(
    catalog: CatalogId,
    table_id: TableId,
    row: &DeleteFileRow,
) -> usize {
    crate::keys::delete_file_key(catalog, row.delete_file_id)
        .len()
        .saturating_add(row.encode().len())
        .saturating_add(crate::keys::current_delete_file_key(catalog, row.data_file_id).len())
        .saturating_add(
            crate::keys::delete_file_end_key(
                catalog,
                table_id,
                row.validity.end_order.unwrap_or_else(incomplete_order),
                row.delete_file_id,
            )
            .len(),
        )
}

fn reject_duplicate_table_ids(table_ids: &[TableId]) -> CatalogResult<()> {
    let mut seen = HashSet::with_capacity(table_ids.len());
    for table_id in table_ids {
        if !seen.insert(*table_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "table {} is listed more than once for drop",
                table_id.0
            )));
        }
    }
    Ok(())
}
