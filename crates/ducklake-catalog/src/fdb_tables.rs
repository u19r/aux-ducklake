use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, CommitAttemptId, CommitAttemptRow, FdbOrderedCatalogKv,
    RawSnapshotSequence, SnapshotCommitMetadata, SnapshotRow, TableId, TableRow,
    TableVersionReplacement, ValidityWindow,
    conflict::{commit_attempt_key, load_commit_attempt},
    conflict_watermarks::stage_fdb_max_catalog_id_watermark,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, estimate_versionstamped_table_create_bytes, incomplete_order,
        snapshot_key_order_offset, snapshot_timestamp_key_order_offset,
        table_object_key_order_offset, table_visibility_key_order_offset, versionstamped_value,
    },
    keys::{
        current_table_name_key, current_table_row_key, snapshot_key, snapshot_timestamp_key,
        table_object_key, table_visibility_key,
    },
    schema_version_state::{
        stage_fdb_next_catalog_snapshot_version, stage_fdb_next_schema_version,
    },
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
};

impl FdbOrderedCatalogKv {
    pub fn create_table_versionstamped(
        &self,
        catalog: crate::CatalogId,
        table: TableRow,
        commit_raw_snapshot: Option<RawSnapshotSequence>,
    ) -> CatalogResult<TableRow> {
        let mut tables = self.create_tables_versionstamped(
            catalog,
            vec![table],
            commit_raw_snapshot,
            None,
            None,
        )?;
        tables.pop().ok_or_else(|| {
            CatalogError::InvalidMutation("foundationdb table create returned no table".to_owned())
        })
    }

    pub fn create_tables_versionstamped(
        &self,
        catalog: CatalogId,
        tables: Vec<TableRow>,
        commit_raw_snapshot: Option<RawSnapshotSequence>,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        recovery_id: Option<CommitAttemptId>,
    ) -> CatalogResult<Vec<TableRow>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(created) = recover_committed_tables(self, catalog, recovery_id, &tables)? {
            return Ok(created);
        }

        let latest = latest_snapshot(self, catalog)?;
        let next_sequence = commit_raw_snapshot.unwrap_or_else(|| {
            latest.map_or(RawSnapshotSequence::initial(), |snapshot| {
                snapshot.sequence.next()
            })
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence)
            .with_optional_commit_metadata(commit_metadata);
        let created = tables
            .into_iter()
            .map(|mut table| {
                table.validity = ValidityWindow::new(placeholder, None);
                table
            })
            .collect::<Vec<_>>();
        let estimated_bytes =
            estimate_tables_create_bytes(catalog, &snapshot, recovery_id, &created);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped table create is {estimated_bytes} bytes, over {} byte limit",
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
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_key(catalog, placeholder),
                snapshot_key_order_offset(catalog),
            )?,
            &snapshot.encode(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_timestamp_key(catalog, snapshot.created_at_micros, placeholder),
                snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
            )?,
            &snapshot.sequence.to_be_bytes(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_latest_snapshot_value(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
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
        if let Some(max_table_id) = created.iter().map(|table| table.table_id.0).max() {
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, max_table_id);
        }
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        Ok(created
            .into_iter()
            .map(|mut table| {
                table.validity = ValidityWindow::new(order, None);
                table
            })
            .collect())
    }

    pub(crate) fn commit_table_replacements_versionstamped(
        &self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        self.commit_table_replacements_with_sequence_versionstamped(
            catalog,
            previous_sequence.next(),
            None,
            replacements,
        )
    }

    pub(crate) fn commit_table_replacements_with_sequence_versionstamped(
        &self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        self.commit_table_changes_with_sequence_versionstamped(
            catalog,
            sequence,
            commit_metadata,
            Vec::new(),
            replacements,
        )
    }

    pub(crate) fn commit_table_changes_with_sequence_versionstamped(
        &self,
        catalog: CatalogId,
        sequence: RawSnapshotSequence,
        commit_metadata: Option<&SnapshotCommitMetadata>,
        created: Vec<TableRow>,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        if created.is_empty() && replacements.is_empty() {
            return Ok(());
        }
        let placeholder = incomplete_order();
        let snapshot =
            SnapshotRow::new(placeholder, sequence).with_optional_commit_metadata(commit_metadata);
        let prepared = prepare_replacements(placeholder, replacements);
        let created = prepare_created_tables(placeholder, created);
        let estimated_bytes = estimate_table_change_bytes(catalog, &snapshot, &created, &prepared);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped table change is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_key(catalog, placeholder),
                snapshot_key_order_offset(catalog),
            )?,
            &snapshot.encode(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_timestamp_key(catalog, snapshot.created_at_micros, placeholder),
                snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
            )?,
            &snapshot.sequence.to_be_bytes(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_latest_snapshot_value(self, &trx, catalog, &snapshot)?;
        if !created.is_empty()
            || prepared.iter().any(|replacement| {
                !replacement
                    .previous
                    .same_user_visible_schema_as(&replacement.next)
            })
        {
            stage_fdb_next_schema_version(self, &trx, catalog)?;
        } else {
            stage_fdb_next_catalog_snapshot_version(self, &trx, catalog)?;
        }
        for replacement in &prepared {
            trx.atomic_op(
                &self.namespaced_key(&table_object_key(
                    catalog,
                    replacement.table_id,
                    replacement.previous.validity.begin_order,
                )),
                &versionstamped_value(
                    &replacement.previous.encode(),
                    TableRow::END_ORDER_BYTES_OFFSET,
                )?,
                MutationType::SetVersionstampedValue,
            );
            trx.atomic_op(
                &self.versionstamped_key(
                    &table_object_key(catalog, replacement.table_id, placeholder),
                    table_object_key_order_offset(catalog, replacement.table_id),
                )?,
                &versionstamped_value(
                    &replacement.next.encode(),
                    TableRow::BEGIN_ORDER_BYTES_OFFSET,
                )?,
                MutationType::SetVersionstampedKey,
            );
            stage_current_table_name_replacement(self, &trx, catalog, replacement);
            stage_current_table_row(self, &trx, catalog, &replacement.next)?;
            stage_table_visibility_end(self, &trx, catalog, &replacement.previous)?;
            stage_table_visibility_begin(self, &trx, catalog, &replacement.next)?;
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
        let max_table_id = created
            .iter()
            .map(|table| table.table_id.0)
            .chain(prepared.iter().map(|replacement| replacement.table_id.0))
            .max();
        if let Some(max_table_id) = max_table_id {
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, max_table_id);
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }
}

pub(crate) fn current_table_name_value(table_id: TableId) -> [u8; 8] {
    table_id.0.to_be_bytes()
}

pub(crate) fn current_table_name_value_id(value: &[u8]) -> CatalogResult<TableId> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| CatalogError::Decode("current table name value is invalid".to_owned()))?;
    Ok(TableId(u64::from_be_bytes(bytes)))
}

pub(crate) fn stage_current_table_name(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: &TableRow,
) {
    trx.set(
        &kv.namespaced_key(&current_table_name_key(
            catalog,
            table.schema_id,
            &table.name,
        )),
        &current_table_name_value(table.table_id),
    );
}

pub(crate) fn stage_current_table_row(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: &TableRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.namespaced_key(&current_table_row_key(catalog, table.table_id)),
        &versionstamped_value(&table.encode(), TableRow::BEGIN_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(crate) fn stage_table_visibility_begin(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: &TableRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &table_visibility_key(catalog, table.validity.begin_order, table.table_id),
            table_visibility_key_order_offset(catalog),
        )?,
        &versionstamped_value(&table.encode(), TableRow::BEGIN_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedKey,
    );
    Ok(())
}

pub(crate) fn stage_table_visibility_end(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: &TableRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.namespaced_key(&table_visibility_key(
            catalog,
            table.validity.begin_order,
            table.table_id,
        )),
        &versionstamped_value(&table.encode(), TableRow::END_ORDER_BYTES_OFFSET)?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(crate) fn stage_remove_current_table_name(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table: &TableRow,
) {
    trx.clear(&kv.namespaced_key(&current_table_name_key(
        catalog,
        table.schema_id,
        &table.name,
    )));
}

pub(crate) fn stage_remove_current_table_row(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table_id: TableId,
) {
    trx.clear(&kv.namespaced_key(&current_table_row_key(catalog, table_id)));
}

fn stage_current_table_name_replacement(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    replacement: &TableVersionReplacement,
) {
    let name_changed = replacement.previous.schema_id != replacement.next.schema_id
        || !replacement
            .previous
            .name
            .eq_ignore_ascii_case(&replacement.next.name);
    if name_changed {
        stage_remove_current_table_name(kv, trx, catalog, &replacement.previous);
        stage_current_table_name(kv, trx, catalog, &replacement.next);
    }
}

pub(crate) fn table_metadata_recovery_attempt_id(
    operation_tag: u8,
    _commit_raw_snapshot: Option<RawSnapshotSequence>,
    dropped_table_ids: &[TableId],
    tables: &[TableRow],
) -> Option<CommitAttemptId> {
    if dropped_table_ids.is_empty() && tables.is_empty() {
        return None;
    }
    let mut hash = Fnv1a64::new();
    hash.write_tag(operation_tag);
    for table_id in dropped_table_ids {
        hash.write_tag(1);
        hash.write_u64(table_id.0);
    }
    for table in tables {
        hash.write_tag(2);
        let mut table = table.clone();
        table.table_id = TableId(0);
        table.validity = ValidityWindow::new(crate::CatalogOrderId::uuid_v7(0), None);
        hash.write_bytes(&table.encode());
    }
    Some(CommitAttemptId(
        (u128::from(0x6d65746164617461_u64) << 64) ^ u128::from(hash.finish()),
    ))
}

fn prepare_replacements(
    placeholder: crate::CatalogOrderId,
    replacements: Vec<TableVersionReplacement>,
) -> Vec<TableVersionReplacement> {
    replacements
        .into_iter()
        .map(|replacement| {
            let mut previous = replacement.previous;
            let mut next = replacement.next;
            previous.validity.end_order = Some(placeholder);
            next.validity = ValidityWindow::new(placeholder, None);
            TableVersionReplacement::new(replacement.table_id, previous, next)
        })
        .collect()
}

fn prepare_created_tables(
    placeholder: crate::CatalogOrderId,
    tables: Vec<TableRow>,
) -> Vec<TableRow> {
    tables
        .into_iter()
        .map(|mut table| {
            table.validity = ValidityWindow::new(placeholder, None);
            table
        })
        .collect()
}

fn estimate_table_change_bytes(
    catalog: CatalogId,
    snapshot: &SnapshotRow,
    created: &[TableRow],
    replacements: &[TableVersionReplacement],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let replacement_bytes = replacements
        .iter()
        .map(|replacement| {
            let previous_len = replacement.previous.encode().len();
            let next_len = replacement.next.encode().len();
            table_object_key(
                catalog,
                replacement.table_id,
                replacement.previous.validity.begin_order,
            )
            .len()
            .saturating_add(previous_len)
            .saturating_add(table_object_key(catalog, replacement.table_id, snapshot.order).len())
            .saturating_add(next_len)
            .saturating_add(current_table_row_key(catalog, replacement.table_id).len())
            .saturating_add(next_len)
            .saturating_add(
                table_visibility_key(
                    catalog,
                    replacement.previous.validity.begin_order,
                    replacement.table_id,
                )
                .len(),
            )
            .saturating_add(previous_len)
            .saturating_add(
                table_visibility_key(catalog, snapshot.order, replacement.table_id).len(),
            )
            .saturating_add(next_len)
        })
        .sum::<usize>();
    let created_bytes = created
        .iter()
        .map(|table| estimate_versionstamped_table_create_bytes(catalog, snapshot, table))
        .sum::<usize>();
    snapshot_bytes
        .saturating_add(replacement_bytes)
        .saturating_add(created_bytes)
}

fn recover_committed_tables(
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
            recover_committed_table(kv, catalog, attempt.commit_order, table)?
                .ok_or_else(|| CatalogError::NotFound("committed table"))
        })
        .collect::<CatalogResult<Vec<_>>>()
        .map(Some)
}

fn recover_committed_table(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    commit_order: crate::CatalogOrderId,
    expected: &TableRow,
) -> CatalogResult<Option<TableRow>> {
    Ok(crate::list_tables_at(kv, catalog, commit_order)?
        .into_iter()
        .find(|committed| same_logical_table(committed, expected)))
}

fn same_logical_table(committed: &TableRow, expected: &TableRow) -> bool {
    let mut committed = committed.clone();
    let mut expected = expected.clone();
    committed.table_id = TableId(0);
    expected.table_id = TableId(0);
    committed.validity = ValidityWindow::new(crate::CatalogOrderId::uuid_v7(0), None);
    expected.validity = ValidityWindow::new(crate::CatalogOrderId::uuid_v7(0), None);
    committed.encode() == expected.encode()
}

fn estimate_tables_create_bytes(
    catalog: CatalogId,
    snapshot: &SnapshotRow,
    recovery_id: Option<CommitAttemptId>,
    tables: &[TableRow],
) -> usize {
    let snapshot_bytes = snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8);
    let attempt_bytes = recovery_id.map_or(0, |attempt_id| {
        commit_attempt_key(catalog, attempt_id)
            .len()
            .saturating_add(
                CommitAttemptRow::new(attempt_id, snapshot.order)
                    .encode()
                    .len(),
            )
    });
    let table_bytes = tables
        .iter()
        .map(|table| {
            let row_len = table.encode().len();
            table_object_key(catalog, table.table_id, snapshot.order)
                .len()
                .saturating_add(row_len)
                .saturating_add(current_table_name_key(catalog, table.schema_id, &table.name).len())
                .saturating_add(current_table_row_key(catalog, table.table_id).len())
                .saturating_add(row_len)
                .saturating_add(table_visibility_key(catalog, snapshot.order, table.table_id).len())
                .saturating_add(row_len)
        })
        .sum::<usize>();
    snapshot_bytes
        .saturating_add(attempt_bytes)
        .saturating_add(table_bytes)
}

struct Fnv1a64(u64);

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn finish(self) -> u64 {
        self.0
    }

    fn write_tag(&mut self, tag: u8) {
        self.write_bytes(&[tag]);
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_be_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

#[cfg(test)]
#[path = "fdb_tables_tests.rs"]
mod fdb_tables_tests;
