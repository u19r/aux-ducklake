use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, DroppedMacro, FdbOrderedCatalogKv, MacroId, MacroRow,
    RawSnapshotSequence, SnapshotRow, ValidityWindow,
    conflict_watermarks::stage_fdb_max_catalog_id_watermark,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, incomplete_order, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, versionstamped_value,
    },
    keys::{macro_object_key, macro_object_prefix, snapshot_key, snapshot_timestamp_key},
    macro_store::{load_macro_at, reject_macro_create_conflicts},
    schema_version_state::stage_fdb_next_schema_version,
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
};

impl FdbOrderedCatalogKv {
    pub fn create_macros_versionstamped(
        &self,
        catalog: CatalogId,
        mut macros: Vec<MacroRow>,
        commit_raw_snapshot: Option<RawSnapshotSequence>,
    ) -> CatalogResult<Vec<MacroRow>> {
        if macros.is_empty() {
            return Ok(Vec::new());
        }
        let latest = latest_snapshot(self, catalog)?;
        if let Some(latest) = &latest {
            reject_macro_create_conflicts(self, catalog, latest.order, &macros)?;
        }
        let next_sequence = commit_raw_snapshot.unwrap_or_else(|| {
            latest.map_or(RawSnapshotSequence::initial(), |snapshot| {
                snapshot.sequence.next()
            })
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        for macro_row in &mut macros {
            macro_row.validity = ValidityWindow::new(placeholder, None);
        }

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for macro_row in &macros {
            trx.atomic_op(
                &self.versionstamped_key(
                    &macro_object_key(catalog, macro_row.macro_id, placeholder),
                    macro_object_key_order_offset(catalog, macro_row.macro_id),
                )?,
                &macro_row.encode(),
                MutationType::SetVersionstampedKey,
            );
        }
        if let Some(max_macro_id) = macros.iter().map(|macro_row| macro_row.macro_id.0).max() {
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, max_macro_id);
        }
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        for macro_row in &mut macros {
            macro_row.validity = ValidityWindow::new(order, None);
        }
        Ok(macros)
    }

    pub fn drop_macros_versionstamped(
        &self,
        catalog: CatalogId,
        macro_ids: &[MacroId],
        commit_raw_snapshot: Option<RawSnapshotSequence>,
    ) -> CatalogResult<Vec<DroppedMacro>> {
        if macro_ids.is_empty() {
            return Ok(Vec::new());
        }
        reject_duplicate_macro_ids(macro_ids)?;
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let mut dropped = Vec::with_capacity(macro_ids.len());
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(
            placeholder,
            commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next()),
        );

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for macro_id in macro_ids {
            let mut macro_row =
                load_macro_at(self, catalog, *macro_id, latest.order)?.ok_or_else(|| {
                    CatalogError::InvalidMutation(format!(
                        "conflict dropping macro {}: macro no longer exists",
                        macro_id.0
                    ))
                })?;
            macro_row.validity.end_order = Some(placeholder);
            trx.atomic_op(
                &self.namespaced_key(&macro_object_key(
                    catalog,
                    macro_row.macro_id,
                    macro_row.validity.begin_order,
                )),
                &versionstamped_value(&macro_row.encode(), MacroRow::END_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            dropped.push(DroppedMacro { macro_row });
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(dropped)
    }
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

fn macro_object_key_order_offset(catalog: CatalogId, macro_id: MacroId) -> usize {
    macro_object_prefix(catalog, macro_id).len()
}

fn reject_duplicate_macro_ids(macro_ids: &[MacroId]) -> CatalogResult<()> {
    for (index, macro_id) in macro_ids.iter().enumerate() {
        if macro_ids[..index].iter().any(|prior| prior == macro_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "macro {} is listed more than once for drop",
                macro_id.0
            )));
        }
    }
    Ok(())
}
