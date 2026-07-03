use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogResult, FdbOrderedCatalogKv, RawSnapshotSequence, SchemaId, SchemaRow,
    SnapshotRow, ValidityWindow,
    conflict_watermarks::stage_fdb_max_catalog_id_watermark,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, estimate_versionstamped_schema_create_bytes, incomplete_order,
        schema_object_key_order_offset, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, versionstamped_value,
    },
    keys::{schema_object_key, snapshot_key, snapshot_timestamp_key},
    schema_store::load_schema_at,
    schema_version_state::stage_fdb_next_schema_version,
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
};

impl FdbOrderedCatalogKv {
    pub fn create_schemas_versionstamped(
        &self,
        catalog: crate::CatalogId,
        mut schemas: Vec<SchemaRow>,
        commit_raw_snapshot: Option<RawSnapshotSequence>,
    ) -> CatalogResult<Vec<SchemaRow>> {
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let latest = latest_snapshot(self, catalog)?;
        let next_sequence = commit_raw_snapshot.unwrap_or_else(|| {
            latest.map_or(RawSnapshotSequence::initial(), |snapshot| {
                snapshot.sequence.next()
            })
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        for schema in &mut schemas {
            schema.validity = ValidityWindow::new(placeholder, None);
        }
        let estimated_bytes =
            estimate_versionstamped_schema_create_bytes(catalog, &snapshot, &schemas);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped schema create is {estimated_bytes} bytes, over {} byte limit",
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
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for schema in &schemas {
            trx.atomic_op(
                &self.versionstamped_key(
                    &schema_object_key(catalog, schema.schema_id, placeholder),
                    schema_object_key_order_offset(catalog, schema.schema_id),
                )?,
                &versionstamped_value(&schema.encode(), SchemaRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedKey,
            );
        }
        if let Some(max_schema_id) = schemas.iter().map(|schema| schema.schema_id.0).max() {
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, max_schema_id);
        }
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        for schema in &mut schemas {
            schema.validity = ValidityWindow::new(order, None);
        }
        Ok(schemas)
    }

    pub fn drop_schemas_versionstamped(
        &self,
        catalog: crate::CatalogId,
        schema_ids: &[SchemaId],
    ) -> CatalogResult<Vec<SchemaRow>> {
        self.drop_schemas_versionstamped_at(catalog, schema_ids, None)
    }

    pub(crate) fn drop_schemas_versionstamped_at(
        &self,
        catalog: crate::CatalogId,
        schema_ids: &[SchemaId],
        commit_raw_snapshot: Option<RawSnapshotSequence>,
    ) -> CatalogResult<Vec<SchemaRow>> {
        if schema_ids.is_empty() {
            return Ok(Vec::new());
        }
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(
            placeholder,
            commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next()),
        );
        let mut schemas = Vec::with_capacity(schema_ids.len());
        for schema_id in schema_ids {
            let mut schema = load_schema_at(self, catalog, *schema_id, latest.order)?
                .ok_or(CatalogError::NotFound("schema"))?;
            schema.validity.end_order = Some(placeholder);
            schemas.push(schema);
        }
        let estimated_bytes =
            estimate_versionstamped_schema_drop_bytes(catalog, &snapshot, &schemas);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped schema drop is {estimated_bytes} bytes, over {} byte limit",
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
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for schema in &schemas {
            trx.atomic_op(
                &self.namespaced_key(&schema_object_key(
                    catalog,
                    schema.schema_id,
                    schema.validity.begin_order,
                )),
                &versionstamped_value(&schema.encode(), SchemaRow::END_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
        }
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        for schema in &mut schemas {
            schema.validity.end_order = Some(order);
        }
        Ok(schemas)
    }

    pub(crate) fn change_schemas_versionstamped_at(
        &self,
        catalog: crate::CatalogId,
        mut created: Vec<SchemaRow>,
        dropped_ids: &[SchemaId],
        commit_raw_snapshot: RawSnapshotSequence,
    ) -> CatalogResult<()> {
        if created.is_empty() && dropped_ids.is_empty() {
            return Ok(());
        }
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, commit_raw_snapshot);
        let mut dropped = Vec::with_capacity(dropped_ids.len());
        for schema_id in dropped_ids {
            let mut schema = load_schema_at(self, catalog, *schema_id, latest.order)?
                .ok_or(CatalogError::NotFound("schema"))?;
            schema.validity.end_order = Some(placeholder);
            dropped.push(schema);
        }
        for schema in &mut created {
            schema.validity = ValidityWindow::new(placeholder, None);
        }
        let estimated_bytes =
            estimate_versionstamped_schema_change_bytes(catalog, &snapshot, &created, &dropped);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped schema change is {estimated_bytes} bytes, over {} byte limit",
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
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for schema in &dropped {
            trx.atomic_op(
                &self.namespaced_key(&schema_object_key(
                    catalog,
                    schema.schema_id,
                    schema.validity.begin_order,
                )),
                &versionstamped_value(&schema.encode(), SchemaRow::END_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
        }
        for schema in &created {
            trx.atomic_op(
                &self.versionstamped_key(
                    &schema_object_key(catalog, schema.schema_id, placeholder),
                    schema_object_key_order_offset(catalog, schema.schema_id),
                )?,
                &versionstamped_value(&schema.encode(), SchemaRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedKey,
            );
        }
        if let Some(max_schema_id) = created.iter().map(|schema| schema.schema_id.0).max() {
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, max_schema_id);
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }
}

fn estimate_versionstamped_schema_drop_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    schemas: &[SchemaRow],
) -> usize {
    let snapshot_bytes = estimate_versionstamped_snapshot_bytes(catalog, snapshot);
    schemas.iter().fold(snapshot_bytes, |bytes, schema| {
        bytes
            .saturating_add(
                schema_object_key(catalog, schema.schema_id, schema.validity.begin_order).len(),
            )
            .saturating_add(schema.encode().len())
    })
}

fn estimate_versionstamped_schema_change_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
    created: &[SchemaRow],
    dropped: &[SchemaRow],
) -> usize {
    let created_bytes = created
        .iter()
        .map(|schema| {
            schema_object_key(catalog, schema.schema_id, schema.validity.begin_order)
                .len()
                .saturating_add(schema.encode().len())
        })
        .sum::<usize>();
    let dropped_bytes = dropped
        .iter()
        .map(|schema| {
            schema_object_key(catalog, schema.schema_id, schema.validity.begin_order)
                .len()
                .saturating_add(schema.encode().len())
        })
        .sum::<usize>();
    estimate_versionstamped_snapshot_bytes(catalog, snapshot)
        .saturating_add(created_bytes)
        .saturating_add(dropped_bytes)
}

fn estimate_versionstamped_snapshot_bytes(
    catalog: crate::CatalogId,
    snapshot: &SnapshotRow,
) -> usize {
    snapshot_key(catalog, snapshot.order)
        .len()
        .saturating_add(snapshot.encode().len())
        .saturating_add(
            snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order).len(),
        )
        .saturating_add(8)
}
