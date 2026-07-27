use std::ops::Deref;

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, FdbOrderedCatalogKv, SchemaRow, SnapshotRow, TableRow,
    ValidityWindow,
    conflict_watermarks::stage_fdb_max_catalog_id_watermark,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_schemas::estimate_versionstamped_schema_change_bytes,
    fdb_table_drop::{
        estimate_drop_bytes, prepare_table_drop, reject_replacement_create_conflicts, stage_drop,
    },
    fdb_tables::{
        estimate_table_change_bytes, prepare_created_tables, prepare_replacements,
        stage_current_table_name, stage_current_table_name_replacement, stage_current_table_row,
        stage_table_visibility_begin, stage_table_visibility_end,
    },
    fdb_versionstamp::{
        committed_order, incomplete_order, schema_object_key_order_offset,
        table_object_key_order_offset, versionstamped_value,
    },
    fdb_views::{
        estimate_view_change_bytes, prepare_view_changes, stage_snapshot, stage_view_changes,
    },
    keys::{schema_object_key, table_object_key},
    runtime_commit_attempt_ops::table_commit::MetadataCommitChanges,
    schema_store::load_schema_at,
    schema_version_state::{
        stage_fdb_next_catalog_snapshot_version, stage_fdb_next_schema_version,
    },
    store::latest_snapshot,
    table_store::{list_tables_at, load_current_table_row},
    view_store::list_views_at,
};

impl FdbOrderedCatalogKv {
    pub(crate) fn commit_metadata_changes_versionstamped(
        &self,
        catalog: CatalogId,
        changes: MetadataCommitChanges,
    ) -> CatalogResult<Vec<TableRow>> {
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, changes.sequence)
            .with_optional_commit_metadata(changes.commit_metadata.as_ref());

        let mut dropped_schemas = Vec::with_capacity(changes.dropped_schema_ids.len());
        for schema_id in &changes.dropped_schema_ids {
            let mut schema = load_schema_at(self, catalog, *schema_id, latest.order)?
                .ok_or(CatalogError::NotFound("schema"))?;
            schema.validity.end_order = Some(placeholder);
            dropped_schemas.push(schema);
        }
        let mut created_schemas = changes.created_schemas;
        for schema in &mut created_schemas {
            schema.validity = ValidityWindow::new(placeholder, None);
        }

        let table_replacements = prepare_replacements(placeholder, changes.table_replacements);
        let created_tables = prepare_created_tables(placeholder, changes.created_tables);

        let mut table_drops = Vec::with_capacity(changes.dropped_table_ids.len());
        for table_id in &changes.dropped_table_ids {
            let table = load_current_table_row(self, catalog, *table_id)?
                .ok_or(CatalogError::NotFound("table"))?;
            table_drops.push(prepare_table_drop(self, catalog, table, placeholder)?);
        }
        if !changes.replacement_tables.is_empty() {
            reject_replacement_create_conflicts(
                self,
                catalog,
                latest.order,
                &table_drops,
                &changes.replacement_tables,
            )?;
        }
        let replacement_tables = prepare_created_tables(placeholder, changes.replacement_tables);

        let has_view_changes = !changes.created_views.is_empty()
            || !changes.view_renames.is_empty()
            || !changes.dropped_view_ids.is_empty()
            || !changes.view_comment_changes.is_empty();
        let views = if has_view_changes {
            let current_views = list_views_at(self, catalog, latest.order)?;
            let current_tables = list_tables_at(self, catalog, latest.order)?;
            prepare_view_changes(
                placeholder,
                changes.created_views,
                changes.view_renames,
                &changes.dropped_view_ids,
                changes.view_comment_changes,
                &current_views,
                &current_tables,
            )?
        } else {
            Default::default()
        };

        let estimated_bytes = estimate_versionstamped_schema_change_bytes(
            catalog,
            &snapshot,
            &created_schemas,
            &dropped_schemas,
        )
        .saturating_add(estimate_table_change_bytes(
            catalog,
            &snapshot,
            &created_tables,
            &table_replacements,
        ))
        .saturating_add(estimate_drop_bytes(catalog, &snapshot, &table_drops))
        .saturating_add(
            replacement_tables
                .iter()
                .map(|table| {
                    crate::fdb_versionstamp::estimate_versionstamped_table_create_bytes(
                        catalog, &snapshot, table,
                    )
                })
                .sum::<usize>(),
        )
        .saturating_add(estimate_view_change_bytes(catalog, &snapshot, &views));
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb mixed metadata commit is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        if changes.public_schema_changed {
            stage_fdb_next_schema_version(self, &trx, catalog, &snapshot)?;
        } else {
            stage_fdb_next_catalog_snapshot_version(self, &trx, catalog)?;
        }
        stage_schemas(self, &trx, catalog, &created_schemas, &dropped_schemas)?;
        stage_tables(self, &trx, catalog, &created_tables, &table_replacements)?;
        for drop in &table_drops {
            stage_drop(self, &trx, catalog, drop)?;
        }
        stage_created_tables(self, &trx, catalog, &replacement_tables)?;
        stage_view_changes(self, &trx, catalog, &views)?;

        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        Ok(replacement_tables
            .into_iter()
            .map(|mut table| {
                table.validity = ValidityWindow::new(order, None);
                table
            })
            .collect())
    }
}

fn stage_schemas(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    created: &[SchemaRow],
    dropped: &[SchemaRow],
) -> CatalogResult<()> {
    for schema in dropped {
        trx.atomic_op(
            &kv.namespaced_key(&schema_object_key(
                catalog,
                schema.schema_id,
                schema.validity.begin_order,
            )),
            &versionstamped_value(&schema.encode(), SchemaRow::END_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
    }
    for schema in created {
        trx.atomic_op(
            &kv.versionstamped_key(
                &schema_object_key(catalog, schema.schema_id, schema.validity.begin_order),
                schema_object_key_order_offset(catalog, schema.schema_id),
            )?,
            &versionstamped_value(&schema.encode(), SchemaRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedKey,
        );
    }
    if let Some(max_schema_id) = created.iter().map(|schema| schema.schema_id.0).max() {
        stage_fdb_max_catalog_id_watermark(kv, trx, catalog, max_schema_id);
    }
    Ok(())
}

fn stage_tables(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    created: &[TableRow],
    replacements: &[crate::TableVersionReplacement],
) -> CatalogResult<()> {
    for replacement in replacements {
        trx.atomic_op(
            &kv.namespaced_key(&table_object_key(
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
            &kv.versionstamped_key(
                &table_object_key(
                    catalog,
                    replacement.table_id,
                    replacement.next.validity.begin_order,
                ),
                table_object_key_order_offset(catalog, replacement.table_id),
            )?,
            &versionstamped_value(
                &replacement.next.encode(),
                TableRow::BEGIN_ORDER_BYTES_OFFSET,
            )?,
            MutationType::SetVersionstampedKey,
        );
        stage_current_table_name_replacement(kv, trx, catalog, replacement);
        stage_current_table_row(kv, trx, catalog, &replacement.next)?;
        stage_table_visibility_end(kv, trx, catalog, &replacement.previous)?;
        stage_table_visibility_begin(kv, trx, catalog, &replacement.next)?;
    }
    stage_created_tables(kv, trx, catalog, created)?;
    let max_table_id = created
        .iter()
        .map(|table| table.table_id.0)
        .chain(
            replacements
                .iter()
                .map(|replacement| replacement.table_id.0),
        )
        .max();
    if let Some(max_table_id) = max_table_id {
        stage_fdb_max_catalog_id_watermark(kv, trx, catalog, max_table_id);
    }
    Ok(())
}

fn stage_created_tables(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    created: &[TableRow],
) -> CatalogResult<()> {
    for table in created {
        trx.atomic_op(
            &kv.versionstamped_key(
                &table_object_key(catalog, table.table_id, table.validity.begin_order),
                table_object_key_order_offset(catalog, table.table_id),
            )?,
            &versionstamped_value(&table.encode(), TableRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedKey,
        );
        stage_current_table_name(kv, trx, catalog, table);
        stage_current_table_row(kv, trx, catalog, table)?;
        stage_table_visibility_begin(kv, trx, catalog, table)?;
    }
    if let Some(max_table_id) = created.iter().map(|table| table.table_id.0).max() {
        stage_fdb_max_catalog_id_watermark(kv, trx, catalog, max_table_id);
    }
    Ok(())
}
