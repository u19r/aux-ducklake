use std::collections::HashSet;

use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MutableCatalogKv, RawSnapshotSequence,
    TableId, TableRow,
    data_file_store::list_current_data_files_with_deletes,
    file_visibility::{stage_expire_current_data_file, stage_expire_current_delete_file},
    keys::table_object_key,
    maintenance::{stage_scheduled_data_file_cleanup, stage_scheduled_delete_file_cleanup},
    schema_version_state::stage_next_schema_version,
    store::{latest_snapshot, stage_snapshot},
    table_store::{
        load_current_table_row, stage_remove_current_table_row, stage_table_visibility_row,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedTable {
    pub table: TableRow,
    pub expired_data_file_count: usize,
    pub expired_delete_file_count: usize,
}

pub fn commit_drop_tables(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_ids: &[TableId],
) -> CatalogResult<Vec<DroppedTable>> {
    commit_drop_tables_at(kv, catalog, table_ids, None)
}

pub fn commit_drop_tables_at(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_ids: &[TableId],
    commit_raw_snapshot: Option<RawSnapshotSequence>,
) -> CatalogResult<Vec<DroppedTable>> {
    if table_ids.is_empty() {
        return Ok(Vec::new());
    }
    reject_duplicate_table_ids(table_ids)?;

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let order = kv.generated_order_id()?;
    let sequence = commit_raw_snapshot.unwrap_or_else(|| latest.sequence.next());
    let snapshot = crate::SnapshotRow::new(order, sequence);
    let mut batch = KvBatch::new();
    let mut dropped = Vec::with_capacity(table_ids.len());
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;

    for table_id in table_ids {
        let mut table = load_current_table_row(kv, catalog, *table_id)?
            .ok_or(CatalogError::NotFound("table"))?;
        let mut expired_data_file_count = 0;
        let mut expired_delete_file_count = 0;
        for attached_file in list_current_data_files_with_deletes(kv, catalog, *table_id)? {
            let data_file = stage_expire_current_data_file(
                kv,
                &mut batch,
                catalog,
                attached_file.data_file.data_file_id,
                order,
            )?;
            stage_scheduled_data_file_cleanup(&mut batch, catalog, data_file.data_file_id);
            expired_data_file_count += 1;
            if let Some(delete_file) = attached_file.delete_file {
                stage_expire_current_delete_file(
                    kv,
                    &mut batch,
                    catalog,
                    &data_file,
                    delete_file.delete_file_id,
                    order,
                )?;
                stage_scheduled_delete_file_cleanup(
                    &mut batch,
                    catalog,
                    data_file.table_id,
                    delete_file.delete_file_id,
                );
                expired_delete_file_count += 1;
            }
        }
        table.validity.end_order = Some(order);
        batch.put(
            table_object_key(catalog, table.table_id, table.validity.begin_order),
            table.encode(),
        );
        stage_table_visibility_row(&mut batch, catalog, &table);
        stage_remove_current_table_row(&mut batch, catalog, table.table_id);
        dropped.push(DroppedTable {
            table,
            expired_data_file_count,
            expired_delete_file_count,
        });
    }

    kv.commit(batch)?;
    Ok(dropped)
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
