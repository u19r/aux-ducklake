use std::ops::Deref;

use futures::executor::block_on;

use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, DeleteFileCleanupRow, DeleteFileId,
    FdbOrderedCatalogKv, FileColumnStatsRow, FilePartitionValueRow, InlineTableCleanupId,
    InlineTableCleanupRow, RangeDirection,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    file_partitions::remove_cached_file_partition_values,
    file_stats::remove_cached_file_column_stats_for_data_file,
    inline_data::{decode_inline_table_item, inline_table_payload_prefix},
    keys::{
        KeyFamily, current_data_file_key, data_file_begin_key, data_file_end_key, data_file_key,
        delete_file_end_key, delete_file_key, delete_file_timeline_key, family_prefix,
        file_column_stats_lookup_key, file_partition_value_prefix, inline_table_end_key,
        partition_value_lookup_key, scheduled_data_file_cleanup_key,
        scheduled_delete_file_cleanup_key,
    },
    kv::{OrderedCatalogKv, RangeItem},
    maintenance::{
        delete_file_is_safe_for_physical_cleanup, list_old_data_files_for_cleanup,
        list_old_delete_files_for_cleanup, list_old_inline_table_payloads_for_cleanup,
        row_is_unreachable,
    },
    store::list_snapshots,
};

impl FdbOrderedCatalogKv {
    pub fn remove_old_data_files_checked(
        &self,
        catalog: CatalogId,
        data_file_ids: &[DataFileId],
    ) -> CatalogResult<Vec<DataFileRow>> {
        let requested = requested_ids(data_file_ids);
        let snapshots = list_snapshots(self, catalog)?;
        let mut removed = Vec::new();
        for row in list_old_data_files_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.data_file_id.0))
        {
            let is_scheduled = self.is_scheduled_data_file_cleanup(catalog, row.data_file_id)?;
            if row_is_unreachable(&row, &snapshots) {
                self.remove_data_file_metadata(catalog, &row)?;
            }
            if is_scheduled {
                self.clear_scheduled_data_file_cleanup(catalog, row.data_file_id)?;
            }
            if row_is_unreachable(&row, &snapshots) || is_scheduled {
                removed.push(row);
            }
        }
        Ok(removed)
    }

    pub fn remove_old_delete_files_checked(
        &self,
        catalog: CatalogId,
        delete_file_ids: &[DeleteFileId],
    ) -> CatalogResult<Vec<DeleteFileCleanupRow>> {
        let requested = requested_ids(delete_file_ids);
        let snapshots = list_snapshots(self, catalog)?;
        let mut removed = Vec::new();
        for row in list_old_delete_files_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.delete_file.delete_file_id.0))
        {
            let is_scheduled =
                self.is_scheduled_delete_file_cleanup(catalog, row.delete_file.delete_file_id)?;
            let is_physically_safe = delete_file_is_safe_for_physical_cleanup(
                self,
                catalog,
                &row.delete_file,
                &snapshots,
            )?;
            if is_physically_safe {
                self.remove_delete_file_metadata(catalog, &row)?;
            }
            if is_scheduled {
                self.clear_scheduled_delete_file_cleanup(catalog, row.delete_file.delete_file_id)?;
            }
            if is_physically_safe || is_scheduled {
                removed.push(row);
            }
        }
        Ok(removed)
    }

    pub fn remove_old_inline_table_payloads_checked(
        &self,
        catalog: CatalogId,
        inline_ids: &[InlineTableCleanupId],
    ) -> CatalogResult<Vec<InlineTableCleanupRow>> {
        let requested = inline_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut removed = Vec::new();
        for row in list_old_inline_table_payloads_for_cleanup(self, catalog)?
            .into_iter()
            .filter(|row| requested.contains(&row.id))
        {
            if self.remove_inline_payload_if_unchanged(catalog, &row)? {
                removed.push(row);
            }
        }
        Ok(removed)
    }

    fn remove_data_file_metadata(
        &self,
        catalog: CatalogId,
        row: &DataFileRow,
    ) -> CatalogResult<()> {
        let partition_rows = self.scan_prefix(
            &file_partition_value_prefix(catalog, row.data_file_id),
            RangeDirection::Forward,
            usize::MAX,
        )?;
        let stats_rows = self.scan_prefix(
            &file_column_stats_prefix(catalog, row.data_file_id),
            RangeDirection::Forward,
            usize::MAX,
        )?;
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&data_file_key(catalog, row.data_file_id)));
        trx.clear(&self.namespaced_key(&data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            row.data_file_id,
        )));
        if let Some(end_order) = row.validity.end_order {
            trx.clear(&self.namespaced_key(&data_file_end_key(
                catalog,
                row.table_id,
                end_order,
                row.data_file_id,
            )));
        }
        trx.clear(&self.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        )));
        for item in partition_rows {
            let partition = FilePartitionValueRow::decode(&item.value)?;
            trx.clear(&self.namespaced_key(&item.key));
            trx.clear(&self.namespaced_key(&partition_value_lookup_key(
                catalog,
                partition.table_id,
                partition.partition_key_index,
                &partition.partition_value,
                partition.data_file_id,
            )));
        }
        for item in stats_rows {
            let stats = FileColumnStatsRow::decode(&item.value)?;
            trx.clear(&self.namespaced_key(&item.key));
            trx.clear(&self.namespaced_key(&file_column_stats_lookup_key(
                catalog,
                stats.table_id,
                stats.column_id,
                stats.data_file_id,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        crate::immutable_file_metadata::remove_immutable_data_file_metadata(
            self,
            catalog,
            row.data_file_id,
        );
        remove_cached_file_partition_values(self, catalog, row.data_file_id);
        remove_cached_file_column_stats_for_data_file(self, catalog, row.data_file_id);
        Ok(())
    }

    fn is_scheduled_data_file_cleanup(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<bool> {
        let key = scheduled_data_file_cleanup_key(catalog, data_file_id);
        Ok(self.get(&key)?.is_some())
    }

    fn clear_scheduled_data_file_cleanup(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<()> {
        let key = scheduled_data_file_cleanup_key(catalog, data_file_id);
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&key));
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn is_scheduled_delete_file_cleanup(
        &self,
        catalog: CatalogId,
        delete_file_id: DeleteFileId,
    ) -> CatalogResult<bool> {
        let key = scheduled_delete_file_cleanup_key(catalog, delete_file_id);
        Ok(self.get(&key)?.is_some())
    }

    fn clear_scheduled_delete_file_cleanup(
        &self,
        catalog: CatalogId,
        delete_file_id: DeleteFileId,
    ) -> CatalogResult<()> {
        let key = scheduled_delete_file_cleanup_key(catalog, delete_file_id);
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&key));
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn remove_delete_file_metadata(
        &self,
        catalog: CatalogId,
        row: &DeleteFileCleanupRow,
    ) -> CatalogResult<()> {
        let delete_file = &row.delete_file;
        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&delete_file_key(catalog, delete_file.delete_file_id)));
        trx.clear(&self.namespaced_key(&delete_file_timeline_key(
            catalog,
            delete_file.data_file_id,
            delete_file.validity.begin_order,
            delete_file.delete_file_id,
        )));
        if let Some(end_order) = delete_file.validity.end_order {
            trx.clear(&self.namespaced_key(&delete_file_end_key(
                catalog,
                row.table_id,
                end_order,
                delete_file.delete_file_id,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn remove_inline_payload_if_unchanged(
        &self,
        catalog: CatalogId,
        row: &InlineTableCleanupRow,
    ) -> CatalogResult<bool> {
        let prefix = inline_table_payload_prefix(
            catalog,
            row.id.table_id,
            row.id.schema_id,
            row.id.begin_order,
        );
        let chunks = self.scan_prefix(&prefix, RangeDirection::Forward, usize::MAX)?;
        if chunks.len() != row.chunk_count {
            return Ok(false);
        }
        let trx = self.create_transaction()?;
        let mut end_order = None;
        for chunk in &chunks {
            if !self.trx_range_item_matches(&trx, chunk)? {
                return Ok(false);
            }
            let decoded = decode_inline_table_item(catalog, &chunk.key, &chunk.value)?;
            end_order = decoded.validity.end_order;
            trx.clear(&self.namespaced_key(&chunk.key));
        }
        if let Some(order) = end_order {
            trx.clear(&self.namespaced_key(&inline_table_end_key(
                catalog,
                row.id.table_id,
                order,
                row.id.schema_id,
                row.id.begin_order,
            )));
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(true)
    }

    fn trx_range_item_matches(
        &self,
        trx: &foundationdb::Transaction,
        item: &RangeItem,
    ) -> CatalogResult<bool> {
        self.trx_value_matches(trx, &item.key, &item.value)
    }

    fn trx_value_matches(
        &self,
        trx: &foundationdb::Transaction,
        key: &[u8],
        expected: &[u8],
    ) -> CatalogResult<bool> {
        let actual = block_on(trx.get(&self.namespaced_key(key), false))
            .map_err(map_fdb_error)?
            .map(|value| value.deref().to_vec());
        Ok(actual.as_deref() == Some(expected))
    }
}

fn requested_ids<T: Copy + IntoId>(ids: &[T]) -> std::collections::BTreeSet<u64> {
    ids.iter().map(|id| id.into_id()).collect()
}

trait IntoId {
    fn into_id(self) -> u64;
}

impl IntoId for DataFileId {
    fn into_id(self) -> u64 {
        self.0
    }
}

impl IntoId for DeleteFileId {
    fn into_id(self) -> u64 {
        self.0
    }
}

fn file_column_stats_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut prefix = family_prefix(catalog, KeyFamily::FileColumnStats);
    prefix.extend_from_slice(&data_file_id.0.to_be_bytes());
    prefix.push(b'/');
    prefix
}
