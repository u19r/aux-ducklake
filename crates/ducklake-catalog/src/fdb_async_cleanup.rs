use crate::{
    CatalogError, CatalogId, CatalogResult, ColumnId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, PartitionKeyIndex, RangeDirection, TableId,
    fdb_runtime::map_fdb_commit_error,
    file_partitions::remove_cached_file_partition_values,
    file_stats::remove_cached_file_column_stats_for_data_file,
    keys::{
        KeyFamily, current_data_file_key, data_file_begin_key, data_file_end_key, data_file_key,
        family_prefix, file_column_stats_key, file_column_stats_lookup_key,
        file_column_stats_lookup_prefix, file_partition_value_key, file_partition_value_prefix,
        partition_value_lookup_key, partition_value_lookup_prefix,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncFileCleanupMetadataCounts {
    pub partition_values: usize,
    pub partition_lookups: usize,
    pub column_stats: usize,
    pub column_stats_lookups: usize,
}

impl FdbOrderedCatalogKv {
    pub async fn remove_expired_data_file_metadata_async(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<bool> {
        let Some(value) = self
            .get_async(&data_file_key(catalog, data_file_id))
            .await?
        else {
            return Ok(false);
        };
        let row = DataFileRow::decode(&value)?;
        if row.validity.end_order.is_none() {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} is not expired",
                data_file_id.0
            )));
        }
        let partition_lookups = self
            .partition_lookup_keys_for_data_file(catalog, data_file_id)
            .await?;
        let stats_lookups = self
            .stats_lookup_keys_for_data_file(catalog, data_file_id)
            .await?;

        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&data_file_key(catalog, data_file_id)));
        trx.clear(&self.namespaced_key(&data_file_begin_key(
            catalog,
            row.table_id,
            row.validity.begin_order,
            data_file_id,
        )));
        if let Some(end_order) = row.validity.end_order {
            trx.clear(&self.namespaced_key(&data_file_end_key(
                catalog,
                row.table_id,
                end_order,
                data_file_id,
            )));
        }
        trx.clear(&self.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            data_file_id,
        )));
        self.clear_prefix_in_transaction(&trx, &file_partition_value_prefix(catalog, data_file_id));
        self.clear_prefix_in_transaction(&trx, &file_column_stats_prefix(catalog, data_file_id));
        for key in partition_lookups.into_iter().chain(stats_lookups) {
            trx.clear(&self.namespaced_key(&key));
        }
        trx.commit().await.map_err(map_fdb_commit_error)?;
        crate::immutable_file_metadata::remove_immutable_data_file_metadata(
            self,
            catalog,
            data_file_id,
        );
        remove_cached_file_partition_values(self, catalog, data_file_id);
        remove_cached_file_column_stats_for_data_file(self, catalog, data_file_id);
        Ok(true)
    }

    pub async fn register_file_cleanup_metadata_async(
        &self,
        catalog: CatalogId,
        partition: FilePartitionValueRow,
        stats: FileColumnStatsRow,
    ) -> CatalogResult<()> {
        validate_cleanup_metadata_pair(&partition, &stats)?;
        let Some(value) = self
            .get_async(&data_file_key(catalog, partition.data_file_id))
            .await?
        else {
            return Err(CatalogError::NotFound("data file"));
        };
        let data_file = DataFileRow::decode(&value)?;
        if data_file.table_id != partition.table_id {
            return Err(CatalogError::InvalidMutation(format!(
                "metadata table {} does not match data file table {}",
                partition.table_id.0, data_file.table_id.0
            )));
        }

        let trx = self.create_transaction()?;
        trx.set(
            &self.namespaced_key(&file_partition_value_key(
                catalog,
                partition.data_file_id,
                partition.partition_key_index,
            )),
            &partition.encode(),
        );
        trx.set(
            &self.namespaced_key(&partition_value_lookup_key(
                catalog,
                partition.table_id,
                partition.partition_key_index,
                &partition.partition_value,
                partition.data_file_id,
            )),
            &partition.encode(),
        );
        let encoded_stats = stats.encode();
        trx.set(
            &self.namespaced_key(&file_column_stats_key(
                catalog,
                stats.data_file_id,
                stats.column_id,
            )),
            &encoded_stats,
        );
        trx.set(
            &self.namespaced_key(&file_column_stats_lookup_key(
                catalog,
                stats.table_id,
                stats.column_id,
                stats.data_file_id,
            )),
            &encoded_stats,
        );
        trx.commit().await.map_err(map_fdb_commit_error)?;
        remove_cached_file_partition_values(self, catalog, partition.data_file_id);
        remove_cached_file_column_stats_for_data_file(self, catalog, stats.data_file_id);
        Ok(())
    }

    pub async fn file_cleanup_metadata_counts_async(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
        table_id: TableId,
        partition_key_index: PartitionKeyIndex,
        partition_value: &str,
        column_id: ColumnId,
    ) -> CatalogResult<AsyncFileCleanupMetadataCounts> {
        Ok(AsyncFileCleanupMetadataCounts {
            partition_values: self
                .scan_prefix_async(
                    &file_partition_value_prefix(catalog, data_file_id),
                    RangeDirection::Forward,
                    usize::MAX,
                )
                .await?
                .len(),
            partition_lookups: self
                .scan_prefix_async(
                    &partition_value_lookup_prefix(
                        catalog,
                        table_id,
                        partition_key_index,
                        partition_value,
                    ),
                    RangeDirection::Forward,
                    usize::MAX,
                )
                .await?
                .len(),
            column_stats: self
                .scan_prefix_async(
                    &file_column_stats_prefix(catalog, data_file_id),
                    RangeDirection::Forward,
                    usize::MAX,
                )
                .await?
                .len(),
            column_stats_lookups: self
                .scan_prefix_async(
                    &file_column_stats_lookup_prefix(catalog, table_id, column_id),
                    RangeDirection::Forward,
                    usize::MAX,
                )
                .await?
                .len(),
        })
    }

    async fn partition_lookup_keys_for_data_file(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<Vec<Vec<u8>>> {
        let rows = self
            .scan_prefix_async(
                &file_partition_value_prefix(catalog, data_file_id),
                RangeDirection::Forward,
                usize::MAX,
            )
            .await?;
        rows.into_iter()
            .map(|item| {
                let row = FilePartitionValueRow::decode(&item.value)?;
                Ok(partition_value_lookup_key(
                    catalog,
                    row.table_id,
                    row.partition_key_index,
                    &row.partition_value,
                    row.data_file_id,
                ))
            })
            .collect()
    }

    async fn stats_lookup_keys_for_data_file(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<Vec<Vec<u8>>> {
        let rows = self
            .scan_prefix_async(
                &file_column_stats_prefix(catalog, data_file_id),
                RangeDirection::Forward,
                usize::MAX,
            )
            .await?;
        rows.into_iter()
            .map(|item| {
                let row = FileColumnStatsRow::decode(&item.value)?;
                Ok(file_column_stats_lookup_key(
                    catalog,
                    row.table_id,
                    row.column_id,
                    row.data_file_id,
                ))
            })
            .collect()
    }
}

fn validate_cleanup_metadata_pair(
    partition: &FilePartitionValueRow,
    stats: &FileColumnStatsRow,
) -> CatalogResult<()> {
    if partition.data_file_id != stats.data_file_id {
        return Err(CatalogError::InvalidMutation(
            "partition and stats metadata must target the same data file".to_owned(),
        ));
    }
    if partition.table_id != stats.table_id {
        return Err(CatalogError::InvalidMutation(
            "partition and stats metadata must target the same table".to_owned(),
        ));
    }
    Ok(())
}

fn file_column_stats_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut prefix = family_prefix(catalog, KeyFamily::FileColumnStats);
    prefix.extend_from_slice(&data_file_id.0.to_be_bytes());
    prefix.push(b'/');
    prefix
}
