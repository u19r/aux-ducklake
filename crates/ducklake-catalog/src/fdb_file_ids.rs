use std::{collections::BTreeMap, ops::Deref};

use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileId, DeleteFileId, FdbDataMutation,
    FdbOrderedCatalogKv, FoundationDbErrorClass,
    fdb_data_mutations::FdbAppendPartitionExpectation,
    fdb_runtime::{classify_fdb_error, map_fdb_error},
    keys::conflict_max_file_id_key,
};

const MAX_FILE_ID_RESERVATION_RETRIES: usize = 8;

impl FdbOrderedCatalogKv {
    pub(crate) fn reserve_mutation_file_ids(
        &self,
        catalog: CatalogId,
        mutation: &mut FdbDataMutation,
        append_partitions: &mut [FdbAppendPartitionExpectation],
    ) -> CatalogResult<()> {
        let proposed = proposed_file_ids(mutation)?;
        if proposed.is_empty() {
            return Ok(());
        }
        let first = self.reserve_file_id_range(catalog, proposed.len())?;
        let remaps = proposed
            .into_iter()
            .enumerate()
            .map(|(offset, proposed)| {
                let offset = u64::try_from(offset).map_err(|_| {
                    CatalogError::InvalidMutation("file ID reservation exceeds u64".to_owned())
                })?;
                let reserved = first.checked_add(offset).ok_or_else(|| {
                    CatalogError::InvalidMutation("file ID reservation overflow".to_owned())
                })?;
                Ok((proposed, reserved))
            })
            .collect::<CatalogResult<BTreeMap<_, _>>>()?;
        remap_mutation_file_ids(mutation, &remaps);
        for expectation in append_partitions {
            expectation.data_file_id =
                DataFileId(remap_file_id(&remaps, expectation.data_file_id.0));
        }
        Ok(())
    }

    fn reserve_file_id_range(&self, catalog: CatalogId, count: usize) -> CatalogResult<u64> {
        let count = u64::try_from(count)
            .map_err(|_| CatalogError::InvalidMutation("file ID count exceeds u64".to_owned()))?;
        let key = self.namespaced_key(&conflict_max_file_id_key(catalog));
        for attempt in 0..=MAX_FILE_ID_RESERVATION_RETRIES {
            let trx = self.create_transaction()?;
            let current = block_on(trx.get(&key, false))
                .map_err(map_fdb_error)?
                .map(|value| decode_file_id_watermark(value.deref()))
                .transpose()?;
            let first = current.map_or(0, |value| value.saturating_add(1));
            let last = first.checked_add(count.saturating_sub(1)).ok_or_else(|| {
                CatalogError::InvalidMutation("file ID range overflow".to_owned())
            })?;
            trx.set(&key, &last.to_be_bytes());
            match block_on(trx.commit()) {
                Ok(_) => return Ok(first),
                Err(error)
                    if attempt < MAX_FILE_ID_RESERVATION_RETRIES
                        && matches!(
                            classify_fdb_error(*error),
                            FoundationDbErrorClass::RetryableNotCommitted
                                | FoundationDbErrorClass::Retryable
                        ) => {}
                Err(error) => return Err(map_fdb_error(*error)),
            }
        }
        Err(CatalogError::InvalidMutation(
            "file ID reservation retry loop did not run".to_owned(),
        ))
    }
}

fn proposed_file_ids(mutation: &FdbDataMutation) -> CatalogResult<Vec<u64>> {
    let mut proposed = mutation
        .data_files
        .iter()
        .map(|row| row.data_file_id.0)
        .chain(mutation.delete_files.iter().map(|row| row.delete_file_id.0))
        .collect::<Vec<_>>();
    proposed.sort_unstable();
    if proposed.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CatalogError::InvalidMutation(
            "data mutation proposes duplicate file IDs".to_owned(),
        ));
    }
    Ok(proposed)
}

fn remap_mutation_file_ids(mutation: &mut FdbDataMutation, remaps: &BTreeMap<u64, u64>) {
    for row in &mut mutation.data_files {
        row.data_file_id = DataFileId(remap_file_id(remaps, row.data_file_id.0));
    }
    for row in &mut mutation.delete_files {
        row.delete_file_id = DeleteFileId(remap_file_id(remaps, row.delete_file_id.0));
        row.data_file_id = DataFileId(remap_file_id(remaps, row.data_file_id.0));
    }
    for row in &mut mutation.partition_values {
        row.data_file_id = DataFileId(remap_file_id(remaps, row.data_file_id.0));
    }
    for row in &mut mutation.inline_file_deletions {
        row.data_file_id = DataFileId(remap_file_id(remaps, row.data_file_id.0));
    }
    for row in &mut mutation.file_column_stats {
        row.data_file_id = DataFileId(remap_file_id(remaps, row.data_file_id.0));
    }
}

fn remap_file_id(remaps: &BTreeMap<u64, u64>, file_id: u64) -> u64 {
    remaps.get(&file_id).copied().unwrap_or(file_id)
}

fn decode_file_id_watermark(value: &[u8]) -> CatalogResult<u64> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| CatalogError::InvalidMutation("invalid file ID watermark value".to_owned()))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
#[path = "fdb_file_ids_tests.rs"]
mod fdb_file_ids_tests;
