use std::ops::Deref;

use foundationdb::options::MutationType;

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, CommitAttemptId,
    CommitAttemptRow, DataFileId, DataFileRow, DataMutationCommit, FdbOrderedCatalogKv,
    FoundationDbErrorClass, RangeDirection, SnapshotRow, ValidityWindow,
    conflict::commit_attempt_key,
    fdb_data_mutation_staging::{stage_data_file, stage_expired_data_file, stage_snapshot},
    fdb_runtime::{classify_fdb_error, map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, estimate_versionstamped_expire_bytes, incomplete_order,
        versionstamped_value,
    },
    keys::{data_file_key, snapshot_prefix},
};

pub(crate) const MAX_ASYNC_FDB_RETRIES: usize = 8;

impl FdbOrderedCatalogKv {
    pub async fn initialize_catalog_if_absent_versionstamped_async(
        &self,
        catalog: CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        match self.latest_snapshot_async(catalog).await? {
            Some(row) => Ok(row),
            None => {
                self.initialize_empty_catalog_versionstamped_async(catalog)
                    .await
            }
        }
    }

    pub async fn commit_data_files_versionstamped_async(
        &self,
        catalog: CatalogId,
        attempt_id: Option<CommitAttemptId>,
        data_files: Vec<DataFileRow>,
    ) -> CatalogResult<DataMutationCommit> {
        if data_files.is_empty() {
            return Ok(DataMutationCommit::default());
        }
        let mut last_retry_error = None;
        for _ in 0..=MAX_ASYNC_FDB_RETRIES {
            match self
                .try_commit_data_files_versionstamped_async(catalog, attempt_id, data_files.clone())
                .await?
            {
                AsyncCommitAttempt::Done(result) => return Ok(result),
                AsyncCommitAttempt::Retry(error) => last_retry_error = Some(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb async data-file retry loop did not run".to_owned(),
            )
        }))
    }

    async fn try_commit_data_files_versionstamped_async(
        &self,
        catalog: CatalogId,
        attempt_id: Option<CommitAttemptId>,
        mut data_files: Vec<DataFileRow>,
    ) -> CatalogResult<AsyncCommitAttempt> {
        if self
            .load_commit_attempt_async(catalog, attempt_id)
            .await?
            .is_some()
        {
            return Ok(AsyncCommitAttempt::Done(DataMutationCommit::default()));
        }

        let latest = self.latest_snapshot_async(catalog).await?;
        let next_sequence = latest.map_or(crate::RawSnapshotSequence::initial(), |snapshot| {
            snapshot.sequence.next()
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        for row in &mut data_files {
            row.validity = ValidityWindow::new(placeholder, None);
        }

        let trx = self.create_transaction()?;
        if let Some(attempt_id) = attempt_id {
            let attempt = CommitAttemptRow::new(attempt_id, placeholder);
            trx.atomic_op(
                &self.namespaced_key(&commit_attempt_key(catalog, attempt_id)),
                &versionstamped_value(
                    &attempt.encode(),
                    CommitAttemptRow::COMMIT_ORDER_BYTES_OFFSET,
                )?,
                MutationType::SetVersionstampedValue,
            );
        }
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        for row in &data_files {
            stage_data_file(self, &trx, catalog, row)?;
        }

        let versionstamp = trx.get_versionstamp();
        if let Err(error) = trx.commit().await {
            if is_retryable_async_commit_error(&error) {
                return Ok(AsyncCommitAttempt::Retry(map_fdb_commit_error(error)));
            }
            return Err(map_fdb_commit_error(error));
        }
        let order = committed_order(versionstamp.await.map_err(map_fdb_error)?.deref())?;
        for row in &mut data_files {
            row.validity = ValidityWindow::new(order, None);
        }
        Ok(AsyncCommitAttempt::Done(DataMutationCommit {
            data_files,
            delete_files: Vec::new(),
            partition_value_count: 0,
            file_column_stats_count: 0,
            flushed_inline_count: 0,
            inline_file_deletion_count: 0,
            dropped_data_file_count: 0,
        }))
    }

    pub async fn expire_data_file_versionstamped_async(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<DataFileRow> {
        let mut last_retry_error = None;
        for _ in 0..=MAX_ASYNC_FDB_RETRIES {
            match self
                .try_expire_data_file_versionstamped_async(catalog, data_file_id)
                .await?
            {
                AsyncExpireAttempt::Done(result) => return Ok(result),
                AsyncExpireAttempt::Retry(error) => last_retry_error = Some(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb async data-file expiration retry loop did not run".to_owned(),
            )
        }))
    }

    async fn try_expire_data_file_versionstamped_async(
        &self,
        catalog: CatalogId,
        data_file_id: DataFileId,
    ) -> CatalogResult<AsyncExpireAttempt> {
        let Some(value) = self
            .get_async(&data_file_key(catalog, data_file_id))
            .await?
        else {
            return Err(CatalogError::NotFound("data file"));
        };
        let mut row = DataFileRow::decode(&value)?;
        if row.validity.end_order.is_some() {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} is already expired",
                data_file_id.0
            )));
        }
        let placeholder = incomplete_order();
        row.validity.end_order = Some(placeholder);
        let estimated_bytes = estimate_versionstamped_expire_bytes(catalog, &row);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb async data-file expiration is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        stage_expired_data_file(self, &trx, catalog, &row)?;
        let versionstamp = trx.get_versionstamp();
        if let Err(error) = trx.commit().await {
            if is_retryable_async_commit_error(&error) {
                return Ok(AsyncExpireAttempt::Retry(map_fdb_commit_error(error)));
            }
            return Err(map_fdb_commit_error(error));
        }
        let order = committed_order(versionstamp.await.map_err(map_fdb_error)?.deref())?;
        row.validity.end_order = Some(order);
        Ok(AsyncExpireAttempt::Done(row))
    }

    async fn initialize_empty_catalog_versionstamped_async(
        &self,
        catalog: CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        let placeholder = incomplete_order();
        let row = SnapshotRow::initial(placeholder);
        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &row)?;
        let versionstamp = trx.get_versionstamp();
        trx.commit().await.map_err(map_fdb_commit_error)?;
        let order = committed_order(versionstamp.await.map_err(map_fdb_error)?.deref())?;
        Ok(SnapshotRow::with_created_at_micros(
            order,
            row.sequence,
            row.created_at_micros,
        ))
    }

    async fn latest_snapshot_async(
        &self,
        catalog: CatalogId,
    ) -> CatalogResult<Option<SnapshotRow>> {
        let rows = self
            .scan_prefix_async(&snapshot_prefix(catalog), RangeDirection::Reverse, 1)
            .await?;
        rows.first()
            .map(|item| decode_snapshot_item(catalog, &item.key, &item.value))
            .transpose()
    }

    pub async fn load_commit_attempt_async(
        &self,
        catalog: CatalogId,
        attempt_id: Option<CommitAttemptId>,
    ) -> CatalogResult<Option<CommitAttemptRow>> {
        let Some(attempt_id) = attempt_id else {
            return Ok(None);
        };
        self.get_async(&commit_attempt_key(catalog, attempt_id))
            .await?
            .map(|bytes| CommitAttemptRow::decode(&bytes))
            .transpose()
    }
}

enum AsyncCommitAttempt {
    Done(DataMutationCommit),
    Retry(CatalogError),
}

enum AsyncExpireAttempt {
    Done(DataFileRow),
    Retry(CatalogError),
}

fn is_retryable_async_commit_error(error: &foundationdb::TransactionCommitError) -> bool {
    let error_class = classify_fdb_error(**error);
    match error_class {
        FoundationDbErrorClass::RetryableNotCommitted | FoundationDbErrorClass::Retryable => true,
        FoundationDbErrorClass::MaybeCommitted | FoundationDbErrorClass::NonRetryable => false,
    }
}

fn decode_snapshot_item(
    catalog: CatalogId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<SnapshotRow> {
    let mut row = SnapshotRow::decode(value)?;
    row.order = snapshot_order_from_key(catalog, key, row.order)?;
    Ok(row)
}

fn snapshot_order_from_key(
    catalog: CatalogId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = snapshot_prefix(catalog);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "snapshot key has wrong prefix".to_owned(),
        ));
    };
    let bytes = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey(format!(
            "snapshot key order must be {} bytes, got {}",
            CatalogOrderId::LEN,
            tail.len()
        ))
    })?;
    let kind = if value_order.as_bytes() == bytes {
        value_order.kind()
    } else {
        CatalogOrderKind::FdbVersionstamp
    };
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}
