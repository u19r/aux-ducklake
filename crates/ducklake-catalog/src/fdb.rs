#[cfg(debug_assertions)]
use std::sync::{Barrier, Mutex, OnceLock};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

use foundationdb::{
    Database,
    options::{MutationType, TransactionOption},
};

use crate::{
    CatalogCacheNamespace, CatalogError, CatalogResult, DataFileChangeKind, DataFileRow,
    MetadataSettingRow, SnapshotRow, ValidityWindow,
    conflict_watermarks::stage_fdb_conflict_watermarks_initialized,
    fdb_data_mutation_staging::stage_data_file,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error, shared_foundationdb_database},
    fdb_versionstamp::{
        append_versionstamp_offset, committed_order, data_file_end_key_order_offset,
        estimate_versionstamped_append_bytes, estimate_versionstamped_expire_bytes,
        incomplete_order, snapshot_data_file_change_key_order_offset, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, table_data_file_change_key_order_offset,
        versionstamped_value,
    },
    keys::{
        conflict_watermarks_initialized_key, current_data_file_key, data_file_begin_key,
        data_file_end_key, data_file_key, snapshot_data_file_change_key, snapshot_key,
        snapshot_timestamp_key, table_data_file_change_key,
    },
    kv::OrderedCatalogKv,
    metadata_settings::metadata_setting_key,
    store::{latest_snapshot, latest_snapshot_uncached, stage_fdb_snapshot_indexes},
};

#[derive(Clone)]
pub struct FdbOrderedCatalogKv {
    db: Arc<Database>,
    key_prefix: Vec<u8>,
    read_version: Option<i64>,
}

impl FdbOrderedCatalogKv {
    // Reserve 2 MB below FoundationDB's 10,000,000-byte transaction limit for
    // conflict ranges and any conservative-estimate drift.
    pub(crate) const MAX_COMMIT_BYTES: usize = 8_000_000;

    pub fn open_default_with_prefix(key_prefix: impl Into<Vec<u8>>) -> CatalogResult<Self> {
        Self::open_with_prefix(None, key_prefix)
    }

    pub fn open_with_prefix(
        cluster_file: Option<&str>,
        key_prefix: impl Into<Vec<u8>>,
    ) -> CatalogResult<Self> {
        let db = shared_foundationdb_database(cluster_file)?;
        Ok(Self::from_shared_database_with_prefix(db, key_prefix))
    }

    pub fn from_database_with_prefix(db: Database, key_prefix: impl Into<Vec<u8>>) -> Self {
        Self::from_shared_database_with_prefix(Arc::new(db), key_prefix)
    }

    pub fn from_shared_database_with_prefix(
        db: Arc<Database>,
        key_prefix: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            db,
            key_prefix: key_prefix.into(),
            read_version: None,
        }
    }

    pub(crate) fn with_read_version(&self) -> CatalogResult<Self> {
        if self.read_version.is_some() {
            return Ok(self.clone());
        }
        let read_version = self.fetch_read_version()?;
        Ok(Self {
            db: Arc::clone(&self.db),
            key_prefix: self.key_prefix.clone(),
            read_version: Some(read_version),
        })
    }

    #[must_use]
    pub fn key_prefix(&self) -> &[u8] {
        &self.key_prefix
    }

    pub(crate) fn namespaced_key(&self, key: &[u8]) -> Vec<u8> {
        crate::fdb_key_codec::encode_key(&self.key_prefix, key)
    }

    pub(crate) fn namespaced_range_bound(&self, key: &[u8]) -> Vec<u8> {
        crate::fdb_key_codec::encode_range_bound(&self.key_prefix, key)
    }

    pub(crate) fn catalog_cache_namespace(&self) -> CatalogCacheNamespace {
        let mut hasher = DefaultHasher::new();
        self.key_prefix.hash(&mut hasher);
        CatalogCacheNamespace::foundationdb(Arc::as_ptr(&self.db) as usize, hasher.finish())
            .with_read_context(crate::store::active_runtime_read_context_id())
    }

    pub(crate) fn strip_namespace(&self, key: &[u8]) -> CatalogResult<Vec<u8>> {
        crate::fdb_key_codec::decode_key(&self.key_prefix, key)
    }

    pub(crate) fn create_transaction(&self) -> CatalogResult<foundationdb::Transaction> {
        let trx = self.create_unpinned_transaction()?;
        if let Some(version) = self.read_version {
            trx.set_read_version(version);
        } else if crate::store::active_runtime_read_context_id().is_some() {
            trx.set_read_version(self.fetch_read_version()?);
        }
        Ok(trx)
    }

    fn fetch_read_version(&self) -> CatalogResult<i64> {
        if crate::store::active_runtime_read_context_id().is_some() {
            return crate::runtime_read_versions::fetch_version(
                self.catalog_cache_namespace(),
                || self.fetch_fresh_read_version(),
            );
        }
        self.fetch_fresh_read_version()
    }

    fn fetch_fresh_read_version(&self) -> CatalogResult<i64> {
        let trx = self.create_unpinned_transaction()?;
        crate::fdb_runtime::wait_for_fdb("wait.fdb.get_read_version", trx.get_read_version())
            .map_err(map_fdb_error)
    }

    fn create_unpinned_transaction(&self) -> CatalogResult<foundationdb::Transaction> {
        let trx = self.db.create_trx().map_err(map_fdb_error)?;
        crate::runtime_metrics::record_runtime_measurement("fdb.transaction.created", 1, 0);
        trx.set_option(TransactionOption::Timeout(5_000))
            .map_err(map_fdb_error)?;
        Ok(trx)
    }

    pub(crate) fn reset_read_transaction_after_error(
        &self,
        transaction: foundationdb::Transaction,
        error: foundationdb::FdbError,
    ) -> CatalogResult<foundationdb::Transaction> {
        let transaction =
            crate::fdb_runtime::wait_for_fdb("wait.fdb.on_error", transaction.on_error(error))
                .map_err(map_fdb_error)?;
        self.apply_read_version(&transaction);
        Ok(transaction)
    }

    pub(super) fn apply_read_version(&self, transaction: &foundationdb::Transaction) {
        if let Some(read_version) = self.read_version.or_else(|| {
            crate::runtime_read_versions::request_version(self.catalog_cache_namespace())
        }) {
            transaction.set_read_version(read_version);
        }
    }

    pub fn initialize_empty_catalog_versionstamped(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        match self.initialize_empty_catalog_with_metadata_versionstamped(catalog, &[])? {
            SyncInitializeAttempt::Created(row) => Ok(row),
            SyncInitializeAttempt::AlreadyInitialized => {
                self.latest_snapshot_for_initialization(catalog)
            }
        }
    }

    fn initialize_empty_catalog_with_metadata_versionstamped(
        &self,
        catalog: crate::CatalogId,
        metadata: &[MetadataSettingRow],
    ) -> CatalogResult<SyncInitializeAttempt> {
        let placeholder = incomplete_order();
        let row = SnapshotRow::initial(placeholder);
        let trx = self.create_transaction()?;
        if crate::fdb_runtime::wait_for_fdb(
            "wait.fdb.get",
            trx.get(
                &self.namespaced_key(&conflict_watermarks_initialized_key(catalog)),
                false,
            ),
        )
        .map_err(map_fdb_error)?
        .is_some()
        {
            return Ok(SyncInitializeAttempt::AlreadyInitialized);
        }
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_key(catalog, placeholder),
                snapshot_key_order_offset(catalog),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_timestamp_key(catalog, row.created_at_micros, placeholder),
                snapshot_timestamp_key_order_offset(catalog, row.created_at_micros),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_snapshot_indexes(self, &trx, catalog, &row)?;
        stage_fdb_conflict_watermarks_initialized(self, &trx, catalog);
        for setting in metadata {
            trx.set(
                &self.namespaced_key(&metadata_setting_key(catalog, setting.scope, &setting.key)),
                setting.value.as_bytes(),
            );
        }
        let versionstamp = trx.get_versionstamp();
        wait_for_sync_initialize_commit_barrier(catalog);
        crate::fdb_runtime::record_transaction_usage(&trx);
        crate::fdb_runtime::wait_for_fdb("wait.fdb.commit", trx.commit())
            .map_err(map_fdb_commit_error)?;
        inject_sync_initialize_commit_unknown(catalog)?;
        let order = committed_order(
            crate::fdb_runtime::wait_for_fdb("wait.fdb.versionstamp", versionstamp)
                .map_err(map_fdb_error)?
                .deref(),
        )?;
        Ok(SyncInitializeAttempt::Created(
            SnapshotRow::with_created_at_micros(order, row.sequence, row.created_at_micros),
        ))
    }

    pub fn initialize_catalog_if_absent_versionstamped(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        self.initialize_catalog_if_absent_with_metadata_versionstamped(catalog, &[])
    }

    pub(crate) fn initialize_catalog_with_metadata_if_absent_versionstamped(
        &self,
        catalog: crate::CatalogId,
        metadata: &[MetadataSettingRow],
    ) -> CatalogResult<SnapshotRow> {
        self.initialize_catalog_if_absent_with_metadata_versionstamped(catalog, metadata)
    }

    fn initialize_catalog_if_absent_with_metadata_versionstamped(
        &self,
        catalog: crate::CatalogId,
        metadata: &[MetadataSettingRow],
    ) -> CatalogResult<SnapshotRow> {
        let mut last_retry_error = None;
        for _ in 0..=crate::fdb_async::MAX_FDB_READ_RETRIES {
            match self.try_initialize_catalog(catalog, metadata)? {
                SyncInitializeOutcome::Complete(row) => return Ok(row),
                SyncInitializeOutcome::Retry(error) => last_retry_error = Some(error),
            }
        }
        Err(last_retry_error.unwrap_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb sync catalog initialization retry loop did not run".to_owned(),
            )
        }))
    }

    fn try_initialize_catalog(
        &self,
        catalog: crate::CatalogId,
        metadata: &[MetadataSettingRow],
    ) -> CatalogResult<SyncInitializeOutcome> {
        match latest_snapshot_uncached(self, catalog).row {
            Ok(Some(row)) => return Ok(SyncInitializeOutcome::Complete(row)),
            Ok(None) => {}
            Err(error) if crate::fdb_async::is_retryable_async_catalog_error(&error) => {
                return Ok(SyncInitializeOutcome::Retry(error));
            }
            Err(error) => return Err(error),
        }
        let attempt =
            match self.initialize_empty_catalog_with_metadata_versionstamped(catalog, metadata) {
                Ok(attempt) => attempt,
                Err(error) if crate::fdb_async::is_retryable_async_catalog_error(&error) => {
                    return Ok(SyncInitializeOutcome::Retry(error));
                }
                Err(error) => return Err(error),
            };
        match attempt {
            SyncInitializeAttempt::Created(row) => Ok(SyncInitializeOutcome::Complete(row)),
            SyncInitializeAttempt::AlreadyInitialized => self.load_initialized_catalog(catalog),
        }
    }

    fn load_initialized_catalog(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SyncInitializeOutcome> {
        match latest_snapshot_uncached(self, catalog).row {
            Ok(Some(row)) => Ok(SyncInitializeOutcome::Complete(row)),
            Ok(None) => Err(CatalogError::InvalidMutation(
                "foundationdb initialization marker exists without a snapshot".to_owned(),
            )),
            Err(error) if crate::fdb_async::is_retryable_async_catalog_error(&error) => {
                Ok(SyncInitializeOutcome::Retry(error))
            }
            Err(error) => Err(error),
        }
    }

    fn latest_snapshot_for_initialization(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        latest_snapshot_uncached(self, catalog).row?.ok_or_else(|| {
            CatalogError::InvalidMutation(
                "foundationdb initialization marker exists without a snapshot".to_owned(),
            )
        })
    }

    pub fn append_data_files_versionstamped(
        &self,
        catalog: crate::CatalogId,
        mut rows: Vec<DataFileRow>,
    ) -> CatalogResult<Vec<DataFileRow>> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let latest = latest_snapshot(self, catalog)?;
        let next_sequence = latest.map_or(crate::RawSnapshotSequence::initial(), |snapshot| {
            snapshot.sequence.next()
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        let estimated_bytes = estimate_versionstamped_append_bytes(catalog, &snapshot, &rows);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped append is {estimated_bytes} bytes, over {} byte limit",
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
            &snapshot.encode(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_snapshot_indexes(self, &trx, catalog, &snapshot)?;
        for row in &mut rows {
            row.validity = ValidityWindow::new(placeholder, None);
            stage_data_file(self, &trx, catalog, row)?;
        }
        let versionstamp = trx.get_versionstamp();
        crate::fdb_runtime::record_transaction_usage(&trx);
        crate::fdb_runtime::wait_for_fdb("wait.fdb.commit", trx.commit())
            .map_err(map_fdb_commit_error)?;
        let order = committed_order(
            crate::fdb_runtime::wait_for_fdb("wait.fdb.versionstamp", versionstamp)
                .map_err(map_fdb_error)?
                .deref(),
        )?;
        for row in &mut rows {
            row.validity = ValidityWindow::new(order, None);
        }
        Ok(rows)
    }

    pub fn expire_data_file_versionstamped(
        &self,
        catalog: crate::CatalogId,
        data_file_id: crate::DataFileId,
    ) -> CatalogResult<DataFileRow> {
        let Some(value) = self.get(&data_file_key(catalog, data_file_id))? else {
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
                "foundationdb versionstamped data-file expiration is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        trx.clear(&self.namespaced_key(&current_data_file_key(
            catalog,
            row.table_id,
            row.data_file_id,
        )));
        trx.atomic_op(
            &self.namespaced_key(&data_file_key(catalog, row.data_file_id)),
            &versionstamped_value(&row.encode(), DataFileRow::END_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.namespaced_key(&data_file_begin_key(
                catalog,
                row.table_id,
                row.validity.begin_order,
                row.data_file_id,
            )),
            &versionstamped_value(&row.encode(), DataFileRow::END_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &data_file_end_key(catalog, row.table_id, placeholder, row.data_file_id),
                data_file_end_key_order_offset(catalog, row.table_id, row.data_file_id),
            )?,
            &row.data_file_id.0.to_be_bytes(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &table_data_file_change_key(
                    catalog,
                    row.table_id,
                    placeholder,
                    DataFileChangeKind::Removed,
                    row.data_file_id,
                ),
                table_data_file_change_key_order_offset(catalog, row.table_id),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &snapshot_data_file_change_key(
                    catalog,
                    row.table_id,
                    placeholder,
                    DataFileChangeKind::Removed,
                    row.data_file_id,
                ),
                snapshot_data_file_change_key_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        let versionstamp = trx.get_versionstamp();
        crate::fdb_runtime::record_transaction_usage(&trx);
        crate::fdb_runtime::wait_for_fdb("wait.fdb.commit", trx.commit())
            .map_err(map_fdb_commit_error)?;
        let order = committed_order(
            crate::fdb_runtime::wait_for_fdb("wait.fdb.versionstamp", versionstamp)
                .map_err(map_fdb_error)?
                .deref(),
        )?;
        row.validity.end_order = Some(order);
        Ok(row)
    }

    pub(crate) fn versionstamped_key(
        &self,
        catalog_key: &[u8],
        order_offset: usize,
    ) -> CatalogResult<Vec<u8>> {
        let prefix = catalog_key.get(..order_offset).ok_or_else(|| {
            CatalogError::InvalidMutation("versionstamp offset exceeds its catalog key".to_owned())
        })?;
        let namespaced_offset = self.namespaced_key(prefix).len();
        let mut key = self.namespaced_key(catalog_key);
        append_versionstamp_offset(&mut key, namespaced_offset)?;
        Ok(key)
    }
}

enum SyncInitializeAttempt {
    Created(SnapshotRow),
    AlreadyInitialized,
}

enum SyncInitializeOutcome {
    Complete(SnapshotRow),
    Retry(CatalogError),
}

#[cfg(debug_assertions)]
fn inject_sync_initialize_commit_unknown(catalog: crate::CatalogId) -> CatalogResult<()> {
    let Ok(request) = std::env::var("AUX_DUCKLAKE_TEST_FDB_SYNC_INITIALIZE_UNKNOWN_ONCE") else {
        return Ok(());
    };
    let Some(requested_catalog) = request
        .strip_prefix("after:")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return Ok(());
    };
    if requested_catalog != catalog.0 {
        return Ok(());
    }
    let Ok(mut last_request) = LAST_SYNC_INITIALIZE_UNKNOWN_INJECTION.lock() else {
        return Ok(());
    };
    if last_request.as_ref() == Some(&request) {
        return Ok(());
    }
    *last_request = Some(request);
    Err(map_fdb_error(foundationdb::FdbError::from_code(1021)))
}

#[cfg(not(debug_assertions))]
const fn inject_sync_initialize_commit_unknown(_catalog: crate::CatalogId) -> CatalogResult<()> {
    Ok(())
}

#[cfg(debug_assertions)]
fn wait_for_sync_initialize_commit_barrier(catalog: crate::CatalogId) {
    let barrier_catalog = std::env::var("AUX_DUCKLAKE_TEST_FDB_SYNC_INITIALIZE_BARRIER")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    if barrier_catalog == Some(catalog.0) {
        SYNC_INITIALIZE_COMMIT_BARRIER
            .get_or_init(|| Barrier::new(2))
            .wait();
    }
}

#[cfg(not(debug_assertions))]
const fn wait_for_sync_initialize_commit_barrier(_catalog: crate::CatalogId) {}

#[cfg(debug_assertions)]
static LAST_SYNC_INITIALIZE_UNKNOWN_INJECTION: Mutex<Option<String>> = Mutex::new(None);

#[cfg(debug_assertions)]
static SYNC_INITIALIZE_COMMIT_BARRIER: OnceLock<Barrier> = OnceLock::new();
