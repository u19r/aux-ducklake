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
use futures::executor::block_on;

use crate::{
    CatalogCacheNamespace, CatalogError, CatalogResult, DataFileChangeKind, DataFileRow,
    DeleteFileRow, SnapshotRow, ValidityWindow,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error, shared_foundationdb_database},
    fdb_versionstamp::{
        append_versionstamp_offset, committed_order, data_file_begin_key_order_offset,
        data_file_end_key_order_offset, delete_file_timeline_key_order_offset,
        estimate_versionstamped_append_bytes, estimate_versionstamped_delete_bytes,
        estimate_versionstamped_expire_bytes, incomplete_order,
        order_delete_file_change_key_order_offset, snapshot_data_file_change_key_order_offset,
        snapshot_key_order_offset, snapshot_timestamp_key_order_offset, strip_namespace,
        table_data_file_change_key_order_offset, table_delete_file_change_key_order_offset,
        versionstamped_value,
    },
    keys::{
        current_data_file_key, current_delete_file_key, data_file_begin_key, data_file_end_key,
        data_file_key, delete_file_key, delete_file_timeline_key, order_delete_file_change_key,
        snapshot_data_file_change_key, snapshot_key, snapshot_timestamp_key,
        table_data_file_change_key, table_delete_file_change_key,
    },
    kv::OrderedCatalogKv,
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
};

#[derive(Clone)]
pub struct FdbOrderedCatalogKv {
    db: Arc<Database>,
    key_prefix: Vec<u8>,
}

impl FdbOrderedCatalogKv {
    pub(crate) const MAX_COMMIT_BYTES: usize = 1024 * 1024;

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
        }
    }

    #[must_use]
    pub fn key_prefix(&self) -> &[u8] {
        &self.key_prefix
    }

    pub(crate) fn namespaced_key(&self, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.key_prefix.len().saturating_add(key.len()));
        out.extend_from_slice(&self.key_prefix);
        out.extend_from_slice(key);
        out
    }

    pub(crate) fn catalog_cache_namespace(&self) -> CatalogCacheNamespace {
        let mut hasher = DefaultHasher::new();
        self.key_prefix.hash(&mut hasher);
        CatalogCacheNamespace::foundationdb(Arc::as_ptr(&self.db) as usize, hasher.finish())
    }

    pub(crate) fn strip_namespace(&self, key: &[u8]) -> CatalogResult<Vec<u8>> {
        strip_namespace(&self.key_prefix, key)
    }

    pub(crate) fn create_transaction(&self) -> CatalogResult<foundationdb::Transaction> {
        let trx = self.db.create_trx().map_err(map_fdb_error)?;
        trx.set_option(TransactionOption::Timeout(5_000))
            .map_err(map_fdb_error)?;
        Ok(trx)
    }

    pub fn initialize_empty_catalog_versionstamped(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        let placeholder = incomplete_order();
        let row = SnapshotRow::initial(placeholder);
        let trx = self.create_transaction()?;
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
            &row.sequence.to_be_bytes(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_latest_snapshot_value(self, &trx, catalog, &row)?;
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        Ok(SnapshotRow::with_created_at_micros(
            order,
            row.sequence,
            row.created_at_micros,
        ))
    }

    pub fn initialize_catalog_if_absent_versionstamped(
        &self,
        catalog: crate::CatalogId,
    ) -> CatalogResult<SnapshotRow> {
        match latest_snapshot(self, catalog)? {
            Some(row) => Ok(row),
            None => self.initialize_empty_catalog_versionstamped(catalog),
        }
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
            &snapshot.sequence.to_be_bytes(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_latest_snapshot_value(self, &trx, catalog, &snapshot)?;
        for row in &mut rows {
            row.validity = ValidityWindow::new(placeholder, None);
            trx.atomic_op(
                &self.namespaced_key(&current_data_file_key(
                    catalog,
                    row.table_id,
                    row.data_file_id,
                )),
                &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            trx.atomic_op(
                &self.namespaced_key(&data_file_key(catalog, row.data_file_id)),
                &versionstamped_value(&row.encode(), DataFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            trx.atomic_op(
                &self.versionstamped_key(
                    &data_file_begin_key(catalog, row.table_id, placeholder, row.data_file_id),
                    data_file_begin_key_order_offset(catalog, row.table_id),
                )?,
                &row.encode(),
                MutationType::SetVersionstampedKey,
            );
            trx.atomic_op(
                &self.versionstamped_key(
                    &table_data_file_change_key(
                        catalog,
                        row.table_id,
                        placeholder,
                        DataFileChangeKind::Added,
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
                        DataFileChangeKind::Added,
                        row.data_file_id,
                    ),
                    snapshot_data_file_change_key_order_offset(catalog),
                )?,
                &[],
                MutationType::SetVersionstampedKey,
            );
        }
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        for row in &mut rows {
            row.validity = ValidityWindow::new(order, None);
        }
        Ok(rows)
    }

    pub fn register_delete_file_versionstamped(
        &self,
        catalog: crate::CatalogId,
        mut row: DeleteFileRow,
    ) -> CatalogResult<DeleteFileRow> {
        let Some(data_file_value) = self.get(&data_file_key(catalog, row.data_file_id))? else {
            return Err(CatalogError::NotFound("data file"));
        };
        let data_file = DataFileRow::decode(&data_file_value)?;
        let placeholder = incomplete_order();
        row.validity = ValidityWindow::new(placeholder, None);
        let estimated_bytes =
            estimate_versionstamped_delete_bytes(catalog, data_file.table_id, &row);
        if estimated_bytes > Self::MAX_COMMIT_BYTES {
            return Err(CatalogError::InvalidMutation(format!(
                "foundationdb versionstamped delete-file registration is {estimated_bytes} bytes, over {} byte limit",
                Self::MAX_COMMIT_BYTES
            )));
        }

        let trx = self.create_transaction()?;
        trx.atomic_op(
            &self.namespaced_key(&current_delete_file_key(catalog, row.data_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.namespaced_key(&delete_file_key(catalog, row.delete_file_id)),
            &versionstamped_value(&row.encode(), DeleteFileRow::BEGIN_ORDER_BYTES_OFFSET)?,
            MutationType::SetVersionstampedValue,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &delete_file_timeline_key(
                    catalog,
                    row.data_file_id,
                    placeholder,
                    row.delete_file_id,
                ),
                delete_file_timeline_key_order_offset(catalog, row.data_file_id),
            )?,
            &row.encode(),
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &table_delete_file_change_key(
                    catalog,
                    data_file.table_id,
                    placeholder,
                    row.delete_file_id,
                ),
                table_delete_file_change_key_order_offset(catalog, data_file.table_id),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        trx.atomic_op(
            &self.versionstamped_key(
                &order_delete_file_change_key(
                    catalog,
                    placeholder,
                    data_file.table_id,
                    row.delete_file_id,
                ),
                order_delete_file_change_key_order_offset(catalog),
            )?,
            &[],
            MutationType::SetVersionstampedKey,
        );
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        row.validity = ValidityWindow::new(order, None);
        Ok(row)
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
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        let order = committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?;
        row.validity.end_order = Some(order);
        Ok(row)
    }

    pub(crate) fn versionstamped_key(
        &self,
        catalog_key: &[u8],
        order_offset: usize,
    ) -> CatalogResult<Vec<u8>> {
        let namespaced_offset = self.key_prefix.len().saturating_add(order_offset);
        let mut key = self.namespaced_key(catalog_key);
        append_versionstamp_offset(&mut key, namespaced_offset)?;
        Ok(key)
    }
}
