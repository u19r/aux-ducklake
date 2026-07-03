use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, OrderedCatalogKv,
    keys::{catalog_snapshot_version_key, current_schema_version_key},
};

pub(crate) fn load_current_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_schema_version(kv, &current_schema_version_key(catalog))
}

pub(crate) fn load_catalog_snapshot_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_schema_version(kv, &catalog_snapshot_version_key(catalog))
}

pub(crate) fn stage_next_schema_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
) -> CatalogResult<()> {
    let current = load_schema_version(kv, &current_schema_version_key(catalog))?.unwrap_or(0);
    batch.put(
        current_schema_version_key(catalog),
        current.saturating_add(1).to_be_bytes().to_vec(),
    );
    stage_next_catalog_snapshot_version(kv, batch, catalog)?;
    Ok(())
}

pub(crate) fn stage_next_catalog_snapshot_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
) -> CatalogResult<()> {
    let current = load_schema_version(kv, &catalog_snapshot_version_key(catalog))?.unwrap_or(0);
    batch.put(
        catalog_snapshot_version_key(catalog),
        current.saturating_add(1).to_be_bytes().to_vec(),
    );
    Ok(())
}

fn load_schema_version(
    kv: &(impl OrderedCatalogKv + ?Sized),
    key: &[u8],
) -> CatalogResult<Option<u64>> {
    kv.get(key)?
        .map(|value| decode_schema_version(key, &value))
        .transpose()
}

fn decode_schema_version(key: &[u8], value: &[u8]) -> CatalogResult<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        let key_label = crate::keys::decode_key(key).unwrap_or_else(|_| "<invalid-key>".to_owned());
        CatalogError::InvalidKey(format!(
            "invalid current schema version value for key {}",
            key_label
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_next_schema_version(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
) -> CatalogResult<()> {
    stage_fdb_next_version_key(kv, trx, current_schema_version_key(catalog))?;
    stage_fdb_next_catalog_snapshot_version(kv, trx, catalog)?;
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_next_catalog_snapshot_version(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
) -> CatalogResult<()> {
    stage_fdb_next_version_key(kv, trx, catalog_snapshot_version_key(catalog))
}

#[cfg(feature = "foundationdb")]
fn stage_fdb_next_version_key(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    key: Vec<u8>,
) -> CatalogResult<()> {
    let namespaced = kv.namespaced_key(&key);
    let current = futures::executor::block_on(trx.get(&namespaced, false))
        .map_err(crate::fdb_runtime::map_fdb_error)?
        .map(|value| decode_schema_version(&key, &value))
        .transpose()?
        .unwrap_or(0);
    trx.set(&namespaced, &current.saturating_add(1).to_be_bytes());
    Ok(())
}

#[cfg(test)]
#[path = "schema_version_state_tests.rs"]
mod schema_version_state_tests;
