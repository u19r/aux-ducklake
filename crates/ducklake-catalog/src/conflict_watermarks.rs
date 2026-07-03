use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, OrderedCatalogKv,
    keys::{conflict_max_catalog_id_key, conflict_max_file_id_key},
};

pub(crate) struct ConflictWatermarkValues {
    pub(crate) max_catalog_id: Option<u64>,
    pub(crate) max_file_id: Option<u64>,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_max_catalog_id_watermark(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_u64(kv, &conflict_max_catalog_id_key(catalog))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_max_file_id_watermark(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Option<u64>> {
    load_u64(kv, &conflict_max_file_id_key(catalog))
}

pub(crate) fn load_conflict_watermarks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<ConflictWatermarkValues> {
    let keys = [
        conflict_max_catalog_id_key(catalog),
        conflict_max_file_id_key(catalog),
    ];
    let values = kv.batch_get(&keys)?;
    Ok(ConflictWatermarkValues {
        max_catalog_id: decode_optional_u64(&keys[0], values.first().and_then(Option::as_deref))?,
        max_file_id: decode_optional_u64(&keys[1], values.get(1).and_then(Option::as_deref))?,
    })
}

pub(crate) fn stage_max_catalog_id_watermark(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
    candidate: u64,
) -> CatalogResult<()> {
    stage_max_u64(kv, batch, conflict_max_catalog_id_key(catalog), candidate)
}

pub(crate) fn stage_max_file_id_watermark(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
    candidate: u64,
) -> CatalogResult<()> {
    stage_max_u64(kv, batch, conflict_max_file_id_key(catalog), candidate)
}

fn stage_max_u64(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    key: Vec<u8>,
    candidate: u64,
) -> CatalogResult<()> {
    let value = load_u64(kv, &key)?.map_or(candidate, |current| current.max(candidate));
    batch.put_max_u64(key, value)
}

fn load_u64(kv: &(impl OrderedCatalogKv + ?Sized), key: &[u8]) -> CatalogResult<Option<u64>> {
    kv.get(key)?
        .map(|value| decode_u64(key, &value))
        .transpose()
}

fn decode_optional_u64(key: &[u8], value: Option<&[u8]>) -> CatalogResult<Option<u64>> {
    value.map(|value| decode_u64(key, value)).transpose()
}

fn decode_u64(key: &[u8], value: &[u8]) -> CatalogResult<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        let key_label = crate::keys::decode_key(key).unwrap_or_else(|_| "<invalid-key>".to_owned());
        CatalogError::InvalidKey(format!("invalid u64 watermark value for key {}", key_label))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_max_catalog_id_watermark(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    candidate: u64,
) {
    stage_fdb_max_u64(kv, trx, conflict_max_catalog_id_key(catalog), candidate);
}

#[cfg(feature = "foundationdb")]
pub(crate) fn stage_fdb_max_file_id_watermark(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    candidate: u64,
) {
    stage_fdb_max_u64(kv, trx, conflict_max_file_id_key(catalog), candidate);
}

#[cfg(feature = "foundationdb")]
fn stage_fdb_max_u64(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    key: Vec<u8>,
    candidate: u64,
) {
    trx.atomic_op(
        &kv.namespaced_key(&key),
        &candidate.to_be_bytes(),
        foundationdb::options::MutationType::ByteMax,
    );
}

#[cfg(test)]
#[path = "conflict_watermarks_tests.rs"]
mod conflict_watermarks_tests;
