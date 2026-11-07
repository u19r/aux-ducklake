#[cfg(test)]
use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, OrderedCatalogKv, TableId,
    keys::{
        conflict_max_catalog_id_key, conflict_max_file_id_key, conflict_watermarks_initialized_key,
        current_schema_version_key, table_next_row_id_key,
    },
};

const CONFLICT_WATERMARKS_INITIALIZED_VALUE: [u8; 1] = [1];
const IMPLICIT_MAIN_SCHEMA_ID: u64 = 0;

pub(crate) struct ConflictWatermarkValues {
    pub(crate) max_catalog_id: Option<u64>,
    pub(crate) max_file_id: Option<u64>,
}

pub(crate) fn stage_conflict_watermarks_initialized(batch: &mut KvBatch, catalog: CatalogId) {
    batch.put(
        conflict_watermarks_initialized_key(catalog),
        CONFLICT_WATERMARKS_INITIALIZED_VALUE.to_vec(),
    );
    batch.put(
        conflict_max_catalog_id_key(catalog),
        IMPLICIT_MAIN_SCHEMA_ID.to_be_bytes().to_vec(),
    );
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
    let values = fetch_conflict_values(kv, catalog, &conflict_watermark_keys(catalog))?;
    decode_conflict_watermarks(catalog, &values)
}

pub(crate) fn load_conflict_snapshot_values(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<(ConflictWatermarkValues, Option<u64>)> {
    let [marker, catalog_key, file_key] = conflict_watermark_keys(catalog);
    let keys = [
        marker,
        catalog_key,
        file_key,
        current_schema_version_key(catalog),
    ];
    let [marker, catalog_id, file_id, schema] = fetch_conflict_values(kv, catalog, &keys)?;
    let watermarks = decode_conflict_watermarks(catalog, &[marker, catalog_id, file_id])?;
    let schema_version = schema
        .as_deref()
        .map(|value| crate::schema_version_state::decode_schema_version(&keys[3], value))
        .transpose()?;
    Ok((watermarks, schema_version))
}

fn fetch_conflict_values<const N: usize>(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    keys: &[Vec<u8>; N],
) -> CatalogResult<[Option<Vec<u8>>; N]> {
    let values = crate::catalog_request_read_set::find_values(kv, catalog, keys)
        .map_or_else(|| kv.batch_get(keys), Ok)?;
    let count = values.len();
    values.try_into().map_err(|_| {
        CatalogError::Backend(format!(
            "conflict watermark batch returned {count} values for {N} keys"
        ))
    })
}

fn conflict_watermark_keys(catalog: CatalogId) -> [Vec<u8>; 3] {
    [
        conflict_watermarks_initialized_key(catalog),
        conflict_max_catalog_id_key(catalog),
        conflict_max_file_id_key(catalog),
    ]
}

fn decode_conflict_watermarks(
    catalog: CatalogId,
    values: &[Option<Vec<u8>>; 3],
) -> CatalogResult<ConflictWatermarkValues> {
    let keys = conflict_watermark_keys(catalog);
    validate_initialized_marker(&keys[0], values[0].as_deref())?;
    Ok(ConflictWatermarkValues {
        max_catalog_id: decode_optional_u64(&keys[1], values[1].as_deref())?,
        max_file_id: decode_optional_u64(&keys[2], values[2].as_deref())?,
    })
}

fn validate_initialized_marker(key: &[u8], value: Option<&[u8]>) -> CatalogResult<()> {
    if value == Some(CONFLICT_WATERMARKS_INITIALIZED_VALUE.as_slice()) {
        return Ok(());
    }
    let key_label = crate::keys::decode_key(key).unwrap_or_else(|_| "<invalid-key>".to_owned());
    Err(CatalogError::InvalidKey(format!(
        "missing or invalid conflict watermarks initialized marker for key {key_label}"
    )))
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

pub(crate) fn load_table_next_row_id(
    kv: &(impl OrderedCatalogKv + ?Sized),
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<u64> {
    Ok(load_u64(kv, &table_next_row_id_key(catalog, table_id))?.unwrap_or_default())
}

#[cfg(test)]
pub(crate) fn decode_table_next_row_ids(
    catalog: CatalogId,
    table_ids: &[TableId],
    values: Vec<Option<Vec<u8>>>,
) -> CatalogResult<BTreeMap<TableId, u64>> {
    if values.len() != table_ids.len() {
        return Err(CatalogError::Backend(format!(
            "table next-row-id batch returned {} values for {} keys",
            values.len(),
            table_ids.len()
        )));
    }
    table_ids
        .iter()
        .copied()
        .zip(values)
        .map(|(table_id, value)| {
            decode_table_next_row_id(catalog, table_id, value.as_deref())
                .map(|next| (table_id, next))
        })
        .collect()
}

pub(crate) fn decode_table_next_row_id(
    catalog: CatalogId,
    table_id: TableId,
    value: Option<&[u8]>,
) -> CatalogResult<u64> {
    Ok(decode_optional_u64(&table_next_row_id_key(catalog, table_id), value)?.unwrap_or_default())
}

pub(crate) fn stage_table_next_row_id(
    kv: &(impl OrderedCatalogKv + ?Sized),
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    candidate: u64,
) -> CatalogResult<()> {
    stage_max_u64(
        kv,
        batch,
        table_next_row_id_key(catalog, table_id),
        candidate,
    )
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
pub(crate) fn stage_fdb_conflict_watermarks_initialized(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
) {
    trx.set(
        &kv.namespaced_key(&conflict_watermarks_initialized_key(catalog)),
        &CONFLICT_WATERMARKS_INITIALIZED_VALUE,
    );
    stage_fdb_max_catalog_id_watermark(kv, trx, catalog, IMPLICIT_MAIN_SCHEMA_ID);
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
pub(crate) fn stage_fdb_table_next_row_id(
    kv: &crate::FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    table_id: TableId,
    candidate: u64,
) {
    stage_fdb_max_u64(kv, trx, table_next_row_id_key(catalog, table_id), candidate);
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
