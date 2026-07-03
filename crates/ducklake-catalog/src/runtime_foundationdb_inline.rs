use crate::{
    CatalogId, CatalogResult, InlineRowChangeKind,
    runtime_inline_ops::{RuntimeInlineDelete, RuntimeInlineRows},
    runtime_inline_rows::{InlineRowChangesPayload, ReadInlineRowsPayload},
};

#[cfg(feature = "foundationdb")]
use crate::{
    runtime_foundationdb::open_foundationdb_catalog,
    runtime_inline_rows::{
        inline_row_changes_payload, read_inline_rows_aggregate_stats_payload,
        read_inline_rows_global_stats_batch_payload, read_inline_rows_global_stats_payload,
        read_inline_rows_payload,
    },
    snapshot_by_ducklake_sequence,
    table_store::load_current_table_row,
};

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows(
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_inline_rows_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats(
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_inline_rows_global_stats_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_aggregate_stats(
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_inline_rows_aggregate_stats_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats_batch(
    catalog: CatalogId,
    payloads: Vec<ReadInlineRowsPayload>,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_inline_rows_global_stats_batch_payload(kv, catalog, payloads)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_list_inline_row_changes(
    catalog: CatalogId,
    payload: InlineRowChangesPayload,
    kind: InlineRowChangeKind,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    inline_row_changes_payload(&kv, catalog, payload, kind)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_register_inline_rows(
    catalog: CatalogId,
    request: RuntimeInlineRows,
) -> CatalogResult<Vec<crate::InlineTableChunkRow>> {
    let kv = open_foundationdb_catalog()?;
    reject_stale_inline_table_metadata(&kv, catalog, &request)?;
    let table = crate::runtime_inline_ops::inline_table_for_register(&kv, catalog, &request)?;
    kv.register_inline_table_payload_with_table_at_snapshot_versionstamped(
        catalog,
        table,
        crate::SchemaId(request.schema_version),
        request.payload.into_bytes(),
        request.commit_snapshot,
        request.read_snapshot,
        Some(&request.commit_metadata),
    )
}

#[cfg(feature = "foundationdb")]
fn reject_stale_inline_table_metadata(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    request: &RuntimeInlineRows,
) -> CatalogResult<()> {
    let Some(read_snapshot) = request.read_snapshot else {
        return Ok(());
    };
    let Some(read_snapshot) = snapshot_by_ducklake_sequence(kv, catalog, read_snapshot)? else {
        return Ok(());
    };
    let Some(current_table) = load_current_table_row(kv, catalog, request.table_id)? else {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing inline rows: table {} was dropped after read snapshot",
            request.table_id.0
        )));
    };
    let Some(read_table) =
        crate::load_table_at(kv, catalog, request.table_id, read_snapshot.order)?
    else {
        return Ok(());
    };
    if !same_user_visible_table_for_inline_insert(&read_table, &current_table) {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing inline rows: table {} metadata changed after read snapshot",
            request.table_id.0
        )));
    }
    if read_table.partition != current_table.partition {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict committing inline rows: table {} partition metadata changed after read snapshot",
            request.table_id.0
        )));
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn same_user_visible_table_for_inline_insert(
    read_table: &crate::TableRow,
    current_table: &crate::TableRow,
) -> bool {
    let mut read_table = read_table.clone();
    let mut current_table = current_table.clone();
    read_table.inlined_data_tables.clear();
    current_table.inlined_data_tables.clear();
    read_table.validity = current_table.validity.clone();
    read_table == current_table
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_delete_inline_rows(
    catalog: CatalogId,
    request: RuntimeInlineDelete,
) -> CatalogResult<crate::InlineTableDeleteCommit> {
    let kv = open_foundationdb_catalog()?;
    let mut commit = crate::InlineTableDeleteCommit {
        deleted_row_count: 0,
        rewritten_payload_count: 0,
    };
    for target in &request.targets {
        let schema_id = crate::runtime_inline_ops::inline_delete_schema_id(&kv, catalog, target)?;
        let next = kv.commit_delete_inline_table_rows_versionstamped(
            catalog,
            target.table_id,
            schema_id,
            &target.row_ids,
            request.commit_snapshot,
        )?;
        commit.deleted_row_count += next.deleted_row_count;
        commit.rewritten_payload_count += next.rewritten_payload_count;
    }
    Ok(commit)
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_read_inline_rows(
    _catalog: CatalogId,
    _payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats(
    _catalog: CatalogId,
    _payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_read_inline_rows_aggregate_stats(
    _catalog: CatalogId,
    _payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats_batch(
    _catalog: CatalogId,
    _payloads: Vec<ReadInlineRowsPayload>,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_list_inline_row_changes(
    _catalog: CatalogId,
    _payload: InlineRowChangesPayload,
    _kind: InlineRowChangeKind,
) -> CatalogResult<Vec<u8>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_register_inline_rows(
    _catalog: CatalogId,
    _request: RuntimeInlineRows,
) -> CatalogResult<Vec<crate::InlineTableChunkRow>> {
    foundationdb_runtime_inline_chunks_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_delete_inline_rows(
    _catalog: CatalogId,
    _request: RuntimeInlineDelete,
) -> CatalogResult<crate::InlineTableDeleteCommit> {
    foundationdb_runtime_inline_delete_error()
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_error() -> CatalogResult<Vec<u8>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_inline_chunks_error() -> CatalogResult<Vec<crate::InlineTableChunkRow>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_inline_delete_error() -> CatalogResult<crate::InlineTableDeleteCommit> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(test)]
#[path = "runtime_foundationdb_inline_tests.rs"]
mod runtime_foundationdb_inline_tests;
