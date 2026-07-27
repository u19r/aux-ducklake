#[cfg(feature = "foundationdb")]
use std::collections::BTreeMap;
#[cfg(all(feature = "foundationdb", not(test)))]
use std::sync::OnceLock;

#[cfg(all(feature = "foundationdb", not(test)))]
use crate::{
    CatalogCacheNamespace,
    bounded_cache::{BoundedCache, static_bounded_cache},
};

use crate::{
    CatalogId, CatalogResult, InlineRowChangeKind, InlineTableChunkRow,
    runtime_inline_ops::{RuntimeInlineDelete, RuntimeInlineRows},
    runtime_inline_rows::{InlineRowChangesPayload, ReadInlineRowsPayload},
};

#[cfg(feature = "foundationdb")]
use crate::{
    runtime_foundationdb::open_foundationdb_catalog,
    runtime_inline_rows::{
        inline_row_changes_payload, read_foundationdb_inline_rows_aggregate_stats_payload,
        read_foundationdb_inline_rows_global_stats_payload, read_foundationdb_inline_rows_payload,
        read_inline_rows_global_stats_batch_payload,
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
    read_foundationdb_inline_rows_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats(
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_foundationdb_inline_rows_global_stats_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_aggregate_stats(
    catalog: CatalogId,
    payload: ReadInlineRowsPayload,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    read_foundationdb_inline_rows_aggregate_stats_payload(&kv, catalog, payload)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_read_inline_rows_global_stats_batch(
    catalog: CatalogId,
    payloads: Vec<ReadInlineRowsPayload>,
) -> CatalogResult<Vec<u8>> {
    let kv = open_foundationdb_catalog()?;
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        let key = InlineGlobalStatsBatchCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            requests: payloads
                .iter()
                .map(|payload| {
                    (
                        payload.table_name.clone(),
                        payload.snapshot.map(|snapshot| snapshot.public_id()),
                    )
                })
                .collect(),
        };
        let cache = inline_global_stats_batch_cache();
        if let Some(payload) = cache.get_ref(&key) {
            return Ok(payload);
        }
        let result = read_inline_rows_global_stats_batch_payload(kv, catalog, payloads)?;
        cache.insert(key, result.clone());
        return Ok(result);
    }
    read_inline_rows_global_stats_batch_payload(kv, catalog, payloads)
}

#[cfg(all(feature = "foundationdb", not(test)))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InlineGlobalStatsBatchCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    requests: Vec<(String, Option<crate::DuckLakeSnapshotId>)>,
}

#[cfg(all(feature = "foundationdb", not(test)))]
static INLINE_GLOBAL_STATS_BATCH_CACHE: OnceLock<
    BoundedCache<InlineGlobalStatsBatchCacheKey, Vec<u8>>,
> = OnceLock::new();

#[cfg(all(feature = "foundationdb", not(test)))]
fn inline_global_stats_batch_cache()
-> &'static BoundedCache<InlineGlobalStatsBatchCacheKey, Vec<u8>> {
    static_bounded_cache(&INLINE_GLOBAL_STATS_BATCH_CACHE, 256)
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
    requests: Vec<RuntimeInlineRows>,
) -> CatalogResult<Vec<crate::InlineTableChunkRow>> {
    let Some(first) = requests.first() else {
        return Ok(Vec::new());
    };
    let read_snapshot = first.read_snapshot;
    let commit_snapshot = first.commit_snapshot;
    let commit_metadata = first.commit_metadata.clone();
    runtime_foundationdb_commit_inline_mutations(
        catalog,
        requests,
        Vec::new(),
        read_snapshot,
        commit_snapshot,
        commit_metadata,
    )
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_commit_inline_mutations(
    catalog: CatalogId,
    requests: Vec<RuntimeInlineRows>,
    deletes: Vec<RuntimeInlineDelete>,
    read_snapshot: Option<crate::DuckLakeSnapshotId>,
    commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    commit_metadata: crate::SnapshotCommitMetadata,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    let kv = open_foundationdb_catalog()?;
    let (tables, payloads, delete_payloads) =
        prepare_foundationdb_inline_mutations(&kv, catalog, requests, deletes, commit_snapshot)?;
    let committed = kv.commit_inline_table_mutations_at_snapshot_versionstamped(
        catalog,
        tables,
        payloads,
        delete_payloads,
        crate::InlineTableCommitContext {
            commit_snapshot,
            read_snapshot,
            commit_metadata: Some(&commit_metadata),
        },
    )?;
    Ok(committed.rows)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn prepare_foundationdb_inline_mutations(
    kv: &crate::FdbOrderedCatalogKv,
    catalog: CatalogId,
    requests: Vec<RuntimeInlineRows>,
    deletes: Vec<RuntimeInlineDelete>,
    commit_snapshot: Option<crate::DuckLakeSnapshotId>,
) -> CatalogResult<(
    Vec<crate::TableRow>,
    Vec<crate::fdb_inline_tables::InlineTablePayload>,
    Vec<crate::fdb_inline_tables::InlineTableDeletePayload>,
)> {
    let mut tables = BTreeMap::<crate::TableId, crate::TableRow>::new();
    let mut changed_tables = std::collections::BTreeSet::new();
    let mut validated_tables = std::collections::BTreeSet::new();
    let mut payloads = BTreeMap::<(crate::TableId, crate::SchemaId), Vec<u8>>::new();
    for request in requests {
        if validated_tables.insert((request.table_id, request.read_snapshot)) {
            reject_stale_inline_table_metadata(kv, catalog, &request)?;
        }
        let table = match tables.entry(request.table_id) {
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                load_current_table_row(kv, catalog, request.table_id)?
                    .ok_or(crate::CatalogError::NotFound("inline table"))?,
            ),
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        };
        if crate::runtime_inline_ops::register_inlined_table(
            table,
            request.table_name,
            request.schema_version,
        ) {
            changed_tables.insert(request.table_id);
        }
        payloads
            .entry((request.table_id, crate::SchemaId(request.schema_version)))
            .or_default()
            .extend_from_slice(request.payload.as_bytes());
    }
    let mut delete_payloads =
        BTreeMap::<(crate::TableId, crate::SchemaId), std::collections::BTreeSet<u64>>::new();
    for delete in deletes {
        if delete.commit_snapshot != commit_snapshot {
            return Err(crate::CatalogError::InvalidMutation(
                "inline delete commit snapshot differs from CommitAttempt".to_owned(),
            ));
        }
        for target in delete.targets {
            let schema_id =
                crate::runtime_inline_ops::inline_delete_schema_id(kv, catalog, &target)?;
            delete_payloads
                .entry((target.table_id, schema_id))
                .or_default()
                .extend(target.row_ids);
        }
    }
    Ok((
        tables
            .into_iter()
            .filter_map(|(table_id, table)| changed_tables.contains(&table_id).then_some(table))
            .collect(),
        payloads
            .into_iter()
            .map(
                |((table_id, schema_id), payload)| crate::fdb_inline_tables::InlineTablePayload {
                    table_id,
                    schema_id,
                    payload,
                },
            )
            .collect(),
        delete_payloads
            .into_iter()
            .map(|((table_id, schema_id), row_ids)| {
                crate::fdb_inline_tables::InlineTableDeletePayload {
                    table_id,
                    schema_id,
                    row_ids: row_ids.into_iter().collect(),
                }
            })
            .collect(),
    ))
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
    read_table.validity = current_table.validity;
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
    _requests: Vec<RuntimeInlineRows>,
) -> CatalogResult<Vec<crate::InlineTableChunkRow>> {
    foundationdb_runtime_inline_chunks_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_commit_inline_mutations(
    _catalog: CatalogId,
    _requests: Vec<RuntimeInlineRows>,
    _deletes: Vec<RuntimeInlineDelete>,
    _read_snapshot: Option<crate::DuckLakeSnapshotId>,
    _commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    _commit_metadata: crate::SnapshotCommitMetadata,
) -> CatalogResult<Vec<InlineTableChunkRow>> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
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
