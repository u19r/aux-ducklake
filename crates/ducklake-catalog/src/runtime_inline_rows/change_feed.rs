use std::collections::BTreeMap;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    AttachedDataFile, CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId,
    InlineRowChangeKind, OrderedCatalogKv, SchemaId,
    inline_change_feed::data_file_covers_inline_begin,
    inline_column_types::inline_columns_payload,
    inline_data::InlineTablePayloadRow,
    inline_read_schema::table_row_for_inline_schema,
    latest_snapshot, list_current_data_files_with_deletes, list_data_files_with_deletes_at,
    list_inline_row_payload_changes, list_snapshots, public_snapshot_sequence_for_order,
    runtime_snapshot_range::{ChangeFeedEndSnapshot, ChangeFeedStartSnapshot},
    runtime_snapshots::{
        ducklake_snapshot_order_span as cached_ducklake_snapshot_order_span,
        public_snapshot_order_span, public_snapshot_sequences_by_order,
    },
    snapshot_by_public_sequence,
};

use crate::runtime_inline_rows::*;

pub(crate) fn inline_row_changes_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    payload: InlineRowChangesPayload,
    kind: InlineRowChangeKind,
) -> CatalogResult<Vec<u8>> {
    let start_order = change_feed_start_order(kv, catalog, payload.start_snapshot)?;
    let end_order = change_feed_end_order(kv, catalog, payload.end_snapshot)?;
    let start_snapshot_id = payload.start_snapshot.public_id().0;
    let end_snapshot_id = payload.end_snapshot.public_id().0;
    let inline_table = InlineTableName::parse(&payload.table_name);
    let (table, schema_id) = load_inlined_table(kv, catalog, end_order, inline_table)?;
    let schema_table = table_row_for_inline_schema(kv, catalog, table.table_id, schema_id.0)?;
    let changes = list_inline_row_payload_changes(
        kv,
        catalog,
        table.table_id,
        schema_id,
        start_order,
        end_order,
        kind,
    )?;
    let mut rendered_changes = Vec::new();
    for change in changes {
        let sequence = change_feed_snapshot_id_for_order(kv, catalog, change.change.order)?
            .ok_or_else(|| missing_snapshot_order(change.change.order))?;
        if sequence < start_snapshot_id || sequence > end_snapshot_id {
            continue;
        }
        let (begin_snapshot, end_snapshot) = match kind {
            InlineRowChangeKind::Inserted => (sequence.to_string(), String::new()),
            InlineRowChangeKind::Deleted => (String::new(), sequence.to_string()),
        };
        let line = String::from_utf8(change.payload).map_err(|error| {
            crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
        })?;
        let fields = inline_row_fields(&line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        if kind == InlineRowChangeKind::Inserted
            && inline_row_materialized_by_visible_file(
                kv,
                catalog,
                table.table_id,
                row_id,
                change.change.order,
                end_order,
            )?
        {
            continue;
        }
        rendered_changes.push(format!(
            "row_change\t{}\t{}\t{}\t{}\n",
            begin_snapshot,
            end_snapshot,
            fields[1],
            fields[2..].join("\t")
        ));
    }
    let mut out = inline_columns_payload(&schema_table)?;
    out.push_str(&format!(
        "inline_row_change_count={}\n",
        rendered_changes.len()
    ));
    for rendered in rendered_changes {
        out.push_str(&rendered);
    }
    Ok(out.into_bytes())
}

pub(super) fn inline_row_materialized_by_visible_file(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    row_id: u64,
    inline_begin_order: CatalogOrderId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<bool> {
    InlineMaterializedFiles::load(kv, catalog, table_id, snapshot_order)
        .map(|files| files.materializes(row_id, inline_begin_order))
}

#[derive(Clone)]
pub(super) struct InlineMaterializedFiles {
    visible_files: Vec<AttachedDataFile>,
}

impl InlineMaterializedFiles {
    pub(super) fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let visible_files = materialized_visible_files(kv, catalog, table_id, snapshot_order)?;
        Ok(Self { visible_files })
    }

    pub(super) fn materializes(&self, row_id: u64, inline_begin_order: CatalogOrderId) -> bool {
        for attached in &self.visible_files {
            if attached
                .delete_file
                .as_ref()
                .is_some_and(|delete| delete.record_count >= attached.data_file.record_count)
            {
                continue;
            }
            let file = &attached.data_file;
            let Some(max_partial_order) = file.max_partial_order else {
                continue;
            };
            let row_id_end = file.row_id_start.saturating_add(file.record_count);
            if file.row_id_start <= row_id
                && row_id < row_id_end
                && file.validity.begin_order <= inline_begin_order
                && inline_begin_order <= max_partial_order
            {
                return true;
            }
        }
        false
    }
}

pub(super) fn materialized_visible_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<AttachedDataFile>> {
    if latest_snapshot(kv, catalog)?.is_some_and(|latest| latest.order == snapshot_order) {
        return list_current_data_files_with_deletes(kv, catalog, table_id);
    }
    list_data_files_with_deletes_at(kv, catalog, table_id, snapshot_order)
}

#[derive(Clone)]
pub(super) struct InlineReadRenderContext {
    snapshot_sequences: BTreeMap<CatalogOrderId, u64>,
    pub(super) materialized_files: InlineMaterializedFiles,
    pub(super) deleted_rows: InlineDeletionIndex,
}

#[cfg(not(test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct InlineReadRenderContextCacheKey {
    namespace: CatalogCacheNamespace,
    pub(super) catalog: CatalogId,
    table_id: crate::TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
}

#[cfg(not(test))]
pub(super) static INLINE_READ_RENDER_CONTEXT_CACHE: OnceLock<
    BoundedCache<InlineReadRenderContextCacheKey, InlineReadRenderContext>,
> = OnceLock::new();

#[cfg(not(test))]
pub(super) fn inline_read_render_context_cache()
-> &'static BoundedCache<InlineReadRenderContextCacheKey, InlineReadRenderContext> {
    static_bounded_cache(&INLINE_READ_RENDER_CONTEXT_CACHE, 512)
}

pub(super) fn load_inline_read_render_context(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: crate::TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<InlineReadRenderContext> {
    #[cfg(test)]
    {
        InlineReadRenderContext::load(kv, catalog, table_id, schema_id, snapshot_order)
    }
    #[cfg(not(test))]
    {
        let key = InlineReadRenderContextCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            table_id,
            schema_id,
            snapshot_order,
        };
        let cache = inline_read_render_context_cache();
        if let Some(context) = cache.get(key) {
            return Ok(context.clone());
        }
        let context =
            InlineReadRenderContext::load(kv, catalog, table_id, schema_id, snapshot_order)?;
        cache.insert(key, context.clone());
        Ok(context)
    }
}

impl InlineReadRenderContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: crate::TableId,
        schema_id: SchemaId,
        snapshot_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let snapshot_sequences = public_snapshot_sequences_by_order(kv, catalog)?;
        let materialized_files =
            InlineMaterializedFiles::load(kv, catalog, table_id, snapshot_order)?;
        let deleted_rows = InlineDeletionIndex::load(
            kv,
            catalog,
            table_id,
            schema_id,
            snapshot_order,
            &snapshot_sequences,
        )?;
        Ok(Self {
            snapshot_sequences,
            materialized_files,
            deleted_rows,
        })
    }

    pub(super) fn snapshot_sequence(&self, order: CatalogOrderId) -> Option<u64> {
        self.snapshot_sequences.get(&order).copied()
    }
}

pub(super) fn retain_unmaterialized_inline_payloads(
    materialized_files: &InlineMaterializedFiles,
    payloads: &mut Vec<InlineTablePayloadRow>,
) {
    if payloads.is_empty() {
        return;
    }
    payloads.retain(|payload| {
        !materialized_files
            .visible_files
            .iter()
            .any(|attached| data_file_covers_inline_begin(&attached.data_file, payload.begin_order))
    });
}

pub(super) fn change_feed_start_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedStartSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((start_order, _)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(start_order);
    }
    public_snapshot_order_span(kv, catalog, snapshot_id)?
        .map(|(start, _)| start)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

pub(super) fn change_feed_end_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: ChangeFeedEndSnapshot,
) -> CatalogResult<CatalogOrderId> {
    let snapshot_id = snapshot.public_id();
    if let Some((_, end_order)) = ducklake_snapshot_order_span(kv, catalog, snapshot_id)? {
        return Ok(end_order);
    }
    snapshot_by_public_sequence(kv, catalog, snapshot_id)?
        .map(|snapshot| snapshot.order)
        .ok_or_else(|| missing_snapshot(snapshot_id))
}

pub(super) fn ducklake_snapshot_order_span(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Option<(CatalogOrderId, CatalogOrderId)>> {
    cached_ducklake_snapshot_order_span(kv, catalog, snapshot_id)
}

pub(super) fn change_feed_snapshot_id_for_order(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<Option<u64>> {
    if let Some(snapshot) = list_snapshots(kv, catalog)?
        .into_iter()
        .find(|snapshot| snapshot.order == order)
    {
        return Ok(Some(snapshot.sequence.0));
    }
    Ok(public_snapshot_sequence_for_order(kv, catalog, order)?.map(|snapshot| snapshot.0))
}

pub(super) fn inline_row_fields(line: &str) -> CatalogResult<Vec<&str>> {
    let trimmed = line.trim_end_matches('\n');
    let fields = trimmed.split('\t').collect::<Vec<_>>();
    if fields.len() < 2 || fields[0] != "row" {
        return Err(crate::CatalogError::Decode(format!(
            "inline row payload has invalid shape: {line}"
        )));
    }
    Ok(fields)
}

pub(super) struct SnapshotAwareRowsInput<'a> {
    pub(super) begin_order: CatalogOrderId,
    pub(super) payload: &'a [u8],
    pub(super) include_deleted: bool,
    pub(super) stats_mode: InlineStatsMode,
    pub(super) render_context: &'a InlineReadRenderContext,
}

pub(super) fn append_snapshot_aware_rows(
    input: SnapshotAwareRowsInput<'_>,
    catalog_stats: &mut InlineCatalogStats,
    out: &mut String,
) -> CatalogResult<()> {
    let begin_snapshot = input
        .render_context
        .snapshot_sequence(input.begin_order)
        .ok_or_else(|| missing_snapshot_order(input.begin_order))?;
    let rows = std::str::from_utf8(input.payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        catalog_stats.observe_row_id(row_id);
        if matches!(input.stats_mode, InlineStatsMode::Conservative) {
            catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
        }
        if !input.include_deleted
            && input
                .render_context
                .deleted_rows
                .hides_row_version(row_id, begin_snapshot)
        {
            continue;
        }
        if input
            .render_context
            .materialized_files
            .materializes(row_id, input.begin_order)
        {
            continue;
        }
        if matches!(input.stats_mode, InlineStatsMode::ExactVisible) {
            catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
        }
        out.push_str(&format!(
            "row_change\t{}\t\t{}\t{}\n",
            begin_snapshot,
            fields[1],
            fields[2..].join("\t")
        ));
    }
    Ok(())
}
