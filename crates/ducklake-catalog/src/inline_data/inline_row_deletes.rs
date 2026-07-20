use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId, KvBatch,
    MutableCatalogKv, OrderedCatalogKv, RangeDirection, SchemaId, TableId,
    inline_change_feed::{InlineRowChangeKind, stage_inline_row_change},
    inline_data::{
        InlineTableChunkRow, assemble_inline_payload, decode_inline_table_item,
        inline_chunk_visible_at, inline_table_end_orders_at, inline_table_prefix,
    },
    list_inline_row_changes, public_snapshot_sequence_for_order, snapshot_by_ducklake_sequence,
    store::{
        invalidate_runtime_read_context, latest_snapshot, snapshot_row_for_next_sequence,
        stage_snapshot,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineTableDeleteCommit {
    pub deleted_row_count: usize,
    pub rewritten_payload_count: usize,
}

pub fn commit_delete_inline_table_rows(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    deleted_row_ids: &[u64],
) -> CatalogResult<InlineTableDeleteCommit> {
    commit_delete_inline_table_rows_at_snapshot(
        kv,
        catalog,
        table_id,
        schema_id,
        deleted_row_ids,
        None,
    )
}

pub fn commit_delete_inline_table_rows_at_snapshot(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    deleted_row_ids: &[u64],
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<InlineTableDeleteCommit> {
    let deleted = deleted_row_ids.iter().copied().collect::<BTreeSet<_>>();
    if deleted.is_empty() {
        return Ok(InlineTableDeleteCommit {
            deleted_row_count: 0,
            rewritten_payload_count: 0,
        });
    }

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("snapshot"))?;
    let target = inline_delete_target(kv, catalog, latest.clone(), commit_snapshot)?;
    let order = target.snapshot.order;
    let visible_payloads =
        visible_inline_payload_chunks(kv, catalog, table_id, schema_id, latest.order)?;
    let existing_deletions =
        ExistingInlineDeletions::load(kv, catalog, table_id, schema_id, latest.order)?;
    let mut batch = KvBatch::new();
    let mut deleted_row_count = 0;
    let rewritten_payload_count = 0;
    let mut hidden_versions_by_row_id = BTreeMap::<u64, usize>::new();

    for (begin_order, chunks) in visible_payloads {
        let begin_snapshot = public_snapshot_sequence_for_order(kv, catalog, begin_order)?
            .ok_or(CatalogError::NotFound("inline payload snapshot"))?
            .0;
        if begin_snapshot >= target.snapshot.sequence.0 {
            continue;
        }
        let payload = assemble_inline_payload(chunks.clone())?;
        let filtered = filter_deleted_rows(&payload, &deleted)?;
        if filtered.deleted_row_ids.is_empty() {
            continue;
        }
        for row_id in &filtered.deleted_row_ids {
            if existing_deletions.hides_row_version_before(
                *row_id,
                begin_snapshot,
                target.snapshot.sequence.0,
            ) {
                continue;
            }
            let hidden_versions = hidden_versions_by_row_id.entry(*row_id).or_default();
            *hidden_versions += 1;
            if *hidden_versions > 1 {
                return Err(CatalogError::InvalidMutation(format!(
                    "inline row id {row_id} matches multiple live payload versions for table {} schema {}",
                    table_id.0, schema_id.0
                )));
            }
            stage_inline_row_change(
                &mut batch,
                catalog,
                table_id,
                schema_id,
                order,
                InlineRowChangeKind::Deleted,
                *row_id,
            );
            deleted_row_count += 1;
        }
    }

    if deleted_row_count == 0 {
        return Ok(InlineTableDeleteCommit {
            deleted_row_count: 0,
            rewritten_payload_count: 0,
        });
    }

    if target.stage_snapshot {
        stage_snapshot(&mut batch, catalog, &target.snapshot);
    }
    kv.commit(batch)?;
    invalidate_runtime_read_context(catalog);
    Ok(InlineTableDeleteCommit {
        deleted_row_count,
        rewritten_payload_count,
    })
}

struct InlineDeleteTarget {
    snapshot: crate::SnapshotRow,
    stage_snapshot: bool,
}

fn inline_delete_target(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    latest: crate::SnapshotRow,
    commit_snapshot: Option<DuckLakeSnapshotId>,
) -> CatalogResult<InlineDeleteTarget> {
    if let Some(commit_snapshot) = commit_snapshot
        && let Some(snapshot) = snapshot_by_ducklake_sequence(kv, catalog, commit_snapshot)?
    {
        return Ok(InlineDeleteTarget {
            snapshot,
            stage_snapshot: false,
        });
    }
    let order = kv.generated_order_id()?;
    Ok(InlineDeleteTarget {
        snapshot: snapshot_row_for_next_sequence(Some(latest), order),
        stage_snapshot: true,
    })
}

struct ExistingInlineDeletions {
    by_row_id: BTreeMap<u64, BTreeSet<u64>>,
}

impl ExistingInlineDeletions {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        latest_order: CatalogOrderId,
    ) -> CatalogResult<Self> {
        let start_order = CatalogOrderId::from_bytes(latest_order.kind(), [0; CatalogOrderId::LEN]);
        let mut by_row_id = BTreeMap::<u64, BTreeSet<u64>>::new();
        for change in list_inline_row_changes(kv, catalog, table_id, start_order, latest_order)? {
            if change.schema_id != schema_id || change.kind != InlineRowChangeKind::Deleted {
                continue;
            }
            let Some(delete_snapshot) =
                public_snapshot_sequence_for_order(kv, catalog, change.order)?
            else {
                continue;
            };
            by_row_id
                .entry(change.row_id)
                .or_default()
                .insert(delete_snapshot.0);
        }
        Ok(Self { by_row_id })
    }

    fn hides_row_version_before(
        &self,
        row_id: u64,
        begin_snapshot: u64,
        before_snapshot: u64,
    ) -> bool {
        self.by_row_id.get(&row_id).is_some_and(|delete_snapshots| {
            delete_snapshots
                .range((begin_snapshot + 1)..before_snapshot)
                .next()
                .is_some()
        })
    }
}

pub(crate) fn visible_inline_payload_chunks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<BTreeMap<CatalogOrderId, Vec<InlineTableChunkRow>>> {
    let end_orders = inline_table_end_orders_at(kv, catalog, table_id, schema_id, snapshot_order)?;
    let mut visible = BTreeMap::<CatalogOrderId, Vec<InlineTableChunkRow>>::new();
    for item in kv.scan_prefix(
        &inline_table_prefix(catalog, table_id, schema_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        if inline_chunk_visible_at(&row, &end_orders, snapshot_order) {
            visible
                .entry(row.validity.begin_order)
                .or_default()
                .push(row);
        }
    }
    Ok(visible)
}

pub(crate) struct FilteredInlineRows {
    pub(crate) deleted_row_ids: Vec<u64>,
}

pub(crate) fn filter_deleted_rows(
    payload: &[u8],
    deleted: &BTreeSet<u64>,
) -> CatalogResult<FilteredInlineRows> {
    let text = std::str::from_utf8(payload)
        .map_err(|error| CatalogError::Decode(format!("inline payload is not utf-8: {error}")))?;
    let mut deleted_row_ids = Vec::new();
    for line in text.lines() {
        let row_id = inline_row_id(line)?;
        if deleted.contains(&row_id) {
            deleted_row_ids.push(row_id);
        }
    }
    Ok(FilteredInlineRows { deleted_row_ids })
}

fn inline_row_id(line: &str) -> CatalogResult<u64> {
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["row", row_id, ..] => row_id.parse::<u64>().map_err(|error| {
            CatalogError::Decode(format!("inline row id {row_id} is invalid: {error}"))
        }),
        _ => Err(CatalogError::Decode(format!(
            "inline payload row has invalid shape: {line}"
        ))),
    }
}
