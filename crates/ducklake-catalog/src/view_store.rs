#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
use crate::{
    CatalogError, CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, SnapshotRow, TableId, ValidityWindow, ViewRow,
    conflict_watermarks::stage_max_catalog_id_watermark,
    ids::{CatalogOrderId, CatalogOrderKind},
    keys::{prefix_end, view_object_key, view_object_prefix, view_object_scan_prefix},
    schema_version_state::stage_next_schema_version,
    store::{latest_snapshot, stage_snapshot},
};

#[cfg(not(test))]
static VIEW_ROWS_CACHE: OnceLock<BoundedCache<(CatalogId, CatalogOrderId), Vec<ViewRow>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewCommentChange {
    pub view_id: TableId,
    pub comment: Option<String>,
}

impl ViewCommentChange {
    #[must_use]
    pub fn new(view_id: TableId, comment: Option<impl Into<String>>) -> Self {
        Self {
            view_id,
            comment: comment.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedViewComment {
    pub view_id: TableId,
    pub previous: Option<String>,
    pub changed: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRename {
    pub view_id: TableId,
    pub new_name: String,
}

impl ViewRename {
    #[must_use]
    pub fn new(view_id: TableId, new_name: impl Into<String>) -> Self {
        Self {
            view_id,
            new_name: new_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenamedView {
    pub previous: ViewRow,
    pub renamed: ViewRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedView {
    pub view: ViewRow,
}

pub fn commit_create_view_row(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    mut view: ViewRow,
) -> CatalogResult<ViewRow> {
    let latest = latest_snapshot(kv, catalog)?;
    if let Some(snapshot) = &latest {
        reject_create_view_conflict(kv, catalog, &view, snapshot.order)?;
    }
    let order = kv.generated_order_id()?;
    let snapshot = crate::store::snapshot_row_for_next_sequence(latest, order);
    view.validity = ValidityWindow::new(order, None);
    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    batch.put(view_object_key(catalog, view.view_id, order), view.encode());
    stage_max_catalog_id_watermark(kv, &mut batch, catalog, view.view_id.0)?;
    kv.commit(batch)?;
    Ok(view)
}

fn reject_create_view_conflict(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    view: &ViewRow,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<()> {
    for current in list_views_at(kv, catalog, snapshot_order)? {
        if current.view_id == view.view_id {
            return Err(CatalogError::InvalidMutation(format!(
                "view id {} already exists",
                view.view_id.0
            )));
        }
        if current.schema_id == view.schema_id && current.name.eq_ignore_ascii_case(&view.name) {
            return Err(CatalogError::InvalidMutation(format!(
                "view name {} already exists in schema {}",
                view.name, view.schema_id.0
            )));
        }
    }
    Ok(())
}

pub fn commit_change_view_comment(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    change: &ViewCommentChange,
) -> CatalogResult<ChangedViewComment> {
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let mut previous = load_view_at(kv, catalog, change.view_id, latest.order)?
        .ok_or(CatalogError::NotFound("view"))?;
    let previous_comment = previous.comment.clone();
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, latest.sequence.next());
    previous.validity.end_order = Some(order);
    let mut next = previous.clone();
    next.comment = change.comment.clone();
    next.validity = ValidityWindow::new(order, None);

    let mut batch = KvBatch::new();
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;
    batch.put(
        view_object_key(catalog, change.view_id, previous.validity.begin_order),
        previous.encode(),
    );
    batch.put(
        view_object_key(catalog, change.view_id, order),
        next.encode(),
    );
    stage_max_catalog_id_watermark(kv, &mut batch, catalog, change.view_id.0)?;
    kv.commit(batch)?;
    Ok(ChangedViewComment {
        view_id: change.view_id,
        previous: previous_comment,
        changed: change.comment.clone(),
    })
}

pub fn commit_rename_views(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    renames: &[ViewRename],
) -> CatalogResult<Vec<RenamedView>> {
    if renames.is_empty() {
        return Ok(Vec::new());
    }
    reject_duplicate_renames(renames)?;

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let current_views = list_views_at(kv, catalog, latest.order)?;
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, latest.sequence.next());
    let mut batch = KvBatch::new();
    let mut renamed = Vec::with_capacity(renames.len());
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;

    for rename in renames {
        let mut previous = current_views
            .iter()
            .find(|view| view.view_id == rename.view_id)
            .cloned()
            .ok_or(CatalogError::NotFound("view"))?;
        reject_view_name_conflict(&current_views, &renamed, &previous, &rename.new_name)?;

        previous.validity.end_order = Some(order);
        let mut next = previous.clone();
        next.name = rename.new_name.clone();
        next.validity = ValidityWindow::new(order, None);

        batch.put(
            view_object_key(catalog, previous.view_id, previous.validity.begin_order),
            previous.encode(),
        );
        batch.put(view_object_key(catalog, next.view_id, order), next.encode());
        stage_max_catalog_id_watermark(kv, &mut batch, catalog, next.view_id.0)?;
        renamed.push(RenamedView {
            previous,
            renamed: next,
        });
    }

    kv.commit(batch)?;
    Ok(renamed)
}

pub fn commit_drop_views(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    view_ids: &[TableId],
) -> CatalogResult<Vec<DroppedView>> {
    if view_ids.is_empty() {
        return Ok(Vec::new());
    }
    reject_duplicate_view_ids(view_ids)?;

    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let order = kv.generated_order_id()?;
    let snapshot = SnapshotRow::new(order, latest.sequence.next());
    let mut batch = KvBatch::new();
    let mut dropped = Vec::with_capacity(view_ids.len());
    stage_snapshot(&mut batch, catalog, &snapshot);
    stage_next_schema_version(kv, &mut batch, catalog)?;

    for view_id in view_ids {
        let mut view = load_view_at(kv, catalog, *view_id, latest.order)?
            .ok_or(CatalogError::NotFound("view"))?;
        view.validity.end_order = Some(order);
        batch.put(
            view_object_key(catalog, view.view_id, view.validity.begin_order),
            view.encode(),
        );
        dropped.push(DroppedView { view });
    }

    kv.commit(batch)?;
    Ok(dropped)
}

pub fn list_views_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<ViewRow>> {
    let mut views = Vec::new();
    for item in kv.scan_prefix(
        &view_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_view_item(catalog, &item.key, &item.value)?;
        if row.validity.is_visible_at(snapshot_order) {
            views.push(row);
        }
    }
    Ok(views)
}

pub(crate) fn list_view_rows(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<ViewRow>> {
    #[cfg(test)]
    {
        return list_view_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let Some(latest) = latest_snapshot(kv, catalog)? else {
            return list_view_rows_uncached(kv, catalog);
        };
        let key = (catalog, latest.order);
        let cache = static_bounded_cache(&VIEW_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_view_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

pub(crate) fn list_view_rows_for_snapshot_cache(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<ViewRow>> {
    #[cfg(test)]
    {
        let _ = snapshot_order;
        return list_view_rows_uncached(kv, catalog);
    }
    #[cfg(not(test))]
    {
        let key = (catalog, snapshot_order);
        let cache = static_bounded_cache(&VIEW_ROWS_CACHE, 1024);
        if let Some(rows) = cache.get(key) {
            return Ok(rows);
        }
        let rows = list_view_rows_uncached(kv, catalog)?;
        cache.insert(key, rows.clone());
        Ok(rows)
    }
}

fn list_view_rows_uncached(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<ViewRow>> {
    kv.scan_prefix(
        &view_object_scan_prefix(catalog),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| decode_view_item(catalog, &item.key, &item.value))
    .collect()
}

pub fn load_view_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    view_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<ViewRow>> {
    let prefix = view_object_prefix(catalog, view_id);
    let end = prefix_end(&view_object_key(catalog, view_id, snapshot_order));
    let Some(item) = kv
        .scan_range(&prefix, &end, RangeDirection::Reverse, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let row = decode_view_item(catalog, &item.key, &item.value)?;
    if row.validity.is_visible_at(snapshot_order) {
        return Ok(Some(row));
    }
    Ok(None)
}

fn decode_view_item(catalog: CatalogId, key: &[u8], value: &[u8]) -> CatalogResult<ViewRow> {
    let mut row = ViewRow::decode(value)?;
    row.validity.begin_order =
        view_order_from_key(catalog, row.view_id, key, row.validity.begin_order)?;
    Ok(row)
}

fn view_order_from_key(
    catalog: CatalogId,
    view_id: TableId,
    key: &[u8],
    value_order: CatalogOrderId,
) -> CatalogResult<CatalogOrderId> {
    let prefix = view_object_prefix(catalog, view_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "view object key has wrong prefix".to_owned(),
        ));
    };
    let bytes: [u8; CatalogOrderId::LEN] = tail.try_into().map_err(|_| {
        CatalogError::InvalidKey(format!(
            "view object key order must be {} bytes, got {}",
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

fn reject_duplicate_renames(renames: &[ViewRename]) -> CatalogResult<()> {
    for (index, rename) in renames.iter().enumerate() {
        if renames[..index]
            .iter()
            .any(|prior| prior.view_id == rename.view_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "view {} is listed more than once for rename",
                rename.view_id.0
            )));
        }
    }
    Ok(())
}

fn reject_duplicate_view_ids(view_ids: &[TableId]) -> CatalogResult<()> {
    for (index, view_id) in view_ids.iter().enumerate() {
        if view_ids[..index].iter().any(|prior| prior == view_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "view {} is listed more than once for drop",
                view_id.0
            )));
        }
    }
    Ok(())
}

fn reject_view_name_conflict(
    current_views: &[ViewRow],
    renamed_views: &[RenamedView],
    previous: &ViewRow,
    new_name: &str,
) -> CatalogResult<()> {
    if current_views.iter().any(|view| {
        view.schema_id == previous.schema_id
            && view.view_id != previous.view_id
            && view.name.eq_ignore_ascii_case(new_name)
    }) || renamed_views.iter().any(|view| {
        view.renamed.schema_id == previous.schema_id
            && view.renamed.name.eq_ignore_ascii_case(new_name)
    }) {
        return Err(CatalogError::InvalidMutation(format!(
            "view name {new_name} already exists in schema {}",
            previous.schema_id.0
        )));
    }
    Ok(())
}
