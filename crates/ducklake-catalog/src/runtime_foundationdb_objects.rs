use crate::{
    CatalogId, CatalogResult, MacroId, MacroRow, TableId, ViewCommentChange, ViewRename, ViewRow,
};
#[cfg(feature = "foundationdb")]
use crate::{
    DuckLakeSnapshotId, latest_snapshot, list_tables_at, list_views_at,
    public_snapshot_sequence_for_order,
};

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_drop_tables(
    catalog: CatalogId,
    table_ids: Vec<TableId>,
    commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<crate::DroppedTable>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    kv.drop_tables_versionstamped_at(catalog, &table_ids, commit_snapshot)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_create_views(
    catalog: CatalogId,
    views: Vec<ViewRow>,
) -> CatalogResult<Vec<ViewRow>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    reject_current_view_create_conflicts(&kv, catalog, &views)?;
    let mut created = Vec::with_capacity(views.len());
    for view in views {
        created.push(kv.create_view_versionstamped(catalog, view)?);
    }
    Ok(created)
}

#[cfg(feature = "foundationdb")]
fn reject_current_view_create_conflicts(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    views: &[ViewRow],
) -> CatalogResult<()> {
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let current_views = list_views_at(kv, catalog, latest.order)?;
    let current_tables = list_tables_at(kv, catalog, latest.order)?;
    for view in views {
        if current_views
            .iter()
            .any(|existing| existing.view_id == view.view_id)
        {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating view {}: view id {} already exists",
                view.name, view.view_id.0
            )));
        }
        let name_exists = current_views.iter().any(|existing| {
            existing.schema_id == view.schema_id && existing.name.eq_ignore_ascii_case(&view.name)
        }) || current_tables.iter().any(|existing| {
            existing.schema_id == view.schema_id && existing.name.eq_ignore_ascii_case(&view.name)
        });
        if name_exists {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "conflict creating view {}: name already exists in schema {}",
                view.name, view.schema_id.0
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_rename_views(
    catalog: CatalogId,
    renames: Vec<ViewRename>,
) -> CatalogResult<Vec<crate::RenamedView>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    kv.rename_views_versionstamped(catalog, &renames)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_drop_views(
    catalog: CatalogId,
    view_ids: Vec<TableId>,
) -> CatalogResult<Vec<crate::DroppedView>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    kv.drop_views_versionstamped(catalog, &view_ids)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_change_view_comment(
    catalog: CatalogId,
    commit_snapshot: Option<DuckLakeSnapshotId>,
    change: ViewCommentChange,
) -> CatalogResult<crate::ChangedViewComment> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    reject_stale_object_commit(&kv, catalog, commit_snapshot, "change view comment")?;
    kv.change_view_comment_versionstamped(catalog, &change)
}

#[cfg(feature = "foundationdb")]
fn reject_stale_object_commit(
    kv: &impl crate::OrderedCatalogKv,
    catalog: CatalogId,
    commit_snapshot: Option<DuckLakeSnapshotId>,
    operation: &str,
) -> CatalogResult<()> {
    let Some(commit_snapshot) = commit_snapshot else {
        return Ok(());
    };
    let Some(latest) = latest_snapshot(kv, catalog)? else {
        return Ok(());
    };
    let latest_public = public_snapshot_sequence_for_order(kv, catalog, latest.order)?
        .map_or(DuckLakeSnapshotId(latest.sequence.0), |sequence| sequence);
    if latest_public.0 > commit_snapshot.0 {
        return Err(crate::CatalogError::InvalidMutation(format!(
            "conflict during {operation}: proposed snapshot {} is stale after latest snapshot {}",
            commit_snapshot.0, latest_public.0
        )));
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_create_macros(
    catalog: CatalogId,
    macros: Vec<MacroRow>,
    commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<MacroRow>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    kv.create_macros_versionstamped(catalog, macros, commit_snapshot)
}

#[cfg(feature = "foundationdb")]
pub(crate) fn runtime_foundationdb_drop_macros(
    catalog: CatalogId,
    macro_ids: Vec<MacroId>,
    commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<crate::DroppedMacro>> {
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog()?;
    kv.drop_macros_versionstamped(catalog, &macro_ids, commit_snapshot)
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_drop_tables(
    _catalog: CatalogId,
    _table_ids: Vec<TableId>,
    _commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<crate::DroppedTable>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_create_views(
    _catalog: CatalogId,
    _views: Vec<ViewRow>,
) -> CatalogResult<Vec<ViewRow>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_rename_views(
    _catalog: CatalogId,
    _renames: Vec<ViewRename>,
) -> CatalogResult<Vec<crate::RenamedView>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_drop_views(
    _catalog: CatalogId,
    _view_ids: Vec<TableId>,
) -> CatalogResult<Vec<crate::DroppedView>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_change_view_comment(
    _catalog: CatalogId,
    _commit_snapshot: Option<crate::DuckLakeSnapshotId>,
    _change: ViewCommentChange,
) -> CatalogResult<crate::ChangedViewComment> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_create_macros(
    _catalog: CatalogId,
    _macros: Vec<MacroRow>,
    _commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<MacroRow>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
pub(crate) fn runtime_foundationdb_drop_macros(
    _catalog: CatalogId,
    _macro_ids: Vec<MacroId>,
    _commit_snapshot: Option<crate::RawSnapshotSequence>,
) -> CatalogResult<Vec<crate::DroppedMacro>> {
    foundationdb_runtime_error()
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_error<T>() -> CatalogResult<T> {
    Err(crate::CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

#[cfg(all(test, feature = "foundationdb"))]
#[cfg(test)]
#[path = "runtime_foundationdb_objects_tests.rs"]
mod runtime_foundationdb_objects_tests;
