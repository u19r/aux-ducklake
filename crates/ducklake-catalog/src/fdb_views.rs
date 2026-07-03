use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use foundationdb::options::MutationType;
use futures::executor::block_on;

use crate::{
    CatalogError, CatalogId, CatalogResult, FdbOrderedCatalogKv, RawSnapshotSequence, RenamedView,
    SnapshotRow, TableId, ValidityWindow, ViewCommentChange, ViewRename, ViewRow,
    conflict_watermarks::stage_fdb_max_catalog_id_watermark,
    fdb_runtime::{map_fdb_commit_error, map_fdb_error},
    fdb_versionstamp::{
        committed_order, incomplete_order, snapshot_key_order_offset,
        snapshot_timestamp_key_order_offset, versionstamped_value,
    },
    keys::{snapshot_key, snapshot_timestamp_key, view_object_key, view_object_prefix},
    schema_version_state::stage_fdb_next_schema_version,
    store::{latest_snapshot, stage_fdb_latest_snapshot_value},
    table_store::list_tables_at,
    view_store::{list_views_at, load_view_at},
};

impl FdbOrderedCatalogKv {
    pub fn create_view_versionstamped(
        &self,
        catalog: CatalogId,
        mut view: ViewRow,
    ) -> CatalogResult<ViewRow> {
        let latest = latest_snapshot(self, catalog)?;
        let next_sequence = latest.map_or(RawSnapshotSequence::initial(), |snapshot| {
            snapshot.sequence.next()
        });
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, next_sequence);
        view.validity = ValidityWindow::new(placeholder, None);

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        trx.atomic_op(
            &self.versionstamped_key(
                &view_object_key(catalog, view.view_id, placeholder),
                view_object_key_order_offset(catalog, view.view_id),
            )?,
            &view.encode(),
            MutationType::SetVersionstampedKey,
        );
        stage_fdb_max_catalog_id_watermark(self, &trx, catalog, view.view_id.0);
        let versionstamp = trx.get_versionstamp();
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        view.validity = ValidityWindow::new(
            committed_order(block_on(versionstamp).map_err(map_fdb_error)?.deref())?,
            None,
        );
        Ok(view)
    }

    pub fn change_view_comment_versionstamped(
        &self,
        catalog: CatalogId,
        change: &ViewCommentChange,
    ) -> CatalogResult<crate::ChangedViewComment> {
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let previous = load_view_at(self, catalog, change.view_id, latest.order)?
            .ok_or(CatalogError::NotFound("view"))?;
        let mut next = previous.clone();
        next.comment = change.comment.clone();
        self.commit_view_replacements(
            catalog,
            latest.sequence,
            vec![(change.view_id, previous.clone(), Some(next))],
        )?;
        Ok(crate::ChangedViewComment {
            view_id: change.view_id,
            previous: previous.comment,
            changed: change.comment.clone(),
        })
    }

    pub fn rename_views_versionstamped(
        &self,
        catalog: CatalogId,
        renames: &[ViewRename],
    ) -> CatalogResult<Vec<RenamedView>> {
        if renames.is_empty() {
            return Ok(Vec::new());
        }
        reject_duplicate_renames(renames)?;
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let current_views = list_views_at(self, catalog, latest.order)?;
        let mut renamed = Vec::with_capacity(renames.len());
        let mut replacements = Vec::with_capacity(renames.len());
        for rename in renames {
            let previous = current_views
                .iter()
                .find(|view| view.view_id == rename.view_id)
                .cloned()
                .ok_or(CatalogError::NotFound("view"))?;
            reject_view_name_conflict(
                &current_views,
                &renamed,
                &[],
                &BTreeSet::new(),
                &previous,
                &rename.new_name,
            )?;
            let mut next = previous.clone();
            next.name = rename.new_name.clone();
            replacements.push((rename.view_id, previous.clone(), Some(next.clone())));
            renamed.push(RenamedView {
                previous,
                renamed: next,
            });
        }
        self.commit_view_replacements(catalog, latest.sequence, replacements)?;
        Ok(renamed)
    }

    pub fn drop_views_versionstamped(
        &self,
        catalog: CatalogId,
        view_ids: &[TableId],
    ) -> CatalogResult<Vec<crate::DroppedView>> {
        if view_ids.is_empty() {
            return Ok(Vec::new());
        }
        reject_duplicate_view_ids(view_ids)?;
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let mut dropped = Vec::with_capacity(view_ids.len());
        let mut replacements = Vec::with_capacity(view_ids.len());
        for view_id in view_ids {
            let previous = load_view_at(self, catalog, *view_id, latest.order)?
                .ok_or(CatalogError::NotFound("view"))?;
            replacements.push((*view_id, previous.clone(), None));
            dropped.push(crate::DroppedView { view: previous });
        }
        self.commit_view_replacements(catalog, latest.sequence, replacements)?;
        Ok(dropped)
    }

    pub(crate) fn change_views_versionstamped_at(
        &self,
        catalog: CatalogId,
        mut created: Vec<ViewRow>,
        renames: Vec<ViewRename>,
        dropped_ids: &[TableId],
        comment_changes: Vec<ViewCommentChange>,
        commit_raw_snapshot: RawSnapshotSequence,
    ) -> CatalogResult<()> {
        if created.is_empty()
            && renames.is_empty()
            && dropped_ids.is_empty()
            && comment_changes.is_empty()
        {
            return Ok(());
        }
        reject_duplicate_view_ids(dropped_ids)?;
        reject_duplicate_renames(&renames)?;
        reject_duplicate_created_views(&created)?;
        let latest =
            latest_snapshot(self, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        let current_views = list_views_at(self, catalog, latest.order)?;
        let current_tables = list_tables_at(self, catalog, latest.order)?;
        let current_view_ids = current_views
            .iter()
            .map(|view| view.view_id)
            .collect::<BTreeSet<_>>();
        let dropped_id_set = dropped_ids.iter().copied().collect::<BTreeSet<_>>();

        let mut current_renames = Vec::new();
        for rename in renames {
            if let Some(view) = created
                .iter_mut()
                .find(|view| view.view_id == rename.view_id)
            {
                view.name = rename.new_name;
            } else {
                current_renames.push(rename);
            }
        }
        let mut current_comment_changes = Vec::new();
        for change in comment_changes {
            if let Some(view) = created
                .iter_mut()
                .find(|view| view.view_id == change.view_id)
            {
                view.comment = change.comment;
            } else if !dropped_id_set.contains(&change.view_id) {
                current_comment_changes.push(change);
            }
        }
        created.retain(|view| {
            !dropped_id_set.contains(&view.view_id) || current_view_ids.contains(&view.view_id)
        });
        reject_duplicate_created_views(&created)?;
        reject_created_view_conflicts(&current_views, &current_tables, &created, &dropped_id_set)?;

        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, commit_raw_snapshot);
        let created_view_ids = created
            .iter()
            .map(|view| view.view_id)
            .collect::<BTreeSet<_>>();
        let mut replacements: BTreeMap<TableId, (ViewRow, Option<ViewRow>)> = BTreeMap::new();
        for view_id in dropped_ids {
            if created_view_ids.contains(view_id) && !current_view_ids.contains(view_id) {
                continue;
            }
            let previous = current_views
                .iter()
                .find(|view| view.view_id == *view_id)
                .cloned()
                .ok_or(CatalogError::NotFound("view"))?;
            replacements.insert(*view_id, (previous, None));
        }
        let mut renamed = Vec::new();
        for rename in current_renames {
            let previous = current_views
                .iter()
                .find(|view| view.view_id == rename.view_id)
                .cloned()
                .ok_or(CatalogError::NotFound("view"))?;
            if dropped_id_set.contains(&previous.view_id) {
                return Err(CatalogError::InvalidMutation(format!(
                    "view {} is both dropped and renamed",
                    previous.view_id.0
                )));
            }
            reject_view_name_conflict(
                &current_views,
                &renamed,
                &created,
                &dropped_id_set,
                &previous,
                &rename.new_name,
            )?;
            let mut next = previous.clone();
            next.name = rename.new_name;
            renamed.push(RenamedView {
                previous: previous.clone(),
                renamed: next.clone(),
            });
            replacements.insert(previous.view_id, (previous, Some(next)));
        }
        for change in current_comment_changes {
            let Some(previous) = current_views
                .iter()
                .find(|view| view.view_id == change.view_id)
                .cloned()
            else {
                return Err(CatalogError::NotFound("view"));
            };
            let (_, next) = replacements
                .entry(change.view_id)
                .or_insert_with(|| (previous.clone(), Some(previous)));
            let Some(next) = next else {
                return Err(CatalogError::NotFound("view"));
            };
            next.comment = change.comment;
        }

        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for (view_id, (mut previous, next)) in replacements {
            previous.validity.end_order = Some(placeholder);
            trx.atomic_op(
                &self.namespaced_key(&view_object_key(
                    catalog,
                    view_id,
                    previous.validity.begin_order,
                )),
                &versionstamped_value(&previous.encode(), ViewRow::END_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            if let Some(mut next) = next {
                next.validity = ValidityWindow::new(placeholder, None);
                trx.atomic_op(
                    &self.versionstamped_key(
                        &view_object_key(catalog, view_id, placeholder),
                        view_object_key_order_offset(catalog, view_id),
                    )?,
                    &versionstamped_value(&next.encode(), ViewRow::BEGIN_ORDER_BYTES_OFFSET)?,
                    MutationType::SetVersionstampedKey,
                );
                stage_fdb_max_catalog_id_watermark(self, &trx, catalog, view_id.0);
            }
        }
        for view in &mut created {
            view.validity = ValidityWindow::new(placeholder, None);
            trx.atomic_op(
                &self.versionstamped_key(
                    &view_object_key(catalog, view.view_id, placeholder),
                    view_object_key_order_offset(catalog, view.view_id),
                )?,
                &versionstamped_value(&view.encode(), ViewRow::BEGIN_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedKey,
            );
            stage_fdb_max_catalog_id_watermark(self, &trx, catalog, view.view_id.0);
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }

    fn commit_view_replacements(
        &self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<(TableId, ViewRow, Option<ViewRow>)>,
    ) -> CatalogResult<()> {
        let placeholder = incomplete_order();
        let snapshot = SnapshotRow::new(placeholder, previous_sequence.next());
        let trx = self.create_transaction()?;
        stage_snapshot(self, &trx, catalog, &snapshot)?;
        stage_fdb_next_schema_version(self, &trx, catalog)?;
        for (view_id, mut previous, next) in replacements {
            previous.validity.end_order = Some(placeholder);
            trx.atomic_op(
                &self.namespaced_key(&view_object_key(
                    catalog,
                    view_id,
                    previous.validity.begin_order,
                )),
                &versionstamped_value(&previous.encode(), ViewRow::END_ORDER_BYTES_OFFSET)?,
                MutationType::SetVersionstampedValue,
            );
            if let Some(mut next) = next {
                next.validity = ValidityWindow::new(placeholder, None);
                trx.atomic_op(
                    &self.versionstamped_key(
                        &view_object_key(catalog, view_id, placeholder),
                        view_object_key_order_offset(catalog, view_id),
                    )?,
                    &next.encode(),
                    MutationType::SetVersionstampedKey,
                );
                stage_fdb_max_catalog_id_watermark(self, &trx, catalog, view_id.0);
            }
        }
        block_on(trx.commit()).map_err(map_fdb_commit_error)?;
        Ok(())
    }
}

fn stage_snapshot(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: CatalogId,
    snapshot: &SnapshotRow,
) -> CatalogResult<()> {
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_key(catalog, snapshot.order),
            snapshot_key_order_offset(catalog),
        )?,
        &snapshot.encode(),
        MutationType::SetVersionstampedKey,
    );
    trx.atomic_op(
        &kv.versionstamped_key(
            &snapshot_timestamp_key(catalog, snapshot.created_at_micros, snapshot.order),
            snapshot_timestamp_key_order_offset(catalog, snapshot.created_at_micros),
        )?,
        &snapshot.sequence.to_be_bytes(),
        MutationType::SetVersionstampedKey,
    );
    stage_fdb_latest_snapshot_value(kv, trx, catalog, snapshot)?;
    Ok(())
}

fn view_object_key_order_offset(catalog: CatalogId, view_id: TableId) -> usize {
    view_object_prefix(catalog, view_id).len()
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

fn reject_duplicate_created_views(views: &[ViewRow]) -> CatalogResult<()> {
    for (index, view) in views.iter().enumerate() {
        if views[..index]
            .iter()
            .any(|prior| prior.view_id == view.view_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "view {} is listed more than once for create",
                view.view_id.0
            )));
        }
        if views[..index].iter().any(|prior| {
            prior.schema_id == view.schema_id && prior.name.eq_ignore_ascii_case(&view.name)
        }) {
            return Err(CatalogError::InvalidMutation(format!(
                "view name {} is listed more than once for create in schema {}",
                view.name, view.schema_id.0
            )));
        }
    }
    Ok(())
}

fn reject_created_view_conflicts(
    current_views: &[ViewRow],
    current_tables: &[crate::TableRow],
    created: &[ViewRow],
    dropped_ids: &std::collections::BTreeSet<TableId>,
) -> CatalogResult<()> {
    for view in created {
        if current_views.iter().any(|existing| {
            existing.view_id == view.view_id && !dropped_ids.contains(&existing.view_id)
        }) {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating view {}: view id {} already exists",
                view.name, view.view_id.0
            )));
        }
        let view_name_exists = current_views.iter().any(|existing| {
            existing.schema_id == view.schema_id
                && existing.name.eq_ignore_ascii_case(&view.name)
                && !dropped_ids.contains(&existing.view_id)
        });
        let table_name_exists = current_tables.iter().any(|existing| {
            existing.schema_id == view.schema_id && existing.name.eq_ignore_ascii_case(&view.name)
        });
        if view_name_exists || table_name_exists {
            return Err(CatalogError::InvalidMutation(format!(
                "conflict creating view {}: name already exists in schema {}",
                view.name, view.schema_id.0
            )));
        }
    }
    Ok(())
}

fn reject_view_name_conflict(
    current_views: &[ViewRow],
    renamed_views: &[RenamedView],
    created_views: &[ViewRow],
    dropped_ids: &BTreeSet<TableId>,
    previous: &ViewRow,
    new_name: &str,
) -> CatalogResult<()> {
    if current_views.iter().any(|view| {
        view.schema_id == previous.schema_id
            && view.view_id != previous.view_id
            && !dropped_ids.contains(&view.view_id)
            && view.name.eq_ignore_ascii_case(new_name)
    }) || renamed_views.iter().any(|view| {
        view.renamed.schema_id == previous.schema_id
            && view.renamed.name.eq_ignore_ascii_case(new_name)
    }) || created_views.iter().any(|view| {
        view.schema_id == previous.schema_id && view.name.eq_ignore_ascii_case(new_name)
    }) {
        return Err(CatalogError::InvalidMutation(format!(
            "view name {new_name} already exists in schema {}",
            previous.schema_id.0
        )));
    }
    Ok(())
}
