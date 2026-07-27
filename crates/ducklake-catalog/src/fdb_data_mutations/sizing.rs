use std::collections::{BTreeMap, BTreeSet};

use foundationdb::options::MutationType;

use crate::{
    CatalogOrderId, CatalogResult, DataFileId, DataFileRow, DeleteFileRow, FdbOrderedCatalogKv,
    InlineFileDeletionRow, SnapshotRow, TableId, ValidityWindow,
    data_mutation_intents::DeleteFileMaterialization,
    fdb_versionstamp::{incomplete_order, versionstamped_value},
    inline_data::list_inline_file_deletion_rows_for_data_files_at,
    keys::inline_file_deletion_key,
    kv::OrderedCatalogKv,
};

use crate::fdb_data_mutations::*;

pub(super) fn prepare_delete_file_for_versionstamped_commit(
    row: &mut DeleteFileRow,
    placeholder: CatalogOrderId,
) {
    if row.validity.begin_order == CatalogOrderId::uuid_v7(0) {
        row.validity = ValidityWindow::new(placeholder, None);
        return;
    }
    row.validity.end_order = None;
}

pub(super) fn complete_materialized_delete_file_visibility(
    delete_files: &mut [DeleteFileMaterialization],
    inline_file_deletions: &[InlineFileDeletionRow],
) {
    let mut visibility_by_data_file =
        BTreeMap::<DataFileId, (CatalogOrderId, CatalogOrderId)>::new();
    for row in inline_file_deletions {
        visibility_by_data_file
            .entry(row.data_file_id)
            .and_modify(|(min_order, max_order)| {
                *min_order = (*min_order).min(row.validity.begin_order);
                *max_order = (*max_order).max(row.validity.begin_order);
            })
            .or_insert((row.validity.begin_order, row.validity.begin_order));
    }
    for materialization in delete_files {
        if !materialization.materializes_inline_deletes() {
            continue;
        }
        let Some((begin_order, max_partial_order)) = visibility_by_data_file
            .get(&materialization.data_file_id())
            .copied()
        else {
            continue;
        };
        let row = materialization.row_mut();
        if row.validity.begin_order == incomplete_order() {
            row.validity.begin_order = begin_order;
        }
        if row.max_partial_order.is_none() || row.max_partial_order == Some(incomplete_order()) {
            row.max_partial_order = Some(max_partial_order);
        }
    }
}

pub(super) fn materialized_inline_file_deletions(
    kv: &impl OrderedCatalogKv,
    catalog: crate::CatalogId,
    data_file_context: &MutationDataFileContext,
    delete_files: &[DeleteFileMaterialization],
    latest: Option<&SnapshotRow>,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let Some(latest) = latest else {
        return Ok(Vec::new());
    };
    let mut materialized_data_files_by_table = BTreeMap::<TableId, BTreeSet<DataFileId>>::new();
    for materialization in delete_files {
        if !materialization.materializes_inline_deletes() {
            continue;
        }
        let data_file = data_file_context.get(materialization.data_file_id())?;
        materialized_data_files_by_table
            .entry(data_file.table_id)
            .or_default()
            .insert(data_file.data_file_id);
    }
    let mut rows = Vec::new();
    for (table_id, data_file_ids) in materialized_data_files_by_table {
        rows.extend(list_inline_file_deletion_rows_for_data_files_at(
            kv,
            catalog,
            table_id,
            latest.order,
            &data_file_ids,
        )?);
    }
    Ok(rows)
}

pub(super) fn stage_materialized_inline_file_deletion(
    kv: &FdbOrderedCatalogKv,
    trx: &foundationdb::Transaction,
    catalog: crate::CatalogId,
    row: &InlineFileDeletionRow,
) -> CatalogResult<()> {
    let mut ended = row.clone();
    ended.validity.end_order = Some(incomplete_order());
    trx.atomic_op(
        &kv.namespaced_key(&inline_file_deletion_key(
            catalog,
            ended.table_id,
            ended.data_file_id,
            ended.validity.begin_order,
            ended.row_id,
        )),
        &versionstamped_value(
            &ended.encode(),
            InlineFileDeletionRow::END_ORDER_BYTES_OFFSET,
        )?,
        MutationType::SetVersionstampedValue,
    );
    Ok(())
}

pub(super) fn delete_file_timeline_order_for_commit(
    proposed_data_file_ids: &ProposedDataFileTimelineLookup<'_>,
    row: &DeleteFileRow,
    placeholder: CatalogOrderId,
) -> CatalogOrderId {
    if proposed_data_file_ids.contains(&row.data_file_id) {
        return row.validity.begin_order;
    }
    placeholder
}

pub(super) enum ProposedDataFileTimelineLookup<'a> {
    Scan(&'a [DataFileRow]),
    Set(BTreeSet<DataFileId>),
}

impl ProposedDataFileTimelineLookup<'_> {
    pub(super) fn contains(&self, data_file_id: &DataFileId) -> bool {
        match self {
            Self::Scan(data_files) => data_files
                .iter()
                .any(|row| row.data_file_id == *data_file_id),
            Self::Set(data_file_ids) => data_file_ids.contains(data_file_id),
        }
    }

    #[cfg(test)]
    pub(super) fn uses_set(&self) -> bool {
        matches!(self, Self::Set(_))
    }
}

pub(super) fn proposed_data_file_ids_for_delete_timeline<'a>(
    data_files: &'a [DataFileRow],
    delete_files: &[DeleteFileMaterialization],
) -> ProposedDataFileTimelineLookup<'a> {
    if delete_files.len() <= 1 || data_files.len() <= 4 {
        return ProposedDataFileTimelineLookup::Scan(data_files);
    }
    ProposedDataFileTimelineLookup::Set(data_files.iter().map(|row| row.data_file_id).collect())
}
