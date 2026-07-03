use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataCommitIntent,
    DataFileChange, DataFileChangeKind, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FdbOrderedCatalogKv, FileColumnStatsRow, InlineFileDeletionRow, MergeAdjacentCompaction,
    RewriteDeleteCompaction, TableId,
    compaction_store::{merge_adjacent_file_column_stats, rewrite_delete_file_column_stats},
    conflict::reject_conflicts_since_base,
    fdb_data_mutations::FdbExpiredDeleteFile,
    file_partitions::list_file_partition_values_for_data_files,
    file_stats::list_file_column_stats_for_data_file_ids,
    file_visibility::current_delete_file_ids,
    inline_data::{inline_file_deletion_begin_order, list_inline_file_deletions_for_data_files_at},
    keys::{
        data_file_key, delete_file_key, delete_file_timeline_order_from_key,
        delete_file_timeline_scan_end, inline_file_deletion_file_prefix,
    },
    kv::OrderedCatalogKv,
    store::latest_snapshot,
};

impl FdbOrderedCatalogKv {
    pub fn commit_merge_adjacent_data_files_versionstamped(
        &self,
        catalog: CatalogId,
        compaction: MergeAdjacentCompaction,
    ) -> CatalogResult<MergeAdjacentCompaction> {
        self.commit_merge_adjacent_data_files_versionstamped_with_metadata(
            catalog,
            None,
            crate::SnapshotCommitMetadata::default(),
            compaction,
        )
    }

    pub(crate) fn commit_merge_adjacent_data_files_versionstamped_with_metadata(
        &self,
        catalog: CatalogId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        compaction: MergeAdjacentCompaction,
    ) -> CatalogResult<MergeAdjacentCompaction> {
        reject_merge_shape(&compaction)?;
        reject_source_delete_files(self, catalog, &compaction.source_file_ids)?;
        let source_context = MergeSourceContext::load(self, catalog, &compaction)?;
        self.commit_merge_adjacent_data_files_versionstamped_with_source_context(
            catalog,
            attempt_id,
            commit_metadata,
            compaction,
            source_context,
        )
    }

    fn commit_merge_adjacent_data_files_versionstamped_with_source_context(
        &self,
        catalog: CatalogId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        mut compaction: MergeAdjacentCompaction,
        source_context: MergeSourceContext,
    ) -> CatalogResult<MergeAdjacentCompaction> {
        normalize_merge_replacements(&source_context, &mut compaction)?;
        let source_file_ids = compaction.source_file_ids.clone();
        let file_column_stats = if compaction.file_column_stats.is_empty() {
            derive_merge_replacement_stats(
                self,
                catalog,
                &source_context,
                &compaction.new_files,
                &compaction.partition_values,
            )?
        } else {
            merge_adjacent_file_column_stats(
                self,
                catalog,
                &source_file_ids,
                &compaction.new_files,
                &compaction.file_column_stats,
            )?
        };
        let commit = self.commit_compaction_data_mutation_versionstamped(
            catalog,
            attempt_id,
            commit_metadata,
            compaction.new_files,
            compaction.partition_values.clone(),
            file_column_stats,
            source_context.sources().to_vec(),
        )?;
        compaction.new_files = commit.data_files;
        Ok(compaction)
    }

    pub fn commit_merge_adjacent_data_files_versionstamped_with_conflict_check(
        &self,
        catalog: CatalogId,
        base_order: CatalogOrderId,
        through_order: CatalogOrderId,
        compaction: MergeAdjacentCompaction,
    ) -> CatalogResult<MergeAdjacentCompaction> {
        self.commit_merge_adjacent_data_files_versionstamped_with_conflict_check_and_metadata(
            catalog,
            base_order,
            through_order,
            None,
            crate::SnapshotCommitMetadata::default(),
            compaction,
        )
    }

    pub(crate) fn commit_merge_adjacent_data_files_versionstamped_with_conflict_check_and_metadata(
        &self,
        catalog: CatalogId,
        base_order: CatalogOrderId,
        through_order: CatalogOrderId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        compaction: MergeAdjacentCompaction,
    ) -> CatalogResult<MergeAdjacentCompaction> {
        reject_merge_shape(&compaction)?;
        reject_source_delete_files(self, catalog, &compaction.source_file_ids)?;
        let source_context = MergeSourceContext::load(self, catalog, &compaction)?;
        reject_conflicts_since_base(
            self,
            catalog,
            source_context.table_id(),
            base_order,
            through_order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )?;
        self.commit_merge_adjacent_data_files_versionstamped_with_source_context(
            catalog,
            attempt_id,
            commit_metadata,
            compaction,
            source_context,
        )
    }

    pub fn commit_rewrite_delete_data_files_versionstamped(
        &self,
        catalog: CatalogId,
        compaction: RewriteDeleteCompaction,
    ) -> CatalogResult<RewriteDeleteCompaction> {
        self.commit_rewrite_delete_data_files_versionstamped_with_metadata(
            catalog,
            None,
            crate::SnapshotCommitMetadata::default(),
            compaction,
        )
    }

    pub(crate) fn commit_rewrite_delete_data_files_versionstamped_with_metadata(
        &self,
        catalog: CatalogId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        compaction: RewriteDeleteCompaction,
    ) -> CatalogResult<RewriteDeleteCompaction> {
        reject_rewrite_shape(&compaction)?;
        let source_context = RewriteSourceContext::load(
            self,
            catalog,
            &compaction.source_file_ids,
            &compaction.new_files,
        )?;
        self.commit_rewrite_delete_data_files_versionstamped_with_source_context(
            catalog,
            attempt_id,
            commit_metadata,
            compaction,
            source_context,
        )
    }

    fn commit_rewrite_delete_data_files_versionstamped_with_source_context(
        &self,
        catalog: CatalogId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        mut compaction: RewriteDeleteCompaction,
        source_context: RewriteSourceContext,
    ) -> CatalogResult<RewriteDeleteCompaction> {
        let source_deletions = rewrite_source_deletions(
            self,
            catalog,
            source_context.table_id(),
            source_context.sources(),
        )?;
        normalize_rewrite_replacement_row_ids_from_sources(
            source_context.sources(),
            &mut compaction.new_files,
        )?;
        let source_file_ids = compaction.source_file_ids.clone();
        let file_column_stats = rewrite_delete_file_column_stats(
            self,
            catalog,
            &source_file_ids,
            &compaction.new_files,
            &compaction.file_column_stats,
        )?;
        let commit = self.commit_rewrite_delete_data_mutation_versionstamped(
            catalog,
            attempt_id,
            commit_metadata,
            compaction.new_files,
            compaction.partition_values.clone(),
            source_deletions.inline_file_deletions,
            file_column_stats,
            source_context.sources().to_vec(),
            source_deletions.expired_delete_files,
            source_context.table_id(),
        )?;
        compaction.new_files = commit.data_files;
        Ok(compaction)
    }

    pub fn commit_rewrite_delete_data_files_versionstamped_with_conflict_check(
        &self,
        catalog: CatalogId,
        base_order: CatalogOrderId,
        through_order: CatalogOrderId,
        compaction: RewriteDeleteCompaction,
    ) -> CatalogResult<RewriteDeleteCompaction> {
        self.commit_rewrite_delete_data_files_versionstamped_with_conflict_check_and_metadata(
            catalog,
            base_order,
            through_order,
            None,
            crate::SnapshotCommitMetadata::default(),
            compaction,
        )
    }

    pub(crate) fn commit_rewrite_delete_data_files_versionstamped_with_conflict_check_and_metadata(
        &self,
        catalog: CatalogId,
        base_order: CatalogOrderId,
        through_order: CatalogOrderId,
        attempt_id: Option<crate::CommitAttemptId>,
        commit_metadata: crate::SnapshotCommitMetadata,
        compaction: RewriteDeleteCompaction,
    ) -> CatalogResult<RewriteDeleteCompaction> {
        reject_rewrite_shape(&compaction)?;
        let source_context = RewriteSourceContext::load(
            self,
            catalog,
            &compaction.source_file_ids,
            &compaction.new_files,
        )?;
        reject_conflicts_since_base(
            self,
            catalog,
            source_context.table_id(),
            base_order,
            through_order,
            DataCommitIntent::RewriteOrDeleteFiles,
        )?;
        reject_rewrite_source_delete_conflicts_since_base(
            self,
            catalog,
            source_context.table_id(),
            base_order,
            through_order,
            &compaction.source_file_ids,
        )?;
        self.commit_rewrite_delete_data_files_versionstamped_with_source_context(
            catalog,
            attempt_id,
            commit_metadata,
            compaction,
            source_context,
        )
    }
}

fn reject_rewrite_source_delete_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<()> {
    if through_order < base_order {
        return Err(CatalogError::InvalidMutation(
            "rewrite-delete source conflict end order cannot precede base order".to_owned(),
        ));
    }
    let order_kind = scan_order_kind(base_order, through_order);
    let source_file_ids = source_file_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut conflicting_changes = Vec::new();
    for data_file_id in source_file_ids {
        for order in source_delete_file_change_orders_since_base(
            kv,
            catalog,
            data_file_id,
            base_order,
            through_order,
            order_kind,
        )? {
            push_rewrite_source_delete_conflict(
                &mut seen,
                &mut conflicting_changes,
                table_id,
                data_file_id,
                order,
            );
        }
        for order in source_inline_delete_change_orders_since_base(
            kv,
            catalog,
            table_id,
            data_file_id,
            base_order,
            through_order,
        )? {
            push_rewrite_source_delete_conflict(
                &mut seen,
                &mut conflicting_changes,
                table_id,
                data_file_id,
                order,
            );
        }
    }
    conflicting_changes.sort_by_key(|change| (change.order, change.data_file_id));
    if conflicting_changes.is_empty() {
        return Ok(());
    }
    Err(CatalogError::LogicalConflict {
        table_id,
        conflicting_changes,
    })
}

fn source_delete_file_change_orders_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    data_file_id: DataFileId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    order_kind: CatalogOrderKind,
) -> CatalogResult<Vec<CatalogOrderId>> {
    let mut orders = Vec::new();
    for item in kv.scan_range(
        &delete_file_timeline_scan_end(catalog, data_file_id, base_order),
        &delete_file_timeline_scan_end(catalog, data_file_id, through_order),
        crate::RangeDirection::Forward,
        usize::MAX,
    )? {
        let order =
            delete_file_timeline_order_from_key(catalog, data_file_id, &item.key, order_kind)?;
        if base_order < order && order <= through_order {
            orders.push(order);
        }
    }
    Ok(orders)
}

fn source_inline_delete_change_orders_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<Vec<CatalogOrderId>> {
    let start = inline_file_deletion_scan_end(catalog, table_id, data_file_id, base_order);
    let end = inline_file_deletion_scan_end(catalog, table_id, data_file_id, through_order);
    let mut orders = Vec::new();
    for item in kv.scan_range(&start, &end, crate::RangeDirection::Forward, usize::MAX)? {
        let row = InlineFileDeletionRow::decode(&item.value)?;
        let order = inline_file_deletion_begin_order(catalog, &item.key, row.validity.begin_order)?;
        if base_order < order && order <= through_order {
            orders.push(order);
        }
    }
    Ok(orders)
}

fn inline_file_deletion_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    snapshot_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = inline_file_deletion_file_prefix(catalog, table_id, data_file_id);
    key.extend_from_slice(&snapshot_order.as_bytes());
    key.push(0xff);
    key
}

fn push_rewrite_source_delete_conflict(
    seen: &mut BTreeSet<(CatalogOrderId, DataFileId)>,
    conflicting_changes: &mut Vec<DataFileChange>,
    table_id: TableId,
    data_file_id: DataFileId,
    order: CatalogOrderId,
) {
    if seen.insert((order, data_file_id)) {
        conflicting_changes.push(DataFileChange {
            table_id,
            order,
            kind: DataFileChangeKind::Removed,
            data_file_id,
        });
    }
}

fn scan_order_kind(base_order: CatalogOrderId, through_order: CatalogOrderId) -> CatalogOrderKind {
    if base_order.kind() == through_order.kind() {
        through_order.kind()
    } else {
        CatalogOrderKind::UuidV7
    }
}

fn reject_merge_shape(compaction: &MergeAdjacentCompaction) -> CatalogResult<()> {
    if compaction.source_file_ids.is_empty() {
        return Err(CatalogError::InvalidMutation(
            "merge-adjacent compaction requires source data files".to_owned(),
        ));
    }
    Ok(())
}

fn reject_rewrite_shape(compaction: &RewriteDeleteCompaction) -> CatalogResult<()> {
    if compaction.source_file_ids.is_empty() {
        return Err(CatalogError::InvalidMutation(
            "rewrite-delete compaction requires source data files".to_owned(),
        ));
    }
    Ok(())
}

fn reject_source_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<()> {
    let current_delete_file_ids = current_delete_file_ids(kv, catalog, &source_file_ids)?;
    for data_file_id in source_file_ids {
        if current_delete_file_ids.contains_key(data_file_id) {
            return Err(CatalogError::InvalidMutation(format!(
                "data file {} has delete files and cannot be merge-adjacent compacted",
                data_file_id.0
            )));
        }
    }
    Ok(())
}

fn normalize_rewrite_replacement_row_ids_from_sources(
    sources: &[DataFileRow],
    new_files: &mut [DataFileRow],
) -> CatalogResult<()> {
    let source_start = min_known_source_row_id_start(&sources)?;
    derive_unknown_rewrite_replacement_row_ids(&sources, new_files)?;
    for new_file in new_files {
        if new_file.row_id_start_known && new_file.row_id_start < source_start {
            new_file.row_id_start = source_start.saturating_add(new_file.row_id_start);
        }
    }
    Ok(())
}

fn normalize_merge_replacements(
    source_context: &MergeSourceContext,
    compaction: &mut MergeAdjacentCompaction,
) -> CatalogResult<()> {
    let sources = source_context.sources();
    reject_non_empty_sources_without_replacement(sources, &compaction.new_files)?;
    clear_sparse_merge_replacement_row_ids(sources, &mut compaction.new_files);
    derive_merge_replacement_row_ids(sources, source_context.partition_values(), compaction)?;
    apply_merge_replacement_visibility(sources, source_context.partition_values(), compaction)?;
    Ok(())
}

fn reject_non_empty_sources_without_replacement(
    sources: &[DataFileRow],
    new_files: &[DataFileRow],
) -> CatalogResult<()> {
    if !new_files.is_empty() || sources.iter().all(|source| source.record_count == 0) {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(
        "merge-adjacent compaction cannot drop non-empty source files without a replacement"
            .to_owned(),
    ))
}

fn min_known_source_row_id_start(sources: &[DataFileRow]) -> CatalogResult<u64> {
    sources
        .iter()
        .filter(|source| source.row_id_start_known)
        .map(|source| source.row_id_start)
        .min()
        .ok_or_else(|| {
            CatalogError::InvalidMutation(
                "rewrite-delete compaction requires source row id metadata".to_owned(),
            )
        })
}

fn load_source_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<Vec<DataFileRow>> {
    let keys = source_file_ids
        .iter()
        .map(|source_file_id| data_file_key(catalog, *source_file_id))
        .collect::<Vec<_>>();
    source_file_ids
        .iter()
        .zip(kv.batch_get(&keys)?)
        .map(|(source_file_id, value)| {
            let Some(value) = value else {
                return Err(CatalogError::NotFound("data file"));
            };
            let row = DataFileRow::decode(&value)?;
            if row.data_file_id != *source_file_id {
                return Err(CatalogError::Decode(format!(
                    "data file key {} decoded as data file {}",
                    source_file_id.0, row.data_file_id.0
                )));
            }
            Ok(row)
        })
        .collect()
}

struct RewriteSourceContext {
    table_id: TableId,
    sources: Vec<DataFileRow>,
}

impl RewriteSourceContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        source_file_ids: &[DataFileId],
        new_files: &[DataFileRow],
    ) -> CatalogResult<Self> {
        let sources = load_source_files(kv, catalog, source_file_ids)?;
        let table_id = compaction_table_id_from_sources(&sources, new_files)?;
        Ok(Self { table_id, sources })
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn sources(&self) -> &[DataFileRow] {
        &self.sources
    }
}

struct MergeSourceContext {
    table_id: TableId,
    sources: Vec<DataFileRow>,
    partition_values: Vec<crate::FilePartitionValueRow>,
}

impl MergeSourceContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        compaction: &MergeAdjacentCompaction,
    ) -> CatalogResult<Self> {
        let sources = load_source_files(kv, catalog, &compaction.source_file_ids)?;
        let table_id = compaction_table_id_from_sources(&sources, &compaction.new_files)?;
        let partition_values = if compaction.new_files.len() > 1 {
            list_partition_values_for_sources(kv, catalog, &sources)?
        } else {
            Vec::new()
        };
        Ok(Self {
            table_id,
            sources,
            partition_values,
        })
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn sources(&self) -> &[DataFileRow] {
        &self.sources
    }

    fn partition_values(&self) -> &[crate::FilePartitionValueRow] {
        &self.partition_values
    }
}

fn derive_unknown_rewrite_replacement_row_ids(
    sources: &[DataFileRow],
    new_files: &mut [DataFileRow],
) -> CatalogResult<()> {
    let unknown_record_count: u64 = new_files
        .iter()
        .filter(|file| !file.row_id_start_known)
        .map(|file| file.record_count)
        .sum();
    if unknown_record_count == 0 {
        return Ok(());
    }
    let source_end = max_known_source_row_id_end(sources)?;
    let mut next_start = source_end.saturating_sub(unknown_record_count);
    for new_file in new_files.iter_mut().filter(|file| !file.row_id_start_known) {
        new_file.row_id_start = next_start;
        new_file.row_id_start_known = true;
        next_start = next_start.saturating_add(new_file.record_count);
    }
    Ok(())
}

fn max_known_source_row_id_end(sources: &[DataFileRow]) -> CatalogResult<u64> {
    sources
        .iter()
        .filter(|source| source.row_id_start_known)
        .map(|source| source.row_id_start.saturating_add(source.record_count))
        .max()
        .ok_or_else(|| {
            CatalogError::InvalidMutation(
                "rewrite-delete compaction requires source row id metadata".to_owned(),
            )
        })
}

fn derive_merge_replacement_stats(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_context: &MergeSourceContext,
    new_files: &[DataFileRow],
    partition_values: &[crate::FilePartitionValueRow],
) -> CatalogResult<Vec<FileColumnStatsRow>> {
    if new_files.is_empty() {
        return Ok(Vec::new());
    }
    let source_files = source_context.sources();
    let source_file_ids = source_files
        .iter()
        .map(|source| source.data_file_id)
        .collect::<Vec<_>>();
    let source_stats = list_file_column_stats_for_data_file_ids(kv, catalog, &source_file_ids)?;
    let source_partitions = source_context.partition_values();
    let mut out = Vec::new();
    for new_file in new_files {
        let new_partition_key = partition_key_for_file(partition_values, new_file.data_file_id);
        let sources = source_files
            .iter()
            .filter(|source_id| {
                new_files.len() == 1
                    || partition_key_for_file(source_partitions, source_id.data_file_id)
                        == new_partition_key
            })
            .collect::<Vec<_>>();
        out.extend(merge_stats_for_replacement(
            &source_stats,
            &sources,
            new_file,
        ));
    }
    Ok(out)
}

fn merge_stats_for_replacement(
    all_stats: &[FileColumnStatsRow],
    sources: &[&DataFileRow],
    new_file: &DataFileRow,
) -> Vec<FileColumnStatsRow> {
    let mut rows = Vec::new();
    let mut column_ids = all_stats
        .iter()
        .filter(|row| {
            sources
                .iter()
                .any(|source| source.data_file_id == row.data_file_id)
        })
        .map(|row| row.column_id)
        .collect::<Vec<_>>();
    column_ids.sort_by_key(|column_id| column_id.0);
    column_ids.dedup();
    for column_id in column_ids {
        let source_stats = all_stats
            .iter()
            .filter(|row| {
                row.column_id == column_id
                    && sources
                        .iter()
                        .any(|source| source.data_file_id == row.data_file_id)
            })
            .collect::<Vec<_>>();
        if source_stats.is_empty() {
            continue;
        }
        let missing_column_nulls = sources
            .iter()
            .filter(|source| {
                !source_stats
                    .iter()
                    .any(|row| row.data_file_id == source.data_file_id)
            })
            .map(|source| source.record_count)
            .sum::<u64>();
        rows.push(FileColumnStatsRow {
            data_file_id: new_file.data_file_id,
            table_id: new_file.table_id,
            column_id,
            value_count: Some(new_file.record_count),
            null_count: source_stats.iter().map(|row| row.null_count).sum::<u64>()
                + missing_column_nulls,
            min_value: source_stats
                .iter()
                .filter_map(|row| row.min_value.as_ref())
                .min()
                .cloned(),
            max_value: source_stats
                .iter()
                .filter_map(|row| row.max_value.as_ref())
                .max()
                .cloned(),
            extra_stats: None,
        });
    }
    rows
}

fn derive_merge_replacement_row_ids(
    sources: &[DataFileRow],
    source_partition_values: &[crate::FilePartitionValueRow],
    compaction: &mut MergeAdjacentCompaction,
) -> CatalogResult<()> {
    if compaction
        .new_files
        .iter()
        .all(|file| file.row_id_start_known)
    {
        return Ok(());
    }
    if compaction.new_files.len() == 1 {
        let source_refs = sources.iter().collect::<Vec<_>>();
        if !source_row_ranges_are_contiguous_for_record_count(
            &source_refs,
            compaction.new_files[0].record_count,
        ) {
            return Ok(());
        }
        let row_id_start = sources
            .iter()
            .filter(|source| source.row_id_start_known)
            .map(|source| source.row_id_start)
            .min()
            .ok_or_else(|| {
                CatalogError::InvalidMutation(
                    "merge-adjacent compaction requires source row id metadata".to_owned(),
                )
            })?;
        let replacement = &mut compaction.new_files[0];
        replacement.row_id_start = row_id_start;
        replacement.row_id_start_known = true;
        return Ok(());
    }
    derive_partitioned_merge_replacement_row_ids(sources, source_partition_values, compaction)
}

fn derive_partitioned_merge_replacement_row_ids(
    sources: &[DataFileRow],
    source_partition_values: &[crate::FilePartitionValueRow],
    compaction: &mut MergeAdjacentCompaction,
) -> CatalogResult<()> {
    for new_file in compaction
        .new_files
        .iter_mut()
        .filter(|file| !file.row_id_start_known)
    {
        let new_key = partition_key_for_file(&compaction.partition_values, new_file.data_file_id);
        let matching_sources = sources
            .iter()
            .filter(|source| {
                partition_key_for_file(source_partition_values, source.data_file_id) == new_key
            })
            .collect::<Vec<_>>();
        if !source_row_ranges_are_contiguous_for_record_count(
            &matching_sources,
            new_file.record_count,
        ) {
            continue;
        }
        let row_id_start = matching_sources
            .iter()
            .filter(|source| source.row_id_start_known)
            .map(|source| source.row_id_start)
            .min()
            .ok_or_else(|| {
                CatalogError::InvalidMutation(
                    "merge-adjacent compaction replacements require row id metadata".to_owned(),
                )
            })?;
        new_file.row_id_start = row_id_start;
        new_file.row_id_start_known = true;
    }
    Ok(())
}

fn partition_key_for_file(
    partition_values: &[crate::FilePartitionValueRow],
    data_file_id: DataFileId,
) -> Vec<(u32, String)> {
    let mut values = partition_values
        .iter()
        .filter(|row| row.data_file_id == data_file_id)
        .map(|row| (row.partition_key_index.0, row.partition_value.clone()))
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[cfg(test)]
fn list_partition_values_for_source_file_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: &[DataFileId],
) -> CatalogResult<Vec<crate::FilePartitionValueRow>> {
    let source_file_ids = source_file_ids.iter().copied();
    list_partition_values_for_source_ids(kv, catalog, source_file_ids)
}

fn list_partition_values_for_sources(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    sources: &[DataFileRow],
) -> CatalogResult<Vec<crate::FilePartitionValueRow>> {
    let source_file_ids = sources.iter().map(|source| source.data_file_id);
    list_partition_values_for_source_ids(kv, catalog, source_file_ids)
}

fn list_partition_values_for_source_ids(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    source_file_ids: impl IntoIterator<Item = DataFileId>,
) -> CatalogResult<Vec<crate::FilePartitionValueRow>> {
    let source_file_ids = source_file_ids.into_iter().collect::<BTreeSet<_>>();
    list_file_partition_values_for_data_files(kv, catalog, &source_file_ids)
}

fn clear_sparse_merge_replacement_row_ids(sources: &[DataFileRow], new_files: &mut [DataFileRow]) {
    if new_files.len() != 1 {
        return;
    }
    let source_refs = sources.iter().collect::<Vec<_>>();
    if source_row_ranges_are_contiguous_for_record_count(&source_refs, new_files[0].record_count) {
        return;
    }
    new_files[0].row_id_start = 0;
    new_files[0].row_id_start_known = false;
}

fn source_row_ranges_are_contiguous_for_record_count(
    sources: &[&DataFileRow],
    record_count: u64,
) -> bool {
    if sources.is_empty() {
        return record_count == 0;
    }
    let mut ranges = Vec::with_capacity(sources.len());
    for source in sources {
        if !source.row_id_start_known {
            return false;
        }
        if source.record_count == 0 {
            continue;
        }
        ranges.push((
            source.row_id_start,
            source.row_id_start.saturating_add(source.record_count),
        ));
    }
    ranges.sort();
    let Some((first_start, first_end)) = ranges.first().copied() else {
        return record_count == 0;
    };
    let mut end = first_end;
    for (start, next_end) in ranges.into_iter().skip(1) {
        if start != end {
            return false;
        }
        end = next_end;
    }
    end.saturating_sub(first_start) == record_count
}

fn apply_merge_replacement_visibility(
    sources: &[DataFileRow],
    source_partition_values: &[crate::FilePartitionValueRow],
    compaction: &mut MergeAdjacentCompaction,
) -> CatalogResult<()> {
    if sources.is_empty() {
        return Ok(());
    }
    let all_sources = sources.iter().collect::<Vec<_>>();
    let replacement_count = compaction.new_files.len();
    for new_file in &mut compaction.new_files {
        let scoped_sources = if replacement_count == 1 {
            all_sources.clone()
        } else {
            replacement_visibility_sources(
                &all_sources,
                &source_partition_values,
                &compaction.partition_values,
                new_file,
            )?
        };
        if has_complete_explicit_merge_visibility(new_file, scoped_sources.len()) {
            continue;
        }
        apply_merge_visibility_from_sources(&scoped_sources, new_file);
    }
    Ok(())
}

fn replacement_visibility_sources<'a>(
    all_sources: &[&'a DataFileRow],
    source_partition_values: &[crate::FilePartitionValueRow],
    replacement_partition_values: &[crate::FilePartitionValueRow],
    new_file: &DataFileRow,
) -> CatalogResult<Vec<&'a DataFileRow>> {
    let new_key = partition_key_for_file(replacement_partition_values, new_file.data_file_id);
    if new_key.is_empty() {
        return Ok(all_sources.to_vec());
    }
    let matching_sources = all_sources
        .iter()
        .copied()
        .filter(|source| {
            partition_key_for_file(source_partition_values, source.data_file_id) == new_key
        })
        .collect::<Vec<_>>();
    if matching_sources.is_empty() {
        return Err(CatalogError::InvalidMutation(format!(
            "merge-adjacent replacement data file {} has partition values that do not match any source file in table {}",
            new_file.data_file_id.0, new_file.table_id.0
        )));
    }
    Ok(matching_sources)
}

fn apply_merge_visibility_from_sources(sources: &[&DataFileRow], new_file: &mut DataFileRow) {
    let Some(first_source) = sources.first() else {
        return;
    };
    let first_begin = first_source.validity.begin_order;
    let same_begin = sources
        .iter()
        .all(|source| source.validity.begin_order == first_begin);
    if same_begin {
        new_file.max_partial_order = None;
        return;
    }
    let max_partial = sources
        .iter()
        .map(|source| {
            source
                .max_partial_order
                .unwrap_or(source.validity.begin_order)
        })
        .max();
    new_file.validity.begin_order = first_begin;
    new_file.max_partial_order = if sources.len() > 1 { max_partial } else { None };
}

fn has_complete_explicit_merge_visibility(file: &DataFileRow, covered_source_count: usize) -> bool {
    file.max_partial_order.is_some()
        || (covered_source_count <= 1
            && file.validity.begin_order != crate::CatalogOrderId::uuid_v7(0))
}

struct RewriteSourceDeletions {
    expired_delete_files: Vec<FdbExpiredDeleteFile>,
    inline_file_deletions: Vec<InlineFileDeletionRow>,
}

fn rewrite_source_deletions(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    sources: &[DataFileRow],
) -> CatalogResult<RewriteSourceDeletions> {
    let source_file_ids = sources
        .iter()
        .map(|source| source.data_file_id)
        .collect::<Vec<_>>();
    let current_delete_file_ids = current_delete_file_ids(kv, catalog, &source_file_ids)?;
    let inline_source_file_ids = source_file_ids
        .iter()
        .copied()
        .filter(|data_file_id| !current_delete_file_ids.contains_key(data_file_id))
        .collect::<BTreeSet<_>>();
    let inline_deletions = if inline_source_file_ids.is_empty() {
        BTreeMap::new()
    } else {
        latest_snapshot(kv, catalog)?
            .map(|snapshot| {
                list_inline_file_deletions_for_data_files_at(
                    kv,
                    catalog,
                    table_id,
                    snapshot.order,
                    &inline_source_file_ids,
                )
            })
            .transpose()?
            .unwrap_or_default()
    };
    let delete_file_ids = source_file_ids
        .iter()
        .filter_map(|data_file_id| current_delete_file_ids.get(data_file_id).copied())
        .collect::<Vec<_>>();
    let delete_files_by_id = load_delete_files(kv, catalog, &delete_file_ids)?
        .into_iter()
        .map(|row| (row.delete_file_id, row))
        .collect::<BTreeMap<_, _>>();
    let mut expired_delete_files = Vec::new();
    let mut inline_file_deletions = Vec::new();
    for data_file in sources {
        let data_file_id = data_file.data_file_id;
        if let Some(delete_file_id) = current_delete_file_ids.get(&data_file_id).copied() {
            let delete_file = delete_files_by_id
                .get(&delete_file_id)
                .ok_or(CatalogError::NotFound("delete file"))?;
            expired_delete_files.push(FdbExpiredDeleteFile {
                table_id: data_file.table_id,
                delete_file: delete_file.clone(),
            });
            continue;
        }
        let Some(row_ids) = inline_deletions.get(&data_file_id) else {
            return Err(rewrite_without_delete_error(data_file_id));
        };
        if row_ids.is_empty() {
            return Err(rewrite_without_delete_error(data_file_id));
        }
        inline_file_deletions.extend(row_ids.iter().map(|row_id| {
            InlineFileDeletionRow::new(table_id, data_file_id, *row_id, CatalogOrderId::uuid_v7(0))
        }));
    }
    Ok(RewriteSourceDeletions {
        expired_delete_files,
        inline_file_deletions,
    })
}

fn rewrite_without_delete_error(data_file_id: DataFileId) -> CatalogError {
    CatalogError::InvalidMutation(format!(
        "data file {} has no delete file or inline deletions to rewrite",
        data_file_id.0
    ))
}

fn compaction_table_id_from_sources(
    sources: &[DataFileRow],
    new_files: &[DataFileRow],
) -> CatalogResult<TableId> {
    let Some(first_source) = sources.first() else {
        return Err(CatalogError::InvalidMutation(
            "compaction conflict check requires source data files".to_owned(),
        ));
    };
    let table_id = first_source.table_id;
    for source in sources.iter().skip(1) {
        if source.table_id != table_id {
            return Err(CatalogError::InvalidMutation(
                "compaction source files must belong to one table".to_owned(),
            ));
        }
    }
    for new_file in new_files {
        if new_file.table_id != table_id {
            return Err(CatalogError::InvalidMutation(
                "compaction replacement files must belong to the source table".to_owned(),
            ));
        }
    }
    Ok(table_id)
}

fn load_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    delete_file_ids: &[DeleteFileId],
) -> CatalogResult<Vec<DeleteFileRow>> {
    if delete_file_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = delete_file_ids
        .iter()
        .map(|delete_file_id| delete_file_key(catalog, *delete_file_id))
        .collect::<Vec<_>>();
    delete_file_ids
        .iter()
        .zip(kv.batch_get(&keys)?)
        .map(|(delete_file_id, value)| {
            let Some(value) = value else {
                return Err(CatalogError::NotFound("delete file"));
            };
            let row = DeleteFileRow::decode(&value)?;
            if row.delete_file_id != *delete_file_id {
                return Err(CatalogError::Decode(format!(
                    "delete file key {} decoded as delete file {}",
                    delete_file_id.0, row.delete_file_id.0
                )));
            }
            Ok(row)
        })
        .collect()
}

#[cfg(test)]
#[path = "fdb_compaction_tests.rs"]
mod fdb_compaction_tests;
