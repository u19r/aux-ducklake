use super::*;
use std::{cell::Cell, collections::BTreeSet};

use crate::{
    CatalogId, CatalogOrderId, ColumnId, DeleteFileId, FakeOrderedCatalogKv, FileColumnStatsRow,
    InlineFileDeletionRow, InlineTableFlush, KvBatch, PartitionKeyIndex, RangeDirection, RangeItem,
    RawSnapshotSequence, SchemaId, TableId, commit_append_data_files, commit_create_table,
    commit_inline_file_deletions,
    keys::{
        KeyFamily, current_data_file_key, current_table_row_prefix, data_file_key, family_prefix,
        inline_file_deletion_file_prefix, inline_file_deletion_table_prefix,
        latest_snapshot_row_key,
    },
    kv::OrderedCatalogKv,
    store::stage_snapshot,
};

#[test]
fn maybe_committed_recovery_with_attempt_row_returns_completed_mutation() {
    let catalog = CatalogId(88);
    let attempt = CommitAttemptId(99);
    let order = CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    batch.put(
        commit_attempt_key(catalog, attempt),
        CommitAttemptRow::new(attempt, order).encode(),
    );
    kv.commit(batch).unwrap();

    assert_eq!(
        recover_committed_mutation(&kv, catalog, Some(attempt)).unwrap(),
        Some(DataMutationCommit::default())
    );
}

#[test]
fn maybe_committed_recovery_without_attempt_id_or_row_returns_none() {
    let kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(88);

    assert_eq!(
        recover_committed_mutation(&kv, catalog, None).unwrap(),
        None
    );
    assert_eq!(
        recover_committed_mutation(&kv, catalog, Some(CommitAttemptId(99))).unwrap(),
        None
    );
}

#[test]
fn mutation_snapshot_sequence_uses_ducklake_commit_snapshot_id_when_present() {
    let kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(88);

    assert_eq!(
        mutation_snapshot_sequence(&kv, catalog, Some(CommitAttemptId(42))).unwrap(),
        crate::RawSnapshotSequence(42)
    );
}

#[test]
fn mutation_snapshot_sequence_with_same_commit_snapshot_id_reuses_ducklake_sequence() {
    let catalog = CatalogId(88);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    stage_snapshot(
        &mut batch,
        catalog,
        &SnapshotRow::new(CatalogOrderId::uuid_v7(7), crate::RawSnapshotSequence(11)),
    );
    kv.commit(batch).unwrap();

    assert_eq!(
        mutation_snapshot_sequence(&kv, catalog, Some(CommitAttemptId(11))).unwrap(),
        crate::RawSnapshotSequence(11)
    );
}

#[test]
fn mutation_snapshot_sequence_with_stale_commit_snapshot_id_uses_next_catalog_sequence() {
    let catalog = CatalogId(88);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    stage_snapshot(
        &mut batch,
        catalog,
        &SnapshotRow::new(CatalogOrderId::uuid_v7(7), crate::RawSnapshotSequence(11)),
    );
    kv.commit(batch).unwrap();

    assert_eq!(
        mutation_snapshot_sequence(&kv, catalog, Some(CommitAttemptId(10))).unwrap(),
        crate::RawSnapshotSequence(12)
    );
}

#[test]
fn mutation_snapshot_sequence_without_commit_snapshot_id_uses_next_catalog_sequence() {
    let catalog = CatalogId(88);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    stage_snapshot(
        &mut batch,
        catalog,
        &SnapshotRow::new(CatalogOrderId::uuid_v7(7), crate::RawSnapshotSequence(7)),
    );
    kv.commit(batch).unwrap();

    assert_eq!(
        mutation_snapshot_sequence(&kv, catalog, None).unwrap(),
        crate::RawSnapshotSequence(8)
    );
}

#[test]
fn given_current_table_index_when_rejecting_missing_tables_then_current_table_scan_is_not_used() {
    let catalog = CatalogId(88);
    let mut inner = FakeOrderedCatalogKv::new();
    crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    commit_create_table(&mut inner, catalog, TableId(1), "target").unwrap();
    commit_create_table(&mut inner, catalog, TableId(2), "other").unwrap();
    let kv = CurrentTableScanRejectingKv::new(inner, catalog);

    reject_missing_current_tables(
        &kv,
        catalog,
        &[
            DataFileRow::new(
                DataFileId(10),
                TableId(1),
                "main/target/file.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            ),
            DataFileRow::new(
                DataFileId(11),
                TableId(2),
                "main/other/file.parquet",
                1,
                128,
                CatalogOrderId::uuid_v7(0),
            ),
        ],
    )
    .unwrap();
}

#[test]
fn given_delete_file_with_explicit_begin_when_preparing_versionstamped_commit_then_begin_survives()
{
    let historical_begin = CatalogOrderId::uuid_v7(3);
    let placeholder = CatalogOrderId::fdb_versionstamp([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 0);
    let mut row = DeleteFileRow::new(
        DeleteFileId(3),
        DataFileId(0),
        "main/test/replacement-delete.parquet",
        80,
        1609,
        historical_begin,
    );
    row.validity.end_order = Some(CatalogOrderId::uuid_v7(99));

    prepare_delete_file_for_versionstamped_commit(&mut row, placeholder);

    assert_eq!(row.validity.begin_order, historical_begin);
    assert_eq!(row.validity.end_order, None);
}

#[test]
fn given_delete_file_without_explicit_begin_when_preparing_versionstamped_commit_then_begin_is_commit_order()
 {
    let placeholder = CatalogOrderId::fdb_versionstamp([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 0);
    let mut row = DeleteFileRow::new(
        DeleteFileId(4),
        DataFileId(0),
        "main/test/new-delete.parquet",
        50,
        1202,
        CatalogOrderId::uuid_v7(0),
    );

    prepare_delete_file_for_versionstamped_commit(&mut row, placeholder);

    assert_eq!(row.validity.begin_order, placeholder);
    assert_eq!(row.validity.end_order, None);
}

#[test]
fn given_delete_file_for_proposed_data_file_when_selecting_timeline_order_then_begin_order_is_used()
{
    let begin = CatalogOrderId::uuid_v7(3);
    let placeholder = CatalogOrderId::fdb_versionstamp([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 0);
    let proposed = ProposedDataFileTimelineLookup::Set(BTreeSet::from([DataFileId(10)]));
    let row = DeleteFileRow::new(
        DeleteFileId(11),
        DataFileId(10),
        "main/t/materialized-delete.parquet",
        20,
        1423,
        begin,
    );

    assert_eq!(
        delete_file_timeline_order_for_commit(&proposed, &row, placeholder),
        begin
    );
}

#[test]
fn given_delete_file_for_existing_data_file_when_selecting_timeline_order_then_commit_order_is_used()
 {
    let begin = CatalogOrderId::uuid_v7(3);
    let placeholder = CatalogOrderId::fdb_versionstamp([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 0);
    let row = DeleteFileRow::new(
        DeleteFileId(11),
        DataFileId(10),
        "main/t/cumulative-delete.parquet",
        20,
        1423,
        begin,
    );
    let proposed = ProposedDataFileTimelineLookup::Scan(&[]);

    assert_eq!(
        delete_file_timeline_order_for_commit(&proposed, &row, placeholder),
        placeholder
    );
}

#[test]
fn given_no_delete_files_when_building_timeline_lookup_then_no_ids_are_collected() {
    let proposed = [DataFileRow::new(
        DataFileId(10),
        TableId(1),
        "main/t/materialized.parquet",
        20,
        786,
        CatalogOrderId::uuid_v7(2),
    )];

    let ids = proposed_data_file_ids_for_delete_timeline(&proposed, &[]);

    assert!(!ids.uses_set());
}

#[test]
fn given_many_delete_files_when_building_timeline_lookup_then_ids_are_collected_once() {
    let proposed = (10..16)
        .map(|id| {
            DataFileRow::new(
                DataFileId(id),
                TableId(1),
                format!("main/t/{id}.parquet"),
                20,
                786,
                CatalogOrderId::uuid_v7(2),
            )
        })
        .collect::<Vec<_>>();
    let delete_files = [
        DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "main/t/delete-20.parquet",
            20,
            1423,
            CatalogOrderId::uuid_v7(3),
        )),
        DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
            DeleteFileId(21),
            DataFileId(11),
            "main/t/delete-21.parquet",
            20,
            1424,
            CatalogOrderId::uuid_v7(4),
        )),
    ];

    let ids = proposed_data_file_ids_for_delete_timeline(&proposed, &delete_files);

    assert!(ids.uses_set());
    assert!(ids.contains(&DataFileId(10)));
    assert!(!ids.contains(&DataFileId(99)));
}

#[test]
fn mutation_recovery_attempt_id_distinguishes_concurrent_commits_for_same_snapshot() {
    let snapshot = Some(CommitAttemptId(2));
    let first = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        &[DataFileRow::new(
            DataFileId(10),
            TableId(1),
            "main/table-a.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
    );
    let second = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        &[DataFileRow::new(
            DataFileId(11),
            TableId(1),
            "main/table-b.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        )],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
    );

    assert_ne!(first, second);
    assert_ne!(first, snapshot);
}

#[test]
fn mutation_recovery_attempt_id_distinguishes_inline_flushes_for_same_snapshot() {
    let snapshot = Some(CommitAttemptId(6));
    let first = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        &[],
        &[],
        &[InlineTableFlush::new(
            TableId(1),
            crate::SchemaId(1),
            RawSnapshotSequence(5),
        )],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
    );
    let second = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        &[],
        &[],
        &[InlineTableFlush::new(
            TableId(1),
            crate::SchemaId(2),
            RawSnapshotSequence(5),
        )],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
    );

    assert_ne!(first, second);
}

#[test]
fn mutation_recovery_attempt_id_distinguishes_stats_and_inline_deletes_for_same_snapshot() {
    let snapshot = Some(CommitAttemptId(8));
    let file = DataFileRow::new(
        DataFileId(7),
        TableId(1),
        "main/t/rewrite.parquet",
        120,
        2967,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(130)
    .with_footer_size(Some(634));
    let first = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        std::slice::from_ref(&file),
        &[],
        &[],
        &[],
        &[InlineFileDeletionRow::new(
            TableId(1),
            DataFileId(1),
            20,
            CatalogOrderId::uuid_v7(0),
        )],
        &[FileColumnStatsRow::new(
            DataFileId(7),
            TableId(1),
            ColumnId(1),
            0,
            Some("0".to_owned()),
            Some("229".to_owned()),
        )
        .with_value_count(Some(120))],
        &[],
        &[],
        &[],
    );
    let second = mutation_recovery_attempt_id(
        snapshot,
        &crate::SnapshotCommitMetadata::default(),
        &[file],
        &[],
        &[],
        &[],
        &[InlineFileDeletionRow::new(
            TableId(1),
            DataFileId(1),
            21,
            CatalogOrderId::uuid_v7(0),
        )],
        &[FileColumnStatsRow::new(
            DataFileId(7),
            TableId(1),
            ColumnId(1),
            0,
            Some("0".to_owned()),
            Some("230".to_owned()),
        )
        .with_value_count(Some(120))],
        &[],
        &[],
        &[],
    );

    assert_ne!(first, second);
}

#[test]
fn given_inline_file_deletions_when_fdb_delete_file_materializes_them_then_replaced_rows_are_discovered()
 {
    let catalog = CatalogId(88);
    let table = TableId(1);
    let data_file = DataFileId(3);
    let mut kv = FakeOrderedCatalogKv::new();
    let initial = crate::initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                data_file,
                table,
                "main/t/ducklake-file.parquet",
                3,
                552,
                initial.order,
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, data_file, 0, initial.order),
            InlineFileDeletionRow::new(table, data_file, 1, initial.order),
        ],
    )
    .unwrap();
    let delete_order = crate::latest_snapshot(&kv, catalog).unwrap().unwrap().order;

    let replaced = inline_file_deletions_replaced_by_delete_files(
        &kv,
        catalog,
        &[],
        &[DeleteFileRow::new(
            DeleteFileId(6),
            data_file,
            "main/t/ducklake-delete.parquet",
            2,
            1073,
            CatalogOrderId::uuid_v7(0),
        )],
        crate::latest_snapshot(&kv, catalog).unwrap().as_ref(),
    )
    .unwrap();

    assert_eq!(replaced.len(), 2);
    assert_eq!(
        replaced
            .iter()
            .map(|row| (
                row.table_id,
                row.data_file_id,
                row.row_id,
                row.validity.begin_order
            ))
            .collect::<Vec<_>>(),
        vec![
            (table, data_file, 0, delete_order),
            (table, data_file, 1, delete_order),
        ]
    );
}

#[test]
fn given_small_materialized_delete_file_set_when_loading_inline_deletions_then_file_prefixes_are_scanned()
 {
    let catalog = CatalogId(88);
    let table = TableId(1);
    let first_data_file = DataFileId(3);
    let second_data_file = DataFileId(4);
    let mut kv = FakeOrderedCatalogKv::new();
    let initial = crate::initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                first_data_file,
                table,
                "main/t/first.parquet",
                3,
                552,
                initial.order,
            )
            .with_row_id_start(0),
            DataFileRow::new(
                second_data_file,
                table,
                "main/t/second.parquet",
                3,
                553,
                initial.order,
            )
            .with_row_id_start(3),
        ],
    )
    .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, first_data_file, 0, initial.order),
            InlineFileDeletionRow::new(table, second_data_file, 3, initial.order),
        ],
    )
    .unwrap();
    let latest = crate::latest_snapshot(&kv, catalog).unwrap();
    let kv = InlineDeletionScanCountingKv::new(
        kv,
        catalog,
        table,
        BTreeSet::from([first_data_file, second_data_file]),
    );
    let mut first_delete = DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
        DeleteFileId(6),
        first_data_file,
        "main/t/first-delete.parquet",
        1,
        1073,
        CatalogOrderId::uuid_v7(0),
    ));
    first_delete.mark_materializes_inline_deletes();
    let mut second_delete = DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
        DeleteFileId(7),
        second_data_file,
        "main/t/second-delete.parquet",
        1,
        1074,
        CatalogOrderId::uuid_v7(0),
    ));
    second_delete.mark_materializes_inline_deletes();
    let data_file_context = MutationDataFileContext::load(
        &kv,
        catalog,
        &[],
        &[],
        BTreeSet::from([first_data_file, second_data_file]),
    )
    .unwrap();

    let replaced = materialized_inline_file_deletions(
        &kv,
        catalog,
        &data_file_context,
        &[first_delete, second_delete],
        latest.as_ref(),
    )
    .unwrap();

    assert_eq!(kv.inline_deletion_table_scans(), 0);
    assert_eq!(kv.inline_deletion_file_scans(), 2);
    assert_eq!(
        replaced
            .iter()
            .map(|row| (row.table_id, row.data_file_id, row.row_id))
            .collect::<Vec<_>>(),
        vec![(table, first_data_file, 0), (table, second_data_file, 3),]
    );
}

#[test]
fn given_materialized_inline_context_when_loaded_then_inline_discovery_reuses_data_files() {
    let catalog = CatalogId(88);
    let table = TableId(1);
    let data_file_id = DataFileId(3);
    let mut inner = FakeOrderedCatalogKv::new();
    let initial = crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    let data_file = commit_append_data_files(
        &mut inner,
        catalog,
        vec![
            DataFileRow::new(
                data_file_id,
                table,
                "main/t/source.parquet",
                3,
                552,
                initial.order,
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap()
    .pop()
    .unwrap();
    commit_inline_file_deletions(
        &mut inner,
        catalog,
        vec![InlineFileDeletionRow::new(
            table,
            data_file_id,
            0,
            initial.order,
        )],
    )
    .unwrap();
    let latest = crate::latest_snapshot(&inner, catalog).unwrap();
    let kv = DataFileReadCountingKv::new(inner, catalog, data_file_id);
    let data_file_context = MutationDataFileContext::load(
        &kv,
        catalog,
        &[],
        std::slice::from_ref(&data_file),
        BTreeSet::from([data_file_id]),
    )
    .unwrap();
    let mut delete_file = DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
        DeleteFileId(6),
        data_file_id,
        "main/t/delete.parquet",
        1,
        1073,
        CatalogOrderId::uuid_v7(0),
    ));
    delete_file.mark_materializes_inline_deletes();

    let replaced = materialized_inline_file_deletions(
        &kv,
        catalog,
        &data_file_context,
        &[delete_file],
        latest.as_ref(),
    )
    .unwrap();

    assert_eq!(replaced.len(), 1);
    assert_eq!(kv.data_file_gets(), 0);
    assert_eq!(kv.data_file_batch_gets(), 0);
}

#[test]
fn given_multiple_dropped_files_when_validating_currentness_then_current_index_reads_are_batched() {
    let catalog = CatalogId(88);
    let table = TableId(1);
    let mut inner = FakeOrderedCatalogKv::new();
    let initial = crate::initialize_catalog_if_absent(&mut inner, catalog).unwrap();
    let rows = commit_append_data_files(
        &mut inner,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(3),
                table,
                "main/t/first.parquet",
                3,
                552,
                initial.order,
            )
            .with_row_id_start(0),
            DataFileRow::new(
                DataFileId(4),
                table,
                "main/t/second.parquet",
                3,
                553,
                initial.order,
            )
            .with_row_id_start(3),
        ],
    )
    .unwrap();
    let kv = CurrentDataFileReadCountingKv::new(inner, catalog, table);

    reject_dropping_non_current_data_files(&kv, catalog, &rows).unwrap();

    assert_eq!(kv.current_data_file_gets(), 0);
    assert_eq!(kv.current_data_file_batch_gets(), 1);
}

#[test]
fn given_materialized_delete_file_without_visibility_when_inline_deletions_exist_then_visibility_is_derived()
 {
    let data_file = DataFileId(3);
    let delete_file = DeleteFileId(6);
    let first_delete = CatalogOrderId::uuid_v7(20);
    let second_delete = CatalogOrderId::uuid_v7(30);
    let mut materialization =
        DeleteFileMaterialization::historical_delete_file(DeleteFileRow::new(
            delete_file,
            data_file,
            "main/t/ducklake-delete.parquet",
            2,
            1073,
            incomplete_order(),
        ));
    materialization.mark_materializes_inline_deletes();
    let mut materializations = vec![materialization];

    complete_materialized_delete_file_visibility(
        &mut materializations,
        &[
            InlineFileDeletionRow::new(TableId(1), data_file, 0, first_delete),
            InlineFileDeletionRow::new(TableId(1), data_file, 1, second_delete),
        ],
    );

    let row = materializations[0].row();
    assert_eq!(row.validity.begin_order, first_delete);
    assert_eq!(row.max_partial_order, Some(second_delete));
}

#[test]
fn given_preloaded_data_file_when_mutation_context_loads_then_data_file_is_not_fetched_again() {
    let catalog = CatalogId(88);
    let table = TableId(7);
    let data_file = DataFileRow::new(
        DataFileId(3),
        table,
        "main/t/source.parquet",
        3,
        552,
        CatalogOrderId::uuid_v7(1),
    )
    .with_row_id_start(0);
    let kv =
        DataFileReadCountingKv::new(FakeOrderedCatalogKv::new(), catalog, data_file.data_file_id);

    let mut context = MutationDataFileContext::load(
        &kv,
        catalog,
        &[],
        std::slice::from_ref(&data_file),
        BTreeSet::from([data_file.data_file_id]),
    )
    .unwrap();

    assert_eq!(context.get(data_file.data_file_id).unwrap(), &data_file);
    assert_eq!(kv.data_file_gets(), 0);
    assert_eq!(kv.data_file_batch_gets(), 0);

    context
        .load_missing(&kv, catalog, BTreeSet::from([data_file.data_file_id]))
        .unwrap();

    assert_eq!(kv.data_file_gets(), 0);
    assert_eq!(kv.data_file_batch_gets(), 0);
}

fn inline_file_deletions_replaced_by_delete_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    _data_files: &[DataFileRow],
    delete_files: &[DeleteFileRow],
    snapshot: Option<&SnapshotRow>,
) -> CatalogResult<Vec<InlineFileDeletionRow>> {
    let Some(snapshot) = snapshot else {
        return Ok(Vec::new());
    };
    let data_file_ids = delete_files
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineFileDeletion),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = InlineFileDeletionRow::decode(&item.value)?;
        if data_file_ids.contains(&row.data_file_id) && row.validity.is_visible_at(snapshot.order) {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| (row.table_id, row.data_file_id, row.row_id));
    Ok(rows)
}

#[test]
fn given_inline_flush_materialization_when_row_ids_overlap_then_overlap_guard_is_not_used() {
    let flushes = [InlineTableFlush::new(
        TableId(1),
        SchemaId(1),
        RawSnapshotSequence(16),
    )];

    assert!(!should_reject_current_row_id_overlaps(
        RowIdOverlapPolicy::RejectCurrentOverlaps,
        &flushes
    ));
    assert!(should_reject_current_row_id_overlaps(
        RowIdOverlapPolicy::RejectCurrentOverlaps,
        &[]
    ));
    assert!(!should_reject_current_row_id_overlaps(
        RowIdOverlapPolicy::TrustCompactionReplacementRows,
        &[]
    ));
}

#[test]
fn duplicate_proposed_data_file_ids_are_rejected_before_commit() {
    let table = TableId(1);
    let rows = vec![
        DataFileRow::new(
            DataFileId(10),
            table,
            "main/table-a.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        ),
        DataFileRow::new(
            DataFileId(10),
            table,
            "main/table-b.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        ),
    ];

    assert_eq!(
        reject_duplicate_proposed_file_ids(&rows, std::iter::empty::<DeleteFileId>()).unwrap_err(),
        CatalogError::InvalidMutation(
            "conflict committing data mutation: data file id 10 is duplicated in the mutation"
                .to_owned()
        )
    );
}

#[test]
fn duplicate_proposed_delete_file_ids_are_rejected_before_commit() {
    let rows = vec![
        DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "main/delete-a.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        ),
        DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(11),
            "main/delete-b.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        ),
    ];

    assert_eq!(
        reject_duplicate_proposed_file_ids(&[], rows.iter().map(|row| row.delete_file_id))
            .unwrap_err(),
        CatalogError::InvalidMutation(
            "conflict committing data mutation: delete file id 20 is duplicated in the mutation"
                .to_owned()
        )
    );
}

#[test]
fn given_multiple_file_ids_when_observing_watermark_then_highest_candidate_is_retained() {
    let mut watermark = FdbFileIdWatermark::default();

    watermark.observe(42);
    watermark.observe(7);
    watermark.observe(123);

    assert_eq!(watermark.candidate, Some(123));
}

#[test]
fn retry_decision_retries_retryable_not_committed_before_budget_is_exhausted() {
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 0),
        MutationFailureAction::Retry
    );
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 2),
        MutationFailureAction::Retry
    );
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::RetryableNotCommitted, 3),
        MutationFailureAction::ReturnError
    );
}

#[test]
fn retry_decision_recovers_maybe_committed_and_rejects_other_classes() {
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::MaybeCommitted, 0),
        MutationFailureAction::RecoverMaybeCommitted
    );
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::Retryable, 0),
        MutationFailureAction::ReturnError
    );
    assert_eq!(
        mutation_failure_action(FoundationDbErrorClass::NonRetryable, 0),
        MutationFailureAction::ReturnError
    );
}

#[test]
fn exhausted_retryable_not_committed_error_reports_retry_budget() {
    let error = CatalogError::FoundationDb {
        code: 1020,
        message: "not_committed".to_owned(),
        class: FoundationDbErrorClass::RetryableNotCommitted,
    };

    assert_eq!(
        mutation_final_error(error, FoundationDbErrorClass::RetryableNotCommitted, 3),
        CatalogError::FoundationDbRetryExhausted {
            operation: "data mutation commit",
            attempts: 4,
            code: 1020,
            message: "not_committed".to_owned(),
            class: FoundationDbErrorClass::RetryableNotCommitted,
        }
    );
}

#[test]
fn final_error_keeps_non_exhausted_and_non_retryable_errors_unchanged() {
    let retryable = CatalogError::FoundationDb {
        code: 1020,
        message: "not_committed".to_owned(),
        class: FoundationDbErrorClass::RetryableNotCommitted,
    };
    assert_eq!(
        mutation_final_error(
            retryable.clone(),
            FoundationDbErrorClass::RetryableNotCommitted,
            2
        ),
        retryable
    );

    let non_retryable = CatalogError::FoundationDb {
        code: 2004,
        message: "invalid_option".to_owned(),
        class: FoundationDbErrorClass::NonRetryable,
    };
    assert_eq!(
        mutation_final_error(
            non_retryable.clone(),
            FoundationDbErrorClass::NonRetryable,
            0
        ),
        non_retryable
    );
}

#[test]
fn many_file_and_partition_metadata_rows_are_rejected_before_commit() {
    let catalog = CatalogId(88);
    let table = TableId(44);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let data_files = (0..7_000)
        .map(|index| {
            DataFileRow::new(
                DataFileId(index),
                table,
                format!("s3://warehouse/table/file-{index:05}.parquet"),
                10,
                1_024,
                incomplete_order(),
            )
        })
        .collect::<Vec<_>>();
    let partition_values = data_files
        .iter()
        .map(|row| {
            FilePartitionValueRow::new(
                row.data_file_id,
                table,
                PartitionKeyIndex(0),
                format!("region-{file_id:05}", file_id = row.data_file_id.0),
            )
        })
        .collect::<Vec<_>>();

    let estimated_bytes = estimate_mutation_metadata_bytes(
        catalog,
        &snapshot,
        Some(CommitAttemptId(99)),
        &data_files,
        &[],
        &partition_values,
        &[],
    );

    assert!(estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES);
    assert!(reject_estimated_mutation(estimated_bytes).is_err());
}

#[test]
fn mutation_metadata_estimate_accepts_borrowed_delete_materialization_rows() {
    let catalog = CatalogId(88);
    let table = TableId(44);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let data_files = vec![DataFileRow::new(
        DataFileId(10),
        table,
        "s3://warehouse/table/file-00010.parquet",
        10,
        1_024,
        incomplete_order(),
    )];
    let delete_files = vec![DeleteFileRow::new(
        DeleteFileId(20),
        DataFileId(10),
        "s3://warehouse/table/delete-00020.parquet",
        1,
        512,
        incomplete_order(),
    )];
    let materializations = delete_files
        .iter()
        .cloned()
        .map(DeleteFileMaterialization::historical_delete_file)
        .collect::<Vec<_>>();

    let slice_estimate = estimate_mutation_metadata_bytes(
        catalog,
        &snapshot,
        Some(CommitAttemptId(99)),
        &data_files,
        delete_files.iter(),
        &[],
        &[],
    );
    let materialization_estimate = estimate_mutation_metadata_bytes(
        catalog,
        &snapshot,
        Some(CommitAttemptId(99)),
        &data_files,
        materializations.iter().map(DeleteFileMaterialization::row),
        &[],
        &[],
    );

    assert_eq!(slice_estimate, materialization_estimate);
}

#[test]
fn partition_estimate_data_file_lookup_uses_map_without_changing_first_match_behavior() {
    let table = TableId(44);
    let data_files = vec![
        DataFileRow::new(
            DataFileId(10),
            table,
            "s3://warehouse/table/first.parquet",
            10,
            1_024,
            incomplete_order(),
        ),
        DataFileRow::new(
            DataFileId(10),
            table,
            "s3://warehouse/table/duplicate.parquet",
            10,
            1_024,
            incomplete_order(),
        ),
        DataFileRow::new(
            DataFileId(11),
            table,
            "s3://warehouse/table/other.parquet",
            10,
            1_024,
            incomplete_order(),
        ),
    ];

    let lookup = PartitionEstimateDataFileLookup::new(&data_files, 32);

    assert!(lookup.uses_map());
    assert_eq!(
        lookup.get(DataFileId(10)).map(|row| row.path.as_str()),
        Some("s3://warehouse/table/first.parquet")
    );
}

struct CurrentTableScanRejectingKv {
    inner: FakeOrderedCatalogKv,
    current_table_prefix: Vec<u8>,
    latest_snapshot_key: Vec<u8>,
}

impl CurrentTableScanRejectingKv {
    fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId) -> Self {
        Self {
            inner,
            current_table_prefix: current_table_row_prefix(catalog),
            latest_snapshot_key: latest_snapshot_row_key(catalog),
        }
    }
}

impl OrderedCatalogKv for CurrentTableScanRejectingKv {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        assert_ne!(
            key, self.latest_snapshot_key,
            "indexed data mutation table validation should not read the latest snapshot row"
        );
        OrderedCatalogKv::get(&self.inner, key)
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
        OrderedCatalogKv::batch_get(&self.inner, keys)
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        assert_ne!(
            prefix, self.current_table_prefix,
            "data mutation table validation should not scan all current tables"
        );
        OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        OrderedCatalogKv::read_conflict_fence(&self.inner, key)
    }
}

struct InlineDeletionScanCountingKv {
    inner: FakeOrderedCatalogKv,
    inline_deletion_table_prefix: Vec<u8>,
    inline_deletion_file_prefixes: BTreeSet<Vec<u8>>,
    inline_deletion_table_scans: Cell<usize>,
    inline_deletion_file_scans: Cell<usize>,
}

struct CurrentDataFileReadCountingKv {
    inner: FakeOrderedCatalogKv,
    first_current_data_file_key: Vec<u8>,
    second_current_data_file_key: Vec<u8>,
    current_data_file_gets: Cell<usize>,
    current_data_file_batch_gets: Cell<usize>,
}

struct DataFileReadCountingKv {
    inner: FakeOrderedCatalogKv,
    data_file_key: Vec<u8>,
    data_file_gets: Cell<usize>,
    data_file_batch_gets: Cell<usize>,
}

impl DataFileReadCountingKv {
    fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId, data_file_id: DataFileId) -> Self {
        Self {
            inner,
            data_file_key: data_file_key(catalog, data_file_id),
            data_file_gets: Cell::new(0),
            data_file_batch_gets: Cell::new(0),
        }
    }

    fn data_file_gets(&self) -> usize {
        self.data_file_gets.get()
    }

    fn data_file_batch_gets(&self) -> usize {
        self.data_file_batch_gets.get()
    }
}

impl OrderedCatalogKv for DataFileReadCountingKv {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        if key == self.data_file_key {
            self.data_file_gets
                .set(self.data_file_gets.get().saturating_add(1));
        }
        OrderedCatalogKv::get(&self.inner, key)
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
        if keys.iter().any(|key| key == &self.data_file_key) {
            self.data_file_batch_gets
                .set(self.data_file_batch_gets.get().saturating_add(1));
        }
        OrderedCatalogKv::batch_get(&self.inner, keys)
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        OrderedCatalogKv::read_conflict_fence(&self.inner, key)
    }
}

impl CurrentDataFileReadCountingKv {
    fn new(inner: FakeOrderedCatalogKv, catalog: CatalogId, table: TableId) -> Self {
        Self {
            inner,
            first_current_data_file_key: current_data_file_key(catalog, table, DataFileId(3)),
            second_current_data_file_key: current_data_file_key(catalog, table, DataFileId(4)),
            current_data_file_gets: Cell::new(0),
            current_data_file_batch_gets: Cell::new(0),
        }
    }

    fn current_data_file_gets(&self) -> usize {
        self.current_data_file_gets.get()
    }

    fn current_data_file_batch_gets(&self) -> usize {
        self.current_data_file_batch_gets.get()
    }

    fn is_tracked_current_data_file_key(&self, key: &[u8]) -> bool {
        key == self.first_current_data_file_key || key == self.second_current_data_file_key
    }
}

impl OrderedCatalogKv for CurrentDataFileReadCountingKv {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        if self.is_tracked_current_data_file_key(key) {
            self.current_data_file_gets
                .set(self.current_data_file_gets.get().saturating_add(1));
        }
        OrderedCatalogKv::get(&self.inner, key)
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
        if keys
            .iter()
            .any(|key| self.is_tracked_current_data_file_key(key))
        {
            self.current_data_file_batch_gets
                .set(self.current_data_file_batch_gets.get().saturating_add(1));
        }
        OrderedCatalogKv::batch_get(&self.inner, keys)
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        OrderedCatalogKv::read_conflict_fence(&self.inner, key)
    }
}

impl InlineDeletionScanCountingKv {
    fn new(
        inner: FakeOrderedCatalogKv,
        catalog: CatalogId,
        table: TableId,
        data_file_ids: BTreeSet<DataFileId>,
    ) -> Self {
        Self {
            inner,
            inline_deletion_table_prefix: inline_file_deletion_table_prefix(catalog, table),
            inline_deletion_file_prefixes: data_file_ids
                .into_iter()
                .map(|data_file_id| inline_file_deletion_file_prefix(catalog, table, data_file_id))
                .collect(),
            inline_deletion_table_scans: Cell::new(0),
            inline_deletion_file_scans: Cell::new(0),
        }
    }

    fn inline_deletion_table_scans(&self) -> usize {
        self.inline_deletion_table_scans.get()
    }

    fn inline_deletion_file_scans(&self) -> usize {
        self.inline_deletion_file_scans.get()
    }
}

impl OrderedCatalogKv for InlineDeletionScanCountingKv {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        OrderedCatalogKv::get(&self.inner, key)
    }

    fn batch_get(&self, keys: &[Vec<u8>]) -> crate::CatalogResult<Vec<Option<Vec<u8>>>> {
        OrderedCatalogKv::batch_get(&self.inner, keys)
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        if prefix == self.inline_deletion_table_prefix {
            self.inline_deletion_table_scans
                .set(self.inline_deletion_table_scans.get().saturating_add(1));
        }
        if self.inline_deletion_file_prefixes.contains(prefix) {
            self.inline_deletion_file_scans
                .set(self.inline_deletion_file_scans.get().saturating_add(1));
        }
        OrderedCatalogKv::scan_prefix(&self.inner, prefix, direction, limit)
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        OrderedCatalogKv::scan_range(&self.inner, start, end, direction, limit)
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        OrderedCatalogKv::read_conflict_fence(&self.inner, key)
    }
}
