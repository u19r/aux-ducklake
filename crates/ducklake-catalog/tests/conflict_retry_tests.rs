use ducklake_catalog::{
    AppendCommitResult, CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind,
    CommitAttemptDecision, CommitAttemptId, CommitAttemptRow, DataCommitIntent, DataFileChange,
    DataFileChangeKind, DataFileId, DataFileRow, FakeOrderedCatalogKv, KvBatch, TableId,
    append_data_file, commit_append_data_file, expire_data_file, keys::conflict_fence_key,
    list_current_data_files, list_data_conflicts_since_base, list_data_file_changes_since_base,
    stage_commit_attempt,
};

#[test]
fn given_fence_observed_when_batch_commits_then_fence_version_is_advanced_atomically() {
    let catalog = CatalogId(9);
    let mut kv = FakeOrderedCatalogKv::new();
    let fence = conflict_fence_key(catalog, b"table/42/data");
    let observed = kv.read_conflict_fence(&fence);
    let mut batch = KvBatch::new();
    batch.check_value(fence.clone(), observed);
    batch.put(b"published".to_vec(), b"value".to_vec());
    batch.write_conflict_fence(fence.clone());

    kv.commit(batch).unwrap();

    assert_eq!(kv.get(b"published"), Some(b"value".to_vec()));
    assert_eq!(
        kv.read_conflict_fence(&fence),
        Some(1_u64.to_be_bytes().to_vec())
    );
}

#[test]
fn given_stale_fence_observation_when_batch_commits_then_no_writes_publish() {
    let catalog = CatalogId(9);
    let mut kv = FakeOrderedCatalogKv::new();
    let fence = conflict_fence_key(catalog, b"table/42/data");
    let stale_observation = kv.read_conflict_fence(&fence);
    let mut first = KvBatch::new();
    first.write_conflict_fence(fence.clone());
    kv.commit(first).unwrap();

    let mut stale = KvBatch::new();
    stale.check_value(fence.clone(), stale_observation);
    stale.put(b"unpublished".to_vec(), b"value".to_vec());
    stale.write_conflict_fence(fence);

    let error = kv.commit(stale).unwrap_err();

    assert!(error.to_string().contains("conflict fence changed"));
    assert_eq!(kv.get(b"unpublished"), None);
}

#[test]
fn given_changes_after_base_when_reading_logical_conflicts_then_base_order_is_excluded() {
    let catalog = CatalogId(9);
    let table = TableId(42);
    let other_table = TableId(43);
    let base = CatalogOrderId::uuid_v7(10);
    let concurrent_append = CatalogOrderId::uuid_v7(20);
    let concurrent_remove = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let base_file = DataFileRow::new(DataFileId(1), table, "base.parquet", 10, 100, base);
    let compatible_file = DataFileRow::new(
        DataFileId(2),
        other_table,
        "other-table.parquet",
        20,
        200,
        concurrent_append,
    );

    append_data_file(&mut kv, catalog, base_file.clone()).unwrap();
    append_data_file(&mut kv, catalog, compatible_file).unwrap();
    expire_data_file(&mut kv, catalog, base_file.data_file_id, concurrent_remove).unwrap();

    assert_eq!(
        list_data_file_changes_since_base(&kv, catalog, base, concurrent_remove).unwrap(),
        vec![
            DataFileChange::new(
                other_table,
                concurrent_append,
                DataFileChangeKind::Added,
                DataFileId(2),
            ),
            DataFileChange::new(
                table,
                concurrent_remove,
                DataFileChangeKind::Removed,
                DataFileId(1),
            ),
        ]
    );
    assert_eq!(
        list_data_conflicts_since_base(
            &kv,
            catalog,
            table,
            base,
            concurrent_remove,
            DataCommitIntent::AppendFiles,
        )
        .unwrap(),
        vec![DataFileChange::new(
            table,
            concurrent_remove,
            DataFileChangeKind::Removed,
            DataFileId(1),
        )]
    );
}

#[test]
fn given_only_concurrent_appends_when_checking_append_intent_then_no_conflict_is_returned() {
    let catalog = CatalogId(9);
    let table = TableId(42);
    let base = CatalogOrderId::uuid_v7(10);
    let concurrent = CatalogOrderId::uuid_v7(20);
    let mut kv = FakeOrderedCatalogKv::new();

    append_data_file(
        &mut kv,
        catalog,
        DataFileRow::new(DataFileId(1), table, "base.parquet", 10, 100, base),
    )
    .unwrap();
    append_data_file(
        &mut kv,
        catalog,
        DataFileRow::new(
            DataFileId(2),
            table,
            "concurrent.parquet",
            20,
            200,
            concurrent,
        ),
    )
    .unwrap();

    assert!(
        list_data_conflicts_since_base(
            &kv,
            catalog,
            table,
            base,
            concurrent,
            DataCommitIntent::AppendFiles,
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn given_attempt_row_exists_when_retrying_same_commit_then_previous_order_is_returned() {
    let catalog = CatalogId(9);
    let attempt = CommitAttemptId(99);
    let order = CatalogOrderId::uuid_v7(10);
    let different_order = CatalogOrderId::uuid_v7(20);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut first_batch = KvBatch::new();

    assert!(matches!(
        stage_commit_attempt(&kv, &mut first_batch, catalog, attempt, order).unwrap(),
        CommitAttemptDecision::FirstCommit(_)
    ));
    kv.commit(first_batch).unwrap();

    let mut retry_batch = KvBatch::new();
    assert!(matches!(
        stage_commit_attempt(&kv, &mut retry_batch, catalog, attempt, order).unwrap(),
        CommitAttemptDecision::AlreadyCommitted(row) if row.commit_order == order
    ));
    assert!(matches!(
        stage_commit_attempt(&kv, &mut retry_batch, catalog, attempt, different_order),
        Err(CatalogError::CommitAttemptOrderChanged { attempt_id }) if attempt_id == attempt
    ));
}

#[test]
fn given_fdb_versionstamped_commit_attempt_when_round_tripped_then_order_kind_is_preserved() {
    let row = CommitAttemptRow::new(
        CommitAttemptId(100),
        CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0),
    );

    let decoded = CommitAttemptRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
    assert_eq!(
        decoded.commit_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
}

#[test]
fn given_old_commit_attempt_v1_when_decoded_then_format_is_rejected() {
    let order = CatalogOrderId::uuid_v7(77);
    let mut encoded = Vec::new();
    encoded.push(1);
    encoded.extend_from_slice(&CommitAttemptId(101).as_bytes());
    encoded.extend_from_slice(&order.as_bytes());

    let error = CommitAttemptRow::decode(&encoded).unwrap_err();

    assert_eq!(
        error.to_string(),
        "decode failed: unsupported commit attempt row version 1"
    );
}

#[test]
fn given_compatible_concurrent_append_when_committing_then_append_publishes_and_retry_is_idempotent()
 {
    let catalog = CatalogId(9);
    let table = TableId(42);
    let base = CatalogOrderId::uuid_v7(10);
    let concurrent = CatalogOrderId::uuid_v7(20);
    let commit_order = CatalogOrderId::uuid_v7(30);
    let attempt = CommitAttemptId(123);
    let mut kv = FakeOrderedCatalogKv::new();
    append_data_file(
        &mut kv,
        catalog,
        DataFileRow::new(DataFileId(1), table, "base.parquet", 10, 100, base),
    )
    .unwrap();
    append_data_file(
        &mut kv,
        catalog,
        DataFileRow::new(
            DataFileId(2),
            table,
            "concurrent.parquet",
            20,
            200,
            concurrent,
        ),
    )
    .unwrap();
    let row = DataFileRow::new(
        DataFileId(3),
        table,
        "retry-append.parquet",
        30,
        300,
        commit_order,
    );

    assert_eq!(
        commit_append_data_file(&mut kv, catalog, attempt, base, concurrent, row.clone()).unwrap(),
        AppendCommitResult::Committed(row)
    );
    assert_eq!(
        commit_append_data_file(
            &mut kv,
            catalog,
            attempt,
            base,
            concurrent,
            DataFileRow::new(
                DataFileId(3),
                table,
                "retry-append.parquet",
                30,
                300,
                commit_order,
            )
        )
        .unwrap(),
        AppendCommitResult::AlreadyCommitted { commit_order }
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap().len(),
        3
    );
}

#[test]
fn given_concurrent_remove_when_committing_append_then_typed_conflict_is_returned_without_publish()
{
    let catalog = CatalogId(9);
    let table = TableId(42);
    let base = CatalogOrderId::uuid_v7(10);
    let remove_order = CatalogOrderId::uuid_v7(20);
    let commit_order = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let base_file = DataFileRow::new(DataFileId(1), table, "base.parquet", 10, 100, base);
    append_data_file(&mut kv, catalog, base_file.clone()).unwrap();
    expire_data_file(&mut kv, catalog, base_file.data_file_id, remove_order).unwrap();

    let error = commit_append_data_file(
        &mut kv,
        catalog,
        CommitAttemptId(123),
        base,
        remove_order,
        DataFileRow::new(
            DataFileId(2),
            table,
            "blocked-append.parquet",
            20,
            200,
            commit_order,
        ),
    )
    .unwrap_err();

    let CatalogError::LogicalConflict {
        table_id,
        conflicting_changes,
    } = error
    else {
        panic!("expected logical conflict");
    };
    assert_eq!(table_id, table);
    assert_eq!(
        conflicting_changes,
        vec![DataFileChange::new(
            table,
            remove_order,
            DataFileChangeKind::Removed,
            base_file.data_file_id
        )]
    );
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
}
