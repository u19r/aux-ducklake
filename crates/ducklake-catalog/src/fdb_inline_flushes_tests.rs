use crate::{
    CatalogError, CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbDataMutation,
    FdbOrderedCatalogKv, InlineFileDeletionRow, InlineTableFlush, InlinedTableRow, SchemaId,
    SnapshotRow, TableColumnRow, TableId, TableRow,
    fdb_inline_flushes::{prepare_inline_flushes, stage_prepared_inline_flushes},
    latest_snapshot, list_inline_file_deletions_at, list_inline_table_payloads_at,
};

fn create_fixture(name: &str, schema: SchemaId) -> (FdbOrderedCatalogKv, SnapshotRow) {
    assert_eq!(std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref(), Ok("1"));
    let prefix = format!(
        "ducklake-review/{name}/{}/{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    let mut table = kv
        .create_table_versionstamped(
            CatalogId(1),
            TableRow::with_catalog_metadata(
                TableId(1),
                SchemaId(0),
                "main",
                "events",
                "main/events",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "BIGINT",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
            None,
        )
        .unwrap();
    table
        .inlined_data_tables
        .push(InlinedTableRow::new("events_inline", schema.0));
    kv.register_inline_table_payload_with_table_versionstamped(
        CatalogId(1),
        table,
        schema,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    (kv, snapshot)
}

#[test]
#[ignore = "requires an explicitly owned FoundationDB cluster"]
fn given_prepared_flush_when_another_flush_commits_before_transaction_then_stale_preparation_is_rejected()
 {
    let (kv, snapshot) = create_fixture("prepared-flush", SchemaId(1));
    let flush = InlineTableFlush::new(TableId(1), SchemaId(1), snapshot.sequence);
    let prepared = prepare_inline_flushes(&kv, CatalogId(1), &[flush]).unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        FdbDataMutation::new(
            vec![DataFileRow::new(
                DataFileId(1),
                TableId(1),
                "first.parquet",
                1,
                10,
                snapshot.order,
            )],
            Vec::new(),
            vec![flush],
            Vec::new(),
            Vec::new(),
        ),
    )
    .unwrap();
    let transaction = kv.create_transaction().unwrap();
    let result = stage_prepared_inline_flushes(&kv, &transaction, CatalogId(1), &prepared);
    assert!(
        matches!(result, Err(CatalogError::ConflictFenceChanged { .. })),
        "preparation predating a successful flush must not publish duplicate files: {result:?}"
    );
}

#[test]
#[ignore = "requires an explicitly owned FoundationDB cluster"]
fn given_all_inline_rows_deleted_when_payload_is_flushed_then_watermark_and_snapshot_still_commit()
{
    let (kv, _) = create_fixture("deleted-inline", SchemaId(1));
    kv.commit_delete_inline_table_rows_versionstamped(
        CatalogId(1),
        TableId(1),
        SchemaId(1),
        &[0],
        None,
    )
    .unwrap();
    let before = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let committed = kv
        .commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            FdbDataMutation::new(
                Vec::new(),
                Vec::new(),
                vec![InlineTableFlush::new(
                    TableId(1),
                    SchemaId(1),
                    before.sequence,
                )],
                Vec::new(),
                Vec::new(),
            ),
        )
        .unwrap();
    assert_eq!(committed.flushed_inline_count, 1);
    let after = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    assert!(after.order > before.order);
    assert!(
        list_inline_table_payloads_at(&kv, CatalogId(1), TableId(1), SchemaId(1), after.order)
            .unwrap()
            .is_empty()
    );
}

#[test]
#[ignore = "requires an explicitly owned FoundationDB cluster"]
fn given_newer_file_deletion_flush_with_older_data_flush_when_committed_then_both_frontiers_are_applied()
 {
    let (kv, _) = create_fixture("mixed-flush-frontiers", SchemaId(0));
    kv.append_data_files_versionstamped(
        CatalogId(1),
        vec![
            DataFileRow::new(
                DataFileId(90),
                TableId(1),
                "old.parquet",
                1,
                10,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(100),
        ],
    )
    .unwrap();
    let data_snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        FdbDataMutation {
            inline_file_deletions: vec![InlineFileDeletionRow::new(
                TableId(1),
                DataFileId(90),
                100,
                CatalogOrderId::uuid_v7(0),
            )],
            ..FdbDataMutation::default()
        },
    )
    .unwrap();
    let deletion_snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    assert_eq!(
        list_inline_file_deletions_at(&kv, CatalogId(1), TableId(1), deletion_snapshot.order)
            .unwrap()
            .len(),
        1
    );
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        FdbDataMutation::new(
            vec![
                DataFileRow::new(
                    DataFileId(91),
                    TableId(1),
                    "new.parquet",
                    1,
                    10,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            Vec::new(),
            vec![
                InlineTableFlush::new(TableId(1), SchemaId(0), data_snapshot.sequence),
                InlineTableFlush::file_deletions_only(TableId(1), deletion_snapshot.sequence),
            ],
            Vec::new(),
            Vec::new(),
        ),
    )
    .unwrap();
    let after = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    assert!(
        list_inline_file_deletions_at(&kv, CatalogId(1), TableId(1), after.order)
            .unwrap()
            .is_empty(),
        "a newer deletion-only flush must not be discarded because another data flush exists"
    );
}

#[test]
#[ignore = "requires an explicitly owned FoundationDB cluster"]
fn given_prepared_file_deletion_flush_when_data_flush_commits_in_another_schema_then_preparation_is_stale()
 {
    let (kv, _) = create_fixture("cross-schema-fence", SchemaId(1));
    kv.append_data_files_versionstamped(
        CatalogId(1),
        vec![
            DataFileRow::new(
                DataFileId(90),
                TableId(1),
                "old.parquet",
                1,
                10,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(100),
        ],
    )
    .unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        FdbDataMutation {
            inline_file_deletions: vec![InlineFileDeletionRow::new(
                TableId(1),
                DataFileId(90),
                100,
                CatalogOrderId::uuid_v7(0),
            )],
            ..FdbDataMutation::default()
        },
    )
    .unwrap();
    let snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let prepared = prepare_inline_flushes(
        &kv,
        CatalogId(1),
        &[InlineTableFlush::file_deletions_only(
            TableId(1),
            snapshot.sequence,
        )],
    )
    .unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        FdbDataMutation::new(
            vec![
                DataFileRow::new(
                    DataFileId(91),
                    TableId(1),
                    "first.parquet",
                    1,
                    10,
                    snapshot.order,
                )
                .with_row_id_start(0),
            ],
            Vec::new(),
            vec![InlineTableFlush::new(
                TableId(1),
                SchemaId(1),
                snapshot.sequence,
            )],
            Vec::new(),
            Vec::new(),
        ),
    )
    .unwrap();
    let transaction = kv.create_transaction().unwrap();
    let result = stage_prepared_inline_flushes(&kv, &transaction, CatalogId(1), &prepared);
    assert!(
        matches!(result, Err(CatalogError::ConflictFenceChanged { .. })),
        "file-deletion visibility is table-wide, so a data flush must invalidate its preparation: {result:?}"
    );
}
