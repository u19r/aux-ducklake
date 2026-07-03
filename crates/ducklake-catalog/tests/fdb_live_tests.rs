#![cfg(feature = "foundationdb")]

use std::{
    sync::{Arc, Barrier},
    time::{SystemTime, UNIX_EPOCH},
};

use ducklake_catalog::{
    AppendCommitResult, CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind,
    ColumnCommentChange, ColumnDefaultChange, ColumnDrop, ColumnId, ColumnRename, CommitAttemptId,
    CommitAttemptRow, DataFileChange, DataFileChangeKind, DataFileId, DataFileRow, DeleteFileId,
    DeleteFileRow, DuckLakeSnapshotId, FdbOrderedCatalogKv, FilePartitionValueRow,
    INLINE_PAYLOAD_LIMIT_BYTES, InlineFileDeletionRow, InlineRowChangeKind, InlineTableFlush,
    InlineTablePayloadCommit, InlinedTableRow, KvBatch, MacroId, MacroImplementationRow, MacroRow,
    MergeAdjacentCompaction, MutableCatalogKv, OrderedCatalogKv, PartitionKeyIndex,
    RawSnapshotSequence, RewriteDeleteCompaction, SchemaId, SchemaRow, TableColumnRow,
    TableCommentChange, TableId, TablePartitionChange, TablePartitionFieldRow, TablePartitionRow,
    TableRename, TableRow, TableSortChange, TableSortFieldRow, TableSortRow, ViewRow,
    append_data_file, commit_append_table_columns, commit_change_table_column_defaults,
    commit_change_table_comments, commit_change_table_partition, commit_change_table_sort,
    commit_drop_table_columns, commit_rename_table_columns, commit_rename_tables, expire_data_file,
    expire_snapshots,
    keys::{
        conflict_fence_key, current_delete_file_key, current_schema_version_key, delete_file_key,
    },
    latest_snapshot, list_current_data_files,
    list_current_data_files_for_partition_scan_with_deletes, list_current_data_files_with_deletes,
    list_data_file_changes, list_data_file_changes_since_base, list_data_files_at,
    list_data_files_with_deletes_at, list_inline_file_deletions_at,
    list_inline_row_payload_changes, list_inline_table_payloads_at,
    list_known_files_for_orphan_cleanup, list_macros_at, list_old_data_files_for_cleanup,
    list_old_delete_files_for_cleanup, list_old_inline_table_payloads_for_cleanup, list_schemas_at,
    list_table_deletion_scan_files, list_tables_at, list_views_at, load_commit_attempt,
    load_macro_at, load_table_at, load_view_at, public_snapshot_sequence_for_order,
    remove_old_data_files, remove_old_delete_files, remove_old_inline_table_payloads,
    snapshot_by_public_sequence,
};

#[test]
fn fdb_live_catalog_initializes_appends_and_reverse_scans_latest_snapshot_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(61);
    let table = TableId(42);
    let prefix = format!(
        "aux-ducklake-test/{}/{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();

    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "fdb_mutation").unwrap();
    let [file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(1),
                table,
                "fdb-live.parquet",
                10,
                100,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();

    assert_eq!(initial.order.kind(), CatalogOrderKind::FdbVersionstamp);
    assert_eq!(
        file.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert!(file.validity.begin_order > initial.order);
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(latest.order, file.validity.begin_order);
    assert_eq!(latest.sequence, initial.sequence.next());
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        vec![file.clone()]
    );

    let table_changes = list_data_file_changes(
        &kv,
        catalog,
        table,
        initial.order,
        file.validity.begin_order,
    )
    .unwrap();
    assert_eq!(table_changes.len(), 1);
    assert_eq!(table_changes[0].kind, DataFileChangeKind::Added);
    assert_eq!(table_changes[0].data_file_id, file.data_file_id);
    assert_eq!(
        table_changes[0].order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(
        table_changes[0].order.as_bytes(),
        file.validity.begin_order.as_bytes()
    );

    let snapshot_changes =
        list_data_file_changes_since_base(&kv, catalog, initial.order, file.validity.begin_order)
            .unwrap();
    assert_eq!(snapshot_changes, table_changes);
}

#[test]
fn fdb_live_replace_table_drops_and_creates_in_one_snapshot_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(66);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("replace-table").into_bytes())
            .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let original = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                TableId(1),
                SchemaId(0),
                "old-table-uuid",
                "replace_me",
                "main/replace_me_old",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "old_col",
                    "INTEGER",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
            Some(RawSnapshotSequence(1)),
        )
        .unwrap();

    let [replacement] = kv
        .replace_tables_versionstamped(
            catalog,
            &[original.table_id],
            vec![TableRow::with_catalog_metadata(
                TableId(2),
                SchemaId(0),
                "new-table-uuid",
                "replace_me",
                "main/replace_me_new",
                vec![TableColumnRow::new(
                    ColumnId(2),
                    "new_col",
                    "DOUBLE",
                    true,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            )],
            Some(RawSnapshotSequence(2)),
        )
        .unwrap()
        .try_into()
        .unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current_tables = list_tables_at(&kv, catalog, latest.order).unwrap();

    assert_eq!(latest.sequence, RawSnapshotSequence(2));
    assert_eq!(replacement.table_id, TableId(2));
    assert_eq!(replacement.name, "replace_me");
    assert_eq!(replacement.validity.begin_order, latest.order);
    assert_eq!(
        load_table_at(&kv, catalog, original.table_id, latest.order).unwrap(),
        None
    );
    assert_eq!(current_tables, vec![replacement]);
}

#[test]
fn fdb_live_concurrent_appends_are_visible_exactly_once_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(62);
    let table = TableId(43);
    let kv = Arc::new(
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("concurrent").into_bytes())
            .unwrap(),
    );
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "fdb_mutation").unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let mut handles = Vec::new();
    for (file_id, path, row_count) in [
        (DataFileId(10), "fdb-concurrent-a.parquet", 11),
        (DataFileId(11), "fdb-concurrent-b.parquet", 22),
    ] {
        let writer_kv = Arc::clone(&kv);
        let writer_barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            writer_barrier.wait();
            let [file] = writer_kv
                .append_data_files_versionstamped(
                    catalog,
                    vec![DataFileRow::new(
                        file_id,
                        table,
                        path,
                        row_count,
                        row_count * 10,
                        initial.order,
                    )],
                )
                .unwrap()
                .try_into()
                .unwrap();
            file
        }));
    }

    barrier.wait();
    let mut committed_files = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    committed_files.sort_by_key(|file| file.data_file_id);

    let listed = list_current_data_files(kv.as_ref(), catalog, table).unwrap();
    assert_eq!(listed, committed_files);
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|file| {
        file.validity.begin_order.kind() == CatalogOrderKind::FdbVersionstamp
            && file.validity.begin_order > initial.order
    }));
    assert_ne!(
        listed[0].validity.begin_order,
        listed[1].validity.begin_order
    );
    assert_eq!(listed[0].data_file_id, DataFileId(10));
    assert_eq!(listed[0].path, "fdb-concurrent-a.parquet");
    assert_eq!(listed[0].record_count, 11);
    assert_eq!(listed[1].data_file_id, DataFileId(11));
    assert_eq!(listed[1].path, "fdb-concurrent-b.parquet");
    assert_eq!(listed[1].record_count, 22);

    let latest = latest_snapshot(kv.as_ref(), catalog).unwrap().unwrap();
    let max_file_order = listed
        .iter()
        .map(|file| file.validity.begin_order)
        .max()
        .unwrap();
    assert_eq!(latest.order, max_file_order);
}

#[test]
fn fdb_live_current_scan_paginates_past_first_range_batch_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(69);
    let table = TableId(49);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("range-pagination").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let rows = (1..=120)
        .map(|id| {
            DataFileRow::new(
                DataFileId(id),
                table,
                format!("fdb-range-{id}.parquet"),
                1,
                10,
                initial.order,
            )
        })
        .collect::<Vec<_>>();

    kv.append_data_files_versionstamped(catalog, rows).unwrap();

    let listed = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(listed.len(), 120);
    assert_eq!(
        listed.iter().map(|file| file.record_count).sum::<u64>(),
        120
    );
}

#[test]
fn fdb_live_table_scans_stay_bounded_to_requested_table_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(70);
    let left_table = TableId(50);
    let right_table = TableId(51);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("table-bounded-scan").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.create_schemas_versionstamped(
        catalog,
        vec![SchemaRow::new(
            SchemaId(1),
            "schema-1",
            "extra",
            "/tmp/extra/",
            initial.order,
        )],
        None,
    )
    .unwrap();
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            left_table,
            SchemaId(1),
            "left-uuid",
            "left_table",
            "/tmp/extra/left_table/",
            Vec::new(),
            initial.order,
        ),
        None,
    )
    .unwrap();
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            right_table,
            SchemaId(1),
            "right-uuid",
            "right_table",
            "/tmp/extra/right_table/",
            Vec::new(),
            initial.order,
        ),
        None,
    )
    .unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(501),
                left_table,
                "left-a.parquet",
                1,
                10,
                initial.order,
            ),
            DataFileRow::new(
                DataFileId(502),
                right_table,
                "right-a.parquet",
                2,
                20,
                initial.order,
            ),
            DataFileRow::new(
                DataFileId(503),
                left_table,
                "left-b.parquet",
                3,
                30,
                initial.order,
            ),
        ],
    )
    .unwrap();

    let schemas = list_schemas_at(
        &kv,
        catalog,
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
    )
    .unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].schema_id, SchemaId(1));

    let left_files = list_current_data_files(&kv, catalog, left_table).unwrap();
    assert_eq!(
        left_files
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(501), DataFileId(503)]
    );
    assert!(left_files.iter().all(|file| file.table_id == left_table));

    let right_files = list_current_data_files(&kv, catalog, right_table).unwrap();
    assert_eq!(right_files.len(), 1);
    assert_eq!(right_files[0].data_file_id, DataFileId(502));
    assert_eq!(right_files[0].table_id, right_table);
}

#[test]
fn fdb_live_implicit_main_schema_and_first_table_share_public_snapshot_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(71);
    let table = TableId(52);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("implicit-main-schema").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let schema = kv
        .create_schemas_versionstamped(
            catalog,
            vec![SchemaRow::new(
                SchemaId(0),
                "main-uuid",
                "main",
                "main/",
                initial.order,
            )],
            None,
        )
        .unwrap()
        .pop()
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table,
                SchemaId(0),
                "orders-uuid",
                "orders",
                "main/orders",
                Vec::new(),
                initial.order,
            ),
            None,
        )
        .unwrap();

    assert_eq!(
        public_snapshot_sequence_for_order(&kv, catalog, schema.validity.begin_order).unwrap(),
        Some(DuckLakeSnapshotId(1))
    );
    assert_eq!(
        public_snapshot_sequence_for_order(&kv, catalog, created.validity.begin_order).unwrap(),
        Some(DuckLakeSnapshotId(1))
    );
}

#[test]
fn fdb_live_delete_file_registration_attaches_current_and_historical_reads_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(66);
    let table = TableId(46);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("delete-file").into_bytes())
            .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(20),
                table,
                "fdb-delete-base.parquet",
                100,
                1_000,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let delete_file = kv
        .register_delete_file_versionstamped(
            catalog,
            DeleteFileRow::new(
                DeleteFileId(30),
                file.data_file_id,
                "fdb-delete-file.parquet",
                7,
                70,
                file.validity.begin_order,
            ),
        )
        .unwrap();

    assert_eq!(
        delete_file.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert!(delete_file.validity.begin_order > file.validity.begin_order);

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file, file);
    assert_eq!(current[0].delete_file, Some(delete_file.clone()));

    let before_delete =
        list_data_files_with_deletes_at(&kv, catalog, table, file.validity.begin_order).unwrap();
    let after_delete =
        list_data_files_with_deletes_at(&kv, catalog, table, delete_file.validity.begin_order)
            .unwrap();
    assert_eq!(before_delete.len(), 1);
    assert_eq!(before_delete[0].delete_file, None);
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].delete_file, Some(delete_file));
}

#[test]
fn fdb_live_file_cleanup_removes_only_expired_unreachable_metadata_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(83);
    let table = TableId(83);
    let mut kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("file-cleanup").into_bytes())
            .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [old_file, current_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![
                DataFileRow::new(
                    DataFileId(830),
                    table,
                    "cleanup-old.parquet",
                    10,
                    100,
                    initial.order,
                ),
                DataFileRow::new(
                    DataFileId(831),
                    table,
                    "cleanup-current.parquet",
                    20,
                    200,
                    initial.order,
                ),
            ],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.expire_data_file_versionstamped(catalog, old_file.data_file_id)
        .unwrap();
    let [newer_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(832),
                table,
                "cleanup-newer.parquet",
                30,
                300,
                current_file.validity.begin_order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    expire_snapshots(&mut kv, catalog, &[append_snapshot.sequence]).unwrap();
    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(old_data.len(), 1);
    assert_eq!(old_data[0].data_file_id, old_file.data_file_id);
    assert_eq!(
        remove_old_data_files(&mut kv, catalog, &[old_file.data_file_id])
            .unwrap()
            .len(),
        1
    );
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        vec![current_file.clone(), newer_file]
    );

    let first_delete = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(8300),
                current_file.data_file_id,
                "cleanup-delete-old.parquet",
                1,
                10,
                current_file.validity.begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
        .delete_files
        .into_iter()
        .next()
        .unwrap();
    let first_delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(8301),
            current_file.data_file_id,
            "cleanup-delete-current.parquet",
            1,
            10,
            current_file.validity.begin_order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    expire_snapshots(&mut kv, catalog, &[first_delete_snapshot.sequence]).unwrap();
    let old_deletes = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(old_deletes.len(), 1);
    assert_eq!(
        old_deletes[0].delete_file.delete_file_id,
        first_delete.delete_file_id
    );
    assert_eq!(
        remove_old_delete_files(&mut kv, catalog, &[first_delete.delete_file_id])
            .unwrap()
            .len(),
        1
    );
    assert!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    let known = list_known_files_for_orphan_cleanup(&kv, catalog).unwrap();
    assert!(known.iter().any(|row| matches!(
        row,
        ducklake_catalog::KnownCleanupFileRow::Data(data_file)
            if data_file.path == "cleanup-current.parquet"
    )));
    assert!(known.iter().any(|row| matches!(
        row,
        ducklake_catalog::KnownCleanupFileRow::Delete { delete_file, table_id }
            if delete_file.path == "cleanup-delete-current.parquet" && *table_id == table
    )));
}

#[test]
fn fdb_live_data_mutation_can_delete_file_appended_in_same_commit_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(91);
    let table = TableId(71);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("mutation-delete-new-file").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "mutation_delete_new_file").unwrap();
    let [existing] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(10),
                table,
                "mutation-existing.parquet",
                100,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();

    kv.commit_data_mutation_versionstamped(
        catalog,
        Some(CommitAttemptId(4001)),
        vec![DataFileRow::new(
            DataFileId(11),
            table,
            "mutation-new-file.parquet",
            100,
            1_024,
            existing.validity.begin_order,
        )],
        vec![
            DeleteFileRow::new(
                DeleteFileId(20),
                existing.data_file_id,
                "mutation-delete-existing.parquet",
                10,
                512,
                existing.validity.begin_order,
            ),
            DeleteFileRow::new(
                DeleteFileId(21),
                DataFileId(11),
                "mutation-delete-new-file.parquet",
                15,
                512,
                existing.validity.begin_order,
            ),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();

    assert_eq!(current.len(), 2);
    assert!(
        current.iter().all(|file| file.delete_file.is_some()),
        "storage should attach delete metadata for both committed and newly appended files"
    );
    assert!(current.iter().any(|file| {
        file.data_file.data_file_id == DataFileId(11)
            && file
                .delete_file
                .as_ref()
                .is_some_and(|delete| delete.delete_file_id == DeleteFileId(21))
    }));
}

#[test]
fn fdb_live_data_file_cleanup_rechecks_stale_candidate_before_removing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(75);
    let table = TableId(75);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("data-cleanup-race").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [old_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(750),
                table,
                "cleanup-race-old.parquet",
                10,
                100,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let old_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.expire_data_file_versionstamped(catalog, old_file.data_file_id)
        .unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![DataFileRow::new(
            DataFileId(751),
            table,
            "cleanup-race-current.parquet",
            10,
            100,
            old_snapshot.order,
        )],
    )
    .unwrap();
    expire_snapshots(&mut kv, catalog, &[old_snapshot.sequence]).unwrap();
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![old_file.data_file_id]
    );

    let [reused_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                old_file.data_file_id,
                table,
                "cleanup-race-reused.parquet",
                11,
                110,
                old_snapshot.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let removed = kv
        .remove_old_data_files_checked(catalog, &[old_file.data_file_id])
        .unwrap();

    assert!(removed.is_empty());
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .contains(&reused_file)
    );
}

#[test]
fn fdb_live_delete_file_cleanup_rechecks_stale_candidate_before_removing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(76);
    let table = TableId(76);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("delete-cleanup-race").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [data_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(760),
                table,
                "delete-cleanup-race-data.parquet",
                10,
                100,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let first_delete = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(7600),
                data_file.data_file_id,
                "delete-cleanup-race-old-delete.parquet",
                1,
                10,
                data_file.validity.begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
        .delete_files
        .into_iter()
        .next()
        .unwrap();
    let first_delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(7601),
            data_file.data_file_id,
            "delete-cleanup-race-newer-delete.parquet",
            1,
            10,
            first_delete.validity.begin_order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    expire_snapshots(&mut kv, catalog, &[first_delete_snapshot.sequence]).unwrap();
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .iter()
            .map(|row| row.delete_file.delete_file_id)
            .collect::<Vec<_>>(),
        vec![first_delete.delete_file_id]
    );

    let replacement = DeleteFileRow::new(
        first_delete.delete_file_id,
        data_file.data_file_id,
        "delete-cleanup-race-reused-delete.parquet",
        1,
        10,
        first_delete_snapshot.order,
    );
    let mut batch = KvBatch::new();
    batch.put(
        delete_file_key(catalog, replacement.delete_file_id),
        replacement.encode(),
    );
    batch.put(
        current_delete_file_key(catalog, replacement.data_file_id),
        replacement.delete_file_id.0.to_be_bytes().to_vec(),
    );
    kv.commit(batch).unwrap();
    let removed = kv
        .remove_old_delete_files_checked(catalog, &[first_delete.delete_file_id])
        .unwrap();

    assert!(removed.is_empty());
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(
        current[0].delete_file.as_ref().unwrap().path,
        "delete-cleanup-race-reused-delete.parquet"
    );
}

#[test]
fn fdb_live_create_table_uses_versionstamped_snapshot_and_table_visibility_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(68);
    let table = TableId(48);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("create-table").into_bytes())
            .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table,
                SchemaId(0),
                "fdb-table-uuid",
                "fdb_table",
                "main/fdb_table",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    true,
                    None,
                )],
                initial.order,
            ),
            None,
        )
        .unwrap();

    assert_eq!(
        created.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(
        kv.get(&current_schema_version_key(catalog)).unwrap(),
        Some(1_u64.to_be_bytes().to_vec())
    );
    assert!(created.validity.begin_order > initial.order);
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(latest.order, created.validity.begin_order);
    assert_eq!(latest.sequence, initial.sequence.next());
    assert_eq!(
        load_table_at(&kv, catalog, table, created.validity.begin_order).unwrap(),
        Some(created.clone())
    );
    assert_eq!(
        list_tables_at(&kv, catalog, created.validity.begin_order).unwrap(),
        vec![created]
    );
}

#[test]
fn fdb_live_inline_rows_attach_read_and_emit_insert_cdf_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(82);
    let table_id = TableId(82);
    let schema_id = SchemaId(7);
    let mut kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("inline-rows").into_bytes())
            .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "inline-table-uuid",
                "inline_source",
                "main/inline_source",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
            None,
        )
        .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let mut attached = created.clone();
    attached
        .inlined_data_tables
        .push(InlinedTableRow::new("inline_source_inlined", schema_id.0));
    let chunks = kv
        .register_inline_table_payload_with_table_versionstamped(
            catalog,
            attached,
            schema_id,
            b"row\t1\tone\nrow\t2\ttwo\n".to_vec(),
        )
        .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(chunks[0].validity.begin_order, inline_snapshot.order);
    assert_eq!(
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, create_snapshot.order)
            .unwrap(),
        Vec::new()
    );
    let payloads =
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, inline_snapshot.order)
            .unwrap();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].begin_order, inline_snapshot.order);
    assert_eq!(payloads[0].payload, b"row\t1\tone\nrow\t2\ttwo\n");

    let current_table = load_table_at(&kv, catalog, table_id, inline_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(current_table.inlined_data_tables.len(), 1);
    assert_eq!(
        current_table.inlined_data_tables[0].table_name,
        "inline_source_inlined"
    );
    let historical_table = load_table_at(&kv, catalog, table_id, create_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(historical_table.inlined_data_tables.is_empty());

    let changes = list_inline_row_payload_changes(
        &kv,
        catalog,
        table_id,
        schema_id,
        created.validity.begin_order,
        inline_snapshot.order,
        InlineRowChangeKind::Inserted,
    )
    .unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].payload, b"row\t1\tone\n");
    assert_eq!(changes[1].payload, b"row\t2\ttwo\n");

    let delete = kv
        .commit_delete_inline_table_rows_versionstamped(catalog, table_id, schema_id, &[1], None)
        .unwrap();
    assert_eq!(delete.deleted_row_count, 1);
    assert_eq!(delete.rewritten_payload_count, 1);
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let historical_after_delete =
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, inline_snapshot.order)
            .unwrap();
    assert_eq!(historical_after_delete.len(), 1);
    assert_eq!(
        historical_after_delete[0].payload,
        b"row\t1\tone\nrow\t2\ttwo\n"
    );
    let latest_after_delete =
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, delete_snapshot.order)
            .unwrap();
    assert_eq!(latest_after_delete.len(), 1);
    assert_eq!(latest_after_delete[0].payload, b"row\t2\ttwo\n");
    let delete_changes = list_inline_row_payload_changes(
        &kv,
        catalog,
        table_id,
        schema_id,
        inline_snapshot.order,
        delete_snapshot.order,
        InlineRowChangeKind::Deleted,
    )
    .unwrap();
    assert_eq!(delete_changes.len(), 1);
    assert_eq!(delete_changes[0].payload, b"row\t1\tone\n");

    let flush = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![DataFileRow::new(
                DataFileId(820),
                table_id,
                "inline-flush.parquet",
                1,
                10,
                delete_snapshot.order,
            )],
            Vec::new(),
            vec![ducklake_catalog::InlineTableFlush::new(
                table_id,
                schema_id,
                delete_snapshot.sequence,
            )],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    assert_eq!(flush.flushed_inline_count, 1);
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, delete_snapshot.order)
            .unwrap()[0]
            .payload,
        b"row\t2\ttwo\n"
    );
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, flush_snapshot.order)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table_id)
            .unwrap()
            .last()
            .unwrap()
            .data_file_id,
        DataFileId(820)
    );

    expire_snapshots(
        &mut kv,
        catalog,
        &[inline_snapshot.sequence, delete_snapshot.sequence],
    )
    .unwrap();
    let cleanup = list_old_inline_table_payloads_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(cleanup.len(), 2);
    let cleanup_ids = cleanup.iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(
        remove_old_inline_table_payloads(&mut kv, catalog, &cleanup_ids,)
            .unwrap()
            .len(),
        2
    );
    assert!(
        list_old_inline_table_payloads_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table_id)
            .unwrap()
            .last()
            .unwrap()
            .data_file_id,
        DataFileId(820)
    );
}

#[test]
fn fdb_live_given_inline_rows_already_flushed_when_stale_flush_replays_then_no_duplicate_file_is_published()
 {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(83);
    let table_id = TableId(83);
    let schema_id = SchemaId(8);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-flush-stale-replay").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "inline-stale-replay-uuid",
                "inline_stale_replay",
                "main/inline_stale_replay",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "id",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::uuid_v7(0),
            ),
            None,
        )
        .unwrap();
    let mut attached = created.clone();
    attached.inlined_data_tables.push(InlinedTableRow::new(
        "inline_stale_replay_inlined",
        schema_id.0,
    ));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached,
        schema_id,
        b"row\t1\tone\nrow\t2\ttwo\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let first_flush = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![DataFileRow::new(
                DataFileId(831),
                table_id,
                "first-flush.parquet",
                2,
                128,
                inline_snapshot.order,
            )],
            Vec::new(),
            vec![InlineTableFlush::new(
                table_id,
                schema_id,
                inline_snapshot.sequence,
            )],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    assert_eq!(first_flush.flushed_inline_count, 1);
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table_id, schema_id, flush_snapshot.order)
            .unwrap()
            .is_empty()
    );

    let stale_replay = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![DataFileRow::new(
                DataFileId(832),
                table_id,
                "stale-replay.parquet",
                2,
                128,
                inline_snapshot.order,
            )],
            Vec::new(),
            vec![InlineTableFlush::new(
                table_id,
                schema_id,
                flush_snapshot.sequence,
            )],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

    assert_eq!(
        stale_replay,
        ducklake_catalog::DataMutationCommit::default()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table_id)
            .unwrap()
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(831)]
    );
}

#[test]
fn fdb_live_given_inline_file_deletions_without_inline_rows_when_flushing_then_file_is_published() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(84);
    let table_id = TableId(84);
    let schema_id = SchemaId(8);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-file-delete-flush-file").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table_id, "inline_file_delete_flush").unwrap();
    let [source] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(841),
                table_id,
                "source.parquet",
                100,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    kv.commit_data_mutation_versionstamped_with_inline_file_deletions(
        catalog,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![
            InlineFileDeletionRow::new(
                table_id,
                source.data_file_id,
                1,
                source.validity.begin_order,
            ),
            InlineFileDeletionRow::new(
                table_id,
                source.data_file_id,
                2,
                source.validity.begin_order,
            ),
        ],
        Vec::new(),
    )
    .unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let flush = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![DataFileRow::new(
                DataFileId(842),
                table_id,
                "delete-position-flush.parquet",
                98,
                2_048,
                delete_snapshot.order,
            )],
            Vec::new(),
            vec![InlineTableFlush::new(
                table_id,
                schema_id,
                delete_snapshot.sequence,
            )],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(flush.flushed_inline_count, 1);
    assert_eq!(
        list_current_data_files(&kv, catalog, table_id)
            .unwrap()
            .iter()
            .map(|row| row.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(841), DataFileId(842)]
    );
    assert!(
        list_inline_file_deletions_at(&kv, catalog, table_id, flush_snapshot.order)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn fdb_live_oversized_inline_payload_routes_to_data_file_without_inline_chunks_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(92);
    let table = TableId(92);
    let schema = SchemaId(9);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-fallback").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "inline-fallback-table-uuid",
            "inline_fallback",
            "main/inline_fallback",
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                false,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        ),
        None,
    )
    .unwrap();

    let fallback = DataFileRow::new(
        DataFileId(920),
        table,
        "main/inline-fallback.parquet",
        1,
        4096,
        CatalogOrderId::uuid_v7(0),
    );
    let payload = oversized_inline_row_payload();
    let result = kv
        .route_inline_table_payload_or_data_file_versionstamped(
            catalog, table, schema, payload, fallback,
        )
        .unwrap();

    let InlineTablePayloadCommit::FileBacked(files) = result else {
        panic!("oversized FDB inline payload should route to file-backed path");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].data_file_id, DataFileId(920));
    assert_eq!(list_current_data_files(&kv, catalog, table).unwrap(), files);
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(latest.order, files[0].validity.begin_order);
    assert!(
        list_inline_table_payloads_at(&kv, catalog, table, schema, latest.order)
            .unwrap()
            .is_empty()
    );
}

fn oversized_inline_row_payload() -> Vec<u8> {
    let mut payload = b"row\t1\ts:".to_vec();
    payload.extend(std::iter::repeat_n(b'x', INLINE_PAYLOAD_LIMIT_BYTES));
    payload.push(b'\n');
    payload
}

#[test]
fn fdb_live_table_metadata_replacements_preserve_current_and_history_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(80);
    let table = TableId(80);
    let mut kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("table-ddl").into_bytes())
            .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table,
                SchemaId(0),
                "table-ddl-uuid",
                "orders",
                "main/orders",
                vec![
                    TableColumnRow::new(ColumnId(1), "id", "BIGINT", false, None),
                    TableColumnRow::new(ColumnId(2), "amount", "INTEGER", true, None),
                ],
                CatalogOrderId::uuid_v7(0),
            ),
            None,
        )
        .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(created.validity.begin_order, create_snapshot.order);

    commit_rename_tables(
        &mut kv,
        catalog,
        &[TableRename::new(table, "orders_archive")],
    )
    .unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            table,
            TableColumnRow::new(ColumnId(2), "total", "INTEGER", true, None),
        )],
    )
    .unwrap();
    commit_change_table_column_defaults(
        &mut kv,
        catalog,
        &[ColumnDefaultChange::new(
            table,
            TableColumnRow::new(ColumnId(2), "total", "INTEGER", true, None).with_default_metadata(
                None::<String>,
                Some("0"),
                "literal",
            ),
        )],
    )
    .unwrap();
    commit_change_table_comments(
        &mut kv,
        catalog,
        &[TableCommentChange::new(table, Some("archived orders"))],
        &[ColumnCommentChange::new(
            table,
            ColumnId(2),
            Some("gross total"),
        )],
    )
    .unwrap();
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                1,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap();
    commit_change_table_sort(
        &mut kv,
        catalog,
        &TableSortChange::new(
            table,
            Some(TableSortRow::new(
                1,
                vec![TableSortFieldRow::new(
                    0,
                    "id",
                    "duckdb",
                    "ASC",
                    "NULLS FIRST",
                )],
            )),
        ),
    )
    .unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current = load_table_at(&kv, catalog, table, latest.order)
        .unwrap()
        .unwrap();
    assert_eq!(current.name, "orders_archive");
    assert_eq!(current.comment.as_deref(), Some("archived orders"));
    assert_eq!(current.columns[1].name, "total");
    assert_eq!(current.columns[1].default_value.as_deref(), Some("0"));
    assert_eq!(current.columns[1].comment.as_deref(), Some("gross total"));
    assert!(current.partition.is_some());
    assert!(current.sort.is_some());
    assert_eq!(
        list_tables_at(&kv, catalog, latest.order).unwrap(),
        vec![current]
    );

    let historical = load_table_at(&kv, catalog, table, create_snapshot.order)
        .unwrap()
        .unwrap();
    assert_eq!(historical.name, "orders");
    assert_eq!(historical.columns[1].name, "amount");
    assert_eq!(historical.columns[1].default_value, None);
    assert_eq!(historical.partition, None);
    assert_eq!(historical.sort, None);

    let nested_table = TableId(81);
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            nested_table,
            SchemaId(0),
            "nested-ddl-uuid",
            "nested_items",
            "main/nested_items",
            vec![
                TableColumnRow::new(
                    ColumnId(10),
                    "payload",
                    "STRUCT(a INTEGER, b INTEGER)",
                    true,
                    None,
                ),
                TableColumnRow::new(ColumnId(11), "a", "INTEGER", true, Some(ColumnId(10))),
                TableColumnRow::new(ColumnId(12), "b", "INTEGER", true, Some(ColumnId(10))),
            ],
            CatalogOrderId::uuid_v7(0),
        ),
        None,
    )
    .unwrap();
    let nested_create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_rename_table_columns(
        &mut kv,
        catalog,
        &[ColumnRename::new(
            nested_table,
            TableColumnRow::new(ColumnId(12), "c", "INTEGER", true, Some(ColumnId(10))),
        )],
    )
    .unwrap();
    commit_drop_table_columns(
        &mut kv,
        catalog,
        &[ColumnDrop::new(nested_table, ColumnId(11))],
    )
    .unwrap();

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let nested_current = load_table_at(&kv, catalog, nested_table, latest.order)
        .unwrap()
        .unwrap();
    assert!(nested_current.columns.iter().any(|column| {
        column.column_id == ColumnId(12)
            && column.name == "c"
            && column.parent_id == Some(ColumnId(10))
    }));
    assert!(
        !nested_current
            .columns
            .iter()
            .any(|column| column.column_id == ColumnId(11))
    );
    let nested_historical = load_table_at(&kv, catalog, nested_table, nested_create_snapshot.order)
        .unwrap()
        .unwrap();
    assert!(nested_historical.columns.iter().any(|column| {
        column.column_id == ColumnId(11)
            && column.name == "a"
            && column.parent_id == Some(ColumnId(10))
    }));
    assert!(nested_historical.columns.iter().any(|column| {
        column.column_id == ColumnId(12)
            && column.name == "b"
            && column.parent_id == Some(ColumnId(10))
    }));
}

#[test]
fn fdb_live_views_and_macros_use_versionstamped_object_history_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(81);
    let view = TableId(81);
    let macro_id = MacroId(81);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("views-macros").into_bytes())
            .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();

    let created_view = kv
        .create_view_versionstamped(
            catalog,
            ViewRow::new(
                view,
                SchemaId(0),
                "view-ddl-uuid",
                "orders_view",
                "duckdb",
                "SELECT 1 AS id",
                vec!["id".to_owned()],
                CatalogOrderId::uuid_v7(0),
            )
            .with_comment(Some("initial view")),
        )
        .unwrap();
    let create_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(created_view.validity.begin_order, create_snapshot.order);

    kv.change_view_comment_versionstamped(
        catalog,
        &ducklake_catalog::ViewCommentChange::new(view, Some("renamed view")),
    )
    .unwrap();
    kv.rename_views_versionstamped(
        catalog,
        &[ducklake_catalog::ViewRename::new(view, "orders_view_v2")],
    )
    .unwrap();
    let latest_view = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let current_view = load_view_at(&kv, catalog, view, latest_view.order)
        .unwrap()
        .unwrap();
    assert_eq!(current_view.name, "orders_view_v2");
    assert_eq!(current_view.comment.as_deref(), Some("renamed view"));
    assert_eq!(
        list_views_at(&kv, catalog, latest_view.order).unwrap(),
        vec![current_view]
    );
    assert_eq!(
        load_view_at(&kv, catalog, view, create_snapshot.order)
            .unwrap()
            .unwrap()
            .name,
        "orders_view"
    );
    kv.drop_views_versionstamped(catalog, &[view]).unwrap();
    let after_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        load_view_at(&kv, catalog, view, after_drop.order).unwrap(),
        None
    );

    kv.create_macros_versionstamped(
        catalog,
        vec![MacroRow::new(
            macro_id,
            SchemaId(0),
            "plus_one",
            vec![MacroImplementationRow {
                dialect: "duckdb".to_owned(),
                sql: "x + 1".to_owned(),
                macro_type: "scalar".to_owned(),
                parameters: Vec::new(),
            }],
            CatalogOrderId::uuid_v7(0),
        )],
        None,
    )
    .unwrap();
    let macro_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        list_macros_at(&kv, catalog, macro_snapshot.order)
            .unwrap()
            .len(),
        1
    );
    kv.drop_macros_versionstamped(catalog, &[macro_id], None)
        .unwrap();
    let after_macro_drop = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        load_macro_at(&kv, catalog, macro_id, after_macro_drop.order).unwrap(),
        None
    );
    assert!(
        load_macro_at(&kv, catalog, macro_id, macro_snapshot.order)
            .unwrap()
            .is_some()
    );
}

#[test]
fn fdb_live_generated_order_id_fails_closed_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("generated-order").into_bytes(),
    )
    .unwrap();

    let error = kv.generated_order_id().unwrap_err();

    assert!(error.to_string().contains("post-commit versionstamps"));
}

#[test]
fn fdb_live_expire_data_file_updates_current_historical_and_change_feed_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(67);
    let table = TableId(47);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("expire-file").into_bytes())
            .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(40),
                table,
                "fdb-expire-base.parquet",
                55,
                550,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let expired = kv
        .expire_data_file_versionstamped(catalog, file.data_file_id)
        .unwrap();
    let expire_order = expired.validity.end_order.unwrap();

    assert_eq!(expire_order.kind(), CatalogOrderKind::FdbVersionstamp);
    assert!(expire_order > file.validity.begin_order);
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, file.validity.begin_order).unwrap(),
        vec![expired.clone()]
    );
    assert!(
        list_data_files_at(&kv, catalog, table, expire_order)
            .unwrap()
            .is_empty()
    );

    let table_changes =
        list_data_file_changes(&kv, catalog, table, initial.order, expire_order).unwrap();
    assert_eq!(table_changes.len(), 2);
    assert_eq!(table_changes[0].kind, DataFileChangeKind::Added);
    assert_eq!(table_changes[1].kind, DataFileChangeKind::Removed);
    assert_eq!(
        table_changes[1].order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(table_changes[1].order.as_bytes(), expire_order.as_bytes());

    let snapshot_changes =
        list_data_file_changes_since_base(&kv, catalog, initial.order, expire_order).unwrap();
    assert_eq!(snapshot_changes, table_changes);
}

#[test]
fn fdb_live_data_mutation_appends_and_registers_delete_files_atomically_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(68);
    let table = TableId(48);
    let kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("data-mutation").into_bytes())
            .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "fdb_data_mutation").unwrap();
    let [base_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(50),
                table,
                "fdb-mutation-base.parquet",
                100,
                1000,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();

    let committed = kv
        .commit_data_mutation_versionstamped(
            catalog,
            Some(CommitAttemptId(7001)),
            vec![DataFileRow::new(
                DataFileId(51),
                table,
                "fdb-mutation-replacement.parquet",
                70,
                700,
                initial.order,
            )],
            vec![DeleteFileRow::new(
                DeleteFileId(60),
                base_file.data_file_id,
                "fdb-mutation-delete.parquet",
                30,
                300,
                initial.order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

    let [replacement_file] = committed.data_files.try_into().unwrap();
    let [delete_file] = committed.delete_files.try_into().unwrap();
    assert_eq!(
        replacement_file.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    assert_eq!(
        replacement_file.validity.begin_order,
        delete_file.validity.begin_order
    );
    assert!(replacement_file.validity.begin_order > base_file.validity.begin_order);
    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert_eq!(current[0].data_file, base_file);
    assert_eq!(current[0].delete_file, Some(delete_file.clone()));
    assert_eq!(current[1].data_file, replacement_file.clone());
    assert_eq!(current[1].delete_file, None);

    let historical =
        list_data_files_with_deletes_at(&kv, catalog, table, base_file.validity.begin_order)
            .unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].data_file.data_file_id, DataFileId(50));
    assert_eq!(historical[0].delete_file, None);

    let changes = list_data_file_changes(
        &kv,
        catalog,
        table,
        base_file.validity.begin_order,
        replacement_file.validity.begin_order,
    )
    .unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[1].kind, DataFileChangeKind::Added);
    assert_eq!(changes[1].data_file_id, replacement_file.data_file_id);

    let deletion_scans = list_table_deletion_scan_files(
        &kv,
        catalog,
        table,
        base_file.validity.begin_order,
        delete_file.validity.begin_order,
    )
    .unwrap();
    assert_eq!(deletion_scans.len(), 1);
    assert_eq!(
        deletion_scans[0].data_file.data_file_id,
        base_file.data_file_id
    );
    assert_eq!(
        deletion_scans[0]
            .delete_file
            .as_ref()
            .unwrap()
            .delete_file_id,
        delete_file.delete_file_id
    );

    assert_eq!(
        kv.commit_data_mutation_versionstamped(
            catalog,
            Some(CommitAttemptId(7001)),
            vec![DataFileRow::new(
                DataFileId(51),
                table,
                "fdb-mutation-replacement.parquet",
                70,
                700,
                initial.order,
            )],
            vec![DeleteFileRow::new(
                DeleteFileId(60),
                base_file.data_file_id,
                "fdb-mutation-delete.parquet",
                30,
                300,
                initial.order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap(),
        ducklake_catalog::DataMutationCommit::default()
    );
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, table).unwrap(),
        current
    );
}

#[test]
fn fdb_live_data_mutation_attempt_recovery_survives_catalog_reopen_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(78);
    let table = TableId(58);
    let attempt = CommitAttemptId(7801);
    let prefix = unique_prefix("data-mutation-reopen").into_bytes();
    let committed_order;
    let committed_file;
    let requested_begin_order;

    {
        let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.clone()).unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        let initial = create_current_table(&kv, catalog, table, "reopen_recovery").unwrap();
        requested_begin_order = initial.order;

        let committed = kv
            .commit_data_mutation_versionstamped(
                catalog,
                Some(attempt),
                vec![DataFileRow::new(
                    DataFileId(780),
                    table,
                    "reopen-recovery.parquet",
                    78,
                    780,
                    initial.order,
                )],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
        let [file] = committed.data_files.try_into().unwrap();
        committed_order = file.validity.begin_order;
        committed_file = file;
        assert_eq!(committed_order.kind(), CatalogOrderKind::FdbVersionstamp);
    }

    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix).unwrap();
    assert_eq!(
        kv.commit_data_mutation_versionstamped(
            catalog,
            Some(attempt),
            vec![DataFileRow::new(
                DataFileId(780),
                table,
                "reopen-recovery.parquet",
                78,
                780,
                requested_begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap(),
        ducklake_catalog::DataMutationCommit::default()
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        vec![committed_file]
    );
}

#[test]
fn fdb_live_inline_delete_replaced_by_delete_file_keeps_next_public_snapshot_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(1);
    let table = TableId(56);
    let data_file = DataFileId(171);
    let prefix = unique_prefix("inline-delete-public-snapshot");
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "inline_delete_public_snapshot").unwrap();
    let [file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![
                DataFileRow::new(
                    data_file,
                    table,
                    "inline-delete-public-snapshot.parquet",
                    100,
                    1024,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let append_public = public_snapshot_sequence_for_order(&kv, catalog, file.validity.begin_order)
        .unwrap()
        .unwrap()
        .0;

    kv.commit_data_mutation_versionstamped_with_inline_file_deletions(
        catalog,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![InlineFileDeletionRow::new(
            table,
            data_file,
            0,
            CatalogOrderId::uuid_v7(0),
        )],
        Vec::new(),
    )
    .unwrap();
    let inline_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(1),
            data_file,
            "inline-delete-public-snapshot-delete.parquet",
            2,
            512,
            CatalogOrderId::uuid_v7(0),
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let physical_delete = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_public_snapshot_order(&kv, catalog, append_public + 1, inline_delete.order);
    assert_public_snapshot_order(&kv, catalog, append_public + 2, physical_delete.order);
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, physical_delete.order).unwrap(),
        std::collections::BTreeMap::from([(data_file, std::collections::BTreeSet::from([0]))])
    );
}

#[test]
fn fdb_live_conflict_fence_rejects_stale_commit_without_publishing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(63);
    let fence = conflict_fence_key(catalog, b"table/42/data");
    let mut kv =
        FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix("conflict-fence").into_bytes())
            .unwrap();

    let stale_observation = kv.read_conflict_fence(&fence).unwrap();
    let mut first = KvBatch::new();
    first.write_conflict_fence(fence.clone());
    kv.commit(first).unwrap();

    let mut stale = KvBatch::new();
    stale.check_value(fence.clone(), stale_observation);
    stale.put(b"unpublished".to_vec(), b"value".to_vec());
    stale.write_conflict_fence(fence);
    let error = kv.commit(stale).unwrap_err();

    assert!(error.to_string().contains("conflict fence changed"));
    assert_eq!(kv.get(b"unpublished").unwrap(), None);
}

#[test]
fn fdb_live_append_retry_is_idempotent_without_duplicate_publish_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(64);
    let table = TableId(44);
    let base = CatalogOrderId::uuid_v7(10);
    let concurrent = CatalogOrderId::uuid_v7(20);
    let attempt = CommitAttemptId(123);
    let prefix = unique_prefix("idempotent-retry").into_bytes();
    let committed_order;

    {
        let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.clone()).unwrap();
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
                "compatible-concurrent.parquet",
                20,
                200,
                concurrent,
            ),
        )
        .unwrap();
        let AppendCommitResult::Committed(committed) = kv
            .commit_append_data_file_versionstamped(
                catalog,
                attempt,
                base,
                concurrent,
                DataFileRow::new(DataFileId(3), table, "retry-append.parquet", 30, 300, base),
            )
            .unwrap()
        else {
            panic!("first FDB append commit should publish");
        };
        committed_order = committed.validity.begin_order;
        assert_eq!(committed_order.kind(), CatalogOrderKind::FdbVersionstamp);
    }

    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix).unwrap();
    assert_eq!(
        kv.commit_append_data_file_versionstamped(
            catalog,
            attempt,
            base,
            concurrent,
            DataFileRow::new(DataFileId(3), table, "retry-append.parquet", 30, 300, base,),
        )
        .unwrap(),
        AppendCommitResult::AlreadyCommitted {
            commit_order: committed_order
        }
    );
    assert_eq!(
        load_commit_attempt(&kv, catalog, attempt).unwrap(),
        Some(CommitAttemptRow::new(attempt, committed_order))
    );
    let files = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(
        files
            .iter()
            .filter(|file| file.data_file_id == DataFileId(3))
            .count(),
        1
    );
}

#[test]
fn fdb_live_append_after_concurrent_schema_change_conflicts_without_publishing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(66);
    let table = TableId(46);
    let attempt = CommitAttemptId(789);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("schema-conflict").into_bytes(),
    )
    .unwrap();

    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "orders-table-uuid",
            "orders",
            "main/orders",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
        None,
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_table_columns(
        &mut kv,
        catalog,
        table,
        vec![TableColumnRow::new(
            ColumnId(2),
            "amount",
            "int32",
            true,
            None,
        )],
    )
    .unwrap();
    let schema_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = kv
        .commit_append_data_file_versionstamped(
            catalog,
            attempt,
            base_snapshot.order,
            schema_snapshot.order,
            DataFileRow::new(
                DataFileId(77),
                table,
                "schema-conflict-should-not-publish.parquet",
                1,
                256,
                base_snapshot.order,
            ),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableSchemaConflict {
            table_id,
            changed_at
        } if table_id == table && changed_at == schema_snapshot.order
    ));
    assert_eq!(load_commit_attempt(&kv, catalog, attempt).unwrap(), None);
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn fdb_live_append_after_table_drop_conflicts_without_publishing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(67);
    let table = TableId(47);
    let attempt = CommitAttemptId(790);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("table-drop-conflict").into_bytes(),
    )
    .unwrap();

    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            "drop-table-uuid",
            "orders_to_drop",
            "main/orders_to_drop",
            vec![TableColumnRow::new(ColumnId(1), "id", "int32", true, None)],
            CatalogOrderId::uuid_v7(0),
        ),
        None,
    )
    .unwrap();
    let [data_file] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(90),
                table,
                "drop-conflict-current.parquet",
                10,
                512,
                CatalogOrderId::uuid_v7(0),
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    kv.register_delete_file_versionstamped(
        catalog,
        DeleteFileRow::new(
            DeleteFileId(91),
            data_file.data_file_id,
            "drop-conflict-delete.parquet",
            1,
            128,
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let dropped = kv.drop_tables_versionstamped(catalog, &[table]).unwrap();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].table.table_id, table);
    assert_eq!(dropped[0].expired_data_file_count, 1);
    assert_eq!(dropped[0].expired_delete_file_count, 1);
    let drop_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        dropped[0].table.validity.end_order,
        Some(drop_snapshot.order)
    );
    assert_eq!(
        load_table_at(&kv, catalog, table, drop_snapshot.order)
            .unwrap()
            .map(|row| row.table_id),
        None
    );
    assert!(
        list_current_data_files_with_deletes(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );

    let error = kv
        .commit_append_data_file_versionstamped(
            catalog,
            attempt,
            base_snapshot.order,
            drop_snapshot.order,
            DataFileRow::new(
                DataFileId(92),
                table,
                "drop-conflict-should-not-publish.parquet",
                1,
                256,
                base_snapshot.order,
            ),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        CatalogError::TableLogicalConflict {
            table_id,
            dropped_at
        } if table_id == table && dropped_at == drop_snapshot.order
    ));
    assert_eq!(load_commit_attempt(&kv, catalog, attempt).unwrap(), None);
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn fdb_live_merge_adjacent_compaction_replaces_source_files_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(68);
    let table = TableId(48);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("merge-compaction").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "merge_compaction").unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(100),
                table,
                "merge-source-a.parquet",
                5,
                512,
                initial.order,
            )
            .with_row_id_start(0),
            DataFileRow::new(
                DataFileId(101),
                table,
                "merge-source-b.parquet",
                7,
                768,
                initial.order,
            )
            .with_row_id_start(5),
        ],
    )
    .unwrap();
    let append_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let committed = kv
        .commit_merge_adjacent_data_files_versionstamped(
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(100), DataFileId(101)],
                new_files: vec![DataFileRow::new(
                    DataFileId(102),
                    table,
                    "merge-replacement.parquet",
                    12,
                    1_280,
                    CatalogOrderId::uuid_v7(0),
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

    assert_eq!(committed.new_files.len(), 1);
    assert_eq!(
        committed.new_files[0].validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file_id, DataFileId(102));
    assert_eq!(current[0].path, "merge-replacement.parquet");
    let cleanup = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(
        cleanup
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(100), DataFileId(101)]
    );
    let append_files = list_data_files_at(&kv, catalog, table, append_snapshot.order).unwrap();
    assert_eq!(
        append_files
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(100), DataFileId(101)]
    );
}

#[test]
fn fdb_live_partitioned_merge_replacement_survives_cleanup_for_time_travel_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(83);
    let table = TableId(83);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("partitioned-merge-cleanup").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "partitioned_merge_cleanup").unwrap();
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                2,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap();

    let mut committed = Vec::new();
    let mut first_partition_second_snapshot = None;
    for (file_id, path, row_id_start, partition_value) in [
        (DataFileId(1), "part-1-a.parquet", 0, "1"),
        (DataFileId(2), "part-1-b.parquet", 1, "1"),
        (DataFileId(3), "part-2-a.parquet", 2, "2"),
        (DataFileId(4), "part-2-b.parquet", 3, "2"),
    ] {
        let file = kv
            .commit_data_mutation_versionstamped(
                catalog,
                None,
                vec![
                    DataFileRow::new(file_id, table, path, 1, 64, CatalogOrderId::uuid_v7(0))
                        .with_row_id_start(row_id_start),
                ],
                Vec::new(),
                Vec::new(),
                vec![FilePartitionValueRow::new(
                    file_id,
                    table,
                    PartitionKeyIndex(0),
                    partition_value,
                )],
                Vec::new(),
            )
            .unwrap()
            .data_files
            .into_iter()
            .next()
            .unwrap();
        committed.push(file);
        if file_id == DataFileId(2) {
            first_partition_second_snapshot = latest_snapshot(&kv, catalog).unwrap();
        }
    }
    let first_partition_second_snapshot = first_partition_second_snapshot.unwrap();

    let partition_one_before_compaction = list_current_data_files_for_partition_scan_with_deletes(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "1",
    )
    .unwrap();
    assert_eq!(
        partition_one_before_compaction
            .iter()
            .map(|file| file.data_file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(1), DataFileId(2)]
    );

    let committed_compaction = kv
        .commit_merge_adjacent_data_files_versionstamped(
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(1), DataFileId(2)],
                new_files: vec![DataFileRow::new(
                    DataFileId(5),
                    table,
                    "part-1-merged.parquet",
                    2,
                    128,
                    CatalogOrderId::uuid_v7(0),
                )],
                partition_values: vec![FilePartitionValueRow::new(
                    DataFileId(5),
                    table,
                    PartitionKeyIndex(0),
                    "1",
                )],
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();
    let replacement = committed_compaction.new_files.into_iter().next().unwrap();
    assert_eq!(replacement.row_id_start, 0);
    assert!(replacement.row_id_start_known);
    assert_eq!(
        replacement.validity.begin_order,
        committed[0].validity.begin_order
    );
    assert_eq!(
        replacement.max_partial_order,
        Some(committed[1].validity.begin_order)
    );

    kv.remove_old_data_files_checked(catalog, &[DataFileId(1), DataFileId(2)])
        .unwrap();

    let files_at_second_insert =
        list_data_files_at(&kv, catalog, table, first_partition_second_snapshot.order).unwrap();
    assert_eq!(
        files_at_second_insert
            .iter()
            .map(|file| (file.data_file_id, file.row_id_start, file.record_count))
            .collect::<Vec<_>>(),
        vec![(DataFileId(5), 0, 2)]
    );
    let partition_one_after_cleanup = list_current_data_files_for_partition_scan_with_deletes(
        &kv,
        catalog,
        table,
        PartitionKeyIndex(0),
        "1",
    )
    .unwrap();
    assert_eq!(
        partition_one_after_cleanup
            .iter()
            .map(|file| file.data_file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(5)]
    );
}

#[test]
fn fdb_live_merge_adjacent_sparse_replacement_can_overlap_unrelated_current_files_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(84);
    let table = TableId(84);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("sparse-merge-rowids").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "sparse_merge_rowids").unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "small-a.parquet",
                1,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
            DataFileRow::new(
                DataFileId(2),
                table,
                "large-a.parquet",
                10_000,
                40_000,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(1),
            DataFileRow::new(
                DataFileId(3),
                table,
                "small-b.parquet",
                1,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10_001),
            DataFileRow::new(
                DataFileId(4),
                table,
                "small-c.parquet",
                1,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10_002),
            DataFileRow::new(
                DataFileId(5),
                table,
                "large-b.parquet",
                10_000,
                40_000,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10_003),
            DataFileRow::new(
                DataFileId(6),
                table,
                "small-d.parquet",
                1,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(20_003),
            DataFileRow::new(
                DataFileId(7),
                table,
                "small-e.parquet",
                1,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(20_004),
        ],
    )
    .unwrap();

    kv.commit_merge_adjacent_data_files_versionstamped(
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![
                DataFileId(1),
                DataFileId(3),
                DataFileId(4),
                DataFileId(6),
                DataFileId(7),
            ],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(8),
                    table,
                    "small-merged.parquet",
                    5,
                    320,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(2), DataFileId(5), DataFileId(8)]
    );
    let replacement = current
        .iter()
        .find(|file| file.data_file_id == DataFileId(8))
        .unwrap();
    assert!(!replacement.row_id_start_known);
}

#[test]
fn fdb_live_partitioned_multi_output_merge_derives_row_ids_by_partition_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(85);
    let table = TableId(85);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("partitioned-multi-output").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "partitioned_multi_output").unwrap();
    commit_change_table_partition(
        &mut kv,
        catalog,
        &TablePartitionChange::new(
            table,
            Some(TablePartitionRow::new(
                2,
                vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
            )),
        ),
        None,
    )
    .unwrap();

    for (file_id, row_id_start, partition) in [
        (DataFileId(1), 0, "1"),
        (DataFileId(2), 1, "2"),
        (DataFileId(3), 2, "1"),
        (DataFileId(4), 3, "2"),
    ] {
        kv.commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![
                DataFileRow::new(
                    file_id,
                    table,
                    format!("source-{}.parquet", file_id.0),
                    1,
                    64,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(row_id_start),
            ],
            Vec::new(),
            Vec::new(),
            vec![FilePartitionValueRow::new(
                file_id,
                table,
                PartitionKeyIndex(0),
                partition,
            )],
            Vec::new(),
        )
        .unwrap();
    }

    let committed = kv
        .commit_merge_adjacent_data_files_versionstamped(
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(1), DataFileId(2), DataFileId(3), DataFileId(4)],
                new_files: vec![
                    DataFileRow::new(
                        DataFileId(5),
                        table,
                        "partition-1-merged.parquet",
                        2,
                        128,
                        CatalogOrderId::uuid_v7(0),
                    ),
                    DataFileRow::new(
                        DataFileId(6),
                        table,
                        "partition-2-merged.parquet",
                        2,
                        128,
                        CatalogOrderId::uuid_v7(0),
                    ),
                ],
                partition_values: vec![
                    FilePartitionValueRow::new(DataFileId(5), table, PartitionKeyIndex(0), "1"),
                    FilePartitionValueRow::new(DataFileId(6), table, PartitionKeyIndex(0), "2"),
                ],
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

    assert_eq!(
        committed
            .new_files
            .iter()
            .map(|file| (file.data_file_id, file.row_id_start_known))
            .collect::<Vec<_>>(),
        vec![(DataFileId(5), false), (DataFileId(6), false)]
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(5), DataFileId(6)]
    );
}

#[test]
fn fdb_live_zero_output_merge_expires_empty_sources_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(86);
    let table = TableId(86);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("zero-output-merge").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    create_current_table(&kv, catalog, table, "zero_output_merge").unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "empty-a.parquet",
                0,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
            DataFileRow::new(
                DataFileId(2),
                table,
                "empty-b.parquet",
                0,
                64,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();

    let committed = kv
        .commit_merge_adjacent_data_files_versionstamped(
            catalog,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(1), DataFileId(2)],
                new_files: Vec::new(),
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();

    assert!(committed.new_files.is_empty());
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(1), DataFileId(2)]
    );
}

#[test]
fn fdb_live_rewrite_delete_compaction_replaces_source_and_delete_file_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(70);
    let table = TableId(50);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("rewrite-delete-compaction").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "rewrite_inline_compaction").unwrap();
    let [source] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(120),
                table,
                "rewrite-delete-source.parquet",
                10,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let delete = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(121),
                source.data_file_id,
                "rewrite-delete-source-delete.parquet",
                1,
                128,
                source.validity.begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
        .delete_files
        .into_iter()
        .next()
        .unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let committed = kv
        .commit_rewrite_delete_data_files_versionstamped(
            catalog,
            RewriteDeleteCompaction {
                source_file_ids: vec![source.data_file_id],
                new_files: vec![DataFileRow::new(
                    DataFileId(122),
                    table,
                    "rewrite-delete-replacement.parquet",
                    9,
                    900,
                    delete_snapshot.order,
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();
    let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(committed.new_files.len(), 1);
    assert_eq!(
        committed.new_files[0].validity.begin_order,
        rewrite_snapshot.order
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        committed.new_files
    );
    let historical =
        list_data_files_with_deletes_at(&kv, catalog, table, delete_snapshot.order).unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].data_file.data_file_id, source.data_file_id);
    let historical_delete = historical[0].delete_file.as_ref().unwrap();
    assert_eq!(historical_delete.delete_file_id, delete.delete_file_id);
    assert_eq!(historical_delete.path, delete.path);
    assert_eq!(
        historical_delete.validity.begin_order,
        delete.validity.begin_order
    );
    assert_eq!(
        historical_delete.validity.end_order,
        Some(rewrite_snapshot.order)
    );
    let after_rewrite =
        list_data_files_with_deletes_at(&kv, catalog, table, rewrite_snapshot.order).unwrap();
    assert_eq!(after_rewrite.len(), 1);
    assert_eq!(after_rewrite[0].data_file.data_file_id, DataFileId(122));
    assert_eq!(after_rewrite[0].delete_file, None);
    assert_eq!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .first()
            .map(|row| row.delete_file.delete_file_id),
        None
    );
}

#[test]
fn fdb_live_rewrite_delete_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled()
 {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(71);
    let table = TableId(51);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("rewrite-delete-conflict").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "rewrite_inline_compaction").unwrap();
    let [source] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(130),
                table,
                "rewrite-conflict-source.parquet",
                10,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(131),
            source.data_file_id,
            "rewrite-conflict-source-delete.parquet",
            1,
            128,
            source.validity.begin_order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![DataFileRow::new(
            DataFileId(132),
            table,
            "rewrite-conflict-concurrent.parquet",
            1,
            128,
            base_snapshot.order,
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = kv
        .commit_rewrite_delete_data_files_versionstamped_with_conflict_check(
            catalog,
            base_snapshot.order,
            concurrent_snapshot.order,
            RewriteDeleteCompaction {
                source_file_ids: vec![source.data_file_id],
                new_files: vec![DataFileRow::new(
                    DataFileId(133),
                    table,
                    "rewrite-conflict-should-not-publish.parquet",
                    9,
                    900,
                    base_snapshot.order,
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
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
            concurrent_snapshot.order,
            DataFileChangeKind::Added,
            DataFileId(132),
        )]
    );
    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(130))
    );
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(132))
    );
    assert!(
        !current
            .iter()
            .any(|file| file.data_file_id == DataFileId(133))
    );
}

#[test]
fn fdb_live_data_mutation_registers_inline_file_deletions_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(73);
    let table = TableId(53);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-file-delete-mutation").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let [source] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(150),
                table,
                "inline-delete-source.parquet",
                10,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();

    let mutation = kv
        .commit_data_mutation_versionstamped_with_inline_file_deletions(
            catalog,
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![
                InlineFileDeletionRow::new(table, source.data_file_id, 3, initial.order),
                InlineFileDeletionRow::new(table, source.data_file_id, 7, initial.order),
            ],
            Vec::new(),
        )
        .unwrap();
    let delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(mutation.inline_file_deletion_count, 2);
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, delete_snapshot.order)
            .unwrap()
            .get(&source.data_file_id)
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![3, 7]
    );
}

#[test]
fn fdb_live_rewrite_delete_compaction_uses_inline_file_deletions_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(74);
    let table = TableId(54);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("rewrite-delete-inline").into_bytes(),
    )
    .unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let initial = create_current_table(&kv, catalog, table, "rewrite_inline_compaction").unwrap();
    let [source] = kv
        .append_data_files_versionstamped(
            catalog,
            vec![DataFileRow::new(
                DataFileId(160),
                table,
                "rewrite-inline-source.parquet",
                10,
                1_024,
                initial.order,
            )],
        )
        .unwrap()
        .try_into()
        .unwrap();
    kv.commit_data_mutation_versionstamped_with_inline_file_deletions(
        catalog,
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![InlineFileDeletionRow::new(
            table,
            source.data_file_id,
            4,
            source.validity.begin_order,
        )],
        Vec::new(),
    )
    .unwrap();
    let inline_delete_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let committed = kv
        .commit_rewrite_delete_data_files_versionstamped(
            catalog,
            RewriteDeleteCompaction {
                source_file_ids: vec![source.data_file_id],
                new_files: vec![DataFileRow::new(
                    DataFileId(161),
                    table,
                    "rewrite-inline-replacement.parquet",
                    9,
                    900,
                    inline_delete_snapshot.order,
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap();
    let rewrite_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let historical = list_data_files_at(&kv, catalog, table, inline_delete_snapshot.order).unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].data_file_id, source.data_file_id);
    assert_eq!(historical[0].path, source.path);
    assert_eq!(
        historical[0].validity.begin_order,
        source.validity.begin_order
    );
    assert_eq!(
        historical[0].validity.end_order,
        Some(rewrite_snapshot.order)
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        committed.new_files
    );
    assert_eq!(
        list_inline_file_deletions_at(&kv, catalog, table, rewrite_snapshot.order)
            .unwrap()
            .get(&DataFileId(160))
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![4]
    );
}

#[test]
fn fdb_live_rewrite_delete_compaction_without_physical_delete_file_fails_closed_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(72);
    let table = TableId(52);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("rewrite-delete-no-physical-delete").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![DataFileRow::new(
            DataFileId(140),
            table,
            "rewrite-no-delete-source.parquet",
            10,
            1_024,
            initial.order,
        )],
    )
    .unwrap();

    let error = kv
        .commit_rewrite_delete_data_files_versionstamped(
            catalog,
            RewriteDeleteCompaction {
                source_file_ids: vec![DataFileId(140)],
                new_files: vec![DataFileRow::new(
                    DataFileId(141),
                    table,
                    "rewrite-no-delete-should-not-publish.parquet",
                    9,
                    900,
                    initial.order,
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
        )
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("has no delete file or inline deletions to rewrite")
    );
    assert_eq!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(140)]
    );
}

#[test]
fn fdb_live_merge_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(69);
    let table = TableId(49);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("merge-compaction-conflict").into_bytes(),
    )
    .unwrap();
    let initial = kv
        .initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![DataFileRow::new(
            DataFileId(110),
            table,
            "merge-conflict-source.parquet",
            5,
            512,
            initial.order,
        )],
    )
    .unwrap();
    let base_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![DataFileRow::new(
            DataFileId(111),
            table,
            "merge-conflict-concurrent.parquet",
            7,
            768,
            initial.order,
        )],
    )
    .unwrap();
    let concurrent_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let error = kv
        .commit_merge_adjacent_data_files_versionstamped_with_conflict_check(
            catalog,
            base_snapshot.order,
            concurrent_snapshot.order,
            MergeAdjacentCompaction {
                source_file_ids: vec![DataFileId(110)],
                new_files: vec![DataFileRow::new(
                    DataFileId(112),
                    table,
                    "merge-conflict-should-not-publish.parquet",
                    12,
                    1_280,
                    initial.order,
                )],
                partition_values: Vec::new(),
                file_column_stats: Vec::new(),
            },
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
            concurrent_snapshot.order,
            DataFileChangeKind::Added,
            DataFileId(111),
        )]
    );
    let current = list_current_data_files(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(110))
    );
    assert!(
        current
            .iter()
            .any(|file| file.data_file_id == DataFileId(111))
    );
    assert!(
        !current
            .iter()
            .any(|file| file.data_file_id == DataFileId(112))
    );
}

#[test]
fn fdb_live_concurrent_remove_conflicts_without_publishing_append_when_enabled() {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(65);
    let table = TableId(45);
    let base = CatalogOrderId::uuid_v7(10);
    let concurrent_remove = CatalogOrderId::uuid_v7(20);
    let mut kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("logical-conflict").into_bytes(),
    )
    .unwrap();

    append_data_file(
        &mut kv,
        catalog,
        DataFileRow::new(DataFileId(1), table, "base.parquet", 10, 100, base),
    )
    .unwrap();
    expire_data_file(&mut kv, catalog, DataFileId(1), concurrent_remove).unwrap();

    let attempt = CommitAttemptId(456);
    let error = kv
        .commit_append_data_file_versionstamped(
            catalog,
            attempt,
            base,
            concurrent_remove,
            DataFileRow::new(
                DataFileId(2),
                table,
                "should-not-publish.parquet",
                20,
                200,
                base,
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
            concurrent_remove,
            DataFileChangeKind::Removed,
            DataFileId(1)
        )]
    );
    assert_eq!(load_commit_attempt(&kv, catalog, attempt).unwrap(), None);
    assert!(
        list_current_data_files(&kv, catalog, table)
            .unwrap()
            .is_empty()
    );
}

fn live_fdb_disabled() -> bool {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() == Ok("1") {
        return false;
    }
    eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
    true
}

fn unique_prefix(test_name: &str) -> String {
    format!(
        "aux-ducklake-test/{}/{}/{}",
        test_name,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn create_current_table(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    name: &str,
) -> ducklake_catalog::CatalogResult<ducklake_catalog::SnapshotRow> {
    kv.create_table_versionstamped(
        catalog,
        TableRow::with_catalog_metadata(
            table,
            SchemaId(0),
            format!("{name}-uuid"),
            name,
            format!("main/{name}/"),
            vec![TableColumnRow::new(
                ColumnId(1),
                "id",
                "INTEGER",
                false,
                None,
            )],
            CatalogOrderId::uuid_v7(0),
        ),
        None,
    )?;
    latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("snapshot"))
}

fn assert_public_snapshot_order(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    public_sequence: u64,
    expected_order: CatalogOrderId,
) {
    let snapshot = snapshot_by_public_sequence(kv, catalog, DuckLakeSnapshotId(public_sequence))
        .unwrap()
        .unwrap_or_else(|| panic!("missing public snapshot {public_sequence}"));
    assert_eq!(snapshot.order, expected_order);
}
