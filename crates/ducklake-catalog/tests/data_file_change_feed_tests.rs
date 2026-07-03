use ducklake_catalog::{
    CatalogId, CatalogOrderId, CatalogOrderKind, ColumnId, DataFileChange, DataFileChangeKind,
    DataFileId, DataFileRow, DeleteFileId, DeleteFileRow, FakeOrderedCatalogKv, FileColumnStatsRow,
    MergeAdjacentCompaction, RangeDirection, RewriteDeleteCompaction, TableId, append_data_file,
    commit_append_data_files, commit_merge_adjacent_data_files, commit_register_delete_files,
    commit_rewrite_delete_data_files, expire_data_file, initialize_catalog_if_absent,
    keys::{
        data_file_begin_prefix, data_file_end_key, decode_key, delete_file_timeline_key,
        file_column_stats_lookup_key,
    },
    latest_snapshot, list_current_data_files, list_current_data_files_with_deletes,
    list_data_file_changes, list_data_files_at, list_data_files_with_deletes_at,
    list_file_column_stats_for_table_column, register_delete_file, register_file_column_stats,
};

#[test]
fn given_file_expired_when_listing_current_then_only_active_files_remain() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin_one = CatalogOrderId::uuid_v7(10);
    let begin_two = CatalogOrderId::uuid_v7(20);
    let end_one = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let expired = DataFileRow::new(
        DataFileId(1),
        table,
        "main/orders/one.parquet",
        10,
        100,
        begin_one,
    );
    let active = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin_two,
    );

    append_data_file(&mut kv, catalog, expired.clone()).unwrap();
    append_data_file(&mut kv, catalog, active.clone()).unwrap();
    let expired_row = expire_data_file(&mut kv, catalog, expired.data_file_id, end_one).unwrap();
    let current = list_current_data_files(&kv, catalog, table).unwrap();

    assert_eq!(expired_row.validity.end_order, Some(end_one));
    assert_eq!(current, vec![active]);
}

#[test]
fn given_file_expired_when_time_traveling_then_visibility_uses_begin_and_end_orders() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin_one = CatalogOrderId::uuid_v7(10);
    let begin_two = CatalogOrderId::uuid_v7(20);
    let end_one = CatalogOrderId::uuid_v7(30);
    let after_end = CatalogOrderId::uuid_v7(40);
    let mut kv = FakeOrderedCatalogKv::new();
    let expired = DataFileRow::new(
        DataFileId(1),
        table,
        "main/orders/one.parquet",
        10,
        100,
        begin_one,
    );
    let active = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin_two,
    );

    append_data_file(&mut kv, catalog, expired.clone()).unwrap();
    append_data_file(&mut kv, catalog, active.clone()).unwrap();
    let expired = expire_data_file(&mut kv, catalog, expired.data_file_id, end_one).unwrap();

    assert_eq!(
        list_data_files_at(&kv, catalog, table, begin_one).unwrap(),
        vec![expired.clone()]
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, begin_two).unwrap(),
        vec![expired, active.clone()]
    );
    assert_eq!(
        list_data_files_at(&kv, catalog, table, after_end).unwrap(),
        vec![active]
    );
}

#[test]
fn given_expired_file_when_end_timeline_key_is_decoded_then_key_is_readable() {
    let key = data_file_end_key(
        CatalogId(7),
        TableId(42),
        CatalogOrderId::uuid_v7(30),
        DataFileId(1),
    );

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=7"));
    assert!(decoded.contains("family=end-order"));
    assert!(decoded.contains("000000000000001e"));
}

#[test]
fn given_time_travel_scan_when_snapshot_precedes_file_then_begin_timeline_range_excludes_it() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin = CatalogOrderId::uuid_v7(20);
    let before_begin = CatalogOrderId::uuid_v7(19);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin,
    );

    append_data_file(&mut kv, catalog, file).unwrap();

    assert!(
        list_data_files_at(&kv, catalog, table, before_begin)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        kv.scan_prefix(
            &data_file_begin_prefix(catalog, table),
            RangeDirection::Forward,
            usize::MAX
        )
        .len(),
        1
    );
}

#[test]
fn given_file_appended_and_removed_when_reading_changes_then_table_feed_is_ordered() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin_one = CatalogOrderId::uuid_v7(10);
    let begin_two = CatalogOrderId::uuid_v7(20);
    let end_one = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let expired = DataFileRow::new(
        DataFileId(1),
        table,
        "main/orders/one.parquet",
        10,
        100,
        begin_one,
    );
    let active = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin_two,
    );

    append_data_file(&mut kv, catalog, expired.clone()).unwrap();
    append_data_file(&mut kv, catalog, active.clone()).unwrap();
    expire_data_file(&mut kv, catalog, expired.data_file_id, end_one).unwrap();

    let changes = list_data_file_changes(&kv, catalog, table, begin_one, end_one).unwrap();

    assert_eq!(
        changes,
        vec![
            DataFileChange::new(
                table,
                begin_one,
                DataFileChangeKind::Added,
                expired.data_file_id
            ),
            DataFileChange::new(
                table,
                begin_two,
                DataFileChangeKind::Added,
                active.data_file_id
            ),
            DataFileChange::new(
                table,
                end_one,
                DataFileChangeKind::Removed,
                expired.data_file_id
            ),
        ]
    );
}

#[test]
fn given_merge_adjacent_compaction_when_committed_then_current_and_historical_visibility_are_preserved()
 {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let appended = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/one.parquet",
                10,
                100,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
            DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/two.parquet",
                20,
                200,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(10),
        ],
    )
    .unwrap();
    let before_compaction = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let replacement = DataFileRow::new(
        DataFileId(3),
        table,
        "main/orders/merged.parquet",
        30,
        250,
        CatalogOrderId::uuid_v7(0),
    );

    let compacted = commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(2)],
            new_files: vec![replacement],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let after_compaction = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        compacted.new_files
    );
    let historical = list_data_files_at(&kv, catalog, table, before_compaction.order).unwrap();
    assert_eq!(historical.len(), 2);
    assert_eq!(historical[0].data_file_id, appended[0].data_file_id);
    assert_eq!(historical[1].data_file_id, appended[1].data_file_id);
    assert_eq!(
        historical[0].validity.end_order,
        Some(after_compaction.order)
    );
    assert_eq!(
        historical[1].validity.end_order,
        Some(after_compaction.order)
    );
    assert_eq!(
        list_data_file_changes(
            &kv,
            catalog,
            table,
            before_compaction.order,
            after_compaction.order
        )
        .unwrap(),
        vec![
            DataFileChange::new(
                table,
                before_compaction.order,
                DataFileChangeKind::Added,
                DataFileId(1)
            ),
            DataFileChange::new(
                table,
                before_compaction.order,
                DataFileChangeKind::Added,
                DataFileId(2)
            ),
            DataFileChange::new(
                table,
                after_compaction.order,
                DataFileChangeKind::Added,
                DataFileId(3)
            ),
            DataFileChange::new(
                table,
                after_compaction.order,
                DataFileChangeKind::Removed,
                DataFileId(1)
            ),
            DataFileChange::new(
                table,
                after_compaction.order,
                DataFileChangeKind::Removed,
                DataFileId(2)
            ),
        ]
    );
}

#[test]
fn given_source_file_with_delete_file_when_merge_adjacent_commits_then_compaction_is_rejected() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let source = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/one.parquet",
                10,
                100,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(9),
            DataFileId(1),
            "main/orders/delete.parquet",
            1,
            50,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    let result = commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/merged.parquet",
                10,
                120,
                CatalogOrderId::uuid_v7(0),
            )],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );

    assert!(result.is_err());
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        source
    );
}

#[test]
fn given_rewrite_delete_compaction_when_committed_then_source_and_delete_file_expire_together() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let source = commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(1),
                table,
                "main/orders/one.parquet",
                10,
                100,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(0),
        ],
    )
    .unwrap();
    let delete_files = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![DeleteFileRow::new(
            DeleteFileId(9),
            DataFileId(1),
            "main/orders/delete.parquet",
            2,
            50,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    let before_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let replacement = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/rewrite.parquet",
        8,
        90,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(0);

    let rewritten = commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![replacement],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    let after_rewrite = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, table).unwrap(),
        vec![ducklake_catalog::AttachedDataFile::new(
            rewritten.new_files[0].clone(),
            None
        )]
    );
    let historical =
        list_data_files_with_deletes_at(&kv, catalog, table, before_rewrite.order).unwrap();
    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].data_file.data_file_id, source[0].data_file_id);
    assert_eq!(
        historical[0].data_file.validity.end_order,
        Some(after_rewrite.order)
    );
    let historical_delete = historical[0].delete_file.as_ref().unwrap();
    assert_eq!(
        historical_delete.delete_file_id,
        delete_files[0].delete_file_id
    );
    assert_eq!(
        historical_delete.validity.end_order,
        Some(after_rewrite.order)
    );
    assert_eq!(
        list_data_files_with_deletes_at(&kv, catalog, table, after_rewrite.order).unwrap()[0]
            .delete_file,
        None
    );
}

#[test]
fn given_source_file_without_delete_file_when_rewrite_delete_commits_then_compaction_is_rejected() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let source = commit_append_data_files(
        &mut kv,
        catalog,
        vec![DataFileRow::new(
            DataFileId(1),
            table,
            "main/orders/one.parquet",
            10,
            100,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();

    let result = commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![DataFileRow::new(
                DataFileId(2),
                table,
                "main/orders/rewrite.parquet",
                10,
                120,
                CatalogOrderId::uuid_v7(0),
            )],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    );

    assert!(result.is_err());
    assert_eq!(
        list_current_data_files(&kv, catalog, table).unwrap(),
        source
    );
}

#[test]
fn given_delete_file_row_when_round_tripped_then_delete_metadata_is_preserved() {
    let row = DeleteFileRow::new(
        DeleteFileId(4),
        DataFileId(2),
        "main/orders/delete-0001.parquet",
        3,
        512,
        CatalogOrderId::uuid_v7(30),
    );

    let decoded = DeleteFileRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
}

#[test]
fn given_fdb_versionstamped_delete_file_row_when_round_tripped_then_order_kind_is_preserved() {
    let begin = CatalogOrderId::fdb_versionstamp([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
    let row = DeleteFileRow::new(
        DeleteFileId(4),
        DataFileId(2),
        "main/orders/delete-0001.parquet",
        3,
        512,
        begin,
    );

    let decoded = DeleteFileRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
    assert_eq!(
        decoded.validity.begin_order.kind(),
        CatalogOrderKind::FdbVersionstamp
    );
}

#[test]
fn given_old_delete_file_row_when_decoded_then_format_is_rejected() {
    let begin = CatalogOrderId::uuid_v7(30);
    let mut bytes = Vec::new();
    bytes.push(1);
    bytes.extend_from_slice(&4_u64.to_be_bytes());
    bytes.extend_from_slice(&2_u64.to_be_bytes());
    bytes.extend_from_slice(&3_u64.to_be_bytes());
    bytes.extend_from_slice(&512_u64.to_be_bytes());
    bytes.extend_from_slice(&begin.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&[0; CatalogOrderId::LEN]);
    bytes.extend_from_slice(b"main/orders/delete-0001.parquet");

    let error = DeleteFileRow::decode(&bytes).unwrap_err();

    assert_eq!(
        error.to_string(),
        "decode failed: unsupported delete file row version 1"
    );
}

#[test]
fn given_current_file_with_delete_when_listing_current_then_delete_file_is_attached() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin = CatalogOrderId::uuid_v7(20);
    let delete_begin = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin,
    );
    let delete_file = DeleteFileRow::new(
        DeleteFileId(4),
        file.data_file_id,
        "main/orders/delete-0001.parquet",
        3,
        512,
        delete_begin,
    );

    append_data_file(&mut kv, catalog, file.clone()).unwrap();
    register_delete_file(&mut kv, catalog, delete_file.clone()).unwrap();

    let files = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].data_file, file);
    assert_eq!(files[0].delete_file, Some(delete_file));
}

#[test]
fn given_delete_file_when_time_traveling_then_delete_timeline_selects_visible_delete() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let begin = CatalogOrderId::uuid_v7(20);
    let before_delete = CatalogOrderId::uuid_v7(29);
    let delete_begin = CatalogOrderId::uuid_v7(30);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        begin,
    );
    let delete_file = DeleteFileRow::new(
        DeleteFileId(4),
        file.data_file_id,
        "main/orders/delete-0001.parquet",
        3,
        512,
        delete_begin,
    );

    append_data_file(&mut kv, catalog, file.clone()).unwrap();
    register_delete_file(&mut kv, catalog, delete_file.clone()).unwrap();

    let before = list_data_files_with_deletes_at(&kv, catalog, table, before_delete).unwrap();
    let after = list_data_files_with_deletes_at(&kv, catalog, table, delete_begin).unwrap();

    assert_eq!(before.len(), 1);
    assert_eq!(before[0].delete_file, None);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].data_file, file);
    assert_eq!(after[0].delete_file, Some(delete_file));
}

#[test]
fn given_delete_file_when_timeline_key_is_decoded_then_key_is_readable() {
    let key = delete_file_timeline_key(
        CatalogId(7),
        DataFileId(2),
        CatalogOrderId::uuid_v7(30),
        DeleteFileId(4),
    );

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=7"));
    assert!(decoded.contains("family=delete-file-timeline"));
    assert!(decoded.contains("000000000000001e"));
}

#[test]
fn given_file_column_stats_row_when_round_tripped_then_stats_are_preserved() {
    let row = FileColumnStatsRow::new(
        DataFileId(2),
        TableId(42),
        ColumnId(3),
        1,
        Some("10".to_owned()),
        Some("99".to_owned()),
    );

    let decoded = FileColumnStatsRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
}

#[test]
fn given_file_column_stats_when_listed_by_table_column_then_lookup_index_bounds_scan() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let column = ColumnId(3);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/two.parquet",
        20,
        200,
        CatalogOrderId::uuid_v7(20),
    );
    let stats = FileColumnStatsRow::new(
        file.data_file_id,
        table,
        column,
        1,
        Some("10".to_owned()),
        Some("99".to_owned()),
    );

    append_data_file(&mut kv, catalog, file).unwrap();
    register_file_column_stats(&mut kv, catalog, stats.clone()).unwrap();

    let rows = list_file_column_stats_for_table_column(&kv, catalog, table, column).unwrap();

    assert_eq!(rows, vec![stats]);
}

#[test]
fn given_file_column_stats_when_lookup_key_is_decoded_then_key_is_readable() {
    let key = file_column_stats_lookup_key(CatalogId(7), TableId(42), ColumnId(3), DataFileId(2));

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=7"));
    assert!(decoded.contains("family=file-column-stats-lookup"));
    assert!(decoded.contains("000000000000002a"));
}
