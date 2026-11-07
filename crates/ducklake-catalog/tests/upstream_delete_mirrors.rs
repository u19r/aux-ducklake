use ducklake_catalog::{
    CatalogId, CatalogOrderId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FakeOrderedCatalogKv, InlineFileDeletionRow, TableId, commit_append_data_files,
    commit_data_mutation, commit_inline_file_deletions, commit_register_delete_files,
    initialize_catalog_if_absent, list_current_data_files_with_deletes,
    list_data_files_with_deletes_at, list_inline_file_deletions_between,
    list_table_deletion_scan_files, public_snapshot_sequence_for_order,
};

#[test]
fn mirrors_basic_delete_test_time_travel_before_delete_returns_undeleted_files() {
    // Mirrors: third_party/ducklake/test/sql/delete/basic_delete.test
    //
    // Storage contract:
    // - DuckLake saves two data files, then saves delete files for both inside the delete commit.
    // - Current reads should attach the delete files.
    // - Time travel to the snapshot before the delete should return the same two data files with
    //   no delete file attached.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1000),
            data_file(2, table, 1000, 1000),
        ],
    )
    .unwrap();
    let before_delete = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file(1, 1, 500), delete_file(2, 2, 500)],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let historical =
        list_data_files_with_deletes_at(&kv, catalog, table, before_delete.order).unwrap();

    assert_eq!(current.len(), 2);
    assert!(
        current.iter().all(|file| file.delete_file.is_some()),
        "current delete scan should attach the delete files saved by the delete commit"
    );
    assert_eq!(historical.len(), 2);
    assert!(
        historical.iter().all(|file| file.delete_file.is_none()),
        "time travel before the delete should not attach later delete files"
    );
    assert_eq!(
        historical
            .iter()
            .map(|file| file.data_file.record_count)
            .sum::<u64>(),
        2000
    );
}

#[test]
fn mirrors_multi_deletes_test_consolidated_delete_file_preserves_first_delete_begin_snapshot() {
    // Mirrors: third_party/ducklake/test/sql/delete/multi_deletes.test
    //
    // Storage contract:
    // - DuckLake saves one physical data file.
    // - The first transaction writes a consolidated delete file for that data file.
    // - A later delete replaces/consolidates that delete file.
    // - The current delete-file metadata should still begin at the first delete's logical
    //   snapshot, because the replacement represents all deletes since that first delete.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10_000)]).unwrap();
    let first_delete =
        commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 2_500)]).unwrap();
    let first_delete_begin = first_delete[0].validity.begin_order;
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(2, 1, 5_000)]).unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let current_delete = current[0].delete_file.as_ref().unwrap();
    let current_begin =
        public_snapshot_sequence_for_order(&kv, catalog, current_delete.validity.begin_order)
            .unwrap()
            .unwrap();
    let expected_begin = public_snapshot_sequence_for_order(&kv, catalog, first_delete_begin)
        .unwrap()
        .unwrap();

    assert_eq!(current_delete.delete_file_id, DeleteFileId(2));
    assert_eq!(
        current_begin, expected_begin,
        "replacement delete file should preserve the logical begin snapshot of the first delete"
    );
}

#[test]
fn mirrors_delete_join_test_insert_and_delete_in_one_transaction() {
    // Mirrors: third_party/ducklake/test/sql/delete/delete_join.test
    //
    // Storage contract:
    // - DuckLake has one existing data file.
    // - One transaction appends a second data file and saves delete files for the joined delete.
    // - Storage must commit the new file and all delete metadata atomically so current reads see
    //   both files and both delete files.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 500)]).unwrap();

    commit_data_mutation(
        &mut kv,
        catalog,
        vec![data_file(2, table, 500, 500)],
        vec![delete_file(1, 1, 250), delete_file(2, 2, 250)],
        &[],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 2);
    assert!(
        current.iter().all(|file| file.delete_file.is_some()),
        "joined delete should attach delete metadata to both old and newly appended files"
    );
}

#[test]
fn mirrors_delete_mixed_formats_test_replacements_keep_cumulative_delete_history() {
    // Mirrors: third_party/ducklake/test/sql/delete/delete_mixed_formats.test
    //
    // Storage contract:
    // - A file receives delete-file replacements in alternating physical formats.
    // - The latest current delete file must hold the cumulative delete count and retain the first
    //   delete's logical begin snapshot for time travel.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let v_create = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let first = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(1, 1, "main/mix/delete-1.puffin", 10)],
    )
    .unwrap();
    let first_begin = first[0].validity.begin_order;
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(2, 1, "main/mix/delete-2.parquet", 20)],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(3, 1, "main/mix/delete-3.puffin", 30)],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let current_delete = current[0].delete_file.as_ref().unwrap();
    let historical = list_data_files_with_deletes_at(&kv, catalog, table, v_create.order).unwrap();

    assert_eq!(current_delete.delete_file_id, DeleteFileId(3));
    assert_eq!(current_delete.record_count, 30);
    assert!(current_delete.path.ends_with(".puffin"));
    assert_eq!(current_delete.validity.begin_order, first_begin);
    assert!(historical[0].delete_file.is_none());
}

#[test]
fn mirrors_delete_same_transaction_test_new_file_can_be_deleted_before_commit() {
    // Mirrors: third_party/ducklake/test/sql/delete/delete_same_transaction.test
    //
    // Storage contract:
    // - DuckLake creates a new table data file and deletes rows from it before the transaction
    //   commits.
    // - Storage must be able to save the data file and its delete file in the same metadata commit.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_data_mutation(
        &mut kv,
        catalog,
        vec![data_file(1, table, 0, 1000)],
        vec![delete_file(1, 1, 625)],
        &[],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].delete_file.as_ref().unwrap().record_count, 625);
}

#[test]
fn mirrors_deletion_vector_test_replacement_delete_files_keep_time_travel_visibility() {
    // Mirrors: third_party/ducklake/test/sql/delete/deletion_vector.test
    //
    // Storage contract:
    // - Two files receive initial delete files, then one file receives a replacement delete file.
    // - The replacement must preserve the first delete snapshot and historical reads before the
    //   first delete must remain undeleted.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 1000),
            data_file(2, table, 1000, 1000),
        ],
    )
    .unwrap();
    let before_delete = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    let first = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file(1, 1, 500), delete_file(2, 2, 500)],
    )
    .unwrap();
    let first_begin = first[0].validity.begin_order;
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(3, 1, 550)]).unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let historical =
        list_data_files_with_deletes_at(&kv, catalog, table, before_delete.order).unwrap();

    assert_eq!(current.len(), 2);
    assert_eq!(
        current
            .iter()
            .find(|file| file.data_file.data_file_id == DataFileId(1))
            .unwrap()
            .delete_file
            .as_ref()
            .unwrap()
            .validity
            .begin_order,
        first_begin
    );
    assert!(historical.iter().all(|file| file.delete_file.is_none()));
}

#[test]
fn mirrors_deletion_vector_inlined_flush_test_inline_deletions_become_one_delete_file() {
    // Mirrors: third_party/ducklake/test/sql/delete/deletion_vector_inlined_flush.test
    //
    // Storage contract:
    // - Small deletes first land as inline file deletions.
    // - A flush materializes them as a delete file.
    // - A later inline delete and flush replaces that delete file while preserving the first
    //   flush's logical begin snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 100)]).unwrap();
    let v_data = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, DataFileId(1), 0, CatalogOrderId::uuid_v7(0)),
            InlineFileDeletionRow::new(table, DataFileId(1), 1, CatalogOrderId::uuid_v7(0)),
        ],
    )
    .unwrap();
    let first_flush = commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(1, 1, "main/test/delete-1.puffin", 2)],
    )
    .unwrap();
    let first_flush_begin = first_flush[0].validity.begin_order;
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![InlineFileDeletionRow::new(
            table,
            DataFileId(1),
            2,
            CatalogOrderId::uuid_v7(0),
        )],
    )
    .unwrap();
    commit_register_delete_files(
        &mut kv,
        catalog,
        vec![delete_file_with_path(2, 1, "main/test/delete-2.puffin", 3)],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let before_deletes =
        list_data_files_with_deletes_at(&kv, catalog, table, v_data.order).unwrap();

    assert_eq!(
        current[0].delete_file.as_ref().unwrap().delete_file_id,
        DeleteFileId(2)
    );
    assert_eq!(current[0].delete_file.as_ref().unwrap().record_count, 3);
    assert_eq!(
        current[0]
            .delete_file
            .as_ref()
            .unwrap()
            .validity
            .begin_order,
        first_flush_begin
    );
    assert!(before_deletes[0].delete_file.is_none());
}

#[test]
fn mirrors_deletion_vector_multi_snapshot_test_delete_scan_keeps_first_seen_attribution() {
    // Mirrors: third_party/ducklake/test/sql/delete/deletion_vector_multi_snapshot.test
    //
    // Storage contract:
    // - Three deletes across three snapshots target one file.
    // - The current delete file is cumulative.
    // - The deletion scan over the full range returns each row id at its first delete snapshot.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();
    let v_create = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    let d1 = commit_register_delete_files(&mut kv, catalog, vec![delete_file(1, 1, 1)]).unwrap();
    let d1_order = d1[0].validity.begin_order;
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(2, 1, 2)]).unwrap();
    commit_register_delete_files(&mut kv, catalog, vec![delete_file(3, 1, 3)]).unwrap();
    let v_d3 = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let scans =
        list_table_deletion_scan_files(&kv, catalog, table, v_create.order, v_d3.order).unwrap();

    assert_eq!(
        current[0].delete_file.as_ref().unwrap().delete_file_id,
        DeleteFileId(3)
    );
    assert_eq!(current[0].delete_file.as_ref().unwrap().record_count, 3);
    assert_eq!(
        current[0]
            .delete_file
            .as_ref()
            .unwrap()
            .validity
            .begin_order,
        d1_order
    );
    assert!(scans.iter().any(|scan| {
        scan.delete_file
            .as_ref()
            .is_some_and(|row| row.delete_file_id == DeleteFileId(3))
    }));
}

#[test]
fn mirrors_parquet_deletion_describe_test_inline_deletions_keep_per_row_snapshot_attribution() {
    // Mirrors: third_party/ducklake/test/sql/delete/parquet_deletion_describe.test
    //
    // Storage contract:
    // - Inline file deletions are saved across multiple snapshots.
    // - Before physical flush, storage can still return the row ids and the snapshot at which each
    //   row was first deleted.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 8)]).unwrap();
    let start = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, DataFileId(1), 1, CatalogOrderId::uuid_v7(0)),
            InlineFileDeletionRow::new(table, DataFileId(1), 4, CatalogOrderId::uuid_v7(0)),
        ],
    )
    .unwrap();
    let delete_twos = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();
    commit_inline_file_deletions(
        &mut kv,
        catalog,
        vec![
            InlineFileDeletionRow::new(table, DataFileId(1), 0, CatalogOrderId::uuid_v7(0)),
            InlineFileDeletionRow::new(table, DataFileId(1), 3, CatalogOrderId::uuid_v7(0)),
        ],
    )
    .unwrap();
    let delete_ones = ducklake_catalog::latest_snapshot(&kv, catalog)
        .unwrap()
        .unwrap();

    let deleted =
        list_inline_file_deletions_between(&kv, catalog, table, start.order, delete_ones.order)
            .unwrap();

    let row_ids = deleted
        .iter()
        .filter(|row| row.data_file_id == DataFileId(1))
        .map(|row| row.row_id)
        .collect::<Vec<_>>();
    assert_eq!(row_ids, vec![1, 4, 0, 3]);
    assert_ne!(delete_twos.order, delete_ones.order);
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/test/file-{id}.parquet"),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> DeleteFileRow {
    delete_file_with_path(
        id,
        data_file_id,
        &format!("main/test/file-{data_file_id}-delete-{id}.parquet"),
        record_count,
    )
}

fn delete_file_with_path(
    id: u64,
    data_file_id: u64,
    path: &str,
    record_count: u64,
) -> DeleteFileRow {
    DeleteFileRow::new(
        DeleteFileId(id),
        DataFileId(data_file_id),
        path,
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}
