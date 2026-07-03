use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    InlineTableFlush, MergeAdjacentCompaction, RawSnapshotSequence, RewriteDeleteCompaction,
    SchemaId, SchemaRow, TableColumnRow, TableId, TableRow, ViewCommentChange, ViewRename, ViewRow,
    commit_append_data_files, commit_change_view_comment, commit_create_schema_rows,
    commit_create_table_row, commit_create_view_row, commit_data_mutation,
    commit_data_mutation_with_file_partitions_and_inline_deletes, commit_drop_schema_rows,
    commit_drop_tables, commit_merge_adjacent_data_files, commit_rename_views,
    commit_rewrite_delete_data_files, expire_snapshots, initialize_catalog_if_absent,
    latest_snapshot, list_current_data_files_with_deletes, list_data_files_at,
    list_old_data_files_for_cleanup, list_old_delete_files_for_cleanup, list_schemas_at,
    list_snapshots, list_views_at, load_view_at, register_inline_table_payload_with_table,
    remove_old_data_files, remove_old_delete_files,
};

#[test]
fn mirrors_catalog_schema_test_schema_create_drop_recreate_and_table_ownership() {
    // Mirrors: third_party/ducklake/test/sql/catalog/schema.test
    //
    // Storage contract:
    // - DuckLake creates schemas, stores tables under those schema ids, drops schemas, and later
    //   recreates a schema name with a new schema row.
    // - Listing schemas at the latest snapshot must reflect only currently visible schemas.
    // - Tables keep their schema id, so dropping one schema must not affect another schema's table.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();

    commit_create_schema_rows(&mut kv, catalog, vec![schema(1, "s1"), schema(2, "s2")]).unwrap();
    create_table_in_schema(&mut kv, catalog, TableId(1), SchemaId(1), "tbl", 1);
    create_table_in_schema(&mut kv, catalog, TableId(2), SchemaId(2), "tbl", 2);
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, TableId(1), 0, 1),
            data_file(2, TableId(2), 0, 1),
        ],
    )
    .unwrap();

    assert_current_schema_names(&kv, catalog, &["s1", "s2"]);
    commit_drop_tables(&mut kv, catalog, &[TableId(1)]).unwrap();
    commit_drop_schema_rows(&mut kv, catalog, &[SchemaId(1)]).unwrap();
    assert_current_schema_names(&kv, catalog, &["s2"]);
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, TableId(2))
            .unwrap()
            .len(),
        1
    );

    commit_drop_schema_rows(&mut kv, catalog, &[SchemaId(2)]).unwrap();
    assert_current_schema_names(&kv, catalog, &[]);
    commit_create_schema_rows(&mut kv, catalog, vec![schema(3, "s1")]).unwrap();
    create_table_in_schema(&mut kv, catalog, TableId(3), SchemaId(3), "tbl", 3);
    commit_append_data_files(&mut kv, catalog, vec![data_file(3, TableId(3), 0, 1)]).unwrap();

    assert_current_schema_names(&kv, catalog, &["s1"]);
    assert_eq!(
        current_schemas(&kv, catalog)[0].schema_id,
        SchemaId(3),
        "recreated schema name should be represented by the new schema id"
    );
}

#[test]
fn mirrors_checkpoint_ducklake_test_checkpoint_compaction_keeps_current_files_and_cleanup_queue() {
    // Mirrors: third_party/ducklake/test/sql/checkpoint/checkpoint_ducklake.test
    //
    // Storage contract:
    // - Checkpoint-like work materializes inline rows, rewrites deleted data, and merges adjacent
    //   files.
    // - Current listings must return only the replacement files, while old source files are queued
    //   for cleanup without deleting the active delete file metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let t = TableId(1);
    let t2 = TableId(2);
    let schema = SchemaId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table_in_schema(&mut kv, catalog, t, schema, "t", 1);
    create_table_in_schema(&mut kv, catalog, t2, schema, "t_2", 1);

    register_inline_table_payload_with_table(
        &mut kv,
        catalog,
        table_with_inline(t, schema),
        schema,
        inline_payload(&[(0, 1), (1, 2), (2, 2), (3, 3), (4, 4)]),
    )
    .unwrap();
    let t_inline = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_data_mutation(
        &mut kv,
        catalog,
        vec![data_file(1, t, 0, 5)],
        Vec::new(),
        &[InlineTableFlush::new(t, schema, t_inline.sequence)],
    )
    .unwrap();

    commit_append_data_files(&mut kv, catalog, vec![data_file(2, t2, 0, 100)]).unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 2, 98)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(2)],
            new_files: vec![data_file(3, t2, 98, 2)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(1), DataFileId(3)],
            new_files: vec![data_file(4, t, 0, 7)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let current_t = list_current_data_files_with_deletes(&kv, catalog, t).unwrap();
    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    let old_deletes = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(current_t[0].data_file.data_file_id, DataFileId(4));
    assert!(
        old_data
            .iter()
            .any(|file| file.data_file_id == DataFileId(1))
    );
    assert!(
        old_data
            .iter()
            .any(|file| file.data_file_id == DataFileId(2))
    );
    assert!(
        old_data
            .iter()
            .any(|file| file.data_file_id == DataFileId(3))
    );
    assert!(
        old_deletes
            .iter()
            .any(|row| row.delete_file.delete_file_id.0 == 1)
    );

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let expirable = list_snapshots(&kv, catalog)
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.order < latest.order)
        .map(|snapshot| snapshot.sequence)
        .collect::<Vec<_>>();
    expire_snapshots(&mut kv, catalog, &expirable).unwrap();

    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    let old_deletes = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert!(
        old_data
            .iter()
            .any(|file| file.data_file_id == DataFileId(1))
    );
    assert!(
        old_data
            .iter()
            .any(|file| file.data_file_id == DataFileId(2))
    );
    assert!(
        old_deletes
            .iter()
            .any(|row| row.delete_file.delete_file_id.0 == 1),
        "rewrite-delete should keep delete file metadata discoverable for cleanup"
    );
}

#[test]
fn mirrors_checkpoint_updates_interleaved_test_uncommitted_checkpoint_writes_are_not_storage_writes()
 {
    // Mirrors: third_party/ducklake/test/sql/checkpoint/checkpoint_updates_interleaved.test
    //
    // Storage contract:
    // - A rollback around checkpoint work means DuckLake must not commit those metadata writes to
    //   storage.
    // - The committed inserts before and after checkpoint calls should remain visible in storage
    //   as ordinary append metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table_in_schema(&mut kv, catalog, table, SchemaId(1), "test", 1);
    commit_append_data_files(&mut kv, catalog, vec![data_file(1, table, 0, 10)]).unwrap();

    commit_append_data_files(&mut kv, catalog, vec![data_file(2, table, 10, 3)]).unwrap();
    let after_first_commit = latest_snapshot(&kv, catalog).unwrap().unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![data_file(3, table, 13, 3), data_file(4, table, 17, 8)],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    let at_first_commit =
        list_data_files_at(&kv, catalog, table, after_first_commit.order).unwrap();
    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file.record_count)
            .sum::<u64>(),
        24
    );
    assert_eq!(
        at_first_commit
            .iter()
            .map(|file| file.record_count)
            .sum::<u64>(),
        13
    );
}

#[test]
fn mirrors_many_inserts_transaction_test_many_files_from_one_transaction_are_all_visible() {
    // Mirrors: third_party/ducklake/test/sql/checkpoint/many_inserts_transaction.test
    //
    // Storage contract:
    // - DuckLake may commit multiple inserted files from one transaction.
    // - After checkpoint, the storage layer still returns all current files for the table.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table_in_schema(&mut kv, catalog, table, SchemaId(1), "integers", 1);
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 10),
            data_file(2, table, 10, 1),
            data_file(3, table, 11, 1),
            data_file(4, table, 12, 1),
        ],
    )
    .unwrap();

    let current = list_current_data_files_with_deletes(&kv, catalog, table).unwrap();
    assert_eq!(current.len(), 4);
    assert_eq!(
        current
            .iter()
            .map(|file| file.data_file.record_count)
            .sum::<u64>(),
        13
    );
}

#[test]
fn mirrors_view_checkpoint_test_checkpoint_leaves_view_metadata_visible() {
    // Mirrors: third_party/ducklake/test/sql/checkpoint/view_checkpoint.test
    //
    // Storage contract:
    // - Checkpointing with a view must not drop current view metadata.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let view = TableId(100);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table_in_schema(&mut kv, catalog, table, SchemaId(1), "t", 3);
    commit_create_view_row(&mut kv, catalog, view_row(view, "v", "select a from t")).unwrap();
    let before_checkpoint = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        list_views_at(&kv, catalog, before_checkpoint.order)
            .unwrap()
            .into_iter()
            .map(|view| view.name)
            .collect::<Vec<_>>(),
        vec!["v"]
    );
    assert!(
        load_view_at(&kv, catalog, view, before_checkpoint.order)
            .unwrap()
            .is_some()
    );
}

#[test]
fn mirrors_cleanup_old_files_test_compaction_cleanup_drains_scheduled_files() {
    // Mirrors: third_party/ducklake/test/sql/cleanup/cleanup_old_files.test
    //
    // Storage contract:
    // - Rewrite and merge compactions schedule replaced data/delete files for cleanup.
    // - Removing old files should drain those cleanup queues and leave current replacement
    //   metadata visible.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    create_table_in_schema(&mut kv, catalog, table, SchemaId(1), "t", 1);
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(1, table, 0, 3),
            data_file(2, table, 3, 2),
            data_file(3, table, 5, 2),
        ],
    )
    .unwrap();
    commit_data_mutation_with_file_partitions_and_inline_deletes(
        &mut kv,
        catalog,
        Vec::new(),
        vec![delete_file(1, 1, 2)],
        &[],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    commit_rewrite_delete_data_files(
        &mut kv,
        catalog,
        RewriteDeleteCompaction {
            source_file_ids: vec![DataFileId(1)],
            new_files: vec![data_file(4, table, 2, 1)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();
    commit_merge_adjacent_data_files(
        &mut kv,
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(2), DataFileId(3), DataFileId(4)],
            new_files: vec![data_file(5, table, 2, 5)],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let old_data = list_old_data_files_for_cleanup(&kv, catalog).unwrap();
    let old_delete = list_old_delete_files_for_cleanup(&kv, catalog).unwrap();
    assert_eq!(old_data.len(), 4);
    assert_eq!(old_delete.len(), 1);

    remove_old_data_files(
        &mut kv,
        catalog,
        &old_data
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    remove_old_delete_files(
        &mut kv,
        catalog,
        &old_delete
            .iter()
            .map(|row| row.delete_file.delete_file_id)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(
        list_old_data_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert!(
        list_old_delete_files_for_cleanup(&kv, catalog)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_current_data_files_with_deletes(&kv, catalog, table).unwrap()[0]
            .data_file
            .data_file_id,
        DataFileId(5)
    );
}

#[test]
fn mirrors_rename_view_preserves_comment_in_transaction_test_comment_survives_view_rename() {
    // Mirrors: third_party/ducklake/test/sql/comments/rename_view_preserves_comment_in_transaction.test
    //
    // Storage contract:
    // - DuckLake creates a view, changes its comment, and renames it.
    // - Current view metadata must retain the comment under the renamed view.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let view = TableId(100);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_create_view_row(&mut kv, catalog, view_row(view, "v", "select * from t")).unwrap();
    commit_change_view_comment(
        &mut kv,
        catalog,
        &ViewCommentChange::new(view, Some("view comment")),
    )
    .unwrap();
    commit_rename_views(&mut kv, catalog, &[ViewRename::new(view, "v2")]).unwrap();

    let current = current_views(&kv, catalog);
    assert_eq!(current[0].name, "v2");
    assert_eq!(current[0].comment.as_deref(), Some("view comment"));
}

fn schema(id: u64, name: &str) -> SchemaRow {
    SchemaRow::new(
        SchemaId(id),
        format!("schema-{id}"),
        name,
        format!("main/{name}"),
        CatalogOrderId::uuid_v7(0),
    )
}

fn create_table_in_schema(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    table: TableId,
    schema: SchemaId,
    name: &str,
    column_count: u64,
) -> TableRow {
    commit_create_table_row(
        kv,
        catalog,
        TableRow::with_catalog_metadata(
            table,
            schema,
            format!("table-{}", table.0),
            name,
            format!("main/{name}"),
            (1..=column_count)
                .map(|id| {
                    TableColumnRow::new(ColumnId(id), format!("c{id}"), "integer", true, None)
                })
                .collect(),
            CatalogOrderId::uuid_v7(0),
        ),
    )
    .unwrap()
}

fn table_with_inline(table: TableId, schema: SchemaId) -> TableRow {
    let mut row = TableRow::with_catalog_metadata(
        table,
        schema,
        format!("table-{}", table.0),
        "t",
        "main/t",
        vec![TableColumnRow::new(ColumnId(1), "a", "integer", true, None)],
        CatalogOrderId::uuid_v7(0),
    );
    row.inlined_data_tables
        .push(ducklake_catalog::InlinedTableRow::new(
            format!("ducklake_inlined_data_{}_{}", table.0, schema.0),
            schema.0,
        ));
    row
}

fn view_row(view: TableId, name: &str, sql: &str) -> ViewRow {
    ViewRow::new(
        view,
        SchemaId(1),
        format!("view-{}", view.0),
        name,
        "duckdb",
        sql,
        vec!["a".to_owned()],
        CatalogOrderId::uuid_v7(0),
    )
}

fn data_file(id: u64, table: TableId, row_id_start: u64, record_count: u64) -> DataFileRow {
    DataFileRow::new(
        DataFileId(id),
        table,
        format!("main/table-{}/file-{id}.parquet", table.0),
        record_count,
        1024,
        CatalogOrderId::uuid_v7(0),
    )
    .with_row_id_start(row_id_start)
}

fn delete_file(id: u64, data_file_id: u64, record_count: u64) -> ducklake_catalog::DeleteFileRow {
    ducklake_catalog::DeleteFileRow::new(
        ducklake_catalog::DeleteFileId(id),
        DataFileId(data_file_id),
        format!("main/delete-{id}.parquet"),
        record_count,
        512,
        CatalogOrderId::uuid_v7(0),
    )
}

fn inline_payload(rows: &[(u64, i64)]) -> Vec<u8> {
    rows.iter()
        .map(|(row_id, value)| format!("row\t{row_id}\ta:{value}\n"))
        .collect::<Vec<_>>()
        .join("")
        .into_bytes()
}

fn current_schemas(kv: &FakeOrderedCatalogKv, catalog: CatalogId) -> Vec<SchemaRow> {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    list_schemas_at(kv, catalog, latest.order).unwrap()
}

fn current_views(kv: &FakeOrderedCatalogKv, catalog: CatalogId) -> Vec<ViewRow> {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    list_views_at(kv, catalog, latest.order).unwrap()
}

fn assert_current_schema_names(kv: &FakeOrderedCatalogKv, catalog: CatalogId, expected: &[&str]) {
    let names = current_schemas(kv, catalog)
        .into_iter()
        .map(|schema| schema.name)
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
}

#[allow(dead_code)]
fn expire_all_but_latest(kv: &mut FakeOrderedCatalogKv, catalog: CatalogId) {
    let latest = latest_snapshot(kv, catalog).unwrap().unwrap();
    let sequences = list_snapshots(kv, catalog)
        .unwrap()
        .into_iter()
        .map(|snapshot| snapshot.sequence)
        .filter(|sequence| *sequence != latest.sequence && *sequence != RawSnapshotSequence(0))
        .collect::<Vec<_>>();
    expire_snapshots(kv, catalog, &sequences).unwrap();
}
