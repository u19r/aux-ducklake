#![cfg(feature = "foundationdb")]

use ducklake_catalog::{
    CatalogError, CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbDataMutation,
    FdbOrderedCatalogKv, InlineTableFlush, InlinedTableRow, SchemaId, TableColumnRow, TableId,
    TableRow, latest_snapshot, list_data_files_at,
};

#[test]
#[ignore = "requires an owned FoundationDB cluster and a separate writer process"]
fn given_cached_preparation_when_another_process_flushes_then_retry_cannot_materialize_twice() {
    assert_eq!(std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref(), Ok("1"));
    if let Ok(prefix) = std::env::var("AUX_DUCKLAKE_CACHE_CHILD_PREFIX") {
        let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        let snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
        kv.commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            flush_mutation(&snapshot, "first.parquet"),
        )
        .unwrap();
        return;
    }
    let prefix = format!(
        "ducklake-review/flush-cache/{}/{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.clone().into_bytes()).unwrap();
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
                    "INTEGER",
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
        .push(InlinedTableRow::new("events_inline", 0));
    kv.register_inline_table_payload_with_table_versionstamped(
        CatalogId(1),
        table,
        SchemaId(0),
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let snapshot = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    // Missing drop validation runs after preparation. Failure must not leave
    // mutable visibility cached for a later attempt in this process.
    let mut invalid = flush_mutation(&snapshot, "aborted.parquet");
    invalid.dropped_data_file_ids.push(DataFileId(999_999));
    assert!(matches!(
        kv.commit_data_mutation_versionstamped(CatalogId(1), None, invalid),
        Err(CatalogError::NotFound("data file"))
    ));
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "given_cached_preparation_when_another_process_flushes_then_retry_cannot_materialize_twice", "--ignored", "--nocapture"])
        .env("AUX_DUCKLAKE_CACHE_CHILD_PREFIX", &prefix).output().unwrap();
    assert!(
        output.status.success(),
        "child: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let retry = kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        flush_mutation(&snapshot, "duplicate.parquet"),
    );
    assert!(
        matches!(retry, Err(CatalogError::InvalidMutation(ref message)) if message.contains("stale")),
        "stale retry was accepted: {retry:?}"
    );
    let current = latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let files = list_data_files_at(&kv, CatalogId(1), TableId(1), current.order).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "first.parquet");
}

fn flush_mutation(snapshot: &ducklake_catalog::SnapshotRow, path: &str) -> FdbDataMutation {
    FdbDataMutation::new(
        vec![
            DataFileRow::new(DataFileId(100), TableId(1), path, 1, 10, snapshot.order)
                .with_row_id_start(0)
                .with_max_partial_order(Some(snapshot.order)),
        ],
        Vec::new(),
        vec![InlineTableFlush::new(
            TableId(1),
            SchemaId(0),
            snapshot.sequence,
        )],
        Vec::new(),
        Vec::new(),
    )
}
