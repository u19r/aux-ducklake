use super::*;
use crate::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FilePartitionValueRow, InlinedTableRow, MacroId, PartitionKeyIndex, SchemaId, TableColumnRow,
    TableId, TableRow, list_data_files_at, load_macro_at, load_schema_at, load_table_at,
    load_view_at, register_file_partition_value, runtime_protocol::RuntimeCatalogBackend,
};
use std::sync::Mutex;
#[cfg(feature = "foundationdb")]
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn ffi_probe_round_trips_runtime_frame() {
    let request = RuntimeRequest::new(
        "ffi-probe",
        RuntimeCatalogBackend::FoundationDb,
        "RuntimeProtocolProbe",
        b"catalog=1".to_vec(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status =
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut out) };

    assert_eq!(status, FFI_OK);
    assert!(!out.ptr.is_null());
    let response_bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { ducklake_catalog_runtime_free(out.ptr, out.len) };
    let response = RuntimeResponse::decode(&response_bytes).unwrap();
    assert_eq!(response.request_id, "ffi-probe");
    assert_eq!(response.status, RuntimeResponseStatus::Ok);
    let payload = String::from_utf8(response.payload).unwrap();
    assert!(payload.contains("runtime_ffi=ok"));
    assert!(payload.contains("backend=foundationdb"));
}

#[test]
fn ffi_resolve_catalog_id_returns_namespaced_foundationdb_identity() {
    let first = runtime_request_payload_for_catalog(
        "ffi-resolve-fdb-identity-a",
        RuntimeCatalogBackend::FoundationDb,
        CatalogId(99),
        "ResolveCatalogId",
        "metadata_path=/tmp/a.duckdb\nmetadata_database=metadata_a\nmetadata_schema=\n".to_owned(),
    );
    let second = runtime_request_payload_for_catalog(
        "ffi-resolve-fdb-identity-b",
        RuntimeCatalogBackend::FoundationDb,
        CatalogId(100),
        "ResolveCatalogId",
        "metadata_path=/tmp/b.duckdb\nmetadata_database=metadata_b\nmetadata_schema=\n".to_owned(),
    );

    assert!(first.contains("operation=ResolveCatalogId"));
    let first_id = payload_line_u64(&first, "runtime_catalog_id");
    let second_id = payload_line_u64(&second, "runtime_catalog_id");
    assert_ne!(first_id, second_id);
    assert!(first_id > 1);
    assert!(second_id > 1);
}

#[test]
fn ffi_resolve_catalog_id_uses_explicit_runtime_catalog_identity() {
    let first = runtime_request_payload_for_catalog(
        "ffi-resolve-explicit-identity-a",
        RuntimeCatalogBackend::FoundationDb,
        CatalogId(99),
        "ResolveCatalogId",
        "metadata_path=/tmp/a.duckdb\nmetadata_database=metadata_a\nmetadata_schema=\ncatalog_identity=benchmark-shared\n"
            .to_owned(),
    );
    let same = runtime_request_payload_for_catalog(
        "ffi-resolve-explicit-identity-b",
        RuntimeCatalogBackend::FoundationDb,
        CatalogId(100),
        "ResolveCatalogId",
        "metadata_path=/tmp/b.duckdb\nmetadata_database=metadata_b\nmetadata_schema=other\ncatalog_identity=benchmark-shared\n"
            .to_owned(),
    );
    let different = runtime_request_payload_for_catalog(
        "ffi-resolve-explicit-identity-c",
        RuntimeCatalogBackend::FoundationDb,
        CatalogId(101),
        "ResolveCatalogId",
        "metadata_path=/tmp/a.duckdb\nmetadata_database=metadata_a\nmetadata_schema=\ncatalog_identity=benchmark-other\n"
            .to_owned(),
    );

    let first_id = payload_line_u64(&first, "runtime_catalog_id");
    assert_eq!(first_id, payload_line_u64(&same, "runtime_catalog_id"));
    assert_ne!(first_id, payload_line_u64(&different, "runtime_catalog_id"));
    assert!(first_id > 1);
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_partition_file_scans_read_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-partition-files");
    let table = TableId(47);
    let partition_key = PartitionKeyIndex(0);
    let mut kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_partition_files", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let committed = kv
        .append_data_files_versionstamped(
            CatalogId(1),
            vec![
                DataFileRow::new(
                    DataFileId(30),
                    table,
                    "main/runtime/fdb-partition-eu.parquet",
                    17,
                    4096,
                    initial.order,
                ),
                DataFileRow::new(
                    DataFileId(31),
                    table,
                    "main/runtime/fdb-partition-us.parquet",
                    19,
                    8192,
                    initial.order,
                ),
            ],
        )
        .unwrap();
    register_file_partition_value(
        &mut kv,
        CatalogId(1),
        FilePartitionValueRow::new(committed[0].data_file_id, table, partition_key, "eu"),
    )
    .unwrap();
    register_file_partition_value(
        &mut kv,
        CatalogId(1),
        FilePartitionValueRow::new(committed[1].data_file_id, table, partition_key, "us"),
    )
    .unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let current = runtime_request_payload(
        "ffi-fdb-current-partition-files",
        RuntimeCatalogBackend::FoundationDb,
        "ListCurrentDataFilesForPartitionScan",
        format!(
            "table_id={}\npartition_key_index={}\npartition_value=eu\n",
            table.0, partition_key.0
        ),
    );
    let at = runtime_request_payload(
        "ffi-fdb-partition-files-at",
        RuntimeCatalogBackend::FoundationDb,
        "ListDataFilesForPartitionScanAt",
        format!(
            "snapshot_id={}\ntable_id={}\npartition_key_index={}\npartition_value=eu\n",
            latest.sequence, table.0, partition_key.0
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    for payload in [current, at] {
        assert!(payload.contains("partition_value=eu"));
        assert!(payload.contains("file_count=1"));
        assert!(payload.contains(&format!(
            "file\t{}\t{}\tmain/runtime/fdb-partition-eu.parquet\t17\t4096",
            committed[0].data_file_id.0, table.0
        )));
        assert!(!payload.contains("fdb-partition-us.parquet"));
    }
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_partition_prune_file_scans_read_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-partition-prune-files");
    let table = TableId(48);
    let mut kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_partition_prune_files", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let committed = kv
        .append_data_files_versionstamped(
            CatalogId(1),
            vec![
                DataFileRow::new(
                    DataFileId(40),
                    table,
                    "main/runtime/fdb-partition-apac.parquet",
                    11,
                    4096,
                    initial.order,
                ),
                DataFileRow::new(
                    DataFileId(41),
                    table,
                    "main/runtime/fdb-partition-eu.parquet",
                    17,
                    4096,
                    initial.order,
                ),
                DataFileRow::new(
                    DataFileId(42),
                    table,
                    "main/runtime/fdb-partition-us.parquet",
                    19,
                    8192,
                    initial.order,
                ),
            ],
        )
        .unwrap();
    for (row, region, bucket) in [
        (&committed[0], "apac", "1"),
        (&committed[1], "eu", "2"),
        (&committed[2], "us", "10"),
    ] {
        register_file_partition_value(
            &mut kv,
            CatalogId(1),
            FilePartitionValueRow::new(row.data_file_id, table, PartitionKeyIndex(0), region),
        )
        .unwrap();
        register_file_partition_value(
            &mut kv,
            CatalogId(1),
            FilePartitionValueRow::new(row.data_file_id, table, PartitionKeyIndex(1), bucket),
        )
        .unwrap();
    }
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let current = runtime_request_payload(
        "ffi-fdb-current-partition-prune-files",
        RuntimeCatalogBackend::FoundationDb,
        "ListCurrentDataFilesForPartitionPrune",
        format!(
            "table_id={}\npartition_key_index=0\npartition_column_type=VARCHAR\ncomparison=greater_than_or_equal\npartition_value=eu\n",
            table.0
        ),
    );
    let at = runtime_request_payload(
        "ffi-fdb-partition-prune-files-at",
        RuntimeCatalogBackend::FoundationDb,
        "ListDataFilesForPartitionPruneAt",
        format!(
            "snapshot_id={}\ntable_id={}\npartition_key_index=1\npartition_column_type=INTEGER\ncomparison=greater_than_or_equal\npartition_value=2\n",
            latest.sequence, table.0
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert_eq!(current.matches("file\t").count(), 2, "{current}");
    assert!(current.contains("fdb-partition-eu.parquet"), "{current}");
    assert!(current.contains("fdb-partition-us.parquet"), "{current}");
    assert!(!current.contains("fdb-partition-apac.parquet"), "{current}");
    assert_eq!(at.matches("file\t").count(), 2, "{at}");
    assert!(at.contains("fdb-partition-eu.parquet"), "{at}");
    assert!(at.contains("fdb-partition-us.parquet"), "{at}");
    assert!(!at.contains("fdb-partition-apac.parquet"), "{at}");
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_inline_reads_and_change_feeds_read_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-inline-rows");
    let table_id = TableId(49);
    let schema_id = SchemaId(0);
    let inlined_table_name = "runtime_fdb_inline_inlined";
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            CatalogId(1),
            inline_test_table(table_id, "runtime_fdb_inline", schema_id),
            None,
        )
        .unwrap();
    let create_snapshot = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let mut with_inline = created.clone();
    with_inline
        .inlined_data_tables
        .push(InlinedTableRow::new(inlined_table_name, schema_id.0));
    kv.register_inline_table_payload_with_table_versionstamped(
        CatalogId(1),
        with_inline,
        schema_id,
        inline_test_payload(),
    )
    .unwrap();
    let insert_snapshot = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    kv.commit_delete_inline_table_rows_versionstamped(
        CatalogId(1),
        table_id,
        schema_id,
        &[2],
        None,
    )
    .unwrap();
    let delete_snapshot = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let read = runtime_request_payload(
        "ffi-fdb-inline-read",
        RuntimeCatalogBackend::FoundationDb,
        "ReadInlineRows",
        format!(
            "inlined_table_name={inlined_table_name}\nsnapshot_id={}\n",
            insert_snapshot.sequence
        ),
    );
    let read_after_delete = runtime_request_payload(
        "ffi-fdb-inline-read-after-delete",
        RuntimeCatalogBackend::FoundationDb,
        "ReadInlineRows",
        format!(
            "inlined_table_name={inlined_table_name}\nsnapshot_id={}\n",
            delete_snapshot.sequence
        ),
    );
    let insertions = runtime_request_payload(
        "ffi-fdb-inline-insertions",
        RuntimeCatalogBackend::FoundationDb,
        "ListInlineRowInsertions",
        format!(
            "inlined_table_name={inlined_table_name}\nstart_snapshot_id={}\nend_snapshot_id={}\n",
            create_snapshot.sequence, insert_snapshot.sequence
        ),
    );
    let deletions = runtime_request_payload(
        "ffi-fdb-inline-deletions",
        RuntimeCatalogBackend::FoundationDb,
        "ListInlineRowDeletions",
        format!(
            "inlined_table_name={inlined_table_name}\nstart_snapshot_id={}\nend_snapshot_id={}\n",
            insert_snapshot.sequence, delete_snapshot.sequence
        ),
    );
    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(read.contains("inline_payload_count=1"));
    assert!(read.contains(&format!(
        "row_change\t{}\t\t1\ti:7\ts:736576656e",
        insert_snapshot.sequence
    )));
    assert!(read.contains(&format!(
        "row_change\t{}\t\t2\ti:8\ts:6569676874",
        insert_snapshot.sequence
    )));
    assert!(read_after_delete.contains(&format!(
        "row_change\t{}\t\t1\ti:7\ts:736576656e",
        insert_snapshot.sequence
    )));
    assert!(!read_after_delete.contains("s:6569676874"));
    assert!(insertions.contains("inline_row_change_count=2"));
    assert!(insertions.contains(&format!(
        "row_change\t{}\t\t1\ti:7\ts:736576656e",
        insert_snapshot.sequence
    )));
    assert!(deletions.contains("inline_row_change_count=1"));
    assert!(deletions.contains(&format!(
        "row_change\t\t{}\t2\ti:8\ts:6569676874",
        delete_snapshot.sequence
    )));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_change_feed_reads_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-change-feed");
    let table = TableId(51);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_change_feed", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let commit = kv
        .commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            vec![
                DataFileRow::new(
                    DataFileId(41),
                    table,
                    "main/runtime/fdb-change-feed.parquet",
                    29,
                    8192,
                    initial.order,
                )
                .with_row_id_start(200),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [committed] = commit.data_files.try_into().unwrap();
    let appended = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![committed.data_file_id],
    )
    .unwrap();
    let expired = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let start_public = crate::public_snapshot_sequence_for_order(&kv, CatalogId(1), initial.order)
        .unwrap()
        .unwrap()
        .0;
    let appended_public =
        crate::public_snapshot_sequence_for_order(&kv, CatalogId(1), appended.order)
            .unwrap()
            .unwrap()
            .0;
    let expired_public =
        crate::public_snapshot_sequence_for_order(&kv, CatalogId(1), expired.order)
            .unwrap()
            .unwrap()
            .0;
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");
    let payload = format!(
        "table_id={}\nstart_snapshot_id={}\nend_snapshot_id={}\n",
        table.0, start_public, expired_public
    );

    let changes = runtime_request_payload(
        "ffi-fdb-change-feed",
        RuntimeCatalogBackend::FoundationDb,
        "ListDataFileChanges",
        payload.clone(),
    );
    let deletions = runtime_request_payload(
        "ffi-fdb-table-deletions",
        RuntimeCatalogBackend::FoundationDb,
        "ListTableDeletions",
        payload,
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(changes.contains("change_count=1"), "{changes}");
    assert!(changes.contains(&format!(
        "change_file\tadded\t{}\t{}\t{}\tmain/runtime/fdb-change-feed.parquet\t29\t8192\t200",
        appended_public, committed.data_file_id.0, table.0
    )));
    assert!(deletions.contains("deletion_scan_count=1"));
    assert!(deletions.contains(&format!(
        "delete_scan\tfull\t{}\t{}\t{}\tmain/runtime/fdb-change-feed.parquet\t29\t8192\t200",
        expired_public, committed.data_file_id.0, table.0
    )));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_get_catalog_for_snapshot_reads_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-catalog-snapshot");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let initialized = kv
        .initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");
    let payload = format!("snapshot_id={}\n", initialized.sequence);
    let request = RuntimeRequest::new(
        "ffi-fdb-catalog-snapshot",
        RuntimeCatalogBackend::FoundationDb,
        "GetCatalogForSnapshot",
        payload.into_bytes(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status =
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut out) };

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert_eq!(status, FFI_OK);
    let response_bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { ducklake_catalog_runtime_free(out.ptr, out.len) };
    let response = RuntimeResponse::decode(&response_bytes).unwrap();
    assert_eq!(response.status, RuntimeResponseStatus::Ok);
    let payload = String::from_utf8(response.payload).unwrap();
    assert!(payload.contains("table_count=0"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_metadata_attach_and_initialization_use_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-metadata-init");
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let attach = runtime_request_payload(
        "ffi-fdb-attach-metadata",
        RuntimeCatalogBackend::FoundationDb,
        "AttachMetadata",
        "catalog=1".to_owned(),
    );
    let before = runtime_request_payload(
        "ffi-fdb-metadata-exists-before",
        RuntimeCatalogBackend::FoundationDb,
        "MetadataExists",
        String::new(),
    );
    let initialized = runtime_request_payload(
        "ffi-fdb-initialize-ducklake",
        RuntimeCatalogBackend::FoundationDb,
        "InitializeDuckLake",
        String::new(),
    );
    let after = runtime_request_payload(
        "ffi-fdb-metadata-exists-after",
        RuntimeCatalogBackend::FoundationDb,
        "MetadataExists",
        String::new(),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(attach.contains("operation=AttachMetadata"));
    assert!(attach.contains("metadata_attached=true"));
    assert!(attach.contains("runtime_catalog_id=1"));
    assert!(before.contains("metadata_exists=false"));
    assert!(initialized.contains("operation=InitializeDuckLake"));
    assert!(initialized.contains("catalog_snapshot_order="));
    assert!(after.contains("metadata_exists=false"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_create_schemas_mutates_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-create-schemas");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let created = runtime_request_payload(
        "ffi-fdb-create-schemas",
        RuntimeCatalogBackend::FoundationDb,
        "CreateSchemas",
        "schema\t8\truntime-fdb-schema-uuid\truntime_fdb_schema\tmain/runtime_fdb_schema/\n"
            .to_owned(),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let schema = load_schema_at(&kv, CatalogId(1), SchemaId(8), latest.order)
        .unwrap()
        .unwrap();
    assert!(created.contains("created_schema_count=1"));
    assert!(created.contains(
        "schema\t8\truntime-fdb-schema-uuid\truntime_fdb_schema\tmain/runtime_fdb_schema/"
    ));
    assert_eq!(schema.name, "runtime_fdb_schema");
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_create_tables_mutates_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-create-tables");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let created = runtime_request_payload(
        "ffi-fdb-create-tables",
        RuntimeCatalogBackend::FoundationDb,
        "CreateTables",
        create_tables_runtime_payload(TableId(10), "runtime_fdb_table"),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let table = load_table_at(&kv, CatalogId(1), TableId(10), latest.order)
        .unwrap()
        .unwrap();
    assert!(created.contains("created_table_count=1"));
    assert_eq!(table.name, "runtime_fdb_table");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[1].comment.as_deref(), Some("text note"));
    assert_eq!(table.columns[1].default_value.as_deref(), Some("'new'"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_create_table_commit_snapshot_keeps_public_snapshot_id_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-create-table-public-snapshot");
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let initial = runtime_request_payload(
        "ffi-fdb-public-initialize",
        RuntimeCatalogBackend::FoundationDb,
        "InitializeDuckLake",
        String::new(),
    );
    let created = runtime_request_payload(
        "ffi-fdb-public-create-schema",
        RuntimeCatalogBackend::FoundationDb,
        "CreateSchemas",
        "commit_snapshot\t1\nschema\t0\truntime-fdb-public-main-uuid\tmain\tmain/\n".to_owned(),
    );
    let created_table = runtime_request_payload(
        "ffi-fdb-public-create-table",
        RuntimeCatalogBackend::FoundationDb,
        "CreateTables",
        format!(
            "commit_snapshot\t1\n{}",
            create_tables_runtime_payload(TableId(14), "runtime_fdb_public_snapshot")
        ),
    );
    let inline_table = runtime_request_payload(
        "ffi-fdb-public-register-inline-table",
        RuntimeCatalogBackend::FoundationDb,
        "RegisterInlineTables",
        "commit_snapshot\t1\ntable\t14\t1\tducklake_inlined_data_14_1\n".to_owned(),
    );
    let catalog_after_inline_table = runtime_request_payload(
        "ffi-fdb-public-catalog-after-inline-table",
        RuntimeCatalogBackend::FoundationDb,
        "GetCatalogForSnapshot",
        "snapshot_id=1\n".to_owned(),
    );
    let inline_rows = runtime_request_payload(
        "ffi-fdb-public-register-inline-rows",
        RuntimeCatalogBackend::FoundationDb,
        "RegisterInlineRows",
        "commit_snapshot\t1\n\
         table\t14\t1\tducklake_inlined_data_14_1\n\
         row\t1\ti:7\ts:736576656e\n"
            .to_owned(),
    );
    let read_rows = runtime_request_payload(
        "ffi-fdb-public-read-inline-rows",
        RuntimeCatalogBackend::FoundationDb,
        "ReadInlineRows",
        "inlined_table_name=ducklake_inlined_data_14_1\nsnapshot_id=1\n".to_owned(),
    );
    let snapshot = runtime_request_payload(
        "ffi-fdb-public-get-snapshot",
        RuntimeCatalogBackend::FoundationDb,
        "GetSnapshot",
        "catalog=1".to_owned(),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(initial.contains("operation=InitializeDuckLake"));
    assert!(created.contains("created_schema_count=1"));
    assert!(created_table.contains("created_table_count=1"));
    assert!(inline_table.contains("inline_table_count=1"));
    assert!(
        catalog_after_inline_table.contains("inlined_table\t14\tducklake_inlined_data_14_1\t1"),
        "{catalog_after_inline_table}"
    );
    assert!(inline_rows.contains("inline_chunk_count=1"));
    assert!(
        read_rows.contains("row_change\t1\t\t1\ti:7\ts:736576656e"),
        "{read_rows}"
    );
    assert!(snapshot.contains("ducklake_snapshot_id=1"), "{snapshot}");
    assert!(snapshot.contains("catalog_snapshot_sequence=1"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_standalone_inline_insert_then_flush_keeps_single_snapshot_insert_feed_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-inline-flush-insert-feed");
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let initialized = runtime_request_payload(
        "ffi-fdb-inline-flush-init",
        RuntimeCatalogBackend::FoundationDb,
        "InitializeDuckLake",
        String::new(),
    );
    let created_table = runtime_request_payload(
        "ffi-fdb-inline-flush-create-table",
        RuntimeCatalogBackend::FoundationDb,
        "CreateTables",
        create_tables_runtime_payload(TableId(15), "runtime_fdb_inline_flush"),
    );
    let registered_inline_table = runtime_request_payload(
        "ffi-fdb-inline-flush-register-table",
        RuntimeCatalogBackend::FoundationDb,
        "RegisterInlineTables",
        "table\t15\t1\tducklake_inlined_data_15_1\n".to_owned(),
    );
    let latest_after_inline_table = runtime_request_payload(
        "ffi-fdb-inline-flush-snapshot-after-table",
        RuntimeCatalogBackend::FoundationDb,
        "GetSnapshot",
        String::new(),
    );
    let inline_insert_snapshot = ducklake_snapshot_id_from_payload(&latest_after_inline_table) + 1;
    let registered_inline_rows = runtime_request_payload(
        "ffi-fdb-inline-flush-register-rows",
        RuntimeCatalogBackend::FoundationDb,
        "RegisterInlineRows",
        format!(
            "commit_snapshot\t{inline_insert_snapshot}\n\
             table\t15\t1\tducklake_inlined_data_15_1\n\
             row\t3\tn:\n\
             row\t4\ti:110\n"
        ),
    );
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let raw_inline_insert = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let public_inline_insert =
        crate::public_snapshot_sequence_for_order(&kv, CatalogId(1), raw_inline_insert.order)
            .unwrap()
            .unwrap()
            .0;
    drop(kv);
    let insertions_before_flush = runtime_request_payload(
        "ffi-fdb-inline-flush-insertions-before",
        RuntimeCatalogBackend::FoundationDb,
        "ListInlineRowInsertions",
        format!(
            "inlined_table_name=ducklake_inlined_data_15_1\n\
             start_snapshot_id={public_inline_insert}\n\
             end_snapshot_id={public_inline_insert}\n"
        ),
    );
    let latest_after_inline_rows = runtime_request_payload(
        "ffi-fdb-inline-flush-snapshot-after-rows",
        RuntimeCatalogBackend::FoundationDb,
        "GetSnapshot",
        String::new(),
    );
    let flush_snapshot = ducklake_snapshot_id_from_payload(&latest_after_inline_rows) + 1;
    let flushed = runtime_request_payload(
        "ffi-fdb-inline-flush-data-mutation",
        RuntimeCatalogBackend::FoundationDb,
        "CommitDataMutation",
        format!(
            "commit_snapshot\t{flush_snapshot}\n\
             file\t30\t15\tmain/runtime/inline-flush.parquet\t2\t565\t3\t\t438\t{inline_insert_snapshot}\t{inline_insert_snapshot}\n\
             file_column_stats\t30\t15\t1\t2\t0\t10\t110\t\n\
             inline_table\tducklake_inlined_data_15_1\t{inline_insert_snapshot}\n"
        ),
    );
    let insertions_after_flush = runtime_request_payload(
        "ffi-fdb-inline-flush-insertions-after",
        RuntimeCatalogBackend::FoundationDb,
        "ListInlineRowInsertions",
        format!(
            "inlined_table_name=ducklake_inlined_data_15_1\n\
             start_snapshot_id={public_inline_insert}\n\
             end_snapshot_id={public_inline_insert}\n"
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(initialized.contains("operation=InitializeDuckLake"));
    assert!(created_table.contains("created_table_count=1"));
    assert!(registered_inline_table.contains("inline_table_count=1"));
    assert!(registered_inline_rows.contains("inline_chunk_count=1"));
    assert!(flushed.contains("appended_file_count=1"));
    assert_inline_flush_insertions(
        "before flush",
        &insertions_before_flush,
        public_inline_insert,
    );
    assert_inline_flush_insertions("after flush", &insertions_after_flush, public_inline_insert);
}

#[cfg(feature = "foundationdb")]
fn assert_inline_flush_insertions(label: &str, payload: &str, public_inline_insert: u64) {
    assert!(
        payload.contains("inline_row_change_count=2"),
        "{label}: {payload}"
    );
    assert!(
        payload.contains(&format!("row_change\t{public_inline_insert}\t\t3\tn:")),
        "{label}: {payload}"
    );
    assert!(
        payload.contains(&format!("row_change\t{public_inline_insert}\t\t4\ti:110")),
        "{label}: {payload}"
    );
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_commit_data_mutation_mutates_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-data-mutation");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(TableId(12), "runtime_fdb_data_mutation", SchemaId(0)),
        None,
    )
    .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let committed = runtime_request_payload(
        "ffi-fdb-commit-data-mutation",
        RuntimeCatalogBackend::FoundationDb,
        "CommitDataMutation",
        data_mutation_runtime_payload(TableId(12), DataFileId(16), 42),
    );
    let batch = runtime_request_payload(
        "ffi-fdb-commit-metadata-batch",
        RuntimeCatalogBackend::FoundationDb,
        "CommitMetadataBatch",
        String::new(),
    );
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    let inline_file_deletions = runtime_request_payload(
        "ffi-fdb-inline-file-deletions-exist",
        RuntimeCatalogBackend::FoundationDb,
        "InlineFileDeletionsExist",
        format!("table_id=12\nsnapshot_id={}\n", latest.sequence),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let files = crate::list_data_files_at(&kv, CatalogId(1), TableId(12), latest.order).unwrap();
    assert!(committed.contains("appended_file_count=1"));
    assert!(committed.contains("file_partition_value_count=1"));
    assert!(batch.contains("metadata_batch_committed=true"));
    assert!(inline_file_deletions.contains("inline_file_deletion_exists=true"));
    assert!(inline_file_deletions.contains("inline_file_deletion_file_count=1"));
    assert!(inline_file_deletions.contains("inline_file_deletion_row_count=1"));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "main/runtime/data-mutation-16.parquet");
    assert_eq!(files[0].row_id_start, 42);
    assert_eq!(files[0].footer_size, Some(512));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_object_mutations_mutate_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-object-mutations");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    assert!(
        runtime_request_payload(
            "ffi-fdb-object-create-table",
            RuntimeCatalogBackend::FoundationDb,
            "CreateTables",
            create_tables_runtime_payload(TableId(22), "runtime_fdb_drop_table"),
        )
        .contains("created_table_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-drop-table",
            RuntimeCatalogBackend::FoundationDb,
            "DropTables",
            "table\t22\n".to_owned(),
        )
        .contains("dropped_table_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-create-view",
            RuntimeCatalogBackend::FoundationDb,
            "CreateViews",
            create_view_runtime_payload(TableId(32), "runtime_fdb_view"),
        )
        .contains("created_view_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-rename-view",
            RuntimeCatalogBackend::FoundationDb,
            "RenameViews",
            "view\t32\truntime_fdb_view_renamed\n".to_owned(),
        )
        .contains("renamed_view_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-comment-view",
            RuntimeCatalogBackend::FoundationDb,
            "ChangeViewComment",
            "view_comment\t32\tvalue\tfdb view comment\n".to_owned(),
        )
        .contains("changed_view_comment_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-create-macro",
            RuntimeCatalogBackend::FoundationDb,
            "CreateMacros",
            create_macro_runtime_payload(MacroId(42), "runtime_fdb_macro"),
        )
        .contains("created_macro_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-drop-macro",
            RuntimeCatalogBackend::FoundationDb,
            "DropMacros",
            "macro\t42\n".to_owned(),
        )
        .contains("dropped_macro_count=1")
    );
    assert!(
        runtime_request_payload(
            "ffi-fdb-object-drop-view",
            RuntimeCatalogBackend::FoundationDb,
            "DropViews",
            "view\t32\n".to_owned(),
        )
        .contains("dropped_view_count=1")
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    assert!(
        load_table_at(&kv, CatalogId(1), TableId(22), latest.order)
            .unwrap()
            .is_none()
    );
    assert!(
        load_view_at(&kv, CatalogId(1), TableId(32), latest.order)
            .unwrap()
            .is_none()
    );
    assert!(
        load_macro_at(&kv, CatalogId(1), MacroId(42), latest.order)
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_schema_changes_mutate_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-schema-changes");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    assert!(
        runtime_request_payload(
            "ffi-fdb-schema-create-table",
            RuntimeCatalogBackend::FoundationDb,
            "CreateTables",
            create_tables_runtime_payload(TableId(62), "runtime_fdb_schema_table"),
        )
        .contains("created_table_count=1")
    );
    run_schema_change_sequence(
        RuntimeCatalogBackend::FoundationDb,
        TableId(62),
        "runtime_fdb_schema",
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    assert_schema_change_sequence_result(&kv, TableId(62), "runtime_fdb_schema_table_renamed");
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_list_snapshots_reads_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-list-snapshots");
    let table = TableId(52);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_snapshot_list", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        vec![DataFileRow::new(
            DataFileId(42),
            table,
            "main/runtime/fdb-snapshot-list.parquet",
            5,
            1024,
            initial.order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(43),
            DataFileId(42),
            "main/runtime/fdb-snapshot-list-delete.parquet",
            2,
            128,
            initial.order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let snapshots = runtime_request_payload(
        "ffi-fdb-list-snapshots",
        RuntimeCatalogBackend::FoundationDb,
        "ListSnapshots",
        String::new(),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(snapshots.contains(&format!("snapshot\t{}\t", initial.sequence)));
    assert!(snapshots.contains(&format!("snapshot\t{}\t", latest.sequence)));
    assert!(snapshots.contains("inserted_into_table:52"));
    assert!(snapshots.contains("deleted_from_table:52"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_list_data_files_at_reads_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-list-files");
    let table = TableId(45);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_list_files", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let [committed] = kv
        .append_data_files_versionstamped(
            CatalogId(1),
            vec![
                DataFileRow::new(
                    DataFileId(10),
                    table,
                    "main/runtime/fdb-file.parquet",
                    7,
                    1024,
                    initial.order,
                )
                .with_row_id_start(11),
            ],
        )
        .unwrap()
        .try_into()
        .unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");
    let payload = format!("snapshot_id={}\ntable_id={}\n", latest.sequence, table.0);
    let request = RuntimeRequest::new(
        "ffi-fdb-list-files",
        RuntimeCatalogBackend::FoundationDb,
        "ListDataFilesAt",
        payload.into_bytes(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status =
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut out) };

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert_eq!(status, FFI_OK);
    let response_bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { ducklake_catalog_runtime_free(out.ptr, out.len) };
    let response = RuntimeResponse::decode(&response_bytes).unwrap();
    assert_eq!(response.status, RuntimeResponseStatus::Ok);
    let payload = String::from_utf8(response.payload).unwrap();
    assert!(payload.contains("file_count=1"));
    assert!(payload.contains(&format!(
        "file\t{}\t{}\tmain/runtime/fdb-file.parquet\t7\t1024\t11",
        committed.data_file_id.0, table.0
    )));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_cleanup_listing_reads_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-cleanup-listing");
    let table = TableId(54);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_cleanup_listing", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let commit = kv
        .commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            vec![DataFileRow::new(
                DataFileId(61),
                table,
                "main/runtime/fdb-cleanup-known.parquet",
                37,
                8192,
                initial.order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [known_file] = commit.data_files.try_into().unwrap();
    let append_snapshot = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![known_file.data_file_id],
    )
    .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let expired = runtime_request_payload(
        "ffi-fdb-expire-snapshots",
        RuntimeCatalogBackend::FoundationDb,
        "ExpireSnapshots",
        format!("snapshot_ids={}\n", append_snapshot.sequence),
    );

    let known = runtime_request_payload(
        "ffi-fdb-known-files-cleanup",
        RuntimeCatalogBackend::FoundationDb,
        "ListKnownFilesForCleanup",
        String::new(),
    );
    let old = runtime_request_payload(
        "ffi-fdb-old-files-cleanup-empty",
        RuntimeCatalogBackend::FoundationDb,
        "ListOldFilesForCleanup",
        String::new(),
    );
    let removed = runtime_request_payload(
        "ffi-fdb-remove-cleanup-files",
        RuntimeCatalogBackend::FoundationDb,
        "RemoveCleanupFiles",
        format!("cleanup_file\tdata\t{}\n", known_file.data_file_id.0),
    );
    let old_after_remove = runtime_request_payload(
        "ffi-fdb-old-files-cleanup-after-remove",
        RuntimeCatalogBackend::FoundationDb,
        "ListOldFilesForCleanup",
        String::new(),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(expired.contains("expired_snapshot_count=1"));
    assert!(expired.contains(&format!("expired_snapshot\t{}", append_snapshot.sequence)));
    assert!(known.contains("known_file_count=1"));
    assert!(known.contains(&format!(
        "known_file\tdata\t{}\t{}\tmain/runtime/fdb-cleanup-known.parquet",
        known_file.data_file_id.0, table.0
    )));
    assert!(old.contains("cleanup_file_count=1"));
    assert!(old.contains(&format!(
        "cleanup_file\tdata\t{}\t{}\tmain/runtime/fdb-cleanup-known.parquet",
        known_file.data_file_id.0, table.0
    )));
    assert!(removed.contains("removed_cleanup_file_count=1"));
    assert!(removed.contains(&format!(
        "removed_cleanup_file\tdata\t{}",
        known_file.data_file_id.0
    )));
    assert!(old_after_remove.contains("cleanup_file_count=0"));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_fdb_cleanup_removal_preserves_reachable_delete_metadata_for_time_travel() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-delete-cleanup-time-travel");
    let catalog = CatalogId(1);
    let table = TableId(154);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    kv.create_table_versionstamped(
        catalog,
        inline_test_table(table, "runtime_fdb_delete_cleanup_time_travel", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
    let data_commit = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            vec![
                DataFileRow::new(
                    DataFileId(161),
                    table,
                    "main/runtime/fdb-delete-cleanup-data.parquet",
                    100,
                    8192,
                    initial.order,
                )
                .with_row_id_start(0),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [data_file] = data_commit.data_files.try_into().unwrap();
    let first_delete_commit = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(171),
                data_file.data_file_id,
                "main/runtime/fdb-delete-cleanup-delete-1.parquet",
                50,
                4096,
                data_file.validity.begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [first_delete_file] = first_delete_commit.delete_files.try_into().unwrap();
    let first_delete_snapshot = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
    let second_delete_commit = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            Vec::new(),
            vec![DeleteFileRow::new(
                DeleteFileId(172),
                data_file.data_file_id,
                "main/runtime/fdb-delete-cleanup-delete-2.parquet",
                75,
                4096,
                first_delete_file.validity.begin_order,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [second_delete_file] = second_delete_commit.delete_files.try_into().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let listed = runtime_request_payload(
        "ffi-fdb-list-reachable-delete-cleanup",
        RuntimeCatalogBackend::FoundationDb,
        "ListOldFilesForCleanup",
        "cleanup_all=true\n".to_owned(),
    );
    let removed = runtime_request_payload(
        "ffi-fdb-remove-reachable-delete-cleanup",
        RuntimeCatalogBackend::FoundationDb,
        "RemoveCleanupFiles",
        format!(
            "cleanup_file\tdelete\t{}\n",
            first_delete_file.delete_file_id.0
        ),
    );
    let historical_files = runtime_request_payload(
        "ffi-fdb-list-files-after-delete-cleanup",
        RuntimeCatalogBackend::FoundationDb,
        "ListDataFilesAt",
        format!(
            "snapshot_id={}\ntable_id={}\n",
            first_delete_snapshot.sequence, table.0
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(listed.contains("cleanup_file_count=1"), "{listed}");
    assert!(
        listed.contains(&format!(
            "cleanup_file\tdelete\t{}\t{}\tmain/runtime/fdb-delete-cleanup-delete-1.parquet",
            first_delete_file.delete_file_id.0, table.0
        )),
        "{listed}"
    );
    assert!(
        removed.contains("removed_cleanup_file_count=1"),
        "{removed}"
    );
    assert!(
        removed.contains(&format!(
            "removed_cleanup_file\tdelete\t{}",
            first_delete_file.delete_file_id.0
        )),
        "{removed}"
    );
    assert!(historical_files.contains("file_count=1"));
    assert!(
        historical_files.contains(&format!(
            "file\t{}\t{}\tmain/runtime/fdb-delete-cleanup-data.parquet\t100\t8192\t0\t\t{}\tmain/runtime/fdb-delete-cleanup-delete-2.parquet\t75\t4096",
            data_file.data_file_id.0, table.0, second_delete_file.delete_file_id.0,
        )),
        "{historical_files}"
    );
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_compaction_mutations_update_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-compaction");
    let table = TableId(57);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_compaction", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let commit = kv
        .commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            vec![
                DataFileRow::new(
                    DataFileId(90),
                    table,
                    "main/runtime/fdb-merge-left.parquet",
                    10,
                    512,
                    initial.order,
                )
                .with_row_id_start(0),
                DataFileRow::new(
                    DataFileId(91),
                    table,
                    "main/runtime/fdb-merge-right.parquet",
                    12,
                    512,
                    initial.order,
                )
                .with_row_id_start(10),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [left, right] = commit.data_files.try_into().unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let merged = runtime_request_payload(
        "ffi-fdb-merge-adjacent-files",
        RuntimeCatalogBackend::FoundationDb,
        "MergeAdjacentFiles",
        format!(
            "source_file\t{}\t{}\nsource_file\t{}\t{}\nfile\t{}\t{}\tmain/runtime/fdb-merged.parquet\t22\t1024\t0\n",
            table.0, left.data_file_id.0, table.0, right.data_file_id.0, 92, table.0
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(merged.contains("compacted_source_file_count=2"));
    assert!(merged.contains("compacted_new_file_count=1"));
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let current = list_data_files_at(&kv, CatalogId(1), table, latest.order).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file_id, DataFileId(92));
}

#[cfg(feature = "foundationdb")]
#[test]
fn ffi_rewrite_delete_mutation_updates_live_foundationdb_catalog_when_enabled() {
    if live_fdb_disabled() {
        return;
    }
    let _guard = ENV_LOCK.lock().unwrap();
    let prefix = unique_fdb_prefix("runtime-rewrite-delete");
    let table = TableId(58);
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(CatalogId(1))
        .unwrap();
    kv.create_table_versionstamped(
        CatalogId(1),
        inline_test_table(table, "runtime_fdb_rewrite_delete", SchemaId(0)),
        None,
    )
    .unwrap();
    let initial = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let commit = kv
        .commit_data_mutation_versionstamped(
            CatalogId(1),
            None,
            vec![
                DataFileRow::new(
                    DataFileId(100),
                    table,
                    "main/runtime/fdb-rewrite-source.parquet",
                    20,
                    2048,
                    initial.order,
                )
                .with_row_id_start(0),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let [source] = commit.data_files.try_into().unwrap();
    kv.commit_data_mutation_versionstamped(
        CatalogId(1),
        None,
        Vec::new(),
        vec![DeleteFileRow::new(
            DeleteFileId(101),
            source.data_file_id,
            "main/runtime/fdb-rewrite-delete.parquet",
            5,
            256,
            initial.order,
        )],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    drop(kv);
    set_env("AUX_DUCKLAKE_FDB_PREFIX", &prefix);
    remove_env("AUX_DUCKLAKE_FDB_CLUSTER_FILE");

    let rewritten = runtime_request_payload(
        "ffi-fdb-rewrite-delete-files",
        RuntimeCatalogBackend::FoundationDb,
        "RewriteDeleteFiles",
        format!(
            "source_file\t{}\t{}\nfile\t{}\t{}\tmain/runtime/fdb-rewrite-replacement.parquet\t15\t1536\t0\n",
            table.0, source.data_file_id.0, 102, table.0
        ),
    );

    remove_env("AUX_DUCKLAKE_FDB_PREFIX");
    assert!(rewritten.contains("rewritten_source_file_count=1"));
    assert!(rewritten.contains("rewritten_new_file_count=1"));
    let kv = crate::FdbOrderedCatalogKv::open_default_with_prefix(prefix.as_bytes()).unwrap();
    let latest = crate::latest_snapshot(&kv, CatalogId(1)).unwrap().unwrap();
    let current = list_data_files_at(&kv, CatalogId(1), table, latest.order).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data_file_id, DataFileId(102));
}

#[test]
fn ffi_probe_returns_error_frame_for_invalid_request() {
    let request = b"not-a-runtime-frame";
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status =
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut out) };

    assert_eq!(status, FFI_OK);
    let response_bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { ducklake_catalog_runtime_free(out.ptr, out.len) };
    let response = RuntimeResponse::decode(&response_bytes).unwrap();
    assert_eq!(response.status, RuntimeResponseStatus::Error);
    assert_eq!(response.request_id, "decode-error");
}

#[test]
fn ffi_probe_rejects_null_pointers() {
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status = unsafe { ducklake_catalog_runtime_probe(std::ptr::null(), 0, &mut out) };

    assert_eq!(status, FFI_INVALID_ARGUMENT);
    assert!(out.ptr.is_null());
}

fn set_env(key: &str, value: &str) {
    unsafe {
        std::env::set_var(key, value);
    }
}

fn remove_env(key: &str) {
    unsafe {
        std::env::remove_var(key);
    }
}

fn inline_test_table(table_id: TableId, name: &str, schema_id: SchemaId) -> TableRow {
    TableRow::with_catalog_metadata(
        table_id,
        schema_id,
        format!("{name}-uuid"),
        name,
        format!("main/{name}"),
        vec![
            TableColumnRow::new(ColumnId(1), "id", "INTEGER", false, None),
            TableColumnRow::new(ColumnId(2), "note", "VARCHAR", true, None),
        ],
        CatalogOrderId::uuid_v7(0),
    )
}

fn inline_test_payload() -> Vec<u8> {
    b"row\t1\ti:7\ts:736576656e\nrow\t2\ti:8\ts:6569676874\n".to_vec()
}

fn create_tables_runtime_payload(table_id: TableId, name: &str) -> String {
    format!(
        "table\t{}\t0\t{}-uuid\t{}\tmain/{}\t\n\
         column\t{}\t1\tid\tINTEGER\tfalse\t\t\t\tNULL\tliteral\n\
         column\t{}\t2\tnote\tVARCHAR\ttrue\t\ttext note\t\t'new'\tliteral\n",
        table_id.0, name, name, name, table_id.0, table_id.0
    )
}

fn data_mutation_runtime_payload(
    table_id: TableId,
    data_file_id: DataFileId,
    row_id_start: u64,
) -> String {
    format!(
        "commit_attempt\t9001\n\
         file\t{}\t{}\tmain/runtime/data-mutation-{}.parquet\t7\t2048\t{}\t\t512\n\
         file_partition\t{}\t{}\t1\teu\n\
         inline_file_delete\t{}\t{}\t{}\n",
        data_file_id.0,
        table_id.0,
        data_file_id.0,
        row_id_start,
        data_file_id.0,
        table_id.0,
        table_id.0,
        data_file_id.0,
        row_id_start
    )
}

fn create_view_runtime_payload(view_id: TableId, name: &str) -> String {
    format!(
        "view\t{}\t0\t{}-uuid\t{}\tduckdb\tSELECT 1 AS id\tid\t\n",
        view_id.0, name, name
    )
}

fn create_macro_runtime_payload(macro_id: MacroId, name: &str) -> String {
    format!(
        "macro\t{}\t0\t{}\nmacro_impl\t{}\t0\tduckdb\tSELECT 1\tscalar\nmacro_param\t{}\t0\t0\tx\tINTEGER\tNULL\tliteral\n",
        macro_id.0, name, macro_id.0, macro_id.0
    )
}

fn run_schema_change_sequence(
    backend: RuntimeCatalogBackend,
    table_id: TableId,
    name_prefix: &str,
) {
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-next-column-before-add"),
            backend,
            "GetNextColumnId",
            format!("table_id={}\n", table_id.0),
        )
        .contains("next_column_id=3")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-created-with-table"),
            backend,
            "IsColumnCreatedWithTable",
            format!("table_name={name_prefix}_table\ncolumn_name=id\n"),
        )
        .contains("created_with_table=true")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-add-columns"),
            backend,
            "AddColumns",
            schema_change_payload(
                2,
                &format!(
                    "column\t{}\t3\tstatus\tVARCHAR\ttrue\t\t'queued'\t'queued'\tliteral\n",
                    table_id.0
                ),
            ),
        )
        .contains("added_column_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-next-column-after-add"),
            backend,
            "GetNextColumnId",
            format!("table_id={}\n", table_id.0),
        )
        .contains("next_column_id=4")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-added-column-not-created-with-table"),
            backend,
            "IsColumnCreatedWithTable",
            format!("table_name={name_prefix}_table\ncolumn_name=status\n"),
        )
        .contains("created_with_table=false")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-rename-columns"),
            backend,
            "RenameColumns",
            schema_change_payload(
                3,
                &format!(
                    "column\t{}\t3\tstate\tVARCHAR\ttrue\t\t'queued'\t'queued'\tliteral\n",
                    table_id.0
                ),
            ),
        )
        .contains("renamed_column_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-change-column-types"),
            backend,
            "ChangeColumnTypes",
            schema_change_payload(
                4,
                &format!(
                    "column\t{}\t3\tstate\tBIGINT\ttrue\t\t'queued'\t'queued'\tliteral\n",
                    table_id.0
                ),
            ),
        )
        .contains("changed_column_type_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-change-column-defaults"),
            backend,
            "ChangeColumnDefaults",
            schema_change_payload(
                5,
                &format!(
                    "column\t{}\t3\tstate\tBIGINT\ttrue\t\t7\t7\tliteral\n",
                    table_id.0
                ),
            ),
        )
        .contains("changed_column_default_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-change-partition"),
            backend,
            "ChangePartitionKeys",
            schema_change_payload(
                6,
                &format!(
                    "partition\t{}\t10\npartition_field\t{}\t10\t0\t1\tidentity\n",
                    table_id.0, table_id.0
                ),
            ),
        )
        .contains("changed_partition_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-change-sort"),
            backend,
            "ChangeSortKeys",
            schema_change_payload(
                7,
                &format!(
                    "sort\t{}\t11\nsort_field\t{}\t11\t0\tid\tduckdb\tASC\tNULLS_FIRST\n",
                    table_id.0, table_id.0
                ),
            ),
        )
        .contains("changed_sort_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-drop-columns"),
            backend,
            "DropColumns",
            schema_change_payload(8, &format!("column\t{}\t3\n", table_id.0)),
        )
        .contains("dropped_column_count=1")
    );
    assert!(
        runtime_request_payload(
            &format!("ffi-{name_prefix}-rename-tables"),
            backend,
            "RenameTables",
            schema_change_payload(
                9,
                &format!("table\t{}\t{name_prefix}_table_renamed\n", table_id.0)
            ),
        )
        .contains("renamed_table_count=1")
    );
    let changed_comments = runtime_request_payload(
        &format!("ffi-{name_prefix}-change-comments"),
        backend,
        "ChangeComments",
        schema_change_payload(
            10,
            &format!(
                "table_comment\t{}\tvalue\tcomment for {name_prefix}\ncolumn_comment\t{}\t1\tvalue\tid comment for {name_prefix}\n",
                table_id.0, table_id.0
            ),
        ),
    );
    assert!(changed_comments.contains("changed_table_comment_count=1"));
    assert!(changed_comments.contains("changed_column_comment_count=1"));
}

fn schema_change_payload(commit_snapshot_id: u64, rows: &str) -> String {
    format!("commit_snapshot\t{commit_snapshot_id}\n{rows}")
}

fn assert_schema_change_sequence_result(
    kv: &impl crate::OrderedCatalogKv,
    table_id: TableId,
    expected_name: &str,
) {
    let latest = crate::latest_snapshot(kv, CatalogId(1)).unwrap().unwrap();
    let table = load_table_at(kv, CatalogId(1), table_id, latest.order)
        .unwrap()
        .unwrap();
    assert_eq!(table.name, expected_name);
    let expected_comment = format!(
        "comment for {}",
        expected_name.trim_end_matches("_table_renamed")
    );
    assert_eq!(table.comment.as_deref(), Some(expected_comment.as_str()));
    assert_eq!(table.columns.len(), 2);
    assert!(
        table.columns[0]
            .comment
            .as_deref()
            .is_some_and(|comment| comment.starts_with("id comment for "))
    );
    assert!(
        table
            .columns
            .iter()
            .all(|column| column.column_id != ColumnId(3))
    );
    assert_eq!(table.partition.unwrap().fields.len(), 1);
    assert_eq!(table.sort.unwrap().fields.len(), 1);
}

#[cfg(not(feature = "foundationdb"))]
#[test]
fn ffi_runtime_shutdown_is_idempotent() {
    assert_eq!(ducklake_catalog_runtime_shutdown(), FFI_OK);
    assert_eq!(ducklake_catalog_runtime_shutdown(), FFI_OK);
}

fn runtime_request_payload(
    request_id: &str,
    backend: RuntimeCatalogBackend,
    operation: &str,
    payload: String,
) -> String {
    runtime_request_payload_for_catalog(request_id, backend, CatalogId(1), operation, payload)
}

fn runtime_request_payload_for_catalog(
    request_id: &str,
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    operation: &str,
    payload: String,
) -> String {
    let request = RuntimeRequest::new(request_id, backend, operation, payload.into_bytes())
        .unwrap()
        .with_catalog_id(catalog)
        .unwrap()
        .encode()
        .unwrap();
    let mut out = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };

    let status =
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut out) };

    assert_eq!(status, FFI_OK);
    let response_bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) }.to_vec();
    unsafe { ducklake_catalog_runtime_free(out.ptr, out.len) };
    let response = RuntimeResponse::decode(&response_bytes).unwrap();
    assert_eq!(
        response.status,
        RuntimeResponseStatus::Ok,
        "{}",
        String::from_utf8_lossy(&response.payload)
    );
    String::from_utf8(response.payload).unwrap()
}

fn payload_line_u64(payload: &str, key: &str) -> u64 {
    let prefix = format!("{key}=");
    payload
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("runtime payload did not contain {key}: {payload}"))
}

#[cfg(feature = "foundationdb")]
fn ducklake_snapshot_id_from_payload(payload: &str) -> u64 {
    payload
        .lines()
        .find_map(|line| line.strip_prefix("ducklake_snapshot_id="))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            panic!("runtime payload did not contain ducklake_snapshot_id: {payload}")
        })
}

#[cfg(feature = "foundationdb")]
fn live_fdb_disabled() -> bool {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() == Ok("1") {
        return false;
    }
    eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
    true
}

#[cfg(feature = "foundationdb")]
fn unique_fdb_prefix(test_name: &str) -> String {
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
