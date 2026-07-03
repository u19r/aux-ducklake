#![cfg(feature = "foundationdb")]

use std::time::{SystemTime, UNIX_EPOCH};

use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    InlinedTableRow, MergeAdjacentCompaction, SchemaId, TableColumnRow, TableId, TableRow,
    latest_snapshot, list_data_file_changes, list_data_files_at, list_inline_row_payload_changes,
    snapshot_changes_made,
};

#[test]
fn given_inline_rows_flushed_to_file_when_snapshot_changes_are_read_then_flush_is_not_user_insert()
{
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(84);
    let table_id = TableId(84);
    let schema_id = SchemaId(4);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-flush-snapshot-changes").into_bytes(),
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
                "inline-flush-snapshot-changes",
                "inline_flush_snapshot_changes",
                "main/inline_flush_snapshot_changes",
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
    let mut attached = created;
    attached.inlined_data_tables.push(InlinedTableRow::new(
        "inline_flush_snapshot_changes_inlined",
        schema_id.0,
    ));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached.clone(),
        schema_id,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        vec![DataFileRow::new(
            DataFileId(840),
            table_id,
            "inline-flush-snapshot-changes.parquet",
            1,
            10,
            CatalogOrderId::uuid_v7(0),
        )],
        Vec::new(),
        vec![ducklake_catalog::InlineTableFlush::new(
            table_id,
            schema_id,
            inline_snapshot.sequence,
        )],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    assert_eq!(
        snapshot_changes_made(&kv, catalog, flush_snapshot.order).unwrap(),
        "flushed_inlined:84"
    );
}

#[test]
fn given_flushed_inline_file_compacted_when_historical_snapshot_is_read_then_one_file_representation_remains()
 {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(85);
    let table_id = TableId(85);
    let schema_id = SchemaId(5);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-flush-compaction-history").into_bytes(),
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
                "inline-flush-compaction-history",
                "inline_flush_compaction_history",
                "main/inline_flush_compaction_history",
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
    let mut attached = created;
    attached.inlined_data_tables.push(InlinedTableRow::new(
        "inline_flush_compaction_history_inlined",
        schema_id.0,
    ));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached.clone(),
        schema_id,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        vec![
            DataFileRow::new(
                DataFileId(850),
                table_id,
                "inline-flush-source.parquet",
                1,
                10,
                inline_snapshot.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(inline_snapshot.order)),
        ],
        Vec::new(),
        vec![ducklake_catalog::InlineTableFlush::new(
            table_id,
            schema_id,
            inline_snapshot.sequence,
        )],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    kv.append_data_files_versionstamped(
        catalog,
        vec![
            DataFileRow::new(
                DataFileId(851),
                table_id,
                "added.parquet",
                1,
                10,
                CatalogOrderId::uuid_v7(0),
            )
            .with_row_id_start(1),
        ],
    )
    .unwrap();
    kv.commit_merge_adjacent_data_files_versionstamped(
        catalog,
        MergeAdjacentCompaction {
            source_file_ids: vec![DataFileId(850), DataFileId(851)],
            new_files: vec![
                DataFileRow::new(
                    DataFileId(852),
                    table_id,
                    "replacement.parquet",
                    2,
                    20,
                    CatalogOrderId::uuid_v7(0),
                )
                .with_row_id_start(0),
            ],
            partition_values: Vec::new(),
            file_column_stats: Vec::new(),
        },
    )
    .unwrap();

    let files = list_data_files_at(&kv, catalog, table_id, inline_snapshot.order).unwrap();

    assert_eq!(
        files
            .iter()
            .map(|file| file.data_file_id)
            .collect::<Vec<_>>(),
        vec![DataFileId(852)]
    );
}

#[test]
fn given_versionstamped_inline_file_materialized_when_listing_insertions_then_inline_row_is_suppressed_after_flush()
 {
    if live_fdb_disabled() {
        return;
    }

    let catalog = CatalogId(86);
    let table_id = TableId(86);
    let schema_id = SchemaId(6);
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        unique_prefix("inline-flush-cdf-suppression").into_bytes(),
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
                "inline-flush-cdf-suppression",
                "inline_flush_cdf_suppression",
                "main/inline_flush_cdf_suppression",
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
    let mut attached = created;
    attached.inlined_data_tables.push(InlinedTableRow::new(
        "inline_flush_cdf_suppression_inlined",
        schema_id.0,
    ));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached.clone(),
        schema_id,
        b"row\t0\ti:1\n".to_vec(),
    )
    .unwrap();
    let inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        vec![
            DataFileRow::new(
                DataFileId(860),
                table_id,
                "inline-flush-cdf-suppression.parquet",
                1,
                10,
                inline_snapshot.order,
            )
            .with_row_id_start(0)
            .with_max_partial_order(Some(inline_snapshot.order)),
        ],
        Vec::new(),
        vec![ducklake_catalog::InlineTableFlush::new(
            table_id,
            schema_id,
            inline_snapshot.sequence,
        )],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let _flush_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached,
        schema_id,
        b"row\t1\ti:2\n".to_vec(),
    )
    .unwrap();
    let second_inline_snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();

    let changes = list_data_file_changes(
        &kv,
        catalog,
        table_id,
        CatalogOrderId::from_bytes(inline_snapshot.order.kind(), [0; CatalogOrderId::LEN]),
        second_inline_snapshot.order,
    )
    .unwrap();
    let inline_insertions = list_inline_row_payload_changes(
        &kv,
        catalog,
        table_id,
        schema_id,
        CatalogOrderId::from_bytes(inline_snapshot.order.kind(), [0; CatalogOrderId::LEN]),
        second_inline_snapshot.order,
        ducklake_catalog::InlineRowChangeKind::Inserted,
    )
    .unwrap();

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].data_file_id, DataFileId(860));
    assert_eq!(inline_insertions.len(), 1);
    assert_eq!(inline_insertions[0].change.row_id, 1);
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
