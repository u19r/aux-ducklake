use crate::{
    CatalogId, CatalogOrderId, ColumnId, DuckLakeSnapshotId, FdbOrderedCatalogKv, InlinedTableRow,
    SchemaId, TableColumnRow, TableId, TableRow,
    runtime_inline_rows::{
        ReadInlineRowsPayload, read_foundationdb_inline_rows_aggregate_stats_payload,
        read_foundationdb_inline_rows_payload,
    },
    runtime_snapshot_range::ReadSnapshot,
};

#[test]
fn fdb_current_inline_stats_track_delete_reinsert_flush_and_high_watermark() {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
        return;
    }
    let catalog = CatalogId(7411);
    let table_id = TableId(9411);
    let schema_id = SchemaId(7);
    let table_name = "current_inline_stats_inlined";
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(unique_prefix().into_bytes()).unwrap();
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let created = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table_id,
                SchemaId(0),
                "current-inline-stats-uuid",
                "current_inline_stats",
                "main/current_inline_stats",
                vec![TableColumnRow::new(
                    ColumnId(1),
                    "value",
                    "INTEGER",
                    false,
                    None,
                )],
                CatalogOrderId::from_u128(0),
            ),
            None,
        )
        .unwrap();
    let mut attached = created;
    attached
        .inlined_data_tables
        .push(InlinedTableRow::new(table_name, schema_id.0));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached.clone(),
        schema_id,
        b"row\t0\ti:1\nrow\t1\ti:5\nrow\t2\ti:10\n".to_vec(),
    )
    .unwrap();
    let inserted = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_delete_inline_table_rows_versionstamped(catalog, table_id, schema_id, &[0, 2], None)
        .unwrap();
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached.clone(),
        schema_id,
        b"row\t0\ti:101\n".to_vec(),
    )
    .unwrap();
    let reinserted = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();

    require_stats(&stats(&kv, catalog, table_name, None), 2, 3, "5", "101");
    require_stats(
        &stats(&kv, catalog, table_name, Some(inserted.sequence.0)),
        3,
        3,
        "1",
        "10",
    );
    let current_rows = rows(&kv, catalog, table_name, None);
    assert!(
        current_rows.contains(&format!(
            "row_change\t{}\t\t0\ti:101",
            reinserted.sequence.0
        )),
        "{current_rows}"
    );
    assert!(
        current_rows.contains(&format!("row_change\t{}\t\t1\ti:5", inserted.sequence.0)),
        "{current_rows}"
    );
    assert!(!current_rows.contains("\t2\ti:10"), "{current_rows}");
    let historical_rows = rows(&kv, catalog, table_name, Some(inserted.sequence.0));
    assert!(historical_rows.contains("\t0\ti:1"), "{historical_rows}");
    assert!(historical_rows.contains("\t1\ti:5"), "{historical_rows}");
    assert!(historical_rows.contains("\t2\ti:10"), "{historical_rows}");

    let before_flush = crate::latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_data_mutation_versionstamped(
        catalog,
        None,
        crate::FdbDataMutation::new(
            vec![
                crate::DataFileRow::new(
                    crate::DataFileId(7411),
                    table_id,
                    "current-inline-stats.parquet",
                    2,
                    128,
                    inserted.order,
                )
                .with_row_id_start(0)
                .with_max_partial_order(Some(before_flush.order)),
            ],
            Vec::new(),
            vec![crate::InlineTableFlush::new(
                table_id,
                schema_id,
                before_flush.sequence,
            )],
            Vec::new(),
            Vec::new(),
        ),
    )
    .unwrap();
    let flushed = stats(&kv, catalog, table_name, None);
    assert!(flushed.contains("inline_aggregate_stats\t0"), "{flushed}");
    assert!(
        flushed.contains("inline_aggregate_next_row_id\t3"),
        "{flushed}"
    );
    let flushed_rows = rows(&kv, catalog, table_name, None);
    assert!(!flushed_rows.contains("row_change\t"), "{flushed_rows}");
    assert!(
        kv.register_inline_table_payload_with_table_versionstamped(
            catalog,
            attached.clone(),
            schema_id,
            b"row\t1\ti:500\n".to_vec(),
        )
        .unwrap_err()
        .to_string()
        .contains("inline row id 1 is already live")
    );
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        attached,
        schema_id,
        b"row\t300\ti:300\n".to_vec(),
    )
    .unwrap();
    kv.commit_delete_inline_table_rows_versionstamped(catalog, table_id, schema_id, &[300], None)
        .unwrap();
    let high = stats(&kv, catalog, table_name, None);
    assert!(high.contains("inline_aggregate_stats\t0"), "{high}");
    assert!(high.contains("inline_aggregate_next_row_id\t301"), "{high}");
}

fn rows(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_name: &str,
    snapshot: Option<u64>,
) -> String {
    String::from_utf8(
        read_foundationdb_inline_rows_payload(
            kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: snapshot.map(|id| ReadSnapshot::new(DuckLakeSnapshotId(id))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap(),
    )
    .unwrap()
}

fn require_stats(output: &str, count: u64, next: u64, min: &str, max: &str) {
    assert!(
        output.contains(&format!("inline_aggregate_stats\t{count}")),
        "{output}"
    );
    assert!(
        output.contains(&format!("inline_aggregate_next_row_id\t{next}")),
        "{output}"
    );
    assert!(
        output.contains(&format!(
            "inline_aggregate_column_stats\t1\t{count}\ttrue\t{min}\ttrue\t{max}"
        )),
        "{output}"
    );
}

fn stats(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    table_name: &str,
    snapshot: Option<u64>,
) -> String {
    String::from_utf8(
        read_foundationdb_inline_rows_aggregate_stats_payload(
            kv,
            catalog,
            ReadInlineRowsPayload {
                table_name: table_name.to_owned(),
                snapshot: snapshot.map(|id| ReadSnapshot::new(DuckLakeSnapshotId(id))),
                include_flushed: false,
                include_deleted: false,
            },
        )
        .unwrap(),
    )
    .unwrap()
}

fn unique_prefix() -> String {
    format!(
        "aux-ducklake-test/current-inline-stats/{}/{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
