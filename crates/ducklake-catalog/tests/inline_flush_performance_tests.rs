#![cfg(feature = "foundationdb")]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbDataMutation,
    FdbOrderedCatalogKv, InlineTableFlush, InlinedTableRow, SchemaId, TableColumnRow, TableId,
    TableRow, latest_snapshot, list_data_files_at, list_inline_table_payloads_at,
};

#[test]
#[ignore = "measures the complete native flush path against an explicitly owned FoundationDB"]
fn given_one_thousand_inline_groups_when_flushed_then_report_native_cost_and_verify_snapshot_visibility()
 {
    assert_eq!(std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref(), Ok("1"));
    let catalog = CatalogId(1);
    let table_id = TableId(1);
    let schema = SchemaId(0);
    for trial in 0..3 {
        let prefix = format!(
            "ducklake-review/flush-cost/{}/{trial}/{}/",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let kv = FdbOrderedCatalogKv::open_default_with_prefix(prefix.into_bytes()).unwrap();
        kv.initialize_catalog_if_absent_versionstamped(catalog)
            .unwrap();
        let mut table = kv
            .create_table_versionstamped(
                catalog,
                TableRow::with_catalog_metadata(
                    table_id,
                    SchemaId(0),
                    "main",
                    "events",
                    "main/events",
                    vec![TableColumnRow::new(
                        ColumnId(1),
                        "value",
                        "VARCHAR",
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
        for row in 0..1_000 {
            kv.register_inline_table_payload_with_table_versionstamped(
                catalog,
                table.clone(),
                schema,
                format!("row\t{row}\ts:{}\n", "x".repeat(1_024)).into_bytes(),
            )
            .unwrap();
        }
        let before = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let started = Instant::now();
        let result = kv
            .commit_data_mutation_versionstamped(
                catalog,
                None,
                FdbDataMutation::new(
                    vec![
                        DataFileRow::new(
                            DataFileId(10_000),
                            table_id,
                            "flushed.parquet",
                            1_000,
                            1_024_000,
                            before.order,
                        )
                        .with_row_id_start(0),
                    ],
                    Vec::new(),
                    vec![InlineTableFlush::new(table_id, schema, before.sequence)],
                    Vec::new(),
                    Vec::new(),
                ),
            )
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(result.flushed_inline_count, 1);
        let after = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_eq!(
            list_inline_table_payloads_at(&kv, catalog, table_id, schema, before.order)
                .unwrap()
                .len(),
            1_000
        );
        assert!(
            list_inline_table_payloads_at(&kv, catalog, table_id, schema, after.order)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            list_data_files_at(&kv, catalog, table_id, after.order)
                .unwrap()
                .len(),
            1
        );
        // Compare persisted visibility metadata across the old E and new W
        // representations without requiring the new enum variant in main.
        let legacy_prefix = ducklake_catalog::keys::family_prefix(
            catalog,
            ducklake_catalog::keys::KeyFamily::EndOrder,
        );
        let mut watermark_prefix = legacy_prefix.clone();
        let family_offset = watermark_prefix.len() - 2;
        watermark_prefix[family_offset] = b'W';
        let markers = [legacy_prefix, watermark_prefix]
            .into_iter()
            .flat_map(|prefix| {
                ducklake_catalog::OrderedCatalogKv::scan_prefix(
                    &kv,
                    &prefix,
                    ducklake_catalog::RangeDirection::Forward,
                    usize::MAX,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let marker_bytes: usize = markers
            .iter()
            .map(|row| row.key.len() + row.value.len())
            .sum();
        println!(
            "NATIVE_FLUSH_MARKERS trial={trial} rows={} logical_bytes={marker_bytes}",
            markers.len()
        );
        println!(
            "NATIVE_FLUSH_SAMPLE trial={trial} groups=1000 rows=1000 payload_bytes=1024000 elapsed_us={} us_per_row={:.3}",
            elapsed.as_micros(),
            elapsed.as_secs_f64() * 1_000_000.0 / 1_000.0
        );
    }
}
