#![cfg(feature = "foundationdb")]

use std::time::{SystemTime, UNIX_EPOCH};

use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FdbDataMutation,
    FdbOrderedCatalogKv, InlineTableFlush, InlinedTableRow, OrderedCatalogKv, SchemaId,
    TableColumnRow, TableId, TableRow, keys::inline_current_row_key, latest_snapshot,
};

#[test]
#[ignore = "requires an explicitly configured isolated FoundationDB cluster"]
fn given_later_rows_between_flush_ids_when_flushing_then_gaps_and_later_rows_are_preserved() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let kv = FdbOrderedCatalogKv::open_default_with_prefix(
        format!("aux-ducklake-inline-range-proof/{suffix}/").into_bytes(),
    )
    .unwrap();
    let catalog = CatalogId(1);
    let table = TableId(1);
    let schema = SchemaId(1);
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let mut registered = kv
        .create_table_versionstamped(
            catalog,
            TableRow::with_catalog_metadata(
                table,
                SchemaId(0),
                "range-proof",
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
    registered
        .inlined_data_tables
        .push(InlinedTableRow::new("range_proof_inline", schema.0));
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        registered.clone(),
        schema,
        b"row\t10\ti:10\nrow\t11\ti:11\nrow\t13\ti:13\n".to_vec(),
    )
    .unwrap();
    let flushed = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.register_inline_table_payload_with_table_versionstamped(
        catalog,
        registered,
        schema,
        b"row\t12\ti:12\nrow\t14\ti:14\n".to_vec(),
    )
    .unwrap();
    let retained = [12, 14].map(|id| {
        let key = inline_current_row_key(catalog, table, schema, id);
        let value = kv
            .get(&key)
            .unwrap()
            .expect("later row exists before flush");
        (key, value)
    });

    let committed = kv
        .commit_data_mutation_versionstamped(
            catalog,
            None,
            FdbDataMutation::new(
                vec![DataFileRow::new(
                    DataFileId(1),
                    table,
                    "range-proof.parquet",
                    3,
                    128,
                    flushed.order,
                )],
                Vec::new(),
                vec![InlineTableFlush::new(table, schema, flushed.sequence)],
                Vec::new(),
                Vec::new(),
            ),
        )
        .unwrap();

    assert_eq!(committed.flushed_inline_count, 1);
    for id in [10, 11, 13] {
        assert!(
            kv.get(&inline_current_row_key(catalog, table, schema, id))
                .unwrap()
                .is_none()
        );
    }
    for (key, value) in retained {
        assert_eq!(
            kv.get(&key).unwrap(),
            Some(value),
            "later payload must remain byte-identical"
        );
    }
}
