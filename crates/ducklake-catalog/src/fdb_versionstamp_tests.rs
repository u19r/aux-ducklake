use super::*;
use crate::{
    CatalogId, CatalogOrderId, CatalogOrderKind, ColumnId, FdbOrderedCatalogKv, MacroId, MacroRow,
    PartitionKeyIndex, SchemaId, TableColumnRow, ValidityWindow, ViewRow,
};

#[test]
fn namespaced_keys_are_stripped_from_scan_results() {
    assert_eq!(
        strip_namespace(b"test-prefix/", b"test-prefix/catalog-key").unwrap(),
        b"catalog-key"
    );
    assert!(strip_namespace(b"test-prefix/", b"other/catalog-key").is_err());
}

#[test]
fn oversized_batches_fail_before_publication() {
    let mut batch = KvBatch::new();
    batch.put(vec![b'k'], vec![b'v'; 1024 * 1024 + 1]);

    assert!(
        batch_exceeds_commit_limit(&batch, 1024 * 1024),
        "test should exercise the same limit checked before commit"
    );
}

#[test]
fn many_cleanup_deletes_fail_before_publication() {
    let catalog = CatalogId(7);
    let mut batch = KvBatch::new();
    for index in 0..20_000 {
        let data_file_id = DataFileId(index);
        batch.delete(crate::keys::file_partition_value_key(
            catalog,
            data_file_id,
            PartitionKeyIndex(0),
        ));
        batch.delete(crate::keys::partition_value_lookup_key(
            catalog,
            TableId(22),
            PartitionKeyIndex(0),
            &format!("partition-value-{index:05}"),
            data_file_id,
        ));
    }

    assert!(
        batch_exceeds_commit_limit(&batch, FdbOrderedCatalogKv::MAX_COMMIT_BYTES),
        "cleanup range/delete bookkeeping must be preflighted before FDB commit"
    );
}

#[test]
fn wide_schema_create_estimate_exceeds_commit_limit() {
    let catalog = CatalogId(7);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let long_path = "warehouse/".repeat(200);
    let schemas = (0..700)
        .map(|index| {
            SchemaRow::new(
                SchemaId(index),
                format!("uuid-{index:04}-{}", "x".repeat(256)),
                format!("schema_{index:04}_{}", "name".repeat(64)),
                format!("{long_path}/schema-{index:04}"),
                incomplete_order(),
            )
        })
        .collect::<Vec<_>>();

    let estimated_bytes = estimate_versionstamped_schema_create_bytes(catalog, &snapshot, &schemas);

    assert!(estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES);
}

#[test]
fn wide_table_create_estimate_exceeds_commit_limit() {
    let catalog = CatalogId(7);
    let snapshot = SnapshotRow::new(incomplete_order(), crate::RawSnapshotSequence(1));
    let columns = (0..6_000)
        .map(|index| {
            TableColumnRow::new(
                ColumnId(index),
                format!("column_{index:04}_{}", "name".repeat(12)),
                format!("VARCHAR({})", 1024 + index),
                true,
                None,
            )
            .with_comment(Some("column comment ".repeat(8)))
            .with_default_metadata(
                Some("initial default ".repeat(8)),
                Some("current default ".repeat(8)),
                "literal",
            )
        })
        .collect::<Vec<_>>();
    let table = TableRow {
        columns,
        validity: ValidityWindow::new(incomplete_order(), None),
        ..TableRow::with_catalog_metadata(
            TableId(22),
            SchemaId(11),
            "table-uuid",
            "wide_table",
            "warehouse/wide_table",
            Vec::new(),
            incomplete_order(),
        )
    };

    let estimated_bytes = estimate_versionstamped_table_create_bytes(catalog, &snapshot, &table);

    assert!(estimated_bytes > FdbOrderedCatalogKv::MAX_COMMIT_BYTES);
}

#[test]
fn versionstamped_key_appends_little_endian_placeholder_offset() {
    let prefix = b"prefix/";
    let catalog = crate::CatalogId(7);
    let catalog_key = snapshot_key(catalog, incomplete_order());
    let key = {
        let mut out = Vec::new();
        out.extend_from_slice(prefix);
        out.extend_from_slice(&catalog_key);
        append_versionstamp_offset(
            &mut out,
            prefix
                .len()
                .saturating_add(snapshot_key_order_offset(catalog)),
        )
        .unwrap();
        out
    };

    assert_eq!(
        &key[key.len() - 4..],
        &u32::try_from(prefix.len() + snapshot_key_order_offset(catalog))
            .unwrap()
            .to_le_bytes()
    );
}

#[test]
fn versionstamped_value_appends_little_endian_placeholder_offset() {
    let payload = b"catalog-row";
    let value = versionstamped_value(payload, 12).unwrap();

    assert_eq!(&value[..payload.len()], payload);
    assert_eq!(&value[payload.len()..], &12_u32.to_le_bytes());
}

#[test]
fn catalog_object_versionstamp_offsets_point_to_fdb_stamp_bytes() {
    let order = CatalogOrderId::uuid_v7(42);
    let schema = SchemaRow::new(SchemaId(1), "schema-uuid", "schema", "schema/", order);
    let table = TableRow::with_catalog_metadata(
        TableId(2),
        SchemaId(1),
        "table-uuid",
        "table",
        "schema/table",
        vec![TableColumnRow::new(
            ColumnId(1),
            "id",
            "INTEGER",
            false,
            None,
        )],
        order,
    );
    let view = ViewRow::new(
        TableId(3),
        SchemaId(1),
        crate::ViewDefinition::new("view-uuid", "view", "duckdb", "SELECT 1", Vec::new()),
        order,
    );
    let macro_row = MacroRow::new(MacroId(4), SchemaId(1), "macro", Vec::new(), order);

    assert_eq!(
        schema.encode()[SchemaRow::BEGIN_ORDER_BYTES_OFFSET],
        order.as_bytes()[0],
        "schema begin-order offset must point at the first FDB-replaced order byte"
    );
    assert_eq!(
        table.encode()[TableRow::BEGIN_ORDER_BYTES_OFFSET],
        order.as_bytes()[0],
        "table begin-order offset must point at the first FDB-replaced order byte"
    );
    assert_eq!(
        view.encode()[ViewRow::BEGIN_ORDER_BYTES_OFFSET],
        order.as_bytes()[0],
        "view begin-order offset must point at the first FDB-replaced order byte"
    );
    assert_eq!(
        macro_row.encode()[MacroRow::BEGIN_ORDER_BYTES_OFFSET],
        order.as_bytes()[0],
        "macro begin-order offset must point at the first FDB-replaced order byte"
    );
}

#[test]
fn committed_versionstamp_becomes_fdb_order_id() {
    let order = committed_order(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();

    assert_eq!(order.kind(), CatalogOrderKind::FdbVersionstamp);
    assert_eq!(
        &order.as_bytes()[..CatalogOrderId::FDB_VERSIONSTAMP_LEN],
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    );
}

#[test]
fn committed_versionstamp_rejects_wrong_length() {
    let error = committed_order(&[1, 2, 3]).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("foundationdb versionstamp must be 10 bytes")
    );
}
