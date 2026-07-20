use ducklake_catalog::{
    CatalogDebugRow, CatalogId, CatalogOrderId, FakeOrderedCatalogKv, SchemaId, TableId, TableRow,
    commit_create_table_row, initialize_catalog_if_absent, latest_snapshot,
    list_catalog_debug_rows, list_inline_table_payloads_at, register_inline_table_payload,
};

#[test]
fn independent_catalog_backends_do_not_share_table_cache_entries() {
    let catalog = CatalogId(1);
    let table = TableId(10);
    let mut first = FakeOrderedCatalogKv::new();
    let mut second = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut first, catalog).unwrap();
    initialize_catalog_if_absent(&mut second, catalog).unwrap();
    commit_create_table_row(
        &mut first,
        catalog,
        TableRow::new(table, "first", CatalogOrderId::from_u128(0)),
    )
    .unwrap();
    commit_create_table_row(
        &mut second,
        catalog,
        TableRow::new(table, "second", CatalogOrderId::from_u128(0)),
    )
    .unwrap();

    let first_rows = list_catalog_debug_rows(&first, catalog, usize::MAX).unwrap();
    let second_rows = list_catalog_debug_rows(&second, catalog, usize::MAX).unwrap();
    let table_name = |rows: &[CatalogDebugRow]| {
        rows.iter().find_map(|row| match row {
            CatalogDebugRow::Table(row) if row.table_id == table => Some(row.name.clone()),
            _ => None,
        })
    };

    assert_eq!(table_name(&first_rows).as_deref(), Some("first"));
    assert_eq!(
        table_name(&second_rows).as_deref(),
        Some("second"),
        "process-global caches must be scoped to the backing catalog instance"
    );
}

#[test]
fn independent_catalog_backends_do_not_share_inline_payload_cache_entries() {
    let catalog = CatalogId(1);
    let table = TableId(10);
    let schema = SchemaId(0);
    let mut first = FakeOrderedCatalogKv::new();
    let mut second = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut first, catalog).unwrap();
    initialize_catalog_if_absent(&mut second, catalog).unwrap();
    register_inline_table_payload(
        &mut first,
        catalog,
        table,
        schema,
        b"row\t1\ti:first\n".to_vec(),
    )
    .unwrap();
    register_inline_table_payload(
        &mut second,
        catalog,
        table,
        schema,
        b"row\t1\ti:second\n".to_vec(),
    )
    .unwrap();
    let first_order = latest_snapshot(&first, catalog).unwrap().unwrap().order;
    let second_order = latest_snapshot(&second, catalog).unwrap().unwrap().order;

    let first_rows =
        list_inline_table_payloads_at(&first, catalog, table, schema, first_order).unwrap();
    let second_rows =
        list_inline_table_payloads_at(&second, catalog, table, schema, second_order).unwrap();

    assert_eq!(first_rows[0].payload, b"row\t1\ti:first\n");
    assert_eq!(
        second_rows[0].payload, b"row\t1\ti:second\n",
        "process-global caches must be scoped to the backing catalog instance"
    );
}
