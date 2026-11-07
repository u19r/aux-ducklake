use crate::{
    CatalogId, CatalogOrderId, ColumnId, DuckLakeSnapshotId, FakeOrderedCatalogKv, InlinedTableRow,
    MutableCatalogKv, TableColumnRow, TableId, TableRow, TableVersionReplacement,
    commit_create_table_row, initialize_catalog_if_absent, latest_snapshot,
    runtime_table_schema_versions::TableSchemaVersions,
};

#[test]
fn given_inline_registration_when_schema_versions_are_read_then_layout_version_stays_at_creation() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table_id = TableId(7);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table = TableRow::new(table_id, "events", CatalogOrderId::uuid_v7(0));
    table
        .columns
        .push(TableColumnRow::new(ColumnId(1), "id", "BIGINT", true, None));
    let created = commit_create_table_row(&mut kv, catalog, table).unwrap();
    let initial = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let mut updated = created.clone();
    updated
        .inlined_data_tables
        .push(InlinedTableRow::new("rows", 1));
    kv.commit_table_replacements(
        catalog,
        initial.sequence,
        vec![TableVersionReplacement::new(table_id, created, updated)],
    )
    .unwrap();
    let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(snapshot.sequence.0)
        )
        .unwrap(),
        TableSchemaVersions {
            creation: Some(1),
            last_change: Some(1)
        }
    );
}

#[test]
fn given_added_column_when_schema_versions_are_read_then_only_new_snapshots_see_the_change() {
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(2);
    let table_id = TableId(7);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let table = TableRow::new(table_id, "events", CatalogOrderId::uuid_v7(0));
    let created = commit_create_table_row(&mut kv, catalog, table).unwrap();
    let initial = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let mut updated = created.clone();
    updated
        .columns
        .push(TableColumnRow::new(ColumnId(1), "id", "BIGINT", true, None));
    kv.commit_table_replacements(
        catalog,
        initial.sequence,
        vec![TableVersionReplacement::new(table_id, created, updated)],
    )
    .unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(latest.sequence.0)
        )
        .unwrap(),
        TableSchemaVersions {
            creation: Some(1),
            last_change: Some(2)
        }
    );
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(initial.sequence.0)
        )
        .unwrap(),
        TableSchemaVersions {
            creation: Some(1),
            last_change: Some(1)
        }
    );
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            TableId(99),
            DuckLakeSnapshotId(latest.sequence.0)
        )
        .unwrap(),
        TableSchemaVersions::default()
    );
    assert!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(latest.sequence.0 + 100)
        )
        .is_err()
    );
}

#[test]
fn given_comments_and_partition_changes_when_loading_versions_then_only_partition_advances_layout()
{
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(3);
    let table_id = TableId(7);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table = TableRow::new(table_id, "events", CatalogOrderId::uuid_v7(0));
    table
        .columns
        .push(TableColumnRow::new(ColumnId(1), "id", "BIGINT", true, None));
    let created = commit_create_table_row(&mut kv, catalog, table).unwrap();
    let mut commented = created.clone();
    commented.columns[0].comment = Some("identifier".to_owned());
    let initial = latest_snapshot(&kv, catalog).unwrap().unwrap();
    kv.commit_table_replacements(
        catalog,
        initial.sequence,
        vec![TableVersionReplacement::new(
            table_id,
            created,
            commented.clone(),
        )],
    )
    .unwrap();
    let comments = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(comments.sequence.0)
        )
        .unwrap()
        .last_change,
        Some(1)
    );
    let mut partitioned = commented.clone();
    partitioned.partition = Some(crate::TablePartitionRow::new(
        1,
        vec![crate::TablePartitionFieldRow::new(
            0,
            ColumnId(1),
            "identity",
        )],
    ));
    kv.commit_table_replacements(
        catalog,
        comments.sequence,
        vec![TableVersionReplacement::new(
            table_id,
            commented,
            partitioned,
        )],
    )
    .unwrap();
    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_eq!(
        TableSchemaVersions::load(
            &kv,
            catalog,
            table_id,
            DuckLakeSnapshotId(latest.sequence.0)
        )
        .unwrap()
        .last_change,
        Some(3)
    );
}
