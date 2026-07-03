use ducklake_catalog::{
    CatalogId, CatalogOrderId, ColumnId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    FileColumnStatsRow, KvBatch, RawSnapshotSequence, TableId, commit_append_data_files,
    initialize_catalog_if_absent,
    keys::{latest_snapshot_row_key, snapshot_key},
    latest_snapshot, list_file_column_stats, list_file_column_stats_for_table_column,
    list_snapshots, register_file_column_stats, snapshot_by_raw_sequence,
};

#[test]
fn mirrors_audit_base_audit_test_snapshot_commit_metadata_is_saved_and_returned() {
    // Mirrors: third_party/ducklake/test/sql/audit/test_base_audit.test
    //
    // Storage contract:
    // - DuckLake asks the catalog to save per-snapshot audit metadata.
    // - Later snapshot reads must return author, commit message, and extra info unchanged.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    snapshot = snapshot.with_commit_metadata(
        "ducklake-user",
        Some("insert geometry audit rows".to_owned()),
        Some(r#"{"query_id":"audit-1","client":"duckdb"}"#.to_owned()),
    );
    replace_snapshot(&mut kv, catalog, &snapshot);

    let latest = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let by_sequence = snapshot_by_raw_sequence(&kv, catalog, RawSnapshotSequence(0))
        .unwrap()
        .unwrap();
    let snapshots = list_snapshots(&kv, catalog).unwrap();

    assert_eq!(latest.created_by, "ducklake-user");
    assert_eq!(
        latest.commit_message.as_deref(),
        Some("insert geometry audit rows")
    );
    assert_eq!(
        latest.commit_extra_info.as_deref(),
        Some(r#"{"query_id":"audit-1","client":"duckdb"}"#)
    );
    assert_eq!(by_sequence, latest);
    assert_eq!(snapshots, vec![latest]);
}

#[test]
fn mirrors_geo_ducklake_geometry_test_geometry_extra_stats_are_saved_for_base_column() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry.test
    //
    // Storage contract:
    // - DuckLake saves a geometry column's serialized extra stats with the file-column stats row.
    // - Listing stats for that table/column must return the exact serialized value.
    let mut kv = seeded_catalog_with_file(TableId(10), DataFileId(1));
    let extra = geometry_extra_stats("POINT", 0.0, 0.0, 10.0, 10.0);

    save_stats(&mut kv, TableId(10), DataFileId(1), ColumnId(2), &extra);

    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(2), &[extra.as_str()]);
}

#[test]
fn mirrors_geo_ducklake_geometry_add_files_test_added_file_extra_stats_are_saved() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry_add_files.test
    //
    // Storage contract:
    // - ADD FILES gives the catalog file-column stats for an existing Parquet object.
    // - The catalog must preserve the geometry extra stats exactly like native append files.
    let mut kv = seeded_catalog_with_file(TableId(10), DataFileId(42));
    let extra = geometry_extra_stats("LINESTRING", -5.0, 1.0, 5.0, 9.0);

    save_stats(&mut kv, TableId(10), DataFileId(42), ColumnId(3), &extra);

    let rows = list_file_column_stats(&kv, CatalogId(1)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data_file_id, DataFileId(42));
    assert_eq!(rows[0].extra_stats.as_deref(), Some(extra.as_str()));
}

#[test]
fn mirrors_geo_ducklake_geometry_nested_list_test_nested_leaf_extra_stats_are_keyed_by_column_id() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry_nested_list.test
    //
    // Storage contract:
    // - Nested geometry leaf stats are still keyed by the DuckLake column id.
    // - Saving one nested leaf must not make the stats visible under a sibling column id.
    let mut kv = seeded_catalog_with_file(TableId(10), DataFileId(7));
    let extra = geometry_extra_stats("LIST<GEOMETRY>", -1.0, -2.0, 3.0, 4.0);

    save_stats(&mut kv, TableId(10), DataFileId(7), ColumnId(11), &extra);

    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(11), &[extra.as_str()]);
    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(12), &[]);
}

#[test]
fn mirrors_geo_ducklake_geometry_nested_map_test_nested_map_extra_stats_are_keyed_by_column_id() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry_nested_map.test
    //
    // Storage contract:
    // - Map value geometry stats are opaque per-column stats rows.
    // - Storage must preserve the serialized extra stats without interpreting map structure.
    let mut kv = seeded_catalog_with_file(TableId(10), DataFileId(8));
    let key_extra = geometry_extra_stats("MAP_KEY", 0.0, 0.0, 1.0, 1.0);
    let value_extra = geometry_extra_stats("MAP_VALUE_GEOMETRY", 10.0, 20.0, 30.0, 40.0);

    save_stats(
        &mut kv,
        TableId(10),
        DataFileId(8),
        ColumnId(20),
        &key_extra,
    );
    save_stats(
        &mut kv,
        TableId(10),
        DataFileId(8),
        ColumnId(21),
        &value_extra,
    );

    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(20), &[key_extra.as_str()]);
    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(21), &[value_extra.as_str()]);
}

#[test]
fn mirrors_geo_ducklake_geometry_nested_struct_test_struct_leaf_extra_stats_are_independent() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry_nested_struct.test
    //
    // Storage contract:
    // - Struct child geometry stats are saved independently for each child column id.
    // - Re-reading one child must not return the sibling's serialized stats.
    let mut kv = seeded_catalog_with_file(TableId(10), DataFileId(9));
    let left = geometry_extra_stats("STRUCT.LEFT", -10.0, -10.0, 0.0, 0.0);
    let right = geometry_extra_stats("STRUCT.RIGHT", 0.0, 0.0, 10.0, 10.0);

    save_stats(&mut kv, TableId(10), DataFileId(9), ColumnId(31), &left);
    save_stats(&mut kv, TableId(10), DataFileId(9), ColumnId(32), &right);

    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(31), &[left.as_str()]);
    assert_extra_stats_for_column(&kv, TableId(10), ColumnId(32), &[right.as_str()]);
}

#[test]
fn mirrors_geo_ducklake_geometry_stats_test_multiple_file_extra_stats_are_returned_for_pruning() {
    // Mirrors: third_party/ducklake/test/sql/geo/ducklake_geometry_stats.test
    //
    // Storage contract:
    // - Spatial pruning depends on reading each file's serialized geometry stats.
    // - The catalog must return every file-column stats row for the requested table/column.
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    let table = TableId(10);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    commit_append_data_files(
        &mut kv,
        catalog,
        vec![
            data_file(table, DataFileId(1)),
            data_file(table, DataFileId(2)),
            data_file(table, DataFileId(3)),
        ],
    )
    .unwrap();
    let first = geometry_extra_stats("BOX_A", 0.0, 0.0, 1.0, 1.0);
    let second = geometry_extra_stats("BOX_B", 5.0, 5.0, 6.0, 6.0);
    let third = geometry_extra_stats("BOX_C", 9.0, 9.0, 10.0, 10.0);

    save_stats(&mut kv, table, DataFileId(1), ColumnId(2), &first);
    save_stats(&mut kv, table, DataFileId(2), ColumnId(2), &second);
    save_stats(&mut kv, table, DataFileId(3), ColumnId(2), &third);

    assert_extra_stats_for_column(
        &kv,
        table,
        ColumnId(2),
        &[first.as_str(), second.as_str(), third.as_str()],
    );
}

fn seeded_catalog_with_file(table: TableId, file: DataFileId) -> FakeOrderedCatalogKv {
    let mut kv = FakeOrderedCatalogKv::new();
    initialize_catalog_if_absent(&mut kv, CatalogId(1)).unwrap();
    commit_append_data_files(&mut kv, CatalogId(1), vec![data_file(table, file)]).unwrap();
    kv
}

fn data_file(table: TableId, file: DataFileId) -> DataFileRow {
    DataFileRow::new(
        file,
        table,
        format!("main/geo/{}.parquet", file.0),
        10,
        2048,
        CatalogOrderId::uuid_v7(0),
    )
}

fn save_stats(
    kv: &mut FakeOrderedCatalogKv,
    table: TableId,
    file: DataFileId,
    column: ColumnId,
    extra_stats: &str,
) {
    register_file_column_stats(
        kv,
        CatalogId(1),
        FileColumnStatsRow::new(
            file,
            table,
            column,
            0,
            Some("GEOMETRY_MIN".to_owned()),
            Some("GEOMETRY_MAX".to_owned()),
        )
        .with_value_count(Some(10))
        .with_extra_stats(Some(extra_stats.to_owned())),
    )
    .unwrap();
}

fn assert_extra_stats_for_column(
    kv: &FakeOrderedCatalogKv,
    table: TableId,
    column: ColumnId,
    expected: &[&str],
) {
    let mut actual = list_file_column_stats_for_table_column(kv, CatalogId(1), table, column)
        .unwrap()
        .into_iter()
        .map(|row| row.extra_stats.unwrap())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
}

fn geometry_extra_stats(kind: &str, xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> String {
    format!(
        r#"{{"type":"ducklake_geometry","kind":"{kind}","bbox":[{xmin},{ymin},{xmax},{ymax}]}}"#
    )
}

fn replace_snapshot(
    kv: &mut FakeOrderedCatalogKv,
    catalog: CatalogId,
    snapshot: &ducklake_catalog::SnapshotRow,
) {
    let mut batch = KvBatch::new();
    batch.put(snapshot_key(catalog, snapshot.order), snapshot.encode());
    batch.put(
        latest_snapshot_row_key(catalog),
        latest_snapshot_value(snapshot),
    );
    kv.commit(batch).unwrap();
}

fn latest_snapshot_value(snapshot: &ducklake_catalog::SnapshotRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + snapshot.encode().len());
    out.push(1);
    out.push(match snapshot.order.kind() {
        ducklake_catalog::CatalogOrderKind::UuidV7 => b'u',
        ducklake_catalog::CatalogOrderKind::FdbVersionstamp => b'f',
    });
    out.extend_from_slice(&snapshot.encode());
    out
}
