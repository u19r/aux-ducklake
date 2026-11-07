use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogOrderId, ColumnId, DataFileId, DataFileRow, FileColumnStatsRow, TableColumnRow, TableId,
    TableRow,
    runtime_column_values::{
        ColumnDefaultHistory, find_file_columns_with_possible_values,
        find_inline_columns_with_possible_values,
    },
};

#[test]
fn given_null_unknown_and_missing_file_counts_when_checking_values_then_only_proven_null_is_excluded()
 {
    let file = DataFileRow::new(
        DataFileId(1),
        TableId(1),
        "data.parquet",
        10,
        100,
        CatalogOrderId::uuid_v7(1),
    );
    let stats = [
        FileColumnStatsRow::new(DataFileId(1), TableId(1), ColumnId(1), 10, None, None)
            .with_value_count(Some(0)),
        FileColumnStatsRow::new(DataFileId(1), TableId(1), ColumnId(2), 10, None, None),
    ];
    let columns = [ColumnId(1), ColumnId(2), ColumnId(3)];
    assert_eq!(
        find_file_columns_with_possible_values(&[file], &stats, &columns, &BTreeMap::new()),
        BTreeSet::from([ColumnId(2), ColumnId(3)])
    );
    assert!(
        find_file_columns_with_possible_values(&[], &stats, &columns, &BTreeMap::new()).is_empty()
    );
}

#[test]
fn given_old_inline_layout_when_column_defaults_are_checked_then_nonnull_defaults_remain_values() {
    let mut table = TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(1));
    for id in 1..=3 {
        table.columns.push(TableColumnRow::new(
            ColumnId(id),
            format!("c{id}"),
            "BIGINT",
            true,
            None,
        ));
    }
    table.columns[1].created_with_table = false;
    table.columns[2].initial_default = Some("9".to_owned());
    let payload =
        b"inline_aggregate_stats\t2\ninline_aggregate_column_stats\t1\t0\tfalse\t\tfalse\t\n";
    assert_eq!(
        find_inline_columns_with_possible_values(
            &table,
            payload,
            &BTreeMap::from([(
                ColumnId(2),
                ColumnDefaultHistory {
                    birth: CatalogOrderId::uuid_v7(1),
                    has_nonnull_initial: false
                }
            )])
        )
        .unwrap(),
        BTreeSet::from([ColumnId(3)])
    );
    assert!(
        find_inline_columns_with_possible_values(
            &table,
            b"inline_aggregate_stats\t0\n",
            &BTreeMap::new()
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        find_inline_columns_with_possible_values(&table, b"invalid\n", &BTreeMap::new()).is_err()
    );
    assert!(
        find_inline_columns_with_possible_values(
            &table,
            b"inline_aggregate_stats\twrong\n",
            &BTreeMap::new()
        )
        .is_err()
    );
}

#[test]
fn given_missing_file_stats_when_null_column_postdates_file_then_only_newer_files_may_hold_values()
{
    let order = CatalogOrderId::uuid_v7(1);
    let file = DataFileRow::new(DataFileId(1), TableId(1), "data", 10, 100, order);
    let columns = [ColumnId(1)];
    let defaults = BTreeMap::from([(
        ColumnId(1),
        ColumnDefaultHistory {
            birth: CatalogOrderId::uuid_v7(2),
            has_nonnull_initial: false,
        },
    )]);
    assert!(
        find_file_columns_with_possible_values(
            std::slice::from_ref(&file),
            &[],
            &columns,
            &defaults
        )
        .is_empty()
    );
    let mut partial = file;
    partial.max_partial_order = Some(CatalogOrderId::uuid_v7(3));
    assert_eq!(
        find_file_columns_with_possible_values(&[partial], &[], &columns, &defaults),
        BTreeSet::from(columns)
    );
}

#[test]
fn given_nested_leaf_with_null_counts_when_inline_rows_exist_then_whole_row_remains_possible() {
    let mut table = TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(1));
    table.columns.push(TableColumnRow::new(
        ColumnId(2),
        "child",
        "BIGINT",
        true,
        Some(ColumnId(1)),
    ));
    let payload = b"inline_active_delete_count\t0\ninline_aggregate_stats\t2\ninline_aggregate_column_stats\t2\t0\tfalse\t\tfalse\t\n";
    assert_eq!(
        find_inline_columns_with_possible_values(&table, payload, &BTreeMap::new()).unwrap(),
        BTreeSet::from([ColumnId(2)])
    );
}

#[test]
fn given_unpersisted_candidate_when_loading_null_proof_then_candidate_is_not_returned() {
    use crate::{
        CatalogId, DuckLakeSnapshotId, FakeOrderedCatalogKv, commit_create_table_row,
        initialize_catalog_if_absent, latest_snapshot,
    };
    let mut kv = FakeOrderedCatalogKv::new();
    let catalog = CatalogId(1);
    initialize_catalog_if_absent(&mut kv, catalog).unwrap();
    let mut table = TableRow::new(TableId(1), "events", CatalogOrderId::uuid_v7(1));
    table.columns.push(TableColumnRow::new(
        ColumnId(1),
        "existing",
        "BIGINT",
        true,
        None,
    ));
    commit_create_table_row(&mut kv, catalog, table).unwrap();
    let snapshot = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let proof = crate::runtime_column_values::list_null_only_columns(
        &kv,
        catalog,
        crate::runtime_column_values::ColumnValuesRequest {
            table_id: TableId(1),
            snapshot_id: DuckLakeSnapshotId(snapshot.sequence.0),
            columns: BTreeSet::from([ColumnId(1), ColumnId(2)]),
        },
    )
    .unwrap();
    assert_eq!(proof, BTreeSet::from([ColumnId(1)]));
}

#[test]
fn given_zero_stats_before_nonnull_default_birth_when_proving_nulls_then_default_remains_possible()
{
    let file = DataFileRow::new(
        DataFileId(1),
        TableId(1),
        "data",
        10,
        100,
        CatalogOrderId::uuid_v7(1),
    );
    let stats = [
        FileColumnStatsRow::new(DataFileId(1), TableId(1), ColumnId(1), 10, None, None)
            .with_value_count(Some(0)),
    ];
    let defaults = BTreeMap::from([(
        ColumnId(1),
        ColumnDefaultHistory {
            birth: CatalogOrderId::uuid_v7(2),
            has_nonnull_initial: true,
        },
    )]);
    assert_eq!(
        find_file_columns_with_possible_values(&[file], &stats, &[ColumnId(1)], &defaults),
        BTreeSet::from([ColumnId(1)])
    );
}
