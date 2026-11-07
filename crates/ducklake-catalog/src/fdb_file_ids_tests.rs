use std::collections::BTreeMap;

use crate::{
    CatalogOrderId, ColumnId, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow,
    FdbDataMutation, FileColumnStatsRow, FilePartitionValueRow, InlineFileDeletionRow,
    PartitionKeyIndex, TableId, fdb_data_mutations::FdbAppendPartitionExpectation,
};

use super::{proposed_file_ids, remap_file_id, remap_mutation_file_ids};

#[test]
fn given_mixed_file_mutation_when_ids_are_reserved_then_all_new_references_are_remapped() {
    let mut mutation = mutation();
    let remaps = BTreeMap::from([(10, 100), (20, 101)]);

    remap_mutation_file_ids(&mut mutation, &remaps);

    assert_eq!(mutation.data_files[0].data_file_id, DataFileId(100));
    assert_eq!(mutation.delete_files[0].delete_file_id, DeleteFileId(101));
    assert_eq!(mutation.delete_files[0].data_file_id, DataFileId(100));
    assert_eq!(mutation.partition_values[0].data_file_id, DataFileId(100));
    assert_eq!(
        mutation.inline_file_deletions[0].data_file_id,
        DataFileId(100)
    );
    assert_eq!(mutation.file_column_stats[0].data_file_id, DataFileId(100));
    assert_eq!(mutation.dropped_data_file_ids, vec![DataFileId(7)]);
    let mut expectation = FdbAppendPartitionExpectation {
        data_file_id: DataFileId(10),
        table_id: TableId(1),
        partition_table_id: None,
        partition_id: None,
        value_count: 0,
    };
    expectation.data_file_id = DataFileId(remap_file_id(&remaps, expectation.data_file_id.0));
    assert_eq!(expectation.data_file_id, DataFileId(100));
}

#[test]
fn given_data_and_delete_files_when_collecting_ids_then_one_ordered_namespace_is_used() {
    assert_eq!(proposed_file_ids(&mutation()).unwrap(), vec![10, 20]);
}

fn mutation() -> FdbDataMutation {
    let order = CatalogOrderId::uuid_v7(1);
    FdbDataMutation {
        data_files: vec![DataFileRow::new(
            DataFileId(10),
            TableId(1),
            "data.parquet",
            1,
            10,
            order,
        )],
        delete_files: vec![DeleteFileRow::new(
            DeleteFileId(20),
            DataFileId(10),
            "delete.parquet",
            1,
            10,
            order,
        )],
        partition_values: vec![FilePartitionValueRow::new(
            DataFileId(10),
            TableId(1),
            PartitionKeyIndex(0),
            "eu",
        )],
        inline_file_deletions: vec![InlineFileDeletionRow::new(
            TableId(1),
            DataFileId(10),
            1,
            order,
        )],
        file_column_stats: vec![FileColumnStatsRow::new(
            DataFileId(10),
            TableId(1),
            ColumnId(1),
            0,
            None,
            None,
        )],
        dropped_data_file_ids: vec![DataFileId(7)],
        ..FdbDataMutation::default()
    }
}
