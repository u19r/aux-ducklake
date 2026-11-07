use ducklake_catalog::{
    CatalogId, CatalogOrderId, DataFileId, DataFileRow, FakeOrderedCatalogKv,
    FilePartitionValueRow, PartitionKeyIndex, TableId, append_data_file, expire_data_file,
    keys::{decode_key, partition_value_lookup_key},
    list_current_data_files_by_partition_value, register_file_partition_value,
};

#[test]
fn given_partition_value_row_when_round_tripped_then_partition_metadata_is_preserved() {
    let row = FilePartitionValueRow::new(
        DataFileId(2),
        TableId(42),
        PartitionKeyIndex(1),
        "us-east-1",
    );

    let decoded = FilePartitionValueRow::decode(&row.encode()).unwrap();

    assert_eq!(decoded, row);
}

#[test]
fn given_partition_value_when_lookup_key_is_decoded_then_key_is_readable() {
    let key = partition_value_lookup_key(
        CatalogId(7),
        TableId(42),
        PartitionKeyIndex(1),
        "us-east-1",
        DataFileId(2),
    );

    let decoded = decode_key(&key).unwrap();

    assert!(decoded.contains("catalog=7"));
    assert!(decoded.contains("family=partition-value-lookup"));
    assert!(decoded.contains("000000000000002a"));
}

#[test]
fn given_partition_value_when_pruning_current_files_then_only_matching_active_files_return() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let partition_key = PartitionKeyIndex(1);
    let mut kv = FakeOrderedCatalogKv::new();
    let matching = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/us/file.parquet",
        20,
        200,
        CatalogOrderId::uuid_v7(20),
    );
    let other = DataFileRow::new(
        DataFileId(3),
        table,
        "main/orders/eu/file.parquet",
        20,
        200,
        CatalogOrderId::uuid_v7(20),
    );

    append_data_file(&mut kv, catalog, matching.clone()).unwrap();
    append_data_file(&mut kv, catalog, other.clone()).unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(matching.data_file_id, table, partition_key, "us-east-1"),
    )
    .unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(other.data_file_id, table, partition_key, "eu-west-1"),
    )
    .unwrap();

    let files =
        list_current_data_files_by_partition_value(&kv, catalog, table, partition_key, "us-east-1")
            .unwrap();

    assert_eq!(files, vec![matching]);
}

#[test]
fn given_partition_value_replaced_when_pruning_then_old_lookup_is_removed() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let partition_key = PartitionKeyIndex(1);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/file.parquet",
        20,
        200,
        CatalogOrderId::uuid_v7(20),
    );

    append_data_file(&mut kv, catalog, file.clone()).unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(file.data_file_id, table, partition_key, "old"),
    )
    .unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(file.data_file_id, table, partition_key, "new"),
    )
    .unwrap();

    let old = list_current_data_files_by_partition_value(&kv, catalog, table, partition_key, "old")
        .unwrap();
    let new = list_current_data_files_by_partition_value(&kv, catalog, table, partition_key, "new")
        .unwrap();

    assert!(old.is_empty());
    assert_eq!(new, vec![file]);
}

#[test]
fn given_partitioned_file_expired_when_pruning_then_partition_lookup_is_cleaned() {
    let catalog = CatalogId(7);
    let table = TableId(42);
    let partition_key = PartitionKeyIndex(1);
    let mut kv = FakeOrderedCatalogKv::new();
    let file = DataFileRow::new(
        DataFileId(2),
        table,
        "main/orders/file.parquet",
        20,
        200,
        CatalogOrderId::uuid_v7(20),
    );

    append_data_file(&mut kv, catalog, file.clone()).unwrap();
    register_file_partition_value(
        &mut kv,
        catalog,
        FilePartitionValueRow::new(file.data_file_id, table, partition_key, "us-east-1"),
    )
    .unwrap();
    expire_data_file(
        &mut kv,
        catalog,
        file.data_file_id,
        CatalogOrderId::uuid_v7(30),
    )
    .unwrap();

    let files =
        list_current_data_files_by_partition_value(&kv, catalog, table, partition_key, "us-east-1")
            .unwrap();

    assert!(files.is_empty());
}
