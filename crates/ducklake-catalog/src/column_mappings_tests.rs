#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, ColumnId, ColumnMappingRow, FakeOrderedCatalogKv, NameMappingColumnRow, TableId,
        column_mappings::{list_column_mappings, put_column_mappings},
    };

    #[test]
    fn given_column_mappings_when_listed_then_round_trips_in_order() {
        let mut kv = FakeOrderedCatalogKv::new();
        let mut row = ColumnMappingRow::new(7, TableId(3), "map_by_name");
        row.columns.push(NameMappingColumnRow {
            column_id: ColumnId(1),
            source_name: "source".to_owned(),
            target_field_id: 2,
            parent_column: None,
            is_partition: false,
        });
        row.columns.push(NameMappingColumnRow {
            column_id: ColumnId(4),
            source_name: "part".to_owned(),
            target_field_id: 5,
            parent_column: Some(1),
            is_partition: true,
        });

        put_column_mappings(&mut kv, CatalogId(1), vec![row.clone()]).unwrap();

        assert_eq!(
            list_column_mappings(&kv, CatalogId(1), Some(7)).unwrap(),
            vec![row]
        );
        assert!(
            list_column_mappings(&kv, CatalogId(1), Some(8))
                .unwrap()
                .is_empty()
        );
    }
}
