#[cfg(test)]
mod tests {
    use crate::{
        CatalogOrderId, ColumnId, InlinedTableRow, TableColumnRow, TableId, TablePartitionFieldRow,
        TablePartitionRow, TableRow, ValidityWindow,
    };

    use super::super::same_user_visible_table_for_inline_insert;

    #[test]
    fn given_only_inline_table_registration_changed_when_checking_inline_insert_then_table_is_same()
    {
        let read_table = table_with_column(CatalogOrderId::from_u128(10));
        let mut current_table = table_with_column(CatalogOrderId::from_u128(11));
        current_table
            .inlined_data_tables
            .push(InlinedTableRow::new("ducklake_inlined_data_7_1", 1));

        assert!(same_user_visible_table_for_inline_insert(
            &read_table,
            &current_table
        ));
    }

    #[test]
    fn given_partition_metadata_changed_when_checking_inline_insert_then_table_is_different() {
        let read_table = table_with_column(CatalogOrderId::from_u128(10));
        let mut current_table = table_with_column(CatalogOrderId::from_u128(11));
        current_table.partition = Some(TablePartitionRow::new(
            1,
            vec![TablePartitionFieldRow::new(0, ColumnId(1), "identity")],
        ));

        assert!(!same_user_visible_table_for_inline_insert(
            &read_table,
            &current_table
        ));
    }

    fn table_with_column(order: CatalogOrderId) -> TableRow {
        let mut table = TableRow::new(TableId(7), "test", order);
        table.uuid = "table-uuid".to_owned();
        table.path = "main/test/".to_owned();
        table.validity = ValidityWindow::new(order, None);
        table
            .columns
            .push(TableColumnRow::new(ColumnId(1), "i", "INTEGER", true, None));
        table
    }
}
