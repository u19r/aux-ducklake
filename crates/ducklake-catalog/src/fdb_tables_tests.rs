#[cfg(test)]
mod tests {
    use crate::{CatalogOrderId, ColumnId, RawSnapshotSequence, TableColumnRow, TableId, TableRow};

    use super::super::table_metadata_recovery_attempt_id;

    #[test]
    fn table_metadata_recovery_attempt_id_ignores_uncommitted_validity() {
        let mut first = table(TableId(7), "events");
        let mut second = table(TableId(7), "events");
        first.validity = crate::ValidityWindow::new(CatalogOrderId::uuid_v7(11), None);
        second.validity = crate::ValidityWindow::new(CatalogOrderId::uuid_v7(22), None);

        assert_eq!(
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(42)), &[], &[first]),
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(42)), &[], &[second])
        );
    }

    #[test]
    fn table_metadata_recovery_attempt_id_distinguishes_payloads() {
        let first = table(TableId(7), "events");
        let mut second = table(TableId(7), "events");
        second.columns.push(TableColumnRow::new(
            ColumnId(2),
            "extra",
            "VARCHAR",
            true,
            None,
        ));

        assert_ne!(
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(42)), &[], &[first]),
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(42)), &[], &[second])
        );
    }

    #[test]
    fn table_metadata_recovery_attempt_id_ignores_retry_allocated_table_ids() {
        let first = table(TableId(7), "events");
        let second = table(TableId(8), "events");

        assert_eq!(
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(42)), &[], &[first]),
            table_metadata_recovery_attempt_id(1, Some(RawSnapshotSequence(43)), &[], &[second])
        );
    }

    fn table(table_id: TableId, name: &str) -> TableRow {
        let mut table = TableRow::new(table_id, name, CatalogOrderId::uuid_v7(0));
        table.columns.push(TableColumnRow::new(
            ColumnId(1),
            "id",
            "INTEGER",
            true,
            None,
        ));
        table
    }
}
