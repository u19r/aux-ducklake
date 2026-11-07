use crate::ColumnId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePartitionFieldRow {
    pub partition_key_index: u64,
    pub column_id: ColumnId,
    pub transform: String,
}

impl TablePartitionFieldRow {
    #[must_use]
    pub fn new(
        partition_key_index: u64,
        column_id: ColumnId,
        transform: impl Into<String>,
    ) -> Self {
        Self {
            partition_key_index,
            column_id,
            transform: transform.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePartitionRow {
    pub partition_id: u64,
    pub fields: Vec<TablePartitionFieldRow>,
}

impl TablePartitionRow {
    #[must_use]
    pub fn new(partition_id: u64, fields: Vec<TablePartitionFieldRow>) -> Self {
        Self {
            partition_id,
            fields,
        }
    }
}
