#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSortFieldRow {
    pub sort_key_index: u64,
    pub expression: String,
    pub dialect: String,
    pub sort_direction: String,
    pub null_order: String,
}

impl TableSortFieldRow {
    #[must_use]
    pub fn new(
        sort_key_index: u64,
        expression: impl Into<String>,
        dialect: impl Into<String>,
        sort_direction: impl Into<String>,
        null_order: impl Into<String>,
    ) -> Self {
        Self {
            sort_key_index,
            expression: expression.into(),
            dialect: dialect.into(),
            sort_direction: sort_direction.into(),
            null_order: null_order.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSortRow {
    pub sort_id: u64,
    pub fields: Vec<TableSortFieldRow>,
}

impl TableSortRow {
    #[must_use]
    pub fn new(sort_id: u64, fields: Vec<TableSortFieldRow>) -> Self {
        Self { sort_id, fields }
    }
}
