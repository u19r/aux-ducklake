use crate::{
    CatalogError, CatalogResult, ColumnCommentChange, ColumnDrop, ColumnId, DuckLakeSnapshotId,
    RawSnapshotSequence, TableColumnRow, TableCommentChange, TableId, TablePartitionChange,
    TablePartitionFieldRow, TablePartitionRow, TableRename, TableSortChange, TableSortFieldRow,
    TableSortRow,
    runtime_tabular_payload::{
        TabularPayload, default_value_to_option, empty_to_none, parse_bool_field, parse_u64_field,
    },
};

pub(crate) const ADD_COLUMNS: &str = "AddColumns";
pub(crate) const RENAME_COLUMNS: &str = "RenameColumns";
pub(crate) const CHANGE_COLUMN_TYPES: &str = "ChangeColumnTypes";
pub(crate) const CHANGE_COLUMN_DEFAULTS: &str = "ChangeColumnDefaults";
pub(crate) const DROP_COLUMNS: &str = "DropColumns";
pub(crate) const RENAME_TABLES: &str = "RenameTables";
pub(crate) const CHANGE_PARTITION_KEYS: &str = "ChangePartitionKeys";
pub(crate) const CHANGE_SORT_KEYS: &str = "ChangeSortKeys";
pub(crate) const CHANGE_COMMENTS: &str = "ChangeComments";

pub(crate) struct DdlPayload<'a> {
    pub(crate) commit_snapshot_id: DuckLakeSnapshotId,
    pub(crate) read_snapshot_id: Option<DuckLakeSnapshotId>,
    pub(crate) rows: Vec<Vec<&'a str>>,
}

impl<'a> DdlPayload<'a> {
    pub(crate) fn commit_raw_sequence(&self) -> RawSnapshotSequence {
        RawSnapshotSequence(self.commit_snapshot_id.0)
    }

    pub(crate) fn parse(operation: &'static str, payload: &'a [u8]) -> CatalogResult<Self> {
        let mut commit_snapshot_id = None;
        let mut read_snapshot_id = None;
        let mut rows = Vec::new();
        for row in TabularPayload::new(operation, payload)? {
            let row = row?;
            let fields = row.fields();
            match fields.as_slice() {
                ["commit_snapshot", value] => {
                    commit_snapshot_id = Some(DuckLakeSnapshotId(parse_u64_field(
                        operation,
                        value,
                        "commit snapshot id",
                    )?));
                }
                ["read_snapshot", value] => {
                    read_snapshot_id = Some(DuckLakeSnapshotId(parse_u64_field(
                        operation,
                        value,
                        "read snapshot id",
                    )?));
                }
                _ => rows.push(fields.to_vec()),
            }
        }
        let commit_snapshot_id = commit_snapshot_id.ok_or_else(|| {
            CatalogError::Decode(format!("{operation} payload requires commit_snapshot row"))
        })?;
        Ok(Self {
            commit_snapshot_id,
            read_snapshot_id,
            rows,
        })
    }
}

pub(crate) fn parse_column_rows(
    operation: &'static str,
    rows: &[Vec<&str>],
) -> CatalogResult<Vec<(TableId, TableColumnRow)>> {
    let mut columns = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            [
                "column",
                table_id,
                column_id,
                name,
                column_type,
                nulls_allowed,
                parent_id,
                initial_default,
                default_value,
                default_value_type,
            ] => columns.push((
                TableId(parse_u64_field(operation, table_id, "table id")?),
                TableColumnRow::new(
                    ColumnId(parse_u64_field(operation, column_id, "column id")?),
                    *name,
                    *column_type,
                    parse_bool_field(operation, nulls_allowed, "column nulls_allowed")?,
                    optional_column_id(operation, parent_id)?,
                )
                .with_default_metadata(
                    empty_to_none(initial_default),
                    default_value_to_option(default_value, default_value_type),
                    (*default_value_type).to_owned(),
                )
                .with_created_with_table(false),
            )),
            _ => return Err(invalid_row(operation, fields)),
        }
    }
    Ok(columns)
}

pub(crate) fn one_column_table(
    operation: &'static str,
    columns: &[(TableId, TableColumnRow)],
) -> CatalogResult<Option<TableId>> {
    let Some((table_id, _)) = columns.first() else {
        return Ok(None);
    };
    if columns.iter().any(|(other, _)| other != table_id) {
        return Err(CatalogError::InvalidMutation(format!(
            "{operation} only supports one table per operation"
        )));
    }
    Ok(Some(*table_id))
}

pub(crate) fn parse_column_drops(rows: &[Vec<&str>]) -> CatalogResult<Vec<ColumnDrop>> {
    let mut drops = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            ["column", table_id, column_id] | [table_id, column_id] => {
                drops.push(ColumnDrop::new(
                    TableId(parse_u64_field(DROP_COLUMNS, table_id, "table id")?),
                    ColumnId(parse_u64_field(DROP_COLUMNS, column_id, "column id")?),
                ));
            }
            _ => return Err(invalid_row(DROP_COLUMNS, fields)),
        }
    }
    Ok(drops)
}

pub(crate) fn parse_table_renames(rows: &[Vec<&str>]) -> CatalogResult<Vec<TableRename>> {
    let mut renames = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            ["table", table_id, new_name] => renames.push(TableRename::new(
                TableId(parse_u64_field(RENAME_TABLES, table_id, "table id")?),
                *new_name,
            )),
            _ => return Err(invalid_row(RENAME_TABLES, fields)),
        }
    }
    Ok(renames)
}

pub(crate) fn parse_partition_changes(
    rows: &[Vec<&str>],
) -> CatalogResult<Vec<TablePartitionChange>> {
    let mut changes = Vec::new();
    let mut table_id = None;
    let mut partition_id = None;
    let mut fields_out = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            ["partition", raw_table_id, raw_partition_id] => {
                push_partition_change(
                    &mut changes,
                    &mut table_id,
                    &mut partition_id,
                    &mut fields_out,
                )?;
                table_id = Some(TableId(parse_u64_field(
                    CHANGE_PARTITION_KEYS,
                    raw_table_id,
                    "partition table id",
                )?));
                partition_id =
                    optional_u64(CHANGE_PARTITION_KEYS, raw_partition_id, "partition id")?;
            }
            [
                "partition_field",
                raw_table_id,
                raw_partition_id,
                key_index,
                column_id,
                transform,
            ] => {
                let field_table_id = TableId(parse_u64_field(
                    CHANGE_PARTITION_KEYS,
                    raw_table_id,
                    "partition field table id",
                )?);
                require_same_table(CHANGE_PARTITION_KEYS, &mut table_id, field_table_id)?;
                let field_partition_id = parse_u64_field(
                    CHANGE_PARTITION_KEYS,
                    raw_partition_id,
                    "partition field partition id",
                )?;
                require_same_optional_id(
                    CHANGE_PARTITION_KEYS,
                    &mut partition_id,
                    field_partition_id,
                    "partition field partition id",
                )?;
                fields_out.push(TablePartitionFieldRow::new(
                    parse_u64_field(CHANGE_PARTITION_KEYS, key_index, "partition key index")?,
                    ColumnId(parse_u64_field(
                        CHANGE_PARTITION_KEYS,
                        column_id,
                        "partition column id",
                    )?),
                    *transform,
                ));
            }
            _ => return Err(invalid_row(CHANGE_PARTITION_KEYS, fields)),
        }
    }
    push_partition_change(
        &mut changes,
        &mut table_id,
        &mut partition_id,
        &mut fields_out,
    )?;
    if changes.is_empty() {
        return Err(CatalogError::Decode(
            "ChangePartitionKeys requires a partition row".to_owned(),
        ));
    }
    Ok(changes)
}

fn push_partition_change(
    changes: &mut Vec<TablePartitionChange>,
    table_id: &mut Option<TableId>,
    partition_id: &mut Option<u64>,
    fields_out: &mut Vec<TablePartitionFieldRow>,
) -> CatalogResult<()> {
    let Some(table_id) = table_id.take() else {
        return Ok(());
    };
    let partition = if fields_out.is_empty() {
        None
    } else {
        partition_id
            .take()
            .map(|id| TablePartitionRow::new(id, std::mem::take(fields_out)))
    };
    changes.push(TablePartitionChange::new(table_id, partition));
    Ok(())
}

pub(crate) fn parse_sort_change(rows: &[Vec<&str>]) -> CatalogResult<TableSortChange> {
    let mut table_id = None;
    let mut sort_id = None;
    let mut fields_out = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            ["sort", raw_table_id, raw_sort_id] => {
                reject_second_key_change(CHANGE_SORT_KEYS, table_id)?;
                table_id = Some(TableId(parse_u64_field(
                    CHANGE_SORT_KEYS,
                    raw_table_id,
                    "sort table id",
                )?));
                sort_id = optional_u64(CHANGE_SORT_KEYS, raw_sort_id, "sort id")?;
            }
            [
                "sort_field",
                raw_table_id,
                raw_sort_id,
                key_index,
                expression,
                dialect,
                sort_direction,
                null_order,
            ] => {
                let field_table_id = TableId(parse_u64_field(
                    CHANGE_SORT_KEYS,
                    raw_table_id,
                    "sort field table id",
                )?);
                require_same_table(CHANGE_SORT_KEYS, &mut table_id, field_table_id)?;
                let field_sort_id =
                    parse_u64_field(CHANGE_SORT_KEYS, raw_sort_id, "sort field sort id")?;
                require_same_optional_id(
                    CHANGE_SORT_KEYS,
                    &mut sort_id,
                    field_sort_id,
                    "sort field sort id",
                )?;
                fields_out.push(TableSortFieldRow::new(
                    parse_u64_field(CHANGE_SORT_KEYS, key_index, "sort key index")?,
                    *expression,
                    *dialect,
                    *sort_direction,
                    *null_order,
                ));
            }
            _ => return Err(invalid_row(CHANGE_SORT_KEYS, fields)),
        }
    }
    let table_id = table_id
        .ok_or_else(|| CatalogError::Decode("ChangeSortKeys requires a sort row".to_owned()))?;
    Ok(TableSortChange::new(
        table_id,
        sort_id.map(|id| TableSortRow::new(id, fields_out)),
    ))
}

pub(crate) fn parse_comment_changes(
    rows: &[Vec<&str>],
) -> CatalogResult<(Vec<TableCommentChange>, Vec<ColumnCommentChange>)> {
    let mut table_comments = Vec::new();
    let mut column_comments = Vec::new();
    for fields in rows {
        match fields.as_slice() {
            ["table_comment", table_id, value_kind, value] => {
                table_comments.push(TableCommentChange::new(
                    TableId(parse_u64_field(
                        CHANGE_COMMENTS,
                        table_id,
                        "table comment table id",
                    )?),
                    parse_comment_value(value_kind, value)?,
                ));
            }
            ["column_comment", table_id, column_id, value_kind, value] => {
                column_comments.push(ColumnCommentChange::new(
                    TableId(parse_u64_field(
                        CHANGE_COMMENTS,
                        table_id,
                        "column comment table id",
                    )?),
                    ColumnId(parse_u64_field(
                        CHANGE_COMMENTS,
                        column_id,
                        "column comment column id",
                    )?),
                    parse_comment_value(value_kind, value)?,
                ));
            }
            _ => return Err(invalid_row(CHANGE_COMMENTS, fields)),
        }
    }
    Ok((table_comments, column_comments))
}

fn optional_column_id(operation: &'static str, value: &str) -> CatalogResult<Option<ColumnId>> {
    optional_u64(operation, value, "parent column id").map(|id| id.map(ColumnId))
}

fn optional_u64(operation: &'static str, value: &str, field: &str) -> CatalogResult<Option<u64>> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u64_field(operation, value, field).map(Some)
    }
}

fn parse_comment_value(kind: &str, value: &str) -> CatalogResult<Option<String>> {
    match kind {
        "value" => Ok(Some(value.to_owned())),
        "null" if value.is_empty() => Ok(None),
        "null" => Err(CatalogError::Decode(
            "ChangeComments null comment values must have an empty payload".to_owned(),
        )),
        _ => Err(CatalogError::Decode(format!(
            "ChangeComments payload has invalid comment value kind {kind}"
        ))),
    }
}

fn reject_second_key_change(
    operation: &'static str,
    existing: Option<TableId>,
) -> CatalogResult<()> {
    if existing.is_none() {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "{operation} only supports one table per operation"
    )))
}

fn require_same_table(
    operation: &'static str,
    existing: &mut Option<TableId>,
    table_id: TableId,
) -> CatalogResult<()> {
    match existing {
        Some(existing) if *existing == table_id => Ok(()),
        Some(_) => Err(CatalogError::Decode(format!(
            "{operation} field table id does not match table id"
        ))),
        None => {
            *existing = Some(table_id);
            Ok(())
        }
    }
}

fn require_same_optional_id(
    operation: &'static str,
    existing: &mut Option<u64>,
    value: u64,
    field: &str,
) -> CatalogResult<()> {
    match existing {
        Some(existing) if *existing == value => Ok(()),
        Some(_) => Err(CatalogError::Decode(format!(
            "{operation} {field} does not match header"
        ))),
        None => {
            *existing = Some(value);
            Ok(())
        }
    }
}

fn invalid_row(operation: &'static str, fields: &[&str]) -> CatalogError {
    CatalogError::Decode(format!(
        "{operation} payload has invalid row: {}",
        fields.join("\t")
    ))
}
