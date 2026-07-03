use crate::{
    CatalogError, CatalogId, CatalogResult, MutableCatalogKv, TableId,
    ids::{CatalogOrderId, ColumnId},
    store::latest_snapshot,
    table_store::{load_current_table_row, reject_table_conflicts_since_base},
    table_version_commit::commit_replaced_table_version,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCommentChange {
    pub table_id: TableId,
    pub comment: Option<String>,
}

impl TableCommentChange {
    #[must_use]
    pub fn new(table_id: TableId, comment: Option<impl Into<String>>) -> Self {
        Self {
            table_id,
            comment: comment.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCommentChange {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub comment: Option<String>,
}

impl ColumnCommentChange {
    #[must_use]
    pub fn new(table_id: TableId, column_id: ColumnId, comment: Option<impl Into<String>>) -> Self {
        Self {
            table_id,
            column_id,
            comment: comment.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedTableComments {
    pub table_id: TableId,
    pub table_comment: Option<TableCommentChange>,
    pub column_comments: Vec<ColumnCommentChange>,
}

pub fn commit_change_table_comments(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_comments: &[TableCommentChange],
    column_comments: &[ColumnCommentChange],
) -> CatalogResult<Option<ChangedTableComments>> {
    let Some(table_id) = validate_comment_batch(table_comments, column_comments)? else {
        return Ok(None);
    };
    let latest = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let previous =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;

    let mut next = previous.clone();
    let table_comment = apply_table_comments(&mut next, table_comments);
    apply_column_comments(&mut next, column_comments)?;
    commit_replaced_table_version(kv, catalog, table_id, latest.sequence, previous, next)?;

    Ok(Some(ChangedTableComments {
        table_id,
        table_comment,
        column_comments: column_comments.to_vec(),
    }))
}

pub fn commit_change_table_comments_with_conflict_check(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    table_comments: &[TableCommentChange],
    column_comments: &[ColumnCommentChange],
) -> CatalogResult<Option<ChangedTableComments>> {
    if let Some(table_id) = validate_comment_batch(table_comments, column_comments)? {
        reject_table_conflicts_since_base(kv, catalog, table_id, base_order, through_order)?;
    }
    commit_change_table_comments(kv, catalog, table_comments, column_comments)
}

fn validate_comment_batch(
    table_comments: &[TableCommentChange],
    column_comments: &[ColumnCommentChange],
) -> CatalogResult<Option<TableId>> {
    if table_comments.len() > 1 {
        return Err(CatalogError::InvalidMutation(
            "comment change supports at most one table comment per operation".to_owned(),
        ));
    }
    let table_id = table_comments
        .first()
        .map(|change| change.table_id)
        .or_else(|| column_comments.first().map(|change| change.table_id));
    let Some(expected_table_id) = table_id else {
        return Ok(None);
    };
    for comment in table_comments {
        reject_cross_table(expected_table_id, comment.table_id)?;
    }
    for (index, comment) in column_comments.iter().enumerate() {
        reject_cross_table(expected_table_id, comment.table_id)?;
        if column_comments[..index]
            .iter()
            .any(|previous| previous.column_id == comment.column_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} is listed more than once for comment change",
                comment.column_id.0
            )));
        }
    }
    Ok(Some(expected_table_id))
}

fn reject_cross_table(expected_table_id: TableId, table_id: TableId) -> CatalogResult<()> {
    if table_id == expected_table_id {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(
        "comment change only supports one table per operation".to_owned(),
    ))
}

fn apply_table_comments(
    table: &mut crate::TableRow,
    table_comments: &[TableCommentChange],
) -> Option<TableCommentChange> {
    let comment = table_comments.first()?.clone();
    table.comment = comment.comment.clone();
    Some(comment)
}

fn apply_column_comments(
    table: &mut crate::TableRow,
    column_comments: &[ColumnCommentChange],
) -> CatalogResult<()> {
    for change in column_comments {
        let Some(column) = table
            .columns
            .iter_mut()
            .find(|column| column.column_id == change.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        column.comment = change.comment.clone();
    }
    Ok(())
}
