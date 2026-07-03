use crate::{
    CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId, MacroId, MacroImplementationRow,
    MacroParameterRow, MacroRow, RawSnapshotSequence, SchemaId, TableId, ViewCommentChange,
    ViewRename, ViewRow,
    runtime_foundationdb_objects::{
        runtime_foundationdb_change_view_comment, runtime_foundationdb_create_macros,
        runtime_foundationdb_create_views, runtime_foundationdb_drop_macros,
        runtime_foundationdb_drop_tables, runtime_foundationdb_drop_views,
        runtime_foundationdb_rename_views,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_tabular_payload::{TabularPayload, empty_to_none, parse_u64_field},
};

pub(crate) const CREATE_VIEWS: &str = "CreateViews";
pub(crate) const RENAME_VIEWS: &str = "RenameViews";
pub(crate) const DROP_VIEWS: &str = "DropViews";
pub(crate) const CHANGE_VIEW_COMMENT: &str = "ChangeViewComment";
const CREATE_MACROS: &str = "CreateMacros";
const DROP_MACROS: &str = "DropMacros";
const DROP_TABLES: &str = "DropTables";

pub(crate) fn create_views(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let views = create_view_rows(payload)?;
    let count = { runtime_foundationdb_create_views(catalog, views)?.len() };
    Ok(format!("created_view_count={count}\n").into_bytes())
}

pub(crate) fn rename_views(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let renames = view_renames(payload)?;
    let count = { runtime_foundationdb_rename_views(catalog, renames)?.len() };
    Ok(format!("renamed_view_count={count}\n").into_bytes())
}

pub(crate) fn drop_views(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ids = table_ids(DROP_VIEWS, "view", payload)?;
    let count = runtime_foundationdb_drop_views(catalog, ids)?.len();
    Ok(format!("dropped_view_count={count}\n").into_bytes())
}

pub(crate) fn change_view_comment(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_snapshot, change) = view_comment_change(payload)?;
    {
        runtime_foundationdb_change_view_comment(catalog, commit_snapshot, change)?;
    };
    Ok(b"changed_view_comment_count=1\n".to_vec())
}

pub(crate) fn create_macros(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_snapshot, macros) = create_macro_rows(payload)?;
    let count = { runtime_foundationdb_create_macros(catalog, macros, commit_snapshot)?.len() };
    Ok(format!("created_macro_count={count}\n").into_bytes())
}

pub(crate) fn drop_macros(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_snapshot, ids) = macro_ids(payload)?;
    let count = { runtime_foundationdb_drop_macros(catalog, ids, commit_snapshot)?.len() };
    Ok(format!("dropped_macro_count={count}\n").into_bytes())
}

pub(crate) fn drop_tables(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_snapshot, ids) = table_ids_with_commit(DROP_TABLES, "table", payload)?;
    let count = { runtime_foundationdb_drop_tables(catalog, ids, commit_snapshot)?.len() };
    Ok(format!("dropped_table_count={count}\n").into_bytes())
}

pub(crate) fn create_view_rows(payload: &[u8]) -> CatalogResult<Vec<ViewRow>> {
    let mut views = Vec::new();
    for row in TabularPayload::new(CREATE_VIEWS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", _] | ["read_snapshot", _] => {}
            [
                "view",
                id,
                schema_id,
                uuid,
                name,
                dialect,
                sql,
                aliases,
                comment,
            ] => {
                views.push(
                    ViewRow::new(
                        TableId(parse_u64_field(CREATE_VIEWS, id, "view id")?),
                        SchemaId(parse_u64_field(CREATE_VIEWS, schema_id, "view schema id")?),
                        *uuid,
                        *name,
                        *dialect,
                        *sql,
                        parse_aliases(aliases),
                        CatalogOrderId::uuid_v7(0),
                    )
                    .with_comment(empty_to_none(comment)),
                );
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok(views)
}

pub(crate) fn view_renames(payload: &[u8]) -> CatalogResult<Vec<ViewRename>> {
    let mut renames = Vec::new();
    for row in TabularPayload::new(RENAME_VIEWS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", _] | ["read_snapshot", _] => {}
            ["view", id, name] => renames.push(ViewRename::new(
                TableId(parse_u64_field(RENAME_VIEWS, id, "view id")?),
                *name,
            )),
            _ => return Err(row.invalid()),
        }
    }
    Ok(renames)
}

pub(crate) fn view_comment_change(
    payload: &[u8],
) -> CatalogResult<(Option<DuckLakeSnapshotId>, ViewCommentChange)> {
    let mut commit_snapshot = None;
    let mut read_snapshot = None;
    let mut change = None;
    for row in TabularPayload::new(CHANGE_VIEW_COMMENT, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(DuckLakeSnapshotId(parse_u64_field(
                    CHANGE_VIEW_COMMENT,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["read_snapshot", snapshot_id] => {
                read_snapshot = Some(DuckLakeSnapshotId(parse_u64_field(
                    CHANGE_VIEW_COMMENT,
                    snapshot_id,
                    "read snapshot id",
                )?));
            }
            ["view_comment", view_id, value_kind, value] => {
                change = Some(ViewCommentChange::new(
                    TableId(parse_u64_field(
                        CHANGE_VIEW_COMMENT,
                        view_id,
                        "view comment view id",
                    )?),
                    comment_value(value_kind, value)?,
                ));
            }
            _ => return Err(row.invalid()),
        }
    }
    let change = change.ok_or_else(|| {
        crate::CatalogError::Decode("ChangeViewComment payload is empty".to_owned())
    })?;
    Ok((read_snapshot.or(commit_snapshot), change))
}

fn create_macro_rows(
    payload: &[u8],
) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<MacroRow>)> {
    let mut commit_snapshot = None;
    let mut macros = Vec::new();
    let mut current: Option<MacroRow> = None;
    let mut current_impl: Option<MacroImplementationRow> = None;
    for row in TabularPayload::new(CREATE_MACROS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    CREATE_MACROS,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["macro", macro_id, schema_id, name] => {
                finish_impl(&mut current, &mut current_impl);
                finish_macro(&mut macros, &mut current);
                current = Some(MacroRow::new(
                    MacroId(parse_u64_field(CREATE_MACROS, macro_id, "macro id")?),
                    SchemaId(parse_u64_field(
                        CREATE_MACROS,
                        schema_id,
                        "macro schema id",
                    )?),
                    *name,
                    Vec::new(),
                    CatalogOrderId::uuid_v7(0),
                ));
            }
            ["macro_impl", macro_id, _impl_id, dialect, sql, macro_type] => {
                require_current_macro(CREATE_MACROS, current.as_ref(), macro_id)?;
                finish_impl(&mut current, &mut current_impl);
                current_impl = Some(MacroImplementationRow {
                    dialect: (*dialect).to_owned(),
                    sql: (*sql).to_owned(),
                    macro_type: (*macro_type).to_owned(),
                    parameters: Vec::new(),
                });
            }
            [
                "macro_param",
                macro_id,
                _impl_id,
                _param_id,
                parameter_name,
                parameter_type,
                default_value,
                default_value_type,
            ] => {
                require_current_macro(CREATE_MACROS, current.as_ref(), macro_id)?;
                let Some(implementation) = current_impl.as_mut() else {
                    return Err(crate::CatalogError::Decode(
                        "CreateMacros payload has macro_param before macro_impl".to_owned(),
                    ));
                };
                implementation.parameters.push(MacroParameterRow {
                    parameter_name: (*parameter_name).to_owned(),
                    parameter_type: (*parameter_type).to_owned(),
                    default_value: (*default_value).to_owned(),
                    default_value_type: (*default_value_type).to_owned(),
                });
            }
            _ => return Err(row.invalid()),
        }
    }
    finish_impl(&mut current, &mut current_impl);
    finish_macro(&mut macros, &mut current);
    Ok((commit_snapshot, macros))
}

fn table_ids(operation: &'static str, tag: &str, payload: &[u8]) -> CatalogResult<Vec<TableId>> {
    Ok(table_ids_with_commit(operation, tag, payload)?.1)
}

pub(crate) fn drop_view_ids(payload: &[u8]) -> CatalogResult<Vec<TableId>> {
    table_ids(DROP_VIEWS, "view", payload)
}

fn table_ids_with_commit(
    operation: &'static str,
    tag: &str,
    payload: &[u8],
) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<TableId>)> {
    let mut commit_snapshot = None;
    let mut ids = Vec::new();
    for row in TabularPayload::new(operation, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    operation,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["read_snapshot", _] => {}
            [prefix, id] if *prefix == tag => {
                ids.push(TableId(parse_u64_field(operation, id, "table id")?));
            }
            [id] => ids.push(TableId(parse_u64_field(operation, id, "table id")?)),
            _ => return Err(row.invalid()),
        }
    }
    Ok((commit_snapshot, ids))
}

fn macro_ids(payload: &[u8]) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<MacroId>)> {
    let mut commit_snapshot = None;
    let mut ids = Vec::new();
    for row in TabularPayload::new(DROP_MACROS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    DROP_MACROS,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["macro", id] | [id] => {
                ids.push(MacroId(parse_u64_field(DROP_MACROS, id, "macro id")?));
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok((commit_snapshot, ids))
}

fn parse_aliases(value: &str) -> Vec<String> {
    if value.is_empty() {
        Vec::new()
    } else {
        value.split('\x1f').map(ToOwned::to_owned).collect()
    }
}

fn comment_value(kind: &str, value: &str) -> CatalogResult<Option<String>> {
    match kind {
        "null" => Ok(None),
        "value" => Ok(Some(value.to_owned())),
        _ => Err(crate::CatalogError::Decode(format!(
            "ChangeViewComment payload has invalid comment value kind {kind}"
        ))),
    }
}

fn finish_impl(current: &mut Option<MacroRow>, current_impl: &mut Option<MacroImplementationRow>) {
    if let Some(implementation) = current_impl.take() {
        if let Some(macro_row) = current.as_mut() {
            macro_row.implementations.push(implementation);
        }
    }
}

fn finish_macro(macros: &mut Vec<MacroRow>, current: &mut Option<MacroRow>) {
    if let Some(macro_row) = current.take() {
        macros.push(macro_row);
    }
}

fn require_current_macro(
    operation: &'static str,
    current: Option<&MacroRow>,
    macro_id: &str,
) -> CatalogResult<()> {
    let Some(macro_row) = current else {
        return Err(crate::CatalogError::Decode(format!(
            "{operation} payload references macro details before macro"
        )));
    };
    let parsed = parse_u64_field(operation, macro_id, "macro id")?;
    if macro_row.macro_id.0 == parsed {
        return Ok(());
    }
    Err(crate::CatalogError::Decode(format!(
        "{operation} payload references macro {parsed} while building macro {}",
        macro_row.macro_id.0
    )))
}
