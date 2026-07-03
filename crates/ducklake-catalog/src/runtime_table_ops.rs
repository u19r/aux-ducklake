use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, ColumnId, OrderedCatalogKv,
    RawSnapshotSequence, SchemaId, TableColumnRow, TableId, TableRow, latest_snapshot,
    list_tables_at, load_table_at,
    runtime_foundationdb::{
        runtime_foundationdb_create_tables, runtime_foundationdb_replace_tables,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_tabular_payload::{
        TabularPayload, default_value_to_option, empty_to_none, parse_bool_field, parse_u64_field,
    },
    table_store::list_table_rows,
};

pub(crate) const CREATE_TABLES: &str = "CreateTables";
pub(crate) const REPLACE_TABLES: &str = "ReplaceTables";

pub(crate) fn get_next_column_id(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_id = TableId(crate::runtime_payload::payload_u64_value(
        payload,
        "table_id",
        "GetNextColumnId missing table_id",
    )?);
    let next_column_id = {
        let kv = open_foundationdb_catalog()?;
        next_column_id(&kv, catalog, table_id)?
    };
    Ok(format!("next_column_id={next_column_id}\n").into_bytes())
}

pub(crate) fn is_column_created_with_table(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let table_name = crate::runtime_payload::payload_string_value(
        payload,
        "table_name",
        "IsColumnCreatedWithTable missing table_name",
    )?;
    let column_name = crate::runtime_payload::payload_string_value(
        payload,
        "column_name",
        "IsColumnCreatedWithTable missing column_name",
    )?;
    let created = {
        let kv = open_foundationdb_catalog()?;
        column_created_with_table(&kv, catalog, &table_name, &column_name)?
    };
    Ok(format!("created_with_table={created}\n").into_bytes())
}

pub(crate) fn create_tables(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_raw_snapshot, tables) = create_tables_payload_values(payload)?;
    let requested_table_ids = tables
        .iter()
        .map(|table| table.table_id)
        .collect::<Vec<_>>();
    let created = { runtime_foundationdb_create_tables(catalog, tables, commit_raw_snapshot)? };
    Ok(create_tables_payload(&requested_table_ids, created))
}

pub(crate) fn replace_tables(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (commit_raw_snapshot, table_ids, tables) = replace_tables_payload_values(payload)?;
    let requested_table_ids = tables
        .iter()
        .map(|table| table.table_id)
        .collect::<Vec<_>>();
    let created =
        { runtime_foundationdb_replace_tables(catalog, table_ids, tables, commit_raw_snapshot)? };
    Ok(create_tables_payload(&requested_table_ids, created))
}

pub(crate) fn create_tables_payload_values(
    payload: &[u8],
) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<TableRow>)> {
    let mut commit_raw_snapshot = None;
    let mut tables = Vec::new();
    let mut current: Option<TableRow> = None;
    for row in TabularPayload::new(CREATE_TABLES, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_raw_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    CREATE_TABLES,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["read_snapshot", _] => {}
            ["table", id, schema_id, uuid, name, path, comment] => {
                push_current_table(&mut tables, &mut current);
                current = Some(table_row(id, schema_id, uuid, name, path, comment)?);
            }
            [
                "column",
                table_id,
                column_id,
                name,
                column_type,
                nulls_allowed,
                parent_id,
                comment,
                initial_default,
                default_value,
                default_value_type,
            ] => {
                let Some(table) = current.as_mut() else {
                    return Err(CatalogError::Decode(
                        "CreateTables payload has column before table".to_owned(),
                    ));
                };
                let parsed_table_id = parse_u64_field(CREATE_TABLES, table_id, "table id")?;
                if parsed_table_id != table.table_id.0 {
                    return Err(CatalogError::Decode(format!(
                        "CreateTables payload has column table id {parsed_table_id} for table {}",
                        table.table_id.0
                    )));
                }
                table.columns.push(table_column_row(
                    column_id,
                    name,
                    column_type,
                    nulls_allowed,
                    parent_id,
                    comment,
                    initial_default,
                    default_value,
                    default_value_type,
                )?);
            }
            _ => return Err(row.invalid()),
        }
    }
    push_current_table(&mut tables, &mut current);
    Ok((commit_raw_snapshot, tables))
}

pub(crate) fn replace_tables_payload_values(
    payload: &[u8],
) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<TableId>, Vec<TableRow>)> {
    let mut commit_raw_snapshot = None;
    let mut table_ids = Vec::new();
    let mut tables = Vec::new();
    let mut current: Option<TableRow> = None;
    for row in TabularPayload::new(REPLACE_TABLES, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_raw_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    REPLACE_TABLES,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["read_snapshot", _] => {}
            ["drop_table", id] => {
                table_ids.push(TableId(parse_u64_field(
                    REPLACE_TABLES,
                    id,
                    "drop table id",
                )?));
            }
            ["table", id, schema_id, uuid, name, path, comment] => {
                push_current_table(&mut tables, &mut current);
                current = Some(table_row(id, schema_id, uuid, name, path, comment)?);
            }
            [
                "column",
                table_id,
                column_id,
                name,
                column_type,
                nulls_allowed,
                parent_id,
                comment,
                initial_default,
                default_value,
                default_value_type,
            ] => {
                let Some(table) = current.as_mut() else {
                    return Err(CatalogError::Decode(
                        "ReplaceTables payload has column before table".to_owned(),
                    ));
                };
                let parsed_table_id = parse_u64_field(REPLACE_TABLES, table_id, "table id")?;
                if parsed_table_id != table.table_id.0 {
                    return Err(CatalogError::Decode(format!(
                        "ReplaceTables payload has column table id {parsed_table_id} for table {}",
                        table.table_id.0
                    )));
                }
                table.columns.push(table_column_row(
                    column_id,
                    name,
                    column_type,
                    nulls_allowed,
                    parent_id,
                    comment,
                    initial_default,
                    default_value,
                    default_value_type,
                )?);
            }
            _ => return Err(row.invalid()),
        }
    }
    push_current_table(&mut tables, &mut current);
    Ok((commit_raw_snapshot, table_ids, tables))
}

#[cfg(feature = "foundationdb")]
fn open_foundationdb_catalog() -> CatalogResult<crate::FdbOrderedCatalogKv> {
    crate::runtime_foundationdb::open_foundationdb_catalog()
}

#[cfg(not(feature = "foundationdb"))]
fn open_foundationdb_catalog() -> CatalogResult<crate::FakeOrderedCatalogKv> {
    Err(CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

fn next_column_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<u64> {
    let max_column_id = list_table_rows(kv, catalog)?
        .iter()
        .filter(|table| table.table_id == table_id)
        .flat_map(|table| table.columns.iter())
        .map(|column| column.column_id.0)
        .max();
    if max_column_id.is_none() {
        let snapshot =
            latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
        load_table_at(kv, catalog, table_id, snapshot.order)?
            .ok_or(CatalogError::NotFound("table"))?;
    }
    Ok(max_column_id.map_or(1, |max| max.saturating_add(1)))
}

fn column_created_with_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_name: &str,
    column_name: &str,
) -> CatalogResult<bool> {
    let snapshot =
        latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let tables = list_tables_at(kv, catalog, snapshot.order)?;
    Ok(tables
        .iter()
        .find(|table| table.name.eq_ignore_ascii_case(table_name))
        .and_then(|table| {
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(column_name))
        })
        .is_some_and(|column| column.created_with_table))
}

fn push_current_table(tables: &mut Vec<TableRow>, current: &mut Option<TableRow>) {
    if let Some(table) = current.take() {
        tables.push(table);
    }
}

fn table_row(
    id: &str,
    schema_id: &str,
    uuid: &str,
    name: &str,
    path: &str,
    comment: &str,
) -> CatalogResult<TableRow> {
    let mut table = TableRow::with_catalog_metadata(
        TableId(parse_u64_field(CREATE_TABLES, id, "table id")?),
        SchemaId(parse_u64_field(CREATE_TABLES, schema_id, "schema id")?),
        uuid.to_owned(),
        name.to_owned(),
        path.to_owned(),
        Vec::new(),
        CatalogOrderId::uuid_v7(0),
    );
    table.comment = empty_to_none(comment);
    Ok(table)
}

fn table_column_row(
    column_id: &str,
    name: &str,
    column_type: &str,
    nulls_allowed: &str,
    parent_id: &str,
    comment: &str,
    initial_default: &str,
    default_value: &str,
    default_value_type: &str,
) -> CatalogResult<TableColumnRow> {
    let parent_id = if parent_id.is_empty() {
        None
    } else {
        Some(ColumnId(parse_u64_field(
            CREATE_TABLES,
            parent_id,
            "column parent id",
        )?))
    };
    Ok(TableColumnRow::new(
        ColumnId(parse_u64_field(CREATE_TABLES, column_id, "column id")?),
        name.to_owned(),
        column_type.to_owned(),
        parse_bool_field(CREATE_TABLES, nulls_allowed, "column nulls_allowed")?,
        parent_id,
    )
    .with_default_metadata(
        empty_to_none(initial_default),
        default_value_to_option(default_value, default_value_type),
        default_value_type.to_owned(),
    )
    .with_comment(empty_to_none(comment)))
}

pub(crate) fn create_tables_payload(
    requested_table_ids: &[TableId],
    created: Vec<TableRow>,
) -> Vec<u8> {
    let mut payload = format!("created_table_count={}\n", created.len());
    for (requested_id, table) in requested_table_ids.iter().zip(created) {
        payload.push_str(&format!(
            "created_table\t{}\t{}\t{}\t{}\n",
            requested_id.0, table.table_id.0, table.schema_id.0, table.name
        ));
    }
    payload.into_bytes()
}

#[cfg(test)]
#[path = "runtime_table_ops_tests.rs"]
mod runtime_table_ops_tests;
