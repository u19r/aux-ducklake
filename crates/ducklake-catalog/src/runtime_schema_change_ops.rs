use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogResult, ColumnDefaultChange, ColumnRename,
    ColumnTypeChange, DuckLakeSnapshotId, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, RangeItem, RawSnapshotSequence, TableColumnRow, TableId,
    TableVersionReplacement, commit_append_table_columns_with_conflict_check,
    commit_change_table_column_defaults, commit_change_table_column_defaults_with_conflict_check,
    commit_change_table_column_types_with_conflict_check,
    commit_change_table_comments_with_conflict_check,
    commit_change_table_partition_with_conflict_check,
    commit_change_table_sort_with_conflict_check, commit_drop_table_columns_with_conflict_check,
    commit_rename_table_columns, commit_rename_table_columns_with_conflict_check,
    commit_rename_tables_with_conflict_check, latest_snapshot,
    normalize_column_renames_to_current_shape,
    runtime_protocol::RuntimeCatalogBackend,
    runtime_schema_change_payload::{
        ADD_COLUMNS, CHANGE_COLUMN_DEFAULTS, CHANGE_COLUMN_TYPES, CHANGE_COMMENTS,
        CHANGE_PARTITION_KEYS, CHANGE_SORT_KEYS, DROP_COLUMNS, DdlPayload, RENAME_COLUMNS,
        RENAME_TABLES, one_column_table, parse_column_drops, parse_column_rows,
        parse_comment_changes, parse_partition_changes, parse_sort_change, parse_table_renames,
    },
    snapshot_by_public_sequence,
    table_store::load_current_table_row,
};

#[cfg(feature = "foundationdb")]
use crate::FdbOrderedCatalogKv;

pub(crate) fn add_columns(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(ADD_COLUMNS, payload)?;
    let columns = parse_column_rows(ADD_COLUMNS, &ddl.rows)?;
    let Some(table_id) = one_column_table(ADD_COLUMNS, &columns)? else {
        return Ok(b"added_column_count=0\n".to_vec());
    };
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    let (append_columns, default_changes, renames) =
        split_add_columns_by_current_table(&kv, catalog, table_id, columns)?;
    let count = append_columns.len();
    if !append_columns.is_empty() {
        commit_append_table_columns_with_conflict_check(
            &mut kv,
            catalog,
            table_id,
            base,
            through,
            append_columns,
        )?;
    }
    if !default_changes.is_empty() {
        commit_change_table_column_defaults(&mut kv, catalog, &default_changes)?;
    }
    if !renames.is_empty() {
        commit_rename_table_columns(&mut kv, catalog, &renames)?;
    }
    Ok(format!("added_column_count={count}\n").into_bytes())
}

pub(crate) fn rename_columns(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(RENAME_COLUMNS, payload)?;
    let renames = parse_column_rows(RENAME_COLUMNS, &ddl.rows)?
        .into_iter()
        .map(|(table_id, column)| ColumnRename::new(table_id, column))
        .collect::<Vec<_>>();
    let count = renames.len();
    if renames.is_empty() {
        return Ok(b"renamed_column_count=0\n".to_vec());
    }
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    let renames = normalize_column_renames_to_current_shape(&kv, catalog, &renames)?;
    commit_rename_table_columns_with_conflict_check(&mut kv, catalog, base, through, &renames)?;
    Ok(format!("renamed_column_count={count}\n").into_bytes())
}

pub(crate) fn change_column_types(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(CHANGE_COLUMN_TYPES, payload)?;
    let changes = parse_column_rows(CHANGE_COLUMN_TYPES, &ddl.rows)?
        .into_iter()
        .map(|(table_id, column)| ColumnTypeChange::new(table_id, column))
        .collect::<Vec<_>>();
    let count = changes.len();
    if changes.is_empty() {
        return Ok(b"changed_column_type_count=0\n".to_vec());
    }
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    commit_change_table_column_types_with_conflict_check(
        &mut kv, catalog, base, through, &changes,
    )?;
    Ok(format!("changed_column_type_count={count}\n").into_bytes())
}

pub(crate) fn change_column_defaults(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(CHANGE_COLUMN_DEFAULTS, payload)?;
    let changes = parse_column_rows(CHANGE_COLUMN_DEFAULTS, &ddl.rows)?
        .into_iter()
        .map(|(table_id, column)| ColumnDefaultChange::new(table_id, column))
        .collect::<Vec<_>>();
    let count = changes.len();
    if changes.is_empty() {
        return Ok(b"changed_column_default_count=0\n".to_vec());
    }
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    commit_change_table_column_defaults_with_conflict_check(
        &mut kv, catalog, base, through, &changes,
    )?;
    Ok(format!("changed_column_default_count={count}\n").into_bytes())
}

pub(crate) fn drop_columns(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(DROP_COLUMNS, payload)?;
    let drops = parse_column_drops(&ddl.rows)?;
    let count = drops.len();
    if drops.is_empty() {
        return Ok(b"dropped_column_count=0\n".to_vec());
    }
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    commit_drop_table_columns_with_conflict_check(&mut kv, catalog, base, through, &drops)?;
    Ok(format!("dropped_column_count={count}\n").into_bytes())
}

pub(crate) fn rename_tables(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(RENAME_TABLES, payload)?;
    let renames = parse_table_renames(&ddl.rows)?;
    let count = renames.len();
    if renames.is_empty() {
        return Ok(b"renamed_table_count=0\n".to_vec());
    }
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    commit_rename_tables_with_conflict_check(&mut kv, catalog, base, through, &renames)?;
    Ok(format!("renamed_table_count={count}\n").into_bytes())
}

pub(crate) fn change_partition_keys(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(CHANGE_PARTITION_KEYS, payload)?;
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    let mut changed_count = 0;
    for change in parse_partition_changes(&ddl.rows)? {
        let changed = commit_change_table_partition_with_conflict_check(
            &mut kv,
            catalog,
            base,
            through,
            &change,
            Some(ddl.commit_raw_sequence()),
        )?;
        changed_count += usize::from(changed.is_some());
    }
    Ok(format!("changed_partition_count={changed_count}\n",).into_bytes())
}

pub(crate) fn change_sort_keys(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(CHANGE_SORT_KEYS, payload)?;
    let change = parse_sort_change(&ddl.rows)?;
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    let changed =
        commit_change_table_sort_with_conflict_check(&mut kv, catalog, base, through, &change)?;
    Ok(format!("changed_sort_count={}\n", usize::from(changed.is_some())).into_bytes())
}

pub(crate) fn change_comments(
    backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let ddl = DdlPayload::parse(CHANGE_COMMENTS, payload)?;
    let (table_comments, column_comments) = parse_comment_changes(&ddl.rows)?;
    let table_count = table_comments.len();
    let column_count = column_comments.len();
    let mut kv = open_runtime_catalog(backend)?;
    let (base, through) = load_ddl_orders(&kv, catalog, &ddl)?;
    let changed = commit_change_table_comments_with_conflict_check(
        &mut kv,
        catalog,
        base,
        through,
        &table_comments,
        &column_comments,
    )?;
    Ok(format!(
        "changed_table_comment_count={table_count}\nchanged_column_comment_count={column_count}\nchanged_comment_table={}\n",
        changed.map_or(0, |changed| changed.table_id.0)
    )
    .into_bytes())
}

pub(crate) enum RuntimeMutableCatalog {
    #[cfg(feature = "foundationdb")]
    FoundationDb(FdbOrderedCatalogKv),
    #[cfg(not(feature = "foundationdb"))]
    Unavailable,
}

impl OrderedCatalogKv for RuntimeMutableCatalog {
    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = key;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.get(key),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (prefix, direction, limit);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_prefix(prefix, direction, limit),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (start, end, direction, limit);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.scan_range(start, end, direction, limit),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = key;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.read_conflict_fence(key),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }
}

impl MutableCatalogKv for RuntimeMutableCatalog {
    fn generated_order_id(&mut self) -> CatalogResult<CatalogOrderId> {
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.generated_order_id(),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn commit(&mut self, batch: KvBatch) -> CatalogResult<()> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = batch;
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => kv.commit(batch),
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }

    fn commit_table_replacements(
        &mut self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        #[cfg(not(feature = "foundationdb"))]
        let _ = (catalog, previous_sequence, replacements);
        match self {
            #[cfg(feature = "foundationdb")]
            Self::FoundationDb(kv) => {
                kv.commit_table_replacements(catalog, previous_sequence, replacements)
            }
            #[cfg(not(feature = "foundationdb"))]
            Self::Unavailable => Err(foundationdb_runtime_unavailable()),
        }
    }
}

#[cfg(not(feature = "foundationdb"))]
fn foundationdb_runtime_unavailable() -> CatalogError {
    CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    )
}

pub(crate) fn open_runtime_catalog(
    _backend: RuntimeCatalogBackend,
) -> CatalogResult<RuntimeMutableCatalog> {
    open_runtime_foundationdb_catalog()
}

#[cfg(feature = "foundationdb")]
fn open_runtime_foundationdb_catalog() -> CatalogResult<RuntimeMutableCatalog> {
    crate::runtime_foundationdb::open_foundationdb_catalog()
        .map(RuntimeMutableCatalog::FoundationDb)
}

#[cfg(not(feature = "foundationdb"))]
fn open_runtime_foundationdb_catalog() -> CatalogResult<RuntimeMutableCatalog> {
    Err(CatalogError::Backend(
        "foundationdb runtime requires ducklake-catalog --features foundationdb".to_owned(),
    ))
}

fn load_ddl_orders(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    ddl: &DdlPayload<'_>,
) -> CatalogResult<(CatalogOrderId, CatalogOrderId)> {
    let base = ddl_base_snapshot(kv, catalog, ddl)?;
    let through =
        latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    Ok((base.order, through.order))
}

pub(crate) fn split_add_columns_by_current_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    columns: Vec<(TableId, TableColumnRow)>,
) -> CatalogResult<(
    Vec<TableColumnRow>,
    Vec<ColumnDefaultChange>,
    Vec<ColumnRename>,
)> {
    let _ = latest_snapshot(kv, catalog)?.ok_or(CatalogError::NotFound("catalog snapshot"))?;
    let table =
        load_current_table_row(kv, catalog, table_id)?.ok_or(CatalogError::NotFound("table"))?;
    let mut append_columns = Vec::new();
    let mut default_changes = Vec::new();
    let mut renames = Vec::new();
    for (column_table_id, column) in columns {
        if column_table_id != table_id {
            return Err(CatalogError::InvalidMutation(
                "AddColumns only supports one table per operation".to_owned(),
            ));
        }
        match table
            .columns
            .iter()
            .find(|existing| existing.column_id == column.column_id)
        {
            Some(existing) if same_column_identity(existing, &column) => {
                default_changes.push(ColumnDefaultChange::new(table_id, column));
            }
            Some(existing) if same_column_shape_except_name(existing, &column) => {
                renames.push(ColumnRename::new(table_id, column));
            }
            Some(_) => {
                return Err(CatalogError::InvalidMutation(format!(
                    "column id {} already exists on table {}",
                    column.column_id.0, table_id.0
                )));
            }
            None => append_columns.push(column),
        }
    }
    Ok((append_columns, default_changes, renames))
}

fn same_column_identity(existing: &TableColumnRow, proposed: &TableColumnRow) -> bool {
    existing.column_id == proposed.column_id
        && existing.name.eq_ignore_ascii_case(&proposed.name)
        && same_column_shape_except_name(existing, proposed)
}

fn same_column_shape_except_name(existing: &TableColumnRow, proposed: &TableColumnRow) -> bool {
    existing.column_id == proposed.column_id
        && existing.column_type == proposed.column_type
        && existing.nulls_allowed == proposed.nulls_allowed
        && existing.parent_id == proposed.parent_id
}

fn ddl_base_snapshot(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    ddl: &DdlPayload<'_>,
) -> CatalogResult<crate::SnapshotRow> {
    if let Some(read_snapshot_id) = ddl.read_snapshot_id {
        return snapshot_by_public_sequence(kv, catalog, read_snapshot_id)?
            .ok_or(CatalogError::NotFound("base snapshot"));
    }
    if let Some(snapshot) = snapshot_by_public_sequence(kv, catalog, ddl.commit_snapshot_id)? {
        return Ok(snapshot);
    }
    let base_snapshot_id = ddl.commit_snapshot_id.0.checked_sub(1).ok_or_else(|| {
        CatalogError::InvalidMutation("commit snapshot id must be greater than 0".to_owned())
    })?;
    snapshot_by_public_sequence(kv, catalog, DuckLakeSnapshotId(base_snapshot_id))?
        .ok_or(CatalogError::NotFound("base snapshot"))
}

#[cfg(test)]
#[path = "runtime_schema_change_ops_tests.rs"]
mod runtime_schema_change_ops_tests;
