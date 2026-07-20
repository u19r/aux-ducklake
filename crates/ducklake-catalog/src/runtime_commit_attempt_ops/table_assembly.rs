use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogId, CatalogResult, OrderedCatalogKv, TableColumnRow, TableId, TableRow,
    TableVersionReplacement, load_table_at, table_store::list_tables_at,
};

use crate::runtime_commit_attempt_ops::*;
pub(super) struct TableIntentAssembler<'a, K>
where
    K: OrderedCatalogKv,
{
    kv: &'a K,
    catalog: CatalogId,
    base_order: crate::CatalogOrderId,
    tables: BTreeMap<TableId, TableIntentTable>,
    created_tables: BTreeMap<TableId, TableRow>,
    created_table_ids: BTreeMap<TableId, TableId>,
}

impl<'a, K> TableIntentAssembler<'a, K>
where
    K: OrderedCatalogKv,
{
    pub(super) fn new(
        kv: &'a K,
        catalog: CatalogId,
        base_order: crate::CatalogOrderId,
    ) -> CatalogResult<Self> {
        Ok(Self {
            kv,
            catalog,
            base_order,
            tables: BTreeMap::new(),
            created_tables: BTreeMap::new(),
            created_table_ids: BTreeMap::new(),
        })
    }

    pub(super) fn table_mut(&mut self, table_id: TableId) -> CatalogResult<&mut TableRow> {
        if self.created_tables.contains_key(&table_id) {
            return self
                .created_tables
                .get_mut(&table_id)
                .ok_or(CatalogError::NotFound("table"));
        }
        if !self.tables.contains_key(&table_id) {
            let table = load_table_at(self.kv, self.catalog, table_id, self.base_order)?
                .ok_or(CatalogError::NotFound("table"))?;
            self.tables.insert(
                table_id,
                TableIntentTable {
                    previous: table.clone(),
                    next: table,
                },
            );
        }
        self.tables
            .get_mut(&table_id)
            .map(|table| &mut table.next)
            .ok_or(CatalogError::NotFound("table"))
    }

    pub(super) fn apply_create_table_fact(&mut self, table: TableRow) -> CatalogResult<()> {
        if self.created_tables.contains_key(&table.table_id)
            || self.tables.contains_key(&table.table_id)
        {
            return Err(CatalogError::InvalidMutation(format!(
                "table id {} appears more than once in CommitAttempt",
                table.table_id.0
            )));
        }
        let requested_table_id = table.table_id;
        let persisted_table_id = self.persisted_table_id_for_create(requested_table_id)?;
        let mut persisted = table;
        persisted.table_id = persisted_table_id;
        self.created_tables.insert(requested_table_id, persisted);
        self.created_table_ids
            .insert(requested_table_id, persisted_table_id);
        Ok(())
    }

    pub(super) fn apply_rename_table_fact(
        &mut self,
        table_id: TableId,
        new_name: String,
    ) -> CatalogResult<()> {
        self.table_mut(table_id)?.name = new_name;
        Ok(())
    }

    pub(super) fn apply_add_column_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        match table
            .columns
            .iter_mut()
            .find(|existing| existing.column_id == column.column_id)
        {
            Some(existing) if same_column_identity(existing, &column) => {
                apply_column_default(existing, &column);
                Ok(())
            }
            Some(existing) if same_column_shape_except_name(existing, &column) => {
                apply_column_default(existing, &column);
                existing.name = column.name;
                Ok(())
            }
            Some(existing) => {
                apply_column_default(existing, &column);
                existing.name = column.name;
                existing.column_type = column.column_type;
                existing.nulls_allowed = column.nulls_allowed;
                existing.parent_id = column.parent_id;
                Ok(())
            }
            None => {
                if let Some(existing_index) = table.columns.iter().position(|existing| {
                    existing.parent_id == column.parent_id
                        && existing.name.eq_ignore_ascii_case(&column.name)
                }) {
                    table.columns[existing_index] = column;
                    return Ok(());
                }
                table.columns.push(column);
                Ok(())
            }
        }
    }

    pub(super) fn apply_rename_column_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing_index) = table
            .columns
            .iter()
            .position(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        if table.columns.iter().enumerate().any(|(index, existing)| {
            index != existing_index && existing.name.eq_ignore_ascii_case(&column.name)
        }) {
            return Err(CatalogError::InvalidMutation(format!(
                "column name {} already exists on table {}",
                column.name, table_id.0
            )));
        }
        let existing = &mut table.columns[existing_index];
        reject_column_shape_change(existing, &column, table_id)?;
        apply_column_default(existing, &column);
        existing.name = column.name;
        Ok(())
    }

    pub(super) fn apply_column_default_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing) = table
            .columns
            .iter_mut()
            .find(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        reject_column_shape_change(existing, &column, table_id)?;
        if !existing.name.eq_ignore_ascii_case(&column.name) {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} default change cannot rename column on table {}",
                column.column_id.0, table_id.0
            )));
        }
        apply_column_default(existing, &column);
        Ok(())
    }

    pub(super) fn apply_column_type_fact(
        &mut self,
        table_id: TableId,
        column: TableColumnRow,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(existing_index) = table
            .columns
            .iter()
            .position(|existing| existing.column_id == column.column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        if table
            .columns
            .iter()
            .any(|existing| existing.parent_id == Some(column.column_id))
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change a parent column on table {}",
                column.column_id.0, table_id.0
            )));
        }
        let existing = &mut table.columns[existing_index];
        if !existing.name.eq_ignore_ascii_case(&column.name)
            || existing.parent_id != column.parent_id
        {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change identity metadata on table {}",
                column.column_id.0, table_id.0
            )));
        }
        if existing.parent_id.is_none() && existing.nulls_allowed != column.nulls_allowed {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type change cannot change top-level nullability on table {}",
                column.column_id.0, table_id.0
            )));
        }
        if existing.column_type == column.column_type {
            return Err(CatalogError::InvalidMutation(format!(
                "column {} type is unchanged on table {}",
                column.column_id.0, table_id.0
            )));
        }
        existing.column_type = column.column_type.clone();
        existing.nulls_allowed = column.nulls_allowed;
        apply_column_default(existing, &column);
        Ok(())
    }

    pub(super) fn apply_drop_column_fact(
        &mut self,
        table_id: TableId,
        column_id: crate::ColumnId,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(index) = table
            .columns
            .iter()
            .position(|column| column.column_id == column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        table.columns.remove(index);
        Ok(())
    }

    pub(super) fn apply_table_comment_fact(
        &mut self,
        table_id: TableId,
        comment: Option<String>,
    ) -> CatalogResult<()> {
        self.table_mut(table_id)?.comment = comment;
        Ok(())
    }

    pub(super) fn apply_column_comment_fact(
        &mut self,
        table_id: TableId,
        column_id: crate::ColumnId,
        comment: Option<String>,
    ) -> CatalogResult<()> {
        let table = self.table_mut(table_id)?;
        let Some(column) = table
            .columns
            .iter_mut()
            .find(|column| column.column_id == column_id)
        else {
            return Err(CatalogError::NotFound("column"));
        };
        column.comment = comment;
        Ok(())
    }

    pub(super) fn into_commit_parts(self) -> CatalogResult<TableCommitParts> {
        self.reject_duplicate_table_names()?;
        let replacements = self
            .tables
            .into_iter()
            .filter(|(_, table)| table.previous != table.next)
            .map(|(table_id, table)| -> CatalogResult<_> {
                reject_duplicate_column_names(&table.next, table_id)?;
                Ok(TableVersionReplacement::new(
                    table_id,
                    table.previous,
                    table.next,
                ))
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let created_tables = self
            .created_tables
            .into_iter()
            .map(|(requested_table_id, table)| {
                reject_duplicate_column_names(&table, table.table_id)?;
                Ok(CreatedTable::new(requested_table_id, table))
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let created = created_tables
            .iter()
            .map(|table| table.persisted.clone())
            .collect();
        Ok(TableCommitParts {
            created,
            replacements,
            created_tables,
        })
    }

    fn reject_duplicate_table_names(&self) -> CatalogResult<()> {
        let mut tables = list_tables_at(self.kv, self.catalog, self.base_order)?;
        for (table_id, table) in &self.tables {
            if let Some(existing) = tables
                .iter_mut()
                .find(|existing| existing.table_id == *table_id)
            {
                *existing = table.next.clone();
            }
        }
        tables.extend(self.created_tables.values().cloned());
        for (index, table) in tables.iter().enumerate() {
            if tables[..index].iter().any(|previous| {
                previous.schema_id == table.schema_id
                    && previous.name.eq_ignore_ascii_case(&table.name)
            }) {
                return Err(CatalogError::InvalidMutation(format!(
                    "conflict creating table {}: name already exists in schema {}",
                    table.name, table.schema_id.0
                )));
            }
        }
        Ok(())
    }

    fn persisted_table_id_for_create(&self, requested_table_id: TableId) -> CatalogResult<TableId> {
        let current_tables = list_tables_at(self.kv, self.catalog, self.base_order)?;
        if !current_tables
            .iter()
            .any(|table| table.table_id == requested_table_id)
            && !self
                .created_tables
                .values()
                .any(|table| table.table_id == requested_table_id)
        {
            return Ok(requested_table_id);
        }
        let max_current = current_tables
            .iter()
            .map(|table| table.table_id.0)
            .max()
            .unwrap_or(0);
        let max_created = self
            .created_tables
            .values()
            .map(|table| table.table_id.0)
            .max()
            .unwrap_or(0);
        Ok(TableId(max_current.max(max_created).saturating_add(1)))
    }
}
