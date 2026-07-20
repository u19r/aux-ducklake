use crate::{
    CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId, OrderedCatalogKv, SchemaId,
    TableId, TableRow, list_tables_at, load_table_at, runtime_tabular_payload::parse_u64_field,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InlineTableName<'a> {
    Generated {
        raw: &'a str,
        table_id: TableId,
        schema_id: SchemaId,
    },
    Legacy {
        raw: &'a str,
    },
}

impl<'a> InlineTableName<'a> {
    pub(super) fn parse(raw: &'a str) -> Self {
        let Some(tail) = raw.strip_prefix("ducklake_inlined_data_") else {
            return Self::Legacy { raw };
        };
        let Some((table_id, schema_id)) = tail.split_once('_') else {
            return Self::Legacy { raw };
        };
        if table_id.is_empty() || schema_id.is_empty() || schema_id.contains('_') {
            return Self::Legacy { raw };
        }
        let Ok(table_id) = parse_u64_field("ReadInlineRows", table_id, "inline table id") else {
            return Self::Legacy { raw };
        };
        let Ok(schema_id) = parse_u64_field("ReadInlineRows", schema_id, "inline schema id") else {
            return Self::Legacy { raw };
        };
        Self::Generated {
            raw,
            table_id: TableId(table_id),
            schema_id: SchemaId(schema_id),
        }
    }
}

pub(super) fn load_inlined_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
    inline_table: InlineTableName<'_>,
) -> CatalogResult<(TableRow, SchemaId)> {
    let InlineTableName::Generated {
        raw,
        table_id,
        schema_id,
    } = inline_table
    else {
        let tables = list_tables_at(kv, catalog, snapshot_order)?;
        let (table, schema_id) = find_legacy_inlined_table(&tables, inline_table)?;
        return Ok((table.clone(), schema_id));
    };
    let table = load_table_at(kv, catalog, table_id, snapshot_order)?
        .ok_or(crate::CatalogError::NotFound("inlined table"))?;
    let has_inline_registration = table
        .inlined_data_tables
        .iter()
        .any(|inlined| inlined.table_name == raw && inlined.schema_version == schema_id.0);
    if has_inline_registration {
        return Ok((table, schema_id));
    }
    Err(crate::CatalogError::NotFound("inlined table registration"))
}

pub(super) fn find_legacy_inlined_table<'a>(
    tables: &'a [TableRow],
    inline_table: InlineTableName<'_>,
) -> CatalogResult<(&'a TableRow, SchemaId)> {
    let InlineTableName::Legacy { raw } = inline_table else {
        return Err(crate::CatalogError::NotFound("inlined table"));
    };
    for table in tables {
        for inlined in &table.inlined_data_tables {
            if inlined.table_name == raw {
                return Ok((table, SchemaId(inlined.schema_version)));
            }
        }
    }
    Err(crate::CatalogError::NotFound("inlined table"))
}

pub(super) fn missing_snapshot(snapshot_id: DuckLakeSnapshotId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot {snapshot_id} does not exist"))
}

pub(super) fn missing_snapshot_order(order: CatalogOrderId) -> crate::CatalogError {
    crate::CatalogError::Decode(format!("snapshot order {order} does not exist"))
}
