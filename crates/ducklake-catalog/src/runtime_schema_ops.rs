use crate::{
    CatalogId, CatalogOrderId, CatalogResult, RawSnapshotSequence, SchemaId, SchemaRow,
    runtime_foundationdb::{
        runtime_foundationdb_create_schemas, runtime_foundationdb_drop_schemas,
    },
    runtime_protocol::RuntimeCatalogBackend,
    runtime_tabular_payload::{TabularPayload, parse_u64_field},
};

pub(crate) const CREATE_SCHEMAS: &str = "CreateSchemas";
pub(crate) const DROP_SCHEMAS: &str = "DropSchemas";

pub(crate) fn create_schemas(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let (snapshot_sequence, schemas) = create_schemas_payload_values(payload)?;
    let created = { runtime_foundationdb_create_schemas(catalog, schemas, snapshot_sequence)? };
    Ok(create_schemas_payload(created))
}

pub(crate) fn drop_schemas(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let schema_ids = drop_schemas_payload_values(payload)?;
    let dropped = { runtime_foundationdb_drop_schemas(catalog, &schema_ids)? };
    Ok(drop_schemas_payload(dropped))
}

pub(crate) fn create_schemas_payload_values(
    payload: &[u8],
) -> CatalogResult<(Option<RawSnapshotSequence>, Vec<SchemaRow>)> {
    let mut commit_raw_snapshot = None;
    let mut schemas = Vec::new();
    for row in TabularPayload::new(CREATE_SCHEMAS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", snapshot_id] => {
                commit_raw_snapshot = Some(RawSnapshotSequence(parse_u64_field(
                    CREATE_SCHEMAS,
                    snapshot_id,
                    "commit snapshot id",
                )?));
            }
            ["read_snapshot", _] => {}
            ["schema", id, uuid, name, path] => {
                schemas.push(SchemaRow::new(
                    SchemaId(parse_u64_field(CREATE_SCHEMAS, id, "schema id")?),
                    *uuid,
                    *name,
                    *path,
                    CatalogOrderId::uuid_v7(0),
                ));
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok((commit_raw_snapshot, schemas))
}

fn create_schemas_payload(created: Vec<SchemaRow>) -> Vec<u8> {
    let mut out = format!("created_schema_count={}\n", created.len());
    for schema in created {
        out.push_str(&format!(
            "schema\t{}\t{}\t{}\t{}\n",
            schema.schema_id.0, schema.uuid, schema.name, schema.path
        ));
    }
    out.into_bytes()
}

pub(crate) fn drop_schemas_payload_values(payload: &[u8]) -> CatalogResult<Vec<SchemaId>> {
    let mut schemas = Vec::new();
    for row in TabularPayload::new(DROP_SCHEMAS, payload)? {
        let row = row?;
        let fields = row.fields();
        match fields.as_slice() {
            ["commit_snapshot", _] | ["read_snapshot", _] => {}
            ["schema", id] => {
                schemas.push(SchemaId(parse_u64_field(DROP_SCHEMAS, id, "schema id")?));
            }
            _ => return Err(row.invalid()),
        }
    }
    Ok(schemas)
}

fn drop_schemas_payload(dropped: Vec<SchemaRow>) -> Vec<u8> {
    let mut out = format!("dropped_schema_count={}\n", dropped.len());
    for schema in dropped {
        out.push_str(&format!(
            "schema\t{}\t{}\n",
            schema.schema_id.0, schema.name
        ));
    }
    out.into_bytes()
}
