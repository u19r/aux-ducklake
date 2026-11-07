use crate::runtime_protocol::RuntimePayload;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
#[cfg(not(test))]
use crate::{CatalogCacheNamespace, runtime_read_context::CatalogSnapshotPayloadIdentity};
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, DuckLakeSnapshotId, OrderedCatalogKv, SnapshotRow,
    conflict_watermarks::{
        ConflictWatermarkValues, load_conflict_snapshot_values, load_conflict_watermarks,
    },
    runtime_metrics::RuntimeMetricStage,
    runtime_read_context::{CatalogSnapshotReadContext, CatalogSnapshotRequestContext},
    schema_version_state::load_schema_version_at,
};

#[cfg(not(test))]
static CATALOG_SNAPSHOT_PAYLOAD_CACHE: OnceLock<
    BoundedCache<CatalogSnapshotPayloadIdentity, std::sync::Arc<Vec<u8>>>,
> = OnceLock::new();

#[cfg(not(test))]
static CONFLICT_SNAPSHOT_PAYLOAD_CACHE: OnceLock<
    BoundedCache<ConflictSnapshotPayloadCacheKey, std::sync::Arc<Vec<u8>>>,
> = OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ConflictSnapshotPayloadCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    order: CatalogOrderId,
}

pub(crate) fn snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot: Option<SnapshotRow>,
) -> CatalogResult<Vec<u8>> {
    let Some(row) = snapshot else {
        return Ok(b"catalog_snapshot_exists=false\n".to_vec());
    };
    let watermarks = snapshot_watermarks(kv, catalog, row.order)?;
    let schema_version = snapshot_schema_version_at(kv, catalog, row.order)?;
    let ducklake_snapshot_id = DuckLakeSnapshotId(row.sequence.0);
    Ok(format!(
        "catalog_snapshot_order={}\ncatalog_snapshot_sequence={}\nducklake_snapshot_id={}\nducklake_schema_version={}\nducklake_next_catalog_id={}\nducklake_next_file_id={}\n",
        row.order,
        ducklake_snapshot_id,
        ducklake_snapshot_id,
        schema_version,
        watermarks.next_catalog_id,
        watermarks.next_file_id
    )
    .into_bytes())
}

pub(crate) fn public_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
) -> CatalogResult<Vec<u8>> {
    snapshot_payload(
        kv,
        catalog,
        crate::snapshot_by_public_sequence(kv, catalog, snapshot_id)?,
    )
}

pub(crate) fn conflict_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<RuntimePayload> {
    let Some(row) = crate::store::prepared_latest_snapshot(
        kv,
        catalog,
        crate::catalog_request_read_set::CatalogHeaderProjection::Conflict,
    )?
    else {
        return Ok(b"catalog_snapshot_exists=false\n".to_vec().into());
    };
    conflict_snapshot_payload_for_row(kv, catalog, row)
}

pub(crate) fn conflict_snapshot_payload_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: SnapshotRow,
) -> CatalogResult<RuntimePayload> {
    #[cfg(not(test))]
    {
        let key = ConflictSnapshotPayloadCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            order: row.order,
        };
        let cache = static_bounded_cache(&CONFLICT_SNAPSHOT_PAYLOAD_CACHE, 1024);
        if let Some(payload) = cache.get(key) {
            return Ok(payload.into());
        }
        let payload = render_conflict_snapshot_payload(kv, catalog, row)?;
        let payload = std::sync::Arc::new(payload);
        cache.insert(key, payload.clone());
        Ok(payload.into())
    }
    #[cfg(test)]
    {
        render_conflict_snapshot_payload(kv, catalog, row).map(Into::into)
    }
}

fn render_conflict_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: SnapshotRow,
) -> CatalogResult<Vec<u8>> {
    let stage_started = RuntimeMetricStage::start();
    let (values, schema_version) = load_conflict_snapshot_values(kv, catalog)?;
    let watermarks = SnapshotWatermarks::from(values);
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_snapshot_payload.watermarks",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let schema_version =
        schema_version.map_or_else(|| snapshot_schema_version_at(kv, catalog, row.order), Ok)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_snapshot_payload.schema_version",
        stage_started,
    );
    let ducklake_snapshot_id = DuckLakeSnapshotId(row.sequence.0);
    Ok(format!(
        "catalog_snapshot_order={}\ncatalog_snapshot_sequence={}\nducklake_snapshot_id={}\nducklake_schema_version={}\nducklake_next_catalog_id={}\nducklake_next_file_id={}\n",
        row.order,
        ducklake_snapshot_id,
        ducklake_snapshot_id,
        schema_version,
        watermarks.next_catalog_id,
        watermarks.next_file_id
    )
    .into_bytes())
}

fn snapshot_schema_version_at(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    let stage_started = RuntimeMetricStage::start();
    load_schema_version_at(kv, catalog, order).inspect(|_| {
        record_runtime_stage(
            "method.runtime_catalog_snapshot.snapshot_schema_version_at",
            stage_started,
        );
    })
}

#[cfg(test)]
pub(crate) fn catalog_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    ducklake_snapshot_id: u64,
) -> CatalogResult<RuntimePayload> {
    catalog_snapshot_payload_with_kind(
        kv,
        catalog,
        DuckLakeSnapshotId(ducklake_snapshot_id),
        CatalogSnapshotIdKind::PublicSnapshot,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CatalogSnapshotIdKind {
    PublicSnapshot,
    DuckLakeSequence,
}

pub(crate) fn catalog_snapshot_payload_with_kind(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_id: DuckLakeSnapshotId,
    kind: CatalogSnapshotIdKind,
) -> CatalogResult<RuntimePayload> {
    let stage = crate::runtime_trace::RuntimeSpan::enter("method.catalog_payload.ResolveSnapshot");
    let mut request = CatalogSnapshotRequestContext::for_current_catalog(kv, catalog)?;
    let requested = request.resolve_snapshot(kv, catalog, snapshot_id, kind)?;
    stage.finish(true);
    #[cfg(not(test))]
    if crate::store::runtime_read_context_enabled() {
        let key = crate::runtime_read_context::catalog_snapshot_payload_identity(
            kv, catalog, &requested,
        )?;
        let cache = static_bounded_cache(&CATALOG_SNAPSHOT_PAYLOAD_CACHE, 1024);
        if let Some(payload) = cache.get(key) {
            crate::runtime_metrics::record_runtime_measurement(
                "cache.catalog_payload.version_hit",
                1,
                payload.len() as u64,
            );
            return Ok(payload.into());
        }
        let payload = render_requested_catalog(kv, catalog, &mut request, requested)?;
        let payload = std::sync::Arc::new(payload);
        cache.insert(key, payload.clone());
        return Ok(payload.into());
    }
    render_requested_catalog(kv, catalog, &mut request, requested).map(Into::into)
}

fn render_requested_catalog(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    request: &mut CatalogSnapshotRequestContext,
    requested: SnapshotRow,
) -> CatalogResult<Vec<u8>> {
    let stage = crate::runtime_trace::RuntimeSpan::enter("method.catalog_payload.BuildContext");
    let context = request.snapshot_context_for(kv, catalog, requested)?;
    stage.finish(true);
    let stage = crate::runtime_trace::RuntimeSpan::enter("method.catalog_payload.Render");
    let payload = render_catalog_snapshot_payload(context);
    stage.finish(true);
    Ok(payload)
}

fn render_catalog_snapshot_payload(context: CatalogSnapshotReadContext) -> Vec<u8> {
    let _snapshot_order = context.snapshot.order;
    let mut out = String::new();
    write!(out, "schema_count={}\n", context.schemas.len()).expect("writing to String cannot fail");
    for schema in context.schemas.iter() {
        write!(
            out,
            "schema\t{}\t{}\t{}\t{}\t{}\n",
            schema.schema_id.0,
            schema.uuid,
            schema.name,
            schema.path,
            schema
                .parent_schema_id
                .map(|id| id.0.to_string())
                .unwrap_or_default()
        )
        .expect("writing to String cannot fail");
    }
    write!(out, "table_count={}\n", context.tables.len()).expect("writing to String cannot fail");
    for table in context.tables.iter() {
        write!(
            out,
            "table\t{}\t{}\t{}\t{}\t{}\t{}\n",
            table.table_id.0,
            table.schema_id.0,
            table.uuid,
            table.name,
            table.path,
            table.comment.clone().unwrap_or_default()
        )
        .expect("writing to String cannot fail");
        for column in columns_parent_before_children(&table.columns) {
            write!(
                out,
                "column\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                table.table_id.0,
                column.column_id.0,
                column.name,
                column.column_type,
                column.nulls_allowed,
                column
                    .parent_id
                    .map_or(String::new(), |id| id.0.to_string()),
                column.comment.as_deref().unwrap_or_default(),
                column.initial_default.as_deref().unwrap_or_default(),
                column.default_value.as_deref().unwrap_or("NULL"),
                column.default_value_type
            )
            .expect("writing to String cannot fail");
        }
        for inlined_table in &table.inlined_data_tables {
            write!(
                out,
                "inlined_table\t{}\t{}\t{}\n",
                table.table_id.0, inlined_table.table_name, inlined_table.schema_version
            )
            .expect("writing to String cannot fail");
        }
        if let Some(partition) = &table.partition {
            for field in &partition.fields {
                write!(
                    out,
                    "partition\t{}\t{}\t{}\t{}\t{}\n",
                    table.table_id.0,
                    partition.partition_id,
                    field.partition_key_index,
                    field.column_id.0,
                    field.transform
                )
                .expect("writing to String cannot fail");
            }
        }
        if let Some(sort) = &table.sort {
            for field in &sort.fields {
                write!(
                    out,
                    "sort\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    table.table_id.0,
                    sort.sort_id,
                    field.sort_key_index,
                    field.expression,
                    field.dialect,
                    field.sort_direction,
                    field.null_order
                )
                .expect("writing to String cannot fail");
            }
        }
    }
    write!(out, "view_count={}\n", context.views.len()).expect("writing to String cannot fail");
    for view in context.views.iter() {
        write!(
            out,
            "view\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            view.view_id.0,
            view.schema_id.0,
            view.uuid,
            view.name,
            view.dialect,
            view.sql,
            view.column_aliases.join("\x1f"),
            view.comment.as_deref().unwrap_or_default()
        )
        .expect("writing to String cannot fail");
    }
    write!(out, "macro_count={}\n", context.macros.len()).expect("writing to String cannot fail");
    for macro_row in context.macros.iter() {
        write!(
            out,
            "macro\t{}\t{}\t{}\n",
            macro_row.macro_id.0, macro_row.schema_id.0, macro_row.name
        )
        .expect("writing to String cannot fail");
        for (impl_id, implementation) in macro_row.implementations.iter().enumerate() {
            write!(
                out,
                "macro_impl\t{}\t{}\t{}\t{}\t{}\n",
                macro_row.macro_id.0,
                impl_id,
                implementation.dialect,
                implementation.sql,
                implementation.macro_type
            )
            .expect("writing to String cannot fail");
            for (param_id, parameter) in implementation.parameters.iter().enumerate() {
                write!(
                    out,
                    "macro_param\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    macro_row.macro_id.0,
                    impl_id,
                    param_id,
                    parameter.parameter_name,
                    duckdb_type_name(&parameter.parameter_type),
                    parameter.default_value,
                    duckdb_type_name(&parameter.default_value_type)
                )
                .expect("writing to String cannot fail");
            }
        }
    }
    out.into_bytes()
}

fn columns_parent_before_children(
    columns: &[crate::TableColumnRow],
) -> Vec<&crate::TableColumnRow> {
    let mut children = BTreeMap::<crate::ColumnId, Vec<usize>>::new();
    let mut ready = BTreeSet::new();
    let mut remaining = (0..columns.len()).collect::<BTreeSet<_>>();
    for (index, column) in columns.iter().enumerate() {
        if let Some(parent_id) = column.parent_id {
            children.entry(parent_id).or_default().push(index);
        } else {
            ready.insert(index);
        }
    }

    let mut ordered = Vec::with_capacity(columns.len());
    while let Some(index) = ready.pop_first().or_else(|| remaining.first().copied()) {
        if !remaining.remove(&index) {
            continue;
        }
        let column = &columns[index];
        ordered.push(column);
        if let Some(child_indexes) = children.get(&column.column_id) {
            ready.extend(
                child_indexes
                    .iter()
                    .copied()
                    .filter(|child| remaining.contains(child)),
            );
        }
    }
    ordered
}

fn duckdb_type_name(type_name: &str) -> &str {
    match type_name {
        "float32" => "float",
        "float64" => "double",
        _ => type_name,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotWatermarks {
    pub(crate) next_catalog_id: u64,
    pub(crate) next_file_id: u64,
}

pub(crate) fn snapshot_watermarks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    _order: CatalogOrderId,
) -> CatalogResult<SnapshotWatermarks> {
    load_conflict_watermarks(kv, catalog).map(SnapshotWatermarks::from)
}

impl From<ConflictWatermarkValues> for SnapshotWatermarks {
    fn from(watermarks: ConflictWatermarkValues) -> Self {
        Self {
            next_catalog_id: watermarks
                .max_catalog_id
                .map_or(0, |id| id.saturating_add(1)),
            next_file_id: watermarks.max_file_id.map_or(0, |id| id.saturating_add(1)),
        }
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_stage(_operation: &str, _started: RuntimeMetricStage) {}

#[cfg(test)]
#[path = "runtime_catalog_snapshot_tests.rs"]
mod runtime_catalog_snapshot_tests;
