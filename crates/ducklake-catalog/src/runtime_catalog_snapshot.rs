use std::collections::BTreeMap;
#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use crate::CatalogCacheNamespace;
#[cfg(not(test))]
use crate::bounded_cache::{BoundedCache, static_bounded_cache};
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogId, CatalogOrderId, CatalogResult, DataFileRow, DeleteFileRow, DuckLakeSnapshotId,
    OrderedCatalogKv, RangeDirection, SnapshotRow,
    conflict_watermarks::load_conflict_watermarks,
    inline_data::decode_inline_table_item,
    keys::{KeyFamily, family_prefix},
    latest_snapshot,
    macro_store::list_macro_rows,
    public_snapshot_sequence_for_order,
    runtime_read_context::{
        CatalogInlineDeletionReadContext, CatalogSnapshotReadContext, CatalogSnapshotRequestContext,
    },
    runtime_snapshot_range::{SnapshotDataChangeOrder, SnapshotWatermarkCutoffOrder},
    runtime_snapshots::{
        public_snapshot_sequences_by_order_shared, snapshot_schema_versions_by_order_shared,
    },
    schema_store::list_schema_rows,
    schema_version_state::load_current_schema_version,
    table_store::list_table_rows,
    view_store::list_view_rows,
};

#[cfg(not(test))]
static CATALOG_SNAPSHOT_PAYLOAD_CACHE: OnceLock<
    BoundedCache<CatalogSnapshotPayloadCacheKey, Vec<u8>>,
> = OnceLock::new();

#[cfg(not(test))]
static CONFLICT_SNAPSHOT_PAYLOAD_CACHE: OnceLock<
    BoundedCache<ConflictSnapshotPayloadCacheKey, Vec<u8>>,
> = OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CatalogSnapshotPayloadCacheKey {
    namespace: CatalogCacheNamespace,
    catalog: CatalogId,
    order: CatalogOrderId,
    kind: CatalogSnapshotIdKind,
}

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
) -> CatalogResult<Vec<u8>> {
    let Some(row) = latest_snapshot(kv, catalog)? else {
        return Ok(b"catalog_snapshot_exists=false\n".to_vec());
    };
    conflict_snapshot_payload_for_row(kv, catalog, row)
}

pub(crate) fn conflict_snapshot_payload_for_row(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: SnapshotRow,
) -> CatalogResult<Vec<u8>> {
    #[cfg(not(test))]
    {
        let key = ConflictSnapshotPayloadCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            order: row.order,
        };
        let cache = static_bounded_cache(&CONFLICT_SNAPSHOT_PAYLOAD_CACHE, 1024);
        if let Some(payload) = cache.get(key) {
            return Ok(payload);
        }
        let payload = render_conflict_snapshot_payload(kv, catalog, row)?;
        cache.insert(key, payload.clone());
        Ok(payload)
    }
    #[cfg(test)]
    {
        render_conflict_snapshot_payload(kv, catalog, row)
    }
}

fn render_conflict_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    row: SnapshotRow,
) -> CatalogResult<Vec<u8>> {
    let stage_started = RuntimeMetricStage::start();
    let watermarks = conflict_snapshot_watermarks(kv, catalog, row.order)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_snapshot_payload.watermarks",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let schema_version = current_conflict_snapshot_schema_version(kv, catalog, row.order)?;
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
    Ok(snapshot_schema_versions_by_order_shared(kv, catalog)?
        .get(&order)
        .copied()
        .unwrap_or(0))
    .inspect(|_| {
        record_runtime_stage(
            "method.runtime_catalog_snapshot.snapshot_schema_version_at",
            stage_started,
        );
    })
}

fn current_conflict_snapshot_schema_version(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<u64> {
    let started = RuntimeMetricStage::start();
    if let Some(version) = load_current_schema_version(kv, catalog)? {
        record_runtime_stage(
            "method.runtime_catalog_snapshot.current_conflict_schema_version.hit",
            started,
        );
        return Ok(version);
    }
    record_runtime_stage(
        "method.runtime_catalog_snapshot.current_conflict_schema_version.miss",
        started,
    );
    snapshot_schema_version_at(kv, catalog, order)
}

#[cfg(test)]
pub(crate) fn catalog_snapshot_payload(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    ducklake_snapshot_id: u64,
) -> CatalogResult<Vec<u8>> {
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
) -> CatalogResult<Vec<u8>> {
    let mut request = CatalogSnapshotRequestContext::for_current_catalog(kv, catalog)?;
    let requested = request.resolve_snapshot(kv, catalog, snapshot_id, kind)?;
    #[cfg(not(test))]
    {
        let key = CatalogSnapshotPayloadCacheKey {
            namespace: kv.catalog_cache_namespace(),
            catalog,
            order: requested.order,
            kind,
        };
        let cache = static_bounded_cache(&CATALOG_SNAPSHOT_PAYLOAD_CACHE, 1024);
        if let Some(payload) = cache.get(key) {
            return Ok(payload);
        }
        let catalog_context = request.snapshot_context_for(kv, catalog, requested)?;
        let payload = render_catalog_snapshot_payload(catalog_context);
        cache.insert(key, payload.clone());
        Ok(payload)
    }
    #[cfg(test)]
    {
        let catalog_context = request.snapshot_context_for(kv, catalog, requested)?;
        Ok(render_catalog_snapshot_payload(catalog_context))
    }
}

fn render_catalog_snapshot_payload(context: CatalogSnapshotReadContext) -> Vec<u8> {
    let _snapshot_order = context.snapshot.order;
    let mut out = String::new();
    out.push_str(&format!("schema_count={}\n", context.schemas.len()));
    for schema in context.schemas {
        out.push_str(&format!(
            "schema\t{}\t{}\t{}\t{}\n",
            schema.schema_id.0, schema.uuid, schema.name, schema.path
        ));
    }
    out.push_str(&format!("table_count={}\n", context.tables.len()));
    for table in context.tables {
        out.push_str(&format!(
            "table\t{}\t{}\t{}\t{}\t{}\t{}\n",
            table.table_id.0,
            table.schema_id.0,
            table.uuid,
            table.name,
            table.path,
            table.comment.clone().unwrap_or_default()
        ));
        for column in columns_parent_before_children(&table.columns) {
            out.push_str(&format!(
                "column\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                table.table_id.0,
                column.column_id.0,
                column.name,
                column.column_type,
                column.nulls_allowed,
                column
                    .parent_id
                    .map_or(String::new(), |id| id.0.to_string()),
                column.comment.unwrap_or_default(),
                column.initial_default.unwrap_or_default(),
                column.default_value.unwrap_or_else(|| "NULL".to_owned()),
                column.default_value_type
            ));
        }
        for inlined_table in table.inlined_data_tables {
            out.push_str(&format!(
                "inlined_table\t{}\t{}\t{}\n",
                table.table_id.0, inlined_table.table_name, inlined_table.schema_version
            ));
        }
        if let Some(partition) = table.partition {
            for field in partition.fields {
                out.push_str(&format!(
                    "partition\t{}\t{}\t{}\t{}\t{}\n",
                    table.table_id.0,
                    partition.partition_id,
                    field.partition_key_index,
                    field.column_id.0,
                    field.transform
                ));
            }
        }
        if let Some(sort) = table.sort {
            for field in sort.fields {
                out.push_str(&format!(
                    "sort\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    table.table_id.0,
                    sort.sort_id,
                    field.sort_key_index,
                    field.expression,
                    field.dialect,
                    field.sort_direction,
                    field.null_order
                ));
            }
        }
    }
    out.push_str(&format!("view_count={}\n", context.views.len()));
    for view in context.views {
        out.push_str(&format!(
            "view\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            view.view_id.0,
            view.schema_id.0,
            view.uuid,
            view.name,
            view.dialect,
            view.sql,
            view.column_aliases.join("\x1f"),
            view.comment.unwrap_or_default()
        ));
    }
    out.push_str(&format!("macro_count={}\n", context.macros.len()));
    for macro_row in context.macros {
        out.push_str(&format!(
            "macro\t{}\t{}\t{}\n",
            macro_row.macro_id.0, macro_row.schema_id.0, macro_row.name
        ));
        for (impl_id, implementation) in macro_row.implementations.iter().enumerate() {
            out.push_str(&format!(
                "macro_impl\t{}\t{}\t{}\t{}\t{}\n",
                macro_row.macro_id.0,
                impl_id,
                implementation.dialect,
                implementation.sql,
                implementation.macro_type
            ));
            for (param_id, parameter) in implementation.parameters.iter().enumerate() {
                out.push_str(&format!(
                    "macro_param\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    macro_row.macro_id.0,
                    impl_id,
                    param_id,
                    parameter.parameter_name,
                    duckdb_type_name(&parameter.parameter_type),
                    parameter.default_value,
                    duckdb_type_name(&parameter.default_value_type)
                ));
            }
        }
    }
    out.into_bytes()
}

fn columns_parent_before_children(columns: &[crate::TableColumnRow]) -> Vec<crate::TableColumnRow> {
    let mut remaining = columns.to_vec();
    let mut ordered = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let next_index = remaining
            .iter()
            .position(|column| {
                column.parent_id.is_none()
                    || ordered.iter().any(|parent: &crate::TableColumnRow| {
                        Some(parent.column_id) == column.parent_id
                    })
            })
            .unwrap_or(0);
        ordered.push(remaining.remove(next_index));
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

pub(crate) fn conflict_snapshot_watermarks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<SnapshotWatermarks> {
    let watermarks = load_conflict_watermarks(kv, catalog)?;
    if let (Some(max_file_id), Some(max_catalog_id)) =
        (watermarks.max_file_id, watermarks.max_catalog_id)
    {
        return Ok(SnapshotWatermarks {
            next_catalog_id: max_catalog_id.saturating_add(1),
            next_file_id: max_file_id.saturating_add(1),
        });
    }
    let stage_started = RuntimeMetricStage::start();
    let max_file_id = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::DataFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DataFileRow::decode(&item.value).map(|row| row.data_file_id.0))
        .try_fold(None, max_decoded_id)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_watermarks.data_files",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let max_delete_file_id = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::DeleteFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DeleteFileRow::decode(&item.value).map(|row| row.delete_file_id.0))
        .try_fold(None, max_decoded_id)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_watermarks.delete_files",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let inline_watermark = raw_inline_data_change_watermark(kv, catalog, order)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_watermarks.inline_watermark",
        stage_started,
    );
    let next_file_id = max_file_id
        .into_iter()
        .chain(max_delete_file_id)
        .chain(inline_watermark)
        .max()
        .map_or(0, |id| id.saturating_add(1));
    let stage_started = RuntimeMetricStage::start();
    let next_catalog_id = next_historical_catalog_id(kv, catalog)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.conflict_watermarks.next_catalog_id",
        stage_started,
    );
    Ok(SnapshotWatermarks {
        next_catalog_id,
        next_file_id,
    })
}

pub(crate) fn snapshot_watermarks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<SnapshotWatermarks> {
    let max_file_id = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::DataFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DataFileRow::decode(&item.value).map(|row| row.data_file_id.0))
        .try_fold(None, max_decoded_id)?;
    let max_delete_file_id = kv
        .scan_prefix(
            &family_prefix(catalog, KeyFamily::DeleteFile),
            RangeDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|item| DeleteFileRow::decode(&item.value).map(|row| row.delete_file_id.0))
        .try_fold(None, max_decoded_id)?;
    let next_file_id = max_file_id
        .into_iter()
        .chain(max_delete_file_id)
        .chain(inline_data_change_watermark(kv, catalog, order)?)
        .max()
        .map_or(0, |id| id.saturating_add(1));
    Ok(SnapshotWatermarks {
        next_catalog_id: next_historical_catalog_id(kv, catalog)?,
        next_file_id,
    })
}

fn max_decoded_id(current: Option<u64>, decoded: CatalogResult<u64>) -> CatalogResult<Option<u64>> {
    let decoded = decoded?;
    Ok(Some(current.map_or(decoded, |value| value.max(decoded))))
}

fn next_historical_catalog_id(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<u64> {
    let stage_started = RuntimeMetricStage::start();
    let max_schema_id = list_schema_rows(kv, catalog)?
        .into_iter()
        .map(|schema| schema.schema_id.0)
        .max();
    record_runtime_stage(
        "method.runtime_catalog_snapshot.next_historical_catalog_id.schemas",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let max_table_id = list_table_rows(kv, catalog)?
        .into_iter()
        .map(|table| table.table_id.0)
        .max();
    record_runtime_stage(
        "method.runtime_catalog_snapshot.next_historical_catalog_id.tables",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let max_view_id = list_view_rows(kv, catalog)?
        .into_iter()
        .map(|view| view.view_id.0)
        .max();
    record_runtime_stage(
        "method.runtime_catalog_snapshot.next_historical_catalog_id.views",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    let max_macro_id = list_macro_rows(kv, catalog)?
        .into_iter()
        .map(|macro_row| macro_row.macro_id.0)
        .max();
    record_runtime_stage(
        "method.runtime_catalog_snapshot.next_historical_catalog_id.macros",
        stage_started,
    );
    Ok(max_schema_id
        .into_iter()
        .chain(max_table_id)
        .chain(max_view_id)
        .chain(max_macro_id)
        .max()
        .map_or(1, |id| id.saturating_add(1)))
}

fn raw_inline_data_change_watermark(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Option<u64>> {
    let stage_started = RuntimeMetricStage::start();
    let sequence_by_order = public_snapshot_sequences_by_order_shared(kv, catalog)?;
    record_runtime_stage(
        "method.runtime_catalog_snapshot.raw_inline_watermark.snapshots",
        stage_started,
    );
    let mut watermark = None;
    let stage_started = RuntimeMetricStage::start();
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineTable),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        update_raw_data_change_watermark(
            sequence_by_order.as_ref(),
            SnapshotWatermarkCutoffOrder::new(snapshot_order),
            SnapshotDataChangeOrder::new(row.validity.begin_order),
            &mut watermark,
        );
        if let Some(end_order) = row.validity.end_order {
            update_raw_data_change_watermark(
                sequence_by_order.as_ref(),
                SnapshotWatermarkCutoffOrder::new(snapshot_order),
                SnapshotDataChangeOrder::new(end_order),
                &mut watermark,
            );
        }
    }
    record_runtime_stage(
        "method.runtime_catalog_snapshot.raw_inline_watermark.inline_tables",
        stage_started,
    );
    let stage_started = RuntimeMetricStage::start();
    for row in CatalogInlineDeletionReadContext::for_catalog(kv, catalog)?.rows() {
        update_raw_data_change_watermark(
            sequence_by_order.as_ref(),
            SnapshotWatermarkCutoffOrder::new(snapshot_order),
            SnapshotDataChangeOrder::new(row.validity.begin_order),
            &mut watermark,
        );
    }
    record_runtime_stage(
        "method.runtime_catalog_snapshot.raw_inline_watermark.inline_deletions",
        stage_started,
    );
    Ok(watermark)
}

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
struct RuntimeMetricStage(std::time::Instant);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(
        operation,
        u64::try_from(started.0.elapsed().as_micros()).unwrap_or(u64::MAX),
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_stage(_operation: &str, _started: RuntimeMetricStage) {}

fn update_raw_data_change_watermark(
    sequence_by_order: &BTreeMap<CatalogOrderId, u64>,
    snapshot_order: SnapshotWatermarkCutoffOrder,
    change_order: SnapshotDataChangeOrder,
    watermark: &mut Option<u64>,
) {
    let snapshot_order = snapshot_order.catalog_order();
    let change_order = change_order.catalog_order();
    if change_order > snapshot_order {
        return;
    }
    let Some(sequence) = sequence_by_order.get(&change_order) else {
        return;
    };
    *watermark = Some(watermark.map_or(*sequence, |value| value.max(*sequence)));
}

fn inline_data_change_watermark(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    order: CatalogOrderId,
) -> CatalogResult<Option<u64>> {
    let mut watermark = None;
    for item in kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::InlineTable),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        update_data_change_watermark(
            kv,
            catalog,
            SnapshotWatermarkCutoffOrder::new(order),
            SnapshotDataChangeOrder::new(row.validity.begin_order),
            &mut watermark,
        )?;
        if let Some(end_order) = row.validity.end_order {
            update_data_change_watermark(
                kv,
                catalog,
                SnapshotWatermarkCutoffOrder::new(order),
                SnapshotDataChangeOrder::new(end_order),
                &mut watermark,
            )?;
        }
    }
    for row in CatalogInlineDeletionReadContext::for_catalog(kv, catalog)?.rows() {
        update_data_change_watermark(
            kv,
            catalog,
            SnapshotWatermarkCutoffOrder::new(order),
            SnapshotDataChangeOrder::new(row.validity.begin_order),
            &mut watermark,
        )?;
    }
    Ok(watermark)
}

fn update_data_change_watermark(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    snapshot_order: SnapshotWatermarkCutoffOrder,
    change_order: SnapshotDataChangeOrder,
    watermark: &mut Option<u64>,
) -> CatalogResult<()> {
    let snapshot_order = snapshot_order.catalog_order();
    let change_order = change_order.catalog_order();
    if change_order > snapshot_order {
        return Ok(());
    }
    let Some(public_sequence) = public_snapshot_sequence_for_order(kv, catalog, change_order)?
    else {
        return Ok(());
    };
    *watermark = Some(watermark.map_or(public_sequence.0, |value| value.max(public_sequence.0)));
    Ok(())
}

#[cfg(test)]
#[path = "runtime_catalog_snapshot_tests.rs"]
mod runtime_catalog_snapshot_tests;
