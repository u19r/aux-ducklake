#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_method_elapsed;
use crate::{
    CatalogError, CatalogId, CatalogOrderId, CatalogOrderKind, CatalogResult, DataFileId,
    DataFileRow, KvBatch, OrderedCatalogKv, RangeDirection, SchemaId, TableId,
    inline_data::{
        InlineTableChunkRow, assemble_inline_payload, decode_inline_table_item,
        inline_table_end_orders_at, inline_table_prefix,
    },
    keys::{
        data_file_begin_prefix, data_file_begin_scan_end, data_file_key, inline_table_change_key,
        table_inline_row_change_key, table_inline_row_change_prefix,
        table_inline_row_change_scan_end, table_inline_row_change_scan_start,
        table_schema_kind_inline_row_change_key, table_schema_kind_inline_row_change_prefix,
        table_schema_kind_inline_row_change_scan_end,
        table_schema_kind_inline_row_change_scan_start,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineRowChangeKind {
    Inserted,
    Deleted,
}

impl InlineRowChangeKind {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Inserted => b'i',
            Self::Deleted => b'd',
        }
    }

    pub fn from_code(code: u8) -> CatalogResult<Self> {
        match code {
            b'i' => Ok(Self::Inserted),
            b'd' => Ok(Self::Deleted),
            _ => Err(CatalogError::InvalidKey(format!(
                "unknown inline row change kind 0x{code:02x}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineRowChange {
    pub table_id: TableId,
    pub schema_id: SchemaId,
    pub order: CatalogOrderId,
    pub kind: InlineRowChangeKind,
    pub row_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineRowPayloadChange {
    pub change: InlineRowChange,
    pub payload: Vec<u8>,
}

pub(crate) fn stage_inline_row_changes_for_payload(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
    payload: &[u8],
) -> CatalogResult<()> {
    let row_ids = optional_inline_payload_row_ids(payload)?;
    if !row_ids.is_empty() {
        stage_inline_table_change(batch, catalog, table_id, order, kind);
    }
    for row_id in row_ids {
        stage_inline_row_change_key(batch, catalog, table_id, schema_id, order, kind, row_id);
    }
    Ok(())
}

fn optional_inline_payload_row_ids(payload: &[u8]) -> CatalogResult<Vec<u64>> {
    match inline_payload_row_ids(payload) {
        Ok(row_ids) => Ok(row_ids),
        Err(CatalogError::Decode(message))
            if message.starts_with("inline payload is not utf-8")
                || message.starts_with("inline payload row has invalid shape") =>
        {
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn stage_inline_row_change(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
    row_id: u64,
) {
    stage_inline_table_change(batch, catalog, table_id, order, kind);
    stage_inline_row_change_key(batch, catalog, table_id, schema_id, order, kind, row_id);
}

fn stage_inline_row_change_key(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
    row_id: u64,
) {
    batch.put(
        table_inline_row_change_key(catalog, table_id, order, kind, schema_id, row_id),
        Vec::new(),
    );
    if kind == InlineRowChangeKind::Deleted {
        batch.put(
            table_schema_kind_inline_row_change_key(
                catalog, table_id, schema_id, kind, order, row_id,
            ),
            Vec::new(),
        );
    }
}

pub(crate) fn stage_inline_table_change(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    kind: InlineRowChangeKind,
) {
    batch.put(
        inline_table_change_key(catalog, order, kind, table_id),
        Vec::new(),
    );
}

pub fn list_inline_row_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineRowChange>> {
    let started = RuntimeMetricStage::start();
    if end_order < start_order {
        return Err(CatalogError::InvalidMutation(
            "inline change-feed end order cannot precede start order".to_owned(),
        ));
    }
    let prefix = table_inline_row_change_prefix(catalog, table_id);
    let order_kind = if start_order.kind() == end_order.kind() {
        end_order.kind()
    } else {
        CatalogOrderKind::UuidV7
    };
    let mut changes = Vec::new();
    for item in kv.scan_range(
        &table_inline_row_change_scan_start(catalog, table_id, start_order),
        &table_inline_row_change_scan_end(catalog, table_id, end_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        changes.push(decode_inline_row_change_key(
            table_id, &prefix, &item.key, order_kind,
        )?);
    }
    record_runtime_method_stage("method.inline_change_feed.list_inline_row_changes", started);
    Ok(changes)
}

pub(crate) fn list_inline_deleted_row_changes_for_schema(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> CatalogResult<Vec<InlineRowChange>> {
    let started = RuntimeMetricStage::start();
    if end_order < start_order {
        return Err(CatalogError::InvalidMutation(
            "inline change-feed end order cannot precede start order".to_owned(),
        ));
    }
    let kind = InlineRowChangeKind::Deleted;
    let prefix = table_schema_kind_inline_row_change_prefix(catalog, table_id, schema_id, kind);
    let order_kind = if start_order.kind() == end_order.kind() {
        end_order.kind()
    } else {
        CatalogOrderKind::UuidV7
    };
    let mut changes = Vec::new();
    for item in kv.scan_range(
        &table_schema_kind_inline_row_change_scan_start(
            catalog,
            table_id,
            schema_id,
            kind,
            start_order,
        ),
        &table_schema_kind_inline_row_change_scan_end(
            catalog, table_id, schema_id, kind, end_order,
        ),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        changes.push(decode_schema_kind_inline_row_change_key(
            table_id, schema_id, kind, &prefix, &item.key, order_kind,
        )?);
    }
    record_runtime_method_stage(
        "method.inline_change_feed.list_inline_deleted_row_changes_for_schema",
        started,
    );
    Ok(changes)
}

pub fn list_inline_row_payload_changes(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
    kind: InlineRowChangeKind,
) -> CatalogResult<Vec<InlineRowPayloadChange>> {
    let started = RuntimeMetricStage::start();
    let mut rows = Vec::new();
    let mut context =
        InlinePayloadChangeContext::load(kv, catalog, table_id, schema_id, end_order, kind)?;
    for change in list_inline_row_changes(kv, catalog, table_id, start_order, end_order)? {
        if change.schema_id != schema_id || change.kind != kind {
            continue;
        }
        let Some(payload) = context.payload_at_change(change.row_id, change.order)? else {
            if kind == InlineRowChangeKind::Inserted {
                continue;
            }
            return Err(CatalogError::Decode(format!(
                "inline row {} is missing at change order {}",
                change.row_id, change.order
            )));
        };
        rows.push(InlineRowPayloadChange { change, payload });
    }
    record_runtime_method_stage(
        "method.inline_change_feed.list_inline_row_payload_changes",
        started,
    );
    Ok(rows)
}

struct InlinePayloadChangeContext {
    kind: InlineRowChangeKind,
    insertion_files: Vec<DataFileRow>,
    payloads: Vec<InlinePayloadChunks>,
}

struct InlinePayloadChunks {
    begin_order: CatalogOrderId,
    end_order: Option<CatalogOrderId>,
    chunks: Vec<InlineTableChunkRow>,
}

impl InlinePayloadChangeContext {
    fn load(
        kv: &impl OrderedCatalogKv,
        catalog: CatalogId,
        table_id: TableId,
        schema_id: SchemaId,
        feed_end_order: CatalogOrderId,
        kind: InlineRowChangeKind,
    ) -> CatalogResult<Self> {
        let started = RuntimeMetricStage::start();
        let insertion_files = if kind == InlineRowChangeKind::Inserted {
            insertion_data_files_through(kv, catalog, table_id, feed_end_order)?
        } else {
            Vec::new()
        };
        record_runtime_method_stage(
            "method.inline_change_feed.payload_context.insertion_files",
            started,
        );
        let started = RuntimeMetricStage::start();
        let end_orders =
            inline_table_end_orders_at(kv, catalog, table_id, schema_id, feed_end_order)?;
        record_runtime_method_stage(
            "method.inline_change_feed.payload_context.end_orders",
            started,
        );
        let started = RuntimeMetricStage::start();
        let payloads = load_inline_payload_chunks(
            kv,
            catalog,
            table_id,
            schema_id,
            feed_end_order,
            &end_orders,
        )?;
        record_runtime_method_stage(
            "method.inline_change_feed.payload_context.inline_payloads",
            started,
        );
        Ok(Self {
            kind,
            insertion_files,
            payloads,
        })
    }

    fn payload_at_change(
        &mut self,
        row_id: u64,
        change_order: CatalogOrderId,
    ) -> CatalogResult<Option<Vec<u8>>> {
        if self.kind == InlineRowChangeKind::Inserted
            && self
                .insertion_files
                .iter()
                .any(|file| data_file_covers_inline_begin(file, change_order))
        {
            return Ok(None);
        }
        for payload in self.payloads.iter().rev() {
            let visible = match self.kind {
                InlineRowChangeKind::Inserted => {
                    payload.begin_order <= change_order
                        && payload
                            .end_order
                            .is_none_or(|end_order| end_order >= change_order)
                }
                InlineRowChangeKind::Deleted => {
                    payload.begin_order < change_order
                        && payload
                            .end_order
                            .is_none_or(|end_order| change_order <= end_order)
                }
            };
            if !visible {
                continue;
            }
            let assembled = assemble_inline_payload(payload.chunks.clone())?;
            if let Some(row) = inline_payload_row(&assembled, row_id)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

fn load_inline_payload_chunks(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    schema_id: SchemaId,
    feed_end_order: CatalogOrderId,
    end_orders: &std::collections::BTreeMap<CatalogOrderId, CatalogOrderId>,
) -> CatalogResult<Vec<InlinePayloadChunks>> {
    let mut payloads_by_begin =
        std::collections::BTreeMap::<CatalogOrderId, Vec<InlineTableChunkRow>>::new();
    let mut end_order_by_begin = std::collections::BTreeMap::new();
    for item in kv.scan_prefix(
        &inline_table_prefix(catalog, table_id, schema_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = decode_inline_table_item(catalog, &item.key, &item.value)?;
        let end_order = row
            .validity
            .end_order
            .or_else(|| end_orders.get(&row.validity.begin_order).copied());
        if row.validity.begin_order <= feed_end_order {
            end_order_by_begin.insert(row.validity.begin_order, end_order);
            payloads_by_begin
                .entry(row.validity.begin_order)
                .or_default()
                .push(row);
        }
    }
    Ok(payloads_by_begin
        .into_iter()
        .map(|(begin_order, chunks)| InlinePayloadChunks {
            begin_order,
            end_order: end_order_by_begin.remove(&begin_order).flatten(),
            chunks,
        })
        .collect())
}

pub(crate) fn insertion_data_files_through(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = std::collections::BTreeMap::new();
    for item in kv.scan_range(
        &data_file_begin_prefix(catalog, table_id),
        &data_file_begin_scan_end(catalog, table_id, snapshot_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_begin_index_item(kv, catalog, table_id, &item.key, &item.value)?;
        rows.insert(row.data_file_id, row);
    }
    for row in list_table_partial_data_files(kv, catalog, table_id)? {
        if data_file_covers_snapshot_order(&row, snapshot_order) {
            rows.insert(row.data_file_id, row);
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|file| (file.validity.begin_order, file.data_file_id));
    Ok(without_compacted_sources(rows))
}

fn list_table_partial_data_files(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
) -> CatalogResult<Vec<DataFileRow>> {
    let mut rows = Vec::new();
    for item in kv.scan_prefix(
        &data_file_begin_prefix(catalog, table_id),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        let row = data_file_from_begin_index_item(kv, catalog, table_id, &item.key, &item.value)?;
        if row.max_partial_order.is_some() {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn data_file_from_begin_index_item(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    value: &[u8],
) -> CatalogResult<DataFileRow> {
    match DataFileRow::decode(value) {
        Ok(mut row) => {
            row.validity.begin_order = data_file_begin_order_from_key(
                catalog,
                table_id,
                key,
                row.validity.begin_order.kind(),
            )?;
            Ok(row)
        }
        Err(_) => {
            let data_file_id = decode_data_file_id(value)?;
            let Some(value) = kv.get(&data_file_key(catalog, data_file_id))? else {
                return Err(CatalogError::NotFound("data file"));
            };
            DataFileRow::decode(&value)
        }
    }
}

fn data_file_begin_order_from_key(
    catalog: CatalogId,
    table_id: TableId,
    key: &[u8],
    kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = data_file_begin_prefix(catalog, table_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "data file begin key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "data file begin key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] = tail[..CatalogOrderId::LEN]
        .try_into()
        .map_err(|_| CatalogError::InvalidKey("data file begin order is truncated".to_owned()))?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

pub(crate) fn data_file_covers_inline_begin(
    file: &DataFileRow,
    inline_begin_order: CatalogOrderId,
) -> bool {
    data_file_covers_snapshot_order(file, inline_begin_order)
}

fn data_file_covers_snapshot_order(file: &DataFileRow, snapshot_order: CatalogOrderId) -> bool {
    file.max_partial_order.is_some_and(|max_partial_order| {
        let span_start = file.validity.begin_order.min(max_partial_order);
        let span_end = file.validity.begin_order.max(max_partial_order);
        span_start <= snapshot_order && snapshot_order <= span_end
    })
}

fn decode_data_file_id(bytes: &[u8]) -> CatalogResult<DataFileId> {
    if bytes.len() != 8 {
        return Err(CatalogError::Decode(format!(
            "data file id pointer must be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(DataFileId(u64::from_be_bytes(bytes.try_into().map_err(
        |_| CatalogError::Decode("data file id pointer is truncated".to_owned()),
    )?)))
}

fn without_compacted_sources(rows: Vec<DataFileRow>) -> Vec<DataFileRow> {
    let compacted_ranges = rows
        .iter()
        .filter_map(|file| {
            if file.validity.end_order.is_some() {
                return None;
            }
            file.max_partial_order
                .map(|max_order| (file.data_file_id, file.validity.begin_order, max_order))
        })
        .collect::<Vec<_>>();
    rows.into_iter()
        .filter(|file| {
            !compacted_ranges
                .iter()
                .any(|(replacement_id, begin_order, max_order)| {
                    *replacement_id != file.data_file_id
                        && *begin_order <= file.validity.begin_order
                        && file.validity.begin_order <= *max_order
                })
        })
        .collect()
}

fn inline_payload_row_ids(payload: &[u8]) -> CatalogResult<Vec<u64>> {
    let text = inline_payload_text(payload)?;
    text.lines().map(inline_row_id).collect()
}

fn inline_payload_row(payload: &[u8], wanted_row_id: u64) -> CatalogResult<Option<Vec<u8>>> {
    let text = inline_payload_text(payload)?;
    for line in text.lines() {
        if inline_row_id(line)? == wanted_row_id {
            let mut row = line.as_bytes().to_vec();
            row.push(b'\n');
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn inline_payload_text(payload: &[u8]) -> CatalogResult<&str> {
    std::str::from_utf8(payload)
        .map_err(|error| CatalogError::Decode(format!("inline payload is not utf-8: {error}")))
}

pub(crate) fn inline_row_id(line: &str) -> CatalogResult<u64> {
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["row", row_id, ..] => row_id.parse::<u64>().map_err(|error| {
            CatalogError::Decode(format!("inline row id {row_id} is invalid: {error}"))
        }),
        _ => Err(CatalogError::Decode(format!(
            "inline payload row has invalid shape: {line}"
        ))),
    }
}

fn decode_inline_row_change_key(
    table_id: TableId,
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<InlineRowChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(CatalogError::InvalidKey(
            "inline row change key has wrong prefix".to_owned(),
        ));
    };
    let expected_len = CatalogOrderId::LEN + 1 + 1 + 1 + 8 + 1 + 8;
    if tail.len() != expected_len {
        return Err(CatalogError::InvalidKey(format!(
            "inline row change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    let order_end = CatalogOrderId::LEN;
    if tail[order_end] != b'/' || tail[order_end + 2] != b'/' || tail[order_end + 11] != b'/' {
        return Err(CatalogError::InvalidKey(
            "inline row change key separator is invalid".to_owned(),
        ));
    }
    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end].try_into().map_err(|_| {
            CatalogError::InvalidKey("inline row change order is truncated".to_owned())
        })?,
    );
    let kind = InlineRowChangeKind::from_code(tail[order_end + 1])?;
    let schema_start = order_end + 3;
    let row_id_start = schema_start + 9;
    let schema_id = SchemaId(u64::from_be_bytes(
        tail[schema_start..schema_start + 8]
            .try_into()
            .map_err(|_| {
                CatalogError::InvalidKey("inline row change schema id is truncated".to_owned())
            })?,
    ));
    let row_id = u64::from_be_bytes(tail[row_id_start..].try_into().map_err(|_| {
        CatalogError::InvalidKey("inline row change row id is truncated".to_owned())
    })?);
    Ok(InlineRowChange {
        table_id,
        schema_id,
        order,
        kind,
        row_id,
    })
}

fn decode_schema_kind_inline_row_change_key(
    table_id: TableId,
    schema_id: SchemaId,
    kind: InlineRowChangeKind,
    prefix: &[u8],
    key: &[u8],
    order_kind: CatalogOrderKind,
) -> CatalogResult<InlineRowChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(CatalogError::InvalidKey(
            "schema-kind inline row change key has wrong prefix".to_owned(),
        ));
    };
    let expected_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != expected_len {
        return Err(CatalogError::InvalidKey(format!(
            "schema-kind inline row change key tail must be {expected_len} bytes, got {}",
            tail.len()
        )));
    }
    let order_end = CatalogOrderId::LEN;
    if tail[order_end] != b'/' {
        return Err(CatalogError::InvalidKey(
            "schema-kind inline row change key separator is invalid".to_owned(),
        ));
    }
    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end].try_into().map_err(|_| {
            CatalogError::InvalidKey("schema-kind inline row change order is truncated".to_owned())
        })?,
    );
    let row_id = u64::from_be_bytes(tail[order_end + 1..].try_into().map_err(|_| {
        CatalogError::InvalidKey("schema-kind inline row change row id is truncated".to_owned())
    })?);
    Ok(InlineRowChange {
        table_id,
        schema_id,
        order,
        kind,
        row_id,
    })
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

    #[cfg(feature = "runtime-metrics")]
    fn elapsed_micros(self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_runtime_method_stage(operation: &str, started: RuntimeMetricStage) {
    record_runtime_method_elapsed(operation, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_method_stage(_operation: &str, _started: RuntimeMetricStage) {}

#[cfg(test)]
#[path = "inline_change_feed_tests.rs"]
mod inline_change_feed_tests;
