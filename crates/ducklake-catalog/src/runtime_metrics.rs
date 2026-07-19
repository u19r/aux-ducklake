use crate::runtime_protocol::RuntimeCatalogBackend;

#[cfg(feature = "runtime-metrics")]
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{ErrorKind, Write},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(feature = "runtime-metrics")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    backend: &'static str,
    family: &'static str,
    operation: String,
    scope: String,
    status: &'static str,
}

#[cfg(feature = "runtime-metrics")]
#[derive(Debug, Clone, Copy, Default)]
struct MetricValue {
    count: u64,
    elapsed_micros: u64,
    items: u64,
    bytes: u64,
}

#[cfg(feature = "runtime-metrics")]
static RUNTIME_COUNTERS: OnceLock<Mutex<BTreeMap<MetricKey, MetricValue>>> = OnceLock::new();
#[cfg(feature = "runtime-metrics")]
static RUNTIME_TRACE: OnceLock<Mutex<Option<File>>> = OnceLock::new();
#[cfg(feature = "runtime-metrics")]
static RUNTIME_TRACE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_runtime_request(
    backend: RuntimeCatalogBackend,
    operation: &str,
    status: RuntimeMetricStatus,
) {
    record_runtime_request_elapsed(backend, operation, status, 0);
}

#[cfg(not(feature = "runtime-metrics"))]
#[allow(dead_code)]
pub(crate) fn record_runtime_request(
    _backend: RuntimeCatalogBackend,
    _operation: &str,
    _status: RuntimeMetricStatus,
) {
}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_runtime_request_elapsed(
    backend: RuntimeCatalogBackend,
    operation: &str,
    status: RuntimeMetricStatus,
    elapsed_micros: u64,
) {
    let key = MetricKey {
        backend: backend.as_str(),
        family: operation_family(operation),
        operation: operation.to_owned(),
        scope: runtime_metrics_scope(),
        status: status.as_str(),
    };
    let counters = RUNTIME_COUNTERS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let Ok(mut counters) = counters.lock() else {
        return;
    };
    let value = counters.entry(key).or_default();
    value.count = value.count.saturating_add(1);
    value.elapsed_micros = value.elapsed_micros.saturating_add(elapsed_micros);
}

#[cfg(not(feature = "runtime-metrics"))]
#[allow(dead_code)]
pub(crate) fn record_runtime_request_elapsed(
    _backend: RuntimeCatalogBackend,
    _operation: &str,
    _status: RuntimeMetricStatus,
    _elapsed_micros: u64,
) {
}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_runtime_method_elapsed(operation: &str, elapsed_micros: u64) {
    record_runtime_request_elapsed(
        RuntimeCatalogBackend::FoundationDb,
        operation,
        RuntimeMetricStatus::Ok,
        elapsed_micros,
    );
}

#[cfg(not(feature = "runtime-metrics"))]
#[allow(dead_code)]
pub(crate) fn record_runtime_method_elapsed(_operation: &str, _elapsed_micros: u64) {}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_runtime_measurement(operation: &str, items: u64, bytes: u64) {
    let key = MetricKey {
        backend: RuntimeCatalogBackend::FoundationDb.as_str(),
        family: "measure",
        operation: operation.to_owned(),
        scope: runtime_metrics_scope(),
        status: RuntimeMetricStatus::Ok.as_str(),
    };
    let counters = RUNTIME_COUNTERS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let Ok(mut counters) = counters.lock() else {
        return;
    };
    let value = counters.entry(key).or_default();
    value.count = value.count.saturating_add(1);
    value.items = value.items.saturating_add(items);
    value.bytes = value.bytes.saturating_add(bytes);
}

#[cfg(not(feature = "runtime-metrics"))]
#[allow(dead_code)]
pub(crate) fn record_runtime_measurement(_operation: &str, _items: u64, _bytes: u64) {}

#[cfg(feature = "runtime-metrics")]
#[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
pub(crate) fn record_runtime_kv(
    backend: RuntimeCatalogBackend,
    operation: &str,
    status: RuntimeMetricStatus,
    items: u64,
    bytes: u64,
    elapsed_micros: u64,
) {
    let key = MetricKey {
        backend: backend.as_str(),
        family: "kv",
        operation: operation.to_owned(),
        scope: runtime_metrics_scope(),
        status: status.as_str(),
    };
    let counters = RUNTIME_COUNTERS.get_or_init(|| Mutex::new(BTreeMap::new()));
    {
        let Ok(mut counters) = counters.lock() else {
            return;
        };
        let value = counters.entry(key).or_default();
        value.count = value.count.saturating_add(1);
        value.elapsed_micros = value.elapsed_micros.saturating_add(elapsed_micros);
        value.items = value.items.saturating_add(items);
        value.bytes = value.bytes.saturating_add(bytes);
    }
    write_runtime_trace(operation, status, items, bytes, elapsed_micros);
}

#[cfg(not(feature = "runtime-metrics"))]
#[allow(dead_code)]
pub(crate) fn record_runtime_kv(
    _backend: RuntimeCatalogBackend,
    _operation: &str,
    _status: RuntimeMetricStatus,
    _items: u64,
    _bytes: u64,
    _elapsed_micros: u64,
) {
}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn flush_runtime_metrics() {
    let Some(counters) = RUNTIME_COUNTERS.get() else {
        return;
    };
    let Ok(counters) = counters.lock() else {
        return;
    };
    write_metrics_if_requested(&counters);
}

#[cfg(not(feature = "runtime-metrics"))]
pub(crate) fn flush_runtime_metrics() {}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "runtime-metrics"), allow(dead_code))]
pub(crate) enum RuntimeMetricStatus {
    Ok,
    Error,
}

#[cfg(feature = "runtime-metrics")]
impl RuntimeMetricStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

#[cfg(feature = "runtime-metrics")]
fn write_metrics_if_requested(counters: &BTreeMap<MetricKey, MetricValue>) {
    let Some(path) = std::env::var_os("AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH") else {
        return;
    };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let _ = file.write_all(runtime_metrics_text(counters).as_bytes());
}

#[cfg(feature = "runtime-metrics")]
fn runtime_metrics_text(counters: &BTreeMap<MetricKey, MetricValue>) -> String {
    let mut out = String::new();
    for (key, value) in counters {
        out.push_str(&format!(
            "aux_ducklake_runtime_requests_total{{backend=\"{}\",family=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
            key.backend, key.family, key.operation, key.scope, key.status, value.count
        ));
        out.push_str(&format!(
            "aux_ducklake_runtime_request_elapsed_micros_total{{backend=\"{}\",family=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
            key.backend, key.family, key.operation, key.scope, key.status, value.elapsed_micros
        ));
        if key.family == "kv" {
            out.push_str(&format!(
                "aux_ducklake_runtime_kv_items_total{{backend=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
                key.backend, key.operation, key.scope, key.status, value.items
            ));
            out.push_str(&format!(
                "aux_ducklake_runtime_kv_bytes_total{{backend=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
                key.backend, key.operation, key.scope, key.status, value.bytes
            ));
        } else if key.family == "measure" {
            out.push_str(&format!(
                "aux_ducklake_runtime_measure_items_total{{backend=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
                key.backend, key.operation, key.scope, key.status, value.items
            ));
            out.push_str(&format!(
                "aux_ducklake_runtime_measure_bytes_total{{backend=\"{}\",operation=\"{}\",scope=\"{}\",status=\"{}\"}} {}\n",
                key.backend, key.operation, key.scope, key.status, value.bytes
            ));
        }
    }
    out
}

#[cfg(feature = "runtime-metrics")]
fn runtime_metrics_scope() -> String {
    std::env::var("AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unscoped".to_owned())
}

#[cfg(feature = "runtime-metrics")]
fn write_runtime_trace(
    operation: &str,
    status: RuntimeMetricStatus,
    items: u64,
    bytes: u64,
    elapsed_micros: u64,
) {
    let trace = RUNTIME_TRACE.get_or_init(|| Mutex::new(open_runtime_trace()));
    let Ok(mut trace) = trace.lock() else {
        return;
    };
    let Some(file) = trace.as_mut() else {
        return;
    };
    let sequence = RUNTIME_TRACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let line = format!(
        "{},{},{},{},{},{},{},{},{:?}\n",
        sequence,
        runtime_metrics_scope(),
        operation,
        status.as_str(),
        items,
        bytes,
        elapsed_micros,
        std::process::id(),
        std::thread::current().id()
    );
    let _ = file.write_all(line.as_bytes());
}

#[cfg(feature = "runtime-metrics")]
fn open_runtime_trace() -> Option<File> {
    let path = std::env::var_os("AUX_DUCKLAKE_BENCHMARK_RUNTIME_TRACE_PATH")?;
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            let _ = writeln!(
                file,
                "seq,scope,operation,status,items,bytes,elapsed_micros,pid,thread_id"
            );
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            if std::fs::metadata(&path).is_ok_and(|metadata| metadata.len() == 0)
                && let Ok(mut file) = OpenOptions::new().append(true).open(&path)
            {
                let _ = writeln!(
                    file,
                    "seq,scope,operation,status,items,bytes,elapsed_micros,pid,thread_id"
                );
            }
        }
        Err(_) => return None,
    }
    OpenOptions::new().create(true).append(true).open(path).ok()
}

#[cfg(feature = "runtime-metrics")]
fn operation_family(operation: &str) -> &'static str {
    if operation.starts_with("method.") {
        return "method";
    }
    match operation {
        "AttachMetadata" | "MetadataExists" | "InitializeDuckLake" | "CommitMetadataBatch" => {
            "metadata"
        }
        "CreateSchemas"
        | "DropSchemas"
        | "CreateTables"
        | "AddColumns"
        | "RenameColumns"
        | "ChangeColumnTypes"
        | "ChangeColumnDefaults"
        | "ChangeComments"
        | "ChangePartitionKeys"
        | "ChangeSortKeys"
        | "DropColumns"
        | "RenameTables"
        | "GetNextColumnId"
        | "IsColumnCreatedWithTable" => "schema",
        "DropTables" | "CreateViews" | "RenameViews" | "DropViews" | "ChangeViewComment"
        | "CreateMacros" | "DropMacros" => "object",
        "CommitDataMutation" => "data_mutation",
        "GetSnapshot"
        | "GetSnapshotAt"
        | "GetSnapshotAtTimestamp"
        | "ListSnapshots"
        | "GetCatalogForSnapshot"
        | "ListGlobalStatsForSnapshot"
        | "ListSnapshotStatsAndChangesInputs"
        | "ListDeleteFiles"
        | "ListDataFilesAt"
        | "ListCurrentDataFilesForPartitionScan"
        | "ListDataFilesForPartitionScanAt" => "read",
        "ReadInlineRows"
        | "ReadInlineRowsForGlobalStats"
        | "ReadInlineRowsForAggregateStats"
        | "ReadInlineRowsForGlobalStatsBatch"
        | "RegisterInlineRows"
        | "DeleteInlineRows"
        | "InlineFileDeletionsExist"
        | "ListInlineRowInsertions"
        | "ListInlineRowDeletions" => "inline",
        "ListDataFileChanges" | "ListTableDeletions" => "change_feed",
        "ListOldFilesForCleanup" | "ListKnownFilesForCleanup" | "RemoveCleanupFiles" => "cleanup",
        "ExpireSnapshots" => "snapshot_maintenance",
        "MergeAdjacentFiles" | "RewriteDeleteFiles" => "compaction",
        _ => "unknown",
    }
}
