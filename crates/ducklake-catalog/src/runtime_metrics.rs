use crate::runtime_protocol::RuntimeCatalogBackend;

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy)]
pub(crate) struct RuntimeMetricStage(Option<std::time::Instant>);

#[cfg(not(feature = "runtime-metrics"))]
#[derive(Clone, Copy)]
pub(crate) struct RuntimeMetricStage;

impl RuntimeMetricStage {
    #[inline]
    pub(crate) fn start() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(Some(std::time::Instant::now()))
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(not(test))]
    #[inline]
    pub(crate) fn zero() -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            Self(None)
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            Self
        }
    }

    #[cfg(feature = "runtime-metrics")]
    pub(crate) fn elapsed_micros(self) -> u64 {
        self.0
            .map(|started| u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

#[cfg(feature = "runtime-metrics")]
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    sync::{Mutex, OnceLock},
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
    record_request_counter(backend, operation, status, elapsed_micros);
    crate::runtime_trace::record_elapsed(operation, status, (0, 0), elapsed_micros);
}

#[cfg(feature = "runtime-metrics")]
fn record_request_counter(
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
    drop(counters);
    crate::runtime_trace::record_elapsed(operation, RuntimeMetricStatus::Ok, (items, bytes), 0);
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
    crate::runtime_trace::record_elapsed(operation, status, (items, bytes), elapsed_micros);
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
    let Ok(mut counters) = counters.lock() else {
        return;
    };
    if counters.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut *counters);
    drop(counters);
    write_metrics_if_requested(&pending);
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
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) fn runtime_metrics_scope() -> String {
    std::env::var("AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unscoped".to_owned())
}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_runtime_span_counter(
    operation: &str,
    status: RuntimeMetricStatus,
    elapsed: u64,
) {
    record_request_counter(
        RuntimeCatalogBackend::FoundationDb,
        operation,
        status,
        elapsed,
    );
}

#[cfg(feature = "runtime-metrics")]
fn operation_family(operation: &str) -> &'static str {
    if operation.starts_with("wait.") {
        return "wait";
    }
    if operation.starts_with("method.") {
        return "method";
    }
    crate::runtime_operations::runtime_operation_policy(operation).family
}
