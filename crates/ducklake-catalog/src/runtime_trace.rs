//! Correlated diagnostic intervals. Guards cover synchronous work only.
#[cfg(feature = "runtime-metrics")]
use std::{
    cell::Cell,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    marker::PhantomData,
    rc::Rc,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::RuntimeMetricStatus;

#[cfg(feature = "runtime-metrics")]
#[derive(Clone, Copy, Default)]
struct TraceContext {
    span: u64,
    request: u64,
    read_context: u64,
    catalog: u64,
}

#[cfg(feature = "runtime-metrics")]
thread_local! {
    static TRACE_CONTEXT: Cell<TraceContext> = const { Cell::new(TraceContext {
        span: 0, request: 0, read_context: 0, catalog: 0,
    }) };
}

#[cfg(feature = "runtime-metrics")]
static NEXT_SPAN: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "runtime-metrics")]
static NEXT_RECORD: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "runtime-metrics")]
static TRACE_EPOCH: OnceLock<Instant> = OnceLock::new();
#[cfg(feature = "runtime-metrics")]
static TRACE_FILE: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();

#[cfg(feature = "runtime-metrics")]
pub(crate) struct RuntimeSpan {
    operation: String,
    context: TraceContext,
    previous: TraceContext,
    parent: u64,
    kind: &'static str,
    start: u64,
    cpu_start: Option<u64>,
    status: RuntimeMetricStatus,
    // A thread-local synchronous span must neither migrate nor cross an await.
    thread_bound: PhantomData<Rc<()>>,
}

#[cfg(not(feature = "runtime-metrics"))]
pub(crate) struct RuntimeSpan;

impl RuntimeSpan {
    #[inline]
    pub(crate) fn enter(operation: &str) -> Self {
        #[cfg(feature = "runtime-metrics")]
        {
            let previous = TRACE_CONTEXT.get();
            let context = TraceContext {
                span: NEXT_SPAN.fetch_add(1, Ordering::Relaxed),
                ..previous
            };
            TRACE_CONTEXT.set(context);
            Self {
                operation: operation.to_owned(),
                context,
                previous,
                parent: previous.span,
                kind: "span",
                start: monotonic_micros(),
                cpu_start: is_recording().then(thread_cpu_micros).flatten(),
                status: RuntimeMetricStatus::Error,
                thread_bound: PhantomData,
            }
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            let _ = operation;
            Self
        }
    }

    pub(crate) fn bind_request(&mut self, catalog: u64, read_context: Option<u64>) {
        #[cfg(feature = "runtime-metrics")]
        {
            self.context.request = self.context.span;
            self.context.catalog = catalog;
            self.context.read_context = read_context.unwrap_or(0);
            TRACE_CONTEXT.set(self.context);
        }
        #[cfg(not(feature = "runtime-metrics"))]
        let _ = (catalog, read_context);
    }

    pub(crate) fn finish(mut self, is_ok: bool) {
        #[cfg(feature = "runtime-metrics")]
        {
            self.status = if is_ok {
                RuntimeMetricStatus::Ok
            } else {
                RuntimeMetricStatus::Error
            };
        }
        #[cfg(not(feature = "runtime-metrics"))]
        {
            let _ = (&mut self, is_ok);
        }
    }
}

#[cfg(feature = "runtime-metrics")]
impl Drop for RuntimeSpan {
    fn drop(&mut self) {
        let elapsed = monotonic_micros().saturating_sub(self.start);
        let cpu = self
            .cpu_start
            .and_then(|start| thread_cpu_micros().map(|end| end.saturating_sub(start)));
        TRACE_CONTEXT.set(self.previous);
        crate::runtime_metrics::record_runtime_span_counter(&self.operation, self.status, elapsed);
        if !is_recording() {
            return;
        }
        write_record(TraceRecord {
            operation: self.operation.clone(),
            status: self.status.as_str(),
            items: 0,
            bytes: 0,
            elapsed,
            start: self.start,
            cpu,
            context: self.context,
            parent: self.parent,
            kind: self.kind,
        });
    }
}

/// Carries attribution to a scoped worker without moving a live span between threads.
#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy)]
pub(crate) struct RuntimeTraceParent {
    #[cfg(feature = "runtime-metrics")]
    context: TraceContext,
}

#[cfg(feature = "foundationdb")]
impl RuntimeTraceParent {
    pub(crate) fn capture() -> Self {
        Self {
            #[cfg(feature = "runtime-metrics")]
            context: TRACE_CONTEXT.get(),
        }
    }

    pub(crate) fn enter(self, operation: &str) -> RuntimeSpan {
        #[cfg(feature = "runtime-metrics")]
        {
            let previous = TRACE_CONTEXT.replace(self.context);
            let mut span = RuntimeSpan::enter(operation);
            span.previous = previous;
            span.kind = "task";
            span
        }
        #[cfg(not(feature = "runtime-metrics"))]
        RuntimeSpan::enter(operation)
    }
}

#[cfg(feature = "runtime-metrics")]
pub(super) struct TraceRecord {
    pub(super) operation: String,
    pub(super) status: &'static str,
    pub(super) items: u64,
    pub(super) bytes: u64,
    pub(super) elapsed: u64,
    pub(super) start: u64,
    pub(super) cpu: Option<u64>,
    context: TraceContext,
    pub(super) parent: u64,
    pub(super) kind: &'static str,
}

#[cfg(feature = "runtime-metrics")]
impl TraceRecord {
    #[cfg(test)]
    pub(super) fn span_id(&self) -> u64 {
        self.context.span
    }
    #[cfg(test)]
    pub(super) fn request_id(&self) -> u64 {
        self.context.request
    }
    #[cfg(test)]
    pub(super) fn read_context_id(&self) -> u64 {
        self.context.read_context
    }

    fn encode(&self) -> String {
        let cpu = self.cpu.map(|value| value.to_string()).unwrap_or_default();
        format!(
            "{},\"{}\",\"{}\",{},{},{},{},{},{:?},{},{},{},{},{},{},{},{}\n",
            NEXT_RECORD.fetch_add(1, Ordering::Relaxed),
            escape_csv(&crate::runtime_metrics::runtime_metrics_scope()),
            escape_csv(&self.operation),
            self.status,
            self.items,
            self.bytes,
            self.elapsed,
            std::process::id(),
            std::thread::current().id(),
            self.start,
            self.context.span,
            self.parent,
            self.context.request,
            self.context.read_context,
            self.context.catalog,
            cpu,
            self.kind
        )
    }
}

#[cfg(feature = "runtime-metrics")]
pub(crate) fn record_elapsed(
    operation: &str,
    status: RuntimeMetricStatus,
    counts: (u64, u64),
    elapsed: u64,
) {
    if !is_recording() {
        return;
    }
    let (items, bytes) = counts;
    let parent = TRACE_CONTEXT.get();
    write_record(TraceRecord {
        operation: operation.to_owned(),
        status: status.as_str(),
        items,
        bytes,
        elapsed,
        start: monotonic_micros().saturating_sub(elapsed),
        cpu: None,
        context: TraceContext {
            span: NEXT_SPAN.fetch_add(1, Ordering::Relaxed),
            ..parent
        },
        parent: parent.span,
        kind: "event",
    });
}

#[cfg(feature = "runtime-metrics")]
fn monotonic_micros() -> u64 {
    u64::try_from(TRACE_EPOCH.get_or_init(Instant::now).elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(feature = "runtime-metrics")]
fn thread_cpu_micros() -> Option<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` points to a writable timespec; this clock has no external state.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    (status == 0)
        .then(|| (time.tv_sec as u64).saturating_mul(1_000_000) + time.tv_nsec as u64 / 1_000)
}

#[cfg(feature = "runtime-metrics")]
fn escape_csv(value: &str) -> String {
    value.replace('"', "\"\"")
}

#[cfg(feature = "runtime-metrics")]
fn write_record(record: TraceRecord) {
    #[cfg(test)]
    if capture_test_record(&record) {
        return;
    }
    if let Some(file) = trace_output()
        && let Ok(mut file) = file.lock()
    {
        let _ = file.write_all(record.encode().as_bytes());
    }
}

#[cfg(feature = "runtime-metrics")]
fn is_recording() -> bool {
    #[cfg(test)]
    if TEST_RECORDS.with_borrow(Option::is_some) {
        return true;
    }
    trace_output().is_some()
}

#[cfg(feature = "runtime-metrics")]
fn trace_output() -> Option<&'static Mutex<BufWriter<File>>> {
    TRACE_FILE
        .get_or_init(|| open_trace_file().map(Mutex::new))
        .as_ref()
}

#[cfg(feature = "runtime-metrics")]
fn open_trace_file() -> Option<BufWriter<File>> {
    let path = std::env::var_os("AUX_DUCKLAKE_BENCHMARK_RUNTIME_TRACE_PATH")?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()?;
    if file.metadata().ok()?.len() == 0 {
        file.write_all(b"seq,scope,operation,status,items,bytes,elapsed_micros,pid,thread_id,start_micros,span_id,parent_span_id,request_span_id,read_context_id,catalog_id,cpu_micros,kind\n").ok()?;
    }
    Some(BufWriter::new(file))
}

pub(crate) fn flush_runtime_trace() {
    #[cfg(feature = "runtime-metrics")]
    if let Some(file) = TRACE_FILE.get().and_then(Option::as_ref)
        && let Ok(mut file) = file.lock()
    {
        let _ = file.flush();
    }
}

#[cfg(all(test, feature = "runtime-metrics"))]
thread_local! {
    static TEST_RECORDS: std::cell::RefCell<Option<Vec<TraceRecord>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "runtime-metrics"))]
pub(super) fn capture_test_records(action: impl FnOnce()) -> Vec<TraceRecord> {
    TEST_RECORDS.with_borrow_mut(|records| *records = Some(Vec::new()));
    action();
    TEST_RECORDS.with_borrow_mut(|records| records.take().unwrap())
}

#[cfg(all(test, feature = "runtime-metrics"))]
fn capture_test_record(record: &TraceRecord) -> bool {
    TEST_RECORDS.with_borrow_mut(|records| {
        if let Some(records) = records {
            records.push(TraceRecord {
                operation: record.operation.clone(),
                ..*record
            });
            true
        } else {
            false
        }
    })
}
