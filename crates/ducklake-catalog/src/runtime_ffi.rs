use crate::runtime_protocol::RuntimePayload;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_request_elapsed;
use crate::{
    CatalogResult,
    runtime_metrics::{RuntimeMetricStage, RuntimeMetricStatus},
    runtime_operations::{payload_for_request, runtime_operation_policy},
    runtime_protocol::{
        RuntimeCatalogBackend, RuntimeRequestRef, RuntimeResponse, RuntimeResponseStatus,
        paged_runtime_response,
    },
    runtime_trace::RuntimeSpan,
};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DuckLakeRuntimeBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

const FFI_OK: i32 = 0;
const FFI_INVALID_ARGUMENT: i32 = 1;
const FFI_RUNTIME_ERROR: i32 = 2;

#[cfg(feature = "runtime-metrics")]
fn record_runtime_request_stage(
    backend: RuntimeCatalogBackend,
    operation: &str,
    status: RuntimeMetricStatus,
    started: RuntimeMetricStage,
) {
    record_runtime_request_elapsed(backend, operation, status, started.elapsed_micros());
}

#[cfg(not(feature = "runtime-metrics"))]
#[inline]
fn record_runtime_request_stage(
    _backend: RuntimeCatalogBackend,
    _operation: &str,
    _status: RuntimeMetricStatus,
    _started: RuntimeMetricStage,
) {
}

#[unsafe(no_mangle)]
/// Executes one catalog runtime request and writes the owned response buffer to
/// `out`.
///
/// # Safety
///
/// `request_ptr` must reference `request_len` readable bytes for the duration
/// of the call, and `out` must be a valid, writable pointer to an exclusively
/// owned buffer descriptor.
pub unsafe extern "C" fn ducklake_catalog_runtime_probe(
    request_ptr: *const u8,
    request_len: usize,
    out: *mut DuckLakeRuntimeBuffer,
) -> i32 {
    if request_ptr.is_null() || out.is_null() {
        return FFI_INVALID_ARGUMENT;
    }
    // SAFETY: The caller provides a non-null pointer and length for the duration of
    // this call.
    let request_bytes = unsafe { std::slice::from_raw_parts(request_ptr, request_len) };
    let mut span = RuntimeSpan::enter("ffi.request");
    let probe = match catch_unwind(AssertUnwindSafe(|| {
        runtime_probe_response(request_bytes, &mut span)
    })) {
        Ok(probe) => probe,
        Err(panic) => RuntimeProbeResult {
            backend: None,
            operation: "panic",
            response: panic_response(panic_message(&panic)),
        },
    };
    let is_ok = probe.response.status == RuntimeResponseStatus::Ok;
    let (status, buffer) = encode_probe_response(probe);
    span.finish(is_ok && status == FFI_OK);
    // SAFETY: `out` is non-null, writable and exclusively owned by the caller.
    unsafe {
        *out = leak_buffer(buffer);
    }
    status
}

#[unsafe(no_mangle)]
/// Releases a response buffer returned by [`ducklake_catalog_runtime_probe`].
///
/// # Safety
///
/// `ptr` and `len` must be the unchanged pair returned by
/// `ducklake_catalog_runtime_probe`, and the buffer must not have been freed
/// previously.
pub unsafe extern "C" fn ducklake_catalog_runtime_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: Buffers returned by this module are allocated as boxed byte slices.
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        let _ = Box::from_raw(slice);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn ducklake_catalog_runtime_shutdown() -> i32 {
    crate::runtime_metrics::flush_runtime_metrics();
    crate::runtime_trace::flush_runtime_trace();
    #[cfg(feature = "foundationdb")]
    crate::fdb_runtime::abandon_foundationdb_for_process_shutdown();
    FFI_OK
}

#[unsafe(no_mangle)]
/// Starts a synchronous C++ diagnostic scope; disabled builds return null.
///
/// # Safety
/// `operation` references `len` readable UTF-8 bytes during this call.
/// End the returned scope exactly once, on this thread, in reverse nesting order.
pub unsafe extern "C" fn ducklake_catalog_runtime_trace_begin(
    operation: *const u8,
    len: usize,
) -> *mut std::ffi::c_void {
    #[cfg(feature = "runtime-metrics")]
    {
        if operation.is_null() {
            return std::ptr::null_mut();
        }
        // SAFETY: The caller retains the operation bytes throughout this call.
        let bytes = unsafe { std::slice::from_raw_parts(operation, len) };
        let Ok(operation) = std::str::from_utf8(bytes) else {
            return std::ptr::null_mut();
        };
        Box::into_raw(Box::new(RuntimeSpan::enter(operation))).cast()
    }
    #[cfg(not(feature = "runtime-metrics"))]
    {
        let _ = (operation, len);
        std::ptr::null_mut()
    }
}

#[unsafe(no_mangle)]
/// Ends a scope returned by `ducklake_catalog_runtime_trace_begin`.
///
/// # Safety
/// `span` is null or an unfreed scope from this thread at the top of its trace stack.
pub unsafe extern "C" fn ducklake_catalog_runtime_trace_end(
    span: *mut std::ffi::c_void,
    is_ok: i32,
) {
    if !span.is_null() {
        // SAFETY: The caller supplies its unique live scope on the originating thread.
        let span = unsafe { Box::from_raw(span.cast::<RuntimeSpan>()) };
        span.finish(is_ok != 0);
    }
}

#[cfg_attr(not(feature = "runtime-metrics"), allow(dead_code))]
struct RuntimeProbeResult<'a> {
    backend: Option<RuntimeCatalogBackend>,
    operation: &'a str,
    response: RuntimeResponse<RuntimePayload>,
}

fn encode_probe_response(probe: RuntimeProbeResult<'_>) -> (i32, Vec<u8>) {
    #[cfg(feature = "runtime-metrics")]
    let started = RuntimeMetricStage::start();
    let encoded = encode_response(probe.response);
    #[cfg(feature = "runtime-metrics")]
    if let Some(backend) = probe.backend {
        let status = if encoded.is_ok() {
            RuntimeMetricStatus::Ok
        } else {
            RuntimeMetricStatus::Error
        };
        #[cfg(feature = "runtime-metrics")]
        record_runtime_request_stage(
            backend,
            &format!("{}:encode", probe.operation),
            status,
            started,
        );
    }
    match encoded {
        Ok(buffer) => (FFI_OK, buffer),
        Err(buffer) => (FFI_RUNTIME_ERROR, buffer),
    }
}

fn runtime_probe_response<'a>(bytes: &'a [u8], span: &mut RuntimeSpan) -> RuntimeProbeResult<'a> {
    #[cfg(feature = "runtime-metrics")]
    let decode_started = RuntimeMetricStage::start();
    let request = match RuntimeRequestRef::decode(bytes) {
        Ok(request) => {
            span.bind_request(request.catalog_id.0, request.read_context_id);
            #[cfg(feature = "runtime-metrics")]
            record_runtime_request_stage(
                request.backend,
                &format!("{}:decode", request.operation),
                RuntimeMetricStatus::Ok,
                decode_started,
            );
            request
        }
        Err(error) => {
            let response =
                RuntimeResponse::error("decode-error", error.to_string().into_bytes().into())
                    .unwrap_or_else(|_| {
                        error_response(crate::CatalogError::Decode(
                            "failed to encode decode error".to_owned(),
                        ))
                    });
            return RuntimeProbeResult {
                backend: None,
                operation: "decode-error",
                response,
            };
        }
    };
    let operation = request.operation;
    let backend = request.backend;
    let response = runtime_response_from_request(request).map_err(error_response);
    let response = match response {
        Ok(response) | Err(response) => response,
    };
    RuntimeProbeResult {
        backend: Some(backend),
        operation,
        response,
    }
}

fn runtime_response_from_request(
    request: RuntimeRequestRef<'_>,
) -> CatalogResult<RuntimeResponse<RuntimePayload>> {
    let started = RuntimeMetricStage::start();
    let mutates_catalog = runtime_operation_policy(&request.operation).mutates_catalog;
    // Commit validation must observe commits made after the caller's pinned read
    // context.
    let context_id = request.read_context_id.filter(|_| !mutates_catalog);
    let mut read_request = crate::store::begin_runtime_read_request(context_id);
    if mutates_catalog && request.page_offset != 0 {
        return Err(crate::CatalogError::Decode(
            "mutating runtime operations cannot be paginated".to_owned(),
        ));
    }
    if mutates_catalog {
        crate::store::invalidate_runtime_read_context(request.catalog_id);
        read_request.invalidate_catalog_on_drop(request.catalog_id, request.read_context_id);
    }
    #[cfg(feature = "foundationdb")]
    let result = crate::runtime_read_versions::read_with_retry(|| payload_for_request(&request));
    #[cfg(not(feature = "foundationdb"))]
    let result = payload_for_request(&request);
    let payload = match result {
        Ok(payload) => {
            record_runtime_request_stage(
                request.backend,
                &request.operation,
                RuntimeMetricStatus::Ok,
                started,
            );
            payload
        }
        Err(error) => {
            record_runtime_request_stage(
                request.backend,
                &request.operation,
                RuntimeMetricStatus::Error,
                started,
            );
            return Err(error);
        }
    };
    let request_id = request.request_id.to_owned();
    if request.operation == "EndRuntimeReadContext" {
        read_request.end_context();
    }
    if mutates_catalog {
        RuntimeResponse::ok(request_id, payload)
    } else {
        paged_runtime_response(request_id, payload, request.page_offset, request.page_etag)
    }
}

fn encode_response(response: RuntimeResponse<RuntimePayload>) -> Result<Vec<u8>, Vec<u8>> {
    response.encode().map_err(|error| {
        RuntimeResponse::error("encode-error", error.to_string().into_bytes())
            .and_then(|response| response.encode())
            .unwrap_or_else(|_| {
                b"aux-ducklake-runtime/2\nrequest_id=encode-error\nstatus=error\npayload_len=0\n\n"
                    .to_vec()
            })
    })
}

fn error_response(error: crate::CatalogError) -> RuntimeResponse<RuntimePayload> {
    RuntimeResponse::error("runtime-error", error.to_string().into_bytes().into()).unwrap_or_else(
        |_| RuntimeResponse {
            request_id: "runtime-error".to_owned(),
            status: RuntimeResponseStatus::Error,
            payload: Vec::new().into(),
            next_page_offset: None,
            page_etag: None,
        },
    )
}

fn panic_response(message: String) -> RuntimeResponse<RuntimePayload> {
    RuntimeResponse::error(
        "panic",
        format!("runtime panic: {message}").into_bytes().into(),
    )
    .unwrap_or_else(|_| RuntimeResponse {
        request_id: "panic".to_owned(),
        status: RuntimeResponseStatus::Error,
        payload: Vec::new().into(),
        next_page_offset: None,
        page_etag: None,
    })
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_owned()
}

fn leak_buffer(buffer: Vec<u8>) -> DuckLakeRuntimeBuffer {
    let mut buffer = buffer.into_boxed_slice();
    let out = DuckLakeRuntimeBuffer {
        ptr: buffer.as_mut_ptr(),
        len: buffer.len(),
    };
    std::mem::forget(buffer);
    out
}

#[cfg(test)]
#[path = "runtime_ffi_tests.rs"]
mod tests;
