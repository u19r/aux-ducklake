use crate::runtime_metrics::RuntimeMetricStatus;
#[cfg(feature = "runtime-metrics")]
use crate::runtime_metrics::record_runtime_request_elapsed;
use crate::{
    CatalogResult,
    runtime_operations::payload_for_request,
    runtime_protocol::{
        RuntimeCatalogBackend, RuntimeRequest, RuntimeResponse, RuntimeResponseStatus,
        paged_runtime_response,
    },
};
use std::panic::{AssertUnwindSafe, catch_unwind};

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
/// Executes one catalog runtime request and writes the owned response buffer to `out`.
///
/// # Safety
///
/// `request_ptr` must reference `request_len` readable bytes for the duration of the call, and
/// `out` must be a valid, writable pointer to an exclusively owned buffer descriptor.
pub unsafe extern "C" fn ducklake_catalog_runtime_probe(
    request_ptr: *const u8,
    request_len: usize,
    out: *mut DuckLakeRuntimeBuffer,
) -> i32 {
    if request_ptr.is_null() || out.is_null() {
        return FFI_INVALID_ARGUMENT;
    }
    // SAFETY: The caller provides a non-null pointer and length for the duration of this call.
    let request_bytes = unsafe { std::slice::from_raw_parts(request_ptr, request_len) };
    let probe = match catch_unwind(AssertUnwindSafe(|| runtime_probe_response(request_bytes))) {
        Ok(probe) => probe,
        Err(panic) => RuntimeProbeResult {
            backend: None,
            operation: "panic".to_owned(),
            response: panic_response(panic_message(&panic)),
        },
    };
    let encode_started = RuntimeMetricStage::start();
    let encoded = encode_response(probe.response);
    if let Some(backend) = probe.backend {
        record_runtime_request_stage(
            backend,
            &format!("{}:encode", probe.operation),
            RuntimeMetricStatus::Ok,
            encode_started,
        );
    }
    match encoded {
        Ok(buffer) => {
            // SAFETY: `out` was validated as non-null and is exclusively owned by the caller.
            unsafe {
                *out = leak_buffer(buffer);
            }
            FFI_OK
        }
        Err(buffer) => {
            // SAFETY: `out` was validated as non-null and is exclusively owned by the caller.
            unsafe {
                *out = leak_buffer(buffer);
            }
            FFI_RUNTIME_ERROR
        }
    }
}

#[unsafe(no_mangle)]
/// Releases a response buffer returned by [`ducklake_catalog_runtime_probe`].
///
/// # Safety
///
/// `ptr` and `len` must be the unchanged pair returned by `ducklake_catalog_runtime_probe`, and
/// the buffer must not have been freed previously.
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
    #[cfg(feature = "foundationdb")]
    crate::fdb_runtime::abandon_foundationdb_for_process_shutdown();
    FFI_OK
}

#[cfg_attr(not(feature = "runtime-metrics"), allow(dead_code))]
struct RuntimeProbeResult {
    backend: Option<RuntimeCatalogBackend>,
    operation: String,
    response: RuntimeResponse,
}

fn runtime_probe_response(bytes: &[u8]) -> RuntimeProbeResult {
    let decode_started = RuntimeMetricStage::start();
    let request = match RuntimeRequest::decode(bytes) {
        Ok(request) => {
            record_runtime_request_stage(
                request.backend,
                &format!("{}:decode", request.operation),
                RuntimeMetricStatus::Ok,
                decode_started,
            );
            request
        }
        Err(error) => {
            let response = RuntimeResponse::error("decode-error", error.to_string().into_bytes())
                .unwrap_or_else(|_| {
                    error_response(crate::CatalogError::Decode(
                        "failed to encode decode error".to_owned(),
                    ))
                });
            return RuntimeProbeResult {
                backend: None,
                operation: "decode-error".to_owned(),
                response,
            };
        }
    };
    let operation = request.operation.clone();
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

fn runtime_response_from_request(request: RuntimeRequest) -> CatalogResult<RuntimeResponse> {
    let started = RuntimeMetricStage::start();
    let mutates_catalog = runtime_operation_mutates_catalog(&request.operation);
    if mutates_catalog && request.page_offset != 0 {
        return Err(crate::CatalogError::Decode(
            "mutating runtime operations cannot be paginated".to_owned(),
        ));
    }
    let payload = match payload_for_request(&request) {
        Ok(payload) => {
            if mutates_catalog {
                crate::store::invalidate_runtime_read_context(request.catalog_id);
            }
            if runtime_operation_mutates_inline_file_deletions(&request.operation, &request.payload)
            {
                crate::runtime_read_context::invalidate_inline_deletion_read_context(
                    request.catalog_id,
                );
            }
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
    let request_id = request.request_id;
    if mutates_catalog {
        crate::store::invalidate_runtime_read_context(request.catalog_id);
    }
    if mutates_catalog {
        RuntimeResponse::ok(request_id, payload)
    } else {
        paged_runtime_response(
            request_id,
            payload,
            request.page_offset,
            request.page_etag.as_deref(),
        )
    }
}

fn runtime_operation_mutates_catalog(operation: &str) -> bool {
    matches!(
        operation,
        "InitializeDuckLake"
            | "CommitMetadataBatch"
            | "SetConfigOption"
            | "CommitColumnMappings"
            | "CreateSchemas"
            | "DropSchemas"
            | "CreateTables"
            | "ReplaceTables"
            | "DropTables"
            | "CreateViews"
            | "RenameViews"
            | "DropViews"
            | "ChangeViewComment"
            | "CreateMacros"
            | "DropMacros"
            | "AddColumns"
            | "RenameColumns"
            | "ChangeColumnTypes"
            | "ChangeColumnDefaults"
            | "ChangeComments"
            | "ChangePartitionKeys"
            | "ChangeSortKeys"
            | "DropColumns"
            | "RenameTables"
            | "CommitDataMutation"
            | "CommitAttempt"
            | "MergeAdjacentFiles"
            | "RewriteDeleteFiles"
            | "RegisterInlineTables"
            | "RegisterInlineRows"
            | "DeleteInlineRows"
            | "RemoveCleanupFiles"
            | "ExpireSnapshots"
    )
}

fn runtime_operation_mutates_inline_file_deletions(operation: &str, payload: &[u8]) -> bool {
    match operation {
        "CommitDataMutation" | "CommitAttempt" => payload
            .windows(b"inline_file_delete".len())
            .any(|window| window == b"inline_file_delete"),
        _ => false,
    }
}

fn encode_response(response: RuntimeResponse) -> Result<Vec<u8>, Vec<u8>> {
    response.encode().map_err(|error| {
        RuntimeResponse::error("encode-error", error.to_string().into_bytes())
            .and_then(|response| response.encode())
            .unwrap_or_else(|_| {
                b"aux-ducklake-runtime/2\nrequest_id=encode-error\nstatus=error\npayload_len=0\n\n"
                    .to_vec()
            })
    })
}

fn error_response(error: crate::CatalogError) -> RuntimeResponse {
    RuntimeResponse::error("runtime-error", error.to_string().into_bytes()).unwrap_or_else(|_| {
        RuntimeResponse {
            request_id: "runtime-error".to_owned(),
            status: RuntimeResponseStatus::Error,
            payload: Vec::new(),
            next_page_offset: None,
            page_etag: None,
        }
    })
}

fn panic_response(message: String) -> RuntimeResponse {
    RuntimeResponse::error("panic", format!("runtime panic: {message}").into_bytes())
        .unwrap_or_else(|_| RuntimeResponse {
            request_id: "panic".to_owned(),
            status: RuntimeResponseStatus::Error,
            payload: Vec::new(),
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
