use crate::{
    runtime_ffi::{
        DuckLakeRuntimeBuffer, ducklake_catalog_runtime_free, ducklake_catalog_runtime_probe,
    },
    runtime_metrics::record_runtime_measurement,
    runtime_protocol::{
        RuntimeCatalogBackend, RuntimeRequest, RuntimeResponse, RuntimeResponseStatus,
    },
    runtime_trace::{RuntimeSpan, capture_test_records},
};

fn execute_request(operation: &str, payload: &[u8], context: u64) -> RuntimeResponse {
    let request = RuntimeRequest::new(
        "trace-fixture",
        RuntimeCatalogBackend::FoundationDb,
        operation,
        payload.to_vec(),
    )
    .unwrap()
    .with_read_context_id(context)
    .unwrap()
    .encode()
    .unwrap();
    let mut buffer = DuckLakeRuntimeBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    // SAFETY: The request and output descriptor remain live throughout the call.
    assert_eq!(
        unsafe { ducklake_catalog_runtime_probe(request.as_ptr(), request.len(), &mut buffer) },
        0
    );
    // SAFETY: The successful probe returned its owned buffer, freed after decoding.
    let response = unsafe {
        RuntimeResponse::decode(std::slice::from_raw_parts(buffer.ptr, buffer.len)).unwrap()
    };
    // SAFETY: This is the unchanged buffer pair, freed exactly once.
    unsafe { ducklake_catalog_runtime_free(buffer.ptr, buffer.len) };
    response
}

#[test]
fn given_nested_stages_when_recorded_then_intervals_and_counters_have_one_request_parent() {
    let records = capture_test_records(|| {
        let mut request = RuntimeSpan::enter("fixture.request");
        request.bind_request(17, Some(42));
        let stage = RuntimeSpan::enter("method.fixture.stage");
        record_runtime_measurement("fixture.rows", 7, 91);
        stage.finish(true);
        request.finish(true);
    });
    let event = &records[0];
    let stage = &records[1];
    let request = &records[2];
    assert_eq!((event.items, event.bytes), (7, 91));
    assert_eq!(event.parent, stage.span_id());
    assert_eq!(stage.parent, request.span_id());
    assert_eq!(request.parent, 0);
    for record in &records {
        assert_eq!(record.request_id(), request.span_id());
        assert_eq!(record.read_context_id(), 42);
        assert!(record.start >= request.start);
        assert!(record.start + record.elapsed <= request.start + request.elapsed);
    }
    assert!(stage.cpu.is_some());
}

#[test]
fn given_failed_ffi_request_when_next_request_runs_then_context_does_not_leak() {
    let records = capture_test_records(|| {
        let error = execute_request("CommitAttempt", b"invalid", 701);
        assert_eq!(error.status, RuntimeResponseStatus::Error);
        let ok = execute_request("EndRuntimeReadContext", b"", 702);
        assert_eq!(ok.status, RuntimeResponseStatus::Ok);
        record_runtime_measurement("fixture.after_request", 1, 0);
    });
    let roots: Vec<_> = records
        .iter()
        .filter(|record| record.operation == "ffi.request")
        .collect();
    assert_eq!(roots.len(), 2);
    assert_eq!(
        (roots[0].status, roots[0].read_context_id()),
        ("error", 701)
    );
    assert_eq!((roots[1].status, roots[1].read_context_id()), ("ok", 702));
    assert_ne!(roots[0].request_id(), roots[1].request_id());
    assert!(roots.iter().all(|record| record.parent == 0));
    let after = records.last().unwrap();
    assert_eq!(
        (after.parent, after.request_id(), after.read_context_id()),
        (0, 0, 0)
    );
    assert!(records.iter().any(|record| record.operation
        == "method.runtime_commit_attempt.ParseIntent"
        && record.status == "error"));
}

#[test]
fn given_cpp_scope_when_runtime_is_called_then_both_layers_share_the_parent_stack() {
    let records = capture_test_records(|| {
        let operation = b"cpp.fixture";
        // SAFETY: The operation lives through begin; the scope ends once on this thread.
        let span = unsafe {
            crate::runtime_ffi::ducklake_catalog_runtime_trace_begin(
                operation.as_ptr(),
                operation.len(),
            )
        };
        assert!(!span.is_null());
        execute_request("EndRuntimeReadContext", b"", 703);
        // SAFETY: The returned scope is live and is now the top scope again.
        unsafe { crate::runtime_ffi::ducklake_catalog_runtime_trace_end(span, 1) };
    });
    let cpp = records.last().unwrap();
    let request = records
        .iter()
        .find(|record| record.operation == "ffi.request")
        .unwrap();
    assert_eq!(cpp.operation, "cpp.fixture");
    assert_eq!(request.parent, cpp.span_id());
    assert_eq!(request.request_id(), request.span_id());
    assert_eq!(cpp.parent, 0);
}

#[test]
#[cfg(feature = "foundationdb")]
fn given_worker_parent_when_forked_then_work_is_attributed_and_worker_context_is_restored() {
    let mut worker_records = Vec::new();
    let records = capture_test_records(|| {
        let mut request = RuntimeSpan::enter("ffi.request");
        request.bind_request(1, Some(704));
        let parent = crate::runtime_trace::RuntimeTraceParent::capture();
        worker_records = std::thread::spawn(move || {
            capture_test_records(|| {
                let worker = parent.enter("method.fixture.Worker");
                record_runtime_measurement("fdb.transaction.created", 1, 0);
                worker.finish(true);
                record_runtime_measurement("fixture.after_worker", 1, 0);
            })
        })
        .join()
        .unwrap();
        request.finish(true);
    });
    let request = &records[0];
    let event = &worker_records[0];
    let worker = &worker_records[1];
    assert_eq!(worker.kind, "task");
    assert_eq!(worker.parent, request.span_id());
    assert_eq!(event.parent, worker.span_id());
    assert_eq!(event.request_id(), request.request_id());
    assert_eq!(event.read_context_id(), 704);
    assert_eq!(
        (worker_records[2].parent, worker_records[2].request_id()),
        (0, 0)
    );
}

#[test]
fn given_panicking_scope_when_unwound_then_thread_and_next_request_are_independent() {
    let records = capture_test_records(|| {
        let _ = std::panic::catch_unwind(|| {
            let mut request = RuntimeSpan::enter("fixture.panic");
            request.bind_request(1, Some(42));
            let foreign = std::thread::spawn(|| {
                capture_test_records(|| record_runtime_measurement("fixture.foreign", 1, 0))
            })
            .join()
            .unwrap();
            assert_eq!((foreign[0].parent, foreign[0].request_id()), (0, 0));
            panic!("fixture unwind");
        });
        execute_request("EndRuntimeReadContext", b"", 77);
    });
    assert_eq!(records[0].status, "error");
    let next = records
        .iter()
        .find(|record| record.operation == "ffi.request")
        .unwrap();
    assert_eq!((next.parent, next.read_context_id()), (0, 77));
}

#[cfg(feature = "foundationdb")]
#[test]
fn given_blocking_database_wait_when_traced_then_wall_time_is_distinct_from_thread_cpu() {
    let records = capture_test_records(|| {
        crate::fdb_runtime::wait_for_fdb("wait.fixture", async {
            std::thread::sleep(std::time::Duration::from_millis(10));
            Ok::<_, ()>(())
        })
        .unwrap();
    });
    let wait = &records[0];
    assert_eq!(wait.kind, "span");
    assert!(wait.elapsed >= 10_000);
    assert!(wait.elapsed > wait.cpu.unwrap() + 5_000);
}
