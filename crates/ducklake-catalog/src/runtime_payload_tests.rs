#[cfg(test)]
mod perf_tests {
    use std::time::Instant;

    use alloc_counter::{AllocationGuard, emit_report};

    use super::super::{payload_str_value, payload_u64_value};

    const ITERATIONS: usize = 50_000;
    const PAYLOAD: &[u8] = b"snapshot_id=42\ntable_id=7\npartition_key_index=3\npartition_value=region-0000000000000000000000000000000000000000\n";

    #[test]
    fn perf_loop_runtime_payload_scalar_lookup_reports_allocations() {
        let guard = AllocationGuard::start(
            module_path!(),
            "perf_loop_runtime_payload_scalar_lookup_reports_allocations",
            file!(),
            line!(),
            Some("runtime_payload_scalar_lookup_50k"),
        );
        for _ in 0..ITERATIONS {
            assert_eq!(
                payload_u64_value(PAYLOAD, "snapshot_id", "missing snapshot").unwrap(),
                42
            );
            assert_eq!(
                payload_u64_value(PAYLOAD, "table_id", "missing table").unwrap(),
                7
            );
            assert_eq!(
                payload_str_value(PAYLOAD, "partition_value", "missing partition").unwrap(),
                "region-0000000000000000000000000000000000000000"
            );
        }
        let report = guard.finish();
        emit_report(&report);
    }

    #[test]
    #[ignore]
    fn perf_loop_runtime_payload_scalar_lookup_reports_cpu() {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            assert_eq!(
                payload_u64_value(PAYLOAD, "snapshot_id", "missing snapshot").unwrap(),
                42
            );
            assert_eq!(
                payload_u64_value(PAYLOAD, "table_id", "missing table").unwrap(),
                7
            );
            assert_eq!(
                payload_str_value(PAYLOAD, "partition_value", "missing partition").unwrap(),
                "region-0000000000000000000000000000000000000000"
            );
        }
        println!(
            "runtime_payload_scalar_lookup_50k_ns={}",
            started.elapsed().as_nanos()
        );
    }
}
