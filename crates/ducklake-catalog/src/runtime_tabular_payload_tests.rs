#[cfg(test)]
mod perf_tests {
    use std::time::Instant;

    use alloc_counter::{AllocationGuard, emit_report};

    use super::super::{TabularPayload, parse_u64_field};

    const ROWS: usize = 1_000;

    fn table_payload() -> Vec<u8> {
        let mut payload = Vec::new();
        for index in 0..ROWS {
            payload.extend_from_slice(
                format!("table\t{index}\t0\tuuid-{index}\ttable_{index}\tmain/table_{index}/\t\n")
                    .as_bytes(),
            );
            payload.extend_from_slice(
                format!("column\t{index}\t0\tid\tINTEGER\ttrue\t\t\tNULL\tliteral\n").as_bytes(),
            );
        }
        payload
    }

    #[test]
    fn perf_loop_tabular_payload_fields_reports_allocations() {
        let payload = table_payload();
        let guard = AllocationGuard::start(
            module_path!(),
            "perf_loop_tabular_payload_fields_reports_allocations",
            file!(),
            line!(),
            Some("tabular_payload_fields_2k_rows"),
        );
        let mut table_sum = 0_u64;
        let mut column_sum = 0_u64;
        for row in TabularPayload::new("PerfTabularPayload", &payload).unwrap() {
            let row = row.unwrap();
            let fields = row.fields();
            match fields.as_slice() {
                ["table", table_id, ..] => {
                    table_sum +=
                        parse_u64_field("PerfTabularPayload", table_id, "table_id").unwrap();
                }
                ["column", table_id, column_id, ..] => {
                    table_sum +=
                        parse_u64_field("PerfTabularPayload", table_id, "table_id").unwrap();
                    column_sum +=
                        parse_u64_field("PerfTabularPayload", column_id, "column_id").unwrap();
                }
                _ => panic!("unexpected row"),
            }
        }
        assert_eq!(table_sum, 999_000);
        assert_eq!(column_sum, 0);
        let report = guard.finish();
        emit_report(&report);
    }

    #[test]
    #[ignore]
    fn perf_loop_tabular_payload_fields_reports_cpu() {
        let payload = table_payload();
        let started = Instant::now();
        let mut table_sum = 0_u64;
        let mut column_sum = 0_u64;
        for row in TabularPayload::new("PerfTabularPayload", &payload).unwrap() {
            let row = row.unwrap();
            let fields = row.fields();
            match fields.as_slice() {
                ["table", table_id, ..] => {
                    table_sum +=
                        parse_u64_field("PerfTabularPayload", table_id, "table_id").unwrap();
                }
                ["column", table_id, column_id, ..] => {
                    table_sum +=
                        parse_u64_field("PerfTabularPayload", table_id, "table_id").unwrap();
                    column_sum +=
                        parse_u64_field("PerfTabularPayload", column_id, "column_id").unwrap();
                }
                _ => panic!("unexpected row"),
            }
        }
        assert_eq!(table_sum, 999_000);
        assert_eq!(column_sum, 0);
        println!(
            "tabular_payload_fields_2k_rows_ns={}",
            started.elapsed().as_nanos()
        );
    }
}
