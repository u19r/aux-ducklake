. as $scenarios
| [$scenarios[] | select(.comparison.status == "complete")] as $complete
| {
    artifact: "ducklake-catalog-robustness-benchmark",
    generated_at: (now | todateiso8601),
    methodology: {
      same_sql_for_backends: true,
      isolated_catalog_per_backend_and_scenario: true,
      backend_execution_order_alternates: true,
      timing_scope: "end-to-end DuckDB process wall time"
    },
    summary: {
      scenario_count: ($scenarios | length),
      completed_scenario_count: ($complete | length),
      fdb_win_count: ([$complete[] | select(.comparison.fdb_postgres_ratio < 1)] | length),
      fdb_total_ms: ([$complete[].comparison.fdb_elapsed_ms] | add // 0),
      postgres_total_ms: ([$complete[].comparison.postgres_elapsed_ms] | add // 0),
      fdb_postgres_total_ratio: (
        ([$complete[].comparison.fdb_elapsed_ms] | add // 0)
        / ([$complete[].comparison.postgres_elapsed_ms] | add // 1)
      )
    },
    scenarios: $scenarios
  }
