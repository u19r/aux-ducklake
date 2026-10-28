. as $scenarios
| [$scenarios[] | select(.comparison.status == "complete")] as $complete
| [$complete[] | select(.fdb != null and .postgres != null)] as $paired
| ([$complete[].comparison.fdb_elapsed_ms | select(. != null)] | if length > 0 then add else null end) as $fdb_total
| ([$complete[].comparison.postgres_elapsed_ms | select(. != null)] | if length > 0 then add else null end) as $postgres_total
| {
    artifact: "ducklake-catalog-robustness-benchmark",
    generated_at: (now | todateiso8601),
    methodology: {
      same_sql_for_backends: true,
      isolated_catalog_per_backend_and_scenario: true,
      backend_execution_order_alternates: (all($scenarios[]; .execution_order == "fdb-first" or .execution_order == "postgres-first")),
      timing_scope: "end-to-end DuckDB process wall time"
    },
    summary: {
      scenario_count: ($scenarios | length),
      completed_scenario_count: ($complete | length),
      fdb_win_count: ([$paired[] | select(.comparison.fdb_postgres_ratio < 1)] | length),
      fdb_total_ms: $fdb_total,
      postgres_total_ms: $postgres_total,
      fdb_postgres_total_ratio: (if ($fdb_total != null and $postgres_total != null and $postgres_total != 0) then ($fdb_total / $postgres_total) else null end)
    },
    scenarios: $scenarios
  }
