# DuckLake FDB Benchmark

This benchmark is a local release-readiness signal for the FoundationDB catalog backend compared
with the DuckLake Postgres catalog backend. It is not a service sizing claim.

## Latest Full Varied Run

Date: 2026-07-02

Command:

```sh
env \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT \
  -u AUX_DUCKLAKE_FDB_CLUSTER_FILE \
  AUX_DUCKLAKE_POSTGRES_DSN='host=127.0.0.1 port=15432 dbname=postgres' \
  AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT=100 \
  AUX_DUCKLAKE_BENCHMARK_BUILD_PROFILE=release \
  CARGO_TARGET_DIR=target/codex-validation \
  ./scripts/ducklake_catalog_benchmark.sh varied
```

Artifacts:

- `docs/benchmarks/ducklake-fdb-feature-parity/fdb-varied-latest.json`
- `docs/benchmarks/ducklake-fdb-feature-parity/postgres-varied-latest.json`

Fixture:

- `100` tables.
- `24` columns per table.
- `13,108` rows per table.
- `5 GiB` target logical data.
- `4,096` preload rows per batch.
- `4` preload workers.
- `12` parallel latest-read workers.
- Same DuckDB/DuckLake SQL shape for both backends.
- Optional runtime metrics disabled; every batch reports `runtime_metric_calls=0`.
- Postgres used Toxiproxy at `127.0.0.1:15432` with a `1ms` downstream latency toxic.
- FDB used the local cluster file directly; the local FDB client rejected the transparent
  Toxiproxy port with a canonical remote port assertion unless the FDB process itself advertised
  the proxied port.

| Batch | FDB ms | Postgres ms | FDB/Postgres |
| --- | ---: | ---: | ---: |
| preload | 119,334.518 | 76,944.731 | 1.551x |
| mixed | 42,182.716 | 60,926.251 | 0.692x |
| dedicated_deletes | 26,847.776 | 62,804.551 | 0.427x |
| dedicated_inlining | 85,413.873 | 61,230.357 | 1.395x |
| dedicated_compaction | 55,461.362 | 29,456.390 | 1.883x |
| join_queries | 3,161.310 | 18,454.307 | 0.171x |
| mutation_churn | 447,686.656 | 301,780.271 | 1.483x |
| latest_queries | 3,284.301 | 9,887.231 | 0.332x |
| time_travel_queries | 3,614.635 | 10,024.259 | 0.361x |
| parallel_latest_queries | 5,188.493 | 11,262.969 | 0.461x |
| total | 795,972.092 | 647,142.375 | 1.230x |

Semantic labels matched across both artifacts after excluding FDB runtime-only attach labels.

The benchmark retry budget is set to `100` because the full varied preload intentionally uses
parallel writers; with the default DuckLake retry budget of `10`, the FDB preload can exhaust retries
while resolving transient data-file-id conflicts.
