# DuckLake FDB Benchmark

This benchmark is a local release-readiness signal for the FoundationDB catalog backend compared
with the DuckLake Postgres catalog backend. It is not a service sizing claim.

## Latest Full Varied Run

Date: 2026-07-22

Command:

```sh
env \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE \
  -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT \
  AUX_DUCKLAKE_FDB_CLUSTER_FILE='/private/tmp/aux-ducklake-benchmark-toxiproxy.Cyd8eu/auxfn-fdb.cluster' \
  AUX_DUCKLAKE_POSTGRES_DSN='host=127.0.0.1 port=15432 dbname=postgres' \
  AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT=100 \
  AUX_DUCKLAKE_BENCHMARK_BUILD_PROFILE=release \
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
- Both proxies had the same `1ms` downstream latency toxic with `0ms` jitter. At completion,
  Toxiproxy had recorded nonzero traffic for both benchmark listeners (FDB: 12,588,175,244
  downstream and 3,792,699,536 upstream received bytes; Postgres: 7,977,848,257 downstream and
  119,523,569 upstream received bytes).
- The DuckLake release build used pinned
  DuckLake `2856687c875bbee90d523fe15627f8d8fd494622` and DuckDB
  `117e1a46be1c903c5a36ee3c881c125597f93c60`.

| Batch                   |        FDB ms |   Postgres ms | FDB/Postgres |
| ----------------------- | ------------: | ------------: | -----------: |
| preload                 |   227,086.468 |   298,956.892 |       0.760x |
| mixed                   |   122,696.232 |   218,291.419 |       0.562x |
| dedicated_deletes       |   104,176.431 |   217,215.991 |       0.480x |
| dedicated_inlining      |   196,803.814 |   242,437.724 |       0.812x |
| dedicated_compaction    |   113,014.962 |   107,773.499 |       1.049x |
| join_queries            |    10,715.817 |    67,449.746 |       0.159x |
| mutation_churn          |   929,773.345 |   970,739.473 |       0.958x |
| latest_queries          |     9,841.486 |    37,922.098 |       0.260x |
| time_travel_queries     |    10,188.120 |    38,208.113 |       0.267x |
| parallel_latest_queries |     9,354.603 |   214,745.286 |       0.044x |
| total                   | 1,737,859.433 | 2,419,786.459 |       0.718x |
