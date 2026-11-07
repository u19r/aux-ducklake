# aux-ducklake

FoundationDB catalog support for DuckLake and DuckDB. Catalog metadata is stored in FoundationDB;
data files remain in object storage or the configured filesystem `DATA_PATH`.

The repository contains:

- `ducklake-catalog`, the Rust catalog runtime and C-compatible DuckLake bridge.
- FoundationDB tests, simulations, and benchmarks.
- The maintained patch under `patches/ducklake/` that adds the aux catalog metadata manager to
  upstream DuckLake.

For binary and extension setup, see [USAGE.md](USAGE.md).

Embedded consumers must use the matching patched DuckDB host, DuckLake extension, and
`libducklake_catalog` runtime described in [USAGE.md](USAGE.md#embedded-host-provenance). A
`LOAD`-only check on stock DuckDB is not an aux-catalog proof; the current aux-duckdb bundle uses
upstream DuckLake and is not a valid `META_TYPE 'aux_catalog'` host.

## Repository Layout

- `crates/ducklake-catalog/`: catalog runtime, FoundationDB backend, FFI, and utilities.
- `crates/ducklake-fdb-sim-*/`: deterministic FoundationDB simulation components.
- `patches/ducklake/`: maintained patch applied to the pinned DuckLake source.

## Prerequisites

- Rust 1.99.1 or newer.
- FoundationDB client tools and library.
- `cmake`, `git`, and either `ninja` or `make` to build the patched DuckLake extension.

## Building

```sh
cargo check --workspace --all-targets
cargo test --workspace --lib --bins
```

Tests that talk to FoundationDB need a reachable cluster. Every shared, staging, production,
benchmark, and test catalog must set a distinct `AUX_DUCKLAKE_FDB_PREFIX`. The runtime otherwise
defaults to `dl/`. Set `AUX_DUCKLAKE_FDB_CLUSTER_FILE` when the default FoundationDB cluster file is
not the intended cluster.

## References

- User setup: [USAGE.md](USAGE.md)
- Benchmark process and baseline: [BENCHMARK.md](BENCHMARK.md)
