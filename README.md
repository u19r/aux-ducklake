# aux-ducklake

FoundationDB catalog support for DuckLake and DuckDB. This repository builds a DuckLake runtime
that stores catalog metadata in FoundationDB while keeping DuckLake data files in object storage or
the configured filesystem `DATA_PATH`.

Contents:

- `ducklake-catalog`, a Rust catalog runtime compiled as both a Rust library and a C-compatible
  dynamic library for the patched DuckLake/DuckDB bridge.
- FoundationDB live tests, simulation workloads, recovery drills, and release gates.
- Scripts for fetching the pinned upstream DuckLake source, applying the aux catalog bridge patch,
  building FoundationDB-capable DuckDB/DuckLake binaries, packaging release artifacts, and running
  benchmarks.

The aux runtime owns catalog metadata in FoundationDB under one catalog-specific prefix. If
`AUX_DUCKLAKE_FDB_PREFIX` is not set, the runtime uses `dl/`.

For end-user setup, see [USAGE.md](USAGE.md). It covers both supported consumption modes: the
standalone loadable DuckLake extension and the packaged DuckDB binary with DuckLake compiled in.

## Repository Layout

- `crates/ducklake-catalog`: Rust catalog runtime, FFI protocol, FoundationDB backend, benchmarks,
  and prefix-copy utility.
- `crates/ducklake-fdb-sim-model`, `crates/ducklake-fdb-sim-runner`, and
  `crates/ducklake-fdb-sim-workload`: FoundationDB simulation model, runner, and workloads.
- `scripts/`: build, release, parity, benchmark, and operational validation scripts.
- `docker/`: Linux release build container definitions. Linux builds use a Red Hat UBI9 base.
- `patches/ducklake/`: patch applied to the pinned upstream DuckLake checkout.
- `docs/runbooks/`: runtime and release-surface runbooks.
- `docs/parity/ducklake-fdb/`: parity scenarios, SQL fixtures, and expected output.
- `third_party/ducklake`: generated local checkout created by `just ducklake-fetch`.

## Prerequisites

- Rust `1.96.0` or newer compatible with this workspace.
- `just`, `cmake`, `git`, and either `ninja` or `make`.
- FoundationDB client tools and client library for FDB tests and release runtime work.
- Docker Buildx for Linux release artifacts.
- macOS developers can run `just setup` to install common Homebrew dependencies.

## Quick Start

```sh
just setup
just ducklake-fetch
just ducklake-catalog-check
just ducklake-runtime-cpp-ffi-smoke
```

`just ducklake-fetch` clones the pinned DuckLake source into `third_party/ducklake`, checks out the
commit recorded in `scripts/fetch_ducklake.sh`, initializes submodules, and applies
`patches/ducklake/0001-add-aux-catalog-metadata-manager.patch`.

Use `just --summary` to list every available command. The `justfile` is the primary command index.

## Development

Common Rust development commands:

```sh
just ducklake-catalog-check
just ducklake-catalog-test
just ducklake-catalog-tests-check
just ducklake-catalog-evolution-cleanup-test
just ducklake-catalog-evolution-cleanup-one <test-name>
```

Build the local debug DuckLake/DuckDB integration:

```sh
just ducklake-build
just ducklake-build-local
```

`just ducklake-build` fetches/patches DuckLake if needed and builds the debug integration. Use
`just ducklake-build-local` when `third_party/ducklake` is already present and should not be
refetched. Set `AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1` when running scripts that may reuse an existing
patched DuckLake build.

The Rust runtime can be built directly:

```sh
./scripts/cargo_with_sccache.sh build -p ducklake-catalog
./scripts/cargo_with_sccache.sh build -p ducklake-catalog --no-default-features --features foundationdb
```

For FDB-backed DuckDB runtime work, set:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='dev/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$PWD/target/debug/libducklake_catalog.dylib"
```

`AUX_DUCKLAKE_FDB_PREFIX` defaults to `dl/` when unset. Set it explicitly for every shared,
staging, production, benchmark, or test catalog so unrelated catalogs do not share the default
prefix.

Use `.so` instead of `.dylib` on Linux. Set `AUX_DUCKLAKE_FDB_CLUSTER_FILE` when the default local
FoundationDB cluster file is not the intended cluster.

## Build And Package

Debug build commands:

```sh
just ducklake-fetch
just ducklake-build
just ducklake-build-local
```

Release build commands:

```sh
just ducklake-build-release
just ducklake-build-release-local
just ducklake-package-release
just ducklake-release
```

Linux release artifacts are built inside the UBI9 Docker container:

```sh
just ducklake-build-linux-amd64
just ducklake-build-linux-arm64
```

`just ducklake-release` is the combined release entrypoint. With no platform argument, it builds
Linux amd64, Linux arm64, and macOS arm64 when running on a macOS arm64 host. Pass an explicit
platform to build one target:

```sh
just ducklake-release linux-amd64
just ducklake-release linux-arm64
just ducklake-release macos-arm64
```

Release packages are written to `artifacts/` as:

```text
aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz
aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz.sha256
```

Each package contains the combined DuckDB binary, `duckdb.h`, `libduckdb*`, the aux
`libducklake_catalog` runtime library, and FoundationDB client libraries when available to the
platform package build. Release packages intentionally omit standalone extension files and other
development-only artifacts.

See `docs/runbooks/ducklake-fdb-release-surface.md` for the release configuration surface and
artifact contract.

## Release Process

1. Fetch and patch the pinned upstream DuckLake source:

   ```sh
   just ducklake-fetch
   ```

2. Run local catalog and runtime checks:

   ```sh
   just ducklake-catalog-check
   just ducklake-runtime-protocol-check
   just ducklake-runtime-ffi-check
   just ducklake-runtime-cpp-ffi-smoke
   ```

3. Run FDB validation against a local or staging FoundationDB cluster:

   ```sh
   just ducklake-catalog-fdb-test
   just ducklake-fdb-runtime-smoke
   just ducklake-fdb-concurrency
   just ducklake-fdb-chaos
   just ducklake-fdb-prefix-clone-drill
   ```

4. Run the release gate:

   ```sh
   just ducklake-fdb-release-gate
   ```

5. Build and package release artifacts:

   ```sh
   just ducklake-release
   ```

6. Run release-shape benchmarks when performance evidence is required:

   ```sh
   env \
     -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH \
     -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES \
     -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE \
     -u AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT \
     AUX_DUCKLAKE_BENCHMARK_BACKEND=fdb \
     AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT=100 \
     AUX_DUCKLAKE_BENCHMARK_BUILD_PROFILE=release \
     ./scripts/ducklake_catalog_benchmark.sh varied
   ```

## Test Processes

Unit and integration tests:

```sh
just ducklake-catalog-test
just ducklake-catalog-check
just ducklake-catalog-tests-check
```

Targeted catalog evolution and cleanup tests:

```sh
just ducklake-catalog-evolution-cleanup-test
just ducklake-catalog-evolution-cleanup-one <test-name>
just ducklake-catalog-evolution-cleanup-dev-loop
```

FoundationDB live tests and fault-style checks:

```sh
just ducklake-catalog-fdb-test
just ducklake-fdb-concurrency
just ducklake-fdb-chaos
just ducklake-fdb-soak
```

FoundationDB simulation:

```sh
just ducklake-fdb-sim-smoke
just ducklake-fdb-sim-multiclient
```

Runtime bridge and recovery drills:

```sh
just ducklake-runtime-protocol-check
just ducklake-runtime-ffi-check
just ducklake-runtime-cpp-ffi-smoke
just ducklake-fdb-runtime-smoke
just ducklake-fdb-prefix-clone-drill
```

Upstream DuckLake compatibility checks for the FoundationDB catalog:

```sh
just ducklake-upstream-fdb-smoke
just ducklake-upstream-fdb-full
just ducklake-upstream-fdb-slow
```

Optional Postgres comparison scripts remain available for development parity investigations. They
are not part of the release package or the default FoundationDB operator path, and require
`pg_config`, `libpq`, a reachable Postgres database, and the DuckDB `postgres_scanner` helper
extension:

```sh
just ducklake-parity-postgres-smoke
just ducklake-parity-postgres-fdb-core
just ducklake-parity-postgres-fdb-mutations
just ducklake-parity-postgres-fdb-ddl
just ducklake-parity-postgres-fdb-inline
just ducklake-upstream-postgres-smoke
just ducklake-upstream-postgres-full
just ducklake-upstream-postgres-slow
```

Use `AUX_DUCKLAKE_RELEASE_SKIP_STEPS` only for local iteration when a previous release-gate step is
known to have passed and the skipped surface is not affected by the current change.

## Benchmarks

Fast benchmark commands:

```sh
just ducklake-catalog-benchmark-smoke
just ducklake-catalog-benchmark-scan10
just ducklake-catalog-benchmark-profile
just ducklake-catalog-benchmark-profile 1000
```

Larger FoundationDB catalog workloads:

```sh
just ducklake-catalog-benchmark-realistic
just ducklake-catalog-benchmark-varied
```

Release benchmark binaries:

```sh
just ducklake-release-benchmark-smoke
just ducklake-release-benchmark-profile
just ducklake-release-benchmark-profile-tiny
```

Benchmark JSON summaries live in `docs/benchmarks/ducklake-fdb-feature-parity/`. The latest full
varied baseline is summarized in `BENCHMARK.md`. Treat benchmark timings as local trend signals, not
external service sizing claims.

Set `AUX_DUCKLAKE_BENCHMARK_BACKEND=both` and `AUX_DUCKLAKE_POSTGRES_DSN` only when running optional
same-SQL comparisons against DuckLake's Postgres catalog backend. Compare semantic labels and row
counts before using timing deltas to choose optimization work.

Optional runtime metrics are disabled for release-shape benchmarks unless explicitly needed for a
diagnostic run. Enable them with `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH` and scope labels with
`AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE`.

See `docs/benchmarks/ducklake-fdb-feature-parity/README.md` for fixture shape and scaling knobs.

## Upgrading The DuckLake Base

The upstream DuckLake checkout is not vendored. It is reproduced from the pin and patch.

To upgrade:

1. Choose the upstream DuckLake commit.
2. Update `DUCKLAKE_COMMIT` in `scripts/fetch_ducklake.sh`.
3. Remove or refresh `third_party/ducklake`.
4. Reapply and update `patches/ducklake/0001-add-aux-catalog-metadata-manager.patch` so the aux
   catalog metadata-manager bridge applies cleanly.
5. Run:

   ```sh
   just ducklake-fetch
   just ducklake-build
   just ducklake-runtime-cpp-ffi-smoke
   just ducklake-upstream-disabled-diff
   just ducklake-upstream-full-gap-summary
   just ducklake-fdb-release-gate
   ```

6. Refresh parity fixtures or expected upstream gap evidence only when the upstream behavior has
   intentionally changed.

The fetch script writes marker files under `third_party/ducklake` for the pinned commit and patch
hash. If a patch no longer applies, fix the patch rather than editing generated files in
`third_party/ducklake` as the source of truth.

## Operations References

- FoundationDB runtime and backup/restore: `docs/runbooks/ducklake-fdb-runtime.md`
- Release surface and package contract: `docs/runbooks/ducklake-fdb-release-surface.md`
- Benchmark process and fixture details: `docs/benchmarks/ducklake-fdb-feature-parity/README.md`
- User-facing binary and extension usage: `USAGE.md`
- Current benchmark baseline: `BENCHMARK.md`

Use `just ducklake-cache-stats` to inspect local `sccache`/`ccache` state when build times change.
