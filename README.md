# aux-ducklake

FoundationDB catalog support for DuckLake and DuckDB. Catalog metadata is stored in FoundationDB;
data files remain in object storage or the configured filesystem `DATA_PATH`.

The repository contains:

- `ducklake-catalog`, the Rust catalog runtime and C-compatible DuckLake bridge.
- FoundationDB tests, simulations, recovery drills, benchmarks, and release gates.
- A reproducible upstream DuckLake/DuckDB checkout generated from pinned commits and the maintained
  patch under `patches/ducklake/`.

For binary and extension setup, see [USAGE.md](USAGE.md).

## Repository Layout

- `crates/ducklake-catalog/`: catalog runtime, FoundationDB backend, FFI, and utilities.
- `crates/ducklake-fdb-sim-*/`: deterministic FoundationDB simulation components.
- `scripts/`: build, validation, release, benchmark, and operational scripts.
- `patches/ducklake/`: maintained patch applied to the pinned DuckLake source.
- `third_party/ducklake/`: generated checkout created by `just ducklake-fetch`.
- `docs/runbooks/`: runtime and release runbooks.

## Prerequisites

- Rust 1.96.0 or newer.
- `just`, `cmake`, `git`, and either `ninja` or `make`.
- FoundationDB client tools and library for FoundationDB tests and runtime work.
- PostgreSQL, including `pg_config`, for parity tests, upstream PostgreSQL tests, and the release gate.
- Docker Buildx for Linux release artifacts.

On macOS, `just setup` installs the common Homebrew dependencies.

## Quick Start

```sh
just setup
just ducklake-fetch
just ducklake-catalog-check
just ducklake-runtime-cpp-ffi-smoke
```

`just ducklake-fetch` checks out the DuckLake and DuckDB commits pinned in
`scripts/fetch_ducklake.sh` and applies
`patches/ducklake/0001-add-aux-catalog-metadata-manager.patch`. Treat
`third_party/ducklake/` as generated output.

## Development And Validation

Use the repository commands rather than invoking upstream build tools directly:

```sh
just style-check
just ducklake-catalog-check
just ducklake-catalog-test
just ducklake-build
just ducklake-runtime-cpp-ffi-smoke
```

The production acceptance command is `just ducklake-fdb-release-gate`. Use `just --summary` to list
focused, simulation, chaos, soak, parity, and benchmark commands.

Every shared, staging, production, benchmark, and test catalog must set a distinct
`AUX_DUCKLAKE_FDB_PREFIX`. The runtime otherwise defaults to `dl/`. Set
`AUX_DUCKLAKE_FDB_CLUSTER_FILE` when the default FoundationDB cluster file is not the intended
cluster.

## Release

Run `just ducklake-fdb-release-gate`, then `just ducklake-release` (or pass `linux-amd64`,
`linux-arm64`, or `macos-arm64`). Packages and SHA-256 files are written under `artifacts/`. See
`docs/runbooks/ducklake-fdb-release-surface.md` for the artifact contract.

Benchmark methodology and the current baseline live in `BENCHMARK.md`; generated benchmark
artifacts are written under `docs/benchmarks/ducklake-fdb-feature-parity/`.

## Upgrading The DuckLake Base

The upstream checkout is generated from the DuckLake and DuckDB pins plus the aux bridge patch.

1. Update `DUCKLAKE_COMMIT` and `DUCKDB_COMMIT` in `scripts/fetch_ducklake.sh`. Advancing DuckDB
   beyond DuckLake's submodule pin may require API compatibility changes.
2. Rebase `patches/ducklake/0001-add-aux-catalog-metadata-manager.patch` onto the DuckLake pin.
3. Run:

   ```sh
   just ducklake-fetch
   just ducklake-build
   just ducklake-runtime-cpp-ffi-smoke
   just ducklake-upstream-disabled-diff
   just ducklake-upstream-postgres-full
   just ducklake-upstream-fdb-full
   just ducklake-upstream-full-gap-summary
   just ducklake-fdb-release-gate
   ```

   The gap summary consumes the two full-suite summaries generated immediately before it.

4. Refresh parity fixtures or expected gap evidence only for an intentional upstream behavior
   change.

The fetch script records both pins and the patch hash under `third_party/ducklake/` and replaces a
stale generated checkout.

## References

- Runtime and backup/restore: `docs/runbooks/ducklake-fdb-runtime.md`
- Release surface: `docs/runbooks/ducklake-fdb-release-surface.md`
- User setup: `USAGE.md`
- Benchmark process and baseline: `BENCHMARK.md`
