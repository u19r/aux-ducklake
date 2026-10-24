# DuckLake FDB Release Surface

This audit was generated during the correctness parity recovery pass. It lists the
`AUX_DUCKLAKE_*` variables used by scripts, Rust, and C++ sources, then classifies
which ones belong in the release surface.

## Release Config

These variables are part of the intended runtime or operator-facing release
contract.

- `AUX_DUCKLAKE_CATALOG_BACKEND`: selects `fdb`, `foundationdb`, or local backends.
- `AUX_DUCKLAKE_FDB_CLUSTER_FILE`: optional FoundationDB cluster file override.
- `AUX_DUCKLAKE_FDB_PREFIX`: FoundationDB key-prefix namespace for a catalog.
- `AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY`: explicit stable catalog identity when
  metadata path/database/schema should not determine the runtime catalog id.
- `AUX_DUCKLAKE_RUNTIME_LIBRARY`: C++ FFI runtime library path.

## Release Automation Config

These variables are acceptable in release/build scripts, but should stay out of
runtime code paths.

- `AUX_DUCKLAKE_RELEASE_EVIDENCE_DIR`
- `AUX_DUCKLAKE_RELEASE_EVIDENCE_ROOT`
- `AUX_DUCKLAKE_RELEASE_SKIP_STEPS`
- `AUX_DUCKLAKE_RELEASE_SCAN_FILES`
- `AUX_DUCKLAKE_RELEASE_UPSTREAM_MODE`
- `AUX_DUCKLAKE_RELEASE_VERSION`
- `AUX_DUCKLAKE_PACKAGE_PLATFORM`
- `AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD`
- `AUX_DUCKLAKE_RUST_SCCACHE`
- `AUX_DUCKLAKE_SKIP_FETCH`

## Benchmark Config

These variables belong only to benchmark scripts and benchmark documentation.

- `AUX_DUCKLAKE_BENCHMARK_BUILD_PROFILE`
- `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH`
- `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE`
- `AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT`
- `AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES`
- `AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT`
- `AUX_DUCKLAKE_POSTGRES_DSN`: Postgres comparison backend DSN for optional
  benchmark parity runs.
- `AUX_DUCKLAKE_REALISTIC_PARALLEL_WORKERS`
- `AUX_DUCKLAKE_REALISTIC_PRELOAD_BATCH_ROWS`
- `AUX_DUCKLAKE_REALISTIC_PRELOAD_WORKERS`
- `AUX_DUCKLAKE_REALISTIC_ROWS_PER_TABLE`
- `AUX_DUCKLAKE_REALISTIC_ROW_BYTES`
- `AUX_DUCKLAKE_REALISTIC_TABLES`
- `AUX_DUCKLAKE_REALISTIC_TARGET_BYTES`
- `AUX_DUCKLAKE_VARIED_CHURN_ROUNDS`
- `AUX_DUCKLAKE_VARIED_PARALLEL_WORKERS`
- `AUX_DUCKLAKE_VARIED_PRELOAD_BATCH_ROWS`
- `AUX_DUCKLAKE_VARIED_PRELOAD_WORKERS`
- `AUX_DUCKLAKE_VARIED_ROWS_PER_TABLE`
- `AUX_DUCKLAKE_VARIED_ROW_BYTES`
- `AUX_DUCKLAKE_VARIED_TABLES`
- `AUX_DUCKLAKE_VARIED_TARGET_BYTES`

## Test Harness Config

These variables are sidecar, e2e, upstream-test, or Rust-test controls. They
should remain isolated to test/support scripts and tests.

The sidecar binary and RocksDB backend are not part of the release runtime
contract. Keep them only as development/test accelerators while they preserve
useful local coverage. Release validation must prove the compiled FFI runtime
path with `AUX_DUCKLAKE_RUNTIME_LIBRARY`; release packages must not require
`AUX_DUCKLAKE_SIDECAR` or `AUX_DUCKLAKE_ROCKSDB_PATH`.

- `AUX_DUCKLAKE_ADD_COLUMNS`
- `AUX_DUCKLAKE_APPEND_FILES`
- `AUX_DUCKLAKE_CATALOG_ID`
- `AUX_DUCKLAKE_CHANGE_COLUMN_DEFAULTS`
- `AUX_DUCKLAKE_CHANGE_COLUMN_TYPES`
- `AUX_DUCKLAKE_CHANGE_COMMENTS`
- `AUX_DUCKLAKE_CHANGE_PARTITION_KEYS`
- `AUX_DUCKLAKE_CHANGE_SORT_KEYS`
- `AUX_DUCKLAKE_CHANGE_VIEW_COMMENT`
- `AUX_DUCKLAKE_CLEANUP_FILES`
- `AUX_DUCKLAKE_COLUMN_MAPPINGS`
- `AUX_DUCKLAKE_COLUMN_NAME`
- `AUX_DUCKLAKE_COMMIT_SNAPSHOT_ID`
- `AUX_DUCKLAKE_CREATE_MACROS`
- `AUX_DUCKLAKE_CREATE_SCHEMAS`
- `AUX_DUCKLAKE_CREATE_TABLES`
- `AUX_DUCKLAKE_CREATE_VIEWS`
- `AUX_DUCKLAKE_DELETE_FILES`
- `AUX_DUCKLAKE_DROPPED_DATA_FILES`
- `AUX_DUCKLAKE_DROP_COLUMNS`
- `AUX_DUCKLAKE_DROP_MACROS`
- `AUX_DUCKLAKE_DROP_SCHEMAS`
- `AUX_DUCKLAKE_DROP_TABLES`
- `AUX_DUCKLAKE_DROP_VIEWS`
- `AUX_DUCKLAKE_E2E_REAL_SIDECAR`
- `AUX_DUCKLAKE_E2E_SCENARIO`
- `AUX_DUCKLAKE_E2E_SIDECAR_BIN`
- `AUX_DUCKLAKE_E2E_SKIP_DUCKLAKE_BUILD`
- `AUX_DUCKLAKE_E2E_SKIP_SIDECAR_BUILD`
- `AUX_DUCKLAKE_END_SNAPSHOT_ID`
- `AUX_DUCKLAKE_FDB_LIVE`
- `AUX_DUCKLAKE_FDB_SOAK_ITERATIONS`
- `AUX_DUCKLAKE_FEATURE_E2E_SCENARIO`
- `AUX_DUCKLAKE_FLUSHED_INLINE`
- `AUX_DUCKLAKE_INLINED_TABLE_NAME`
- `AUX_DUCKLAKE_INLINE_CLEANUP_PAYLOADS`
- `AUX_DUCKLAKE_INLINE_DELETES`
- `AUX_DUCKLAKE_INLINE_DELETE_ROWS`
- `AUX_DUCKLAKE_INLINE_FILE_DELETES`
- `AUX_DUCKLAKE_INLINE_FIRST_ROWS`
- `AUX_DUCKLAKE_INLINE_PRELOAD_ROWS`
- `AUX_DUCKLAKE_INLINE_PRELOAD_TABLES`
- `AUX_DUCKLAKE_INLINE_ROWS`
- `AUX_DUCKLAKE_INLINE_SECOND_ROWS`
- `AUX_DUCKLAKE_INLINE_SPLIT_STEPS`
- `AUX_DUCKLAKE_INLINE_TABLES`
- `AUX_DUCKLAKE_MERGE_ADJACENT`
- `AUX_DUCKLAKE_NESTED_DDL_E2E_SCENARIO`
- `AUX_DUCKLAKE_PARTITION_KEY_INDEX`
- `AUX_DUCKLAKE_PARTITION_VALUE`
- `AUX_DUCKLAKE_READ_SNAPSHOT_ID`
- `AUX_DUCKLAKE_RENAME_COLUMNS`
- `AUX_DUCKLAKE_RENAME_TABLES`
- `AUX_DUCKLAKE_RENAME_VIEWS`
- `AUX_DUCKLAKE_REWRITE_DELETE`
- `AUX_DUCKLAKE_ROCKSDB_PATH`
- `AUX_DUCKLAKE_SIDECAR`
- `AUX_DUCKLAKE_SNAPSHOT_ID`
- `AUX_DUCKLAKE_SNAPSHOT_IDS`
- `AUX_DUCKLAKE_SNAPSHOT_OLDER_THAN_MICROS`
- `AUX_DUCKLAKE_START_MAPPING_ID`
- `AUX_DUCKLAKE_START_SNAPSHOT_ID`
- `AUX_DUCKLAKE_TABLE_ID`
- `AUX_DUCKLAKE_TABLE_NAME`
- `AUX_DUCKLAKE_TEST_FDB_COMMIT_DATA_MUTATION_FAULT`
- `AUX_DUCKLAKE_TEST_OBJECT_STORAGE_DELAY_MS`
- `AUX_DUCKLAKE_UPSTREAM_ARTIFACT_DIR`
- `AUX_DUCKLAKE_UPSTREAM_GAP_SUMMARY`
- `AUX_DUCKLAKE_VALIDATE_MIRROR_KEYS`

## Release Build Commands

Local release builds use the dedicated release scripts instead of the debug/e2e
build path. Linux release builds are always produced inside a Red Hat UBI9
Docker container so local workstation libraries do not shape the Linux artifact:

```sh
just ducklake-release
just ducklake-build-release-local
just ducklake-package-release
just ducklake-build-linux-amd64
just ducklake-build-linux-arm64
```

`just ducklake-release` is the local/CI entrypoint. It delegates to
`scripts/release.sh`, which in turn calls the same per-platform build/package
scripts listed above. On macOS arm64 it builds `macos-arm64`, `linux-amd64`,
and `linux-arm64`; on other hosts pass explicit platform arguments and build
macOS arm64 on a macOS arm64 runner.

The package script emits artifacts named:

```text
artifacts/aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz
artifacts/aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz.sha256
```

The package contains:

- `opt/duckdb/duckdb`
- `opt/duckdb/duckdb.h` (C API v1)
- `opt/duckdb/duckdb_v2.h` (C API v2)
- `opt/duckdb/libduckdb*`
- `opt/duckdb/libcurl.so*` on Linux (curl 8.23.0 for HTTPFS)
- `opt/aux-ducklake/lib/libducklake_catalog.{so,dylib}`
- `opt/aux-ducklake/lib/libfdb_c*` when the platform package manager provides
  the FoundationDB client library at build time

The DuckLake extension is statically linked into the DuckDB build, following the
`../aux-duckdb` release contract. Release packages intentionally do not include
`*.duckdb_extension` files.

Default release smoke tests run the compiled runtime without optional runtime
metrics. Set `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH` explicitly only for a
benchmark or metrics diagnostic run.

Release packages also intentionally omit the development sidecar binary and
RocksDB catalog state. Those paths can remain in tests until equivalent
compiled-runtime coverage replaces them, but they must not define release
readiness.

Required release platforms:

- `linux-amd64`
- `linux-arm64`
- `macos-arm64`

macOS x86_64 is not a required release target. Add it only if a
consumer is identified and the build is covered by the same release entrypoints.

## DuckDB 2.0 / DuckLake 1.1 integration

The release pins live in `scripts/ducklake-upstream-pins.sh`. DuckLake has no release tag;
the pin is the commit selected by DuckDB v2.0.0's extension configuration. The shared host
reports `v2.0.0` even when the source checkout is shallow. Both `duckdb.h` and `duckdb_v2.h`
are packaged; existing consumers continue to use the v1 C API.

`just ducklake-release-smoke build/release/macos-arm64` verifies the extracted shared
library, built-in extensions, the 1.1 SQL catalog and FoundationDB catalog, inline
updates/deletes, flush history, schema evolution, nested schemas, sorted Parquet output,
encrypted analytics-style writes/deletes, and detach/reattach. `scripts/release.sh` runs
this proof for macOS. Linux container builds
start an isolated FoundationDB server and run the same proof before exporting packages.
The test clears only its unique FoundationDB prefix. It does not reset a shared database.
It also serves a local Parquet file over HTTP and reads it with the curl client.
Linux builds use Clang and package a checksum-pinned curl 8.23.0 shared library,
because the new HTTPFS code requires APIs absent from UBI 9’s system curl headers.

Update analytics pins with an explicit linked worktree and release tag:

```sh
./scripts/update-aux-repos.sh /path/to/aux-analytics-worktree <published-release-tag>
```

Use a published release tag with verified assets for every required platform. For an
unpublished build, configure the analytics downloader with the custom bundle URL and
SHA-256 inputs. The Rust binding must match the DuckDB revision in the bundle.

Existing catalogs created by the previous development pin can report `1.1-dev1`.
Use `AUTOMATIC_MIGRATION TRUE` explicitly when attaching those catalogs with the new
library; analytics already sets this option. The upgrade proof creates the catalog with
the old native binaries, reopens it
with the new binaries, then checks inline/file reads, nested schemas, added defaults,
flush, historical reads, and a second reopen. Version-1 schema rows remain readable
because they are an existing persisted format.

The upgrade performance report is [DuckDB 2.0 upgrade](../benchmarks/duckdb-2-upgrade.md).

### Complete a release draft

The Linux workflow creates a draft and verifies the release tag identifies the built
commit. It never replaces published assets. Before publishing, build macOS from a clean
checkout of the same tag, run `scripts/release.sh macos-arm64`, and retain the successful
shared-library smoke receipt with the tag commit and artifact SHA-256. Compare
`git rev-parse HEAD` with `git rev-parse "$tag^{commit}"` before attaching it.

```sh
# Run from the tagged macOS checkout after its release command succeeds.
(cd artifacts && shasum -a 256 -c *-macos-arm64.tar.gz.sha256)
# The tag must still identify the recorded Linux and macOS source commit.
gh release view "$tag" --json isDraft --jq .isDraft  # must print true
gh release upload "$tag" artifacts/*-macos-arm64.tar.gz artifacts/*-macos-arm64.tar.gz.sha256
gh release view "$tag" --json assets --jq '.assets[].name'
```

Verify exactly one archive and matching checksum for each required platform. Download
all three archives from the draft and verify their checksums before promotion. Publish
only after all platform smoke receipts identify that tag's source and package hashes:
`gh release edit "$tag" --draft=false`. Publishing is a separate release action; local
builds and this dependency update do not authorize it.

### Known support limits

FoundationDB view-column comments/tags introduced in DuckLake 1.1 are rejected before
commit. Whole-view comments retain their existing support.

Historical reads after DELETE and inline flush in the *same transaction* fail in the
old base as well as this upgrade. Current rows remain correct. Commit deletes before
flushing when historical snapshots are required. The non-gating regression fixture is
`scripts/test-fixtures/inline-delete-flush-time-travel.test`; it expects the correct
historical result and remains a known failure. The release smoke separately requires
historical correctness when deletes commit before flush.

Changing the partition specification while old inline rows remain also fails flush in
the old base and in this upgrade. Flush existing inline rows before changing the
partition specification. The non-gating regression fixture is
`scripts/test-fixtures/inline-partition-evolution.test`.
