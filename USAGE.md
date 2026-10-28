# Usage

Use the packaged DuckDB shell or shared library for normal use. DuckLake is compiled in;
there is no separate extension to install. The FoundationDB catalog is opt-in through
`META_TYPE 'aux_catalog'`. Regular DuckLake attaches still use upstream metadata backends.

## Install and connect

Verify and unpack a release archive from `artifacts/`:

```sh
archive='aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz'
(cd artifacts && shasum -a 256 -c "$archive.sha256")  # Or use sha256sum -c
install_root="$PWD/aux-ducklake-install"
mkdir -p "$install_root"
tar -C "$install_root" -xzf "artifacts/$archive"
```

The archive contains the DuckDB shell, C headers, shared library, and matching
`libducklake_catalog` runtime under `opt/`. Linux packages also contain HTTPFS's `libcurl`.
The FoundationDB client library is included only when available during packaging;
otherwise install the FoundationDB client package on the host.

Configure the runtime before starting DuckDB. On Linux:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='prod/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$install_root/opt/aux-ducklake/lib/libducklake_catalog.so"
export LD_LIBRARY_PATH="$install_root/opt/aux-ducklake/lib:$install_root/opt/duckdb:${LD_LIBRARY_PATH:-}"
"$install_root/opt/duckdb/duckdb"
```

On macOS, use `libducklake_catalog.dylib` and `DYLD_LIBRARY_PATH` in place of
`LD_LIBRARY_PATH`. The FoundationDB client library must be loadable by the process.
To override the system's default cluster file, also set:

```sh
export AUX_DUCKLAKE_FDB_CLUSTER_FILE='/etc/foundationdb/fdb.cluster'
```

Use a unique prefix ending in `/` for each catalog, including staging, tests, and restore
targets. An unset prefix falls back to `dl/`; shared, benchmark, test, and production
catalogs must set an explicit prefix. Set `AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY` only to
override the stable identity normally derived from the metadata path, database, and schema.

This build uses tuple-encoded FoundationDB keys for server-side index joins and a new
snapshot-index format. Create catalogs under a fresh prefix. Catalogs written with earlier
formats require recreation; this build does not provide a layout migration.

Attach and verify reads and writes. Replace these absolute paths with writable locations:

```sql
LOAD ducklake;
ATTACH 'ducklake:/var/lib/aux-ducklake/example/metadata.duckdb' AS dl (
    DATA_PATH '/var/lib/aux-ducklake/example/data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);
CREATE TABLE dl.main.usage_smoke(id INTEGER, value VARCHAR);
INSERT INTO dl.main.usage_smoke VALUES (1, 'ok');
SELECT * FROM dl.main.usage_smoke ORDER BY id;
DROP TABLE dl.main.usage_smoke;
```

With `aux_catalog`, metadata lives in FoundationDB under the configured prefix; the
metadata path remains part of the attach syntax and catalog identity. Data files live
under `DATA_PATH`. Backups must cover both the catalog metadata and data-file storage.
DuckLake writes immutable files; compaction replaces files rather than updating them in place.

`DATA_INLINING_ROW_LIMIT 0` disables data inlining. Set a positive limit, such as `100`,
on attach to enable it.

## Embedded host provenance

Embed with the packaged `duckdb.h` and `libduckdb*`, using the same environment and library
search paths as the shell. The runtime library path is fixed after the first FFI operation;
start a fresh process to change it.

The host library, patched DuckLake extension, and `libducklake_catalog` must come from the
same aux-ducklake build. For a standalone extension, link the sibling
`third_party/ducklake/build/debug/src/libduckdb.{dylib,so}`. Verify the process actually maps
that library, then complete a FoundationDB `ATTACH ... META_TYPE 'aux_catalog'` read/write
probe. A successful `LOAD` alone does not prove the aux catalog is available.

Stock DuckDB hosts and the upstream DuckLake build in the aux-duckdb bundle do not provide
the patched catalog manager. ABI compatibility alone does not establish aux-catalog support.

## Build and development

Run repository build commands from the repository root. Before local Cargo builds, follow
the [Cargo target-slot guide](../aux-infra/docs/cargo-target-slots.md).

| Command | Output |
| --- | --- |
| `just ducklake-release` | Default release archives under `artifacts/` |
| `just ducklake-release linux-arm64` | One platform archive; also supports `linux-amd64` and `macos-arm64` |
| `just ducklake-build-release` | Local release shell and shared library under `third_party/ducklake/build/release/` |
| `just ducklake-fetch` then `just ducklake-build` | Matching debug shell and standalone extension |

For standalone-extension development, also build the FoundationDB runtime:

```sh
./scripts/cargo_with_sccache.sh build -p ducklake-catalog \
  --no-default-features --features foundationdb
```

The runtime is in `$CARGO_TARGET_DIR/debug/` (or `target/debug/` when no target override is
set). Use its absolute path as `AUX_DUCKLAKE_RUNTIME_LIBRARY`, with the environment from
[Install and connect](#install-and-connect), then start:

```sh
third_party/ducklake/build/debug/duckdb
```

In that shell, `LOAD ducklake;` uses the compiled-in extension. A matching embedded host
can instead load
`third_party/ducklake/build/debug/extension/ducklake/ducklake.duckdb_extension` by absolute
path. Standalone loading also depends on the host's signing and extension-loading policies.
For performance measurements, use the release build and release runtime.

Verification commands:

```sh
just ducklake-runtime-cpp-ffi-smoke
just ducklake-fdb-runtime-smoke
just ducklake-catalog-fdb-test
just ducklake-fdb-release-gate  # Required before publishing artifacts
```

See the [release surface](docs/runbooks/ducklake-fdb-release-surface.md) for packaging details
and `just --summary` for all commands.

## Transaction snapshot fences

A FoundationDB catalog transaction can require its catalog snapshot to remain unchanged
until commit. Enable the fence before the mutations it protects:

```sql
CREATE TABLE dl.main.items(id INTEGER, name VARCHAR);
BEGIN;
CALL ducklake_set_transaction_fence('dl');
INSERT INTO dl.main.items VALUES (3, 'gamma');
COMMIT;
```

If another transaction advances the snapshot, the fenced commit fails with a snapshot-fence
conflict and publishes no partial mutation. Retry the complete transaction against a fresh
snapshot. Fenced transactions support atomic data and inline-row mutations; they reject
schema changes, configuration changes, and maintenance operations.

## Regular DuckLake metadata

For an upstream DuckDB-backed catalog, leave the aux environment variables unset and omit
`META_TYPE 'aux_catalog'`:

```sql
LOAD ducklake;
ATTACH 'ducklake:/tmp/ducklake-metadata.duckdb' AS dl (
    DATA_PATH '/tmp/ducklake-data'
);
```

The aux runtime library is unnecessary for this mode. For Postgres metadata, use the
[upstream configuration](https://ducklake.select/docs/stable/duckdb/usage/connecting).


## Troubleshooting

| Symptom | Check |
| --- | --- |
| Runtime library missing or cannot load | Use an absolute path to the matching runtime; `.so` on Linux, `.dylib` on macOS. Check dependent libraries. |
| Runtime library changed after loading | Start a fresh DuckDB process. |
| FoundationDB connection failure | Check the client library, cluster file, and catalog-owned prefix. |
| `aux_catalog` is not recognized | Verify the mapped host library comes from the patched build and complete the attach probe described above. |
| Standalone extension fails to load | Use its matching debug shell or packaged release shell; check ABI and loading policy. |
| `LOAD ducklake` downloads an extension | Use the packaged shell or `third_party/ducklake/build/{debug,release}/duckdb`. |

Runtime metrics require a runtime built with the `runtime-metrics` Cargo feature. Set
`AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH` to record them for diagnostics.
