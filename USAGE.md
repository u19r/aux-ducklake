# Usage

Run aux-ducklake in either supported runtime shape:

- A full packaged DuckDB build with the repo's DuckDB extension set compiled in.
- A standalone loadable DuckLake extension for a compatible host DuckDB build.

FoundationDB stores catalog metadata under the prefix selected by `AUX_DUCKLAKE_FDB_PREFIX`.
When the variable is unset, the runtime uses `dl/`.

For normal use, prefer the packaged full DuckDB build. DuckLake is compiled into DuckDB, and no `ducklake.duckdb_extension` file is required at runtime. Use the standalone extension only when you control the host DuckDB build and can keep its ABI matched to the extension.

The aux FoundationDB catalog is opt-in. A patched DuckLake build behaves like regular DuckLake until
you attach with `META_TYPE 'aux_catalog'` and provide the aux runtime environment.

## Pick A Runtime Shape

Use the packaged full DuckDB build when:

- You want the public release artifact.
- You want `LOAD ducklake;` to resolve the compiled-in DuckLake extension.
- You do not want to deploy, sign, download, or load a separate DuckDB extension file.
- You want the matching `libducklake_catalog` runtime packaged beside the DuckDB shell and library.
- You embed DuckDB through `duckdb.h` and `libduckdb*` and want the same compiled-extension build.
- You want the least ABI-sensitive installation path.

Use the standalone loadable extension when:

- You specifically need a `ducklake.duckdb_extension` file.
- You are doing local development, integration testing, or controlled embedding.
- You can run the extension with the exact DuckDB build that produced it, or another known-compatible
  DuckDB binary.
- You accept that extension loading may be blocked by host policy, signature policy, or ABI drift.

Use upstream DuckLake metadata backends when:

- You do not want the aux FoundationDB catalog.
- You do not set `META_TYPE 'aux_catalog'`.
- You leave the aux FoundationDB runtime environment variables unset.

## Runtime Requirements

The FoundationDB catalog path needs:

- The patched aux-ducklake DuckLake extension, either compiled into DuckDB or loadable.
- The matching `libducklake_catalog` Rust FFI runtime built with FoundationDB support.
- A FoundationDB client library loadable by the DuckDB process.
- A FoundationDB cluster file, either the system default or `AUX_DUCKLAKE_FDB_CLUSTER_FILE`.
- One unique FoundationDB prefix per logical DuckLake catalog.

Set these variables before starting DuckDB when using `META_TYPE 'aux_catalog'`:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='prod/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY='/absolute/path/to/libducklake_catalog.so'
```

Use `libducklake_catalog.dylib` on macOS. Set the cluster file only when the process should not use
the system default FoundationDB cluster file:

```sh
export AUX_DUCKLAKE_FDB_CLUSTER_FILE='/etc/foundationdb/fdb.cluster'
```

`AUX_DUCKLAKE_FDB_PREFIX` must end in `/`. Do not share one prefix between production, staging,
tests, restore targets, or unrelated catalogs. If it is unset, the runtime falls back to `dl/`;
set an explicit value for shared or long-lived catalogs so they do not collide with that default.
Set `AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY` only when the metadata path, database, and schema should
not define the stable catalog identity.

## Release Artifacts

Release archives are written under `artifacts/`:

```text
aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz
aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz.sha256
```

The archive contains:

```text
opt/duckdb/duckdb
opt/duckdb/duckdb.h
opt/duckdb/libduckdb*
opt/aux-ducklake/lib/libducklake_catalog.{so,dylib}
opt/aux-ducklake/lib/libfdb_c*    # when available to the platform package build
```

The archive intentionally does not contain `ducklake.duckdb_extension`. DuckLake is compiled into
the packaged DuckDB shell and `libduckdb*` library. The release build uses DuckLake's extension
configuration, which also compiles the standard `icu`, `json`, and `tpch` extensions unless that
build explicitly disables them.

Build all default release artifacts:

```sh
just ducklake-release
```

Build one release target:

```sh
just ducklake-release linux-amd64
just ducklake-release linux-arm64
just ducklake-release macos-arm64
```

Linux artifacts are built in the Red Hat UBI9 release container:

```sh
just ducklake-build-linux-amd64
just ducklake-build-linux-arm64
```

Build the local release tree without packaging:

```sh
just ducklake-build-release
```

The local release DuckDB shell is:

```text
third_party/ducklake/build/release/duckdb
```

The release runtime library is built in Cargo's release target directory:

```text
target/release/libducklake_catalog.dylib
target/release/libducklake_catalog.so
```

Verify a release archive before unpacking:

```sh
shasum -a 256 -c artifacts/aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz.sha256
```

Use `sha256sum -c` on systems where `sha256sum` is the available checksum tool.

Inspect a release archive without unpacking it:

```sh
tar -tzf artifacts/aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz
```

## Option 1: Full DuckDB With Compiled Extensions

This is the release and production path. Use the packaged `opt/duckdb/duckdb` shell for direct SQL
usage, or link/embed with the packaged `opt/duckdb/duckdb.h` and `opt/duckdb/libduckdb*` files. Both
entrypoints use the same compiled-in DuckLake extension and the same aux runtime library contract.

Unpack the release archive:

```sh
install_root="$PWD/aux-ducklake-install"
mkdir -p "$install_root"
tar -C "$install_root" -xzf artifacts/aux-ducklake-fdb-v<ducklake-commit>-<platform>.tar.gz
```

Start the packaged DuckDB binary on Linux:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='prod/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$install_root/opt/aux-ducklake/lib/libducklake_catalog.so"
ducklake_lib_path="$install_root/opt/aux-ducklake/lib:$install_root/opt/duckdb"
export LD_LIBRARY_PATH="$ducklake_lib_path:${LD_LIBRARY_PATH:-}"

"$install_root/opt/duckdb/duckdb"
```

Start the packaged DuckDB binary on macOS:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='dev/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$install_root/opt/aux-ducklake/lib/libducklake_catalog.dylib"
ducklake_lib_path="$install_root/opt/aux-ducklake/lib:$install_root/opt/duckdb"
export DYLD_LIBRARY_PATH="$ducklake_lib_path:${DYLD_LIBRARY_PATH:-}"

"$install_root/opt/duckdb/duckdb"
```

Inside DuckDB, load DuckLake and attach a FoundationDB-backed aux catalog:

```sql
LOAD ducklake;

ATTACH 'ducklake:/var/lib/aux-ducklake/example/metadata.duckdb' AS dl (
    DATA_PATH '/var/lib/aux-ducklake/example/data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);

CREATE TABLE dl.main.items(id INTEGER, name VARCHAR);
INSERT INTO dl.main.items VALUES (1, 'alpha'), (2, 'beta');
SELECT * FROM dl.main.items ORDER BY id;
```

The `metadata.duckdb` path remains part of DuckLake attach syntax. With
`META_TYPE 'aux_catalog'`, catalog truth lives in FoundationDB under `AUX_DUCKLAKE_FDB_PREFIX`.
Because DuckLake is compiled into the packaged binary, `LOAD ducklake;` does not download or read a
separate extension file.

Embedded applications that load `libduckdb*` need the same environment variables and dynamic-library
search path as the shell examples above before they create a DuckDB connection. The aux runtime is
loaded from `AUX_DUCKLAKE_RUNTIME_LIBRARY` the first time an aux catalog operation crosses the FFI
bridge, and that path is fixed for the lifetime of the process.

To smoke-test the installed binary without touching FoundationDB, leave the aux variables unset and
run a regular DuckLake attach as shown in the non-aux catalog section below.

## Option 2: Standalone Loadable Extension

Use this mode when you need a `ducklake.duckdb_extension` file. This is mainly for development,
integration tests, and tightly controlled deployments where the host DuckDB binary is known to match
the extension ABI.

Build the patched DuckLake checkout and debug extension:

```sh
just ducklake-fetch
just ducklake-build
```

Build the matching debug runtime library:

```sh
./scripts/cargo_with_sccache.sh build -p ducklake-catalog \
  --no-default-features \
  --features foundationdb
```

The debug extension is:

```text
third_party/ducklake/build/debug/extension/ducklake/ducklake.duckdb_extension
```

The debug runtime library is:

```text
target/debug/libducklake_catalog.dylib
target/debug/libducklake_catalog.so
```

The safest host shell is the debug DuckDB binary built from the same checkout:

```sh
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='dev/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$PWD/target/debug/libducklake_catalog.dylib"

third_party/ducklake/build/debug/duckdb
```

Use `.so` instead of `.dylib` on Linux.

Inside that matching shell, load DuckLake:

```sql
LOAD ducklake;
```

From another compatible DuckDB binary, load the extension by absolute path:

```sql
LOAD '/absolute/path/to/ducklake.duckdb_extension';
```

Then attach and use the aux catalog:

```sql
ATTACH 'ducklake:/tmp/aux-ducklake-metadata.duckdb' AS dl (
    DATA_PATH '/tmp/aux-ducklake-data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);

CREATE TABLE dl.main.items(id INTEGER, name VARCHAR);
INSERT INTO dl.main.items VALUES (1, 'alpha'), (2, 'beta');
SELECT * FROM dl.main.items ORDER BY id;
```

Do not copy a standalone extension to an unrelated DuckDB installation unless the host DuckDB ABI,
platform, extension signing policy, and extension-loading policy are known to match. If extension
loading fails, retry with `third_party/ducklake/build/debug/duckdb` before changing anything else.

If you want release-mode performance, use the combined release DuckDB binary instead of a standalone
extension:

```sh
just ducklake-build-release

export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX='dev/ducklake/catalogs/example/'
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$PWD/target/release/libducklake_catalog.dylib"

third_party/ducklake/build/release/duckdb
```

Use `.so` instead of `.dylib` on Linux. The release archive is produced from this combined-binary
shape and does not ship `ducklake.duckdb_extension`.

## Using DuckLake Without The Aux FDB Catalog

The packaged binary and standalone extension can still run regular DuckLake metadata
configurations. Do not set `META_TYPE 'aux_catalog'`, and leave the aux FoundationDB variables
unset:

```sh
unset AUX_DUCKLAKE_CATALOG_BACKEND
unset AUX_DUCKLAKE_FDB_PREFIX
unset AUX_DUCKLAKE_RUNTIME_LIBRARY
unset AUX_DUCKLAKE_FDB_CLUSTER_FILE
unset AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY
```

If `META_TYPE 'aux_catalog'` is still used with `AUX_DUCKLAKE_CATALOG_BACKEND=fdb`,
`AUX_DUCKLAKE_FDB_PREFIX` being unset selects the default FoundationDB prefix `dl/`.

Use DuckLake's normal DuckDB-backed attach form:

```sql
LOAD ducklake;

ATTACH 'ducklake:/tmp/ducklake-metadata.duckdb' AS dl (
    DATA_PATH '/tmp/ducklake-data'
);
```

For Postgres-backed DuckLake metadata, use the upstream DuckLake/Postgres attach configuration
expected by your DuckLake version. The aux FoundationDB catalog path does not require Postgres.

If you are using the packaged binary only for regular DuckLake, `AUX_DUCKLAKE_RUNTIME_LIBRARY` is not
needed. Set it only for `META_TYPE 'aux_catalog'`.

## First-Run Verification

After attaching a catalog, run a small read/write check:

```sql
CREATE TABLE dl.main.usage_smoke(id INTEGER, value VARCHAR);
INSERT INTO dl.main.usage_smoke VALUES (1, 'ok');
SELECT * FROM dl.main.usage_smoke ORDER BY id;
DROP TABLE dl.main.usage_smoke;
```

For local development, these repo commands exercise the same runtime bridge:

```sh
just ducklake-runtime-cpp-ffi-smoke
just ducklake-fdb-runtime-smoke
```

Run live FoundationDB catalog tests:

```sh
just ducklake-catalog-fdb-test
```

Run the full release gate before publishing artifacts:

```sh
just ducklake-fdb-release-gate
```

## Command Map

Common commands for users and packagers:

```sh
just --summary
just ducklake-fetch
just ducklake-build
just ducklake-build-release
just ducklake-release
just ducklake-runtime-cpp-ffi-smoke
just ducklake-fdb-release-gate
```

Use `just ducklake-build` for a debug development shell and extension. Use
`just ducklake-build-release` for a local release-profile combined binary. Use `just ducklake-release`
for distributable archives under `artifacts/`.

## Data Inlining

`DATA_INLINING_ROW_LIMIT 0` disables DuckLake data inlining. This is the conservative setup example.

To use DuckLake inlining with the aux catalog, set a positive row limit:

```sql
ATTACH 'ducklake:/tmp/aux-ducklake-inline-metadata.duckdb' AS dl (
    DATA_PATH '/tmp/aux-ducklake-inline-data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 100
);
```

Use one FoundationDB prefix per attached catalog, including test catalogs created for inlining
experiments.

## Operational Rules

- Objects are immutable: DuckLake creates and deletes files; it does not update files in place.
- Compaction creates new files and deletes old files.
- Catalog metadata belongs to the configured FoundationDB prefix.
- Data files belong to the configured `DATA_PATH` or object-storage system.
- Backups must cover both the FoundationDB catalog prefix and the data-file storage.
- Optional runtime metrics are off by default. Enable them only for diagnostics or benchmarks with
  `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH`.

## Troubleshooting

`AUX_DUCKLAKE_RUNTIME_LIBRARY` errors:

Check that the path is absolute, points to the matching `libducklake_catalog` build, and uses the
right platform suffix: `.so` on Linux or `.dylib` on macOS.

`AUX_DUCKLAKE_RUNTIME_LIBRARY changed after the runtime was loaded`:

Start a fresh DuckDB process. The runtime library path is fixed after the bridge first loads the FFI
runtime.

FoundationDB connection errors:

Check that the FoundationDB client library is loadable, the cluster file points at the intended
cluster, and `AUX_DUCKLAKE_FDB_PREFIX` is set to the expected catalog-owned prefix.

`META_TYPE 'aux_catalog'` is not recognized:

The loaded DuckLake extension is not the patched aux-ducklake build. Use the packaged DuckDB binary
or load `third_party/ducklake/build/debug/extension/ducklake/ducklake.duckdb_extension` into a
compatible DuckDB binary.

Extension load errors:

Use the DuckDB binary built by `just ducklake-build`, or switch to the packaged release binary. A
standalone extension must match the host DuckDB ABI and the host extension-loading policy.

`LOAD ducklake;` downloads an extension instead of using this build:

Use the packaged binary from `opt/duckdb/duckdb` or the local shell under
`third_party/ducklake/build/{debug,release}/duckdb`. Those binaries are built with this DuckLake
extension configuration.

Missing FoundationDB client library:

Install the FoundationDB client package on the host or include `libfdb_c` in the runtime library
search path. Release packages include `libfdb_c` only when the package build host or container had it
available.
