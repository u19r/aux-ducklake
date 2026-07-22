# DuckLake FoundationDB Runtime Runbook

This runbook covers the aux DuckLake catalog runtime when `AUX_DUCKLAKE_CATALOG_BACKEND=fdb`.
It is intentionally scoped to catalog operations. Data files remain in DuckLake object storage or
the configured filesystem `DATA_PATH` and must be backed up by that storage layer.

## Required Configuration

- Build the runtime library with FoundationDB support:
  `./scripts/cargo_with_sccache.sh build -p ducklake-catalog --features foundationdb`.
- Configure DuckDB with `AUX_DUCKLAKE_RUNTIME_LIBRARY` pointing at the built
  `libducklake_catalog` library.
- Set `AUX_DUCKLAKE_CATALOG_BACKEND=fdb`.
- Set `AUX_DUCKLAKE_FDB_PREFIX` to one catalog-owned prefix ending in `/`, for example
  `prod/ducklake/catalogs/<catalog-name>/`.
- Set `AUX_DUCKLAKE_FDB_CLUSTER_FILE` when the default local FoundationDB cluster file is not the
  intended cluster.

## Prefix Ownership

Each DuckLake catalog owns exactly one FoundationDB prefix. Do not share a prefix between catalogs,
test runs, or restore targets. Restore and clone targets must use a fresh prefix or one explicitly
cleared as part of the restore operation.

The local prefix clone helper rejects empty, identical, and overlapping prefixes. Preserve that rule
for manual operations:

```bash
CARGO_TARGET_DIR=target/codex-validation \
  ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --features foundationdb \
  --bin ducklake-fdb-prefix-copy -- \
  --source-prefix 'prod/ducklake/catalogs/source/' \
  --destination-prefix 'drill/ducklake/catalogs/restored/' \
  --clear-destination
```

The helper copies keys in bounded transactions. It is for drills, tests, and small controlled
prefix clones. For production disaster recovery, use FoundationDB backup/restore for the cluster or
for the selected key range, then validate the restored prefix with DuckDB.

## Backup And Restore Boundary

Back up these surfaces together:

- FoundationDB catalog key range under `AUX_DUCKLAKE_FDB_PREFIX`.
- DuckLake data files under the catalog `DATA_PATH` or object-storage location.
- The release artifact that contains the DuckLake extension and matching Rust runtime library.
- The environment/configuration values that select cluster file, catalog backend, prefix, and data
  path.

The aux catalog path does not rely on DuckLake's native `ducklake_metadata` tables for catalog
truth. A fresh DuckLake metadata shell can attach to a restored FDB prefix as long as the data path
is also available.

## Local Restore Drill

Run the release drill against the local FoundationDB cluster:

```bash
CARGO_TARGET_DIR=target/codex-validation just ducklake-fdb-prefix-clone-drill
```

The drill:

- builds the FoundationDB-capable runtime library;
- seeds a DuckDB/DuckLake catalog through the compiled runtime bridge;
- copies the FDB source prefix to a fresh restored prefix with `ducklake-fdb-prefix-copy`;
- attaches DuckDB to the restored prefix using a fresh metadata path;
- verifies `restored_readback=3,60,alpha|beta|gamma`.

Use `AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1` to reuse an already-built patched DuckLake binary.
Run `./scripts/ducklake_fdb_prefix_clone_drill.sh --keep-tmp` only when debugging a failed drill and
you need to inspect the temporary DuckLake files.

## Disaster Recovery Procedure

1. Stop writes to the affected catalog or choose a FoundationDB backup version before the incident.
2. Restore the FoundationDB cluster or selected catalog range into an isolated cluster or fresh
   prefix. FoundationDB's backup documentation describes point-in-time backup/restore and the need
   to combine copied data with mutation logs for a consistent snapshot.
3. Restore or verify the DuckLake data-file storage for the same point in time.
4. Configure DuckDB with the restored cluster file, restored prefix, runtime library, and restored
   data path.
5. Run readback SQL for representative tables plus release-safe metadata checks such as
   `ducklake_snapshots('dl')`, `ducklake_table_info('dl')`, and `ducklake_list_files('dl')`.
6. Run `just ducklake-fdb-prefix-clone-drill` or an equivalent catalog-specific drill before
   promoting the restored prefix for production reads.

## Operator Signals

- `just ducklake-fdb-runtime-smoke` must pass against the target local or staging cluster before
  release promotion.
- When `AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH` is intentionally enabled for a benchmark or
  diagnostic smoke, runtime metrics should show nonzero successful counters for metadata, schema,
  data mutation, read, and cleanup families.
- Prefix clone output must include nonzero `copied_key_count` and `ducklake_fdb_prefix_clone_drill=ok`.

## Runtime Response Pagination

The runtime protocol transports read results in pages of at most 512 KiB. The C++ bridge follows
`next_page_offset` automatically and presents the reassembled payload to DuckLake, so operation
callers must not implement their own catalog pagination. This boundary covers every read family,
including catalog snapshots, metadata mirrors, file and statistics listings, change feeds, inline
rows, and cleanup inventories. It also leaves headroom below the runtime's 2 MiB frame limit and
deployment surfaces hosted in AWS Lambda. Lambda query/API responses remain a separate
consumer-owned pagination boundary; reassembling catalog pages does not waive that external
response limit.

Pagination is runtime protocol version 2. The DuckLake extension and Rust runtime library must be
released and deployed as one matching artifact; version 1 peers are rejected rather than risking a
silently truncated result.

Each continuation carries a SHA-256 digest of the complete logical result. The runtime returns a
retryable conflict if the result changes between pages; it never combines pages from different
catalog states. Mutating operations are executed once and cannot accept a page continuation. Keep
mutation responses request-bounded rather than adding transport retries that could repeat effects.

`cargo test -p ducklake-catalog runtime_protocol_tests` proves multi-page reassembly, legacy
single-page compatibility, and changed-result rejection. `just ducklake-runtime-cpp-ffi-smoke`
proves the patched C++ bridge builds and retains end-to-end catalog behavior.

## References

- FoundationDB backup/restore overview:
  https://apple.github.io/foundationdb/backups.html
- FoundationDB administration overview:
  https://apple.github.io/foundationdb/administration.html
