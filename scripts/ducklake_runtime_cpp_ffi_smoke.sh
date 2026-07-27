#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"
DUCKLAKE_EXTENSION="$DUCKLAKE_DIR/build/debug/extension/ducklake/ducklake.duckdb_extension"

fail() {
    echo "ducklake runtime cpp ffi smoke failure: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    [[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    [[ "$haystack" != *"$needle"* ]] || fail "expected output not to contain: $needle"
}

catalog_backend() {
    case "${1:-fdb}" in
        fdb | foundationdb) printf 'foundationdb' ;;
        *) fail "usage: $0 [fdb]" ;;
    esac
}

join_features() {
    local IFS=,
    printf '%s' "$*"
}

if [[ "$#" -gt 1 ]]; then
    fail "usage: $0 [fdb]"
fi

echo "e2e_step=build_rust_runtime_cdylib"
backend="$(catalog_backend "${1:-fdb}")"
metrics_path="${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}"
no_default_features=1
runtime_features=(foundationdb)
if [[ -n "$metrics_path" ]]; then
    runtime_features+=(runtime-metrics)
fi
runtime_library="$(
    ducklake_build_debug_catalog_runtime \
        "$ROOT_DIR" \
        "$no_default_features" \
        "$(join_features "${runtime_features[@]}")"
)" || fail "runtime library was not built"

echo "e2e_step=build_modified_ducklake"
if ! ducklake_reuse_debug_build_enabled; then
    AUX_DUCKLAKE_SKIP_FETCH=1 "$ROOT_DIR/scripts/build_ducklake_debug.sh"
fi
[[ -x "$DUCKDB_BIN" ]] || fail "modified duckdb executable was not built"
[[ -f "$DUCKLAKE_EXTENSION" ]] || fail "modified ducklake extension was not built"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if [[ -n "$metrics_path" ]]; then
    mkdir -p "$(dirname "$metrics_path")"
    rm -f "$metrics_path"
fi
catalog_run_id="$(date +%s)-$$"

export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_library"
if [[ -n "$metrics_path" ]]; then
    export AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH="$metrics_path"
else
    unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH || true
fi
export AUX_DUCKLAKE_CATALOG_BACKEND="$backend"
export AUX_DUCKLAKE_FDB_PREFIX="aux-ducklake/runtime-smoke/$catalog_run_id/main/"
export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$AUX_DUCKLAKE_FDB_PREFIX"
mkdir -p "$tmp_dir/data"
printf 'orphan\n' > "$tmp_dir/data/runtime-orphan.parquet"

set +e
output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD '$DUCKLAKE_EXTENSION';
ATTACH 'ducklake:$tmp_dir/metadata.duckdb' AS dl (
    DATA_PATH '$tmp_dir/data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);
SELECT COUNT(*) AS snapshot_count
FROM ducklake_snapshots('dl');
CREATE SCHEMA dl.runtime_schema;
CREATE TABLE dl.runtime_schema.schema_probe(id INTEGER);
SELECT 'schema_probe_count=' || count(*)
FROM dl.runtime_schema.schema_probe;
CREATE TABLE dl.main.runtime_probe(id INTEGER);
SET VARIABLE created_snapshot = (
    SELECT max(snapshot_id)
    FROM ducklake_snapshots('dl')
);
SELECT 'time_travel_count=' || count(*)
FROM dl.main.runtime_probe AT (VERSION => getvariable('created_snapshot')::BIGINT);
CREATE TABLE dl.main.runtime_drop_probe(id INTEGER);
DROP TABLE dl.main.runtime_drop_probe;
CREATE TABLE dl.main.runtime_schema_change_probe(id INTEGER);
COMMENT ON TABLE dl.main.runtime_schema_change_probe IS 'runtime schema comment';
COMMENT ON COLUMN dl.main.runtime_schema_change_probe.id IS 'runtime id comment';
ALTER TABLE dl.main.runtime_schema_change_probe ADD COLUMN status VARCHAR DEFAULT 'new';
ALTER TABLE dl.main.runtime_schema_change_probe RENAME COLUMN status TO state;
ALTER TABLE dl.main.runtime_schema_change_probe ALTER state SET DEFAULT 'ready';
ALTER TABLE dl.main.runtime_schema_change_probe DROP COLUMN state;
ALTER TABLE dl.main.runtime_schema_change_probe RENAME TO runtime_schema_change_probe_renamed;
DROP TABLE dl.main.runtime_schema_change_probe_renamed;
CREATE VIEW dl.main.runtime_view_probe AS SELECT 42 AS id;
SELECT 'runtime_view_probe=' || sum(id)
FROM dl.main.runtime_view_probe;
DROP VIEW dl.main.runtime_view_probe;
CREATE TABLE dl.main.runtime_partition(id INTEGER, region VARCHAR, amount INTEGER);
INSERT INTO dl.main.runtime_partition VALUES (1, 'eu', 10), (2, 'us', 20);
ALTER TABLE dl.main.runtime_partition SET PARTITIONED BY (region);
INSERT INTO dl.main.runtime_partition VALUES (3, 'eu', 30), (4, 'us', 40), (5, 'apac', 50);
SET VARIABLE partition_snapshot = (
    SELECT max(snapshot_id)
    FROM ducklake_snapshots('dl')
);
INSERT INTO dl.main.runtime_partition VALUES (6, 'eu', 60);
SELECT 'partition_current=' || count(*) || ',' || sum(amount)
FROM dl.main.runtime_partition
WHERE region = 'eu';
SELECT 'partition_historical=' || count(*) || ',' || sum(amount)
FROM dl.main.runtime_partition AT (VERSION => getvariable('partition_snapshot')::BIGINT)
WHERE region = 'us';
CREATE TABLE dl.main.runtime_cdf(id INTEGER, amount INTEGER);
SET VARIABLE before_cdf = (SELECT id FROM ducklake_current_snapshot('dl'));
INSERT INTO dl.main.runtime_cdf VALUES (10, 100), (20, 200);
SET VARIABLE after_cdf_insert = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'cdf_insert=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'insert')
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_cdf',
    getvariable('before_cdf')::BIGINT + 1,
    getvariable('after_cdf_insert')::BIGINT
);
SET VARIABLE before_cdf_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.main.runtime_cdf WHERE id = 10;
SET VARIABLE after_cdf_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'cdf_delete=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'delete')
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_cdf',
    getvariable('before_cdf_delete')::BIGINT + 1,
    getvariable('after_cdf_delete')::BIGINT
)
WHERE id = 10;
SELECT 'old_cleanup_dry_run=' || count(*)
FROM ducklake_cleanup_old_files('dl', dry_run => true, cleanup_all => true);
SELECT 'orphan_cleanup_dry_run=' || count(*)
FROM ducklake_delete_orphaned_files('dl', dry_run => true, cleanup_all => true)
WHERE path LIKE '%runtime-orphan.parquet';
SQL
)"
status=$?
set -e

printf '%s\n' "$output"

[[ "$status" -eq 0 ]] || fail "expected aux_catalog attach to succeed"
assert_contains "$output" "runtime_bridge=cpp_ffi"
assert_contains "$output" "runtime_ffi=ok"
assert_contains "$output" "operation=AttachMetadata"
assert_contains "$output" "backend=$backend"
assert_contains "$output" "operation=InitializeDuckLake"
assert_contains "$output" "snapshot_count"
assert_contains "$output" "1"
assert_contains "$output" "schema_probe_count=0"
assert_contains "$output" "time_travel_count=0"
assert_contains "$output" "partition_current=3,100"
assert_contains "$output" "partition_historical=2,60"
assert_contains "$output" "cdf_insert=2,30,2"
assert_contains "$output" "cdf_delete=1,10,1"
assert_contains "$output" "old_cleanup_dry_run=0"
assert_contains "$output" "orphan_cleanup_dry_run=1"

set +e
reattach_output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD '$DUCKLAKE_EXTENSION';
ATTACH 'ducklake:$tmp_dir/metadata.duckdb' AS dl (
    DATA_PATH '$tmp_dir/data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0,
    AUTOMATIC_MIGRATION true
);
SELECT 'reattach_count=' || count(*)
FROM dl.main.runtime_probe;
DETACH dl;
ATTACH 'ducklake:$tmp_dir/reattach-metadata.duckdb' AS dl (
    DATA_PATH '$tmp_dir/data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0,
    AUTOMATIC_MIGRATION true
);
INSERT INTO dl.main.runtime_probe VALUES (1);
SELECT 'fresh_mirror_append=' || count(*) || ',' || sum(id)
FROM dl.main.runtime_probe;
SQL
)"
reattach_status=$?
set -e

printf '%s\n' "$reattach_output"

[[ "$reattach_status" -eq 0 ]] || fail "expected existing aux_catalog attach to succeed"
assert_contains "$reattach_output" "reattach_count=0"
assert_contains "$reattach_output" "fresh_mirror_append=1,1"

export AUX_DUCKLAKE_FDB_PREFIX="aux-ducklake/runtime-smoke/$catalog_run_id/inline/"
export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$AUX_DUCKLAKE_FDB_PREFIX"

set +e
inline_output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD '$DUCKLAKE_EXTENSION';
ATTACH 'ducklake:$tmp_dir/inline-metadata.duckdb' AS dl (
    DATA_PATH '$tmp_dir/inline-data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 100
);
CREATE TABLE dl.main.runtime_inline(id INTEGER, note VARCHAR);
CREATE TABLE dl.main.runtime_boundary_inline(id INTEGER, note VARCHAR);
CREATE TABLE dl.main.runtime_oversized_inline(id INTEGER, note VARCHAR);
CREATE TABLE dl.main.runtime_aggregate_inline(id INTEGER, note VARCHAR);
SET VARIABLE before_inline = (SELECT id FROM ducklake_current_snapshot('dl'));
INSERT INTO dl.main.runtime_inline VALUES (101, 'inline_101'), (102, 'inline_102');
SET VARIABLE after_inline = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'inline_current=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM dl.main.runtime_inline;
SELECT 'inline_cdf_insert=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'insert') || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_inline',
    getvariable('before_inline')::BIGINT + 1,
    getvariable('after_inline')::BIGINT
)
WHERE id IN (101, 102);
SET VARIABLE before_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.main.runtime_inline WHERE id = 101;
SET VARIABLE after_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'inline_after_delete=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%') || ',' ||
       count(*) FILTER (WHERE id = 101)
FROM dl.main.runtime_inline;
SELECT 'inline_cdf_delete=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'delete') || ',' ||
       count(*) FILTER (WHERE note = 'inline_101')
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_inline',
    getvariable('before_delete')::BIGINT + 1,
    getvariable('after_delete')::BIGINT
)
WHERE id = 101;
SELECT 'inline_cdf_unmatched_delete=' || count(*)
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_inline',
    getvariable('before_delete')::BIGINT + 1,
    getvariable('after_delete')::BIGINT
)
WHERE id = 102;
INSERT INTO dl.main.runtime_boundary_inline
VALUES (200, repeat('s', 45000));
SELECT 'boundary_inline_current=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note))
FROM dl.main.runtime_boundary_inline;
SELECT 'boundary_inline_file_count=' || count(*)
FROM glob('$tmp_dir/inline-data/**/*.parquet');
INSERT INTO dl.main.runtime_oversized_inline
VALUES (201, repeat('x', 65536));
SELECT 'oversized_inline_current=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note))
FROM dl.main.runtime_oversized_inline;
SELECT 'oversized_inline_file_count=' || count(*)
FROM glob('$tmp_dir/inline-data/**/*.parquet');
UPDATE dl.main.runtime_oversized_inline
SET note = repeat('y', 65537)
WHERE id = 201;
SELECT 'oversized_update_current=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note)) || ',' || min(substr(note, 1, 1))
FROM dl.main.runtime_oversized_inline;
CREATE TABLE dl.main.runtime_oversized_merge(id INTEGER, note VARCHAR);
MERGE INTO dl.main.runtime_oversized_merge AS target
USING (SELECT 301 AS id, repeat('m', 65536) AS note) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET note = source.note
WHEN NOT MATCHED THEN INSERT (id, note) VALUES (source.id, source.note);
MERGE INTO dl.main.runtime_oversized_merge AS target
USING (SELECT 301 AS id, repeat('n', 65538) AS note) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET note = source.note
WHEN NOT MATCHED THEN INSERT (id, note) VALUES (source.id, source.note);
SELECT 'oversized_merge_current=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note)) || ',' || min(substr(note, 1, 1))
FROM dl.main.runtime_oversized_merge;
CREATE TABLE dl.main.runtime_oversized_ctas AS
SELECT value::INTEGER AS id,
       CASE WHEN value = 7 THEN repeat('c', 65539) ELSE 'small' END AS note
FROM range(10) AS rows(value);
SELECT 'oversized_ctas_current=' || count(*) || ',' || sum(id) || ',' ||
       max(length(note)) || ',' || count(*) FILTER (WHERE note = 'small')
FROM dl.main.runtime_oversized_ctas;
SET VARIABLE before_aggregate_files = (
    SELECT count(*) FROM glob('$tmp_dir/inline-data/**/*.parquet')
);
INSERT INTO dl.main.runtime_aggregate_inline
SELECT value::INTEGER, repeat('a', 40000)
FROM range(100) AS rows(value);
SELECT 'aggregate_inline_current=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note))
FROM dl.main.runtime_aggregate_inline;
SELECT 'aggregate_inline_file_delta=' ||
       (count(*) - getvariable('before_aggregate_files')::BIGINT)
FROM glob('$tmp_dir/inline-data/**/*.parquet');
SQL
)"
inline_status=$?
set -e

printf '%s\n' "$inline_output"

[[ "$inline_status" -eq 0 ]] || fail "expected aux_catalog inline runtime smoke to succeed"
assert_contains "$inline_output" "inline_current=2,203,2"
assert_contains "$inline_output" "inline_cdf_insert=2,203,2,2"
assert_contains "$inline_output" "inline_after_delete=1,102,1,0"
assert_contains "$inline_output" "inline_cdf_delete=1,101,1,1"
assert_contains "$inline_output" "inline_cdf_unmatched_delete=0"
assert_contains "$inline_output" "boundary_inline_current=1,200,45000"
assert_contains "$inline_output" "boundary_inline_file_count=0"
assert_contains "$inline_output" "oversized_inline_current=1,201,65536"
assert_contains "$inline_output" "oversized_inline_file_count=1"
assert_contains "$inline_output" "oversized_update_current=1,201,65537,y"
assert_contains "$inline_output" "oversized_merge_current=1,301,65538,n"
assert_contains "$inline_output" "oversized_ctas_current=10,45,65539,9"
assert_contains "$inline_output" "aggregate_inline_current=100,4950,40000"
assert_contains "$inline_output" "aggregate_inline_file_delta=1"

set +e
inline_reattach_output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD '$DUCKLAKE_EXTENSION';
ATTACH 'ducklake:$tmp_dir/inline-metadata.duckdb' AS dl (
    DATA_PATH '$tmp_dir/inline-data',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 100,
    AUTOMATIC_MIGRATION true
);
SELECT 'inline_reattach_current=' || count(*) || ',' || sum(id)
FROM dl.main.runtime_inline;
SET VARIABLE inline_reattach_snapshot = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'inline_reattach_delete=' || count(*) || ',' || sum(id)
FROM ducklake_table_changes(
    'dl', 'main', 'runtime_inline',
    1,
    getvariable('inline_reattach_snapshot')::BIGINT
)
WHERE id = 101 AND change_type = 'delete';
SELECT 'boundary_inline_reattach=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note))
FROM dl.main.runtime_boundary_inline;
SELECT 'oversized_inline_reattach=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note)) || ',' || min(substr(note, 1, 1))
FROM dl.main.runtime_oversized_inline;
SELECT 'oversized_merge_reattach=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note)) || ',' || min(substr(note, 1, 1))
FROM dl.main.runtime_oversized_merge;
SELECT 'oversized_ctas_reattach=' || count(*) || ',' || sum(id) || ',' ||
       max(length(note)) || ',' || count(*) FILTER (WHERE note = 'small')
FROM dl.main.runtime_oversized_ctas;
SELECT 'aggregate_inline_reattach=' || count(*) || ',' || sum(id) || ',' ||
       min(length(note))
FROM dl.main.runtime_aggregate_inline;
SQL
)"
inline_reattach_status=$?
set -e

printf '%s\n' "$inline_reattach_output"

[[ "$inline_reattach_status" -eq 0 ]] || fail "expected aux_catalog inline reattach smoke to succeed"
assert_contains "$inline_reattach_output" "inline_reattach_current=1,102"
assert_contains "$inline_reattach_output" "inline_reattach_delete=1,101"
assert_contains "$inline_reattach_output" "boundary_inline_reattach=1,200,45000"
assert_contains "$inline_reattach_output" "oversized_inline_reattach=1,201,65537,y"
assert_contains "$inline_reattach_output" "oversized_merge_reattach=1,301,65538,n"
assert_contains "$inline_reattach_output" "oversized_ctas_reattach=10,45,65539,9"
assert_contains "$inline_reattach_output" "aggregate_inline_reattach=100,4950,40000"
if [[ -n "$metrics_path" ]]; then
    [[ -f "$metrics_path" ]] || fail "runtime metrics artifact was not written at $metrics_path"
    metrics_output="$(cat "$metrics_path")"
    assert_contains "$metrics_output" 'family="metadata",operation="AttachMetadata",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="schema",operation="CreateTables",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="object",operation="CreateViews",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="data_mutation",operation="CommitDataMutation",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="read",operation="ListDataFilesAt",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="inline",operation="RegisterInlineRows",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="change_feed",operation="ListDataFileChanges",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="cleanup",operation="ListKnownFilesForCleanup",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="metadata",operation="RenderBoundedAppendMirrorSql",scope="unscoped",status="ok"'
    assert_contains "$metrics_output" 'family="metadata",operation="RenderBoundedDeleteFileMirrorSql",scope="unscoped",status="ok"'
    assert_not_contains "$metrics_output" 'operation="RenderCurrentMetadataDataFileMirrorSql"'
    assert_not_contains "$metrics_output" 'operation="RenderCurrentMetadataFileColumnStatsMirrorSql"'
    assert_not_contains "$metrics_output" 'operation="RenderDeleteFileMirrorSql"'
    assert_not_contains "$metrics_output" 'operation="RenderScheduledCleanupMirrorSql"'
    echo "runtime_metrics_path=$metrics_path"
fi

echo "ducklake_runtime_cpp_ffi_smoke=ok"
