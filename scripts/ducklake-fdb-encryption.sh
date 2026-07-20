#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ducklake_dir="$root_dir/third_party/ducklake"
. "$root_dir/scripts/ducklake_build_common.sh"
duckdb_bin="$ducklake_dir/build/debug/duckdb"

fail() {
    echo "ducklake FoundationDB encryption failure: $*" >&2
    exit 1
}

run_duckdb() {
    local sql="$1"
    set +e
    duckdb_output="$("$duckdb_bin" -unsigned -batch 2>&1 <<<"$sql")"
    duckdb_status=$?
    set -e
}

ducklake_build_debug_duckdb_if_needed "$root_dir" "$duckdb_bin" ||
    fail "modified DuckDB was not built"
runtime_library="$(ducklake_build_debug_catalog_runtime "$root_dir" 1 foundationdb)" ||
    fail "FoundationDB runtime library was not built"

test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX="aux-ducklake/e2e/encryption/$(date +%s)-$$/"
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_library"

run_duckdb "
LOAD ducklake;
ATTACH 'ducklake:$test_dir/metadata.duckdb' AS dl (
    DATA_PATH '$test_dir/data',
    META_TYPE 'aux_catalog',
    ENCRYPTED,
    DATA_INLINING_ROW_LIMIT 0
);
CREATE TABLE dl.main.encrypted_orders AS SELECT i id FROM range(10) t(i);
DELETE FROM dl.main.encrypted_orders WHERE id % 2 = 0;
DETACH dl;
ATTACH 'ducklake:$test_dir/metadata.duckdb' AS dl (META_TYPE 'aux_catalog');
SELECT 'encrypted_readback=' || count(*) || ',' || sum(id) FROM dl.main.encrypted_orders;
SELECT 'encrypted_file_keys=' ||
       count_if(data_file_encryption_key IS NOT NULL) || ',' ||
       count_if(delete_file_encryption_key IS NOT NULL)
FROM ducklake_list_files('dl', 'encrypted_orders');
"
[[ "$duckdb_status" -eq 0 ]] || {
    printf '%s\n' "$duckdb_output" >&2
    fail "encrypted catalog create, delete, detach, and readback failed"
}
[[ "$duckdb_output" == *"encrypted_readback=5,25"* ]] || fail "encrypted readback result was not preserved"
[[ "$duckdb_output" == *"encrypted_file_keys=1,1"* ]] || fail "catalog did not return both file encryption keys"

run_duckdb "LOAD parquet; SELECT * FROM '$test_dir/data/**/*.parquet';"
[[ "$duckdb_status" -ne 0 ]] || fail "raw Parquet read unexpectedly succeeded without the catalog key"
[[ "$duckdb_output" == *"encrypted"* ]] || fail "raw Parquet read did not fail as encrypted"

printf '%s\n' "$duckdb_output"
echo "ducklake_fdb_encryption=ok"
