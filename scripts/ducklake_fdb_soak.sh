#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"

fail() {
    echo "ducklake fdb soak failure: $*" >&2
    exit 1
}

sql_literal() {
    local value="$1"
    printf "'%s'" "${value//\'/\'\'}"
}

iterations="${AUX_DUCKLAKE_FDB_SOAK_ITERATIONS:-20}"
if ! ducklake_reuse_debug_build_enabled || [[ ! -x "$DUCKDB_BIN" ]]; then
    AUX_DUCKLAKE_SKIP_FETCH=1 "$ROOT_DIR/scripts/build_ducklake_debug.sh"
fi
runtime_library="$(ducklake_build_debug_catalog_runtime "$ROOT_DIR" 1 foundationdb)" ||
    fail "foundationdb runtime library was not built"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
data_dir="$tmp_dir/data"
mkdir -p "$data_dir"

export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_library"
export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_PREFIX="aux-ducklake/release/soak/$(date +%s)-$$/"

attach_sql() {
    cat <<SQL
ATTACH 'ducklake:$tmp_dir/metadata.duckdb' AS dl (
    DATA_PATH $(sql_literal "$data_dir"),
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 10
);
SQL
}

run_duckdb() {
    local sql="$1"
    set +e
    duckdb_output="$("$DUCKDB_BIN" -batch 2>&1 <<<"$sql")"
    duckdb_status=$?
    set -e
}

run_duckdb "
LOAD ducklake;
$(attach_sql)
CREATE TABLE dl.main.soak_probe(id INTEGER, batch INTEGER, note VARCHAR);
DETACH dl;
"
[[ "$duckdb_status" -eq 0 ]] || {
    printf '%s\n' "$duckdb_output" >&2
    fail "soak initialization failed"
}

for iteration in $(seq 1 "$iterations"); do
    run_duckdb "
LOAD ducklake;
$(attach_sql)
INSERT INTO dl.main.soak_probe VALUES
    ($((iteration * 10 + 1)), $iteration, 'inline_$iteration'),
    ($((iteration * 10 + 2)), $iteration, 'inline_${iteration}_b');
UPDATE dl.main.soak_probe SET note = 'updated_$iteration' WHERE id = $((iteration * 10 + 1));
DELETE FROM dl.main.soak_probe WHERE id = $((iteration * 10 + 2));
CALL ducklake_flush_inlined_data('dl', table_name => 'soak_probe');
SELECT 'soak_iteration=$iteration,' || count(*) || ',' || sum(id) FROM dl.main.soak_probe;
DETACH dl;
LOAD ducklake;
$(attach_sql)
SELECT 'soak_reattach=$iteration,' || count(*) || ',' || sum(id) FROM dl.main.soak_probe;
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "soak iteration $iteration failed"
    }
    expected_count="$iteration"
    expected_sum=$((10 * iteration * (iteration + 1) / 2 + iteration))
    [[ "$duckdb_output" == *"soak_iteration=$iteration,$expected_count,$expected_sum"* ]] || fail "iteration label mismatch at $iteration"
    [[ "$duckdb_output" == *"soak_reattach=$iteration,$expected_count,$expected_sum"* ]] || fail "reattach label mismatch at $iteration"
done

run_duckdb "
LOAD ducklake;
$(attach_sql)
CALL ducklake_expire_snapshots('dl', dry_run => false);
SELECT 'soak_snapshot_count=' || count(*) FROM ducklake_snapshots('dl');
SELECT 'soak_final=' || count(*) || ',' || sum(id) FROM dl.main.soak_probe;
DETACH dl;
"
[[ "$duckdb_status" -eq 0 ]] || {
    printf '%s\n' "$duckdb_output" >&2
    fail "soak maintenance failed"
}
expected_sum=$((10 * iterations * (iterations + 1) / 2 + iterations))
[[ "$duckdb_output" == *"soak_final=$iterations,$expected_sum"* ]] || fail "final soak label mismatch"
printf '%s\n' "$duckdb_output"
echo "ducklake_fdb_soak=ok"
