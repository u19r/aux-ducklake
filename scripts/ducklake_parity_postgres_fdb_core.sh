#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"
POSTGRES_SCANNER_EXTENSION="$DUCKLAKE_DIR/build/debug/extension/postgres_scanner/postgres_scanner.duckdb_extension"
SCENARIO_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/core_parity.sql"
READBACK_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/core_parity_readback.sql"
EXPECTED="$ROOT_DIR/docs/parity/ducklake-fdb/expected/core_parity.out"

fail() {
    echo "ducklake postgres/fdb core parity failure: $*" >&2
    exit 1
}

sql_literal() {
    local value="$1"
    printf "'%s'" "${value//\'/\'\'}"
}

run_duckdb() {
    local sql="$1"
    set +e
    duckdb_output="$("$DUCKDB_BIN" -unsigned -batch 2>&1 <<<"$sql")"
    duckdb_status=$?
    set -e
}

assert_expected_output() {
    local output="$1"
    local line
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        [[ "$output" == *"$line"* ]] || {
            printf '%s\n' "$output" >&2
            fail "expected output to contain: $line"
        }
    done < "$EXPECTED"
}

assert_failure_contains() {
    local output="$1"
    local status="$2"
    local needle="$3"
    [[ "$status" -ne 0 ]] || fail "expected failure containing: $needle"
    [[ "$output" == *"$needle"* ]] || fail "expected failure output to contain: $needle"
}

build_ducklake_if_needed() {
    ducklake_build_debug_duckdb_with_postgres_if_needed "$ROOT_DIR" "$DUCKLAKE_DIR" "$DUCKDB_BIN" "$POSTGRES_SCANNER_EXTENSION" ||
        fail "modified DuckDB with postgres_scanner was not built"
}

build_runtime_library() {
    RUNTIME_LIBRARY="$(ducklake_build_debug_catalog_runtime "$ROOT_DIR" 1 foundationdb)" ||
        fail "foundationdb runtime library was not built"
}

scale_sql() {
    local idx padded
    for idx in $(seq 1 100); do
        padded="$(printf '%03d' "$idx")"
        printf "CREATE TABLE dl.extra.scale_%s AS SELECT i::INTEGER AS id, %d::INTEGER AS table_no FROM range(1, 101) r(i);\n" "$padded" "$idx"
    done
}

scale_readback_sql() {
    local idx padded prefix="SELECT 'many_tables_total=' || sum(row_count) FROM ("
    printf "%s" "$prefix"
    for idx in $(seq 1 100); do
        padded="$(printf '%03d' "$idx")"
        if [[ "$idx" -gt 1 ]]; then
            printf " UNION ALL "
        fi
        printf "SELECT count(*) AS row_count FROM dl.extra.scale_%s" "$padded"
    done
    printf ");\n"
}

postgres_attach_sql() {
    local data_path="$1" metadata_schema="$2" postgres_dsn="$3"
    cat <<SQL
ATTACH 'ducklake:postgres:$postgres_dsn' AS dl (
    DATA_PATH $(sql_literal "$data_path"),
    METADATA_SCHEMA $(sql_literal "$metadata_schema"),
    DATA_INLINING_ROW_LIMIT 0
);
SQL
}

fdb_attach_sql() {
    local data_path="$1"
    cat <<SQL
ATTACH 'ducklake:$data_path/metadata.duckdb' AS dl (
    DATA_PATH $(sql_literal "$data_path/data"),
    DATA_INLINING_ROW_LIMIT 0,
    META_TYPE 'aux_catalog'
);
SQL
}

run_postgres_core() {
    local tmp_dir="$1" metadata_schema postgres_dsn dsn_literal scanner_literal
    postgres_dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
    metadata_schema="ducklake_core_parity_$(date +%s)_$$"
    dsn_literal="$(sql_literal "$postgres_dsn")"
    scanner_literal="$(sql_literal "$POSTGRES_SCANNER_EXTENSION")"
    mkdir -p "$tmp_dir/data"
    run_duckdb "
LOAD ducklake;
LOAD $scanner_literal;
ATTACH $dsn_literal AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $metadata_schema CASCADE');
CALL postgres_execute('pg', 'CREATE SCHEMA $metadata_schema');
DETACH pg;
$(postgres_attach_sql "$tmp_dir/data" "$metadata_schema" "$postgres_dsn")
$(cat "$SCENARIO_SQL")
$(scale_sql)
DETACH dl;
$(postgres_attach_sql "$tmp_dir/data" "$metadata_schema" "$postgres_dsn")
$(cat "$READBACK_SQL")
$(scale_readback_sql)
DETACH dl;
ATTACH $dsn_literal AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $metadata_schema CASCADE');
DETACH pg;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "Postgres-backed core parity failed"
    }
    assert_expected_output "$duckdb_output"
    printf '%s\n' "$duckdb_output"
}

run_fdb_core() {
    local tmp_dir="$1" fdb_prefix="$2"
    mkdir -p "$tmp_dir"
    export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
    export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
    export AUX_DUCKLAKE_RUNTIME_LIBRARY="$RUNTIME_LIBRARY"
    run_duckdb "
LOAD ducklake;
$(fdb_attach_sql "$tmp_dir")
$(cat "$SCENARIO_SQL")
$(scale_sql)
DETACH dl;
$(fdb_attach_sql "$tmp_dir")
$(cat "$READBACK_SQL")
$(scale_readback_sql)
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "FoundationDB-backed core parity failed"
    }
    assert_expected_output "$duckdb_output"
    printf '%s\n' "$duckdb_output"
}

run_fdb_snapshot_failures() {
    local tmp_dir="$1" fdb_prefix="$2"
    export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
    export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
    export AUX_DUCKLAKE_RUNTIME_LIBRARY="$RUNTIME_LIBRARY"
    run_duckdb "
LOAD ducklake;
$(fdb_attach_sql "$tmp_dir")
SELECT * FROM dl.extra.core_orders AT (VERSION => 999999999);
"
    assert_failure_contains "$duckdb_output" "$duckdb_status" "No snapshot found"
    run_duckdb "
LOAD ducklake;
$(fdb_attach_sql "$tmp_dir")
SET VARIABLE core_parity_expired_snapshot = (SELECT id FROM ducklake_current_snapshot('dl'));
CREATE TABLE dl.extra.expire_probe AS SELECT 1 AS id;
CALL ducklake_expire_snapshots('dl', dry_run => false, versions => [getvariable('core_parity_expired_snapshot')::BIGINT]);
SELECT * FROM dl.extra.core_orders AT (VERSION => getvariable('core_parity_expired_snapshot')::BIGINT);
"
    assert_failure_contains "$duckdb_output" "$duckdb_status" "No snapshot found"
}

[[ -f "$SCENARIO_SQL" ]] || fail "missing scenario SQL: $SCENARIO_SQL"
[[ -f "$READBACK_SQL" ]] || fail "missing readback SQL: $READBACK_SQL"
[[ -f "$EXPECTED" ]] || fail "missing expected output: $EXPECTED"

build_ducklake_if_needed
build_runtime_library

tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

echo "core_parity_step=postgres_core"
run_postgres_core "$tmp_root/postgres"
fdb_prefix="aux-ducklake-e2e/core_parity-core/$(date +%s)/$$/"
echo "core_parity_step=fdb_core"
run_fdb_core "$tmp_root/fdb" "$fdb_prefix"
echo "core_parity_step=fdb_snapshot_failures"
run_fdb_snapshot_failures "$tmp_root/fdb" "$fdb_prefix"
echo "ducklake_parity_postgres_fdb_core=ok"
