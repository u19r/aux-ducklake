#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"
POSTGRES_SCANNER_EXTENSION="$DUCKLAKE_DIR/build/debug/extension/postgres_scanner/postgres_scanner.duckdb_extension"
SCENARIO_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/mutations_parity.sql"
READBACK_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/mutations_parity_readback.sql"
EXPECTED="$ROOT_DIR/docs/parity/ducklake-fdb/expected/mutations_parity.out"

fail() {
    echo "ducklake postgres/fdb mutation parity failure: $*" >&2
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

build_ducklake_if_needed() {
    ducklake_build_debug_duckdb_with_postgres_if_needed "$ROOT_DIR" "$DUCKLAKE_DIR" "$DUCKDB_BIN" "$POSTGRES_SCANNER_EXTENSION" ||
        fail "modified DuckDB with postgres_scanner was not built"
}

build_runtime_library() {
    RUNTIME_LIBRARY="$(ducklake_build_debug_catalog_runtime "$ROOT_DIR" 1 foundationdb)" ||
        fail "foundationdb runtime library was not built"
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

run_postgres_mutations() {
    local tmp_dir="$1" metadata_schema postgres_dsn dsn_literal scanner_literal
    postgres_dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
    metadata_schema="ducklake_mutations_parity_$(date +%s)_$$"
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
DETACH dl;
$(postgres_attach_sql "$tmp_dir/data" "$metadata_schema" "$postgres_dsn")
$(cat "$READBACK_SQL")
DETACH dl;
ATTACH $dsn_literal AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $metadata_schema CASCADE');
DETACH pg;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "Postgres-backed mutation parity failed"
    }
    assert_expected_output "$duckdb_output"
    printf '%s\n' "$duckdb_output"
}

run_fdb_mutations() {
    local tmp_dir="$1" fdb_prefix="$2"
    mkdir -p "$tmp_dir"
    export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
    export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
    export AUX_DUCKLAKE_RUNTIME_LIBRARY="$RUNTIME_LIBRARY"
    run_duckdb "
LOAD ducklake;
$(fdb_attach_sql "$tmp_dir")
$(cat "$SCENARIO_SQL")
DETACH dl;
$(fdb_attach_sql "$tmp_dir")
$(cat "$READBACK_SQL")
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "FoundationDB-backed mutation parity failed"
    }
    assert_expected_output "$duckdb_output"
    printf '%s\n' "$duckdb_output"
}

[[ -f "$SCENARIO_SQL" ]] || fail "missing scenario SQL: $SCENARIO_SQL"
[[ -f "$READBACK_SQL" ]] || fail "missing readback SQL: $READBACK_SQL"
[[ -f "$EXPECTED" ]] || fail "missing expected output: $EXPECTED"

build_ducklake_if_needed
build_runtime_library

tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

echo "mutations_parity_step=postgres_mutations"
run_postgres_mutations "$tmp_root/postgres"
fdb_prefix="aux-ducklake-e2e/mutations_parity-mutations/$(date +%s)/$$/"
echo "mutations_parity_step=fdb_mutations"
run_fdb_mutations "$tmp_root/fdb" "$fdb_prefix"
echo "ducklake_parity_postgres_fdb_mutations=ok"
