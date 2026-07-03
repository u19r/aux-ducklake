#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"
POSTGRES_SCANNER_EXTENSION="$DUCKLAKE_DIR/build/debug/extension/postgres_scanner/postgres_scanner.duckdb_extension"
SCENARIO_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/core_smoke.sql"
READBACK_SQL="$ROOT_DIR/docs/parity/ducklake-fdb/sql/core_smoke_readback.sql"
EXPECTED="$ROOT_DIR/docs/parity/ducklake-fdb/expected/core_smoke.out"

fail() {
    echo "ducklake postgres parity smoke failure: $*" >&2
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
    local line
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        [[ "$duckdb_output" == *"$line"* ]] || fail "expected output to contain: $line"
    done < "$EXPECTED"
}

[[ -f "$SCENARIO_SQL" ]] || fail "missing scenario SQL: $SCENARIO_SQL"
[[ -f "$READBACK_SQL" ]] || fail "missing readback SQL: $READBACK_SQL"
[[ -f "$EXPECTED" ]] || fail "missing expected output: $EXPECTED"

ducklake_build_debug_duckdb_with_postgres_if_needed "$ROOT_DIR" "$DUCKLAKE_DIR" "$DUCKDB_BIN" "$POSTGRES_SCANNER_EXTENSION" ||
    fail "modified DuckDB with postgres_scanner was not built"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

postgres_dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
metadata_schema="ducklake_parity_$(date +%s)_$$"
data_path="$tmp_dir/data"
mkdir -p "$data_path"

dsn_literal="$(sql_literal "$postgres_dsn")"
schema_literal="$(sql_literal "$metadata_schema")"
data_path_literal="$(sql_literal "$data_path")"
postgres_scanner_literal="$(sql_literal "$POSTGRES_SCANNER_EXTENSION")"

run_duckdb "
LOAD ducklake;
LOAD $postgres_scanner_literal;
ATTACH $dsn_literal AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $metadata_schema CASCADE');
CALL postgres_execute('pg', 'CREATE SCHEMA $metadata_schema');
DETACH pg;
ATTACH 'ducklake:postgres:$postgres_dsn' AS dl (
    DATA_PATH $data_path_literal,
    METADATA_SCHEMA $schema_literal,
    DATA_INLINING_ROW_LIMIT 0
);
$(cat "$SCENARIO_SQL")
DETACH dl;
ATTACH 'ducklake:postgres:$postgres_dsn' AS dl (
    DATA_PATH $data_path_literal,
    METADATA_SCHEMA $schema_literal,
    DATA_INLINING_ROW_LIMIT 0
);
$(cat "$READBACK_SQL")
DETACH dl;
ATTACH $dsn_literal AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $metadata_schema CASCADE');
DETACH pg;
"

if [[ "$duckdb_status" -ne 0 ]]; then
    printf '%s\n' "$duckdb_output" >&2
    fail "Postgres-backed DuckLake core smoke failed; set AUX_DUCKLAKE_POSTGRES_DSN if the default dbname=postgres is not valid"
fi

assert_expected_output
printf '%s\n' "$duckdb_output"
echo "ducklake_parity_postgres_smoke=ok"
