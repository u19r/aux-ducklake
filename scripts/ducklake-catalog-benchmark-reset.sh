#!/usr/bin/env bash
set -euo pipefail

fail() {
    echo "ducklake catalog benchmark reset failure: $*" >&2
    exit 1
}

resolve_psql() {
    if command -v psql >/dev/null 2>&1; then
        command -v psql
        return
    fi
    if command -v brew >/dev/null 2>&1; then
        local libpq_prefix
        libpq_prefix="$(brew --prefix libpq 2>/dev/null || true)"
        if [[ -x "$libpq_prefix/bin/psql" ]]; then
            printf '%s\n' "$libpq_prefix/bin/psql"
            return
        fi
    fi
    fail "psql is required"
}

reset_foundationdb() {
    local cluster_file="${AUX_DUCKLAKE_FDB_CLUSTER_FILE:-}"
    [[ -f "$cluster_file" ]] ||
        fail "AUX_DUCKLAKE_FDB_CLUSTER_FILE must name the proxy-backed cluster file"
    rg -q '@127[.]0[.]0[.]1:14691$' "$cluster_file" ||
        fail "FoundationDB benchmark reset must use Toxiproxy port 14691"

    fdbcli -C "$cluster_file" --exec 'writemode on; clearrange "" \xff' >/dev/null
    local remaining
    remaining="$(
        fdbcli -C "$cluster_file" --exec 'getrange "" \xff 1' |
            sed '/^[[:space:]]*$/d; /^Range limited to 1 keys$/d'
    )"
    [[ -z "$remaining" ]] || fail "FoundationDB is not empty after reset"
}

reset_postgres() {
    local dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-}"
    [[ -n "$dsn" ]] || fail "AUX_DUCKLAKE_POSTGRES_DSN is required"
    [[ "$dsn" == *"port=15432"* || "$dsn" == *":15432/"* ]] ||
        fail "Postgres benchmark reset must use Toxiproxy port 15432"

    local psql_bin
    psql_bin="$(resolve_psql)"
    "$psql_bin" "$dsn" -X -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
DO $$
DECLARE
    schema_name text;
BEGIN
    FOR schema_name IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname <> 'information_schema'
          AND nspname NOT LIKE 'pg_%'
    LOOP
        EXECUTE format('DROP SCHEMA %I CASCADE', schema_name);
    END LOOP;
END
$$;
CREATE SCHEMA public;
SQL

    local remaining
    remaining="$("$psql_bin" "$dsn" -X -Atqc 'SELECT count(*) FROM pg_stat_user_tables')"
    [[ "$remaining" == "0" ]] || fail "Postgres has $remaining user tables after reset"
}

case "${AUX_DUCKLAKE_BENCHMARK_BACKEND:-both}" in
    both)
        reset_foundationdb
        reset_postgres
        ;;
    fdb) reset_foundationdb ;;
    postgres) reset_postgres ;;
    *) fail "AUX_DUCKLAKE_BENCHMARK_BACKEND must be both, fdb, or postgres" ;;
esac

