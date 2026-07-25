#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
OUT_DIR="${AUX_DUCKLAKE_BENCHMARK_OUTPUT_DIR:-$ROOT_DIR/docs/benchmarks/ducklake-fdb-feature-parity}"
BUILD_PROFILE="${AUX_DUCKLAKE_BENCHMARK_BUILD_PROFILE:-release}"
DUCKDB_BIN="$DUCKLAKE_DIR/build/$BUILD_PROFILE/duckdb"
POSTGRES_SCANNER_EXTENSION="$DUCKLAKE_DIR/build/$BUILD_PROFILE/extension/postgres_scanner/postgres_scanner.duckdb_extension"
BENCHMARK_BACKEND="${AUX_DUCKLAKE_BENCHMARK_BACKEND:-both}"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
. "$ROOT_DIR/scripts/ducklake-catalog-benchmark-fixtures.sh"

fail() {
    echo "ducklake catalog benchmark failure: $*" >&2
    exit 1
}

sql_literal() {
    local value="$1"
    printf "'%s'" "${value//\'/\'\'}"
}

now_micros() {
    python3 - <<'PY'
import time
print(time.time_ns() // 1000)
PY
}

elapsed_ms() {
    local started="$1" ended="$2"
    python3 - <<PY
print(f"{($ended - $started) / 1000:.3f}")
PY
}

copy_metric_snapshot() {
    local source="$1" target="$2"
    if [[ -n "$source" && -f "$source" ]]; then
        cp "$source" "$target"
    else
        : > "$target"
    fi
}

write_metric_accounting() {
    local output="$1" scope="$2" duration_ms="$3" runtime_before="$4" runtime_after="$5"
    python3 - "$output" "$scope" "$duration_ms" "$runtime_before" "$runtime_after" <<'PY'
import json
import re
import sys
from collections import defaultdict

output, scope, duration_ms, runtime_before, runtime_after = sys.argv[1:6]
duration_ms = float(duration_ms)
label_re = re.compile(r'(\w+)="([^"]*)"')


def parse_prom(path):
    metrics = defaultdict(float)
    try:
        handle = open(path, "r", encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return metrics
    with handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                metric, value = line.rsplit(" ", 1)
                value = float(value)
            except ValueError:
                continue
            name = metric.split("{", 1)[0]
            labels = tuple(sorted(label_re.findall(metric)))
            metrics[(name, labels)] += value
    return metrics


def delta(before_path, after_path):
    before = parse_prom(before_path)
    after = parse_prom(after_path)
    result = {}
    for key, value in after.items():
        changed = value - before.get(key, 0.0)
        if changed:
            result[key] = changed
    return result


def labels_dict(labels):
    return dict(labels)


runtime = delta(runtime_before, runtime_after)

runtime_top_level_us = 0.0
runtime_nested_us = 0.0
runtime_kv_us = 0.0
runtime_calls = 0.0
runtime_kv_rows = 0.0
runtime_kv_bytes = 0.0
for (name, labels_tuple), value in runtime.items():
    labels = labels_dict(labels_tuple)
    if labels.get("scope", "unscoped") != scope:
        continue
    family = labels.get("family", "")
    operation = labels.get("operation", "")
    if name == "aux_ducklake_runtime_request_elapsed_micros_total":
        if family == "kv":
            runtime_kv_us += value
        elif family in {"method", "measure"} or ":" in operation:
            runtime_nested_us += value
        else:
            runtime_top_level_us += value
    elif name == "aux_ducklake_runtime_requests_total":
        runtime_calls += value
    elif name == "aux_ducklake_runtime_kv_items_total":
        runtime_kv_rows += value
    elif name == "aux_ducklake_runtime_kv_bytes_total":
        runtime_kv_bytes += value

inside_rust_storage_ms = runtime_top_level_us / 1000.0
accounting = {
    "scope": scope,
    "scenario_wall_ms": duration_ms,
    "inside_rust_storage_ms": inside_rust_storage_ms,
    "inside_rust_storage_call_ms": runtime_top_level_us / 1000.0,
    "rust_runtime_reported_storage_ms": runtime_top_level_us / 1000.0,
    "inside_rust_nested_measurements_ms": runtime_nested_us / 1000.0,
    "inside_rust_fdb_kv_ms": runtime_kv_us / 1000.0,
    "measured_storage_wall_ms": inside_rust_storage_ms,
    "measured_storage_call_ms": runtime_top_level_us / 1000.0,
    "duckdb_extension_outside_storage_ms": duration_ms - inside_rust_storage_ms,
    "unaccounted_wall_ms": duration_ms - inside_rust_storage_ms,
    "runtime_metric_calls": int(runtime_calls),
    "fdb_rows_read": int(runtime_kv_rows),
    "fdb_bytes_read": int(runtime_kv_bytes),
}

with open(output, "w", encoding="utf-8") as handle:
    json.dump(accounting, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

benchmark_runtime_scope_enter() {
    local scope="$1"
    BENCHMARK_RUNTIME_METRICS_SCOPE_WAS_SET=0
    BENCHMARK_RUNTIME_METRICS_SCOPE_PREVIOUS=
    BENCHMARK_RUNTIME_READ_CONTEXT_WAS_SET=0
    BENCHMARK_RUNTIME_READ_CONTEXT_PREVIOUS=
    if [[ -n "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE+x}" ]]; then
        BENCHMARK_RUNTIME_METRICS_SCOPE_PREVIOUS="$AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE"
        BENCHMARK_RUNTIME_METRICS_SCOPE_WAS_SET=1
    fi
    if [[ -n "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT+x}" ]]; then
        BENCHMARK_RUNTIME_READ_CONTEXT_PREVIOUS="$AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT"
        BENCHMARK_RUNTIME_READ_CONTEXT_WAS_SET=1
    fi
    export AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE="$scope"
    export AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT=1
}

benchmark_runtime_scope_restore() {
    if [[ "${BENCHMARK_RUNTIME_METRICS_SCOPE_WAS_SET:-0}" == "1" ]]; then
        export AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE="$BENCHMARK_RUNTIME_METRICS_SCOPE_PREVIOUS"
    else
        unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_SCOPE || true
    fi
    if [[ "${BENCHMARK_RUNTIME_READ_CONTEXT_WAS_SET:-0}" == "1" ]]; then
        export AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT="$BENCHMARK_RUNTIME_READ_CONTEXT_PREVIOUS"
    else
        unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_READ_CONTEXT || true
    fi
}

run_duckdb_sql() {
    local sql="$1"
    set +e
    duckdb_output="$("$DUCKDB_BIN" -unsigned -csv -batch 2>&1 <<<"$sql")"
    duckdb_status=$?
    set -e
}

extract_label_from_output() {
    local output="$1" name="$2"
    awk -v name="$name" '
        {
            gsub(/\r/, "");
            gsub(/^"/, "");
            gsub(/"$/, "");
            if (index($0, name "=") == 1) {
                sub(name "=", "");
                print;
                exit;
            }
        }
    ' <<<"$output"
}

assert_label() {
    local output="$1" name="$2" expected="$3"
    local actual
    actual="$(extract_label_from_output "$output" "$name")"
    [[ "$actual" == "$expected" ]] || {
        printf '%s\n' "$output" >&2
        fail "$name mismatch: expected $expected, got ${actual:-<missing>}"
    }
}

profile="${1:-smoke}"
case "$profile" in
    inline)
        inline_table_count="${AUX_DUCKLAKE_INLINE_TABLES:-5}"
        inline_first_rows="${AUX_DUCKLAKE_INLINE_FIRST_ROWS:-5}"
        inline_second_rows="${AUX_DUCKLAKE_INLINE_SECOND_ROWS:-12}"
        inline_delete_rows="${AUX_DUCKLAKE_INLINE_DELETE_ROWS:-2}"
        inline_split_steps="${AUX_DUCKLAKE_INLINE_SPLIT_STEPS:-0}"
        inline_preload_tables="${AUX_DUCKLAKE_INLINE_PRELOAD_TABLES:-0}"
        inline_preload_rows="${AUX_DUCKLAKE_INLINE_PRELOAD_ROWS:-1}"
        inline_flush_interval="${AUX_DUCKLAKE_INLINE_FLUSH_INTERVAL:-end}"
        realistic_row_bytes="${AUX_DUCKLAKE_REALISTIC_ROW_BYTES:-4096}"
        scan_rows=0
        parallel_workers=1
        table_count="$inline_table_count"
        target_data_bytes=0
        ;;
    scan10)
        scan_rows=35
        parallel_workers=2
        table_count=2
        target_data_bytes=0
        ;;
    smoke)
        scan_rows=100
        parallel_workers=4
        table_count=2
        target_data_bytes=0
        ;;
    profile)
        scan_rows="${2:-10000}"
        parallel_workers=4
        table_count=2
        target_data_bytes=0
        ;;
    realistic)
        table_count="${AUX_DUCKLAKE_REALISTIC_TABLES:-50}"
        target_data_bytes="${AUX_DUCKLAKE_REALISTIC_TARGET_BYTES:-2147483648}"
        realistic_row_bytes="${AUX_DUCKLAKE_REALISTIC_ROW_BYTES:-4096}"
        scan_rows="${AUX_DUCKLAKE_REALISTIC_ROWS_PER_TABLE:-$(((target_data_bytes + table_count * realistic_row_bytes - 1) / (table_count * realistic_row_bytes)))}"
        parallel_workers="${AUX_DUCKLAKE_REALISTIC_PARALLEL_WORKERS:-8}"
        preload_batch_rows="${AUX_DUCKLAKE_REALISTIC_PRELOAD_BATCH_ROWS:-16384}"
        preload_workers="${AUX_DUCKLAKE_REALISTIC_PRELOAD_WORKERS:-1}"
        ;;
    varied)
        table_count="${AUX_DUCKLAKE_VARIED_TABLES:-100}"
        target_data_bytes="${AUX_DUCKLAKE_VARIED_TARGET_BYTES:-5368709120}"
        realistic_row_bytes="${AUX_DUCKLAKE_VARIED_ROW_BYTES:-4096}"
        scan_rows="${AUX_DUCKLAKE_VARIED_ROWS_PER_TABLE:-$(((target_data_bytes + table_count * realistic_row_bytes - 1) / (table_count * realistic_row_bytes)))}"
        parallel_workers="${AUX_DUCKLAKE_VARIED_PARALLEL_WORKERS:-12}"
        preload_batch_rows="${AUX_DUCKLAKE_VARIED_PRELOAD_BATCH_ROWS:-4096}"
        preload_workers="${AUX_DUCKLAKE_VARIED_PRELOAD_WORKERS:-4}"
        varied_churn_rounds="${AUX_DUCKLAKE_VARIED_CHURN_ROUNDS:-4}"
        varied_schema="${AUX_DUCKLAKE_VARIED_SCHEMA:-legacy}"
        varied_concurrent_writers="${AUX_DUCKLAKE_VARIED_CONCURRENT_WRITERS:-0}"
        ;;
    *) fail "usage: $0 scan10|smoke|profile [scan_rows]|realistic|varied|inline" ;;
esac

case "${inline_flush_interval:-end}" in
    each_batch | end | never) ;;
    *) fail "AUX_DUCKLAKE_INLINE_FLUSH_INTERVAL must be each_batch, end, or never" ;;
esac

case "${varied_schema:-legacy}" in
    legacy | mixed | narrow | wide) ;;
    *) fail "AUX_DUCKLAKE_VARIED_SCHEMA must be legacy, mixed, narrow, or wide" ;;
esac

[[ -x "$DUCKDB_BIN" ]] || fail "missing $BUILD_PROFILE DuckDB binary: $DUCKDB_BIN"
if [[ "$BENCHMARK_BACKEND" != "fdb" ]]; then
    [[ -f "$POSTGRES_SCANNER_EXTENSION" ]] || fail "missing $BUILD_PROFILE postgres_scanner helper extension: $POSTGRES_SCANNER_EXTENSION"
fi

mkdir -p "$OUT_DIR"

build_fdb_runtime() {
    local features="foundationdb"
    if [[ -n "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES:-}" ]]; then
        features="$features,${AUX_DUCKLAKE_BENCHMARK_RUNTIME_EXTRA_FEATURES}"
    fi
    AUX_DUCKLAKE_FDB_LIVE=1 "$ROOT_DIR/scripts/cargo_with_sccache.sh" build -q --release \
        -p ducklake-catalog --no-default-features --features "$features"
    FDB_RUNTIME_LIBRARY="$(ducklake_release_runtime_library "$ROOT_DIR")"
    [[ -f "$FDB_RUNTIME_LIBRARY" ]] || fail "foundationdb runtime library was not built: $FDB_RUNTIME_LIBRARY"
}

postgres_prepare_sql() {
    local dsn="$1" schema="$2"
    cat <<SQL
LOAD ducklake;
LOAD $(sql_literal "$POSTGRES_SCANNER_EXTENSION");
ATTACH $(sql_literal "$dsn") AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $schema CASCADE');
CALL postgres_execute('pg', 'CREATE SCHEMA $schema');
DETACH pg;
SQL
}

postgres_session_sql() {
    local retry_count="${AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT:-10}"
    cat <<SQL
LOAD ducklake;
LOAD $(sql_literal "$POSTGRES_SCANNER_EXTENSION");
SET ducklake_max_retry_count = $retry_count;
SQL
}

postgres_cleanup_sql() {
    local dsn="$1" schema="$2"
    cat <<SQL
ATTACH $(sql_literal "$dsn") AS pg (TYPE postgres);
CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS $schema CASCADE');
DETACH pg;
SQL
}

postgres_attach_sql() {
    local dsn="$1" schema="$2" data_path="$3" inline_limit="$4"
    cat <<SQL
ATTACH 'ducklake:postgres:$dsn' AS dl (
    DATA_PATH $(sql_literal "$data_path"),
    METADATA_SCHEMA $(sql_literal "$schema"),
    DATA_INLINING_ROW_LIMIT $inline_limit
);
SQL
}

fdb_prepare_sql() {
    local retry_count="${AUX_DUCKLAKE_BENCHMARK_DUCKLAKE_MAX_RETRY_COUNT:-10}"
    cat <<SQL
LOAD ducklake;
SET ducklake_max_retry_count = $retry_count;
SQL
}

fdb_attach_sql() {
    local metadata_path="$1" data_path="$2" inline_limit="$3"
    cat <<SQL
ATTACH 'ducklake:$metadata_path' AS dl (
    DATA_PATH $(sql_literal "$data_path"),
    DATA_INLINING_ROW_LIMIT $inline_limit,
    META_TYPE 'aux_catalog'
);
SQL
}

file_backed_workload_sql() {
    local rows="$1"
    cat <<SQL
CREATE TABLE dl.main.file_fact(
    id INTEGER,
    bucket VARCHAR,
    amount INTEGER,
    c03 INTEGER,
    c04 INTEGER,
    c05 INTEGER,
    c06 INTEGER,
    c07 INTEGER,
    c08 INTEGER,
    c09 INTEGER,
    c10 INTEGER,
    c11 INTEGER,
    c12 INTEGER,
    c13 INTEGER,
    c14 INTEGER,
    c15 INTEGER,
    c16 INTEGER,
    c17 INTEGER,
    c18 INTEGER,
    c19 INTEGER,
    c20 INTEGER,
    c21 INTEGER,
    c22 INTEGER,
    c23 VARCHAR
);
INSERT INTO dl.main.file_fact
SELECT i::INTEGER, CASE WHEN i % 3 = 0 THEN 'a' WHEN i % 3 = 1 THEN 'b' ELSE 'c' END,
       (i * 10)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'v_' || i::VARCHAR
FROM range(1, 6) t(i);
INSERT INTO dl.main.file_fact
SELECT i::INTEGER, CASE WHEN i % 3 = 0 THEN 'a' WHEN i % 3 = 1 THEN 'b' ELSE 'c' END,
       (i * 10)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'v_' || i::VARCHAR
FROM range(6, 16) t(i);
INSERT INTO dl.main.file_fact
SELECT i::INTEGER, CASE WHEN i % 3 = 0 THEN 'a' WHEN i % 3 = 1 THEN 'b' ELSE 'c' END,
       (i * 10)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'v_' || i::VARCHAR
FROM range(16, 36) t(i);
INSERT INTO dl.main.file_fact
SELECT i::INTEGER, CASE WHEN i % 3 = 0 THEN 'a' WHEN i % 3 = 1 THEN 'b' ELSE 'c' END,
       (i * 10)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'v_' || i::VARCHAR
FROM range(36, $((rows + 1))) t(i);
SET VARIABLE before_file_deletes = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.main.file_fact WHERE id = 3;
DELETE FROM dl.main.file_fact WHERE id IN (9, 10);
SELECT 'latest_scan=' || count(*) || ',' || coalesce(sum(id), 0) FROM dl.main.file_fact;
SELECT 'time_travel_scan=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.file_fact AT (VERSION => getvariable('before_file_deletes')::BIGINT);
CALL ducklake_merge_adjacent_files('dl', 'file_fact');
SET VARIABLE before_cleanup = (
    SELECT count(*) FROM ducklake_cleanup_old_files('dl', dry_run => true, cleanup_all => true)
);
CALL ducklake_cleanup_old_files('dl', dry_run => false, cleanup_all => true);
SELECT 'compaction_cleanup=' || count(*) || ',' || coalesce(sum(id), 0) || ',' ||
       getvariable('before_cleanup')::BIGINT || ',' ||
       (SELECT count(*) FROM ducklake_cleanup_old_files('dl', dry_run => true, cleanup_all => true))
FROM dl.main.file_fact;
SQL
}

inline_workload_sql() {
    cat <<'SQL'
CREATE TABLE dl.main.inline_fact(
    id INTEGER,
    bucket VARCHAR,
    amount INTEGER,
    c03 INTEGER,
    c04 INTEGER,
    c05 INTEGER,
    c06 INTEGER,
    c07 INTEGER,
    c08 INTEGER,
    c09 INTEGER,
    c10 INTEGER,
    c11 INTEGER,
    c12 INTEGER,
    c13 INTEGER,
    c14 INTEGER,
    c15 INTEGER,
    c16 INTEGER,
    c17 INTEGER,
    c18 INTEGER,
    c19 INTEGER,
    c20 INTEGER,
    c21 INTEGER,
    c22 INTEGER,
    c23 VARCHAR
);
INSERT INTO dl.main.inline_fact
SELECT i::INTEGER, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END,
       (i * 100)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'inline_' || i::VARCHAR
FROM range(1, 6) t(i);
INSERT INTO dl.main.inline_fact
SELECT i::INTEGER, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END,
       (i * 100)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'inline_' || i::VARCHAR
FROM range(6, 18) t(i);
INSERT INTO dl.main.inline_fact
SELECT i::INTEGER, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END,
       (i * 100)::INTEGER, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11,
       i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19, i+20, i+21, i+22,
       'inline_' || i::VARCHAR
FROM range(18, 38) t(i);
SET VARIABLE before_inline_deletes = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.main.inline_fact WHERE id = 2;
DELETE FROM dl.main.inline_fact WHERE id IN (11, 12);
SELECT 'inline_latest=' || count(*) || ',' || coalesce(sum(id), 0) FROM dl.main.inline_fact;
SELECT 'inline_time_travel=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.inline_fact AT (VERSION => getvariable('before_inline_deletes')::BIGINT);
CALL ducklake_flush_inlined_data('dl', table_name => 'inline_fact');
SELECT 'inline_after_flush=' || count(*) || ',' || coalesce(sum(id), 0) FROM dl.main.inline_fact;
SQL
}

parallel_worker_sql() {
    cat <<'SQL'
SELECT count(*), coalesce(sum(id), 0) FROM dl.main.file_fact;
SQL
}

parallel_readback_sql() {
    local workers="$1"
    cat <<SQL
SELECT 'parallel_latest=' || $workers || ',' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.file_fact;
SQL
}


backend_artifact() {
    local backend="$1" output="$2" generated="$3" elapsed="$4" labels_json="$5"
    cat > "$output" <<JSON
{
  "artifact": "ducklake-fdb-feature-parity-realistic-benchmark",
  "profile": "$profile",
  "generated_at_micros": $generated,
  "elapsed_ms": $elapsed,
  "fixture": {
    "backend": "$backend",
    "duckdb_build_profile": "$BUILD_PROFILE",
    "scan_rows": "$scan_rows",
    "parallel_workers": "$parallel_workers",
    "workload": "same-duckdb-sql"
  },
  "batches": [
    {
      "name": "same_duckdb_sql_workload",
      "duration_ms": $elapsed,
      "labels": $labels_json,
      "operation_counts": {
        "small_write_batches": 7,
        "narrow_delete_statements": 4,
        "parallel_workers": $parallel_workers
      },
      "transaction_estimates": {
        "columns_per_table": "24",
        "inline_insert_batch_rows": "5,12,20",
        "file_insert_batch_rows": "5,10,20,$((scan_rows - 35))"
      }
    }
  ]
}
JSON
}

run_backend() {
    local backend="$1" output="$2" tmp_dir="$3"
    local data_dir="$tmp_dir/data"
    mkdir -p "$data_dir"

    local prepare session_prepare attach_file attach_inline cleanup
    if [[ "$backend" == "postgres" ]]; then
        local dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
        local schema="ducklake_benchmark_${profile}_$(date +%s)_$$"
        unset AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY || true
        prepare="$(postgres_prepare_sql "$dsn" "$schema")"
        session_prepare="$(postgres_session_sql)"
        attach_file="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 0)"
        attach_inline="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 100)"
        cleanup="$(postgres_cleanup_sql "$dsn" "$schema")"
    else
        local fdb_prefix="aux-ducklake-benchmark/${profile}/$(date +%s)/$$/${backend}/"
        export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
        export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
        export AUX_DUCKLAKE_RUNTIME_LIBRARY="$FDB_RUNTIME_LIBRARY"
        export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$fdb_prefix"
        if [[ -z "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" ]]; then
            unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH || true
        fi
        prepare="$(fdb_prepare_sql)"
        session_prepare="$prepare"
        attach_file="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 0)"
        attach_inline="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 100)"
        cleanup=""
    fi

    local started ended elapsed all_output
    started="$(now_micros)"
    run_duckdb_sql "$prepare
$attach_file
$(file_backed_workload_sql "$scan_rows")
DETACH dl;
$attach_inline
$(inline_workload_sql)
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "$backend benchmark setup workload failed"
    }
    all_output="$duckdb_output"

    local worker_outputs=()
    local worker_pids=()
    for ((worker = 0; worker < parallel_workers; worker++)); do
        local worker_sql worker_out worker_attach
        worker_attach="$attach_file"
        if [[ "$backend" == "fdb" ]]; then
            worker_attach="$(fdb_attach_sql "$tmp_dir/worker-${worker}.duckdb" "$data_dir" 0)"
        fi
        worker_sql="$session_prepare
$worker_attach
$(parallel_worker_sql "$worker")
DETACH dl;
"
        worker_out="$tmp_dir/worker-${worker}.out"
        (
            "$DUCKDB_BIN" -unsigned -csv -batch >"$worker_out" 2>&1 <<<"$worker_sql"
        ) &
        worker_pids+=("$!")
        worker_outputs+=("$worker_out")
    done
    for index in "${!worker_pids[@]}"; do
        if ! wait "${worker_pids[$index]}"; then
            cat "${worker_outputs[$index]}" >&2
            fail "$backend parallel worker $index failed"
        fi
    done

    run_duckdb_sql "$session_prepare
$attach_file
$(parallel_readback_sql "$parallel_workers")
DETACH dl;
$cleanup
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "$backend benchmark readback failed"
    }
    all_output+=$'\n'"$duckdb_output"
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"

    local latest_expected=$((scan_rows * (scan_rows + 1) / 2 - 22))
    local time_travel_expected=$((scan_rows * (scan_rows + 1) / 2))
    assert_label "$all_output" "latest_scan" "$((scan_rows - 3)),$latest_expected"
    assert_label "$all_output" "time_travel_scan" "$scan_rows,$time_travel_expected"
    assert_label "$all_output" "inline_latest" "34,678"
    assert_label "$all_output" "inline_time_travel" "37,703"
    assert_label "$all_output" "inline_after_flush" "34,678"
    assert_label "$all_output" "parallel_latest" "$parallel_workers,$((scan_rows - 3)),$latest_expected"
    local compaction_cleanup
    compaction_cleanup="$(extract_label_from_output "$all_output" "compaction_cleanup")"
    [[ "$compaction_cleanup" == "$((scan_rows - 3)),$latest_expected,"*,0 ]] || {
        printf '%s\n' "$all_output" >&2
        fail "$backend compaction_cleanup mismatch: $compaction_cleanup"
    }

    local labels_json
    labels_json="$(python3 - <<PY
import json
labels = {
  "latest_scan": "$(extract_label_from_output "$all_output" "latest_scan")",
  "time_travel_scan": "$(extract_label_from_output "$all_output" "time_travel_scan")",
  "inline_latest": "$(extract_label_from_output "$all_output" "inline_latest")",
  "inline_time_travel": "$(extract_label_from_output "$all_output" "inline_time_travel")",
  "inline_after_flush": "$(extract_label_from_output "$all_output" "inline_after_flush")",
  "parallel_latest": "$(extract_label_from_output "$all_output" "parallel_latest")",
  "compaction_cleanup": "$compaction_cleanup",
}
print(json.dumps(labels, indent=8))
PY
)"
    backend_artifact "$backend" "$output" "$ended" "$elapsed" "$labels_json"
    echo "ducklake_fdb_feature_parity_${backend}_benchmark_artifact=$output"
}

append_realistic_batch_artifact() {
    local batches_file="$1" name="$2" duration="$3" output_file="$4" accounting_file="${5:-}"
    python3 - "$batches_file" "$name" "$duration" "$output_file" "$accounting_file" <<'PY'
import json
import sys

batches_file, name, duration, output_file, accounting_file = sys.argv[1:6]
labels = {}
with open(output_file, "r", encoding="utf-8", errors="replace") as handle:
    for raw in handle:
        line = raw.strip().strip('"')
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key and all(ch.isalnum() or ch == "_" for ch in key):
            labels[key] = value
batch = {
    "name": name,
    "duration_ms": float(duration),
    "labels": labels,
}
if accounting_file:
    try:
        with open(accounting_file, "r", encoding="utf-8") as handle:
            batch["accounting"] = json.load(handle)
    except FileNotFoundError:
        pass
with open(batches_file, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(batch, sort_keys=True))
    handle.write("\n")
PY
}

realistic_artifact() {
    local backend="$1" output="$2" generated="$3" elapsed="$4" batches_file="$5"
    local schema_columns
    schema_columns="$(realistic_schema_column_count)"
    python3 - "$backend" "$output" "$generated" "$elapsed" "$batches_file" "$profile" "$BUILD_PROFILE" "$scan_rows" "$parallel_workers" "$table_count" "$target_data_bytes" "${preload_batch_rows:-0}" "${preload_workers:-0}" "${varied_schema:-legacy}" "${realistic_row_bytes:-4096}" "${varied_concurrent_writers:-0}" "$schema_columns" <<'PY'
import json
import sys

backend, output, generated, elapsed, batches_file, profile, build_profile, rows, workers, tables, target_bytes, preload_batch_rows, preload_workers, schema_shape, payload_bytes, concurrent_writers, schema_columns = sys.argv[1:18]
with open(batches_file, "r", encoding="utf-8") as handle:
    batches = [json.loads(line) for line in handle if line.strip()]
component_batches = [batch["name"] for batch in batches]
artifact = {
    "artifact": "ducklake-fdb-feature-parity-realistic-component-benchmark",
    "profile": profile,
    "generated_at_micros": int(generated),
    "elapsed_ms": float(elapsed),
    "fixture": {
        "backend": backend,
        "duckdb_build_profile": build_profile,
        "same_sql_for_backends": True,
        "table_count": int(tables),
        "rows_per_table": int(rows),
        "target_logical_data_bytes": int(target_bytes),
        "parallel_workers": int(workers),
        "columns_per_table": int(schema_columns),
        "preload_batch_rows": int(preload_batch_rows),
        "small_write_batch_rows": "5-20",
        "preload_parallelism": int(preload_workers),
        "schema_shape": schema_shape,
        "payload_bytes": int(payload_bytes),
        "concurrent_writers": int(concurrent_writers),
        "component_batches": component_batches,
    },
    "batches": batches,
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

inline_operation_names() {
    if [[ "${inline_flush_interval:-end}" == "never" ]]; then
        printf '["create_inline_table","insert_inline_rows_first_batch","insert_inline_rows_second_batch","delete_inline_rows","read_current_rows"]\n'
        return
    fi
    printf '["create_inline_table","insert_inline_rows_first_batch","insert_inline_rows_second_batch","delete_inline_rows","flush_inlined_data","read_current_rows"]\n'
}

inline_micro_artifact() {
    local backend="$1" output="$2" generated="$3" elapsed="$4" batches_file="$5"
    local operation_names
    operation_names="$(inline_operation_names)"
    python3 - "$backend" "$output" "$generated" "$elapsed" "$batches_file" "$profile" "$BUILD_PROFILE" "$inline_table_count" "$inline_first_rows" "$inline_second_rows" "$inline_delete_rows" "${inline_split_steps:-0}" "${inline_preload_tables:-0}" "${inline_preload_rows:-0}" "${inline_flush_interval:-end}" "$operation_names" <<'PY'
import json
import sys

backend, output, generated, elapsed, batches_file, profile, build_profile, tables, first_rows, second_rows, delete_rows, split_steps, preload_tables, preload_rows, flush_interval, operation_names = sys.argv[1:17]
with open(batches_file, "r", encoding="utf-8") as handle:
    batches = [json.loads(line) for line in handle if line.strip()]
artifact = {
    "artifact": "ducklake-fdb-feature-parity-inline-micro-benchmark",
    "profile": profile,
    "generated_at_micros": int(generated),
    "elapsed_ms": float(elapsed),
    "fixture": {
        "backend": backend,
        "duckdb_build_profile": build_profile,
        "same_sql_for_backends": True,
        "table_count": int(tables),
        "first_insert_rows_per_table": int(first_rows),
        "second_insert_rows_per_table": int(second_rows),
        "deleted_rows_per_table": int(delete_rows),
        "split_steps": split_steps == "1",
        "flush_interval": flush_interval,
        "preload_table_count": int(preload_tables),
        "preload_rows_per_table": int(preload_rows),
        "operations_per_table": json.loads(operation_names),
    },
    "batches": batches,
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

run_realistic_batch() {
    local backend="$1" batch_name="$2" output_file="$3" sql="$4" session_prepare="$5" attach="$6"
    local started ended elapsed
    local runtime_before runtime_after accounting_file
    benchmark_runtime_scope_enter "$batch_name"
    runtime_before="$(mktemp)"
    runtime_after="$(mktemp)"
    accounting_file="$(mktemp)"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_before"
    started="$(now_micros)"
    run_duckdb_sql "$session_prepare
$attach
$sql
DETACH dl;
"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_after"
    benchmark_runtime_scope_restore
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"
    write_metric_accounting "$accounting_file" "$batch_name" "$elapsed" "$runtime_before" "$runtime_after"
    printf '%s\n' "$duckdb_output" > "$output_file"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "$backend realistic batch $batch_name failed"
    }
    REALISTIC_LAST_BATCH_MS="$elapsed"
    REALISTIC_LAST_ACCOUNTING_FILE="$accounting_file"
    rm -f "$runtime_before" "$runtime_after"
}

run_inline_micro_backend() {
    local backend="$1" output="$2" tmp_dir="$3"
    local data_dir="$tmp_dir/data"
    mkdir -p "$data_dir"

    local prepare session_prepare attach_file attach_inline cleanup
    if [[ "$backend" == "postgres" ]]; then
        local dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
        local schema="ducklake_inline_${profile}_$(date +%s)_$$"
        unset AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY || true
        prepare="$(postgres_prepare_sql "$dsn" "$schema")"
        session_prepare="$(postgres_session_sql)"
        attach_file="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 0)"
        attach_inline="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 100)"
        cleanup="$(postgres_cleanup_sql "$dsn" "$schema")"
    else
        local fdb_prefix="aux-ducklake-benchmark/${profile}/$(date +%s)/$$/${backend}/"
        export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
        export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
        export AUX_DUCKLAKE_RUNTIME_LIBRARY="$FDB_RUNTIME_LIBRARY"
        export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$fdb_prefix"
        if [[ -z "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" ]]; then
            unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH || true
        fi
        prepare="$(fdb_prepare_sql)"
        session_prepare="$prepare"
        attach_file="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 0)"
        attach_inline="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 100)"
        cleanup=""
    fi

    local batches_file="$tmp_dir/batches.jsonl"
    : > "$batches_file"
    local started ended elapsed table batch_out
    started="$(now_micros)"
    if [[ "${inline_preload_tables:-0}" -gt 0 ]]; then
        batch_out="$tmp_dir/inline-preload.out"
        run_realistic_batch "$backend" "preload_catalog_shape" "$batch_out" "$(realistic_preload_sql "$inline_preload_tables" "$inline_preload_rows")" "$prepare" "$attach_file"
        append_realistic_batch_artifact "$batches_file" "preload_catalog_shape" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"
    fi
    for ((table = 0; table < inline_table_count; table++)); do
        if [[ "${inline_split_steps:-0}" == "1" ]]; then
            local step
            for step in create insert_first insert_second delete flush_read; do
                batch_out="$tmp_dir/inline-${table}-${step}.out"
                run_realistic_batch "$backend" "inline_table_${table}_${step}" "$batch_out" "$(inline_micro_step_sql "$step" "$table" "$inline_first_rows" "$inline_second_rows" "$inline_delete_rows")" "$session_prepare" "$attach_inline"
                append_realistic_batch_artifact "$batches_file" "inline_table_${table}_${step}" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"
            done
        else
            batch_out="$tmp_dir/inline-${table}.out"
            run_realistic_batch "$backend" "inline_table_${table}" "$batch_out" "$(inline_micro_table_sql "$table" "$inline_first_rows" "$inline_second_rows" "$inline_delete_rows")" "$session_prepare" "$attach_inline"
            append_realistic_batch_artifact "$batches_file" "inline_table_${table}" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"
        fi
    done
    if [[ -n "$cleanup" ]]; then
        run_duckdb_sql "$session_prepare
$cleanup"
    fi
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"
    inline_micro_artifact "$backend" "$output" "$ended" "$elapsed" "$batches_file"
    echo "ducklake_fdb_feature_parity_${backend}_inline_benchmark_artifact=$output"
}

run_realistic_preload() {
    local backend="$1" output_file="$2" prepare_once="$3" session_prepare="$4" attach="$5" tmp_dir="$6" data_dir="$7"
    local started ended elapsed schema_out worker worker_sql worker_out worker_attach
    local runtime_before runtime_after accounting_file
    local worker_pids=()
    local worker_outputs=()

    benchmark_runtime_scope_enter preload
    runtime_before="$(mktemp)"
    runtime_after="$(mktemp)"
    accounting_file="$(mktemp)"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_before"
    started="$(now_micros)"
    run_duckdb_sql "$prepare_once
$attach
$(realistic_schema_sql "$table_count")
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || {
        printf '%s\n' "$duckdb_output" >&2
        fail "$backend realistic preload schema failed"
    }
    schema_out="$tmp_dir/preload-schema.out"
    printf '%s\n' "$duckdb_output" > "$schema_out"

    : > "$output_file"
    cat "$schema_out" >> "$output_file"
    for ((worker = 0; worker < preload_workers; worker++)); do
        worker_attach="$attach"
        if [[ "$backend" == "fdb" ]]; then
            worker_attach="$(fdb_attach_sql "$tmp_dir/preload-worker-${worker}.duckdb" "$data_dir" 0)"
        fi
        worker_sql="$session_prepare
$worker_attach
$(realistic_preload_worker_sql "$table_count" "$scan_rows" "$worker" "$preload_workers" "$preload_batch_rows")
DETACH dl;
"
        worker_out="$tmp_dir/preload-worker-${worker}.out"
        (
            "$DUCKDB_BIN" -unsigned -csv -batch >"$worker_out" 2>&1 <<<"$worker_sql"
        ) &
        worker_pids+=("$!")
        worker_outputs+=("$worker_out")
    done
    for index in "${!worker_pids[@]}"; do
        if ! wait "${worker_pids[$index]}"; then
            cat "${worker_outputs[$index]}" >&2
            fail "$backend realistic preload worker $index failed"
        fi
        cat "${worker_outputs[$index]}" >> "$output_file"
    done
    printf 'realistic_preload=%s,%s,%s,%s\n' "$table_count" "$scan_rows" "$((table_count * scan_rows))" "$((table_count * scan_rows * realistic_row_bytes))" >> "$output_file"
    printf 'realistic_preload_parallelism=%s\n' "$preload_workers" >> "$output_file"
    printf 'realistic_preload_batch_rows=%s\n' "$preload_batch_rows" >> "$output_file"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_after"
    benchmark_runtime_scope_restore
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"
    write_metric_accounting "$accounting_file" "preload" "$elapsed" "$runtime_before" "$runtime_after"
    REALISTIC_LAST_BATCH_MS="$elapsed"
    REALISTIC_LAST_ACCOUNTING_FILE="$accounting_file"
    rm -f "$runtime_before" "$runtime_after"
}

launch_concurrent_reader() {
    local backend="$1" worker="$2" tmp_dir="$3"
    local worker_attach="$attach_file"
    if [[ "$backend" == "fdb" ]]; then
        worker_attach="$(fdb_attach_sql "$tmp_dir/concurrent-reader-${worker}.duckdb" "$data_dir" 0)"
    fi
    local worker_sql="$session_prepare
$worker_attach
$(realistic_latest_query_sql "$table_count")
DETACH dl;
"
    local worker_out="$tmp_dir/concurrent-reader-${worker}.out"
    ("$DUCKDB_BIN" -unsigned -csv -batch > "$worker_out" 2>&1 <<< "$worker_sql") &
    worker_pids+=("$!")
    worker_outputs+=("$worker_out")
}

launch_concurrent_writer() {
    local backend="$1" worker="$2" tmp_dir="$3"
    local worker_attach="$attach_file"
    if [[ "$backend" == "fdb" ]]; then
        worker_attach="$(fdb_attach_sql "$tmp_dir/concurrent-writer-${worker}.duckdb" "$data_dir" 0)"
    fi
    local worker_sql="$session_prepare
$worker_attach
$(varied_concurrent_writer_sql "$table_count" "$scan_rows" "$worker")
DETACH dl;
"
    local worker_out="$tmp_dir/concurrent-writer-${worker}.out"
    ("$DUCKDB_BIN" -unsigned -csv -batch > "$worker_out" 2>&1 <<< "$worker_sql") &
    worker_pids+=("$!")
    worker_outputs+=("$worker_out")
}

await_concurrent_workers() {
    local backend="$1" output_file="$2" index
    for index in "${!worker_pids[@]}"; do
        if ! wait "${worker_pids[$index]}"; then
            cat "${worker_outputs[$index]}" >&2
            fail "$backend concurrent read/write worker $index failed"
        fi
        cat "${worker_outputs[$index]}" >> "$output_file"
    done
}

record_concurrent_readback() {
    local backend="$1" output_file="$2"
    run_duckdb_sql "$session_prepare
$attach_file
$(realistic_latest_query_sql "$table_count")
DETACH dl;
"
    [[ "$duckdb_status" -eq 0 ]] || fail "$backend concurrent read/write readback failed"
    printf '%s\n' "$duckdb_output" >> "$output_file"
    printf 'concurrent_readers=%s\nconcurrent_writers=%s\n' "$parallel_workers" "$varied_concurrent_writers" >> "$output_file"
}

run_concurrent_read_write() {
    local backend="$1" output_file="$2" tmp_dir="$3"
    local started ended elapsed worker runtime_before runtime_after accounting_file
    local worker_pids=()
    local worker_outputs=()
    benchmark_runtime_scope_enter concurrent_read_write
    runtime_before="$(mktemp)"
    runtime_after="$(mktemp)"
    accounting_file="$(mktemp)"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_before"
    : > "$output_file"
    started="$(now_micros)"
    for ((worker = 0; worker < parallel_workers; worker++)); do
        launch_concurrent_reader "$backend" "$worker" "$tmp_dir"
    done
    for ((worker = 0; worker < varied_concurrent_writers; worker++)); do
        launch_concurrent_writer "$backend" "$worker" "$tmp_dir"
    done
    await_concurrent_workers "$backend" "$output_file"
    record_concurrent_readback "$backend" "$output_file"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$runtime_after"
    benchmark_runtime_scope_restore
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"
    write_metric_accounting "$accounting_file" "concurrent_read_write" "$elapsed" "$runtime_before" "$runtime_after"
    REALISTIC_LAST_BATCH_MS="$elapsed"
    REALISTIC_LAST_ACCOUNTING_FILE="$accounting_file"
    rm -f "$runtime_before" "$runtime_after"
}

run_realistic_backend() {
    local backend="$1" output="$2" tmp_dir="$3"
    local data_dir="$tmp_dir/data"
    mkdir -p "$data_dir"

    local prepare session_prepare attach_file attach_inline cleanup
    if [[ "$backend" == "postgres" ]]; then
        local dsn="${AUX_DUCKLAKE_POSTGRES_DSN:-dbname=postgres}"
        local schema="ducklake_realistic_${profile}_$(date +%s)_$$"
        unset AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY || true
        prepare="$(postgres_prepare_sql "$dsn" "$schema")"
        session_prepare="$(postgres_session_sql)"
        attach_file="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 0)"
        attach_inline="$(postgres_attach_sql "$dsn" "$schema" "$data_dir" 100)"
        cleanup="$(postgres_cleanup_sql "$dsn" "$schema")"
    else
        local fdb_prefix="aux-ducklake-benchmark/${profile}/$(date +%s)/$$/${backend}/"
        export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
        export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
        export AUX_DUCKLAKE_RUNTIME_LIBRARY="$FDB_RUNTIME_LIBRARY"
        export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$fdb_prefix"
        if [[ -z "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" ]]; then
            unset AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH || true
        fi
        prepare="$(fdb_prepare_sql)"
        session_prepare="$prepare"
        attach_file="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 0)"
        attach_inline="$(fdb_attach_sql "$tmp_dir/metadata.duckdb" "$data_dir" 100)"
        cleanup=""
    fi

    local batches_file="$tmp_dir/batches.jsonl"
    : > "$batches_file"
    local started ended elapsed batch_out
    started="$(now_micros)"

    batch_out="$tmp_dir/preload.out"
    run_realistic_preload "$backend" "$batch_out" "$prepare" "$session_prepare" "$attach_file" "$tmp_dir" "$data_dir"
    append_realistic_batch_artifact "$batches_file" "preload" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    if [[ "$profile" == "varied" && "${AUX_DUCKLAKE_VARIED_CHURN_ONLY_AFTER_PRELOAD:-0}" == "1" ]]; then
        : > "$batches_file"
        started="$(now_micros)"
        batch_out="$tmp_dir/mutation_churn.out"
        run_realistic_batch "$backend" "mutation_churn" "$batch_out" "$(varied_churn_sql "$table_count" "$scan_rows" "$varied_churn_rounds")" "$session_prepare" "$attach_file"
        append_realistic_batch_artifact "$batches_file" "mutation_churn" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

        if [[ -n "$cleanup" ]]; then
            run_duckdb_sql "$session_prepare
$cleanup"
        fi
        ended="$(now_micros)"
        elapsed="$(elapsed_ms "$started" "$ended")"
        realistic_artifact "$backend" "$output" "$ended" "$elapsed" "$batches_file"
        echo "ducklake_fdb_feature_parity_${backend}_realistic_benchmark_artifact=$output"
        return
    fi

    batch_out="$tmp_dir/mixed.out"
    run_realistic_batch "$backend" "mixed" "$batch_out" "$(realistic_mixed_sql "$table_count" "$scan_rows")" "$session_prepare" "$attach_file"
    append_realistic_batch_artifact "$batches_file" "mixed" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    batch_out="$tmp_dir/deletes.out"
    run_realistic_batch "$backend" "dedicated_deletes" "$batch_out" "$(realistic_delete_sql "$table_count")" "$session_prepare" "$attach_file"
    append_realistic_batch_artifact "$batches_file" "dedicated_deletes" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    batch_out="$tmp_dir/inlining.out"
    run_realistic_batch "$backend" "dedicated_inlining" "$batch_out" "$(realistic_inline_sql "$table_count")" "$session_prepare" "$attach_inline"
    append_realistic_batch_artifact "$batches_file" "dedicated_inlining" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    batch_out="$tmp_dir/compaction.out"
    run_realistic_batch "$backend" "dedicated_compaction" "$batch_out" "$(realistic_compaction_sql "$table_count")" "$session_prepare" "$attach_file"
    append_realistic_batch_artifact "$batches_file" "dedicated_compaction" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    if [[ "$profile" == "varied" ]]; then
        batch_out="$tmp_dir/join_queries.out"
        run_realistic_batch "$backend" "join_queries" "$batch_out" "SET VARIABLE varied_join_snapshot = (SELECT id FROM ducklake_current_snapshot('dl'));
$(varied_join_query_sql "$table_count")
$(varied_join_query_sql "$table_count" "varied_join_snapshot")" "$session_prepare" "$attach_file"
        append_realistic_batch_artifact "$batches_file" "join_queries" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

        batch_out="$tmp_dir/mutation_churn.out"
        run_realistic_batch "$backend" "mutation_churn" "$batch_out" "$(varied_churn_sql "$table_count" "$scan_rows" "$varied_churn_rounds")" "$session_prepare" "$attach_file"
        append_realistic_batch_artifact "$batches_file" "mutation_churn" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

        if [[ "$varied_concurrent_writers" -gt 0 ]]; then
            batch_out="$tmp_dir/concurrent-read-write.out"
            run_concurrent_read_write "$backend" "$batch_out" "$tmp_dir"
            append_realistic_batch_artifact "$batches_file" "concurrent_read_write" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"
        fi
    fi

    batch_out="$tmp_dir/latest.out"
    run_realistic_batch "$backend" "latest_queries" "$batch_out" "$(realistic_latest_query_sql "$table_count")" "$session_prepare" "$attach_file"
    append_realistic_batch_artifact "$batches_file" "latest_queries" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    batch_out="$tmp_dir/time-travel.out"
    run_realistic_batch "$backend" "time_travel_queries" "$batch_out" "SET VARIABLE realistic_query_snapshot = (SELECT id FROM ducklake_current_snapshot('dl'));
$(realistic_time_travel_query_sql "$table_count" "realistic_query_snapshot")" "$session_prepare" "$attach_file"
    append_realistic_batch_artifact "$batches_file" "time_travel_queries" "$REALISTIC_LAST_BATCH_MS" "$batch_out" "$REALISTIC_LAST_ACCOUNTING_FILE"

    batch_out="$tmp_dir/parallel.out"
    : > "$batch_out"
    local parallel_started parallel_ended parallel_elapsed worker worker_sql worker_out worker_attach
    local parallel_runtime_before parallel_runtime_after parallel_accounting_file
    local worker_pids=()
    local worker_outputs=()
    benchmark_runtime_scope_enter parallel_latest_queries
    parallel_runtime_before="$(mktemp)"
    parallel_runtime_after="$(mktemp)"
    parallel_accounting_file="$(mktemp)"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$parallel_runtime_before"
    parallel_started="$(now_micros)"
    for ((worker = 0; worker < parallel_workers; worker++)); do
        worker_attach="$attach_file"
        if [[ "$backend" == "fdb" ]]; then
            worker_attach="$(fdb_attach_sql "$tmp_dir/worker-${worker}.duckdb" "$data_dir" 0)"
        fi
        worker_sql="$session_prepare
$worker_attach
$(realistic_latest_query_sql "$table_count")
DETACH dl;
"
        worker_out="$tmp_dir/parallel-worker-${worker}.out"
        (
            "$DUCKDB_BIN" -unsigned -csv -batch >"$worker_out" 2>&1 <<<"$worker_sql"
        ) &
        worker_pids+=("$!")
        worker_outputs+=("$worker_out")
    done
    for index in "${!worker_pids[@]}"; do
        if ! wait "${worker_pids[$index]}"; then
            cat "${worker_outputs[$index]}" >&2
            fail "$backend realistic parallel worker $index failed"
        fi
        cat "${worker_outputs[$index]}" >> "$batch_out"
    done
    parallel_ended="$(now_micros)"
    parallel_elapsed="$(elapsed_ms "$parallel_started" "$parallel_ended")"
    copy_metric_snapshot "${AUX_DUCKLAKE_BENCHMARK_RUNTIME_METRICS_PATH:-}" "$parallel_runtime_after"
    benchmark_runtime_scope_restore
    write_metric_accounting "$parallel_accounting_file" "parallel_latest_queries" "$parallel_elapsed" "$parallel_runtime_before" "$parallel_runtime_after"
    printf 'realistic_parallel_workers=%s\n' "$parallel_workers" >> "$batch_out"
    append_realistic_batch_artifact "$batches_file" "parallel_latest_queries" "$parallel_elapsed" "$batch_out" "$parallel_accounting_file"
    rm -f "$parallel_runtime_before" "$parallel_runtime_after"

    if [[ -n "$cleanup" ]]; then
        run_duckdb_sql "$session_prepare
$cleanup"
    fi
    ended="$(now_micros)"
    elapsed="$(elapsed_ms "$started" "$ended")"
    realistic_artifact "$backend" "$output" "$ended" "$elapsed" "$batches_file"
    echo "ducklake_fdb_feature_parity_${backend}_realistic_benchmark_artifact=$output"
}

case "$BENCHMARK_BACKEND" in
    both | fdb | postgres) ;;
    *) fail "AUX_DUCKLAKE_BENCHMARK_BACKEND must be both, fdb, or postgres" ;;
esac

if [[ "$BENCHMARK_BACKEND" != "postgres" ]]; then
    build_fdb_runtime
fi
tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

case "$profile" in
    scan10)
        fdb_output="$OUT_DIR/fdb-scan10-latest.json"
        postgres_output="$OUT_DIR/postgres-scan10-latest.json"
        ;;
    smoke)
        fdb_output="$OUT_DIR/fdb-smoke-latest.json"
        postgres_output="$OUT_DIR/postgres-smoke-latest.json"
        ;;
    profile)
        fdb_output="$OUT_DIR/fdb-profile-latest.json"
        postgres_output="$OUT_DIR/postgres-profile-latest.json"
        ;;
    realistic)
        fdb_output="$OUT_DIR/fdb-realistic-latest.json"
        postgres_output="$OUT_DIR/postgres-realistic-latest.json"
        ;;
    varied)
        fdb_output="$OUT_DIR/fdb-varied-latest.json"
        postgres_output="$OUT_DIR/postgres-varied-latest.json"
        ;;
    inline)
        fdb_output="$OUT_DIR/fdb-inline-latest.json"
        postgres_output="$OUT_DIR/postgres-inline-latest.json"
        ;;
esac

if [[ "$profile" == "inline" ]]; then
    if [[ "$BENCHMARK_BACKEND" != "postgres" ]]; then
        run_inline_micro_backend "fdb" "$fdb_output" "$tmp_root/fdb"
    fi
    if [[ "$BENCHMARK_BACKEND" != "fdb" ]]; then
        run_inline_micro_backend "postgres" "$postgres_output" "$tmp_root/postgres"
    fi
elif [[ "$profile" == "realistic" || "$profile" == "varied" ]]; then
    if [[ "$BENCHMARK_BACKEND" != "postgres" ]]; then
        run_realistic_backend "fdb" "$fdb_output" "$tmp_root/fdb"
    fi
    if [[ "$BENCHMARK_BACKEND" != "fdb" ]]; then
        run_realistic_backend "postgres" "$postgres_output" "$tmp_root/postgres"
    fi
else
    if [[ "$BENCHMARK_BACKEND" != "postgres" ]]; then
        run_backend "fdb" "$fdb_output" "$tmp_root/fdb"
    fi
    if [[ "$BENCHMARK_BACKEND" != "fdb" ]]; then
        run_backend "postgres" "$postgres_output" "$tmp_root/postgres"
    fi
fi
