#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
benchmark="$root_dir/scripts/ducklake_catalog_benchmark.sh"
artifact_dir="${AUX_DUCKLAKE_BENCHMARK_OUTPUT_DIR:-$root_dir/docs/benchmarks/ducklake-fdb-feature-parity}"
benchmark_backend="${AUX_DUCKLAKE_BENCHMARK_BACKEND:-both}"
output="$artifact_dir/robustness-latest.json"
tmp_dir="$(mktemp -d)"
benchmark_output_dir="$tmp_dir/component-artifacts"
trap 'rm -rf "$tmp_dir"' EXIT

fail() {
    echo "ducklake robustness benchmark failure: $*" >&2
    exit 1
}

require_environment() {
    command -v jq >/dev/null 2>&1 || fail "jq is required"
    case "$benchmark_backend" in
    both | fdb | postgres) ;;
    *) fail "AUX_DUCKLAKE_BENCHMARK_BACKEND must be both, fdb, or postgres" ;;
    esac
    if [[ "$benchmark_backend" != postgres ]]; then
        [[ -n "${AUX_DUCKLAKE_FDB_CLUSTER_FILE:-}" ]] || fail "AUX_DUCKLAKE_FDB_CLUSTER_FILE is required"
    fi
    if [[ "$benchmark_backend" != fdb ]]; then
        [[ -n "${AUX_DUCKLAKE_POSTGRES_DSN:-}" ]] || fail "AUX_DUCKLAKE_POSTGRES_DSN is required"
    fi
}

run_backend() {
    local backend="$1" profile="$2" target="$3"
    local log="$target.log"
    if AUX_DUCKLAKE_BENCHMARK_BACKEND="$backend" \
        AUX_DUCKLAKE_BENCHMARK_OUTPUT_DIR="$benchmark_output_dir" \
        "$benchmark" "$profile" >"$log" 2>&1; then
        cat "$log"
        cp "$benchmark_output_dir/$backend-$profile-latest.json" "$target"
        return
    fi
    cat "$log" >&2
    jq -Rs \
        --arg backend "$backend" \
        --arg profile "$profile" \
        '{artifact: "ducklake-catalog-robustness-failure", backend: $backend, profile: $profile, status: "failed", error: .}' \
        "$log" >"$target"
}

run_scenario() {
    local profile="$1" name="$2" sequence="$3"
    local fdb_artifact="$tmp_dir/$name-fdb.json"
    local postgres_artifact="$tmp_dir/$name-postgres.json"
    local backend
    local backend_order=(fdb postgres)
    [[ "$sequence" == fdb-first ]] || backend_order=(postgres fdb)
    printf 'null\n' >"$fdb_artifact"
    printf 'null\n' >"$postgres_artifact"
    for backend in "${backend_order[@]}"; do
        if [[ "$benchmark_backend" == both || "$benchmark_backend" == "$backend" ]]; then
            run_backend "$backend" "$profile" "$tmp_dir/$name-$backend.json"
        fi
    done
    jq -cn \
        --arg name "$name" \
        --arg profile "$profile" \
        --arg execution_order "$sequence" \
        --arg backends "$benchmark_backend" \
        --slurpfile fdb "$fdb_artifact" \
        --slurpfile postgres "$postgres_artifact" \
        -f "$root_dir/scripts/ducklake-catalog-robustness-scenario.jq" \
        >>"$tmp_dir/scenarios.jsonl"
}

run_varied_scenario() {
    local record="$1"
    local name schema tables rows payload batch preload_workers readers churn writers sequence target_bytes
    IFS='|' read -r name schema tables rows payload batch preload_workers readers churn writers sequence <<<"$record"
    [[ -n "$name" && -n "$sequence" ]] || fail "invalid varied scenario: $record"
    target_bytes=$((tables * rows * payload))
    env \
        AUX_DUCKLAKE_VARIED_SCHEMA="$schema" \
        AUX_DUCKLAKE_VARIED_TABLES="$tables" \
        AUX_DUCKLAKE_VARIED_ROWS_PER_TABLE="$rows" \
        AUX_DUCKLAKE_VARIED_ROW_BYTES="$payload" \
        AUX_DUCKLAKE_VARIED_TARGET_BYTES="$target_bytes" \
        AUX_DUCKLAKE_VARIED_PRELOAD_BATCH_ROWS="$batch" \
        AUX_DUCKLAKE_VARIED_PRELOAD_WORKERS="$preload_workers" \
        AUX_DUCKLAKE_VARIED_PARALLEL_WORKERS="$readers" \
        AUX_DUCKLAKE_VARIED_CHURN_ROUNDS="$churn" \
        AUX_DUCKLAKE_VARIED_CONCURRENT_WRITERS="$writers" \
        bash -c "run_scenario varied \"\$1\" \"\$2\"" _ "$name" "$sequence"
}

run_inline_scenario() {
    local record="$1"
    local name flush_interval tables first_rows second_rows preload_tables sequence
    IFS='|' read -r name flush_interval tables first_rows second_rows preload_tables sequence <<<"$record"
    [[ -n "$name" && -n "$sequence" ]] || fail "invalid inline scenario: $record"
    env \
        AUX_DUCKLAKE_INLINE_FLUSH_INTERVAL="$flush_interval" \
        AUX_DUCKLAKE_INLINE_TABLES="$tables" \
        AUX_DUCKLAKE_INLINE_FIRST_ROWS="$first_rows" \
        AUX_DUCKLAKE_INLINE_SECOND_ROWS="$second_rows" \
        AUX_DUCKLAKE_INLINE_DELETE_ROWS=1 \
        AUX_DUCKLAKE_INLINE_PRELOAD_TABLES="$preload_tables" \
        AUX_DUCKLAKE_INLINE_PRELOAD_ROWS=2 \
        bash -c "run_scenario inline \"\$1\" \"\$2\"" _ "$name" "$sequence"
}

write_artifact() {
    jq -s -f "$root_dir/scripts/ducklake-catalog-robustness-summary.jq" \
        "$tmp_dir/scenarios.jsonl" >"$output"
}

export -f run_backend run_scenario
export benchmark benchmark_output_dir tmp_dir root_dir benchmark_backend
require_environment
mkdir -p "$benchmark_output_dir" "$artifact_dir"
: >"$tmp_dir/scenarios.jsonl"

varied_scenarios=(
    'narrow-tiny-batches|narrow|4|32|128|1|1|2|1|0|fdb-first'
    'mixed-balanced|mixed|8|128|1024|32|2|4|1|0|postgres-first'
    'wide-large-rows|wide|4|64|16384|16|2|2|1|0|fdb-first'
    'many-small-tables|narrow|24|16|256|4|4|6|1|0|postgres-first'
    'concurrent-read-write|mixed|8|64|1024|16|2|4|1|3|fdb-first'
)
for scenario in "${varied_scenarios[@]}"; do
    run_varied_scenario "$scenario"
    write_artifact
done

inline_scenarios=(
    'inline-flush-each-batch|each_batch|8|1|15|0|postgres-first'
    'inline-flush-at-end|end|8|1|15|24|fdb-first'
    'inline-never-flush|never|8|1|15|0|postgres-first'
)
for scenario in "${inline_scenarios[@]}"; do
    run_inline_scenario "$scenario"
    write_artifact
done

jq -e '.summary.completed_scenario_count == .summary.scenario_count' "$output" >/dev/null ||
    fail "one or more robustness scenarios failed; see $output"
echo "ducklake_catalog_robustness_benchmark_artifact=$output"
