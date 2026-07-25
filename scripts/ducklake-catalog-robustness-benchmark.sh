#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
benchmark="$root_dir/scripts/ducklake_catalog_benchmark.sh"
artifact_dir="$root_dir/docs/benchmarks/ducklake-fdb-feature-parity"
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
    [[ -n "${AUX_DUCKLAKE_FDB_CLUSTER_FILE:-}" ]] || fail "AUX_DUCKLAKE_FDB_CLUSTER_FILE is required"
    [[ -n "${AUX_DUCKLAKE_POSTGRES_DSN:-}" ]] || fail "AUX_DUCKLAKE_POSTGRES_DSN is required"
}

run_backend() {
    local backend="$1" profile="$2" target="$3"
    local log="$target.log"
    if AUX_DUCKLAKE_BENCHMARK_BACKEND="$backend" \
        AUX_DUCKLAKE_BENCHMARK_OUTPUT_DIR="$benchmark_output_dir" \
        "$benchmark" "$profile" > "$log" 2>&1; then
        cat "$log"
        cp "$benchmark_output_dir/$backend-$profile-latest.json" "$target"
        return
    fi
    cat "$log" >&2
    jq -Rs \
        --arg backend "$backend" \
        --arg profile "$profile" \
        '{artifact: "ducklake-catalog-robustness-failure", backend: $backend, profile: $profile, status: "failed", error: .}' \
        "$log" > "$target"
}

run_paired() {
    local profile="$1" name="$2" sequence="$3"
    local fdb_artifact="$tmp_dir/$name-fdb.json"
    local postgres_artifact="$tmp_dir/$name-postgres.json"
    if [[ "$sequence" == "fdb-first" ]]; then
        run_backend fdb "$profile" "$fdb_artifact"
        run_backend postgres "$profile" "$postgres_artifact"
    else
        run_backend postgres "$profile" "$postgres_artifact"
        run_backend fdb "$profile" "$fdb_artifact"
    fi
    jq -cn \
        --arg name "$name" \
        --arg profile "$profile" \
        --arg execution_order "$sequence" \
        --slurpfile fdb "$fdb_artifact" \
        --slurpfile postgres "$postgres_artifact" \
        '$fdb[0] as $f | $postgres[0] as $p | {
            name: $name,
            profile: $profile,
            execution_order: $execution_order,
            comparison: (if ($f.elapsed_ms and $p.elapsed_ms) then {
                    status: "complete",
                    fdb_elapsed_ms: $f.elapsed_ms,
                    postgres_elapsed_ms: $p.elapsed_ms,
                    fdb_postgres_ratio: ($f.elapsed_ms / $p.elapsed_ms)
                } else {
                    status: "failed",
                    fdb_status: ($f.status // "complete"),
                    postgres_status: ($p.status // "complete")
                } end),
            fdb: $f,
            postgres: $p
        }' \
        >> "$tmp_dir/scenarios.jsonl"
}

run_varied_scenario() {
    local record="$1"
    local name schema tables rows payload batch preload_workers readers churn writers sequence target_bytes
    IFS='|' read -r name schema tables rows payload batch preload_workers readers churn writers sequence <<< "$record"
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
        bash -c 'run_paired varied "$1" "$2"' _ "$name" "$sequence"
}

run_inline_scenario() {
    local record="$1"
    local name flush_interval tables first_rows second_rows preload_tables sequence
    IFS='|' read -r name flush_interval tables first_rows second_rows preload_tables sequence <<< "$record"
    [[ -n "$name" && -n "$sequence" ]] || fail "invalid inline scenario: $record"
    env \
        AUX_DUCKLAKE_INLINE_FLUSH_INTERVAL="$flush_interval" \
        AUX_DUCKLAKE_INLINE_TABLES="$tables" \
        AUX_DUCKLAKE_INLINE_FIRST_ROWS="$first_rows" \
        AUX_DUCKLAKE_INLINE_SECOND_ROWS="$second_rows" \
        AUX_DUCKLAKE_INLINE_DELETE_ROWS=1 \
        AUX_DUCKLAKE_INLINE_PRELOAD_TABLES="$preload_tables" \
        AUX_DUCKLAKE_INLINE_PRELOAD_ROWS=2 \
        bash -c 'run_paired inline "$1" "$2"' _ "$name" "$sequence"
}

write_artifact() {
    jq -s -f "$root_dir/scripts/ducklake-catalog-robustness-summary.jq" \
        "$tmp_dir/scenarios.jsonl" > "$output"
}

export -f run_backend run_paired
export benchmark benchmark_output_dir tmp_dir
require_environment
mkdir -p "$benchmark_output_dir"
: > "$tmp_dir/scenarios.jsonl"

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

echo "ducklake_catalog_robustness_benchmark_artifact=$output"
