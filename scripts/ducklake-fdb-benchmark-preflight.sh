#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

fail() {
    echo "ducklake FDB benchmark preflight failure: $*" >&2
    exit 1
}

. "$root_dir/scripts/ducklake-benchmark-artifacts.sh"

usage() {
    cat >&2 <<'USAGE'
usage: ducklake-fdb-benchmark-preflight.sh \
    --duckdb <debug-duckdb> \
    --extension <ducklake-extension> \
    --runtime <debug-runtime-library> \
    (requires AUX_DUCKLAKE_FDB_CLUSTER_FILE for a private cluster) \
    [--prefix-clear <debug-prefix-clear>] \
    [--source-root <aux-ducklake-root>] \
    [--duckdb-sha256 <sha256>] \
    [--extension-sha256 <sha256>] \
    [--runtime-sha256 <sha256>] \
    [--metadata-table-count <count>] \
    [--inline-row-limit <count>] \
    [--validate-only]
USAGE
}

check_runtime_freshness() {
    [[ -d "$source_root/crates/ducklake-catalog" ]] ||
        fail "source root does not contain crates/ducklake-catalog: $source_root"
    check_artifact_freshness "FoundationDB runtime" "$runtime_path" \
        "$source_root/crates/ducklake-catalog" \
        "$source_root/Cargo.toml" \
        "$source_root/Cargo.lock"
}

check_ducklake_freshness() {
    local ducklake_source="$source_root/third_party/ducklake/src"
    [[ -d "$ducklake_source" ]] ||
        fail "source root does not contain third_party/ducklake/src: $source_root"
    check_artifact_freshness "DuckDB binary" "$duckdb_path" \
        "$ducklake_source" \
        "$source_root/patches/ducklake"
    check_artifact_freshness "DuckLake extension" "$extension_path" \
        "$ducklake_source" \
        "$source_root/patches/ducklake"
}

runtime_mapping_matches() {
    local pid="$1"
    case "$runtime_mapping_tool" in
    vmmap)
        vmmap "$pid" 2>/dev/null | grep -Fq -- "$runtime_path"
        ;;
    proc_maps)
        grep -Fq -- "$runtime_path" "/proc/$pid/maps" 2>/dev/null
        ;;
    *)
        return 1
        ;;
    esac
}

check_artifacts_unchanged() {
    local phase="$1"
    local duckdb_sha_after
    local extension_sha_after
    local runtime_sha_after

    duckdb_sha_after="$(sha256_file "$duckdb_path")"
    extension_sha_after="$(sha256_file "$extension_path")"
    runtime_sha_after="$(sha256_file "$runtime_path")"
    [[ "$duckdb_sha_after" == "$duckdb_sha_before" ]] ||
        fail "DuckDB binary changed while running $phase: before $duckdb_sha_before, after $duckdb_sha_after"
    [[ "$extension_sha_after" == "$extension_sha_before" ]] ||
        fail "DuckLake extension changed while running $phase: before $extension_sha_before, after $extension_sha_after"
    [[ "$runtime_sha_after" == "$runtime_sha_before" ]] ||
        fail "FoundationDB runtime changed while running $phase: before $runtime_sha_before, after $runtime_sha_after"
}

run_duckdb_sql() {
    local phase="$1"
    local sql_file="$2"
    local output_file="$tmp_dir/$phase.out"
    local pid
    local status=0

    "$duckdb_path" -unsigned -bail -batch >"$output_file" 2>&1 <"$sql_file" &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        if [[ "$runtime_mapping_seen" == "0" ]] && runtime_mapping_matches "$pid"; then
            runtime_mapping_seen=1
        fi
        sleep 0.02
    done
    wait "$pid" || status=$?

    cat "$output_file"
    [[ "$status" -eq 0 ]] || fail "$phase DuckDB scenario failed (see $output_file)"

    check_artifacts_unchanged "$phase"
}

assert_label() {
    local output_file="$1"
    local label="$2"
    local expected="$3"
    local actual

    actual="$(label_value "$output_file" "$label")"
    [[ "$actual" == "$expected" ]] || {
        echo "--- $output_file ---" >&2
        cat "$output_file" >&2
        fail "$label expected '$expected', got '${actual:-<missing>}'"
    }
}

label_value() {
    local output_file="$1"
    local label="$2"

    awk -v key="$label" '
        {
            marker = key "="
            position = index($0, marker)
            if (position > 0) {
                value = substr($0, position + length(marker))
                if (substr(value, 1, 1) == "\047") {
                    next
                }
                sub(/[[:space:]│].*$/, "", value)
                print value
                exit
            }
        }
    ' "$output_file"
}

assert_label_regex() {
    local output_file="$1"
    local label="$2"
    local pattern="$3"
    local actual

    actual="$(label_value "$output_file" "$label")"
    [[ "$actual" =~ ^${pattern}$ ]] || {
        echo "--- $output_file ---" >&2
        cat "$output_file" >&2
        fail "$label did not match '$pattern' (got '${actual:-<missing>}')"
    }
}

duckdb_path=""
extension_path=""
runtime_path=""
fdb_cluster_file="${AUX_DUCKLAKE_FDB_CLUSTER_FILE:-}"
prefix_clear_path="${AUX_DUCKLAKE_FDB_PREFLIGHT_PREFIX_CLEAR_BIN:-}"
source_root="${AUX_DUCKLAKE_FDB_PREFLIGHT_SOURCE_ROOT:-$root_dir}"
expected_duckdb_sha256="${AUX_DUCKLAKE_FDB_PREFLIGHT_DUCKDB_SHA256:-}"
expected_extension_sha256="${AUX_DUCKLAKE_FDB_PREFLIGHT_EXTENSION_SHA256:-}"
expected_runtime_sha256="${AUX_DUCKLAKE_FDB_PREFLIGHT_RUNTIME_SHA256:-}"
metadata_table_count="${AUX_DUCKLAKE_FDB_PREFLIGHT_TABLE_COUNT:-260}"
inline_row_limit="${AUX_DUCKLAKE_FDB_PREFLIGHT_INLINE_ROW_LIMIT:-100}"
validate_only=0

while (($# > 0)); do
    case "$1" in
    --duckdb)
        (($# >= 2)) || {
            usage
            exit 2
        }
        duckdb_path="$2"
        shift 2
        ;;
    --extension)
        (($# >= 2)) || {
            usage
            exit 2
        }
        extension_path="$2"
        shift 2
        ;;
    --runtime)
        (($# >= 2)) || {
            usage
            exit 2
        }
        runtime_path="$2"
        shift 2
        ;;
    --prefix-clear)
        (($# >= 2)) || {
            usage
            exit 2
        }
        prefix_clear_path="$2"
        shift 2
        ;;
    --source-root)
        (($# >= 2)) || {
            usage
            exit 2
        }
        source_root="$2"
        shift 2
        ;;
    --duckdb-sha256)
        (($# >= 2)) || {
            usage
            exit 2
        }
        expected_duckdb_sha256="$2"
        shift 2
        ;;
    --extension-sha256)
        (($# >= 2)) || {
            usage
            exit 2
        }
        expected_extension_sha256="$2"
        shift 2
        ;;
    --runtime-sha256)
        (($# >= 2)) || {
            usage
            exit 2
        }
        expected_runtime_sha256="$2"
        shift 2
        ;;
    --metadata-table-count)
        (($# >= 2)) || {
            usage
            exit 2
        }
        metadata_table_count="$2"
        shift 2
        ;;
    --inline-row-limit)
        (($# >= 2)) || {
            usage
            exit 2
        }
        inline_row_limit="$2"
        shift 2
        ;;
    --validate-only)
        validate_only=1
        shift
        ;;
    --help | -h)
        usage
        exit 0
        ;;
    *)
        usage
        exit 2
        ;;
    esac
done

[[ -n "$duckdb_path" && -n "$extension_path" && -n "$runtime_path" ]] || {
    usage
    exit 2
}
[[ -n "$fdb_cluster_file" ]] ||
    fail "AUX_DUCKLAKE_FDB_CLUSTER_FILE is required; refusing system FoundationDB fallback"
[[ -f "$fdb_cluster_file" ]] ||
    fail "AUX_DUCKLAKE_FDB_CLUSTER_FILE must name an existing cluster file: $fdb_cluster_file"
[[ "$metadata_table_count" =~ ^[0-9]+$ && "$metadata_table_count" -gt 256 ]] ||
    fail "metadata table count must be greater than 256"
metadata_table_count=$((10#$metadata_table_count))
[[ "$inline_row_limit" =~ ^[0-9]+$ ]] ||
    fail "inline row limit must be a non-negative integer"
inline_row_limit=$((10#$inline_row_limit))

case "$(uname -s)" in
Darwin) runtime_suffix=".dylib" ;;
Linux) runtime_suffix=".so" ;;
*) fail "unsupported host for FoundationDB runtime preflight: $(uname -s)" ;;
esac

validate_artifact "DuckDB binary" "$duckdb_path" 1
validate_artifact "DuckLake extension" "$extension_path"
validate_artifact "FoundationDB runtime" "$runtime_path"
[[ "$runtime_path" == *"$runtime_suffix" ]] ||
    fail "FoundationDB runtime must end in $runtime_suffix: $runtime_path"

duckdb_path="$(canonical_file "$duckdb_path")" || fail "could not resolve DuckDB binary: $duckdb_path"
extension_path="$(canonical_file "$extension_path")" || fail "could not resolve DuckLake extension: $extension_path"
runtime_path="$(canonical_file "$runtime_path")" || fail "could not resolve FoundationDB runtime: $runtime_path"
fdb_cluster_file="$(canonical_file "$fdb_cluster_file")" ||
    fail "could not resolve FoundationDB cluster file: $fdb_cluster_file"
source_root="$(cd "$source_root" && pwd -P)" || fail "could not resolve source root: $source_root"

expected_duckdb_sha256="$(validate_sha256_argument "DuckDB SHA-256" "$expected_duckdb_sha256")"
expected_extension_sha256="$(validate_sha256_argument "DuckLake extension SHA-256" "$expected_extension_sha256")"
expected_runtime_sha256="$(validate_sha256_argument "FoundationDB runtime SHA-256" "$expected_runtime_sha256")"

duckdb_sha256="$(sha256_file "$duckdb_path")"
extension_sha256="$(sha256_file "$extension_path")"
runtime_sha_before="$(sha256_file "$runtime_path")"
duckdb_sha_before="$duckdb_sha256"
extension_sha_before="$extension_sha256"
check_expected_sha256 "DuckDB" "$duckdb_sha256" "$expected_duckdb_sha256"
check_expected_sha256 "DuckLake extension" "$extension_sha256" "$expected_extension_sha256"
check_expected_sha256 "FoundationDB runtime" "$runtime_sha_before" "$expected_runtime_sha256"
check_runtime_freshness
check_ducklake_freshness

printf 'preflight_duckdb=%s\n' "$duckdb_path"
printf 'preflight_duckdb_sha256=%s\n' "$duckdb_sha256"
printf 'preflight_extension=%s\n' "$extension_path"
printf 'preflight_extension_sha256=%s\n' "$extension_sha256"
printf 'preflight_runtime=%s\n' "$runtime_path"
printf 'preflight_runtime_sha256=%s\n' "$runtime_sha_before"
printf 'preflight_fdb_cluster_file=%s\n' "$fdb_cluster_file"
printf 'preflight_metadata_table_count=%s\n' "$metadata_table_count"
printf 'preflight_inline_row_limit=%s\n' "$inline_row_limit"
printf 'preflight_reverse_scan=rust-focused-tests-only\n'

if ((validate_only == 1)); then
    printf 'preflight_validation=ok\n'
    exit 0
fi

[[ -n "$prefix_clear_path" ]] ||
    prefix_clear_path="${CARGO_TARGET_DIR:-$root_dir/target}/debug/ducklake-fdb-prefix-clear"
case "$prefix_clear_path" in
/*) ;;
*) prefix_clear_path="$root_dir/$prefix_clear_path" ;;
esac
validate_artifact "FoundationDB prefix-clear helper" "$prefix_clear_path" 1
prefix_clear_path="$(canonical_file "$prefix_clear_path")" ||
    fail "could not resolve prefix-clear helper"
check_artifact_freshness "FoundationDB prefix-clear helper" "$prefix_clear_path" \
    "$source_root/crates/ducklake-catalog" \
    "$source_root/Cargo.toml" \
    "$source_root/Cargo.lock"
prefix_clear_sha_before="$(sha256_file "$prefix_clear_path")"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/ducklake-fdb-preflight.XXXXXX")"
started_seconds="$(date +%s)"
preflight_cost_reported=0
fdb_prefix="aux-ducklake/benchmark-preflight/$(date +%s)-$$-${RANDOM}/"
runtime_mapping_seen=0
runtime_mapping_tool="unavailable"
case "$(uname -s)" in
Darwin)
    command -v vmmap >/dev/null 2>&1 && runtime_mapping_tool="vmmap"
    ;;
Linux)
    [[ -r /proc/self/maps ]] && runtime_mapping_tool="proc_maps"
    ;;
esac

clear_prefix() {
    local args=(--prefix "$fdb_prefix" --cluster-file "$fdb_cluster_file")
    "$prefix_clear_path" "${args[@]}"
}

cleanup() {
    local status=$?
    trap - EXIT
    if [[ "$preflight_cost_reported" == "0" && -d "$tmp_dir" ]]; then
        printf 'preflight_tmp_bytes=%s\n' "$(du -sk "$tmp_dir" | awk '{ print $1 * 1024 }')"
        printf 'preflight_elapsed_seconds=%s\n' "$(($(date +%s) - started_seconds))"
        printf 'preflight_prefix=%s\n' "$fdb_prefix"
        printf 'preflight_status=fail\n'
    fi
    if ! clear_prefix; then
        echo "could not clear exact FoundationDB preflight prefix: $fdb_prefix" >&2
        status=1
    fi
    local prefix_clear_sha_after
    prefix_clear_sha_after="$(sha256_file "$prefix_clear_path")"
    if [[ "$prefix_clear_sha_after" != "$prefix_clear_sha_before" ]]; then
        echo "FoundationDB prefix-clear helper changed during cleanup:" \
            "before $prefix_clear_sha_before, after $prefix_clear_sha_after" >&2
        status=1
    fi
    if [[ -d "$tmp_dir" ]]; then
        rm -rf -- "$tmp_dir" || status=1
    fi
    exit "$status"
}
trap cleanup EXIT

export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
export AUX_DUCKLAKE_FDB_CLUSTER_FILE="$fdb_cluster_file"
export AUX_DUCKLAKE_FDB_PREFIX="$fdb_prefix"
export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$fdb_prefix"
export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_path"

phase_one_sql="$tmp_dir/phase-one.sql"
phase_two_sql="$tmp_dir/phase-two.sql"
python3 "$root_dir/scripts/ducklake-benchmark-preflight-sql.py" \
    "$extension_path" "$tmp_dir" "$inline_row_limit" "$metadata_table_count"

run_duckdb_sql "phase-one" "$phase_one_sql"
assert_label "$tmp_dir/phase-one.out" preflight_initial_snapshots 1
assert_label "$tmp_dir/phase-one.out" preflight_inline_before_delete 2,3
assert_label "$tmp_dir/phase-one.out" preflight_inline_after_delete 1,2
assert_label "$tmp_dir/phase-one.out" preflight_inline_after_flush 1,2
assert_label "$tmp_dir/phase-one.out" preflight_inline_overshoot 11,66
assert_label "$tmp_dir/phase-one.out" preflight_inline_overshoot_after_flush 11,66
assert_label "$tmp_dir/phase-one.out" preflight_maintenance_candidates 1
assert_label "$tmp_dir/phase-one.out" preflight_maintenance_remaining 0
assert_label "$tmp_dir/phase-one.out" preflight_maintenance_files 0
assert_label "$tmp_dir/phase-one.out" preflight_schema_tables 1,1
assert_label "$tmp_dir/phase-one.out" preflight_ctas 1,1,ctas
assert_label "$tmp_dir/phase-one.out" preflight_view 1,1,ctas
assert_label "$tmp_dir/phase-one.out" preflight_central_metadata "$metadata_table_count"
assert_label "$tmp_dir/phase-one.out" preflight_central_bounds \
    "preflight_central_000|preflight_central_$((metadata_table_count - 1))"
assert_label "$tmp_dir/phase-one.out" preflight_identity_stats "$metadata_table_count,$metadata_table_count"
identity_before="$(label_value "$tmp_dir/phase-one.out" preflight_identity)"
[[ "$identity_before" == *"preflight_central_000:"* &&
    "$identity_before" == *"preflight_central_$((metadata_table_count - 1)):"* ]] ||
    fail "phase-one central table identities are incomplete"

run_duckdb_sql "phase-two" "$phase_two_sql"
assert_label "$tmp_dir/phase-two.out" preflight_central_metadata "$metadata_table_count"
assert_label "$tmp_dir/phase-two.out" preflight_central_bounds \
    "preflight_central_000|preflight_central_$((metadata_table_count - 1))"
assert_label "$tmp_dir/phase-two.out" preflight_identity "$identity_before"
assert_label "$tmp_dir/phase-two.out" preflight_identity_stats "$metadata_table_count,$metadata_table_count"
assert_label "$tmp_dir/phase-two.out" preflight_reopen_schema 1,1
assert_label "$tmp_dir/phase-two.out" preflight_reopen_ctas 1,1,ctas
assert_label "$tmp_dir/phase-two.out" preflight_reopen_view 1,1,ctas
assert_label "$tmp_dir/phase-two.out" preflight_reopen_inline 1,2
assert_label_regex "$tmp_dir/phase-two.out" preflight_reopen_maintenance '[0-9]+'

data_bytes="$(du -sk "$tmp_dir" | awk '{ print $1 * 1024 }')"
elapsed_seconds=$(($(date +%s) - started_seconds))
preflight_cost_reported=1
if [[ "$runtime_mapping_seen" == "1" ]]; then
    runtime_mapping_status=observed
else
    runtime_mapping_status=not-observed
fi
printf 'preflight_runtime_mapping=%s\n' "$runtime_mapping_status"
printf 'preflight_tmp_bytes=%s\n' "$data_bytes"
printf 'preflight_elapsed_seconds=%s\n' "$elapsed_seconds"
printf 'preflight_prefix=%s\n' "$fdb_prefix"
printf 'preflight_status=pass\n'
