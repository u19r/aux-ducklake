#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
reset="$root_dir/scripts/ducklake-catalog-benchmark-reset.sh"
preflight="$root_dir/scripts/ducklake-fdb-benchmark-preflight.sh"

usage() {
    echo "usage: $0 [--fdb-preflight] command [argument ...]" >&2
}

cleanup() {
    local status=$?
    trap - EXIT
    if ! "$reset"; then
        status=1
    fi
    exit "$status"
}

run_preflight=0
if [[ "${1:-}" == "--fdb-preflight" ]]; then
    run_preflight=1
    shift
    if [[ -z "${AUX_DUCKLAKE_BENCHMARK_BACKEND:-}" ]]; then
        export AUX_DUCKLAKE_BENCHMARK_BACKEND=fdb
    fi
fi

[[ "$#" -gt 0 ]] || {
    usage
    exit 1
}

"$reset"
if ((run_preflight == 1)); then
    target_dir="${CARGO_TARGET_DIR:-$root_dir/target}"
    case "$target_dir" in
    /*) ;;
    *) target_dir="$root_dir/$target_dir" ;;
    esac
    case "$(uname -s)" in
    Darwin) runtime_name=libducklake_catalog.dylib ;;
    Linux) runtime_name=libducklake_catalog.so ;;
    *)
        usage
        exit 1
        ;;
    esac
    preflight_duckdb="${AUX_DUCKLAKE_FDB_PREFLIGHT_DUCKDB_BIN:-$root_dir/third_party/ducklake/build/debug/duckdb}"
    preflight_extension="${AUX_DUCKLAKE_FDB_PREFLIGHT_DUCKLAKE_EXTENSION:-}"
    if [[ -z "$preflight_extension" ]]; then
        preflight_extension="$root_dir/third_party/ducklake/build/debug/extension/ducklake/ducklake.duckdb_extension"
    fi
    preflight_runtime="${AUX_DUCKLAKE_FDB_PREFLIGHT_RUNTIME_LIBRARY:-$target_dir/debug/$runtime_name}"
    preflight_prefix_clear="${AUX_DUCKLAKE_FDB_PREFLIGHT_PREFIX_CLEAR_BIN:-$target_dir/debug/ducklake-fdb-prefix-clear}"
    "$preflight" \
        --duckdb "$preflight_duckdb" \
        --extension "$preflight_extension" \
        --runtime "$preflight_runtime" \
        --prefix-clear "$preflight_prefix_clear"
fi
trap cleanup EXIT
"$@"
