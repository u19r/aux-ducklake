#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
preflight="$root_dir/scripts/ducklake-fdb-benchmark-preflight.sh"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/ducklake-fdb-preflight-test.XXXXXX")"

cleanup() {
    local status=$?
    trap - EXIT
    rm -rf -- "$test_dir"
    exit "$status"
}
trap cleanup EXIT

case "$(uname -s)" in
Darwin) runtime_suffix=.dylib ;;
Linux) runtime_suffix=.so ;;
*)
    echo "unsupported host" >&2
    exit 1
    ;;
esac

source_file="$test_dir/source/crates/ducklake-catalog/src/lib.rs"
ducklake_source_file="$test_dir/source/third_party/ducklake/src/ducklake.cpp"
patch_file="$test_dir/source/patches/ducklake/0001-preflight.patch"
duckdb_file="$test_dir/artifacts/duckdb"
extension_file="$test_dir/artifacts/ducklake.duckdb_extension"
runtime_file="$test_dir/artifacts/libducklake_catalog$runtime_suffix"
cluster_file="$test_dir/fdb.cluster"
mkdir -p "$(dirname "$source_file")" "$(dirname "$ducklake_source_file")" \
    "$(dirname "$patch_file")" "$(dirname "$duckdb_file")"
printf '// preflight fixture\n' >"$source_file"
printf '// ducklake fixture\n' >"$ducklake_source_file"
printf '# preflight patch fixture\n' >"$patch_file"
printf '#!/bin/sh\nexit 0\n' >"$duckdb_file"
chmod +x "$duckdb_file"
printf 'extension fixture\n' >"$extension_file"
printf 'runtime fixture\n' >"$runtime_file"
printf 'test:cluster\n' >"$cluster_file"
touch -t 202001010000 "$source_file" "$ducklake_source_file" "$patch_file"
touch -t 202001010100 "$runtime_file"
export AUX_DUCKLAKE_FDB_CLUSTER_FILE="$cluster_file"

missing_cluster_output="$test_dir/missing-cluster.out"
if env -u AUX_DUCKLAKE_FDB_CLUSTER_FILE "$preflight" \
    --validate-only \
    --source-root "$test_dir/source" \
    --duckdb "$duckdb_file" \
    --extension "$extension_file" \
    --runtime "$runtime_file" >"$missing_cluster_output" 2>&1; then
    echo "missing private FoundationDB cluster unexpectedly passed" >&2
    exit 1
fi
grep -Fq 'AUX_DUCKLAKE_FDB_CLUSTER_FILE is required; refusing system FoundationDB fallback' \
    "$missing_cluster_output"

valid_output="$test_dir/valid.out"
"$preflight" \
    --validate-only \
    --source-root "$test_dir/source" \
    --duckdb "$duckdb_file" \
    --extension "$extension_file" \
    --runtime "$runtime_file" >"$valid_output"
grep -Fq 'preflight_validation=ok' "$valid_output"

sha_output="$test_dir/sha.out"
if "$preflight" \
    --validate-only \
    --source-root "$test_dir/source" \
    --duckdb "$duckdb_file" \
    --extension "$extension_file" \
    --runtime "$runtime_file" \
    --runtime-sha256 "0000000000000000000000000000000000000000000000000000000000000000" >"$sha_output" 2>&1; then
    echo "runtime SHA validation unexpectedly passed" >&2
    exit 1
fi
grep -Fq 'FoundationDB runtime SHA-256 mismatch' "$sha_output"

touch -t 202001010200 "$source_file"
stale_output="$test_dir/stale.out"
if "$preflight" \
    --validate-only \
    --source-root "$test_dir/source" \
    --duckdb "$duckdb_file" \
    --extension "$extension_file" \
    --runtime "$runtime_file" >"$stale_output" 2>&1; then
    echo "stale runtime validation unexpectedly passed" >&2
    exit 1
fi
grep -Fq 'stale FoundationDB runtime' "$stale_output"

touch -t 202001010000 "$source_file"
count_output="$test_dir/count.out"
if "$preflight" \
    --validate-only \
    --source-root "$test_dir/source" \
    --duckdb "$duckdb_file" \
    --extension "$extension_file" \
    --runtime "$runtime_file" \
    --metadata-table-count 256 >"$count_output" 2>&1; then
    echo "central scan count validation unexpectedly passed" >&2
    exit 1
fi
grep -Fq 'greater than 256' "$count_output"

printf 'ducklake_fdb_benchmark_preflight_tests=ok\n'
