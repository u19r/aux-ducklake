#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
UNITTEST="$DUCKLAKE_DIR/build/debug/test/unittest"
FDB_CONFIG="$DUCKLAKE_DIR/test/configs/aux_fdb.json"
POSTGRES_CONFIG="$DUCKLAKE_DIR/test/configs/postgres.json"
POSTGRES_SCANNER_EXTENSION="$DUCKLAKE_DIR/build/debug/extension/postgres_scanner/postgres_scanner.duckdb_extension"

fail() {
    echo "ducklake upstream catalog tests failure: $*" >&2
    exit 1
}

usage() {
    cat <<'USAGE'
usage: ducklake_upstream_catalog_tests.sh [--keep-going] postgres|fdb [smoke|full|slow|exhaustive] [test-path ...]

Runs DuckLake's upstream SQLLogic tests against either the native Postgres
catalog config or the aux FoundationDB catalog config. The FDB backend runs one
test file per process with a fresh AUX_DUCKLAKE_FDB_PREFIX so the current
runtime env configuration cannot leak catalog state between tests.
USAGE
}

keep_going=0
while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --keep-going)
            keep_going=1
            shift
            ;;
        --help|-h)
            usage
            exit 2
            ;;
        *)
            usage >&2
            fail "unknown option: $1"
            ;;
    esac
done

backend="${1:-}"
mode="${2:-smoke}"
if [[ -z "$backend" || "$backend" == "--help" || "$backend" == "-h" ]]; then
    usage
    exit 2
fi
shift
shift || true

case "$backend" in
    postgres|fdb) ;;
    *) usage >&2; fail "backend must be postgres or fdb" ;;
esac
case "$mode" in
    smoke|full|slow|exhaustive) ;;
    *) usage >&2; fail "mode must be smoke, full, slow, or exhaustive" ;;
esac

build_ducklake_if_needed() {
    ducklake_build_debug_unittest_if_needed "$ROOT_DIR" "$UNITTEST" "$backend" "$POSTGRES_SCANNER_EXTENSION" ||
        fail "DuckLake unittest binary was not built at $UNITTEST"
}

build_runtime_if_needed() {
    [[ "$backend" == "fdb" ]] || return 0
    local runtime_library
    runtime_library="$(ducklake_build_debug_catalog_runtime "$ROOT_DIR" 1 foundationdb)" ||
        fail "foundationdb runtime library was not built"
    export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_library"
    export AUX_DUCKLAKE_CATALOG_BACKEND=fdb
}

ensure_postgres_database() {
    [[ "$backend" == "postgres" ]] || return 0
    if command -v createdb >/dev/null 2>&1; then
        createdb ducklakedb >/dev/null 2>&1 || true
    fi
}

default_smoke_tests=(
    "test/sql/ducklake_basic.test"
    "test/sql/initialize/ducklake_create_new.test"
    "test/sql/time_travel/basic_time_travel.test"
    "test/sql/table_changes/ducklake_table_insertions.test"
    "test/sql/delete/basic_delete.test"
    "test/sql/comments/comments.test"
)

discover_full_tests() {
    find test/sql -type f -name '*.test' | sort
}

discover_slow_tests() {
    find test/sql -type f -name '*.test_slow' | sort
}

discover_exhaustive_tests() {
    find test/sql -type f \( -name '*.test' -o -name '*.test_slow' \) | sort
}

tests=("$@")
if [[ "${#tests[@]}" -eq 0 ]]; then
    case "$mode" in
        smoke) tests=("${default_smoke_tests[@]}") ;;
        full) mapfile -t tests < <(cd "$DUCKLAKE_DIR" && discover_full_tests) ;;
        slow) mapfile -t tests < <(cd "$DUCKLAKE_DIR" && discover_slow_tests) ;;
        exhaustive) mapfile -t tests < <(cd "$DUCKLAKE_DIR" && discover_exhaustive_tests) ;;
    esac
fi

build_ducklake_if_needed
build_runtime_if_needed
ensure_postgres_database

config="$POSTGRES_CONFIG"
[[ "$backend" == "fdb" ]] && config="$FDB_CONFIG"
artifact_root="${AUX_DUCKLAKE_UPSTREAM_ARTIFACT_DIR:-$ROOT_DIR/docs/evidence/ducklake-fdb-release/upstream-$backend-$mode}"
rm -rf "$artifact_root"
mkdir -p "$artifact_root"

printf 'backend=%s\nmode=%s\nconfig=%s\ntest_count=%d\n' \
    "$backend" "$mode" "$config" "${#tests[@]}" >"$artifact_root/summary.txt"
if [[ "$backend" == "fdb" ]]; then
    printf 'runtime_library=%s\n' "${AUX_DUCKLAKE_RUNTIME_LIBRARY:-}" >>"$artifact_root/summary.txt"
fi

passed=0
skipped=0
failed=0
provided_fdb_prefix="${AUX_DUCKLAKE_FDB_PREFIX:-}"

run_one_test() {
    local test_path="$1"
    local safe_name output_file status
    safe_name="${test_path//\//__}"
    output_file="$artifact_root/$safe_name.log"
    status=0
    if [[ "$backend" == "fdb" && -z "$provided_fdb_prefix" ]]; then
        export AUX_DUCKLAKE_FDB_PREFIX="aux-ducklake/upstream/${mode}/${safe_name}/$(date +%s)-$$/"
        printf 'test_prefix %s %s\n' "$test_path" "$AUX_DUCKLAKE_FDB_PREFIX" >>"$artifact_root/summary.txt"
    fi
    (
        cd "$DUCKLAKE_DIR"
        "$UNITTEST" --test-config "$config" --test-dir ./ "$test_path"
    ) >"$output_file" 2>&1 || status=$?
    if [[ "$status" -eq 0 ]]; then
        if rg -q "All tests skipped|No tests ran|test cases: 0" "$output_file"; then
            echo "skip $test_path" | tee -a "$artifact_root/summary.txt"
            skipped=$((skipped + 1))
        else
            echo "pass $test_path" | tee -a "$artifact_root/summary.txt"
            passed=$((passed + 1))
        fi
        return
    fi
    echo "fail $test_path" | tee -a "$artifact_root/summary.txt"
    failed=$((failed + 1))
    if [[ "$keep_going" != "1" ]]; then
        tail -n 120 "$output_file" >&2 || true
        fail "$backend upstream test failed: $test_path"
    fi
}

for test_path in "${tests[@]}"; do
    run_one_test "$test_path"
done

{
    printf 'passed=%d\n' "$passed"
    printf 'skipped=%d\n' "$skipped"
    printf 'failed=%d\n' "$failed"
} >>"$artifact_root/summary.txt"

[[ "$failed" -eq 0 ]] || fail "$failed upstream $backend tests failed; see $artifact_root"
echo "ducklake_upstream_catalog_tests_${backend}_${mode}=ok"
