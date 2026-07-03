#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"
DUCKDB_BIN="$DUCKLAKE_DIR/build/debug/duckdb"

fail() {
    echo "ducklake fdb prefix clone drill failure: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    [[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

usage() {
    cat <<'USAGE'
usage: ducklake_fdb_prefix_clone_drill.sh [--keep-tmp]

Runs the local FoundationDB prefix clone drill. Use --keep-tmp only when
debugging a failed drill and you need to inspect the temporary DuckLake files.
USAGE
}

keep_tmp=0
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --keep-tmp) keep_tmp=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            fail "unknown argument: $1"
            ;;
    esac
    shift
done

echo "drill_step=build_runtime"
runtime_library="$(ducklake_build_debug_catalog_runtime "$ROOT_DIR" 1 foundationdb)" ||
    fail "foundationdb runtime library was not built"

echo "drill_step=build_ducklake"
if ! ducklake_reuse_debug_build_enabled; then
    AUX_DUCKLAKE_SKIP_FETCH=1 "$ROOT_DIR/scripts/build_ducklake_debug.sh"
fi
[[ -x "$DUCKDB_BIN" ]] || fail "modified duckdb executable was not built"

tmp_dir="$(mktemp -d)"
if [[ "$keep_tmp" == "1" ]]; then
    echo "tmp_dir=$tmp_dir"
else
    trap 'rm -rf "$tmp_dir"' EXIT
fi

run_id="$(date +%s)-$$"
source_prefix="aux-ducklake/runbook-drill/$run_id/source/"
restored_prefix="aux-ducklake/runbook-drill/$run_id/restored/"
runtime_catalog_identity="prefix-clone-drill/$run_id"
data_dir="$tmp_dir/data"
mkdir -p "$data_dir"

export AUX_DUCKLAKE_RUNTIME_LIBRARY="$runtime_library"
export AUX_DUCKLAKE_CATALOG_BACKEND="fdb"
export AUX_DUCKLAKE_FDB_PREFIX="$source_prefix"
export AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY="$runtime_catalog_identity"

echo "drill_step=seed_source_catalog"
set +e
source_output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD ducklake;
ATTACH 'ducklake:$tmp_dir/source-metadata.duckdb' AS dl (
    DATA_PATH '$data_dir',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);
CREATE TABLE dl.main.restore_probe(id INTEGER, amount INTEGER, note VARCHAR);
INSERT INTO dl.main.restore_probe VALUES
    (1, 10, 'alpha'),
    (2, 20, 'beta'),
    (3, 30, 'gamma');
SELECT 'source_readback=' || count(*) || ',' || sum(amount) || ',' ||
       string_agg(note, '|' ORDER BY id)
FROM dl.main.restore_probe;
SQL
)"
source_status=$?
set -e
printf '%s\n' "$source_output"
[[ "$source_status" -eq 0 ]] || fail "source catalog seed failed"
assert_contains "$source_output" "runtime_bridge=cpp_ffi"
assert_contains "$source_output" "backend=foundationdb"
assert_contains "$source_output" "source_readback=3,60,alpha|beta|gamma"
source_runtime_catalog_id="$(awk -F= '/^runtime_catalog_id=/ { print $2; exit }' <<<"$source_output")"
[[ "$source_runtime_catalog_id" =~ ^[0-9]+$ ]] || fail "source attach did not report a runtime catalog id"

echo "drill_step=clone_catalog_prefix"
clone_args=(
    run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-prefix-copy --
    --source-prefix "$source_prefix"
    --destination-prefix "$restored_prefix"
    --clear-destination
)
if [[ -n "${AUX_DUCKLAKE_FDB_CLUSTER_FILE:-}" ]]; then
    clone_args+=(--cluster-file "$AUX_DUCKLAKE_FDB_CLUSTER_FILE")
fi
clone_output="$("$ROOT_DIR/scripts/cargo_with_sccache.sh" "${clone_args[@]}")"
printf '%s\n' "$clone_output"
assert_contains "$clone_output" "source_prefix=$source_prefix"
assert_contains "$clone_output" "destination_prefix=$restored_prefix"
assert_contains "$clone_output" "copied_key_count="

copied_key_count="$(awk -F= '/^copied_key_count=/ { print $2 }' <<<"$clone_output")"
[[ "$copied_key_count" =~ ^[0-9]+$ ]] || fail "missing copied key count"
(( copied_key_count > 0 )) || fail "expected copied key count to be non-zero"

echo "drill_step=read_restored_catalog"
export AUX_DUCKLAKE_FDB_PREFIX="$restored_prefix"
set +e
restored_output="$("$DUCKDB_BIN" -batch 2>&1 <<SQL
LOAD ducklake;
ATTACH 'ducklake:$tmp_dir/restored-metadata.duckdb' AS dl (
    DATA_PATH '$data_dir',
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT 0
);
SELECT 'restored_readback=' || count(*) || ',' || sum(amount) || ',' ||
       string_agg(note, '|' ORDER BY id)
FROM dl.main.restore_probe;
SQL
)"
restored_status=$?
set -e
printf '%s\n' "$restored_output"
[[ "$restored_status" -eq 0 ]] || fail "restored catalog readback failed"
assert_contains "$restored_output" "runtime_bridge=cpp_ffi"
assert_contains "$restored_output" "backend=foundationdb"
assert_contains "$restored_output" "restored_readback=3,60,alpha|beta|gamma"
restored_runtime_catalog_id="$(awk -F= '/^runtime_catalog_id=/ { print $2; exit }' <<<"$restored_output")"
[[ "$restored_runtime_catalog_id" =~ ^[0-9]+$ ]] || fail "restored attach did not report a runtime catalog id"
[[ "$restored_runtime_catalog_id" == "$source_runtime_catalog_id" ]] ||
    fail "restored runtime catalog id $restored_runtime_catalog_id did not match source $source_runtime_catalog_id"

echo "source_prefix=$source_prefix"
echo "restored_prefix=$restored_prefix"
echo "runtime_catalog_identity=$runtime_catalog_identity"
echo "ducklake_fdb_prefix_clone_drill=ok"
