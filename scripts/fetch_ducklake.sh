#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake-upstream-pins.sh"
PIN_FILE="$DUCKLAKE_DIR/.aux-ducklake-pinned-commit"
DUCKDB_PIN_FILE="$DUCKLAKE_DIR/.aux-duckdb-pinned-commit"
PATCH_FILE="$ROOT_DIR/patches/ducklake/0001-add-aux-catalog-metadata-manager.patch"
PATCH_MARKER="$DUCKLAKE_DIR/.aux-ducklake-bridge-patch"
. "$ROOT_DIR/scripts/ducklake_patch_state.sh"

PATCH_HASH="$(ducklake_patch_hash "$PATCH_FILE")"

if [[ -d "$DUCKLAKE_DIR/.git" ]] &&
    [[ -f "$PIN_FILE" ]] &&
    [[ "$(cat "$PIN_FILE")" == "$DUCKLAKE_COMMIT" ]] &&
    [[ -f "$DUCKDB_PIN_FILE" ]] &&
    [[ "$(cat "$DUCKDB_PIN_FILE")" == "$DUCKDB_COMMIT" ]] &&
    [[ -e "$DUCKLAKE_DIR/duckdb/.git" ]] &&
    [[ "$(git -C "$DUCKLAKE_DIR/duckdb" rev-parse HEAD)" == "$DUCKDB_COMMIT" ]]; then
    if ducklake_ensure_canonical_patch "$DUCKLAKE_DIR" "$PATCH_FILE" "$PATCH_MARKER" "$PATCH_HASH"; then
        exit 0
    fi
    exit 1
fi

rm -rf "$DUCKLAKE_DIR"
mkdir -p "$(dirname "$DUCKLAKE_DIR")"
git clone --no-checkout --filter=blob:none https://github.com/duckdb/ducklake.git "$DUCKLAKE_DIR"
git -C "$DUCKLAKE_DIR" fetch --depth 1 origin "$DUCKLAKE_COMMIT"
git -C "$DUCKLAKE_DIR" checkout --detach "$DUCKLAKE_COMMIT"
git -C "$DUCKLAKE_DIR" submodule update --init --recursive --depth 1
git -C "$DUCKLAKE_DIR/duckdb" fetch --depth 1 origin "$DUCKDB_COMMIT"
git -C "$DUCKLAKE_DIR/duckdb" checkout --detach "$DUCKDB_COMMIT"

actual_commit="$(git -C "$DUCKLAKE_DIR" rev-parse HEAD)"
if [[ "$actual_commit" != "$DUCKLAKE_COMMIT" ]]; then
    echo "DuckLake commit mismatch: expected $DUCKLAKE_COMMIT, got $actual_commit" >&2
    exit 1
fi
actual_duckdb_commit="$(git -C "$DUCKLAKE_DIR/duckdb" rev-parse HEAD)"
if [[ "$actual_duckdb_commit" != "$DUCKDB_COMMIT" ]]; then
    echo "DuckDB commit mismatch: expected $DUCKDB_COMMIT, got $actual_duckdb_commit" >&2
    exit 1
fi

ducklake_ensure_canonical_patch "$DUCKLAKE_DIR" "$PATCH_FILE" "$PATCH_MARKER" "$PATCH_HASH"

printf '%s' "$DUCKLAKE_COMMIT" >"$PIN_FILE"
printf '%s' "$DUCKDB_COMMIT" >"$DUCKDB_PIN_FILE"
printf '%s' "$PATCH_HASH" >"$PATCH_MARKER"
