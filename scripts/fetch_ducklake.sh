#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
DUCKLAKE_COMMIT="7e3c8e97cc5acddbcd2a1ebfb8530e6c52efdacf"
PIN_FILE="$DUCKLAKE_DIR/.aux-ducklake-pinned-commit"
PATCH_FILE="$ROOT_DIR/patches/ducklake/0001-add-aux-catalog-metadata-manager.patch"
PATCH_MARKER="$DUCKLAKE_DIR/.aux-ducklake-bridge-patch"

ducklake_sha256() {
    local path="$1"

    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" | awk '{print $1}'
    else
        echo "sha256sum or shasum is required to hash $path" >&2
        exit 1
    fi
}

PATCH_HASH="$(ducklake_sha256 "$PATCH_FILE")"

ducklake_configure_release_extensions() {
    local extension_config="$DUCKLAKE_DIR/extension_config.cmake"

    [[ -f "$extension_config" ]] || return
    if grep -Fq 'duckdb/.github/config/extensions/httpfs.cmake' "$extension_config"; then
        return
    fi

    perl -0pi -e 's#(duckdb_extension_load\(tpch\)\n)#$1    include("\${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/config/extensions/httpfs.cmake")\n#' "$extension_config"
}

if [[ -d "$DUCKLAKE_DIR/.git" ]] \
    && [[ -f "$PIN_FILE" ]] \
    && [[ "$(cat "$PIN_FILE")" == "$DUCKLAKE_COMMIT" ]]; then
    if [[ -f "$PATCH_MARKER" ]] && [[ "$(cat "$PATCH_MARKER")" == "$PATCH_HASH" ]]; then
        ducklake_configure_release_extensions
        exit 0
    fi

    if git -C "$DUCKLAKE_DIR" apply --reverse --check "$PATCH_FILE"; then
        ducklake_configure_release_extensions
        printf '%s' "$PATCH_HASH" > "$PATCH_MARKER"
        exit 0
    fi
fi

rm -rf "$DUCKLAKE_DIR"
mkdir -p "$(dirname "$DUCKLAKE_DIR")"
git clone --no-checkout --filter=blob:none https://github.com/duckdb/ducklake.git "$DUCKLAKE_DIR"
git -C "$DUCKLAKE_DIR" fetch --depth 1 origin "$DUCKLAKE_COMMIT"
git -C "$DUCKLAKE_DIR" checkout --detach "$DUCKLAKE_COMMIT"
git -C "$DUCKLAKE_DIR" submodule update --init --recursive --depth 1

actual_commit="$(git -C "$DUCKLAKE_DIR" rev-parse HEAD)"
if [[ "$actual_commit" != "$DUCKLAKE_COMMIT" ]]; then
    echo "DuckLake commit mismatch: expected $DUCKLAKE_COMMIT, got $actual_commit" >&2
    exit 1
fi

if git -C "$DUCKLAKE_DIR" apply --check "$PATCH_FILE"; then
    git -C "$DUCKLAKE_DIR" apply "$PATCH_FILE"
elif git -C "$DUCKLAKE_DIR" apply --reverse --check "$PATCH_FILE"; then
    true
else
    echo "DuckLake bridge patch cannot be applied cleanly" >&2
    exit 1
fi

ducklake_configure_release_extensions
printf '%s' "$DUCKLAKE_COMMIT" > "$PIN_FILE"
printf '%s' "$PATCH_HASH" > "$PATCH_MARKER"
