#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"

ducklake_ensure_source_tree "$ROOT_DIR" "$DUCKLAKE_DIR"
ducklake_configure_build_environment debug "$DUCKLAKE_DIR"

make -C "$DUCKLAKE_DIR" debug
