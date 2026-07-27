#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
reset="$root_dir/scripts/ducklake-catalog-benchmark-reset.sh"

cleanup() {
    local status=$?
    trap - EXIT
    if ! "$reset"; then
        status=1
    fi
    exit "$status"
}

[[ "$#" -gt 0 ]] || {
    echo "usage: $0 command [argument ...]" >&2
    exit 1
}

"$reset"
trap cleanup EXIT
"$@"

