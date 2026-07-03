#!/usr/bin/env bash
set -euo pipefail

if [[ "${AUX_DUCKLAKE_RUST_SCCACHE:-0}" == "1" ]] && command -v sccache >/dev/null 2>&1; then
    RUSTC_WRAPPER="$(command -v sccache)"
    export RUSTC_WRAPPER
    export CARGO_INCREMENTAL=0
else
    if [[ "${RUSTC_WRAPPER:-}" == *sccache* ]]; then
        unset RUSTC_WRAPPER
    fi
    export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-1}"
fi

exec cargo "$@"
