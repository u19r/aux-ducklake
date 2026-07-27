#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVIDENCE_DIR="${AUX_DUCKLAKE_RELEASE_EVIDENCE_DIR:-$ROOT_DIR/docs/evidence/ducklake-fdb-release/latest}"
UPSTREAM_MODE="${AUX_DUCKLAKE_RELEASE_UPSTREAM_MODE:-smoke}"
SCAN_FILES="${AUX_DUCKLAKE_RELEASE_SCAN_FILES:-1000}"
SKIP_STEPS=",${AUX_DUCKLAKE_RELEASE_SKIP_STEPS:-},"

mkdir -p "$EVIDENCE_DIR"
rm -f "$EVIDENCE_DIR/release-gate.log"

run_gate() {
    local name="$1"
    shift
    if [[ "$SKIP_STEPS" == *",$name,"* ]]; then
        echo "release_gate_step=$name skipped" | tee -a "$EVIDENCE_DIR/release-gate.log"
        return
    fi
    echo "release_gate_step=$name" | tee -a "$EVIDENCE_DIR/release-gate.log"
    "$@" 2>&1 | tee "$EVIDENCE_DIR/$name.log"
}

export AUX_DUCKLAKE_RELEASE_EVIDENCE_DIR="$EVIDENCE_DIR"

run_gate fmt "$ROOT_DIR/scripts/cargo_with_sccache.sh" fmt --check
run_gate check "$ROOT_DIR/scripts/cargo_with_sccache.sh" check --workspace --all-targets --no-default-features --features ducklake-catalog/foundationdb
run_gate parity_core env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-parity-postgres-fdb-core
run_gate parity_mutations env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-parity-postgres-fdb-mutations
run_gate parity_ddl env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-parity-postgres-fdb-ddl
run_gate parity_inline env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-parity-postgres-fdb-inline
run_gate upstream_postgres env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 AUX_DUCKLAKE_UPSTREAM_ARTIFACT_DIR="$EVIDENCE_DIR/upstream-postgres-$UPSTREAM_MODE" "$ROOT_DIR/scripts/ducklake_upstream_catalog_tests.sh" postgres "$UPSTREAM_MODE"
run_gate upstream_fdb env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 AUX_DUCKLAKE_UPSTREAM_ARTIFACT_DIR="$EVIDENCE_DIR/upstream-fdb-$UPSTREAM_MODE" "$ROOT_DIR/scripts/ducklake_upstream_catalog_tests.sh" fdb "$UPSTREAM_MODE"
run_gate upstream_disabled_diff "$ROOT_DIR/scripts/ducklake_upstream_disabled_diff.sh"
run_gate release_build just ducklake-build-release-local
run_gate runtime_smoke env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-fdb-runtime-smoke
run_gate benchmark_profile just ducklake-catalog-benchmark-profile "$SCAN_FILES"
run_gate benchmark_operational just ducklake-catalog-benchmark-operational
run_gate chaos just ducklake-fdb-chaos
run_gate concurrency just ducklake-fdb-concurrency
run_gate encryption env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-fdb-encryption
run_gate prefix_clone env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 just ducklake-fdb-prefix-clone-drill
run_gate soak env AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD=1 AUX_DUCKLAKE_FDB_SOAK_ITERATIONS="${AUX_DUCKLAKE_FDB_SOAK_ITERATIONS:-20}" "$ROOT_DIR/scripts/ducklake_fdb_soak.sh"

echo "ducklake_fdb_release_gate=ok" | tee -a "$EVIDENCE_DIR/release-gate.log"
