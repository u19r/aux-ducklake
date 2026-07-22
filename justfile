set unstable := true

# List available public commands.
default:
    @just _default

# Install local development tools.
setup:
    @just _setup

# Update sibling repositories to consume this release.
update-aux-repos:
    ./scripts/update-aux-repos.sh

# Build the primary local DuckLake debug artifact.
build:
    @just ducklake-build

# Run cargo check for the full workspace.
check:
    @just _check

# Check Rust formatting without modifying files.
style-check:
    @just _style-check

# Apply deterministic Rust formatting.
style-fix:
    @just _style-fix

# Print the curated style cleanup queue when one exists.
style-audit:
    @if test -f style-debt.tsv; then cat style-debt.tsv; else echo "No curated style debt."; fi

# Run workspace tests.
test:
    @just _test

# Run the full local sign-off gate before pushing.
pre-push:
    @just _pre-push

# Run the CI repository gate.
ci-check:
    @just _ci-check

_default:
    @just --list

_setup:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v brew >/dev/null 2>&1; then
        brew list sccache >/dev/null 2>&1 || brew install sccache
        brew list ccache >/dev/null 2>&1 || brew install ccache
        brew list ninja >/dev/null 2>&1 || brew install ninja
        brew list cargo-nextest >/dev/null 2>&1 || brew install cargo-nextest
        brew list croaring >/dev/null 2>&1 || brew install croaring
        if ! command -v fdbcli >/dev/null 2>&1 || [[ ! -e /usr/local/lib/libfdb_c.dylib && ! -e /opt/homebrew/lib/libfdb_c.dylib ]]; then
            brew install foundationdb
        fi
    else
        echo "Homebrew is required to install croaring on macOS" >&2
        exit 1
    fi

_check:
    ./scripts/fetch_ducklake.sh
    ./scripts/cargo_with_sccache.sh check --workspace --all-targets --all-features

_style-fix:
    ./scripts/cargo_with_sccache.sh fmt --all

_style-check:
    ./scripts/cargo_with_sccache.sh fmt --all --check
    ./scripts/cargo_with_sccache.sh clippy --workspace --all-targets --all-features --no-deps -- -D warnings

_test:
    ./scripts/cargo_with_sccache.sh test --workspace --all-targets

_pre-push:
    @just _style-check
    @just _check
    @just _test
    @just _runtime-protocol-check
    @just _workload-inventory-verify
    @just _upstream-disabled-diff-temp

_ci-check:
    #!/usr/bin/env bash
    set -euo pipefail
    ./scripts/fetch_ducklake.sh
    ./scripts/cargo_with_sccache.sh check -p ducklake-catalog --all-targets
    just _runtime-protocol-check
    just _workload-inventory-verify
    just _upstream-disabled-diff-temp

_runtime-protocol-check:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --no-default-features --features foundationdb ffi_probe_round_trips_runtime_frame

_workload-inventory-verify:
    ./scripts/verify_ducklake_workload_inventory.sh

_upstream-disabled-diff-temp:
    #!/usr/bin/env bash
    set -euo pipefail
    disabled_diff_dir="$(mktemp -d)"
    trap 'rm -rf "$disabled_diff_dir"' EXIT
    AUX_DUCKLAKE_RELEASE_EVIDENCE_DIR="$disabled_diff_dir" ./scripts/ducklake_upstream_disabled_diff.sh

ducklake-catalog-test:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog

ducklake-catalog-check:
    ./scripts/cargo_with_sccache.sh check -p ducklake-catalog --all-targets

ducklake-catalog-evolution-cleanup-test:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --test catalog_evolution_cleanup_tests -- --nocapture

ducklake-fdb-concurrency:
    ./scripts/ducklake_fdb_concurrency.sh

ducklake-fdb-chaos:
    ./scripts/ducklake_fdb_chaos.sh

ducklake-fdb-sim-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    ./scripts/cargo_with_sccache.sh build -p ducklake-fdb-sim-workload --release
    target_dir="$(./scripts/cargo_with_sccache.sh metadata --format-version 1 --no-deps | rg -o '"target_directory":"[^"]+"' | cut -d '"' -f 4)"
    for workload in catalog_smoke catalog_expire catalog_cleanup catalog_read_age catalog_recovery; do
        ./scripts/cargo_with_sccache.sh run -p ducklake-fdb-sim-runner -- run --workload "$workload" --profile smoke --seed 1 --buggify off --library-path "$target_dir/release"
    done

ducklake-fdb-sim-multiclient:
    #!/usr/bin/env bash
    set -euo pipefail
    ./scripts/cargo_with_sccache.sh build -p ducklake-fdb-sim-workload --release
    target_dir="$(./scripts/cargo_with_sccache.sh metadata --format-version 1 --no-deps | rg -o '"target_directory":"[^"]+"' | cut -d '"' -f 4)"
    for workload in catalog_smoke catalog_expire catalog_cleanup catalog_read_age catalog_recovery; do
        ./scripts/cargo_with_sccache.sh run -p ducklake-fdb-sim-runner -- run --workload "$workload" --profile multi-client --seed 1 --buggify on --library-path "$target_dir/release"
    done

ducklake-catalog-tests-check:
    ./scripts/cargo_with_sccache.sh check -p ducklake-catalog --tests

ducklake-runtime-protocol-check:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --no-default-features --features foundationdb ffi_probe_round_trips_runtime_frame

ducklake-runtime-ffi-check:
    CARGO_TARGET_DIR=target/codex-validation ./scripts/cargo_with_sccache.sh test -p ducklake-catalog runtime_ffi

ducklake-runtime-cpp-ffi-smoke:
    ./scripts/ducklake_runtime_cpp_ffi_smoke.sh

ducklake-fdb-runtime-smoke:
    ./scripts/ducklake_runtime_cpp_ffi_smoke.sh fdb

ducklake-fdb-encryption:
    ./scripts/ducklake-fdb-encryption.sh

ducklake-fdb-prefix-clone-drill:
    ./scripts/ducklake_fdb_prefix_clone_drill.sh

ducklake-upstream-postgres-smoke:
    ./scripts/ducklake_upstream_catalog_tests.sh postgres smoke

ducklake-upstream-fdb-smoke:
    ./scripts/ducklake_upstream_catalog_tests.sh fdb smoke

ducklake-upstream-postgres-full:
    ./scripts/ducklake_upstream_catalog_tests.sh --keep-going postgres full

ducklake-upstream-fdb-full:
    ./scripts/ducklake_upstream_catalog_tests.sh --keep-going fdb full

ducklake-upstream-postgres-slow:
    ./scripts/ducklake_upstream_catalog_tests.sh --keep-going postgres slow

ducklake-upstream-fdb-slow:
    ./scripts/ducklake_upstream_catalog_tests.sh --keep-going fdb slow

ducklake-upstream-disabled-diff:
    ./scripts/ducklake_upstream_disabled_diff.sh

ducklake-upstream-full-gap-summary:
    ./scripts/ducklake_upstream_full_gap_summary.sh

ducklake-fdb-soak:
    ./scripts/ducklake_fdb_soak.sh

ducklake-fdb-release-gate:
    ./scripts/ducklake_fdb_release_gate.sh

ducklake-catalog-evolution-cleanup-dev-loop test='':
    #!/usr/bin/env bash
    set -euo pipefail
    test_filter='{{test}}'
    if [[ -z "$test_filter" ]]; then
        ./scripts/cargo_with_sccache.sh check -p ducklake-catalog --tests
        exit 0
    fi
    if command -v cargo-nextest >/dev/null 2>&1; then
        ./scripts/cargo_with_sccache.sh nextest run -p ducklake-catalog --test catalog_evolution_cleanup_tests "$test_filter"
    else
        ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --test catalog_evolution_cleanup_tests "$test_filter" -- --nocapture
    fi

ducklake-catalog-evolution-cleanup-one test:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v cargo-nextest >/dev/null 2>&1; then
        ./scripts/cargo_with_sccache.sh nextest run -p ducklake-catalog --test catalog_evolution_cleanup_tests {{test}}
    else
        ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --test catalog_evolution_cleanup_tests {{test}} -- --nocapture
    fi

ducklake-catalog-fdb-test:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --no-default-features --features foundationdb

ducklake-parity-postgres-smoke:
    ./scripts/ducklake_parity_postgres_smoke.sh

ducklake-parity-postgres-fdb-core:
    ./scripts/ducklake_parity_postgres_fdb_core.sh

ducklake-parity-postgres-fdb-mutations:
    ./scripts/ducklake_parity_postgres_fdb_mutations.sh

ducklake-parity-postgres-fdb-ddl:
    ./scripts/ducklake_parity_postgres_fdb_ddl.sh

ducklake-parity-postgres-fdb-inline:
    ./scripts/ducklake_parity_postgres_fdb_inline.sh

# Development/debug DuckLake builds. These are not release artifacts.
ducklake-fetch:
    ./scripts/fetch_ducklake.sh

ducklake-build:
    ./scripts/build_ducklake_debug.sh

ducklake-build-local:
    AUX_DUCKLAKE_SKIP_FETCH=1 ./scripts/build_ducklake_debug.sh

# Release build/package entrypoints. These follow the aux-duckdb combined-binary shape.
ducklake-build-release:
    ./scripts/build_ducklake_release.sh

ducklake-build-release-local:
    AUX_DUCKLAKE_SKIP_FETCH=1 ./scripts/build_ducklake_release.sh

ducklake-build-linux-amd64:
    ./scripts/build_ducklake_linux_release.sh linux-amd64

ducklake-build-linux-arm64:
    ./scripts/build_ducklake_linux_release.sh linux-arm64

ducklake-package-release platform='':
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{platform}}" ]]; then
        ./scripts/package_ducklake_release.sh "{{platform}}"
    else
        ./scripts/package_ducklake_release.sh
    fi

ducklake-release platform='':
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{platform}}" ]]; then
        ./scripts/release.sh "{{platform}}"
    else
        ./scripts/release.sh
    fi

ducklake-workload-inventory-verify:
    ./scripts/verify_ducklake_workload_inventory.sh

ducklake-catalog-benchmark-smoke:
    ./scripts/ducklake_catalog_benchmark.sh smoke

ducklake-catalog-benchmark-scan10:
    ./scripts/ducklake_catalog_benchmark.sh scan10

ducklake-catalog-benchmark-profile scan_files='10000':
    ./scripts/ducklake_catalog_benchmark.sh profile {{scan_files}}

ducklake-catalog-benchmark-realistic:
    ./scripts/ducklake_catalog_benchmark.sh realistic

ducklake-catalog-benchmark-varied:
    ./scripts/ducklake_catalog_benchmark.sh varied

ducklake-release-benchmark-smoke:
    AUX_DUCKLAKE_FDB_LIVE=1 ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-benchmark -- --profile smoke

ducklake-release-benchmark-profile scan_files='10000':
    AUX_DUCKLAKE_FDB_LIVE=1 ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-benchmark -- --profile full --scan-files {{scan_files}}

ducklake-release-benchmark-profile-tiny:
    AUX_DUCKLAKE_FDB_LIVE=1 ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-benchmark -- --profile tiny

ducklake-cache-stats:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v sccache >/dev/null 2>&1; then
        sccache --show-stats
    fi
    if command -v ccache >/dev/null 2>&1; then
        ccache -s
    fi
