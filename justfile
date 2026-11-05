set unstable

# List commands in workflow order.
default:
    @just --list --unsorted

# Install local development tools.
setup: setup-linux-linker
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v brew >/dev/null 2>&1; then
        brew install rustup just jq cmake pkg-config lcov
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
    rustup component add clippy rustfmt llvm-tools
    cargo install --locked cargo-llvm-cov samply

# Typecheck; forward Cargo arguments, such as -p <crate>.
[positional-arguments]
check *ARGS='--workspace':
    ./scripts/cargo_with_sccache.sh check --all-targets --all-features "$@"

# Run unit tests; forward Cargo test arguments.
[positional-arguments]
test *ARGS='--workspace':
    ./scripts/cargo_with_sccache.sh test --lib --bins "$@"

# Check Rust formatting without modifying files.
style-check:
    ./scripts/cargo_with_sccache.sh fmt --all --check

# Apply deterministic Rust formatting.
style-fix:
    ./scripts/cargo_with_sccache.sh fmt --all

# Check lint rules without changing files.
[positional-arguments]
lint-check *ARGS='--workspace':
    ./scripts/cargo_with_sccache.sh clippy --all-targets --all-features --no-deps "$@" -- -D warnings

# Apply lint fixes and fail if lint errors remain.
[positional-arguments]
lint-fix *ARGS='--workspace':
    ./scripts/cargo_with_sccache.sh clippy --fix --all-targets --all-features --allow-dirty --allow-staged --no-deps "$@" -- -D warnings
    @just lint-check "$@"

# Apply style and lint fixes, then run unit tests.
pre-push:
    @just style-fix
    @just lint-fix
    @just test

# Run unit tests without style checks or live services.
ci-check:
    @just test

# Run style, lint, unit tests, scenarios, and available canary suites.
test-full:
    @just style-check
    @just lint-check
    @just test

# Build the debug DuckLake artifact; forward make arguments.
[positional-arguments]
build *ARGS:
    ./scripts/build_ducklake_debug.sh "$@"

# Build the release DuckLake artifact.
[positional-arguments]
release *ARGS:
    ./scripts/build_ducklake_release.sh "$@"

# Build and start the debug DuckDB SQL shell; forward shell arguments.
[positional-arguments]
run-local *ARGS:
    @just build
    @echo "DuckDB interactive SQL shell; no HTTP listener."
    third_party/ducklake/build/debug/duckdb "$@"

# Build and start the release DuckDB SQL shell; forward shell arguments.
[positional-arguments]
run-local-release *ARGS:
    @just release
    @echo "DuckDB interactive SQL shell; no HTTP listener."
    third_party/ducklake/build/release/duckdb "$@"

# Build the ARM64 Linux package with Docker on macOS.
[positional-arguments]
docker-build-mac *ARGS:
    @just docker-build-linux "$@"

[positional-arguments]
docker-build-linux *ARGS:
    ./scripts/build_ducklake_linux_release.sh linux-arm64 "$@"

[positional-arguments]
docker-build-linux-x86 *ARGS:
    ./scripts/build_ducklake_linux_release.sh linux-amd64 "$@"

# Generate unit test coverage; forward Cargo coverage arguments.
[positional-arguments]
coverage *ARGS='--workspace':
    cargo llvm-cov --lib --bins --html "$@"

[positional-arguments]
profile *ARGS:
    @just build
    samply record third_party/ducklake/build/debug/duckdb "$@"

# Update an explicit analytics worktree to consume a published release.
update-aux-repos analytics_worktree release_tag:
    ./scripts/update-aux-repos.sh {{ quote(analytics_worktree) }} {{ quote(release_tag) }}

# Print the curated style cleanup queue when one exists.
style-audit:
    @if test -f style-debt.tsv; then cat style-debt.tsv; else echo "No curated style debt."; fi

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
    for workload in catalog_smoke catalog_expire catalog_cleanup catalog_read_age catalog_recovery catalog_scale_maintenance; do
        ./scripts/cargo_with_sccache.sh run -p ducklake-fdb-sim-runner -- run --workload "$workload" --profile smoke --seed 1 --buggify off --library-path "$target_dir/release"
    done

ducklake-fdb-sim-multiclient:
    #!/usr/bin/env bash
    set -euo pipefail
    ./scripts/cargo_with_sccache.sh build -p ducklake-fdb-sim-workload --release
    target_dir="$(./scripts/cargo_with_sccache.sh metadata --format-version 1 --no-deps | rg -o '"target_directory":"[^"]+"' | cut -d '"' -f 4)"
    for workload in catalog_smoke catalog_expire catalog_cleanup catalog_read_age catalog_recovery catalog_scale_maintenance; do
        ./scripts/cargo_with_sccache.sh run -p ducklake-fdb-sim-runner -- run --workload "$workload" --profile multi-client --seed 1 --buggify on --library-path "$target_dir/release"
    done

ducklake-catalog-tests-check:
    ./scripts/cargo_with_sccache.sh check -p ducklake-catalog --tests

ducklake-runtime-protocol-check:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --no-default-features --features foundationdb ffi_probe_round_trips_runtime_frame

ducklake-runtime-ffi-check:
    ./scripts/cargo_with_sccache.sh test -p ducklake-catalog runtime_ffi

ducklake-runtime-cpp-ffi-smoke:
    ./scripts/ducklake_runtime_cpp_ffi_smoke.sh

ducklake-fdb-benchmark-preflight-test:
    ./scripts/ducklake-fdb-benchmark-preflight-test.sh

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
    test_filter='{{ test }}'
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
        ./scripts/cargo_with_sccache.sh nextest run -p ducklake-catalog --test catalog_evolution_cleanup_tests {{ test }}
    else
        ./scripts/cargo_with_sccache.sh test -p ducklake-catalog --test catalog_evolution_cleanup_tests {{ test }} -- --nocapture
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

ducklake-build-local:
    AUX_DUCKLAKE_SKIP_FETCH=1 ./scripts/build_ducklake_debug.sh

ducklake-build-release-local:
    AUX_DUCKLAKE_SKIP_FETCH=1 ./scripts/build_ducklake_release.sh

ducklake-package-release platform='':
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{ platform }}" ]]; then
        ./scripts/package_ducklake_release.sh "{{ platform }}"
    else
        ./scripts/package_ducklake_release.sh
    fi

# Verify a release bundle through the embedded C API and isolated FoundationDB catalogs.
ducklake-release-smoke bundle_root:
    python3 ./scripts/ducklake-release-smoke.py {{ quote(bundle_root) }}

ducklake-release platform='':
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{ platform }}" ]]; then
        ./scripts/release.sh "{{ platform }}"
    else
        ./scripts/release.sh
    fi

ducklake-workload-inventory-verify:
    ./scripts/verify_ducklake_workload_inventory.sh

ducklake-catalog-benchmark-smoke:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh smoke

# SQL catalog scan profile; distinct from the interactive runtime profiler.
ducklake-catalog-benchmark-profile scan_files='10000':
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh profile {{scan_files}}

ducklake-catalog-benchmark-scan10:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh scan10

ducklake-catalog-benchmark-operational:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh operational

ducklake-catalog-benchmark-operational-growth:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh operational-growth

ducklake-catalog-benchmark-inline:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh inline

ducklake-catalog-benchmark-realistic:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh realistic

ducklake-catalog-benchmark-varied:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake_catalog_benchmark.sh varied

ducklake-catalog-benchmark-robustness:
    ./scripts/ducklake-catalog-benchmark-run.sh ./scripts/ducklake-catalog-robustness-benchmark.sh

ducklake-catalog-benchmark-full:
    just release
    just ducklake-catalog-benchmark-smoke
    just ducklake-catalog-benchmark-scan10
    just ducklake-catalog-benchmark-profile
    just ducklake-catalog-benchmark-inline
    just ducklake-catalog-benchmark-operational
    just ducklake-catalog-benchmark-operational-growth
    just ducklake-catalog-benchmark-realistic
    just ducklake-catalog-benchmark-varied
    just ducklake-catalog-benchmark-robustness

ducklake-release-benchmark-smoke:
    AUX_DUCKLAKE_FDB_LIVE=1 ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-benchmark -- --profile smoke

ducklake-release-benchmark-profile scan_files='10000':
    AUX_DUCKLAKE_FDB_LIVE=1 ./scripts/cargo_with_sccache.sh run -q -p ducklake-catalog --no-default-features --features foundationdb --bin ducklake-fdb-benchmark -- --profile full --scan-files {{ scan_files }}

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

# Install the pinned Linux linker; no action is needed on macOS.
setup-linux-linker:
    #!/usr/bin/env bash
    set -euo pipefail
    exec bash scripts/install-mold.sh

# Check catalog-managed software versions without a sibling checkout.
versions-check:
    node scripts/software-versions.ts check

# List or update the software pins maintained by this repository.
versions-list:
    node scripts/software-versions.ts list

versions-sync:
    node scripts/software-versions.ts sync

versions-update:
    node scripts/software-versions.ts update

versions-test:
    node --test scripts/software-versions.test.ts
