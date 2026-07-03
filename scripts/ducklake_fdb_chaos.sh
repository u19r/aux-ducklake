#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "$CARGO_TARGET_DIR" in
        /*) ;;
        *) CARGO_TARGET_DIR="$ROOT_DIR/$CARGO_TARGET_DIR" ;;
    esac
else
    CARGO_TARGET_DIR="$ROOT_DIR/target/codex-validation"
fi
export CARGO_TARGET_DIR

catalog_test() {
    "$ROOT_DIR/scripts/cargo_with_sccache.sh" test \
        -p ducklake-catalog \
        --no-default-features \
        --features foundationdb \
        "$@"
}

catalog_test fdb_error_classifier
catalog_test maybe_committed_recovery
catalog_test retry_decision
catalog_test exhausted_retryable_not_committed
catalog_test final_error_keeps

AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_conflict_fence_rejects_stale_commit_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_append_retry_is_idempotent_without_duplicate_publish_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_append_after_concurrent_schema_change_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_append_after_table_drop_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_rewrite_delete_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_merge_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 catalog_test --test fdb_live_tests \
    fdb_live_concurrent_remove_conflicts_without_publishing_append_when_enabled

echo "ducklake_fdb_chaos=ok"
