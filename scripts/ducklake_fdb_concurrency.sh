#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO="$ROOT_DIR/scripts/cargo_with_sccache.sh"

echo "e2e_step=fdb_live_concurrent_writers"
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_concurrent_appends_are_visible_exactly_once_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_append_after_concurrent_schema_change_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_append_after_table_drop_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_merge_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_rewrite_delete_compaction_after_concurrent_append_conflicts_without_publishing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_concurrent_remove_conflicts_without_publishing_append_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_data_file_cleanup_rechecks_stale_candidate_before_removing_when_enabled
AUX_DUCKLAKE_FDB_LIVE=1 "$CARGO" test -p ducklake-catalog --no-default-features --features foundationdb --test fdb_live_tests \
    fdb_live_delete_file_cleanup_rechecks_stale_candidate_before_removing_when_enabled

echo "ducklake_fdb_concurrency=ok"
