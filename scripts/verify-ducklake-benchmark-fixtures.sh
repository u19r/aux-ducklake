#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT_DIR/scripts/ducklake-catalog-benchmark-fixtures.sh"

verify_selective_join() {
    local sql="$1"
    grep -F -q "ON b.id = a.id AND b.bucket = a.bucket" <<<"$sql"
    if grep -F -q "USING (bucket)" <<<"$sql"; then
        echo "varied benchmark join must not create a low-cardinality many-to-many intermediate" >&2
        exit 1
    fi
}

verify_selective_join "$(varied_join_query_sql 4)"
verify_selective_join "$(varied_join_query_sql 4 benchmark_snapshot)"

[[ "$(default_varied_tables_per_transaction 4)" == "4" ]]
[[ "$(default_varied_tables_per_transaction 100)" == "10" ]]

mutation_sql="$(varied_churn_sql 4 100 3 mutate 2)"
[[ "$(grep -F -c "BEGIN TRANSACTION;" <<<"$mutation_sql")" -eq 6 ]]
[[ "$(grep -F -c "COMMIT;" <<<"$mutation_sql")" -eq 6 ]]

compaction_sql="$(realistic_compaction_sql 100)"
[[ "$(grep -F -c "BEGIN TRANSACTION;" <<<"$compaction_sql")" -eq 10 ]]
[[ "$(grep -F -c "CALL ducklake_merge_adjacent_files('dl'," <<<"$compaction_sql")" -eq 100 ]]
[[ "$(grep -F -c "COMMIT;" <<<"$compaction_sql")" -eq 10 ]]

echo "ducklake benchmark fixtures verified"
