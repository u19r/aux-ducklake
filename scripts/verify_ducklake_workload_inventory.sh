#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_SOURCE="$ROOT_DIR/third_party/ducklake/src"
WORKLOAD_SOURCE="$ROOT_DIR/crates/ducklake-catalog/src/workload.rs"

"$ROOT_DIR/scripts/fetch_ducklake.sh"

contains_literal() {
    local needle="$1"
    local haystack="$2"

    if [[ -d "$haystack" ]]; then
        grep -R -F -q -- "$needle" "$haystack"
    else
        grep -F -q -- "$needle" "$haystack"
    fi
}

required_ducklake_symbols=(
    "LoadDuckLake"
    "GetCatalogForSnapshot"
    "WriteNewDataFiles"
    "GetFilesForTable"
    "GenerateFileColumnStatsCTEBody"
    "GetSnapshot(BoundAtClause"
    "WriteNewDeleteFiles"
    "WriteNewPartitionKeys"
    "WriteNewSortKeys"
    "WriteNewInlinedData"
    "GetTableInsertions"
    "GetTableDeletions"
    "GetAllSnapshots"
    "GetFilesForCleanup"
    "SetConfigOption"
    "GetTableSizes"
    "MigrateV10"
)

required_inventory_variants=(
    "CatalogOpen"
    "CreateSchema"
    "CreateTable"
    "AppendDataFiles"
    "CurrentTableScan"
    "FilePruning"
    "TimeTravelScan"
    "UpdateOrDelete"
    "AlterTable"
    "Compaction"
    "DataInlining"
    "ChangeDataFeed"
    "SnapshotMaintenance"
    "CleanupFiles"
    "ConfigOptions"
    "MetadataInspection"
    "Migration"
)

for symbol in "${required_ducklake_symbols[@]}"; do
    if ! contains_literal "$symbol" "$DUCKLAKE_SOURCE"; then
        echo "missing DuckLake metadata workload symbol: $symbol" >&2
        exit 1
    fi
done

for variant in "${required_inventory_variants[@]}"; do
    if ! contains_literal "$variant" "$WORKLOAD_SOURCE"; then
        echo "missing ducklake-catalog workload inventory variant: $variant" >&2
        exit 1
    fi
done

echo "ducklake workload inventory verified against pinned source"
