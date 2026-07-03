#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_CONFIG="$ROOT_DIR/third_party/ducklake/test/configs/postgres.json"
FDB_CONFIG="$ROOT_DIR/third_party/ducklake/test/configs/aux_fdb.json"
OUT_DIR="${AUX_DUCKLAKE_RELEASE_EVIDENCE_DIR:-$ROOT_DIR/docs/evidence/ducklake-fdb-release/latest}"

mkdir -p "$OUT_DIR"

python3 - "$POSTGRES_CONFIG" "$FDB_CONFIG" "$OUT_DIR/upstream-disabled-diff.md" <<'PY'
import json
import sys
from pathlib import Path

postgres_path = Path(sys.argv[1])
fdb_path = Path(sys.argv[2])
out_path = Path(sys.argv[3])

def load(path):
    data = json.loads(path.read_text())
    by_path = {}
    for group in data.get("skip_tests", []):
        reason = group["reason"]
        for test_path in group.get("paths", []):
            by_path[test_path] = reason
    return by_path

postgres = load(postgres_path)
fdb = load(fdb_path)
common = sorted(set(postgres) & set(fdb))
postgres_only = sorted(set(postgres) - set(fdb))
fdb_only = sorted(set(fdb) - set(postgres))

lines = [
    "# Upstream DuckLake Disabled Test Diff",
    "",
    f"- Postgres disabled tests: {len(postgres)}",
    f"- FoundationDB disabled tests: {len(fdb)}",
    f"- Common disabled tests: {len(common)}",
    f"- Postgres-only disabled tests: {len(postgres_only)}",
    f"- FoundationDB-only disabled tests: {len(fdb_only)}",
    "",
    "## FoundationDB-Only Disabled Tests",
    "",
]
if fdb_only:
    for test_path in fdb_only:
        lines.append(f"- `{test_path}`: {fdb[test_path]}")
else:
    lines.append("- None.")
lines += ["", "## Postgres-Only Disabled Tests", ""]
if postgres_only:
    for test_path in postgres_only:
        lines.append(f"- `{test_path}`: {postgres[test_path]}")
else:
    lines.append("- None.")
lines += ["", "## Common Disabled Tests With Different Reasons", ""]
different = [(p, postgres[p], fdb[p]) for p in common if postgres[p] != fdb[p]]
if different:
    for test_path, postgres_reason, fdb_reason in different:
        lines.append(f"- `{test_path}`")
        lines.append(f"  - Postgres: {postgres_reason}")
        lines.append(f"  - FoundationDB: {fdb_reason}")
else:
    lines.append("- None.")

out_path.write_text("\n".join(lines) + "\n")
print(out_path)
PY
