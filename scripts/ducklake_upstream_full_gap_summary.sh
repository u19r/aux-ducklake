#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVIDENCE_ROOT="${AUX_DUCKLAKE_RELEASE_EVIDENCE_ROOT:-$ROOT_DIR/docs/evidence/ducklake-fdb-release}"
POSTGRES_DIR="${1:-$EVIDENCE_ROOT/upstream-postgres-full}"
FDB_DIR="${2:-$EVIDENCE_ROOT/upstream-fdb-full}"
OUT_PATH="${3:-${AUX_DUCKLAKE_UPSTREAM_GAP_SUMMARY:-$EVIDENCE_ROOT/upstream-full-gap-summary.md}}"

python3 - "$POSTGRES_DIR" "$FDB_DIR" "$OUT_PATH" <<'PY'
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

postgres_dir = Path(sys.argv[1])
fdb_dir = Path(sys.argv[2])
out_path = Path(sys.argv[3])

def fail(message):
    raise SystemExit(f"ducklake upstream full gap summary failure: {message}")

def read_summary(path):
    summary = path / "summary.txt"
    if not summary.exists():
        fail(f"missing summary {summary}")
    rows = []
    totals = Counter()
    for line in summary.read_text().splitlines():
        if line.startswith(("pass ", "fail ", "skip ")):
            status, test_path = line.split(" ", 1)
            rows.append((status, test_path))
            totals[status] += 1
    return rows, totals

def log_path(root, test_path):
    return root / f"{test_path.replace('/', '__')}.log"

def classify_failure(root, test_path):
    log = log_path(root, test_path)
    text = log.read_text(errors="replace") if log.exists() else ""
    if "Table with name ducklake_" in text and "does not exist" in text:
        return "hidden metadata table access"
    if "aux_catalog metadata option writes are not implemented" in text:
        return "metadata option writes"
    if "aux_catalog data inlining is not implemented" in text:
        return "data inlining option write"
    if "old-file cleanup only supports cleanup_all" in text:
        return "checkpoint cleanup mode"
    if "parent column drop is not implemented" in text:
        return "nested column drop"
    if "null partition values are not implemented" in text:
        return "null partition values"
    if "aux_catalog cannot execute unhandled SQL metadata queries" in text:
        return "unhandled metadata SQL query"
    if "not implemented" in text.lower():
        return "other not implemented"
    if "Mismatch" in text or "Wrong result" in text:
        return "result mismatch"
    if "INTERNAL Error" in text or "Assertion triggered" in text:
        return "internal assertion"
    if "Catalog Error" in text:
        return "other catalog error"
    return "other"

postgres_rows, postgres_totals = read_summary(postgres_dir)
fdb_rows, fdb_totals = read_summary(fdb_dir)
postgres_by_path = {path: status for status, path in postgres_rows}
fdb_failures = [path for status, path in fdb_rows if status == "fail"]

classes = Counter()
dirs = Counter()
examples = defaultdict(list)
missing_tables = Counter()
for test_path in fdb_failures:
    classification = classify_failure(fdb_dir, test_path)
    classes[classification] += 1
    if len(examples[classification]) < 8:
        examples[classification].append(test_path)
    parts = test_path.split("/")
    dirs["/".join(parts[:3]) if len(parts) >= 3 else test_path] += 1
    log = log_path(fdb_dir, test_path)
    text = log.read_text(errors="replace") if log.exists() else ""
    for table in re.findall(r"Table with name (ducklake_[A-Za-z0-9_]+) does not exist", text):
        missing_tables[table] += 1

postgres_clean_for_fdb_failures = sum(1 for path in fdb_failures if postgres_by_path.get(path) == "pass")

lines = [
    "# Upstream DuckLake Full Gap Summary",
    "",
    "This report compares regular upstream SQLLogic coverage. Slow `.test_slow` files are tracked",
    "by the explicit `slow` runner mode and are not included here.",
    "",
    "## Totals",
    "",
    f"- Postgres full: {postgres_totals['pass']} passed, {postgres_totals['skip']} skipped, {postgres_totals['fail']} failed.",
    f"- FoundationDB full: {fdb_totals['pass']} passed, {fdb_totals['skip']} skipped, {fdb_totals['fail']} failed.",
    f"- FDB failures that pass on Postgres: {postgres_clean_for_fdb_failures}.",
    "",
    "## FDB Failure Classes",
    "",
]
for name, count in classes.most_common():
    lines.append(f"- {count}: {name}")
    for path in examples[name]:
        lines.append(f"  - `{path}`")

lines += ["", "## FDB Failures By Directory", ""]
for name, count in dirs.most_common():
    lines.append(f"- {count}: `{name}`")

lines += ["", "## Missing Hidden Metadata Tables", ""]
if missing_tables:
    for table, count in missing_tables.most_common():
        lines.append(f"- {count}: `{table}`")
else:
    lines.append("- None.")

out_path.write_text("\n".join(lines) + "\n")
print(out_path)
PY
