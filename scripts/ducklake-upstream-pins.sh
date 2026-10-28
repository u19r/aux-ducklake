#!/usr/bin/env bash
# Versions: software-versions.snapshot.json; details in software-version-consumers.md.
set -euo pipefail

# DuckLake integration pin selected by the paired DuckDB release configuration.
export DUCKLAKE_COMMIT="417a9117f590e8141742ac0579d02f6a90162e37"
export DUCKDB_COMMIT="d54cbbe3df17e8e5ce68312f483b4155836112aa"
export DUCKDB_VERSION="v2.0.0"
