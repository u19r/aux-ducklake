#!/usr/bin/env python3
"""Render benchmark preflight SQL with escaped artifact and catalog paths."""

from pathlib import Path
import sys


def quote_sql(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    extension, directory, row_limit, table_count = sys.argv[1:]
    root = Path(directory)
    substitutions = {
        "@EXTENSION@": quote_sql(extension),
        "@METADATA@": quote_sql("ducklake:" + str(root / "metadata.duckdb")),
        "@DATA_PATH@": quote_sql(root / "data"),
        "@INLINE_ROW_LIMIT@": str(int(row_limit)),
        "@MAINTENANCE_GLOB@": quote_sql(root / "data/main/preflight_maintenance/**/*.parquet"),
        "@CENTRAL_TABLES@": "\n".join(
            f"CREATE TABLE dl.main.preflight_central_{index:03d}(id INTEGER);"
            for index in range(int(table_count))
        ),
    }
    fixtures = Path(__file__).parent / "test-fixtures"
    for phase, fixture in [("one", "create"), ("two", "reopen")]:
        sql = (fixtures / f"benchmark-preflight-{fixture}.sql").read_text()
        for marker, value in substitutions.items():
            sql = sql.replace(marker, value)
        (root / f"phase-{phase}.sql").write_text(sql)


if __name__ == "__main__":
    main()
