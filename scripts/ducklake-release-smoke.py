#!/usr/bin/env python3
"""Verify a release bundle through the shared-library C API and FoundationDB."""

import ctypes
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import re
import threading
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid


class DuckDbResult(ctypes.Structure):
    _fields_ = [
        ("column_count", ctypes.c_uint64),
        ("row_count", ctypes.c_uint64),
        ("rows_changed", ctypes.c_uint64),
        ("columns", ctypes.c_void_p),
        ("error", ctypes.c_void_p),
        ("internal_data", ctypes.c_void_p),
    ]


class DuckDbLibrary:
    def __init__(self, path):
        self.library = ctypes.CDLL(str(path))
        result = ctypes.POINTER(DuckDbResult)
        pointer = ctypes.c_void_p
        index = ctypes.c_uint64
        signatures = [
            ("duckdb_open", [ctypes.c_char_p, ctypes.POINTER(pointer)], ctypes.c_int),
            ("duckdb_close", [ctypes.POINTER(pointer)], None),
            ("duckdb_connect", [pointer, ctypes.POINTER(pointer)], ctypes.c_int),
            ("duckdb_disconnect", [ctypes.POINTER(pointer)], None),
            ("duckdb_query", [pointer, ctypes.c_char_p, result], ctypes.c_int),
            ("duckdb_destroy_result", [result], None),
            ("duckdb_result_error", [result], ctypes.c_char_p),
            ("duckdb_column_count", [result], index),
            ("duckdb_row_count", [result], index),
            ("duckdb_value_varchar", [result, index, index], pointer),
            ("duckdb_free", [pointer], None),
        ]
        for name, arguments, returns in signatures:
            function = getattr(self.library, name)
            function.argtypes = arguments
            function.restype = returns
        self.database = pointer()
        self.connection = pointer()
        assert self.library.duckdb_open(None, ctypes.byref(self.database)) == 0
        assert self.library.duckdb_connect(self.database, ctypes.byref(self.connection)) == 0

    def query(self, sql):
        result = DuckDbResult()
        address = ctypes.byref(result)
        try:
            status = self.library.duckdb_query(self.connection, sql.encode(), address)
            if status:
                raise RuntimeError(self.library.duckdb_result_error(address).decode())
            return [
                [self.read_value(address, column, row) for column in range(self.library.duckdb_column_count(address))]
                for row in range(self.library.duckdb_row_count(address))
            ]
        finally:
            self.library.duckdb_destroy_result(address)

    def read_value(self, result, column, row):
        value = self.library.duckdb_value_varchar(result, column, row)
        if not value:
            return None
        try:
            return ctypes.string_at(value).decode()
        finally:
            self.library.duckdb_free(value)

    def close(self):
        self.library.duckdb_disconnect(ctypes.byref(self.connection))
        self.library.duckdb_close(ctypes.byref(self.database))


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


class ParquetHandler(BaseHTTPRequestHandler):
    def write_headers(self, status, start, end):
        self.send_response(status)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(self.server.parquet)}")
        self.end_headers()

    def do_HEAD(self):
        self.write_headers(200, 0, len(self.server.parquet) - 1)

    def do_GET(self):
        start, end, status = 0, len(self.server.parquet) - 1, 200
        if self.headers.get("Range"):
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", self.headers["Range"])
            if not match:
                self.send_error(400)
                return
            start = int(match[1])
            end = min(int(match[2]) if match[2] else end, end)
            status = 206
        self.write_headers(status, start, end)
        self.wfile.write(self.server.parquet[start:end + 1])

    def log_message(self, *_arguments):
        pass


def verify_http_parquet(library, root):
    parquet = root / "http.parquet"
    library.query(f"COPY (SELECT 42 AS id) TO {sql_string(parquet)} (FORMAT PARQUET)")
    server = HTTPServer(("127.0.0.1", 0), ParquetHandler)
    server.parquet = parquet.read_bytes()
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        library.query("SET GLOBAL httpfs_client_implementation = 'curl'")
        actual = library.query(f"SELECT id FROM read_parquet('http://127.0.0.1:{server.server_port}/data.parquet')")
        assert actual == [["42"]], actual
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


def verify_catalog(library, root, metadata_type):
    metadata = root / "catalog.duckdb"
    attach = (
        f"ATTACH {sql_string('ducklake:' + str(metadata))} AS lake "
        f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 100"
        + (", META_TYPE 'aux_catalog'" if metadata_type == "fdb" else "")
        + ")"
    )
    library.query(attach)
    if metadata_type == "duckdb":
        assert library.query("SELECT value FROM __ducklake_metadata_lake.ducklake_metadata WHERE key = 'version'") == [["1.1"]]
    else:
        assert library.query("SELECT catalog_type FROM ducklake_settings('lake')") == [["aux_catalog"]]
    verify_committed_flush(library, attach)
    verify_transactional_flush(library)
    verify_default_evolution(library)
    verify_nested_schemas(library, attach)
    verify_concurrent_schemas(library)
    verify_sorted_flush(library)
    if metadata_type == "fdb":
        verify_view_column_tag_rejection(library)
        verify_transaction_fence(library)
    library.query("DETACH lake")


def verify_committed_flush(library, attach):
    library.query("CREATE SCHEMA lake.events")
    library.query("CREATE TABLE lake.events.rows(id BIGINT, row_id BIGINT, begin_snapshot BIGINT, payload VARCHAR)")
    library.query("INSERT INTO lake.events.rows VALUES (1, 11, 21, 'a'), (2, 12, 22, 'b'), (3, 13, 23, 'c')")
    inserted_snapshot = library.query("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")[0][0]
    library.query("BEGIN")
    library.query("UPDATE lake.events.rows SET payload = 'changed' WHERE id = 2")
    library.query("DELETE FROM lake.events.rows WHERE id = 1")
    expected = [["2", "12", "22", "changed"], ["3", "13", "23", "c"]]
    actual = library.query("SELECT * FROM lake.events.rows ORDER BY id")
    assert actual == expected, actual
    library.query("COMMIT")
    library.query("CALL ducklake_flush_inlined_data('lake')")
    actual = library.query("SELECT * FROM lake.events.rows ORDER BY id")
    assert actual == expected, actual
    assert library.query(f"SELECT count(*) FROM lake.events.rows AT (VERSION => {inserted_snapshot})") == [["3"]]
    library.query("DETACH lake")
    library.query(attach)
    actual = library.query("SELECT * FROM lake.events.rows ORDER BY id")
    assert actual == expected, actual


def verify_transactional_flush(library):
    library.query("CREATE TABLE lake.events.pending(id BIGINT, payload VARCHAR)")
    library.query("INSERT INTO lake.events.pending VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    library.query("BEGIN")
    library.query("DELETE FROM lake.events.pending WHERE id = 1")
    library.query("CALL ducklake_flush_inlined_data('lake', table_name => 'pending', schema_name => 'events')")
    library.query("COMMIT")
    assert library.query("SELECT id FROM lake.events.pending ORDER BY id") == [["2"], ["3"]]
    library.query("ALTER TABLE lake.events.pending ADD COLUMN extra INTEGER DEFAULT 7")
    library.query("INSERT INTO lake.events.pending VALUES (4, 'd', 8)")
    actual = library.query("SELECT id, extra FROM lake.events.pending ORDER BY id")
    assert actual == [["2", "7"], ["3", "7"], ["4", "8"]], actual


def verify_default_evolution(library):
    library.query("CREATE TABLE lake.events.defaults(id BIGINT, nullable BIGINT)")
    library.query("INSERT INTO lake.events.defaults VALUES (1, NULL), (2, NULL)")
    library.query("ALTER TABLE lake.events.defaults ADD COLUMN added BIGINT DEFAULT 9")
    library.query("INSERT INTO lake.events.defaults VALUES (3, 7, 0)")
    library.query("BEGIN; ALTER TABLE lake.events.defaults ADD COLUMN concurrent_default BIGINT DEFAULT 9; INSERT INTO lake.events.defaults VALUES (4, 8, 0, 10); COMMIT")
    actual = library.query("SELECT id, concurrent_default FROM lake.events.defaults ORDER BY id")
    assert actual == [["1", "9"], ["2", "9"], ["3", "9"], ["4", "10"]], actual
    actual = library.query("SELECT id FROM lake.events.defaults WHERE concurrent_default = 9 ORDER BY id")
    assert actual == [["1"], ["2"], ["3"]], actual
    assert library.query("SELECT id FROM lake.events.defaults WHERE added = 9 ORDER BY id") == [["1"], ["2"]]
    assert library.query("SELECT id FROM lake.events.defaults WHERE nullable = 7") == [["3"]]
    library.query("CALL ducklake_flush_inlined_data('lake', table_name => 'defaults', schema_name => 'events')")
    assert library.query("SELECT id FROM lake.events.defaults WHERE added = 9 ORDER BY id") == [["1"], ["2"]]


def verify_nested_schemas(library, attach):
    library.query("CREATE SCHEMA lake.events.child")
    library.query("CREATE SCHEMA lake.other")
    library.query("CREATE SCHEMA lake.other.child")
    library.query("CREATE TABLE lake.events.child.items AS SELECT 1 AS id")
    library.query("CREATE TABLE lake.other.child.items AS SELECT 2 AS id")
    library.query("DETACH lake")
    library.query(attach)
    assert library.query("SELECT * FROM lake.events.child.items") == [["1"]]
    assert library.query("SELECT * FROM lake.other.child.items") == [["2"]]
    try:
        library.query("DROP SCHEMA lake.events")
    except RuntimeError as error:
        assert "depend" in str(error).lower(), str(error)
    else:
        raise AssertionError("nonempty parent schema was dropped")


def verify_concurrent_schemas(library):
    first = library.connection
    second = ctypes.c_void_p()
    assert library.library.duckdb_connect(library.database, ctypes.byref(second)) == 0
    try:
        for connection in [first, second]:
            library.connection = connection
            library.query("SET immediate_transaction_mode = true")
            library.query("BEGIN")
        library.connection = first
        library.query("CREATE SCHEMA lake.events.parallel")
        library.connection = second
        library.query("CREATE SCHEMA lake.other.parallel")
        library.connection = first
        library.query("COMMIT")
        library.connection = second
        library.query("COMMIT")
        library.connection = first
        library.query("CREATE TABLE lake.events.parallel.items AS SELECT 1 AS id")
        library.query("CREATE TABLE lake.other.parallel.items AS SELECT 2 AS id")
        assert library.query("SELECT * FROM lake.events.parallel.items UNION ALL SELECT * FROM lake.other.parallel.items ORDER BY id") == [["1"], ["2"]]
    finally:
        library.connection = first
        library.library.duckdb_disconnect(ctypes.byref(second))


def verify_sorted_flush(library):
    library.query("CREATE TABLE lake.events.sorted(id BIGINT, amount DECIMAL(10, 2), bucket VARCHAR)")
    library.query("ALTER TABLE lake.events.sorted SET PARTITIONED BY (bucket)")
    library.query("INSERT INTO lake.events.sorted VALUES (1, 9.9, 'a'), (2, 10.0, 'a'), (3, NULL, 'a')")
    library.query("ALTER TABLE lake.events.sorted SET SORTED BY (amount DESC NULLS FIRST)")
    library.query("INSERT INTO lake.events.sorted VALUES (4, 11.0, 'a'), (5, 12.0, 'a')")
    library.query("DELETE FROM lake.events.sorted WHERE id = 2")
    library.query("CALL ducklake_flush_inlined_data('lake', table_name => 'sorted', schema_name => 'events')")
    assert library.query("SELECT id FROM lake.events.sorted ORDER BY id") == [["1"], ["3"], ["4"], ["5"]]
    files = library.query("SELECT data_file FROM ducklake_list_files('lake', 'sorted', schema => 'events')")
    assert files
    for [file] in files:
        amounts = library.query(f"SELECT amount FROM read_parquet({sql_string(file)})")
        assert amounts == sorted(amounts, key=lambda row: (row[0] is not None, -float(row[0] or 0))), amounts


def verify_view_column_tag_rejection(library):
    library.query("CREATE TABLE lake.events.ducklake_view_column_tag(id INTEGER)")
    library.query("INSERT INTO lake.events.ducklake_view_column_tag VALUES (17)")
    assert library.query("SELECT * FROM lake.events.ducklake_view_column_tag") == [["17"]]
    library.query("CREATE VIEW lake.events.comment_view AS SELECT 1 AS id")
    try:
        library.query("COMMENT ON COLUMN lake.events.comment_view.id IS 'identifier'")
    except RuntimeError as error:
        assert "does not support view column tags" in str(error), str(error)
    else:
        raise AssertionError("view column tag was silently acknowledged")
    assert library.query("SELECT * FROM lake.events.comment_view") == [["1"]]


def verify_transaction_fence(library):
    library.query("CREATE TABLE lake.events.fenced_items(id INTEGER)")
    library.query("CREATE TABLE lake.events.fence_competitor(id INTEGER)")
    library.query("BEGIN")
    library.query("CALL ducklake_set_transaction_fence('lake')")
    library.query("INSERT INTO lake.events.fenced_items VALUES (1)")
    library.query("COMMIT")
    assert library.query("SELECT * FROM lake.events.fenced_items") == [["1"]]

    first = library.connection
    second = ctypes.c_void_p()
    assert library.library.duckdb_connect(library.database, ctypes.byref(second)) == 0
    try:
        library.query("BEGIN")
        library.query("CALL ducklake_set_transaction_fence('lake')")
        library.query("INSERT INTO lake.events.fenced_items VALUES (2)")
        library.connection = second
        library.query("INSERT INTO lake.events.fence_competitor VALUES (3)")
        library.connection = first
        try:
            library.query("COMMIT")
        except RuntimeError as error:
            assert "snapshot fence changed" in str(error).lower(), str(error)
        else:
            raise AssertionError("a competing catalog commit did not reject the fenced transaction")
        assert library.query("SELECT * FROM lake.events.fenced_items") == [["1"]]
        assert library.query("SELECT * FROM lake.events.fence_competitor") == [["3"]]
    finally:
        library.connection = first
        library.library.duckdb_disconnect(ctypes.byref(second))


def verify_encrypted_catalog(library, root):
    attach = (
        f"ATTACH {sql_string('ducklake:' + str(root / 'encrypted.duckdb'))} AS secure "
        f"(DATA_PATH {sql_string(root / 'encrypted-data')}, META_TYPE 'aux_catalog', "
        "ENCRYPTED, DATA_INLINING_ROW_LIMIT 100, AUTOMATIC_MIGRATION true)"
    )
    library.query(attach)
    library.query("CREATE TABLE secure.items(id BIGINT, payload VARCHAR)")
    library.query("INSERT INTO secure.items VALUES (1, 'keep'), (2, 'delete')")
    library.query("CALL ducklake_flush_inlined_data('secure')")
    library.query("DELETE FROM secure.items WHERE id = 2")
    library.query("DETACH secure")
    library.query(attach)
    assert library.query("SELECT * FROM secure.items") == [["1", "keep"]]
    library.query("DETACH secure")


def clear_test_prefix(prefix):
    command = ["fdbcli", "--timeout", "10"]
    cluster = os.environ.get("AUX_DUCKLAKE_FDB_CLUSTER_FILE")
    if cluster:
        command += ["-C", cluster]
    command += ["--exec", f'writemode on; clearrange "{prefix}" "{prefix[:-1]}0"']
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: scripts/ducklake-release-smoke.py <bundle-root>")
    root = Path(sys.argv[1]).resolve()
    suffix = "dylib" if sys.platform == "darwin" else "so"
    prefix = f"aux-ducklake/release-smoke/{uuid.uuid4()}/"
    os.environ["AUX_DUCKLAKE_FDB_PREFIX"] = prefix
    os.environ["AUX_DUCKLAKE_CATALOG_BACKEND"] = "fdb"
    os.environ["AUX_DUCKLAKE_RUNTIME_LIBRARY"] = str(root / f"opt/aux-ducklake/lib/libducklake_catalog.{suffix}")
    library = DuckDbLibrary(root / f"opt/duckdb/libduckdb.{suffix}")
    try:
        version = library.query("SELECT version()")[0][0]
        assert version == "v2.0.0", version
        extensions = library.query("SELECT extension_name FROM duckdb_extensions() WHERE loaded ORDER BY extension_name")
        assert {"ducklake", "httpfs", "icu", "json", "parquet", "tpch"} <= {row[0] for row in extensions}
        with tempfile.TemporaryDirectory(prefix="aux-ducklake-release-") as temporary:
            verify_http_parquet(library, Path(temporary))
            for backend in ["duckdb", "fdb"]:
                catalog_root = Path(temporary) / backend
                catalog_root.mkdir()
                verify_catalog(library, catalog_root, backend)
            verify_encrypted_catalog(library, Path(temporary))
        print(json.dumps({"status": "passed", "duckdb": version, "ducklake": "1.1", "catalogs": ["duckdb", "fdb"], "transaction_fence": "passed"}))
    finally:
        library.close()
        clear_test_prefix(prefix)


if __name__ == "__main__":
    main()
