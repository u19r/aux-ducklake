CREATE SCHEMA dl.ddl;
CREATE TABLE dl.ddl.orders(id INTEGER, qty INTEGER DEFAULT 5, note VARCHAR);
INSERT INTO dl.ddl.orders(id, note) VALUES (1, 'first'), (2, 'second');
SET VARIABLE ddl_parity_after_insert = (SELECT id FROM ducklake_current_snapshot('dl'));
COMMENT ON TABLE dl.ddl.orders IS 'orders table';
COMMENT ON COLUMN dl.ddl.orders.qty IS 'order quantity';
ALTER TABLE dl.ddl.orders RENAME TO orders_archive;
ALTER TABLE dl.ddl.orders_archive RENAME COLUMN qty TO quantity;
ALTER TABLE dl.ddl.orders_archive ALTER quantity TYPE BIGINT;
ALTER TABLE dl.ddl.orders_archive ALTER quantity SET DEFAULT 7;
ALTER TABLE dl.ddl.orders_archive DROP COLUMN note;
INSERT INTO dl.ddl.orders_archive(id) VALUES (3);
CREATE VIEW dl.ddl.orders_view AS SELECT id, quantity FROM dl.ddl.orders_archive;
COMMENT ON VIEW dl.ddl.orders_view IS 'orders view';
ALTER VIEW dl.ddl.orders_view RENAME TO orders_view_v2;
SELECT 'ddl_parity_table=' || table_name || ',' || comment
FROM duckdb_tables()
WHERE schema_name = 'ddl' AND table_name = 'orders_archive';
SELECT 'ddl_parity_column=' || column_name || ',' || data_type || ',' || column_default || ',' || comment
FROM duckdb_columns()
WHERE schema_name = 'ddl' AND table_name = 'orders_archive' AND column_name = 'quantity';
SELECT 'ddl_parity_rows=' || count(*) || ',' || sum(quantity)
FROM dl.ddl.orders_archive;
SELECT 'ddl_parity_historical=' || count(*) || ',' || sum(qty)
FROM dl.ddl.orders AT (VERSION => getvariable('ddl_parity_after_insert')::BIGINT);
SELECT 'ddl_parity_view=' || view_name || ',' || comment
FROM duckdb_views()
WHERE schema_name = 'ddl' AND view_name = 'orders_view_v2';
SELECT 'ddl_parity_view_rows=' || count(*) || ',' || sum(quantity)
FROM dl.ddl.orders_view_v2;
CREATE MACRO dl.ddl.plus_one(x) AS (x + 1);
SELECT 'ddl_parity_macro=' || dl.ddl.plus_one(4);
DROP MACRO dl.ddl.plus_one;
DROP VIEW dl.ddl.orders_view_v2;
CREATE TABLE dl.ddl.partitioned_items(id INTEGER, region VARCHAR, amount INTEGER);
ALTER TABLE dl.ddl.partitioned_items SET PARTITIONED BY (region);
INSERT INTO dl.ddl.partitioned_items VALUES (1, 'eu', 10), (2, 'us', 20), (3, 'eu', 30);
SET VARIABLE ddl_parity_after_partition_insert = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'ddl_parity_partition_filter=' || count(*) || ',' || sum(amount)
FROM dl.ddl.partitioned_items
WHERE region = 'eu';
ALTER TABLE dl.ddl.partitioned_items RESET PARTITIONED BY;
INSERT INTO dl.ddl.partitioned_items VALUES (4, 'apac', 40);
SELECT 'ddl_parity_partition_historical=' || count(*) || ',' || sum(amount)
FROM dl.ddl.partitioned_items AT (VERSION => getvariable('ddl_parity_after_partition_insert')::BIGINT)
WHERE region = 'us';
CREATE TABLE dl.ddl.sorted_items(id INTEGER, amount INTEGER);
ALTER TABLE dl.ddl.sorted_items SET SORTED BY (amount DESC);
INSERT INTO dl.ddl.sorted_items VALUES (1, 10), (2, 30), (3, 20);
SELECT 'ddl_parity_sort_rows=' || string_agg(id::VARCHAR, ',' ORDER BY amount DESC)
FROM dl.ddl.sorted_items;
ALTER TABLE dl.ddl.sorted_items RESET SORTED BY;
