SELECT 'ddl_parity_readback_rows=' || count(*) || ',' || sum(quantity)
FROM dl.ddl.orders_archive;
SELECT 'ddl_parity_readback_view_missing=' || count(*)
FROM duckdb_views()
WHERE schema_name = 'ddl' AND view_name = 'orders_view_v2';
