LOAD @EXTENSION@;
ATTACH @METADATA@ AS dl (
    DATA_PATH @DATA_PATH@,
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT @INLINE_ROW_LIMIT@
);
SELECT 'preflight_central_metadata=' || count(*)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_central_bounds=' || min(table_name) || '|' || max(table_name)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_identity=' ||
       string_agg(table_name || ':' || CAST(table_id AS VARCHAR), '|' ORDER BY table_name)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_identity_stats=' || count(*) || ',' || count(DISTINCT table_id)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_reopen_schema=' ||
       (SELECT count(*) FROM duckdb_tables()
        WHERE database_name = 'dl' AND schema_name = 'main' AND table_name = 'preflight_main') || ',' ||
       (SELECT count(*) FROM duckdb_tables()
        WHERE database_name = 'dl' AND schema_name = 'analytics' AND table_name = 'preflight_explicit');
SELECT 'preflight_reopen_ctas=' || count(*) || ',' || coalesce(sum(id), 0) || ',' ||
       coalesce(string_agg(marker, ',' ORDER BY marker), '')
FROM dl.main.preflight_ctas;
SELECT 'preflight_reopen_view=' || count(*) || ',' || coalesce(sum(id), 0) || ',' ||
       coalesce(string_agg(marker, ',' ORDER BY marker), '')
FROM dl.analytics.preflight_view;
SELECT 'preflight_reopen_inline=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline;
SELECT 'preflight_reopen_maintenance=' || count(*)
FROM ducklake_cleanup_old_files('dl', dry_run => true, cleanup_all => true);
DETACH dl;
