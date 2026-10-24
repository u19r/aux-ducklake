LOAD @EXTENSION@;
ATTACH @METADATA@ AS dl (
    DATA_PATH @DATA_PATH@,
    META_TYPE 'aux_catalog',
    DATA_INLINING_ROW_LIMIT @INLINE_ROW_LIMIT@
);
SELECT 'preflight_initial_snapshots=' || count(*) FROM ducklake_snapshots('dl');
CREATE SCHEMA dl.analytics;
CREATE TABLE dl.main.preflight_main(id INTEGER, marker VARCHAR);
CREATE TABLE dl.analytics.preflight_explicit(id INTEGER, marker VARCHAR);
CREATE TABLE dl.main.preflight_ctas AS SELECT 1 AS id, 'ctas' AS marker;
CREATE VIEW dl.analytics.preflight_view AS SELECT id, marker FROM dl.main.preflight_ctas;
CREATE TABLE dl.main.preflight_inline(id INTEGER, marker VARCHAR);
INSERT INTO dl.main.preflight_inline VALUES (1, 'one'), (2, 'two');
SELECT 'preflight_inline_before_delete=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline;
DELETE FROM dl.main.preflight_inline WHERE id = 1;
SELECT 'preflight_inline_after_delete=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline;
CALL ducklake_flush_inlined_data('dl', table_name => 'preflight_inline');
SELECT 'preflight_inline_after_flush=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline;
CREATE TABLE dl.main.preflight_inline_overshoot(id INTEGER, marker VARCHAR);
INSERT INTO dl.main.preflight_inline_overshoot
SELECT id, 'overshoot' FROM range(1, 12) AS ids(id);
SELECT 'preflight_inline_overshoot=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline_overshoot;
CALL ducklake_flush_inlined_data('dl', table_name => 'preflight_inline_overshoot');
SELECT 'preflight_inline_overshoot_after_flush=' || count(*) || ',' || coalesce(sum(id), 0)
FROM dl.main.preflight_inline_overshoot;
CREATE TABLE dl.main.preflight_maintenance(id INTEGER);
INSERT INTO dl.main.preflight_maintenance VALUES (1), (2), (3);
CALL ducklake_flush_inlined_data('dl', table_name => 'preflight_maintenance');
DROP TABLE dl.main.preflight_maintenance;
CALL ducklake_expire_snapshots('dl', older_than => now());
SELECT 'preflight_maintenance_candidates=' || count(*)
FROM (
    SELECT * FROM ducklake_cleanup_old_files('dl', cleanup_all => true)
    UNION ALL
    SELECT * FROM ducklake_cleanup_old_files(
        'dl', older_than => TIMESTAMP '1970-01-01', dry_run => true
    )
) candidates;
SELECT 'preflight_maintenance_remaining=' || count(*)
FROM ducklake_cleanup_old_files('dl', dry_run => true, cleanup_all => true);
SELECT 'preflight_maintenance_files=' || count(*)
FROM glob(@MAINTENANCE_GLOB@);
@CENTRAL_TABLES@
SELECT 'preflight_schema_tables=' ||
       (SELECT count(*) FROM duckdb_tables()
        WHERE database_name = 'dl' AND schema_name = 'main' AND table_name = 'preflight_main') || ',' ||
       (SELECT count(*) FROM duckdb_tables()
        WHERE database_name = 'dl' AND schema_name = 'analytics' AND table_name = 'preflight_explicit');
SELECT 'preflight_ctas=' || count(*) || ',' || coalesce(sum(id), 0) || ',' ||
       coalesce(string_agg(marker, ',' ORDER BY marker), '')
FROM dl.main.preflight_ctas;
SELECT 'preflight_view=' || count(*) || ',' || coalesce(sum(id), 0) || ',' ||
       coalesce(string_agg(marker, ',' ORDER BY marker), '')
FROM dl.analytics.preflight_view;
SELECT 'preflight_central_metadata=' || count(*)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_central_bounds=' || min(table_name) || '|' || max(table_name)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_identity_stats=' || count(*) || ',' || count(DISTINCT table_id)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
SELECT 'preflight_identity=' ||
       string_agg(table_name || ':' || CAST(table_id AS VARCHAR), '|' ORDER BY table_name)
FROM ducklake_table_info('dl')
WHERE table_name LIKE 'preflight_central_%';
DETACH dl;
