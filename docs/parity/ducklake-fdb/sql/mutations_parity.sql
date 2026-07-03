CREATE SCHEMA dl.mut;
CREATE TABLE dl.mut.items(id INTEGER, qty INTEGER);
INSERT INTO dl.mut.items VALUES (1, 10), (2, 20), (3, 30);
SET VARIABLE mutations_parity_after_insert = (SELECT id FROM ducklake_current_snapshot('dl'));
UPDATE dl.mut.items SET qty = qty + 100 WHERE id <= 2;
SET VARIABLE mutations_parity_after_update = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.mut.items WHERE id = 3;
SET VARIABLE mutations_parity_after_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
INSERT INTO dl.mut.items VALUES (4, 40);
SELECT 'mutations_parity_final=' || count(*) || ',' || sum(qty)
FROM dl.mut.items;
SELECT 'mutations_parity_insert_cdf=' || count(*) || ',' || sum(qty)
FROM ducklake_table_changes('dl', 'mut', 'items', 0, getvariable('mutations_parity_after_insert')::BIGINT)
WHERE change_type = 'insert';
SELECT 'mutations_parity_update_preimage=' || count(*) || ',' || sum(qty)
FROM ducklake_table_changes(
    'dl',
    'mut',
    'items',
    getvariable('mutations_parity_after_update')::BIGINT,
    getvariable('mutations_parity_after_update')::BIGINT
)
WHERE change_type = 'update_preimage';
SELECT 'mutations_parity_update_postimage=' || count(*) || ',' || sum(qty)
FROM ducklake_table_changes(
    'dl',
    'mut',
    'items',
    getvariable('mutations_parity_after_update')::BIGINT,
    getvariable('mutations_parity_after_update')::BIGINT
)
WHERE change_type = 'update_postimage';
SELECT 'mutations_parity_delete_cdf=' || count(*) || ',' || sum(qty)
FROM ducklake_table_changes(
    'dl',
    'mut',
    'items',
    getvariable('mutations_parity_after_delete')::BIGINT,
    getvariable('mutations_parity_after_delete')::BIGINT
)
WHERE change_type = 'delete';
SELECT 'mutations_parity_time_travel_update=' || count(*) || ',' || sum(qty)
FROM dl.mut.items AT (VERSION => getvariable('mutations_parity_after_update')::BIGINT);
