SET VARIABLE before_inline = (SELECT id FROM ducklake_current_snapshot('dl'));
INSERT INTO dl.main.mixed_probe VALUES (101, 'inline_101'), (102, 'inline_102');
SET VARIABLE after_inline = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'mixed_latest=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM dl.main.mixed_probe;
SELECT 'mixed_inline_cdf_insert=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'insert') || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM ducklake_table_changes(
    'dl', 'main', 'mixed_probe',
    getvariable('before_inline')::BIGINT + 1,
    getvariable('after_inline')::BIGINT
)
WHERE id IN (101, 102);
SET VARIABLE before_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
DELETE FROM dl.main.mixed_probe WHERE id = 101;
SET VARIABLE after_delete = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'mixed_inline_delete=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%') || ',' ||
       count(*) FILTER (WHERE id = 101)
FROM dl.main.mixed_probe;
SELECT 'mixed_inline_cdf_delete=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE change_type = 'delete') || ',' ||
       count(*) FILTER (WHERE note = 'inline_101')
FROM ducklake_table_changes(
    'dl', 'main', 'mixed_probe',
    getvariable('before_delete')::BIGINT + 1,
    getvariable('after_delete')::BIGINT
)
WHERE id = 101;
CALL ducklake_flush_inlined_data('dl', table_name => 'mixed_probe');
SELECT 'mixed_after_flush=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM dl.main.mixed_probe;
