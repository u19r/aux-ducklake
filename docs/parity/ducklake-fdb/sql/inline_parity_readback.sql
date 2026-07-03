SELECT 'mixed_inline_delete_readback=' || count(*) || ',' || sum(id) || ',' ||
       count(*) FILTER (WHERE note LIKE 'inline_%')
FROM dl.main.mixed_probe;
