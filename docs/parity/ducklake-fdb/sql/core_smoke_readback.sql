SELECT 'core_readback=' || count(*) || ',' || sum(amount) || ',' || string_agg(note, ',' ORDER BY id)
FROM dl.main.core_probe;
