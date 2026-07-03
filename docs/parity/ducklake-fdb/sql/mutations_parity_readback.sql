SELECT 'mutations_parity_readback=' || count(*) || ',' || sum(qty)
FROM dl.mut.items;
SET VARIABLE mutations_parity_readback_snapshot = (SELECT id FROM ducklake_current_snapshot('dl'));
SELECT 'mutations_parity_readback_changes=' || count(*)
FROM ducklake_table_changes('dl', 'mut', 'items', 0, getvariable('mutations_parity_readback_snapshot')::BIGINT);
