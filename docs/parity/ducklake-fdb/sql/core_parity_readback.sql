SELECT 'core_parity_readback=' || count(*) || ',' || sum(amount)
FROM dl.extra.core_orders;
