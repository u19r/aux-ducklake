CREATE SCHEMA dl.extra;
CREATE TABLE dl.extra.core_customers AS
SELECT i::INTEGER AS id, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END AS segment
FROM range(1, 6) r(i);
CREATE TABLE dl.extra.core_orders AS
SELECT i::INTEGER AS order_id, ((i % 5) + 1)::INTEGER AS customer_id, (i * 100)::INTEGER AS amount
FROM range(1, 4) r(i);
SET VARIABLE core_parity_before_append = (SELECT id FROM ducklake_current_snapshot('dl'));
INSERT INTO dl.extra.core_orders VALUES (4, 2, 400);
SELECT 'core_parity_join_filter=' || count(*) || ',' || sum(o.amount)
FROM dl.extra.core_orders o
JOIN dl.extra.core_customers c ON c.id = o.customer_id
WHERE c.segment = 'even' OR o.amount >= 300;
SELECT 'core_parity_projection=' || max(amount) || ',' || count(*)
FROM (SELECT order_id, amount / 10 AS amount FROM dl.extra.core_orders WHERE amount >= 200) projected;
SELECT 'core_parity_time_travel=' || count(*) || ',' || sum(amount)
FROM dl.extra.core_orders AT (VERSION => getvariable('core_parity_before_append')::BIGINT);
