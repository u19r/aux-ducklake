CREATE TABLE dl.main.core_probe(id INTEGER, amount INTEGER, note VARCHAR);
INSERT INTO dl.main.core_probe VALUES (1, 10, 'one'), (2, 20, 'two');
SELECT 'core_latest=' || count(*) || ',' || sum(amount) || ',' || string_agg(note, ',' ORDER BY id)
FROM dl.main.core_probe;
INSERT INTO dl.main.core_probe VALUES (3, 30, 'three');
SELECT 'core_after_append=' || count(*) || ',' || sum(amount) || ',' || string_agg(note, ',' ORDER BY id)
FROM dl.main.core_probe;
SELECT 'core_time_travel=' || count(*) || ',' || sum(amount) || ',' || string_agg(note, ',' ORDER BY id)
FROM dl.main.core_probe AT (VERSION => 2);
