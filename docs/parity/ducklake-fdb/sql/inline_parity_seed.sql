CREATE TABLE dl.main.mixed_probe(id INTEGER, note VARCHAR);
INSERT INTO dl.main.mixed_probe
SELECT i::INTEGER, 'file_' || i::VARCHAR
FROM range(1, 101) t(i);
SELECT 'mixed_file_seed=' || count(*) || ',' || sum(id)
FROM dl.main.mixed_probe;
