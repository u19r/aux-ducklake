import { strict as assert } from 'node:assert';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { test } from 'node:test';

function runFixture(directory: string, mismatch: boolean, queryDirectory?: string, selectedBackends = 'both') {
  const fake = `#!${process.execPath}
const fs = require('node:fs');
const sql = fs.readFileSync(0, 'utf8');
if (process.argv[1].endsWith('fdb')) console.log('runtime_bridge=cpp_ffi\\nruntime_ffi=ok\\noperation=AttachMetadata\\nbackend=foundationdb\\npayload_bytes=123\\nmetadata_attached=true\\nruntime_catalog_id=1');
if (sql.includes('FROM tpch_queries()')) {
  if (${queryDirectory !== undefined}) throw new Error('Unexpected query generation');
  console.log(JSON.stringify(Array.from({length: 22}, () => ({query: 'SELECT 42 AS answer;'}))));
} else if (sql.includes('CALL dbgen')) {
  process.stdout.write('');
} else if (sql.includes('CREATE TABLE')) {
  if (process.argv[1].endsWith('fdb')) console.log('runtime_bridge=cpp_ffi\\nruntime_ffi=ok\\noperation=InitializeDuckLake\\nbackend=foundationdb\\ncatalog_snapshot_order=00000000000000000001');
  console.log('[{"lineitem":30000000}]');
} else {
  const wrong = ${mismatch} && process.argv[1].endsWith('fdb');
  console.log(JSON.stringify([{answer: wrong ? 43 : 42}]));
  if (sql.includes('.timer on')) console.log('Run Time (s): real 0.001 user 0.000 sys 0.000');
}
`;
  for (const name of ['stock', 'fdb']) {
    writeFileSync(join(directory, name), fake);
    chmodSync(join(directory, name), 0o755);
  }
  return spawnSync(process.execPath, [resolve('scripts/ducklake-tpch-benchmark.ts'),
    '--scale', '5', '--work-dir', join(directory, 'work'), '--output-dir', join(directory, 'output'),
    '--stock-duckdb', join(directory, 'stock'), '--backends', selectedBackends,
    ...(selectedBackends === 'both' ? ['--fdb-duckdb', join(directory, 'fdb'), '--runtime', '/fixture/runtime.so', '--cluster-file', '/fixture/fdb.cluster'] : []),
    '--postgres-dsn', 'host=127.0.0.1 dbname=fixture', '--trials', '2',
    ...(queryDirectory === undefined ? [] : ['--query-dir', queryDirectory]),
  ], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
}

test('given matching results when benchmarking then all 22 queries have checked warmup and alternating measured pairs', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ducklake-tpch-'));
  try {
    const result = runFixture(directory, false);
    assert.equal(result.status, 0, result.stderr);
    const samples = readFileSync(join(directory, 'output/samples.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    assert.equal(samples.length, 22 * 3 * 2);
    assert.equal(samples[0].queryMs, 1);
    assert.equal(samples.filter(sample => !sample.warmup).length, 22 * 2 * 2);
    assert.deepEqual(samples.slice(0, 6).map(sample => sample.backend), ['postgres', 'fdb', 'fdb', 'postgres', 'postgres', 'fdb']);
    assert.equal(JSON.parse(readFileSync(join(directory, 'output/complete.json'), 'utf8')).status, 'passed');
    assert.match(readFileSync(join(directory, 'output/fdb-attach.sql'), 'utf8'), /META_TYPE 'aux_catalog'/);
    assert.match(readFileSync(join(directory, 'output/postgres-attach.sql'), 'utf8'), /ducklake:postgres:/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('given a wrong backend result when benchmarking then the run fails without a passing receipt', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ducklake-tpch-'));
  try {
    const result = runFixture(directory, true);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /fdb q1 trial 0.*expected 42, received 43/);
    assert.equal(existsSync(join(directory, 'output/complete.json')), false);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('given canonical query files when comparing releases then their SQL is used without version-specific regeneration', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ducklake-tpch-'));
  try {
    for (let query = 1; query <= 22; query++) {
      writeFileSync(join(directory, `q${query}.sql`), `SELECT 42 AS answer; -- canonical query ${query}\n`);
    }
    const result = runFixture(directory, false, directory);
    assert.equal(result.status, 0, result.stderr);
    const sample = JSON.parse(readFileSync(join(directory, 'output/samples.jsonl'), 'utf8').split('\n').at(0) ?? 'null');
    const expected = createHash('sha256').update('SELECT 42 AS answer; -- canonical query 1').digest('hex');
    assert.equal(sample.sqlSha256, expected);
    assert.equal(readFileSync(join(directory, 'output/q1.sql'), 'utf8'), 'SELECT 42 AS answer; -- canonical query 1\n');
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('given a Postgres-only release control when benchmarking then no aux binary or runtime is required', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ducklake-tpch-'));
  try {
    const result = runFixture(directory, false, undefined, 'postgres');
    assert.equal(result.status, 0, result.stderr);
    const samples = readFileSync(join(directory, 'output/samples.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    assert.equal(samples.length, 22 * 3);
    assert.ok(samples.every(sample => sample.backend === 'postgres'));
    const receipt = JSON.parse(readFileSync(join(directory, 'output/complete.json'), 'utf8'));
    assert.deepEqual(receipt.backends, ['postgres']);
    assert.equal(existsSync(join(directory, 'output/fdb-environment.json')), false);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
