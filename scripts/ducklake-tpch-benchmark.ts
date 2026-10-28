import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { parseArgs } from 'node:util';

interface Backend {
  name: 'fdb' | 'postgres';
  binary: string;
  prepare: string;
  environment: NodeJS.ProcessEnv;
}

function quoteSql(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function requireOption(values: Record<string, string | boolean | undefined>, name: string): string {
  const value = values[name];
  if (typeof value !== 'string' || value.length === 0) throw new Error(`--${name} is required`);
  return value;
}

function executeSql(backend: Backend, sql: string): { elapsedMs: number; queryMs: number | null; output: string } {
  const start = performance.now();
  const result = spawnSync(backend.binary, ['-unsigned', '-batch', '-bail', '-json'], {
    input: backend.prepare + '\n' + sql,
    encoding: 'utf8',
    env: backend.environment,
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`${backend.name}: ${result.error?.message ?? result.stderr}`);
  const elapsedMs = performance.now() - start;
  const timings = [...result.stdout.matchAll(/^Run Time \(s\): real ([0-9.]+).*$/gm)];
  if (timings.length > 1) throw new Error('Expected one timed query');
  const seconds = timings[0]?.[1];
  const output = result.stdout
    .replace(/^Run Time \(s\): real .*$/gm, '')
    .replace(/^runtime_bridge=cpp_ffi\n/gm, '')
    .replace(/^runtime_ffi=ok\noperation=AttachMetadata\nbackend=foundationdb\npayload_bytes=\d+\nmetadata_attached=true\nruntime_catalog_id=\d+\n/gm, '')
    .replace(/^runtime_ffi=ok\noperation=InitializeDuckLake\nbackend=foundationdb\ncatalog_snapshot_order=[0-9a-f]+\n/gm, '')
    .trim();
  return { elapsedMs, queryMs: seconds === undefined ? null : Number(seconds) * 1000, output };
}

function hashText(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

function compareValues(expected: unknown, actual: unknown, location = 'result'): void {
  if (typeof expected === 'number' && typeof actual === 'number') {
    if (Math.abs(expected - actual) <= 1e-9 * Math.max(1, Math.abs(expected))) return;
  } else if (Array.isArray(expected) && Array.isArray(actual)) {
    if (expected.length !== actual.length) throw new Error(`${location}: row count differs`);
    expected.forEach((value: unknown, index: number) => compareValues(value, actual[index], `${location}[${index}]`));
    return;
  } else if (expected !== null && actual !== null && typeof expected === 'object' && typeof actual === 'object') {
    const expectedEntries = Object.entries(expected);
    const actualEntries = Object.entries(actual);
    if (JSON.stringify(expectedEntries.map(([key]) => key)) !== JSON.stringify(actualEntries.map(([key]) => key))) {
      throw new Error(`${location}: columns differ`);
    }
    expectedEntries.forEach(([key, value], index) => compareValues(value, actualEntries[index]?.[1], `${location}.${key}`));
    return;
  } else if (expected === actual) return;
  throw new Error(`${location}: expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
}

function runBenchmark(): void {
  const { values } = parseArgs({ options: Object.fromEntries([
    'scale', 'work-dir', 'output-dir', 'fdb-duckdb', 'stock-duckdb', 'runtime', 'cluster-file',
    'postgres-dsn', 'threads', 'memory', 'trials', 'query-dir', 'backends',
  ].map(name => [name, { type: 'string' as const }])) });
  if (!['both', 'postgres'].includes(values.backends ?? 'both')) throw new Error('--backends must be both or postgres');
  const scale = Number(requireOption(values, 'scale'));
  if (scale !== 5 && scale !== 50) throw new Error('--scale must be 5 or 50');
  const trials = Number(values.trials ?? '3');
  const threads = Number(values.threads ?? '32');
  if (!Number.isInteger(trials) || trials < 2 || !Number.isInteger(threads) || threads < 1) {
    throw new Error('trials must be >= 2 and threads must be positive integers');
  }
  const work = resolve(requireOption(values, 'work-dir'));
  const output = resolve(requireOption(values, 'output-dir'));
  if (existsSync(work) || existsSync(output)) throw new Error('work and output directories must be new');
  mkdirSync(work, { recursive: true });
  mkdirSync(output, { recursive: true });
  const settings = `SET threads=${threads}; SET memory_limit=${quoteSql(String(values.memory ?? '48GiB'))}; SET enable_progress_bar=false;`;
  const source = join(work, 'source.duckdb');
  const stock = requireOption(values, 'stock-duckdb');
  const baseline: Backend = { name: 'postgres', binary: stock, prepare: settings, environment: { ...process.env, DUCKDB_AGENT_MODE: '0' } };
  const configuration = { scale, trials, threads, memory: values.memory ?? '48GiB', values, started: new Date().toISOString() };
  writeFileSync(join(output, 'configuration.json'), JSON.stringify(configuration, null, 2) + '\n');
  generateSource({ baseline, source, scale, output });
  const backends = createBackends({ values, work, settings, baseline });
  loadCatalogs({ backends, source, output });
  runQueries({ backends, baseline, source, output, trials, queryDirectory: values['query-dir'] });
  writeFileSync(join(output, 'complete.json'), JSON.stringify({ status: 'passed', scale, trials, queries: 22, backends: backends.map(backend => backend.name) }) + '\n');
}

interface SourceInput { baseline: Backend; source: string; scale: number; output: string }

function generateSource(input: SourceInput): void {
  const sql = `LOAD tpch; ATTACH ${quoteSql(input.source)} AS source; USE source; CALL dbgen(sf=${input.scale}); CHECKPOINT;`;
  writeFileSync(join(input.output, 'generate.sql'), sql + '\n');
  const result = executeSql(input.baseline, sql);
  writeFileSync(join(input.output, 'generate.json'), JSON.stringify(result) + '\n');
}

interface BackendInput {
  values: Record<string, string | boolean | undefined>;
  work: string;
  settings: string;
  baseline: Backend;
}

function createBackends(input: BackendInput): Backend[] {
  const { values, work, settings, baseline } = input;
  const runId = Date.now();
  const prefix = `aux-ducklake-benchmark/tpch/sf${values.scale}/${runId}/`;
  const common = `${settings} LOAD ducklake; SET ducklake_max_retry_count=10;`;
  const postgres: Backend = { ...baseline,
    prepare: `${common} LOAD postgres; ATTACH ${quoteSql('ducklake:postgres:' + requireOption(values, 'postgres-dsn'))} AS dl (DATA_PATH ${quoteSql(join(work, 'postgres-data'))}, METADATA_SCHEMA 'tpch_sf${values.scale}_${runId}', DATA_INLINING_ROW_LIMIT 0); USE dl;`,
  };
  if (values.backends === 'postgres') return [postgres];
  const fdb: Backend = {
    name: 'fdb', binary: requireOption(values, 'fdb-duckdb'),
    prepare: `${common} ATTACH ${quoteSql('ducklake:' + join(work, 'fdb.duckdb'))} AS dl (DATA_PATH ${quoteSql(join(work, 'fdb-data'))}, META_TYPE 'aux_catalog', DATA_INLINING_ROW_LIMIT 0); USE dl;`,
    environment: { ...baseline.environment, AUX_DUCKLAKE_CATALOG_BACKEND: 'fdb', AUX_DUCKLAKE_FDB_PREFIX: prefix,
      AUX_DUCKLAKE_RUNTIME_LIBRARY: requireOption(values, 'runtime'), AUX_DUCKLAKE_FDB_CLUSTER_FILE: requireOption(values, 'cluster-file'),
      AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY: prefix },
  };
  return [fdb, postgres];
}

interface CatalogInput { backends: Backend[]; source: string; output: string }
const tables = ['region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem'];

function loadCatalogs(input: CatalogInput): void {
  let expected: unknown;
  for (const backend of input.backends) {
    writeFileSync(join(input.output, `${backend.name}-attach.sql`), backend.prepare + '\n');
    if (backend.name === 'fdb') {
      const names = ['AUX_DUCKLAKE_CATALOG_BACKEND', 'AUX_DUCKLAKE_FDB_PREFIX', 'AUX_DUCKLAKE_RUNTIME_LIBRARY',
        'AUX_DUCKLAKE_FDB_CLUSTER_FILE', 'AUX_DUCKLAKE_RUNTIME_CATALOG_IDENTITY'];
      const environment = Object.fromEntries(names.map(name => [name, backend.environment[name]]));
      writeFileSync(join(input.output, 'fdb-environment.json'), JSON.stringify(environment, null, 2) + '\n');
    }
    const sql = `ATTACH ${quoteSql(input.source)} AS source (READ_ONLY);\n` + tables.map(table =>
      `CREATE TABLE dl.main.${table} AS SELECT * FROM source.main.${table};`).join('\n') + '\n' +
      `SELECT ${tables.map(table => `(SELECT count(*) FROM dl.main.${table}) AS ${table}`).join(', ')};`;
    writeFileSync(join(input.output, `${backend.name}-load.sql`), backend.prepare + '\n' + sql);
    const result = executeSql(backend, sql);
    writeFileSync(join(input.output, `${backend.name}-load.json`), JSON.stringify(result) + '\n');
    const counts: unknown = JSON.parse(result.output);
    if (expected === undefined) expected = counts;
    else compareValues(expected, counts, 'loaded table counts');
  }
}

interface QueryInput extends CatalogInput { baseline: Backend; trials: number; queryDirectory: string | undefined }

function loadQueries(input: QueryInput): string[] {
  const directory = input.queryDirectory;
  if (directory !== undefined) {
    return Array.from({ length: 22 }, (_, index) =>
      readFileSync(join(directory, `q${index + 1}.sql`), 'utf8').replace(/\n$/, ''));
  }
  const result = executeSql(input.baseline, 'LOAD tpch; SELECT query FROM tpch_queries() ORDER BY query_nr;');
  const rows: unknown = JSON.parse(result.output);
  if (!Array.isArray(rows) || rows.length !== 22) throw new Error('Expected all 22 TPC-H queries');
  return rows.map((row: unknown) => {
    if (row === null || typeof row !== 'object' || !('query' in row) || typeof row.query !== 'string') {
      throw new Error('Invalid TPC-H query');
    }
    return row.query;
  });
}

function runQueries(input: QueryInput): void {
  loadQueries(input).forEach((query, index) => {
    writeFileSync(join(input.output, `q${index + 1}.sql`), query + '\n');
    const reference = executeSql(input.baseline, `ATTACH ${quoteSql(input.source)} AS source (READ_ONLY); USE source; ${query}`);
    const expected: unknown = JSON.parse(reference.output);
    writeFileSync(join(input.output, `q${index + 1}-expected.json`), reference.output + '\n');
    runQueryTrials({ ...input, query, queryNumber: index + 1, expected });
  });
}

interface TrialInput extends QueryInput { query: string; queryNumber: number; expected: unknown }

function runQueryTrials(input: TrialInput): void {
  for (let trial = 0; trial <= input.trials; trial++) {
    const order = (trial + input.queryNumber) % 2 === 0 ? input.backends : [...input.backends].reverse();
    for (const backend of order) {
      const result = executeSql(backend, `.timer on\n${input.query}\n.timer off\n`);
      if (result.queryMs === null) throw new Error('Missing DuckDB query timing');
      const actual: unknown = JSON.parse(result.output);
      compareValues(input.expected, actual, `${backend.name} q${input.queryNumber} trial ${trial}`);
      writeFileSync(join(input.output, `${backend.name}-q${input.queryNumber}-trial${trial}.json`), result.output + '\n');
      const sample = { backend: backend.name, query: input.queryNumber, trial, warmup: trial === 0,
        elapsedMs: result.elapsedMs, queryMs: result.queryMs, sqlSha256: hashText(input.query), resultSha256: hashText(result.output), status: 'passed' };
      appendFileSync(join(input.output, 'samples.jsonl'), JSON.stringify(sample) + '\n');
      console.log(JSON.stringify(sample));
    }
  }
}

runBenchmark();
