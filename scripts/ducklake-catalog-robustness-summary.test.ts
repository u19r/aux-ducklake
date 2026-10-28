import { strict as assert } from 'node:assert';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

function renderScenario(backends: string, fdb: unknown, postgres: unknown) {
  const result = spawnSync('jq', ['-cn', '--arg', 'name', 'fixture', '--arg', 'profile', 'varied',
    '--arg', 'execution_order', 'fdb-first', '--arg', 'backends', backends,
    '--argjson', 'fdb', JSON.stringify([fdb]), '--argjson', 'postgres', JSON.stringify([postgres]),
    '-f', resolve('scripts/ducklake-catalog-robustness-scenario.jq')], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

function renderSummary(scenario: unknown) {
  const result = spawnSync('jq', ['-s', '-f', resolve('scripts/ducklake-catalog-robustness-summary.jq')], {
    input: JSON.stringify(scenario), encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test('given a complete pair when summarizing then both measured totals and the ratio are retained', () => {
  const scenario = renderScenario('both', { elapsed_ms: 100 }, { elapsed_ms: 200 });
  const result = renderSummary(scenario);
  assert.equal(scenario.comparison.status, 'complete');
  assert.equal(result.summary.fdb_total_ms, 100);
  assert.equal(result.summary.postgres_total_ms, 200);
  assert.equal(result.summary.fdb_postgres_total_ratio, 0.5);
  assert.equal(result.summary.fdb_win_count, 1);
  assert.equal(result.methodology.backend_execution_order_alternates, true);
});

test('given a Postgres-only control when summarizing then skipped FDB has no invented time or ratio', () => {
  const scenario = renderScenario('postgres', null, { elapsed_ms: 200 });
  const result = renderSummary(scenario);
  assert.equal(scenario.comparison.status, 'complete');
  assert.equal(scenario.comparison.fdb_status, 'skipped');
  assert.equal(scenario.execution_order, 'postgres');
  assert.equal(result.summary.fdb_total_ms, null);
  assert.equal(result.summary.postgres_total_ms, 200);
  assert.equal(result.summary.fdb_postgres_total_ratio, null);
  assert.equal(result.summary.fdb_win_count, 0);
  assert.equal(result.methodology.backend_execution_order_alternates, false);
});

test('given a failed member of a required pair when summarizing then the scenario remains failed', () => {
  const scenario = renderScenario('both', { status: 'failed', error: 'fixture failure' }, { elapsed_ms: 200 });
  const result = renderSummary(scenario);
  assert.equal(scenario.comparison.status, 'failed');
  assert.equal(result.summary.completed_scenario_count, 0);
  assert.equal(result.summary.scenario_count, 1);
  assert.equal(result.summary.fdb_win_count, 0);
});


test('given small tables when validating churn then deletes and updates include inserted rows', () => {
  const script = readFileSync(resolve('scripts/ducklake_catalog_benchmark.sh'), 'utf8');
  const begin = script.indexOf('def expected_integrity(state):');
  const end = script.indexOf('\nstate = dict(mixed=False', begin);
  assert.ok(begin >= 0 && end > begin);
  const model = script.slice(begin, end);
  const program = `${model}\nchurn_mode = 'all'
for rows, tables in [('16', '24'), ('38', '1')]:
    print(expected_integrity(dict(mixed=True, deletes=True, rounds=1, writers=0)))
`;
  const result = spawnSync('python3', ['-'], { input: program, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, '643,599551,5995510\n42,6125,61251\n');
});
