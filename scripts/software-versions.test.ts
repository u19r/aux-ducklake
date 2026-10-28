import { strict as assert } from 'node:assert';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const script = join(root, 'scripts/software-versions.ts');

function runVersion(root: string, ...args: string[]) {
  return spawnSync(process.execPath, [script, ...args, '--root', root], { encoding: 'utf8' });
}

function withCheckout(run: (directory: string) => void): void {
  const directory = mkdtempSync(join(tmpdir(), 'ducklake-versions-'));
  try {
    const snapshot = JSON.parse(readFileSync(join(root, 'software-versions.snapshot.json'), 'utf8'));
    const files = new Set<string>(snapshot.bindings.bindings.flatMap((binding: { files: string[] }) => binding.files));
    for (const file of files) {
      const local = file.slice('aux-ducklake/'.length);
      cpSync(join(root, local), join(directory, local), { recursive: true });
    }
    for (const file of ['software-versions.snapshot.json', 'software-version-consumers.md']) cpSync(join(root, file), join(directory, file));
    run(directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test('given a standalone checkout when listing and checking then no sibling is needed', () => {
  withCheckout(directory => {
    const listed = runVersion(directory, 'list');
    assert.equal(listed.status, 0, listed.stderr);
    assert.match(listed.stdout, /tools.rust.version/);
    const checked = runVersion(directory, 'check');
    assert.equal(checked.status, 0, checked.stderr);
  });
});

test('given drift when checking then sync repairs it and a second sync is stable', () => {
  withCheckout(directory => {
    const path = join(directory, 'rust-toolchain.toml');
    const snapshot = JSON.parse(readFileSync(join(directory, 'software-versions.snapshot.json'), 'utf8'));
    writeFileSync(path, readFileSync(path, 'utf8').replace(snapshot.catalog.tools.rust.version, '0.0.0'));
    assert.equal(runVersion(directory, 'check').status, 1);
    const dry = runVersion(directory, 'sync', '--dry-run');
    assert.equal(dry.status, 0, dry.stderr);
    assert.match(readFileSync(path, 'utf8'), /0\.0\.0/);
    assert.equal(runVersion(directory, 'sync').status, 0);
    assert.equal(runVersion(directory, 'check').status, 0);
    assert.doesNotMatch(runVersion(directory, 'sync').stdout, /generated:/);
  });
});

test('given a changed catalog when syncing then every Rust consumer follows the selection', () => {
  withCheckout(directory => {
    const path = join(directory, 'software-versions.snapshot.json');
    const snapshot = JSON.parse(readFileSync(path, 'utf8'));
    snapshot.catalog.tools.rust.version = '9.87.6';
    writeFileSync(path, JSON.stringify(snapshot));
    assert.equal(runVersion(directory, 'check').status, 1);
    const synced = runVersion(directory, 'sync');
    assert.equal(synced.status, 0, synced.stderr);
    assert.match(readFileSync(join(directory, 'Cargo.toml'), 'utf8'), /9\.87\.6/);
    assert.match(readFileSync(join(directory, 'rust-toolchain.toml'), 'utf8'), /9\.87\.6/);
    assert.equal(runVersion(directory, 'check').status, 0);
  });
});

test('given a stale artifact lock when syncing then actionable local guidance is printed', () => {
  withCheckout(directory => {
    const path = join(directory, 'software-versions.snapshot.json');
    const snapshot = JSON.parse(readFileSync(path, 'utf8'));
    snapshot.catalog.tools.mold.version = '0.0.0';
    writeFileSync(path, JSON.stringify(snapshot));
    const synced = runVersion(directory, 'sync');
    assert.equal(synced.status, 1);
    assert.match(synced.stderr, /just versions-update/);
    assert.doesNotMatch(synced.stderr, /sibling|source workspace/);
  });
});

test('given local pins when updating then resolved artifacts propagate without network in the fixture', () => {
  withCheckout(directory => {
    const path = join(directory, 'software-versions.snapshot.json');
    const snapshot = JSON.parse(readFileSync(path, 'utf8'));
    snapshot.catalog = { ...snapshot.catalog, tools: { rust: snapshot.catalog.tools.rust }, images: {}, actions: {} };
    snapshot.bindings.bindings = snapshot.bindings.bindings.filter((binding: { key: string }) => binding.key === 'tools.rust.version');
    snapshot.lock = { schema: 1, tools: {}, images: {}, actions: {} };
    snapshot.catalog.tools.rust.version = '9.87.6';
    writeFileSync(path, JSON.stringify(snapshot));
    // Exclude unrelated sources from declaration discovery in this reduced fixture.
    for (const file of ['scripts', 'docker', '.github', '.cargo']) rmSync(join(directory, file), { recursive: true, force: true });
    const updated = runVersion(directory, 'update');
    assert.equal(updated.status, 0, updated.stderr);
    assert.match(readFileSync(join(directory, 'rust-toolchain.toml'), 'utf8'), /9\.87\.6/);
    assert.equal(runVersion(directory, 'check').status, 0);
  });
});

test('given release metadata when resolving then checksums and action commits are locked', async () => {
  const { resolveArtifacts } = await import('./software-version-resolution.ts');
  const { buildValues, parseCatalog, parseLock } = await import('./software-version-catalog.ts');
  const snapshot = JSON.parse(readFileSync(join(root, 'software-versions.snapshot.json'), 'utf8'));
  const catalog = parseCatalog({ ...snapshot.catalog, images: {}, actions: { checkout: { repository: 'actions/checkout', version: 'v99' } } });
  const originalFetch = globalThis.fetch;
  const checksum = 'a'.repeat(64);
  const revision = 'b'.repeat(40);
  globalThis.fetch = async input => {
    const url = String(input);
    if (url.includes('/git/ref/tags/')) return Response.json({ object: { type: 'commit', sha: revision } });
    const tool = Object.values(catalog.tools).find(tool => Object.values(tool.downloads).some(download => url.includes(`/repos/${download.repository}/`)));
    assert.ok(tool, url);
    const assets = Object.values(tool.downloads).map(download => ({ name: download.asset.replaceAll('{version}', tool.version).replaceAll('{version_underscored}', tool.version.replaceAll('.', '_')), digest: `sha256:${checksum}` }));
    return Response.json({ assets });
  };
  try {
    const lock = await resolveArtifacts(catalog, parseLock(snapshot.lock));
    const values = buildValues(catalog, lock);
    assert.equal(values['lock.tools.mold.aarch64.sha256'], checksum);
    assert.equal(values['lock.tools.curl.source.sha256'], checksum);
    assert.equal(values['actions.checkout.revision'], revision);
    globalThis.fetch = async () => new Response('', { status: 404 });
    await assert.rejects(resolveArtifacts(catalog), /GitHub resolution failed/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('given an escaping consumer path when syncing then no outside file is written', () => {
  withCheckout(directory => {
    const path = join(directory, 'software-versions.snapshot.json');
    const snapshot = JSON.parse(readFileSync(path, 'utf8'));
    snapshot.bindings.bindings[0].files = ['aux-ducklake/../outside.toml'];
    writeFileSync(path, JSON.stringify(snapshot));
    const synced = runVersion(directory, 'sync');
    assert.equal(synced.status, 1);
    assert.match(synced.stderr, /Invalid local consumer path/);
  });
});
