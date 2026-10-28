import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { resolveArtifacts } from './software-version-resolution.ts';
import { applyGeneration, listVersions, loadContext, planGeneration } from './software-version-generation.ts';

async function run(): Promise<void> {
  const { positionals, values } = parseArgs({ allowPositionals: true, options: { root: { type: 'string' }, 'dry-run': { type: 'boolean', default: false } } });
  const [mode] = positionals;
  if (positionals.length !== 1 || !['list', 'check', 'sync', 'update'].includes(mode ?? '')) throw new Error('Usage: node scripts/software-versions.ts list|check|sync|update [--dry-run]');
  const root = resolve(values.root ?? join(dirname(fileURLToPath(import.meta.url)), '..'));
  const context = loadContext(root, mode === 'check' || mode === 'list');
  if (mode === 'list') {
    console.log(listVersions(context));
    return;
  }
  if (mode === 'update') context.lock = await resolveArtifacts(context.catalog, context.lock);
  const changes = planGeneration(root, context);
  const stale = applyGeneration(changes, mode === 'check' || values['dry-run']);
  for (const file of stale) console.log(`${mode === 'check' ? 'stale' : 'generated'}: ${file}`);
  if (mode === 'check' && stale.length) throw new Error('Software versions drifted; run just versions-sync');
  console.log(`${mode}: ${changes.length} consumers checked${values['dry-run'] ? ' (dry run)' : ''}`);
}

try {
  await run();
} catch (error: unknown) {
  console.error(error instanceof Error ? error.message : 'Software version operation failed');
  process.exitCode = 1;
}
