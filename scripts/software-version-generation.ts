import { checkDeclarations } from './software-version-discovery.ts';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { buildValues, snapshotDigest, parseBindings, parseCatalog, parseLock, parseObject, parseString, readJson } from './software-version-catalog.ts';
import type { ArtifactLock, Binding, Bindings, Catalog } from './software-version-catalog.ts';

export type Context = { repository: string; catalog: Catalog; lock: ArtifactLock; bindings: Bindings };
export type Change = { path: string; content: string };

export function loadContext(root: string, verifyDigest = true): Context {
  const snapshot = parseObject(readJson(join(root, 'software-versions.snapshot.json')), 'snapshot');
  const catalog = parseCatalog(snapshot.catalog);
  const lock = parseLock(snapshot.lock);
  const repository = parseString(snapshot.repository, 'repository');
  const bindings = parseBindings(snapshot.bindings);
  if (verifyDigest && snapshot.payload_digest !== snapshotDigest(repository, catalog, lock, bindings)) throw new Error('Snapshot changed; run just versions-sync, or just versions-update after changing artifact selections');
  return { repository, catalog, lock, bindings };
}

function replaceJsonValue(value: unknown, parts: string[], replacement: string): unknown {
  const [part, ...rest] = parts;
  if (part === undefined) {
    if (typeof value !== 'string') throw new Error('A version JSON pointer must select an existing string');
    return replacement;
  }
  if (Array.isArray(value)) {
    const index = Number(part);
    if (!Number.isInteger(index) || index < 0 || index >= value.length) throw new Error(`Missing JSON array index ${part}`);
    return value.map((item, position) => position === index ? replaceJsonValue(item, rest, replacement) : item);
  }
  const object = parseObject(value, 'JSON pointer parent');
  if (!Object.hasOwn(object, part)) throw new Error(`Missing JSON field ${part}`);
  return Object.fromEntries(Object.entries(object).map(([key, item]) => [key, key === part ? replaceJsonValue(item, rest, replacement) : item]));
}

function replaceBinding(source: string, binding: Binding, value: string, file: string): string {
  if (binding.kind === 'json') {
    if (!binding.pointer.startsWith('/')) throw new Error(`Invalid JSON pointer ${binding.pointer}`);
    const parts = binding.pointer.slice(1).split('/').map(part => part.replaceAll('~1', '/').replaceAll('~0', '~'));
    const parsed: unknown = JSON.parse(source);
    return `${JSON.stringify(replaceJsonValue(parsed, parts, value), null, 2)}\n`;
  }
  const matches = [...source.matchAll(new RegExp(binding.pattern, 'gd'))];
  if (!matches.length) throw new Error(`Binding ${binding.key} no longer matches ${file}`);
  let output = source;
  for (const match of matches.reverse()) {
    const span = match.indices?.groups?.value;
    if (!span) throw new Error(`Missing value capture in ${binding.key}`);
    const current = source.slice(span[0], span[1]);
    let replacement = binding.encoding === 'regex' ? value.replaceAll('.', '\\.') : value;
    const indent = current.match(/\\\n([ \t]+)/)?.[1];
    if (indent && binding.encoding === 'rust-wrapped') replacement = replacement.replace('@sha256:', `@sha256:\\\n${indent}`);
    output = output.slice(0, span[0]) + replacement + output.slice(span[1]);
  }
  return output;
}

function resolveConsumer(root: string, context: Context, file: string): string {
  const [repository, ...parts] = file.split('/');
  if (repository !== context.repository || !parts.length || parts.some(part => !part || part === '..' || part === '.')) throw new Error(`Invalid local consumer path ${file}`);
  return join(root, ...parts);
}

export function renderSnapshot(context: Context): string {
  const { repository, catalog, lock, bindings } = context;
  const snapshot = { repository, payload_digest: snapshotDigest(repository, catalog, lock, bindings), catalog, lock, bindings };
  return `${JSON.stringify(snapshot, null, 2)}\n`;
}

const VERSION_NOTICE = 'Versions: software-versions.snapshot.json; details in software-version-consumers.md.';

function annotateConsumer(file: string, source: string): string {
  if (file.endsWith('.json') || file.includes('/generated/')) return source;
  const comment = file.endsWith('.json.j2') ? `{# ${VERSION_NOTICE} #}` : `${file.endsWith('.rs') ? '//' : '#'} ${VERSION_NOTICE}`;
  source = source.replace(/^# Versions: software-versions\.json; details in software-version-consumers\.md\.\n/m, '');
  if (source.includes(comment)) return source;
  // Interpreter and Docker parser directives must remain at the start of the file.
  const prefix = source.match(/^(?:#![^\n]*\n|#\s*(?:syntax|check|escape)=[^\n]*\n)*/)?.[0] ?? '';
  return `${prefix}${comment}\n${source.slice(prefix.length)}`;
}

function renderConsumerNotes(repository: string, bindings: Bindings): string {
  const rows = new Map<string, Set<string>>();
  for (const binding of bindings.bindings) for (const file of binding.files) {
    if (!file.startsWith(`${repository}/`)) continue;
    const local = file.slice(repository.length + 1);
    const fields = rows.get(local) ?? new Set<string>();
    fields.add(`${binding.key}${binding.kind === 'json' ? ` (JSON pointer: ${binding.pointer})` : ''}`);
    rows.set(local, fields);
  }
  const lines = [
    '# Software version consumers', '',
    'Generated by the software version synchronizer. Do not edit this inventory.', '',
    'Edit version selections in `software-versions.snapshot.json`, then run `just versions-update`.',
    'This resolves artifact checksums, container digests, and action revisions, then updates consumers.',
    'It requires Node.js and skopeo, plus network access; GH_TOKEN is optional for GitHub API limits.',
    'Run `just versions-sync` to propagate existing pins offline, or `just versions-check` to detect drift.',
    'No sibling repositories or private tools are required.', '',
    'Comment-capable consumer files carry the same ownership notice. JSON does not permit comments;',
    'its tracked fields are identified by JSON pointer below. Native library dependency selections,',
    'schema/protocol numbers, and deliberately fixed test fixtures are outside this catalog.', '',
    '| Consumer | Catalog fields |', '| --- | --- |',
  ];
  for (const [file, fields] of [...rows].sort(([left], [right]) => left.localeCompare(right))) lines.push(`| [${file}](${file}) | ${[...fields].sort().map(field => `\`${field}\``).join('<br>')} |`);
  return `${lines.join('\n')}\n`;
}

export function planGeneration(root: string, context: Context): Change[] {
  const values = buildValues(context.catalog, context.lock);
  const contents = new Map<string, string>();
  for (const binding of context.bindings.bindings) {
    const value = binding.template.replace(/\{\{([^}]+)\}\}/g, (_match: string, key: string) => {
      const selected = values[key];
      if (selected === undefined) throw new Error(`Unknown catalog binding ${key}`);
      return selected;
    });
    for (const file of binding.files) {
      const path = resolveConsumer(root, context, file);
      const source = contents.get(path) ?? readFileSync(path, 'utf8');
      contents.set(path, replaceBinding(source, binding, value, file));
    }
  }
  const errors = checkDeclarations(root, context.repository, context.bindings, contents);
  if (errors.length) throw new Error(errors.join('\n'));
  for (const [path, source] of contents) contents.set(path, annotateConsumer(path, source));
  contents.set(join(root, 'software-version-consumers.md'), renderConsumerNotes(context.repository, context.bindings));
  contents.set(join(root, 'software-versions.snapshot.json'), renderSnapshot(context));
  return [...contents].map(([path, content]) => ({ path, content }));
}

export function applyGeneration(changes: Change[], check: boolean): string[] {
  const stale = changes.filter(change => !existsSync(change.path) || readFileSync(change.path, 'utf8') !== change.content);
  if (check) return stale.map(change => change.path);
  for (const change of stale) {
    mkdirSync(dirname(change.path), { recursive: true });
    writeFileSync(change.path, change.content);
  }
  return stale.map(change => change.path);
}

export function listVersions(context: Context): string {
  const values = buildValues(context.catalog, context.lock);
  const rows = Object.entries(values).filter(([key, value]) => key.endsWith('.version') || key.endsWith('.reference') || (key.endsWith('.revision') && value));
  return rows.map(([key, value]) => `${key}\t${value}`).join('\n');
}
