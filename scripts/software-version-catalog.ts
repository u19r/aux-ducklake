import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

export type Download = { repository: string; tag: string; asset: string };
export type Tool = { version: string; purpose: string; downloads: Record<string, Download> };
export type Image = { repository: string; tag: string };
export type Action = { repository: string; version: string };
export type Catalog = { schema: number; tools: Record<string, Tool>; images: Record<string, Image>; actions: Record<string, Action>; cache_images: string[]; repositories: string[] };
export type ToolArtifact = { selection: string; revision: string; downloads: Record<string, { url: string; sha256: string }> };
export type ImageArtifact = { selection: string; digest: string; revision: string };
export type ActionArtifact = { selection: string; revision: string };
export type ArtifactLock = { schema: number; tools: Record<string, ToolArtifact>; images: Record<string, ImageArtifact>; actions: Record<string, ActionArtifact> };
export type Binding = { key: string; files: string[]; template: string } & ({ kind: 'text'; pattern: string; encoding: string } | { kind: 'json'; pointer: string });
export type Bindings = { schema: number; bindings: Binding[]; copies: { source: string; files: string[] }[] };

export function readJson(path: string): unknown {
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function parseObject(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return Object.fromEntries(Object.entries(value));
}

export function parseString(value: unknown, name: string): string {
  if (typeof value !== 'string' || !value.trim() || /[\r\n]/.test(value)) throw new Error(`${name} must be a nonempty single-line string`);
  return value;
}

export function parseStrings(value: unknown, name: string): string[] {
  if (!Array.isArray(value)) throw new Error(`${name} must be an array`);
  return value.map(item => parseString(item, name));
}

export function parseRecord<T>(value: unknown, name: string, parse: (value: unknown, key: string) => T): Record<string, T> {
  return Object.fromEntries(Object.entries(parseObject(value, name)).map(([key, item]) => [key, parse(item, key)]));
}

function parseDownload(value: unknown): Download {
  const entry = parseObject(value, 'download');
  return { repository: parseString(entry.repository, 'repository'), tag: parseString(entry.tag, 'tag'), asset: parseString(entry.asset, 'asset') };
}

function parseTool(value: unknown): Tool {
  const entry = parseObject(value, 'tool');
  return { version: parseString(entry.version, 'version'), purpose: parseString(entry.purpose, 'purpose'), downloads: parseRecord(entry.downloads, 'downloads', parseDownload) };
}

export function parseCatalog(value: unknown): Catalog {
  const entry = parseObject(value, 'catalog');
  if (entry.schema !== 1) throw new Error('Unsupported catalog schema');
  const tools = parseRecord(entry.tools, 'tools', parseTool);
  const images = parseRecord(entry.images, 'images', item => {
    const image = parseObject(item, 'image');
    return { repository: parseString(image.repository, 'repository'), tag: parseString(image.tag, 'tag') };
  });
  const actions = parseRecord(entry.actions, 'actions', item => {
    const action = parseObject(item, 'action');
    return { repository: parseString(action.repository, 'repository'), version: parseString(action.version, 'version') };
  });
  return { schema: 1, tools, images, actions, cache_images: parseStrings(entry.cache_images, 'cache_images'), repositories: parseStrings(entry.repositories, 'repositories') };
}

export function parseLock(value: unknown): ArtifactLock {
  const entry = parseObject(value, 'artifact lock');
  if (entry.schema !== 1) throw new Error('Unsupported artifact lock schema');
  const tools = parseRecord(entry.tools, 'tools', item => {
    const artifact = parseObject(item, 'tool artifact');
    const downloads = parseRecord(artifact.downloads, 'downloads', item => {
      const download = parseObject(item, 'download');
      return { url: parseString(download.url, 'url'), sha256: parseDigest(download.sha256, /^[a-f0-9]{64}$/, 'sha256') };
    });
    return { selection: parseString(artifact.selection, 'selection'), revision: typeof artifact.revision === 'string' ? artifact.revision : '', downloads };
  });
  const images = parseRecord(entry.images, 'images', item => {
    const image = parseObject(item, 'image artifact');
    return { selection: parseString(image.selection, 'selection'), digest: parseDigest(image.digest, /^sha256:[a-f0-9]{64}$/, 'image digest'), revision: typeof image.revision === 'string' ? image.revision : '' };
  });
  const actions = parseRecord(entry.actions, 'actions', item => {
    const action = parseObject(item, 'action artifact');
    return { selection: parseString(action.selection, 'selection'), revision: parseDigest(action.revision, /^[a-f0-9]{40}$/, 'action revision') };
  });
  return { schema: 1, tools, images, actions };
}

function parseDigest(value: unknown, pattern: RegExp, name: string): string {
  const text = parseString(value, name);
  if (!pattern.test(text)) throw new Error(`Invalid ${name}`);
  return text;
}

export function parseBindings(value: unknown): Bindings {
  const entry = parseObject(value, 'bindings');
  if (entry.schema !== 1 || !Array.isArray(entry.bindings)) throw new Error('Unsupported bindings schema');
  const bindings: Binding[] = entry.bindings.map(item => {
    const binding = parseObject(item, 'binding');
    const key = parseString(binding.key, 'key');
    const template = typeof binding.template === 'string' ? binding.template : `{{${key}}}`;
    const files = parseStrings(binding.files, 'files');
    if (binding.kind === 'json') return { kind: 'json', key, template, files, pointer: parseString(binding.pointer, 'pointer') };
    if (binding.kind !== 'text') throw new Error('Unknown binding kind');
    const pattern = parseString(binding.pattern, 'pattern');
    if (!pattern.includes('(?<value>')) throw new Error('Binding must capture a named value');
    new RegExp(pattern, 'gd');
    const encoding = parseString(binding.encoding, 'encoding');
    if (!['plain', 'regex', 'rust-wrapped'].includes(encoding)) throw new Error(`Unknown encoding ${encoding}`);
    return { kind: 'text', key, template, files, pattern, encoding };
  });
  const copies = entry.copies === undefined ? [] : parseObject({ copies: entry.copies }, 'copies').copies;
  if (!Array.isArray(copies)) throw new Error('copies must be an array');
  return { schema: 1, bindings, copies: copies.map(item => {
    const copy = parseObject(item, 'copy');
    return { source: parseString(copy.source, 'source'), files: parseStrings(copy.files, 'files') };
  }) };
}

export function snapshotDigest(repository: string, catalog: Catalog, lock: ArtifactLock, bindings: Bindings): string {
  return createHash('sha256').update(JSON.stringify({ repository, catalog, lock, bindings })).digest('hex');
}

export function renderSelection(template: string, version: string): string {
  return template.replaceAll('{version}', version).replaceAll('{version_underscored}', version.replaceAll('.', '_'));
}

export function selectImageTag(image: Image, catalog: Catalog): string {
  return image.tag.replace(/\{\{tools\.([a-z0-9_]+)\.version\}\}/g, (_match: string, key: string) => {
    const tool = catalog.tools[key];
    if (!tool) throw new Error(`Unknown image tag source ${key}`);
    return tool.version;
  });
}

function buildToolValues(catalog: Catalog, lock: ArtifactLock): Record<string, string> {
  const values: Record<string, string> = {};
  for (const [key, tool] of Object.entries(catalog.tools)) {
    values[`tools.${key}.version`] = tool.version;
    const unprefixed = tool.version.replace(/^v/, '');
    values[`tools.${key}.unprefixed`] = unprefixed;
    values[`tools.${key}.major_minor`] = unprefixed.split('.').slice(0, 2).join('.');
    values[`tools.${key}.major`] = unprefixed.split('.')[0] ?? unprefixed;
    const parts = unprefixed.split('.');
    values[`tools.${key}.host_id`] = `${parts[0]}${parts[1]?.padStart(2, '0') ?? ''}`;
    if (!Object.keys(tool.downloads).length) continue;
    const artifact = lock.tools[key];
    if (artifact?.selection !== tool.version) throw new Error(`Stale tool lock: ${key}; run just versions-update`);
    values[`lock.tools.${key}.revision`] = artifact.revision;
    for (const [platform, download] of Object.entries(tool.downloads)) {
      const resolved = artifact.downloads[platform];
      if (resolved?.url !== renderSelection(download.asset, tool.version).replaceAll('{revision}', artifact.revision).replace(/^/, `https://github.com/${download.repository}/releases/download/${renderSelection(download.tag, tool.version)}/`)) throw new Error(`Stale download lock: ${key}/${platform}`);
      values[`lock.tools.${key}.${platform}.sha256`] = resolved.sha256;
      values[`lock.tools.${key}.${platform}.url`] = resolved.url;
    }
  }
  return values;
}

export function buildValues(catalog: Catalog, lock: ArtifactLock): Record<string, string> {
  const exactKeys = (expected: string[], actual: string[], name: string): void => {
    if (expected.sort().join('\n') !== actual.sort().join('\n')) throw new Error(`Stale ${name} lock entries; run just versions-update`);
  };
  exactKeys(Object.keys(catalog.tools).filter(key => Object.keys(catalog.tools[key]!.downloads).length > 0), Object.keys(lock.tools), 'tool');
  exactKeys(Object.keys(catalog.images), Object.keys(lock.images), 'image');
  exactKeys(Object.keys(catalog.actions), Object.keys(lock.actions), 'action');
  for (const [key, artifact] of Object.entries(lock.tools)) exactKeys(Object.keys(catalog.tools[key]!.downloads), Object.keys(artifact.downloads), `${key} platform`);
  const values: Record<string, string> = {};
  Object.assign(values, buildToolValues(catalog, lock));
  for (const [key, image] of Object.entries(catalog.images)) {
    const selection = `${image.repository}:${selectImageTag(image, catalog)}`;
    const artifact = lock.images[key];
    if (artifact?.selection !== selection) throw new Error(`Stale image lock: ${key}; run just versions-update`);
    values[`images.${key}.tag`] = selectImageTag(image, catalog);
    values[`images.${key}.tagref`] = selection;
    values[`images.${key}.reference`] = `${selection}@${artifact.digest}`;
    values[`lock.images.${key}.digest`] = artifact.digest;
    values[`lock.images.${key}.revision`] = artifact.revision;
  }
  for (const [key, action] of Object.entries(catalog.actions)) {
    const artifact = lock.actions[key];
    if (artifact?.selection !== `${action.repository}@${action.version}`) throw new Error(`Stale action lock: ${key}; run just versions-update`);
    values[`actions.${key}.version`] = action.version;
    values[`actions.${key}.revision`] = artifact.revision;
  }
  for (const key of catalog.cache_images) if (!catalog.images[key]) throw new Error(`Unknown cache image ${key}`);
  return values;
}
