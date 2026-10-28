
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { parseObject, parseString, renderSelection, selectImageTag } from './software-version-catalog.ts';
import type { Action, ActionArtifact, ArtifactLock, Catalog, Image, ImageArtifact, Tool, ToolArtifact } from './software-version-catalog.ts';

async function fetchGithub(path: string): Promise<Record<string, unknown>> {
  const token = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
  const headers: Record<string, string> = { Accept: 'application/vnd.github+json', 'User-Agent': 'aux-software-versions' };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(`https://api.github.com/${path}`, { headers, signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`GitHub resolution failed: ${path} (${response.status})`);
  const value: unknown = await response.json();
  return parseObject(value, path);
}

function selectReleaseAsset(assets: Record<string, unknown>[], template: string): { asset: Record<string, unknown>; revision: string } {
  const parts = template.split('{revision}');
  const escaped = parts.map(part => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const pattern = new RegExp(`^${escaped.join('(?<revision>[a-zA-Z0-9]+)')}$`);
  const matches = assets.flatMap(asset => {
    const match = typeof asset.name === 'string' ? asset.name.match(pattern) : null;
    return match ? [{ asset, revision: match.groups?.revision ?? '' }] : [];
  });
  if (matches.length !== 1 || !matches[0]) throw new Error(`Expected one release asset matching ${template}`);
  return matches[0];
}

function resolveDownloadChecksum(asset: Record<string, unknown>, previous: { url: string; sha256: string } | undefined, url: string): string {
  // Historical releases can lack API digests. Preserve only the exact reviewed artifact pin.
  if (asset.digest == null && previous?.url === url && /^[a-f0-9]{64}$/.test(previous.sha256)) return previous.sha256;
  const digest = parseString(asset.digest, 'asset digest');
  if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`Missing SHA256 for ${url}`);
  return digest.slice(7);
}

async function resolveTool(tool: Tool, previous: ToolArtifact | undefined): Promise<ToolArtifact> {
  const downloads: ToolArtifact['downloads'] = {};
  let revision = '';
  const releases = new Map<string, Record<string, unknown>>();
  for (const [platform, download] of Object.entries(tool.downloads)) {
    const tag = renderSelection(download.tag, tool.version);
    const assetName = renderSelection(download.asset, tool.version);
    const releasePath = `repos/${download.repository}/releases/tags/${encodeURIComponent(tag)}`;
    const release = releases.get(releasePath) ?? await fetchGithub(releasePath);
    releases.set(releasePath, release);
    if (!Array.isArray(release.assets)) throw new Error(`Missing assets for ${download.repository}@${tag}`);
    const assets = release.assets.map(item => parseObject(item, 'release asset'));
    const selected = selectReleaseAsset(assets, assetName);
    const asset = selected.asset;
    if (revision && revision !== selected.revision) throw new Error('Release assets disagree on their build revision');
    revision = selected.revision;
    const url = `https://github.com/${download.repository}/releases/download/${tag}/${parseString(asset.name, 'asset name')}`;
    downloads[platform] = { url, sha256: resolveDownloadChecksum(asset, previous?.downloads[platform], url) };
  }
  return { selection: tool.version, revision, downloads };
}

function resolveImage(image: Image, catalog: Catalog): ImageArtifact {
  const selection = `${image.repository}:${selectImageTag(image, catalog)}`;
  const raw = execFileSync('skopeo', ['inspect', '--raw', `docker://${selection}`], { maxBuffer: 16 * 1024 * 1024 });
  const manifest: unknown = JSON.parse(raw.toString('utf8'));
  const annotations = parseObject(parseObject(manifest, 'image manifest').annotations ?? {}, 'image annotations');
  let revision = typeof annotations['org.opencontainers.image.revision'] === 'string' ? annotations['org.opencontainers.image.revision'] : '';
  if (!revision) {
    const configBytes = execFileSync('skopeo', ['inspect', '--config', '--override-os', 'linux', '--override-arch', 'amd64', `docker://${selection}`], { maxBuffer: 16 * 1024 * 1024 });
    const config: unknown = JSON.parse(configBytes.toString('utf8'));
    const metadata = parseObject(parseObject(config, 'image config').config ?? {}, 'image config metadata');
    const labels = parseObject(metadata.Labels ?? {}, 'image labels');
    const label = labels['org.opencontainers.image.revision'] ?? labels['vcs-ref'];
    revision = typeof label === 'string' ? label : '';
  }
  return { selection, digest: `sha256:${createHash('sha256').update(raw).digest('hex')}`, revision };
}

async function resolveAction(action: Action): Promise<ActionArtifact> {
  const selection = `${action.repository}@${action.version}`;
  if (/^[a-f0-9]{40}$/.test(action.version)) return { selection, revision: action.version };
  const ref = await fetchGithub(`repos/${action.repository}/git/ref/tags/${encodeURIComponent(action.version)}`);
  let object = parseObject(ref.object, 'action ref');
  for (let depth = 0; object.type === 'tag' && depth < 5; depth++) {
    const tag = await fetchGithub(`repos/${action.repository}/git/tags/${parseString(object.sha, 'tag SHA')}`);
    object = parseObject(tag.object, 'tag object');
  }
  if (object.type !== 'commit') throw new Error(`Action ${selection} does not resolve to a commit`);
  return { selection, revision: parseString(object.sha, 'action commit') };
}

export async function resolveArtifacts(catalog: Catalog, previous?: ArtifactLock): Promise<ArtifactLock> {
  const tools: ArtifactLock['tools'] = {};
  const images: ArtifactLock['images'] = {};
  const actions: ArtifactLock['actions'] = {};
  for (const [key, tool] of Object.entries(catalog.tools)) if (Object.keys(tool.downloads).length) tools[key] = await resolveTool(tool, previous?.tools[key]?.selection === tool.version ? previous.tools[key] : undefined);
  for (const [key, image] of Object.entries(catalog.images)) images[key] = resolveImage(image, catalog);
  for (const [key, action] of Object.entries(catalog.actions)) actions[key] = await resolveAction(action);
  return { schema: 1, tools, images, actions };
}
