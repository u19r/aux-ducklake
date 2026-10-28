import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';
import type { Bindings } from './software-version-catalog.ts';

const SOURCE_DIRECTORIES = ['.github', '.cargo', 'docker', 'scripts', 'crates', 'terraform'];
const DECLARATIONS = [
  /variable "[A-Z_]*(?:VERSION|RELEASE_TAG|RELEASE_BUILD)"\s*\{\s*default\s*=\s*"(?<value>v?\d+[^"\n]*)"/gd,
  /(?:_VERSION\s*[=:]\s*["']?|--default-toolchain |toolchain:\s*["']?)(?<value>v?\d+\.\d+(?:\.\d+)?)/gd,
  /const [A-Z][A-Z_0-9]*VERSION: &str = "(?<value>v?\d+\.\d+\.\d+)"/gd,
  /uses:\s*[\w.-]+\/[\w./-]+@(?<value>[\w.-]+)/gd,
];

function listSources(directory: string): string[] {
  if (!existsSync(directory)) return [];
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    if (['node_modules', 'target', '.git', 'generated', 'testdata', 'tests', '.terraform'].includes(entry.name)) return [];
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return listSources(path);
    if (!entry.isFile() || /(?:_tests\.rs|\.test\.|\.spec\.|-test\.sh)$/.test(entry.name)) return [];
    return /(?:\.(?:rs|sh|ya?ml|toml|j2|hcl|tf|tftpl)|Dockerfile)$/.test(entry.name) ? [path] : [];
  });
}

function findSpans(source: string, pattern: RegExp): [number, number][] {
  return [...source.matchAll(pattern)].flatMap(match => {
    const span = match.indices?.groups?.value;
    return span ? [span] : [];
  });
}

export function checkDeclarations(root: string, repository: string, bindings: Bindings, generated: Map<string, string>): string[] {
  const patterns = [...DECLARATIONS, ...bindings.bindings.flatMap(binding => binding.kind === 'text' ? [new RegExp(binding.pattern, 'gd')] : [])];
  const files = SOURCE_DIRECTORIES.flatMap(directory => listSources(join(root, directory))).filter(path => !path.endsWith('.rs'));
  for (const name of ['docker-bake.hcl', 'justfile', 'rust-toolchain.toml', 'Cargo.toml']) if (existsSync(join(root, name))) files.push(join(root, name));
  const errors: string[] = [];
  for (const path of files) {
    const file = `${repository}/${relative(root, path).split('\\').join('/')}`;
    const source = generated.get(path) ?? readFileSync(path, 'utf8');
    const owners = bindings.bindings.filter(binding => binding.kind === 'text' && binding.files.includes(file));
    const spans = owners.flatMap(binding => binding.kind === 'text' ? findSpans(source, new RegExp(binding.pattern, 'gd')) : []);
    for (const pattern of patterns) for (const [start, end] of findSpans(source, pattern)) {
      if (spans.some(([left, right]) => left <= start && right >= end)) continue;
      const line = source.slice(0, start).split('\n').length;
      errors.push(`${file}:${line}: undeclared software version ${source.slice(start, end)}`);
    }
  }
  return [...new Set(errors)];
}
