import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

type Interval = { start: number; end: number };
type TraceRecord = Interval & {
  id: string; parent: string; request: string; context: string; thread: string;
  operation: string; kind: string; status: string; cpu: number | null; items: number; bytes: number;
};
type OperationCost = {
  operation: string; count: number; errors: number; wallMicros: number; exclusiveWallMicros: number;
  threadCpuMicros: number; cpuSamples: number; items: number; bytes: number;
};

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  const token = /(?:"((?:[^"]|"")*)"|([^",\n]*))(,|\n|$)/y;
  let cursor = 0, row: string[] = [], delimiter = '';
  while (cursor < text.length) {
    token.lastIndex = cursor;
    const match = token.exec(text);
    if (!match || !match[0].length) throw new Error('Incomplete or invalid CSV trace field');
    cursor = token.lastIndex;
    row.push(match[1] === undefined ? match[2] ?? '' : match[1].replaceAll('""', '"'));
    delimiter = match[3] ?? '';
    if (delimiter !== ',') { rows.push(row); row = []; }
  }
  if (delimiter === ',') row.push('');
  if (row.length) rows.push(row);
  return rows;
}

function parseMicros(value: string): number {
  if (!/^\d+$/.test(value)) throw new Error(`Invalid trace number: ${value}`);
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error(`Unsafe trace number: ${value}`);
  return number;
}

export function parseTrace(text: string): TraceRecord[] {
  const [header, ...rows] = parseCsv(text);
  if (!header?.includes('parent_span_id')) throw new Error('Correlated trace columns are required');
  return rows.filter(row => row.length > 1).map(row => {
    if (row.length !== header.length) throw new Error('Trace row has wrong column count');
    const fields = new Map(header.map((name, index) => [name, row[index] ?? '']));
    function field(name: string): string {
      const value = fields.get(name);
      if (value === undefined) throw new Error(`Missing trace column: ${name}`);
      return value;
    }
    const pid = field('pid'), span = field('span_id');
    const start = parseMicros(field('start_micros')), elapsed = parseMicros(field('elapsed_micros'));
    return { id: `${pid}:${span}`, parent: `${pid}:${field('parent_span_id')}`,
      request: `${pid}:${field('request_span_id')}`, context: `${pid}:${field('read_context_id')}`,
      thread: `${pid}:${field('thread_id')}`, start, end: start + elapsed,
      operation: field('operation'), kind: field('kind'), status: field('status'),
      cpu: field('cpu_micros') === '' ? null : parseMicros(field('cpu_micros')),
      items: parseMicros(field('items')), bytes: parseMicros(field('bytes')) };
  });
}

export function intervalUnion(intervals: Interval[]): number {
  let total = 0, end = -1;
  for (const interval of [...intervals].sort((left, right) => left.start - right.start)) {
    total += Math.max(0, interval.end - Math.max(end, interval.start));
    end = Math.max(end, interval.end);
  }
  return total;
}

function buildChildren(records: TraceRecord[]): Map<string, TraceRecord[]> {
  const byId = new Map<string, TraceRecord>();
  for (const record of records) {
    if (byId.has(record.id)) throw new Error(`Duplicate trace span: ${record.id}`);
    byId.set(record.id, record);
  }
  const children = new Map<string, TraceRecord[]>();
  for (const record of records) {
    if (record.parent.endsWith(':0')) continue;
    const parent = byId.get(record.parent);
    if (!parent) throw new Error(`Missing parent: ${record.parent}`);
    if (parent.thread !== record.thread && record.kind !== 'task') throw new Error('Synchronous trace parent crossed threads');
    if (record.start < parent.start || record.end > parent.end) throw new Error(`Child exceeds parent: ${record.id}`);
    const values = children.get(record.parent) ?? [];
    values.push(record); children.set(record.parent, values);
  }
  return children;
}

function summarizeOperations(records: TraceRecord[], children: Map<string, TraceRecord[]>): OperationCost[] {
  const operations = new Map<string, OperationCost>();
  for (const record of records) {
    const cost = operations.get(record.operation) ?? { operation: record.operation, count: 0, errors: 0,
      wallMicros: 0, exclusiveWallMicros: 0, threadCpuMicros: 0, cpuSamples: 0, items: 0, bytes: 0 };
    cost.count++; cost.errors += Number(record.status !== 'ok');
    cost.wallMicros += record.end - record.start;
    cost.exclusiveWallMicros += record.end - record.start - intervalUnion(children.get(record.id) ?? []);
    cost.threadCpuMicros += record.cpu ?? 0; cost.cpuSamples += Number(record.cpu !== null);
    cost.items += record.items; cost.bytes += record.bytes;
    operations.set(record.operation, cost);
  }
  return [...operations.values()].sort((left, right) => right.wallMicros - left.wallMicros);
}

type RequestWork = {
  transactions: number; sampledAttempts: number; nativePointReads: number; nativeRangeBatches: number;
  nativeFetchedItems: number; nativeReadBytes: number; nativeWriteBytes: number;
  logicalRangePages: number; rangeRows: number; rangeBytes: number; retries: number; invalidations: number;
};

type RequestCost = { id: string; context: string; operation: string; wallMicros: number; work: RequestWork };
const workFields = new Map<string, { events?: keyof RequestWork; items?: keyof RequestWork; bytes?: keyof RequestWork }>([
  ['fdb.transaction.created', { items: 'transactions' }],
  ['fdb.usage.sampled_attempt', { items: 'sampledAttempts' }],
  ['fdb.usage.get', { items: 'nativePointReads' }],
  ['fdb.usage.range_batch', { items: 'nativeRangeBatches' }],
  ['fdb.usage.fetched', { items: 'nativeFetchedItems', bytes: 'nativeReadBytes' }],
  ['fdb.usage.set', { bytes: 'nativeWriteBytes' }],
  ['fdb.range.logical_page', { events: 'logicalRangePages', items: 'rangeRows', bytes: 'rangeBytes' }],
  ['fdb.mutation.retry', { items: 'retries' }], ['fdb.read.retry', { items: 'retries' }],
  ['fdb.range.retry', { items: 'retries' }], ['cache.catalog.invalidate', { items: 'invalidations' }],
]);

function addRequestWork(work: RequestWork, record: TraceRecord): void {
  const fields = workFields.get(record.operation);
  if (fields?.events) work[fields.events]++;
  if (fields?.items) work[fields.items] += record.items;
  if (fields?.bytes) work[fields.bytes] += record.bytes;
}

function summarizeRequests(records: TraceRecord[]): RequestCost[] {
  const requests = new Map<string, RequestCost>();
  for (const record of records.filter(record => record.operation === 'ffi.request')) {
    requests.set(record.id, { id: record.id, context: record.context, operation: '', wallMicros: record.end - record.start,
      work: { transactions: 0, sampledAttempts: 0, nativePointReads: 0, nativeRangeBatches: 0,
        nativeFetchedItems: 0, nativeReadBytes: 0, nativeWriteBytes: 0,
        logicalRangePages: 0, rangeRows: 0, rangeBytes: 0, retries: 0, invalidations: 0 } });
  }
  for (const record of records) {
    const request = requests.get(record.request);
    if (!request || record.operation === 'ffi.request') continue;
    if (/^[A-Z][A-Za-z]+$/.test(record.operation)) request.operation = record.operation;
    addRequestWork(request.work, record);
  }
  return [...requests.values()];
}

export function summarizeTrace(text: string) {
  const records = parseTrace(text), children = buildChildren(records);
  const threads = Map.groupBy(records, record => record.thread);
  let threadBusyMicros = 0, synchronousWaitMicros = 0;
  for (const local of threads.values()) {
    threadBusyMicros += intervalUnion(local);
    synchronousWaitMicros += intervalUnion(local.filter(record => record.kind === 'span' && record.operation.startsWith('wait.')));
  }
  return { records: records.length, threadBusyMicros, synchronousWaitMicros,
    unattributedFdbEvents: records.filter(record => record.operation.startsWith('fdb.') && record.request.endsWith(':0')).length,
    operations: summarizeOperations(records, children), requests: summarizeRequests(records),
    limits: ['Operation wall and CPU totals are inclusive; do not add nested operations.',
      'Busy and wait intervals are unioned per thread; their sum across threads is not process wall time.',
      'CPU measures the calling thread, excluding the FDB network thread.',
      'Native usage covers sampled attempts; uninstrumented transaction paths are not counted as zero.',
      'Point reads and range batches count resolved client operations, not physical network packets.',
      'Logical range pages can contain several native FDB batches.',
      'Read-context identifiers group transactions, which can contain several SQL statements.'] };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const path = process.argv[2];
  if (!path) throw new Error('Usage: node scripts/ducklake-runtime-trace.ts TRACE.csv');
  console.log(JSON.stringify(summarizeTrace(readFileSync(path, 'utf8')), null, 2));
}
