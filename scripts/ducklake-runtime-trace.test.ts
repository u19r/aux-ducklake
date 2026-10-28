import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { intervalUnion, summarizeTrace } from './ducklake-runtime-trace.ts';

const header = 'seq,scope,operation,status,items,bytes,elapsed_micros,pid,thread_id,start_micros,span_id,parent_span_id,request_span_id,read_context_id,catalog_id,cpu_micros,kind\n';

test('given nested overlapping intervals when summarized then elapsed work is not double counted', () => {
  const csv = header + [
    '1,"scope,quoted","wait.fdb.get",ok,0,0,50,1,t1,10,2,1,1,42,1,3,span',
    '2,s,kv.get,ok,1,9,50,1,t1,10,3,2,1,42,1,,event',
    '3,s,parallel.read,ok,1,8,40,1,t1,30,4,1,1,42,1,,event',
    '4,s,fdb.transaction.created,ok,1,0,0,1,t1,5,5,1,1,42,1,,event',
    '5,s,CommitAttempt,ok,0,0,90,1,t1,5,6,1,1,42,1,,event',
    '6,s,ffi.request,ok,0,0,100,1,t1,0,1,0,1,42,1,20,span',
  ].join('\n') + '\n';
  const summary = summarizeTrace(csv);
  assert.equal(summary.threadBusyMicros, 100);
  assert.equal(summary.synchronousWaitMicros, 50);
  assert.equal(summary.operations.find(cost => cost.operation === 'ffi.request')?.exclusiveWallMicros, 10);
  assert.equal(summary.operations.find(cost => cost.operation === 'wait.fdb.get')?.exclusiveWallMicros, 0);
  assert.equal(summary.requests[0]?.work.transactions, 1);
  assert.equal(summary.requests[0]?.operation, 'CommitAttempt');
  assert.equal(intervalUnion([{ start: 0, end: 10 }, { start: 2, end: 4 }, { start: 15, end: 20 }]), 15);
});

test('given separate processes when span ids repeat then attribution stays process local', () => {
  const csv = header + '1,s,ffi.request,ok,0,0,100,1,t,0,1,0,1,42,1,20,span\n'
    + '1,s,ffi.request,ok,0,0,100,2,t,0,1,0,1,42,1,20,span\n';
  const summary = summarizeTrace(csv);
  assert.equal(summary.threadBusyMicros, 200);
  assert.equal(summary.requests.length, 2);
});

test('given linked workers when their intervals overlap then work reaches the request without summing parallel wall time', () => {
  const csv = header + [
    '1,s,fdb.transaction.created,ok,1,0,0,1,t2,10,4,2,1,42,1,,event',
    '2,s,method.Worker,ok,0,0,60,1,t2,10,2,1,1,42,1,20,task',
    '3,s,method.Worker,ok,0,0,60,1,t3,20,3,1,1,42,1,20,task',
    '4,s,ffi.request,ok,0,0,100,1,t1,0,1,0,1,42,1,10,span',
  ].join('\n') + '\n';
  const result = summarizeTrace(csv);
  assert.equal(result.requests[0]?.work.transactions, 1);
  assert.equal(result.operations.find(cost => cost.operation === 'ffi.request')?.exclusiveWallMicros, 30);
  assert.equal(result.threadBusyMicros, 220);
  assert.throws(() => summarizeTrace(csv.replaceAll(',task', ',span')), /crossed threads/);
});

test('given an incomplete or inconsistent trace when analyzed then it fails rather than publishing attribution', () => {
  assert.throws(() => summarizeTrace('seq,operation\n1,get\n'), /columns/);
  assert.throws(() => summarizeTrace(header + '1,s,get,ok,0,0,1,1,t,0,2,1,1,1,1,,event\n'), /Missing parent/);
  assert.throws(() => summarizeTrace(header + '1,s,get,ok,0,0,1,1,t,0,1,0,1,1,1,,event\n'.repeat(2)), /Duplicate/);
  assert.throws(() => summarizeTrace(header + '1,s,get,ok,0,0,1,1,t,0,1,0,1,1,1,,"unfinished'), /Incomplete/);
});
