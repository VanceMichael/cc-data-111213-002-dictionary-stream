import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { chunkedSource } from '../src/demo.js';
import StreamIntake, { RecoveryLedger, IntakeError } from '../src/stream.js';

const bytes = await readFile('fixtures/session.arrow');
const plan = JSON.parse(await readFile('fixtures/stream-plan.json', 'utf8'));

test('恢复账本落盘后可重新载入，确认位置连续', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'ledger-'));
  try {
    const path = join(folder, 'ledger.json');
    const ledger = new RecoveryLedger();
    const intake = new StreamIntake({
      ledger,
      maxInflightBatches: 8,
      onRecord(record) {
        if (record.ordinal < 2) record.acknowledge();
      },
    });
    await intake.ingest(chunkedSource(bytes, plan.cuts));
    await ledger.save(path);
    const loaded = await RecoveryLedger.load(path);
    assert.equal(loaded.size, 2);
    assert.equal(loaded.confirmedThrough(), 2056);
    const reparsed = JSON.parse(await readFile(path, 'utf8'));
    assert.equal(reparsed.version, 1);
    assert.deepEqual(reparsed.entries.map(entry => entry.ordinal), [0, 1]);
    assert.ok(reparsed.entries.every(entry => /^[a-f0-9]{64}$/.test(entry.digest)));
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

test('账本拒绝同一位置的不同内容', () => {
  const ledger = new RecoveryLedger();
  ledger.confirm({ key: 'a', ordinal: 0, start: 10, end: 20, digest: 'x', rows: 1 });
  assert.throws(() => ledger.confirm({
    key: 'a', ordinal: 0, start: 10, end: 20, digest: 'y', rows: 1,
  }), error => error instanceof IntakeError && error.code === 'REPLAY_CONFLICT');
});

test('确认缺口之前不夸大连贯确认位置', () => {
  const ledger = new RecoveryLedger([
    { key: 'a', ordinal: 0, start: 0, end: 100, digest: 'a' },
    { key: 'c', ordinal: 2, start: 200, end: 300, digest: 'c' },
  ]);
  assert.equal(ledger.confirmedThrough(), 100);
  ledger.confirm({ key: 'b', ordinal: 1, start: 100, end: 200, digest: 'b' });
  assert.equal(ledger.confirmedThrough(), 300);
});

test('中断后已发布未确认的批次不能再被确认', async () => {
  const records = [];
  const run = new StreamIntake({
    maxInflightBatches: 8,
    onRecord(record) {
      records.push(record);
      if (record.ordinal === 0) record.acknowledge();
    },
  });
  const controller = new AbortController();
  const running = run.ingest(chunkedSource(bytes, plan.cuts, { delayMs: 1 }),
    { signal: controller.signal });
  while (records.length < 3) await new Promise(resolve => setImmediate(resolve));
  controller.abort('link reset');
  await running;
  assert.equal(run.unconfirmedCount, 0);
  assert.ok(records.slice(1).every(record => record.released));
  assert.throws(() => records[1].acknowledge(), error =>
    error instanceof IntakeError && error.code === 'UNKNOWN_BATCH');
  assert.equal(run.ledger.size, 1);
});
