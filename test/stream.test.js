import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { chunkedSource } from '../src/demo.js';
import StreamIntake, { RecoveryLedger } from '../src/stream.js';

const bytes = await readFile('fixtures/session.arrow');
const plan = JSON.parse(await readFile('fixtures/stream-plan.json', 'utf8'));

function collectingIntake(options = {}) {
  const delivered = [];
  const intake = new StreamIntake({
    maxInflightBatches: 8,
    ...options,
    onRecord(record) {
      // 业务方在回调里读取并留存需要的行值，然后确认。
      delivered.push({
        ordinal: record.ordinal,
        rows: record.rows,
        dictionaries: record.dictionaries,
        values: record.preview(),
        contentReleasedAfterAck: false,
      });
      record.acknowledge();
      delivered.at(-1).contentReleasedAfterAck = record.contentReleased;
    },
  });
  return { intake, delivered };
}

test('任意网络切分下逐批发布，批次绑定当时的字典版本', async () => {
  const { intake, delivered } = collectingIntake();
  const summary = await intake.ingest(chunkedSource(bytes, plan.cuts));
  assert.equal(summary.batchesPublished, 3);
  assert.deepEqual(summary.batches.map(batch => batch.ordinal), [0, 1, 2]);
  assert.deepEqual(summary.batches.map(batch => batch.rows), [4, 3, 3]);
  assert.deepEqual(delivered.map(batch => batch.dictionaries[0].version), [1, 2, 3]);
  assert.deepEqual(delivered.map(batch => batch.values[0].category),
    ['合格', '待定', '通过']);
  assert.ok(delivered.every(batch => batch.contentReleasedAfterAck),
    '确认后接收端释放 Arrow 数据');
  assert.equal(summary.position, bytes.length);
  assert.equal(summary.replayedConfirmedSkips, 0);
});

test('同一字典被两个字段引用，替换后新批次不改变已发布批次', async () => {
  const { intake, delivered } = collectingIntake();
  await intake.ingest(chunkedSource(bytes, plan.cuts));
  assert.deepEqual(delivered[0].dictionaries, [
    { field: 'category', id: 7, version: 1 },
    { field: 'review', id: 7, version: 1 },
  ]);
  // 字典被整份替换之后，第一批的取值仍然是替换前的内容。
  assert.deepEqual(delivered[0].values.slice(0, 2), [
    { category: '合格', review: '合格', tags: [], value: 1.5 },
    { category: '待复核', review: '待复核', tags: null, value: null },
  ]);
  assert.deepEqual(delivered[2].values.slice(0, 1),
    [{ category: '通过', review: '驳回', tags: null, value: 4.5 }]);
});

test('空数组与空值在同一列共存', async () => {
  const { intake, delivered } = collectingIntake();
  await intake.ingest(chunkedSource(bytes, plan.cuts));
  const rows = delivered[0].values;
  assert.ok(rows.some(row => Array.isArray(row.tags) && row.tags.length === 0));
  assert.ok(rows.some(row => row.tags === null));
  assert.deepEqual(rows[2].tags, ['甲', '乙']);
});

test('下游停住时来源立即暂停，确认后恢复', async () => {
  const state = { deliveredBytes: 0 };
  let firstRecord;
  const intake = new StreamIntake({
    maxInflightBatches: 1,
    onRecord(record) {
      if (!firstRecord) {
        firstRecord = record;
        return;
      }
      record.acknowledge();
    },
  });
  const source = {
    [Symbol.asyncIterator]() {
      const inner = chunkedSource(bytes, plan.cuts)[Symbol.asyncIterator]();
      return {
        async next() {
          const result = await inner.next();
          if (!result.done) state.deliveredBytes += result.value.byteLength;
          return result;
        },
      };
    },
  };
  const running = intake.ingest(source);
  while (!firstRecord) await new Promise(resolve => setImmediate(resolve));
  const deliveredAtStall = state.deliveredBytes;
  // 下游故意不动：接收端必须在收满一块之前停住。
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(state.deliveredBytes, deliveredAtStall);
  assert.ok(intake.bufferedBytes <= 8, '接收端不应积压后续原始块');
  assert.equal(intake.events.filter(event => event.kind === 'backpressure').length, 1);
  firstRecord.acknowledge();
  const summary = await running;
  assert.equal(summary.batchesPublished, 3);
  assert.equal(summary.batchesAcknowledged, 3);
  assert.equal(summary.events.filter(event => event.kind === 'resumed').length, 1);
});

test('断线重播：已确认不重复发布，未确认不静默跳过', async () => {
  const ledger = new RecoveryLedger();
  const confirmed = [];
  const first = new StreamIntake({
    ledger,
    maxInflightBatches: 8,
    onRecord(record) {
      if (record.ordinal < 2) {
        record.acknowledge();
        confirmed.push(record.ordinal);
      }
    },
  });
  const firstSummary = await first.ingest(chunkedSource(bytes, plan.cuts));
  assert.deepEqual(firstSummary.batches.map(batch => batch.acknowledged),
    [true, true, false]);
  assert.equal(firstSummary.resumeHint.firstUnconfirmedBatchStart, 2280);
  assert.equal(firstSummary.confirmedThrough, 2056);
  first.abort('drop');
  assert.equal(ledger.size, 2);

  const republished = [];
  const second = new StreamIntake({
    ledger,
    maxInflightBatches: 8,
    onRecord(record) {
      republished.push(record.ordinal);
      record.acknowledge();
    },
  });
  const secondSummary = await second.ingest(chunkedSource(bytes, plan.cuts));
  assert.deepEqual(republished, [2]);
  assert.equal(secondSummary.replayedConfirmedSkips, 2);
  assert.equal(secondSummary.ledger.entries, 3);
  assert.equal(secondSummary.confirmedThrough, 2848);
  assert.equal(secondSummary.replayedConfirmedSkips,
    secondSummary.events.filter(event => event.kind === 'replaySkip').length);
});

test('重播内容不一致会被拒绝，而不是按位置静默接受', async () => {
  const ledger = new RecoveryLedger();
  const first = collectingIntake({ ledger, maxInflightBatches: 8 });
  await first.intake.ingest(chunkedSource(bytes, plan.cuts));
  const tampered = Buffer.from(bytes);
  tampered[664 + 8 + 60] ^= 0xff; // 改坏第一批元数据中的一个字节
  const second = collectingIntake({ ledger, maxInflightBatches: 8 });
  await assert.rejects(
    second.intake.ingest(chunkedSource(tampered, plan.cuts)),
    error => error.code === 'REPLAY_CONFLICT' || error.code === 'BAD_METADATA');
});

test('没有尾标记的断流不会假装接收完成', async () => {
  const { intake } = collectingIntake();
  await assert.rejects(
    intake.ingest(chunkedSource(bytes.subarray(0, bytes.length - 4), plan.cuts)),
    error => error.code === 'MISSING_END');
});

test('批次引用的字典尚未到达时拒绝解码', async () => {
  const withoutDictionary = Buffer.concat([bytes.subarray(0, 440), bytes.subarray(664)]);
  const { intake } = collectingIntake();
  await assert.rejects(intake.ingest(chunkedSource(withoutDictionary, plan.cuts)),
    error => error.code === 'MISSING_DICTIONARY');
});

test('未确认批次不计入确认位置，确认后才向前推进', async () => {
  const held = [];
  const intake = new StreamIntake({
    maxInflightBatches: 8,
    onRecord(record) {
      held.push(record);
      if (record.ordinal === 0) record.acknowledge();
    },
  });
  const summary = await intake.ingest(chunkedSource(bytes, plan.cuts));
  assert.equal(summary.confirmedThrough, 1272);
  assert.equal(summary.unconfirmed, 2);
  held[2].acknowledge();
  assert.equal(intake.confirmedThrough, 1272, '中间批次未确认时不能越过缺口');
  held[1].acknowledge();
  assert.equal(intake.confirmedThrough, 2848);
  assert.equal(intake.unconfirmedCount, 0);
});

test('确认同一批次两次是幂等的', async () => {
  const records = [];
  const intake = new StreamIntake({
    maxInflightBatches: 8,
    onRecord(record) { records.push(record); },
  });
  await intake.ingest(chunkedSource(bytes, plan.cuts));
  const first = records[0].acknowledge();
  const second = records[0].acknowledge();
  assert.equal(first, second);
  assert.equal(intake.ledger.size, 1);
});
