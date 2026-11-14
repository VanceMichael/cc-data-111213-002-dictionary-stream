import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { DictionaryCatalog, rowsToJson } from '../src/decode.js';
import IpcFrameParser from '../src/framing.js';

const bytes = await readFile('fixtures/session.arrow');

function catalogWithFrames() {
  const parser = new IpcFrameParser();
  parser.append(bytes);
  const catalog = new DictionaryCatalog();
  const frames = [];
  let frame;
  while ((frame = parser.next())) {
    if (frame.kind === 'schema') catalog.acceptSchema(frame.message.header());
    else if (frame.kind === 'dictionary') {
      const header = frame.message.header();
      catalog.apply(frame, header.id, header.isDelta);
    } else if (frame.kind === 'recordBatch') {
      // 与接收端一致：批次到达时就取快照，而不是事后才看当前字典。
      frames.push({ frame, snapshot: catalog.snapshot(frame.start) });
    }
  }
  return { catalog, frames };
}

test('同一字典编号在 schema 中只登记一次，可被多个字段共用', () => {
  const parser = new IpcFrameParser();
  parser.append(bytes);
  const schemaFrame = parser.next();
  const catalog = new DictionaryCatalog();
  catalog.acceptSchema(schemaFrame.message.header());
  assert.deepEqual([...catalog.schema.dictionaries.keys()], [7]);
  assert.deepEqual(catalog.schema.schema.fields.map(field => field.name),
    ['category', 'review', 'tags', 'value']);
});

test('增量追加产生新版本，整份替换丢弃旧条目', async () => {
  const { catalog, frames } = catalogWithFrames();
  assert.deepEqual(frames.map(({ snapshot }) => snapshot.versions.get(7).version), [1, 2, 3]);
  assert.deepEqual([...frames[0].snapshot.dictionaries.get(7)], ['合格', '待复核']);
  assert.deepEqual([...frames[1].snapshot.dictionaries.get(7)], ['合格', '待复核', '待定']);
  assert.deepEqual([...frames[2].snapshot.dictionaries.get(7)], ['通过', '驳回']);
  for (const { snapshot } of frames) catalog.release(snapshot);
});

test('快照在字典替换后仍然指向旧版本，引用释放后才可丢弃', async () => {
  const { catalog, frames } = catalogWithFrames();
  for (const { snapshot } of frames.slice(1)) catalog.release(snapshot);
  const snapshot = catalog.snapshot(frames[0].frame.start);
  catalog.release(frames[0].snapshot);
  const before = [...snapshot.dictionaries.get(7)];
  assert.equal(catalog.current(7).references, 1);
  // 再应用一次替换：已持有的快照不受影响。
  const parser = new IpcFrameParser();
  parser.append(bytes.subarray(2056, 2280));
  const replacement = parser.next();
  catalog.apply(replacement, 7, false);
  assert.deepEqual([...snapshot.dictionaries.get(7)], before);
  assert.deepEqual([...catalog.current(7).vector], ['通过', '驳回']);
  catalog.release(snapshot);
  const retention = catalog.retention();
  assert.equal(retention.currentVersions, 1);
  assert.equal(retention.referencedByInflightBatches, 0);
});

test('rowsToJson 保留空数组与空值的区别', async () => {
  const { catalog, frames } = catalogWithFrames();
  const { decodeRecordBatch } = await import('../src/decode.js');
  const snapshot = frames[0].snapshot;
  const decoded = decodeRecordBatch(frames[0].frame, snapshot);
  const rows = rowsToJson(decoded.batch);
  assert.equal(rows[0].tags.length, 0);
  assert.equal(rows[1].tags, null);
  assert.deepEqual(rows[2].tags, ['甲', '乙']);
  assert.equal(rows[3].value, 9.2);
});

test('字典编号未知或增量缺少基准时拒绝', async () => {
  const { catalog, frames } = catalogWithFrames();
  assert.throws(() => catalog.apply(frames[0].frame, 99, false), error =>
    error.code === 'UNKNOWN_DICTIONARY');
  const fresh = new DictionaryCatalog();
  const parser = new IpcFrameParser();
  parser.append(bytes);
  fresh.acceptSchema(parser.next().message.header());
  const base = parser.next();
  assert.equal(base.kind, 'dictionary');
  fresh.apply(base, 7, false);
  const batch = parser.next();
  assert.equal(batch.kind, 'recordBatch');
  const delta = parser.next();
  assert.equal(delta.kind, 'dictionary');
  assert.throws(() => {
    const withoutBase = new DictionaryCatalog();
    const replay = new IpcFrameParser();
    replay.append(bytes);
    withoutBase.acceptSchema(replay.next().message.header());
    withoutBase.apply(delta, 7, true);
  }, error => error.code === 'DICTIONARY_DELTA_WITHOUT_BASE');
});
