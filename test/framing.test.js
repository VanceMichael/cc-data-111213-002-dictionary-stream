import test from 'node:test';
import assert from 'node:assert/strict';
import IpcFrameParser, { FrameError } from '../src/framing.js';

const stream = await import('node:fs/promises')
  .then(fs => fs.readFile('fixtures/session.arrow'));

function parseInChunks(bytes, cuts) {
  const parser = new IpcFrameParser();
  const frames = [];
  const points = [...new Set(cuts.filter(cut => cut > 0 && cut < bytes.length))]
    .sort((a, b) => a - b);
  let previous = 0;
  for (const boundary of [...points, bytes.length]) {
    parser.append(bytes.subarray(previous, boundary));
    previous = boundary;
    let frame;
    while ((frame = parser.next())) frames.push(frame);
  }
  parser.finish();
  return frames;
}

function withoutMessages(frames) {
  return frames.map(frame => ({
    kind: frame.kind, start: frame.start, end: frame.end,
    metadataStart: frame.metadataStart, metadataEnd: frame.metadataEnd,
    bodyStart: frame.bodyStart, metadataLength: frame.metadataLength,
    bodyLength: frame.bodyLength,
  }));
}

test('逐字节切分与整块读取得到完全相同的帧边界', () => {
  const whole = withoutMessages(parseInChunks(stream, []));
  const bytewise = withoutMessages(
    parseInChunks(stream, [...Array(stream.length).keys()].map(i => i + 1)));
  assert.deepEqual(bytewise, whole);
});

test('切点落在长度前缀、元数据、正文和尾标记内部都不影响结果', () => {
  const whole = withoutMessages(parseInChunks(stream, []));
  const nasty = [
    1, 2, 3, 4, 5, 6, 7, 8, 9,
    439, 440, 441, 443, 447, 448, 449, 631, 632, 663, 664, 665, 666,
    1015, 1016, 1017, 1271, 1272, 1273,
    2847, 2848, 2849, 2855, 2856,
  ];
  assert.deepEqual(withoutMessages(parseInChunks(stream, nasty)), whole);
});

test('帧位置满足对齐与长度闭合关系', () => {
  const frames = parseInChunks(stream, [1, 3, 8, 21, 64]);
  for (const frame of frames) {
    assert.equal(frame.end - frame.start,
      frame.prefixBytes + frame.metadataLength + frame.bodyLength);
    assert.equal(frame.bodyStart, frame.metadataEnd);
    if (frame.kind !== 'end') {
      assert.equal(frame.metadataLength % 8, 0);
      assert.equal(frame.bodyLength % 8, 0);
    }
  }
  assert.equal(frames.at(-1).kind, 'end');
  assert.equal(frames.at(-1).end, stream.length);
});

test('未到齐的帧不会提前交出，且缓冲只保留当前帧', () => {
  const parser = new IpcFrameParser();
  // 600 字节停在第二个帧（字典，440..664）的正文中间。
  parser.append(stream.subarray(0, 600));
  const first = parser.next();
  assert.equal(first.kind, 'schema');
  assert.equal(parser.next(), null);
  assert.equal(parser.bufferedBytes, 600 - 440);
  parser.append(stream.subarray(600));
  const second = parser.next();
  assert.equal(second.kind, 'dictionary');
  assert.equal(second.start, 440);
  assert.deepEqual([second.metadataEnd, second.end], [632, 664]);
});

test('缺少尾标记、尾标记后有多余字节、元数据长度越界都会拒绝', () => {
  const truncated = new IpcFrameParser();
  truncated.append(stream.subarray(0, stream.length - 8));
  while (truncated.next()) { /* 消费完整帧 */ }
  assert.throws(() => truncated.finish(), error =>
    error instanceof FrameError && error.code === 'MISSING_END');

  const trailing = new IpcFrameParser();
  trailing.append(stream);
  let frame;
  while ((frame = trailing.next())) { /* 消费全部帧，最后一个应为 end */ }
  assert.equal(frame, null);
  assert.throws(() => trailing.append(Buffer.from([1])), error =>
    error instanceof FrameError && error.code === 'BYTES_AFTER_END');

  const capped = new IpcFrameParser({ maxMetadataBytes: 32 });
  capped.append(stream.subarray(0, 16));
  assert.throws(() => capped.next(), error =>
    error instanceof FrameError && error.code === 'METADATA_LIMIT');
});

test('旧格式（无续接标记）的流也能按帧解析', async () => {
  const { RecordBatchStreamWriter, tableFromArrays, vectorFromArray, Dictionary, Utf8, Int32 }
    = await import('apache-arrow');
  const writer = new RecordBatchStreamWriter({ autoDestroy: false, writeLegacyIpcFormat: true });
  writer.write(tableFromArrays({
    category: vectorFromArray(['甲', '乙'], new Dictionary(new Utf8(), new Int32(), 7)),
    value: [1, 2],
  }));
  writer.reset();
  const bytes = Buffer.from(writer.toUint8Array(true));
  const frames = parseInChunks(bytes, [1, 2, 3, 4, 5, 9, 13]);
  assert.deepEqual(frames.map(frame => frame.kind),
    ['schema', 'dictionary', 'recordBatch', 'end']);
  assert.ok(frames.every(frame => frame.legacy));
  assert.equal(frames.at(-1).end, bytes.length);
});
