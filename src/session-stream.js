import {
  Schema, Field, Dictionary, Utf8, Int32, Float64, List, Struct,
  RecordBatch, makeData, vectorFromArray,
} from 'apache-arrow';
import { VectorAssembler } from 'apache-arrow/visitor/vectorassembler';
import { Message, RecordBatch as MetadataRecordBatch,
  DictionaryBatch as MetadataDictionaryBatch } from 'apache-arrow/ipc/metadata/message';

const DICTIONARY_ID = 7;
const END_OF_STREAM = Buffer.from([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);

/** 按 IPC 规则拼接正文：每个缓冲补齐到 8 字节，空缓冲不占位置。 */
function alignBody(buffers) {
  const parts = [];
  for (const buffer of buffers) {
    const bytes = Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    if (bytes.length === 0) continue;
    parts.push(bytes);
    const padding = (8 - (bytes.length % 8)) % 8;
    if (padding > 0) parts.push(Buffer.alloc(padding));
  }
  return Buffer.concat(parts);
}

function encodeFrame(message, buffers) {
  const metadata = Buffer.from(Message.encode(message));
  const aligned = (metadata.byteLength + 7) & ~7;
  const head = Buffer.alloc(8);
  head.writeInt32LE(-1, 0);
  head.writeInt32LE(aligned, 4);
  return Buffer.concat([
    head, metadata, Buffer.alloc(aligned - metadata.byteLength), alignBody(buffers),
  ]);
}

/** 构造一个记录批次；两个字典列共用同一字典编号和同一份字典内容。 */
function buildBatch(schema, dictionary, rows) {
  const children = schema.fields.map((field) => {
    const values = rows.map(row => row[field.name]);
    if (field.type.typeId !== -1) return vectorFromArray(values, field.type).data[0];
    const lookup = new Map(dictionary.toArray().map((entry, index) => [entry, index]));
    const missing = values.filter(value => value !== null && !lookup.has(value));
    if (missing.length > 0) throw new Error(`字典缺少取值: ${missing.join(',')}`);
    const bitmap = new Uint8Array(Math.ceil(values.length / 8));
    values.forEach((value, index) => {
      if (value !== null) bitmap[index >> 3] |= 1 << (index & 7);
    });
    return makeData({
      type: field.type,
      data: Int32Array.from(values.map(value => (value === null ? 0 : lookup.get(value)))),
      nullBitmap: bitmap,
      dictionary,
      length: values.length,
      nullCount: values.filter(value => value === null).length,
    });
  });
  return new RecordBatch(schema, makeData({
    type: new Struct(schema.fields), length: rows.length, children, nullCount: 0,
  }));
}

function dictionaryFrame(vector, isDelta) {
  const asm = VectorAssembler.assemble(vector);
  const header = new MetadataRecordBatch(vector.length, asm.nodes, asm.bufferRegions, null);
  return encodeFrame(
    Message.from(new MetadataDictionaryBatch(header, DICTIONARY_ID, isDelta), asm.byteLength),
    asm.buffers,
  );
}

function batchFrame(batch) {
  const asm = VectorAssembler.assemble(batch);
  const header = new MetadataRecordBatch(batch.numRows, asm.nodes, asm.bufferRegions, null);
  return encodeFrame(Message.from(header, asm.byteLength), asm.buffers);
}

/**
 * 合成演示流：中文分类字典先给出两词，再以增量追加一词，最后整份替换；
 * 两个字段引用同一字典编号；tags 列同时出现空数组与空值。
 *
 * `frames` 记录每一帧的语义位置，`bytes` 是实际可发送的字节；
 * 帧边界由字节偏移累加得出，后续测试用它校验解析结果。
 */
export function buildSessionStream() {
  const dictionaryType = new Dictionary(new Utf8(), new Int32(), DICTIONARY_ID);
  const schema = new Schema([
    new Field('category', dictionaryType, true),
    new Field('review', dictionaryType, true),
    new Field('tags', new List(new Field('item', new Utf8(), true)), true),
    new Field('value', new Float64(), true),
  ], new Map(), new Map([[DICTIONARY_ID, new Utf8()]]));

  const segments = [];
  const frames = [];
  let position = 0;

  const push = (bytes, meta) => {
    segments.push(bytes);
    frames.push({ ...meta, start: position, end: position + bytes.length });
    position += bytes.length;
  };

  push(encodeFrame(Message.from(schema), []), { kind: 'schema' });

  const base = vectorFromArray(['合格', '待复核'], new Utf8());
  push(dictionaryFrame(base, false),
    { kind: 'dictionary', version: 1, change: 'replacement', entries: 2 });

  const rowsA = [
    { category: '合格', review: '合格', tags: [], value: 1.5 },
    { category: '待复核', review: '待复核', tags: null, value: null },
    { category: '合格', review: '待复核', tags: ['甲', '乙'], value: 0 },
    { category: null, review: null, tags: [], value: 9.2 },
  ];
  push(batchFrame(buildBatch(schema, base, rowsA)),
    { kind: 'recordBatch', rows: rowsA.length, version: 1 });

  const appended = base.concat(vectorFromArray(['待定'], new Utf8()));
  push(dictionaryFrame(vectorFromArray(['待定'], new Utf8()), true),
    { kind: 'dictionary', version: 2, change: 'delta', entries: 3 });

  const rowsB = [
    { category: '待定', review: '待定', tags: [], value: 2.5 },
    { category: '合格', review: '待复核', tags: ['丙'], value: 3.5 },
    { category: null, review: '合格', tags: null, value: null },
  ];
  push(batchFrame(buildBatch(schema, appended, rowsB)),
    { kind: 'recordBatch', rows: rowsB.length, version: 2 });

  const replaced = vectorFromArray(['通过', '驳回'], new Utf8());
  push(dictionaryFrame(replaced, false),
    { kind: 'dictionary', version: 3, change: 'replacement', entries: 2 });

  const rowsC = [
    { category: '通过', review: '驳回', tags: null, value: 4.5 },
    { category: '驳回', review: '通过', tags: [], value: null },
    { category: '通过', review: '驳回', tags: ['丁'], value: 5.5 },
  ];
  push(batchFrame(buildBatch(schema, replaced, rowsC)),
    { kind: 'recordBatch', rows: rowsC.length, version: 3 });

  push(END_OF_STREAM, { kind: 'end' });

  return {
    bytes: Buffer.concat(segments),
    frames,
    schema,
    rows: { a: rowsA, b: rowsB, c: rowsC },
  };
}

/** 演示与测试共用的切点：故意落在长度前缀、元数据、正文和尾标记内部。 */
export function writeFixturePlan(session) {
  const [, dictionaryFrame, firstBatch, , secondBatch] = session.frames;
  return {
    cuts: [
      1, 3, 8, 21, 64, 199,
      dictionaryFrame.end,
      firstBatch.start + 1, firstBatch.end - 1, firstBatch.end,
      firstBatch.end + 36, secondBatch.start - 1, secondBatch.start,
      2500, 2700, 2800, session.bytes.length - 1,
    ],
    consumer: { pauseAfterBatch: 1, maxInflightBatches: 1 },
    replayFrom: 0,
    expected: session.frames,
  };
}

export const DICTIONARY_IDENTIFIER = DICTIONARY_ID;
