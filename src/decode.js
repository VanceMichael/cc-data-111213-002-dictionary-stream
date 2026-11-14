import {
  Vector, makeData, Struct, RecordBatch,
} from 'apache-arrow';
import { VectorLoader } from 'apache-arrow/visitor/vectorloader';

export class DecodeError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'DecodeError';
    this.code = code;
    this.details = details;
  }
}

const isDictionary = (type) => type?.typeId === -1;

/**
 * 一份不可变的 schema 加上若干字典版本。
 *
 * 每个版本记录版本号、内容、来源帧和仍在引用的批次数。批次解码时取一份快照，
 * 快照里放的是当时每个字典的版本对象；之后字典被替换只是发布新版本，
 * 已经产出的批次仍指向旧版本，索引不会变义。版本被释放后内容不再被任何批次引用，
 * 接收器即可丢弃旧字典，而不需要保留整段历史。
 */
export class DictionaryCatalog {
  #schema = null;
  #versions = new Map();
  #staleReleased = 0;

  get schema() { return this.#schema; }

  acceptSchema(schema) {
    if (this.#schema) {
      throw new DecodeError('DUPLICATE_SCHEMA', '同一个流里出现第二份 schema', {
        first: this.#schema.signature,
      });
    }
    this.#schema = {
      schema,
      signature: schemaSignature(schema),
      dictionaries: schema.dictionaries,
      fields: collectDictionaryFields(schema.fields),
    };
    return this.#schema;
  }

  requireSchema(position) {
    if (!this.#schema) {
      throw new DecodeError('MISSING_SCHEMA', '在 schema 之前出现其它消息', { position });
    }
    return this.#schema;
  }

  /**
   * 应用一个字典批次。`isDelta` 为真表示追加在上一版本之后，否则整份替换。
   */
  apply(frame, id, isDelta) {
    const schema = this.requireSchema(frame.start);
    if (!schema.dictionaries.has(id)) {
      throw new DecodeError('UNKNOWN_DICTIONARY', '字典批次引用了 schema 中没有的字典编号', {
        id, position: frame.start,
      });
    }
    const entries = decodeDictionaryEntries(frame, schema, id);
    const previous = this.#versions.get(id);
    if (isDelta && !previous) {
      throw new DecodeError('DICTIONARY_DELTA_WITHOUT_BASE',
        '字典增量没有已存在的基准版本', { id, position: frame.start });
    }
    const vector = isDelta ? previous.vector.concat(entries) : entries;
    // 被替换的旧版本是否已经没有在途批次引用；是则内容可直接丢弃。
    // 仍被引用的版本由持有它的批次在确认后释放，批次不会因此改变含义。
    const supersededUnused = previous !== undefined && previous.references === 0;
    const version = {
      id,
      version: previous ? previous.version + 1 : 1,
      kind: isDelta ? 'delta' : 'replacement',
      entries: vector.length,
      addedEntries: entries.length,
      supersededUnused,
      vector,
      frame,
      references: 0,
    };
    this.#versions.set(id, version);
    if (previous) this.#staleReleased += 1;
    return version;
  }

  /** 取一份批次解码用的快照，并把用到的版本记上一次引用。 */
  snapshot(position) {
    const schema = this.requireSchema(position);
    const dictionaries = new Map();
    const versions = new Map();
    for (const field of schema.fields) {
      const current = this.#versions.get(field.id);
      if (!current) {
        throw new DecodeError('MISSING_DICTIONARY',
          '批次引用的字典还没有收到', { position, id: field.id });
      }
      current.references += 1;
      dictionaries.set(field.id, current.vector);
      versions.set(field.id, current);
    }
    return { schema, dictionaries, versions };
  }

  /** 批次消费完成（或被丢弃）后释放快照引用。 */
  release(snapshot) {
    if (!snapshot) return;
    for (const version of snapshot.versions.values()) {
      version.references = Math.max(0, version.references - 1);
    }
  }

  current(id) {
    return this.#versions.get(id);
  }

  /** 当前每个字典的版本与内容长度。 */
  versions() {
    return [...this.#versions.entries()].map(([id, version]) => ({
      id, version: version.version, kind: version.kind,
      entries: version.entries, references: version.references,
      frame: frameLocation(version.frame),
    }));
  }

  /** 丢弃没有被任何在途批次引用的旧字典内容。 */
  retention() {
    return {
      currentVersions: this.#versions.size,
      supersededReleased: this.#staleReleased,
      referencedByInflightBatches: [...this.#versions.values()]
        .reduce((sum, version) => sum + version.references, 0),
      trackedEntries: [...this.#versions.values()].reduce((sum, v) => sum + v.entries, 0),
    };
  }
}

export function schemaSignature(schema) {
  return schema.fields
    .map(field => `${field.name}:${String(field.type)}:${field.nullable ? 1 : 0}`)
    .join('|');
}

/** 收集 schema 里所有字典字段，同一字典编号只保留一处。 */
export function collectDictionaryFields(fields, seen = new Set()) {
  const found = [];
  const visit = (field) => {
    const type = field.type;
    if (isDictionary(type) && !seen.has(type.id)) {
      seen.add(type.id);
      found.push({ id: type.id, path: field.name, type });
    }
    for (const child of type?.children ?? []) visit(child);
  };
  for (const field of fields) visit(field);
  return found;
}

export function frameLocation(frame) {
  if (!frame) return null;
  return {
    start: frame.start, end: frame.end,
    metadataStart: frame.metadataStart, metadataEnd: frame.metadataEnd,
    bodyStart: frame.bodyStart, bodyEnd: frame.end,
    metadataLength: frame.metadataLength, bodyLength: frame.bodyLength,
    legacy: frame.legacy,
  };
}

function decodeDictionaryEntries(frame, schema, id) {
  const dictionaryType = schema.dictionaries.get(id);
  const header = frame.message.header();
  try {
    const loader = new VectorLoader(
      frame.body, header.data.nodes, header.data.buffers, new Map(),
      schema.schema.metadataVersion);
    const data = loader.visit(dictionaryType);
    return new Vector([data]).memoize();
  } catch (error) {
    throw new DecodeError('BAD_DICTIONARY_BATCH', `字典批次无法解码: ${error.message}`, {
      id, position: frame.start,
    });
  }
}

/**
 * 解码一个记录批次。返回值携带本轮使用的字典版本和来源帧；
 * 批次内的索引只按本轮快照解释，不受之后到达的字典影响。
 */
export function decodeRecordBatch(frame, snapshot) {
  const header = frame.message.header();
  const fields = snapshot.schema.schema.fields;
  if (!Number.isSafeInteger(header.length) || header.length < 0) {
    throw new DecodeError('BAD_LENGTH', '记录批次数无效', {
      position: frame.start, rows: header.length,
    });
  }
  try {
    const loader = new VectorLoader(
      frame.body, header.nodes, header.buffers, snapshot.dictionaries,
      snapshot.schema.schema.metadataVersion);
    const children = loader.visitMany(fields);
    const data = makeData({
      type: new Struct(fields), length: header.length, children, nullCount: 0,
    });
    const batch = new RecordBatch(snapshot.schema.schema, data);
    return {
      batch,
      rows: header.length,
      columns: fields.map(field => describeColumn(field, snapshot)),
    };
  } catch (error) {
    if (error instanceof DecodeError) throw error;
    throw new DecodeError('BAD_RECORD_BATCH', `记录批次无法解码: ${error.message}`, {
      position: frame.start, bodyLength: frame.bodyLength,
    });
  }
}

function describeColumn(field, snapshot) {
  const type = field.type;
  const version = isDictionary(type) ? snapshot.versions.get(type.id) : null;
  return {
    name: field.name,
    type: String(type),
    nullable: field.nullable,
    dictionaryId: version ? version.id : null,
    dictionaryVersion: version ? version.version : null,
  };
}

/** 行转普通对象：列表保持数组，空数组与 null 各自保留。 */
export function rowsToJson(batch, limit = Number.POSITIVE_INFINITY) {
  const rows = [];
  for (let index = 0; index < batch.numRows && rows.length < limit; index += 1) {
    const row = batch.get(index);
    const object = {};
    for (const field of batch.schema.fields) {
      object[field.name] = toPlain(row[field.name]);
    }
    rows.push(object);
  }
  return rows;
}

function toPlain(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'bigint') return Number(value);
  if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
    return value;
  }
  if (Array.isArray(value)) return value.map(toPlain);
  if (typeof value.toArray === 'function') return Array.from(value, toPlain);
  if (ArrayBuffer.isView(value)) return Array.from(value, toPlain);
  if (typeof value === 'object' && value[Symbol.iterator]) return Array.from(value, toPlain);
  return value;
}
