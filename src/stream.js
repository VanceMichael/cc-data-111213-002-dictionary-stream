import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { writeFileAtomic } from './atomic.js';
import IpcFrameParser, { FrameError } from './framing.js';
import {
  DictionaryCatalog, DecodeError, decodeRecordBatch, rowsToJson, frameLocation,
} from './decode.js';

export class IntakeError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'IntakeError';
    this.code = code;
    this.details = details;
  }
}

export class AbortedIntake extends Error {
  constructor(reason) {
    super(`接收已中断: ${reason}`);
    this.name = 'AbortedIntake';
    this.reason = reason;
  }
}

const frameKey = frame => `${frame.start}+${frame.metadataLength}+${frame.bodyLength}`;

function frameDigest(frame) {
  return createHash('sha256')
    .update(frame.metadataBytes)
    .update(frame.body)
    .digest('hex');
}

/**
 * 已确认批次的持久记录。
 *
 * 合作方只允许从流起点重播，因此同一来源的帧起点在重播中稳定；键由帧起点与两段长度组成，
 * 摘要覆盖元数据和正文，用于发现重播内容不一致。只有业务确认的批次才写入账本；
 * 未确认批次不进账本，重播时必然重新发布。
 */
export class RecoveryLedger {
  #entries = new Map();

  constructor(entries = []) {
    for (const entry of entries) this.#entries.set(entry.key, entry);
  }

  static async load(path) {
    try {
      const parsed = JSON.parse(await readFile(path, 'utf8'));
      return new RecoveryLedger(parsed.entries ?? []);
    } catch (error) {
      if (error.code === 'ENOENT') return new RecoveryLedger();
      throw error;
    }
  }

  get size() { return this.#entries.size; }

  get(key) { return this.#entries.get(key); }

  has(key) { return this.#entries.has(key); }

  confirm(entry) {
    const previous = this.#entries.get(entry.key);
    if (previous) {
      if (previous.digest !== entry.digest) {
        throw new IntakeError('REPLAY_CONFLICT', '同一来源位置在重播中出现了不同内容', {
          key: entry.key, first: previous.digest, replayed: entry.digest,
        });
      }
      return previous;
    }
    this.#entries.set(entry.key, { ...entry });
    return this.#entries.get(entry.key);
  }

  entries() {
    return [...this.#entries.values()].sort((a, b) => a.ordinal - b.ordinal || a.start - b.start);
  }

  /** 连续确认到的流位置：按批次序号逐段拼接，遇到缺口即停止。 */
  confirmedThrough() {
    let through = 0;
    let expected = 0;
    for (const entry of this.entries()) {
      if (entry.ordinal !== expected) break;
      expected += 1;
      through = entry.end;
    }
    return through;
  }

  toJSON() {
    return { version: 1, entries: this.entries() };
  }

  async save(path) {
    await writeFileAtomic(path, `${JSON.stringify(this.toJSON(), null, 2)}\n`);
  }

  clone() {
    return new RecoveryLedger(this.entries());
  }
}

/**
 * 连续接收端。
 *
 * - 字节一到就按帧解析；只有完整帧才交给解码，未完整部分只保留当前帧可能的前缀。
 * - schema 与字典批次到达即生效；记录批次立即按「当时有效」的字典快照解码并发布。
 * - 每个发布批次带稳定来源键和所用字典版本；消费方确认后写入恢复账本。
 * - 已发布未确认批次数达到 `maxInflightBatches` 时立即暂停拉取来源字节；
 *   接收端不替慢消费者保存后续原始块，也不会把整个流先收进内存。
 * - 重播从流起点进行：已确认批次按来源键跳过且校验内容一致；未确认批次重新发布。
 *   已确认的不重复发布，未确认的不静默跳过。
 *
 * 一个实例处理一次连接；断线后用同一份账本新建实例，从 0 重播即可续上。
 */
export class StreamIntake {
  #parser;
  #catalog = new DictionaryCatalog();
  #ledger;
  #onRecord;
  #maxInflight;
  #pending = new Map();
  #published = [];
  #events = [];
  #schemaSeen = false;
  #abortController = null;
  #abortReason = null;
  #closed = false;
  #started = false;
  #chunksPulled = 0;
  #bytesPulled = 0;
  #replayMode = false;
  #batchOrdinal = 0;

  constructor({
    onRecord = () => {},
    ledger = new RecoveryLedger(),
    maxInflightBatches = 2,
    maxMetadataBytes = 16 * 1024 * 1024,
    maxBodyBytes = 128 * 1024 * 1024,
  } = {}) {
    if (!Number.isInteger(maxInflightBatches) || maxInflightBatches < 1) {
      throw new TypeError('maxInflightBatches 必须是正整数');
    }
    this.#onRecord = onRecord;
    this.#ledger = ledger;
    this.#maxInflight = maxInflightBatches;
    this.#parser = new IpcFrameParser({ maxMetadataBytes, maxBodyBytes });
  }

  get position() { return this.#parser.position; }
  get bufferedBytes() { return this.#parser.bufferedBytes; }
  get chunksPulled() { return this.#chunksPulled; }
  get unconfirmedCount() { return this.#pending.size; }
  get catalog() { return this.#catalog; }
  get ledger() { return this.#ledger; }
  get events() { return this.#events.map(event => ({ ...event })); }
  get published() { return [...this.#published]; }

  /** 连续确认到的流位置。 */
  get confirmedThrough() { return this.#ledger.confirmedThrough(); }

  /** 断线后恢复位置；合作方只支持从 0 重播，这里说明需要重新覆盖到哪一帧。 */
  get resumeHint() {
    const first = this.#published.find(record => !record.acknowledged && !record.released);
    return {
      partnerReplayFrom: 0,
      firstUnconfirmedBatchStart: first ? first.start : null,
      confirmedThrough: this.confirmedThrough,
      acknowledged: this.#published.filter(record => record.acknowledged).length,
      ledgerSize: this.#ledger.size,
    };
  }

  /**
   * 拉取来源字节直到尾标记。达到背压上限时暂停拉取，直到有批次被确认。
   * `awaitPending` 为真时，来源结束后继续等待未确认批次；否则原样返回它们。
   */
  async ingest(source, { signal = null, awaitPending = false } = {}) {
    if (this.#closed) throw new IntakeError('CLOSED', '接收端已经关闭');
    if (this.#started) throw new IntakeError('ALREADY_INGESTED', '该接收端已经处理过一条连接');
    this.#started = true;
    this.#abortController = new AbortController();
    this.#abortReason = null;
    this.#replayMode = this.#ledger.size > 0;
    if (signal) {
      if (signal.aborted) this.#triggerAbort(signal.reason);
      else signal.addEventListener('abort', () => this.#triggerAbort(signal.reason), { once: true });
    }
    const iterator = toIterator(source);
    this.#emit('start', {
      maxInflightBatches: this.#maxInflight,
      ledgerSize: this.#ledger.size,
      replay: this.#replayMode,
    });
    try {
      for (;;) {
        const frame = await this.#nextFrame(iterator);
        if (frame === null) break;
        this.#handleFrame(frame);
      }
      this.#parser.finish();
      this.#emit('end', {
        position: this.position,
        chunksPulled: this.#chunksPulled,
        bytesPulled: this.#bytesPulled,
        confirmedThrough: this.confirmedThrough,
        unconfirmed: this.#pending.size,
      });
    } catch (error) {
      if (this.#abortReason !== null) {
        this.#releasePending();
        this.#emit('aborted', {
          position: this.position,
          reason: String(this.#abortReason),
          unconfirmedReleased: this.#published.filter(record => record.released).length,
        });
      } else {
        throw this.#wrap(error);
      }
    } finally {
      this.#abortController = null;
    }
    if (awaitPending) await this.waitForPending();
    return this.summary();
  }

  /** 业务确认一个已发布批次；重复确认幂等。确认后释放该批次持有的字典版本。 */
  acknowledge(record, { by = 'consumer' } = {}) {
    if (!record || record.session !== this || record.released) {
      throw new IntakeError('UNKNOWN_BATCH', '确认的批次不属于当前可确认范围', {
        key: record?.key ?? null,
      });
    }
    if (record.acknowledged) return record.acknowledgedAt;
    const confirmedAt = new Date().toISOString();
    this.#ledger.confirm({
      key: record.key,
      ordinal: record.ordinal,
      start: record.start,
      end: record.end,
      digest: record.digest,
      rows: record.rows,
      confirmedAt,
      confirmedBy: by,
    });
    record.acknowledged = true;
    record.acknowledgedAt = confirmedAt;
    record.acknowledgedBy = by;
    record.releaseContent();
    this.#pending.delete(record.key);
    this.#catalog.release(record.snapshot);
    record.resolveAcknowledged(true);
    this.#emit('acknowledged', {
      key: record.key,
      ordinal: record.ordinal,
      start: record.start,
      end: record.end,
      rows: record.rows,
      confirmedThrough: this.confirmedThrough,
      ledgerSize: this.#ledger.size,
      pending: this.#pending.size,
    });
    return confirmedAt;
  }

  /** 主动中断本次接收；已发布未确认的批次标记为释放，不进入账本。 */
  abort(reason = 'aborted') {
    this.#triggerAbort(reason);
  }

  async close() {
    this.#closed = true;
    this.#emit('close', { position: this.position });
  }

  /** 等所有已发布批次被确认。 */
  async waitForPending() {
    while (this.#pending.size > 0) {
      const records = [...this.#pending.values()];
      this.#emit('drainStart', { pending: records.length });
      await Promise.all(records.map(record => record.acknowledgedPromise));
      this.#emit('drainEnd', { pending: this.#pending.size });
    }
  }

  /** 每个批次的来源位置与所用字典版本，而不是只证明最后一张表能打开。 */
  batchReport() {
    return this.#published.map(record => ({
      ordinal: record.ordinal,
      key: record.key,
      replayed: record.replayed,
      released: record.released,
      start: record.start,
      end: record.end,
      metadata: { start: record.metadataStart, end: record.metadataEnd },
      body: { start: record.bodyStart, end: record.bodyEnd },
      rows: record.rows,
      acknowledged: record.acknowledged,
      acknowledgedAt: record.acknowledgedAt,
      dictionaries: record.dictionaries,
      columns: record.columns,
      contentReleased: record.contentReleased,
      // 已确认批次的 Arrow 数据已释放；行值只在确认前可读。
      values: record.preview(),
    }));
  }

  summary() {
    return {
      position: this.position,
      chunksPulled: this.#chunksPulled,
      bytesPulled: this.#bytesPulled,
      bufferedBytes: this.bufferedBytes,
      batchesPublished: this.#published.length,
      batchesAcknowledged: this.#published.filter(record => record.acknowledged).length,
      batchesReleased: this.#published.filter(record => record.released).length,
      batchesReplayed: this.#published.filter(record => record.replayed).length,
      replayedConfirmedSkips: this.events.filter(event => event.kind === 'replaySkip').length,
      confirmedThrough: this.confirmedThrough,
      resumeHint: this.resumeHint,
      ledger: { entries: this.#ledger.size, confirmedThrough: this.#ledger.confirmedThrough() },
      dictionaries: this.#catalog.versions(),
      dictionaryRetention: this.#catalog.retention(),
      unconfirmed: this.#pending.size,
      batches: this.batchReport(),
      events: this.events,
    };
  }

  /**
   * 只在当前缓冲没有完整帧时才拉取下一块；发布下一批会超过在途上限时先等确认。
   * 这样来源端承受的暂停来自真正的消费延迟，接收端不会把后续原始块收进来。
   */
  async #nextFrame(iterator) {
    for (;;) {
      const frame = this.#parser.next();
      if (frame) {
        if (frame.kind === 'recordBatch') await this.#ensureCapacity();
        return frame;
      }
      // 缓冲里没有完整帧时，只有未确认批次数低于上限才继续要字节。
      // 达到上限就先等确认，来源端因此感受到真正的暂停。
      if (this.#pending.size >= this.#maxInflight) {
        await this.#waitForCapacity();
        continue;
      }
      const chunk = await this.#raceAbort(Promise.resolve(iterator.next()));
      if (chunk.done) return null;
      if (chunk.value.byteLength === 0) continue;
      this.#chunksPulled += 1;
      this.#bytesPulled += chunk.value.byteLength;
      this.#emit('chunk', {
        bytes: chunk.value.byteLength,
        position: this.position,
        bufferedBeforeAppend: this.bufferedBytes,
        pending: this.#pending.size,
      });
      this.#parser.append(chunk.value);    }
  }

  async #ensureCapacity() {
    if (this.#pending.size >= this.#maxInflight) await this.#waitForCapacity();
  }

  async #waitForCapacity() {
    while (this.#pending.size >= this.#maxInflight) {
      const oldest = this.#pending.values().next().value;
      this.#emit('backpressure', {
        pending: this.#pending.size,
        limit: this.#maxInflight,
        position: this.position,
        bufferedBytes: this.bufferedBytes,
        waitingFor: { key: oldest.key, start: oldest.start, end: oldest.end, rows: oldest.rows },
        sourcePaused: true,
      });
      const resumed = await this.#raceAbort(oldest.acknowledgedPromise);
      if (!resumed) throw new AbortedIntake(this.#abortReason);
      this.#pending.delete(oldest.key);
      this.#emit('resumed', {
        pending: this.#pending.size,
        position: this.position,
        bufferedBytes: this.bufferedBytes,
      });
    }
  }

  #handleFrame(frame) {
    if (frame.kind === 'end') {
      this.#emit('eos', { start: frame.start, end: frame.end });
      return;
    }
    if (frame.kind === 'schema') {
      if (this.#schemaSeen) {
        throw new IntakeError('DUPLICATE_SCHEMA', '同一个流里出现第二份 schema', {
          position: frame.start,
        });
      }
      const schema = frame.message.header();
      try {
        this.#catalog.acceptSchema(schema);
      } catch (error) {
        throw this.#wrap(error);
      }
      this.#schemaSeen = true;
      this.#emit('schema', {
        start: frame.start,
        end: frame.end,
        fields: schema.fields.map(field => ({
          name: field.name, type: String(field.type), nullable: field.nullable,
        })),
        dictionaryIds: [...schema.dictionaries.keys()],
      });
      return;
    }
    if (frame.kind === 'dictionary') {
      const header = frame.message.header();
      let version;
      try {
        version = this.#catalog.apply(frame, header.id, header.isDelta);
      } catch (error) {
        throw this.#wrap(error);
      }
      this.#emit('dictionary', {
        id: header.id,
        version: version.version,
        change: version.kind,
        added: version.addedEntries,
        entries: version.entries,
        start: frame.start,
        end: frame.end,
        superseded: version.supersededUnused,
      });
      return;
    }
    this.#publishRecordBatch(frame);
  }

  #publishRecordBatch(frame) {
    const ordinal = this.#batchOrdinal;
    this.#batchOrdinal += 1;
    const key = frameKey(frame);
    const digest = frameDigest(frame);
    const confirmed = this.#ledger.get(key);
    if (confirmed) {
      if (confirmed.digest !== digest) {
        throw new IntakeError('REPLAY_CONFLICT', '已确认批次在重播中内容不一致', {
          key, start: frame.start, expected: confirmed.digest, actual: digest,
        });
      }
      this.#emit('replaySkip', {
        key, start: frame.start, end: frame.end,
        confirmedAt: confirmed.confirmedAt,
        position: this.position,
      });
      return;
    }
    let snapshot = null;
    let decoded;
    try {
      snapshot = this.#catalog.snapshot(frame.start);
      decoded = decodeRecordBatch(frame, snapshot);
    } catch (error) {
      this.#catalog.release(snapshot);
      throw this.#wrap(error);
    }
    const record = makeRecord({
      session: this,
      key,
      digest,
      frame,
      snapshot,
      decoded,
      replayed: this.#replayMode,
      ordinal,
      acknowledge: options => this.acknowledge(record, options),
    });
    this.#pending.set(key, record);
    this.#published.push(record);
    this.#emit('batch', {
      key,
      ordinal,
      start: frame.start,
      end: frame.end,
      rows: decoded.rows,
      replayed: record.replayed,
      dictionaries: record.dictionaries,
      pending: this.#pending.size,
    });
    try {
      const maybe = this.#onRecord(record);
      if (maybe && typeof maybe.then === 'function') maybe.catch(error => this.abort(error));
    } catch (error) {
      this.abort(error);
    }
  }

  async #raceAbort(promise) {
    if (this.#abortReason !== null) throw new AbortedIntake(this.#abortReason);
    const signal = this.#abortController.signal;
    let onAbort = null;
    const aborted = new Promise((_, reject) => {
      onAbort = () => reject(new AbortedIntake(this.#abortReason));
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
    try {
      return await Promise.race([promise, aborted]);
    } finally {
      // 悬空的中断 promise 不能留下未处理的拒绝。
      aborted.catch(() => {});
      if (onAbort) signal.removeEventListener('abort', onAbort);
    }
  }

  #triggerAbort(reason) {
    if (this.#abortReason !== null) return;
    this.#abortReason = reason;
    this.#abortController?.abort(reason);
    this.#releasePending();
    this.#emit('abort', {
      position: this.position,
      reason: String(reason),
      released: this.#published.filter(record => record.released).length,
    });
  }

  #releasePending() {
    for (const record of this.#pending.values()) {
      record.released = true;
      this.#catalog.release(record.snapshot);
      record.resolveAcknowledged(false);
    }
    this.#pending.clear();
  }

  #emit(kind, details) {
    this.#events.push({ at: new Date().toISOString(), kind, ...details });
  }

  #wrap(error) {
    if (error instanceof IntakeError || error instanceof FrameError
      || error instanceof DecodeError || error instanceof AbortedIntake) {
      return error;
    }
    return new IntakeError('INTAKE_FAILED', error.message, { cause: error.message });
  }
}

function makeRecord({ session, key, digest, frame, snapshot, decoded, replayed, ordinal, acknowledge }) {
  let resolveAcknowledged;
  const acknowledgedPromise = new Promise((resolve) => { resolveAcknowledged = resolve; });
  const location = frameLocation(frame);
  const dictionaries = decoded.columns
    .filter(column => column.dictionaryId !== null)
    .map(column => ({ field: column.name, id: column.dictionaryId, version: column.dictionaryVersion }));
  let batch = decoded.batch;
  const record = {
    session,
    key,
    digest,
    ordinal,
    replayed,
    start: frame.start,
    end: frame.end,
    metadataStart: location.metadataStart,
    metadataEnd: location.metadataEnd,
    bodyStart: location.bodyStart,
    bodyEnd: location.bodyEnd,
    metadataLength: location.metadataLength,
    bodyLength: location.bodyLength,
    legacy: location.legacy,
    rows: decoded.rows,
    columns: decoded.columns,
    dictionaries,
    get batch() { return batch; },
    get contentReleased() { return batch === null; },
    snapshot,
    acknowledged: false,
    acknowledgedAt: null,
    acknowledgedBy: null,
    released: false,
    acknowledgedPromise,
    resolveAcknowledged,
    /** 行值只在批次仍在手时可用；确认后内容释放，返回 null。 */
    preview(limit = Number.POSITIVE_INFINITY) {
      return batch === null ? null : rowsToJson(batch, limit);
    },
    /** 确认或丢弃后释放 Arrow 数据与字典引用，接收端不保留已消费批次。 */
    releaseContent() {
      batch = null;
      record.snapshot = null;
    },
    acknowledge(options = {}) { return acknowledge(options); },
  };
  return record;
}

function toIterator(source) {
  if (!source) throw new IntakeError('BAD_SOURCE', '接收来源为空');
  if (typeof source[Symbol.asyncIterator] === 'function') return source[Symbol.asyncIterator]();
  if (typeof source[Symbol.iterator] === 'function') return source[Symbol.iterator]();
  throw new IntakeError('BAD_SOURCE', '接收来源不是可迭代的字节来源');
}

export { IpcFrameParser, FrameError, DictionaryCatalog, DecodeError };
export default StreamIntake;
