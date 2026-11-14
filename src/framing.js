import { Message } from 'apache-arrow';

export class FrameError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'FrameError';
    this.code = code;
    this.details = details;
  }
}

const KIND_BY_HEADER = new Map([
  [1, 'schema'],
  [2, 'dictionary'],
  [3, 'recordBatch'],
]);

const int32At = (bytes, offset = 0) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getInt32(offset, true);

function toBytes(chunk) {
  if (chunk instanceof Uint8Array) return chunk;
  if (ArrayBuffer.isView(chunk)) {
    return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  throw new TypeError('字节来源必须是 Uint8Array、Buffer 或 ArrayBuffer');
}

/**
 * Arrow IPC stream 增量帧解析器。
 *
 * 偏移一律相对流起点，单位字节：
 *   start         帧首（含长度前缀）
 *   metadataStart 长度前缀之后
 *   metadataEnd   元数据结束，即正文起点
 *   bodyStart     正文首字节
 *   end           正文之后，下一帧开始
 *
 * 现代格式：前缀为 4 字节续接标记 -1 加 4 字节元数据长度；元数据长度和正文长度都必须
 * 为 8 的倍数。尾标记为 -1 后跟长度 0。旧格式：4 字节直接给元数据长度，长度 0 即尾标记，
 * 不强制对齐。两种格式都接受，但一帧里不能混用。
 *
 * `next()` 只在整帧到齐后交出一帧；未到齐的部分留在内部，且只保留当前这个未完成帧的字节。
 */
export class IpcFrameParser {
  #chunks = [];
  #buffered = 0;
  #position = 0;
  #pending = null;
  #ended = false;
  #finished = false;
  #maxMetadataBytes;
  #maxBodyBytes;

  constructor({ maxMetadataBytes = 16 * 1024 * 1024, maxBodyBytes = 128 * 1024 * 1024 } = {}) {
    this.#maxMetadataBytes = maxMetadataBytes;
    this.#maxBodyBytes = maxBodyBytes;
  }

  get position() { return this.#position; }
  get bufferedBytes() { return this.#buffered; }
  get ended() { return this.#ended; }

  append(chunk) {
    const bytes = toBytes(chunk);
    if (bytes.byteLength === 0) return this;
    if (this.#ended) {
      throw new FrameError('BYTES_AFTER_END', '尾标记之后仍收到字节', {
        position: this.#position, extraBytes: bytes.byteLength,
      });
    }
    this.#chunks.push(bytes);
    this.#buffered += bytes.byteLength;
    return this;
  }

  /** 返回完整帧；字节不足一帧时返回 null。 */
  next() {
    if (this.#ended) return null;

    if (this.#pending === null) {
      const prefix = this.#readPrefix();
      if (prefix === null) return null;
      if (prefix.endFrame) return prefix.endFrame;
      this.#pending = prefix;
    }

    const pending = this.#pending;
    const metadataEnd = pending.metadataStart + pending.metadataLength;
    if (pending.message === null) {
      if (this.#buffered < pending.prefixBytes + pending.metadataLength) return null;
      const metadata = this.#copyRange(pending.prefixBytes, pending.metadataLength);
      let message;
      try {
        message = Message.decode(metadata);
      } catch (error) {
        throw new FrameError('BAD_METADATA', `元数据无法解码: ${error.message}`, {
          position: pending.start, metadataLength: pending.metadataLength,
        });
      }
      const kind = KIND_BY_HEADER.get(message.headerType);
      if (!kind) {
        throw new FrameError('UNSUPPORTED_MESSAGE', '流中出现不支持的 IPC 消息类型', {
          position: pending.start, headerType: message.headerType,
        });
      }
      const bodyLength = message.bodyLength;
      if (!Number.isSafeInteger(bodyLength) || bodyLength < 0) {
        throw new FrameError('BAD_LENGTH', '正文长度无效', {
          position: pending.start, bodyLength,
        });
      }
      if (bodyLength > this.#maxBodyBytes) {
        throw new FrameError('BODY_LIMIT', '正文长度超过接收上限', {
          position: pending.start, bodyLength, limit: this.#maxBodyBytes,
        });
      }
      if (!pending.legacy) {
        if (pending.metadataLength % 8 !== 0) {
          throw new FrameError('BAD_ALIGNMENT', '元数据长度未按 8 字节对齐', {
            position: pending.start, metadataLength: pending.metadataLength,
          });
        }
        if (bodyLength % 8 !== 0) {
          throw new FrameError('BAD_ALIGNMENT', '正文长度未按 8 字节对齐', {
            position: pending.start, bodyLength,
          });
        }
      }
      pending.message = message;
      pending.kind = kind;
      pending.bodyLength = bodyLength;
      pending.metadataBytes = metadata;
      pending.metadataEnd = metadataEnd;
      pending.bodyStart = metadataEnd;
      pending.end = metadataEnd + bodyLength;
    }

    const total = pending.prefixBytes + pending.metadataLength + pending.bodyLength;
    if (this.#buffered < total) return null;

    const body = this.#copyRange(pending.prefixBytes + pending.metadataLength, pending.bodyLength);
    this.#drop(total);
    this.#pending = null;
    this.#position = pending.end;

    return {
      kind: pending.kind,
      start: pending.start,
      end: pending.end,
      metadataStart: pending.metadataStart,
      metadataEnd: pending.metadataEnd,
      bodyStart: pending.bodyStart,
      prefixBytes: pending.prefixBytes,
      metadataLength: pending.metadataLength,
      bodyLength: pending.bodyLength,
      legacy: pending.legacy,
      message: pending.message,
      metadataBytes: pending.metadataBytes,
      body,
    };
  }

  /** 来源正常结束：必须恰好停在尾标记之后。 */
  finish() {
    if (this.#finished) return;
    this.#finished = true;
    if (!this.#ended) {
      throw new FrameError('MISSING_END', '来源在完整尾标记之前结束', {
        position: this.#position,
        bufferedBytes: this.#buffered,
        incompleteFrame: this.#pending !== null,
      });
    }
  }

  /** 尾标记必须把缓冲用尽，之后不允许再有内容。 */
  #readPrefix() {
    if (this.#buffered < 4) return null;
    const start = this.#position;
    const first = int32At(this.#peek(4));
    if (first === -1) {
      if (this.#buffered < 8) return null;
      const second = int32At(this.#peek(8), 4);
      if (second === 0) return { endFrame: this.#consumeEnd(8, false) };
      return this.#pendingFor(start, 8, second, false);
    }
    if (first === 0) return { endFrame: this.#consumeEnd(4, true) };
    return this.#pendingFor(start, 4, first, true);
  }

  #pendingFor(start, prefixBytes, metadataLength, legacy) {
    if (metadataLength < 0) {
      throw new FrameError('BAD_LENGTH', '元数据长度为负', {
        position: start, metadataLength,
      });
    }
    if (metadataLength > this.#maxMetadataBytes) {
      throw new FrameError('METADATA_LIMIT', '元数据长度超过接收上限', {
        position: start, metadataLength, limit: this.#maxMetadataBytes,
      });
    }
    const metadataStart = start + prefixBytes;
    const bodyStart = metadataStart + metadataLength;
    return {
      start, prefixBytes, metadataStart, metadataLength, legacy,
      message: null, kind: null, bodyLength: 0,
      metadataBytes: null, metadataEnd: bodyStart,
      bodyStart, end: bodyStart,
    };
  }

  #consumeEnd(prefixBytes, legacy) {
    const start = this.#position;
    const end = start + prefixBytes;
    this.#drop(prefixBytes);
    if (this.#buffered > 0) {
      throw new FrameError('BYTES_AFTER_END', '尾标记之后仍有字节', {
        position: end, extraBytes: this.#buffered,
      });
    }
    this.#ended = true;
    this.#position = end;
    this.#pending = null;
    return {
      kind: 'end', start, end,
      metadataStart: end, metadataEnd: end, bodyStart: end,
      prefixBytes, metadataLength: 0, bodyLength: 0, legacy,
      message: null, metadataBytes: new Uint8Array(0), body: new Uint8Array(0),
    };
  }

  #peek(size) {
    const first = this.#chunks[0];
    if (first && first.byteLength >= size) return first.subarray(0, size);
    const out = new Uint8Array(size);
    let filled = 0;
    for (const chunk of this.#chunks) {
      const take = Math.min(chunk.byteLength, size - filled);
      out.set(chunk.subarray(0, take), filled);
      filled += take;
      if (filled === size) break;
    }
    return out;
  }

  /** 从 offset 起复制 length 字节，返回的数组不再引用内部缓冲。 */
  #copyRange(offset, length) {
    const out = new Uint8Array(length);
    if (length === 0) return out;
    let skip = offset;
    let filled = 0;
    for (const chunk of this.#chunks) {
      if (skip > 0) {
        if (skip >= chunk.byteLength) { skip -= chunk.byteLength; continue; }
        const from = skip;
        skip = 0;
        const take = Math.min(chunk.byteLength - from, length - filled);
        out.set(chunk.subarray(from, from + take), filled);
        filled += take;
      } else {
        const take = Math.min(chunk.byteLength, length - filled);
        out.set(chunk.subarray(0, take), filled);
        filled += take;
      }
      if (length === 0 || filled === length) break;
    }
    return out;
  }

  #drop(size) {
    let remaining = size;
    while (remaining > 0) {
      const chunk = this.#chunks[0];
      if (chunk.byteLength <= remaining) {
        this.#chunks.shift();
        remaining -= chunk.byteLength;
      } else {
        this.#chunks[0] = chunk.subarray(remaining);
        remaining = 0;
      }
    }
    this.#buffered -= size;
  }
}

export default IpcFrameParser;
