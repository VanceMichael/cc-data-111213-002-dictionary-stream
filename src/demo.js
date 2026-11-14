import { readFile } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { StreamIntake, RecoveryLedger, IntakeError } from './stream.js';

/** 把一个完整字节串按给定切点拆成异步块；切点可以落在任何字节位置。 */
export async function* chunkedSource(bytes, cuts = [], { delayMs = 0 } = {}) {
  const points = [...new Set(cuts.filter(cut => cut > 0 && cut < bytes.length))].sort((a, b) => a - b);
  let previous = 0;
  const boundaries = [...points, bytes.length];
  for (const boundary of boundaries) {
    if (delayMs > 0) await sleep(delayMs);
    yield Buffer.from(bytes.subarray(previous, boundary));
    previous = boundary;
  }
}

/** 包一层来源，只记录它实际交出的字节数；背压时这个数字应当停住。 */
export function trackedSource(bytes, cuts, state, options) {
  const inner = chunkedSource(bytes, cuts, options);
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          const result = await inner.next();
          if (!result.done) {
            state.deliveredBytes += result.value.byteLength;
            state.chunks += 1;
          }
          return result;
        },
      };
    },
  };
}

function dictionaryTimeline(events) {
  return events
    .filter(event => event.kind === 'dictionary')
    .map(event => ({
      sourceRange: [event.start, event.end],
      id: event.id,
      version: event.version,
      change: event.change,
      entries: event.entries,
      supersededPrior: event.superseded,
    }));
}

/**
 * 演示一：字典演进 + 空数组/空值 + 下游故意停住。
 *
 * 下游只确认第一批；接收端达到在途上限后停止向来源要字节，
 * 直到下游确认，才继续拉取并发布后续批次。
 */
export async function runStalledConsumerDemo(bytes, plan, { maxInflightBatches = 1, pauseMs = 40 } = {}) {
  const state = { deliveredBytes: 0, chunks: 0 };
  let allowAcknowledge = false;
  let resolveFirstPublished;
  const firstPublished = new Promise((resolve) => { resolveFirstPublished = resolve; });
  // 业务方在批次到达时读取行值；确认后接收端会释放 Arrow 数据。
  const delivered = [];
  const records = new Map();
  const intake = new StreamIntake({
    maxInflightBatches,
    onRecord(record) {
      records.set(record.ordinal, record);
      delivered.push({
        ordinal: record.ordinal,
        sourceRange: [record.start, record.end],
        metadataRange: [record.metadataStart, record.metadataEnd],
        bodyRange: [record.bodyStart, record.bodyEnd],
        rows: record.rows,
        dictionaries: record.dictionaries,
        values: record.preview(),
      });
      if (record.ordinal === 0) {
        resolveFirstPublished(record);
        return;
      }
      if (allowAcknowledge) record.acknowledge({ by: 'stalled-consumer' });
    },
  });

  const run = intake.ingest(trackedSource(bytes, plan.cuts, state, { delayMs: 1 }));

  const firstRecord = await firstPublished;
  const deliveredAtStall = state.deliveredBytes;
  const positionAtStall = intake.position;
  await sleep(pauseMs);
  const duringStall = {
    deliveredAtStall,
    deliveredAfterPause: state.deliveredBytes,
    chunksAtStall: state.chunks,
    positionAtStall,
    positionAfterPause: intake.position,
    sourcePaused: state.deliveredBytes === deliveredAtStall,
    bufferedBytes: intake.bufferedBytes,
    pending: intake.unconfirmedCount,
  };

  firstRecord.acknowledge({ by: 'stalled-consumer' });
  allowAcknowledge = true;
  const summary = await run;
  await intake.waitForPending();
  const finalSummary = intake.summary();
  for (const batch of delivered) {
    batch.contentReleasedAfterAck = records.get(batch.ordinal).contentReleased;
  }

  return {
    intake,
    summary: finalSummary,
    stall: duringStall,
    firstBatch: firstRecord,
    delivered,
    backpressureEvents: finalSummary.events.filter(event => event.kind === 'backpressure'),
    resumedEvents: finalSummary.events.filter(event => event.kind === 'resumed'),
  };
}

/**
 * 演示二：断线后从 0 重播。
 *
 * 第一条连接只在消费方确认了前两批后断开；第三条批次仍挂在接收端。
 * 新连接从流起点重播：已确认的两批按来源键跳过，未确认的第三批重新发布。
 */
export async function runReplayDemo(bytes, plan, { maxInflightBatches = 8 } = {}) {
  const ledger = new RecoveryLedger();
  const firstConnection = [];
  let firstHeld;
  const first = new StreamIntake({
    ledger,
    maxInflightBatches,
    onRecord(record) {
      firstConnection.push({
        ordinal: record.ordinal,
        sourceRange: [record.start, record.end],
        rows: record.rows,
        dictionaries: record.dictionaries,
        values: record.preview(),
      });
      // 第三批故意不确认，模拟业务还没落库就断线。
      if (record.ordinal < 2) record.acknowledge({ by: 'first-connection' });
      else firstHeld = record;
    },
  });
  const firstSummary = await first.ingest(chunkedSource(bytes, plan.cuts, { delayMs: 1 }));
  const releasedBeforeDrop = first.published
    .filter(record => !record.acknowledged)
    .map(record => ({ ordinal: record.ordinal, start: record.start, end: record.end }));
  const heldValues = firstHeld?.preview() ?? null;
  const hintBeforeDrop = firstSummary.resumeHint;
  const ledgerAfterDrop = ledger.toJSON();
  first.abort('connection dropped');

  const secondConnection = [];
  const second = new StreamIntake({
    ledger,
    maxInflightBatches,
    onRecord(record) {
      secondConnection.push({
        ordinal: record.ordinal,
        sourceRange: [record.start, record.end],
        rows: record.rows,
        dictionaries: record.dictionaries,
        values: record.preview(),
      });
      record.acknowledge({ by: 'second-connection' });
    },
  });
  const secondSummary = await second.ingest(chunkedSource(bytes, plan.cuts, { delayMs: 1 }));

  return {
    first: {
      summary: firstSummary,
      published: firstConnection,
      confirmedBeforeDrop: firstSummary.batches
        .filter(batch => batch.acknowledged)
        .map(batch => ({ ordinal: batch.ordinal, sourceRange: [batch.start, batch.end] })),
      releasedBeforeDrop,
      heldValues,
      resumeHint: hintBeforeDrop,
      ledgerAfterDrop,
      ledgerEntriesAfterDrop: ledgerAfterDrop.entries.length,
      ledgerKeysAfterDrop: ledgerAfterDrop.entries.map(entry => entry.key),
    },
    second: {
      summary: secondSummary,
      published: secondConnection,
      publishedOrdinals: secondConnection.map(record => record.ordinal),
      republishedRanges: secondSummary.batches.map(batch => ({
        ordinal: batch.ordinal,
        sourceRange: [batch.start, batch.end],
        replayed: batch.replayed,
        dictionaries: batch.dictionaries,
      })),
      skippedConfirmed: secondSummary.events
        .filter(event => event.kind === 'replaySkip')
        .map(event => ({ key: event.key, sourceRange: [event.start, event.end], confirmedAt: event.confirmedAt })),
    },
    ledger: ledger.toJSON(),
  };
}

/** 演示三：同一字典被不同字段引用；批次按当时版本解释。 */
export function dictionaryReport(delivered, summary) {
  return {
    timeline: dictionaryTimeline(summary.events),
    retention: summary.dictionaryRetention,
    batches: delivered.map(batch => ({
      ordinal: batch.ordinal,
      sourceRange: batch.sourceRange,
      metadataRange: batch.metadataRange,
      bodyRange: batch.bodyRange,
      dictionaries: batch.dictionaries,
      values: batch.values,
    })),
  };
}

export async function runAcceptanceDemo({
  sample = 'fixtures/session.arrow',
  planPath = 'fixtures/stream-plan.json',
} = {}) {
  const bytes = await readFile(sample);
  const plan = JSON.parse(await readFile(planPath, 'utf8'));
  const stopped = await runStalledConsumerDemo(bytes, plan, {
    maxInflightBatches: plan.consumer?.maxInflightBatches ?? 1,
    pauseMs: 60,
  });
  const replay = await runReplayDemo(bytes, plan);
  return {
    source: sample,
    bytes: bytes.length,
    plannedFrames: plan.expected,
    stoppedConsumer: {
      stall: stopped.stall,
      published: stopped.delivered.map(batch => ({
        ordinal: batch.ordinal,
        sourceRange: batch.sourceRange,
        dictionaries: batch.dictionaries,
        contentReleasedAfterAck: batch.contentReleasedAfterAck,
      })),
      backpressure: stopped.backpressureEvents.map(event => ({
        atPosition: event.position,
        waitingFor: event.waitingFor,
        sourcePaused: event.sourcePaused,
      })),
      resumed: stopped.resumedEvents.map(event => ({ atPosition: event.position, pending: event.pending })),
      retention: stopped.summary.dictionaryRetention,
    },
    replay: {
      confirmedBeforeDrop: replay.first.confirmedBeforeDrop,
      releasedBeforeDrop: replay.first.releasedBeforeDrop,
      heldValuesBeforeDrop: replay.first.heldValues,
      resumeHint: replay.first.resumeHint,
      ledgerEntriesAfterDrop: replay.first.ledgerEntriesAfterDrop,
      ledgerKeysAfterDrop: replay.first.ledgerKeysAfterDrop,
      replayedBatches: replay.second.republishedRanges,
      skippedConfirmed: replay.second.skippedConfirmed,
      ledgerEntries: replay.ledger.entries.length,
      ledgerConfirmedThrough: replay.ledger.entries.at(-1)?.end ?? 0,
    },
    dictionaryUsage: dictionaryReport(stopped.delivered, stopped.summary),
  };
}

export { IntakeError };
