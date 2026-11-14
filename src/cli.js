import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Archive } from './archive.js';
import { inspect, sample } from './domain.js';
import { runAcceptanceDemo } from './demo.js';

const [command = 'help', file, directory = '.local/archive'] = process.argv.slice(2);

function print(title, value) {
  console.log(`\n== ${title} ==`);
  console.log(JSON.stringify(value, null, 2));
}

async function run() {
  if (command === 'help') {
    console.log(JSON.stringify({ usage: 'npm start -- <命令> [资料路径] [归档目录]',
      commands: ['inspect', 'capture', 'list', 'demo'] }, null, 2));
  } else if (command === 'inspect') {
    if (!file) throw new Error('需要资料路径');
    console.log(JSON.stringify(await inspect(resolve(file)), null, 2));
  } else if (command === 'capture') {
    if (!file) throw new Error('需要资料路径');
    const inventory = await inspect(resolve(file));
    const receipt = await new Archive(directory).accept(await readFile(file),
      { source: resolve(file), inventory });
    console.log(JSON.stringify(receipt, null, 2));
  } else if (command === 'list') {
    console.log(JSON.stringify(await new Archive(file || directory).receipts(), null, 2));
  } else if (command === 'demo') {
    await demo();
  } else {
    throw new Error('支持 inspect、capture、list、demo');
  }
}

async function demo() {
  const temporary = await mkdtemp(join(tmpdir(), 'intake-'));
  try {
    print('既有资料查看与字节归档', await legacyCheck(temporary));
    const result = await runAcceptanceDemo();
    print('来源帧位置（由生成器给出）', result.plannedFrames);
    print('下游停住时的接收端状态', result.stoppedConsumer.stall);
    print('背压与恢复', {
      backpressure: result.stoppedConsumer.backpressure,
      resumed: result.stoppedConsumer.resumed,
    });
    print('每个批次的来源位置与所用字典版本', result.dictionaryUsage.batches.map(batch => ({
      ordinal: batch.ordinal,
      sourceRange: batch.sourceRange,
      metadataRange: batch.metadataRange,
      bodyRange: batch.bodyRange,
      dictionaries: batch.dictionaries,
      rows: batch.values.length,
      values: batch.values,
    })));
    print('字典版本时间线', result.dictionaryUsage.timeline);
    print('字典保留情况', result.dictionaryUsage.retention);
    print('断线前已确认批次', result.replay.confirmedBeforeDrop);
    print('断线时被释放的未确认批次', result.replay.releasedBeforeDrop);
    print('断线时仍持有的未确认批次行值（不会静默跳过）', result.replay.heldValuesBeforeDrop);
    print('断线时的账本（只含已确认批次）', {
      entries: result.replay.ledgerEntriesAfterDrop,
      keys: result.replay.ledgerKeysAfterDrop,
    });
    print('恢复位置提示', result.replay.resumeHint);
    print('重播时跳过的已确认批次', result.replay.skippedConfirmed);
    print('重播时重新发布的批次', result.replay.replayedBatches);
    print('恢复账本', {
      entries: result.replay.ledgerEntries,
      confirmedThrough: result.replay.ledgerConfirmedThrough,
    });
    const checks = acceptanceChecks(result);
    print('验收结论', checks);
    if (checks.some(check => !check.pass)) process.exitCode = 1;
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

async function legacyCheck(temporary) {
  const inventory = await inspect(resolve(sample));
  const archive = new Archive(temporary);
  const receipt = await archive.accept(await readFile(sample), { source: sample, inventory });
  return { rows: inventory.rows, columns: inventory.columns,
    sha256: receipt.sha256, verifiedBytes: (await archive.read(receipt.sha256)).length };
}

function acceptanceChecks(result) {
  const batches = result.dictionaryUsage.batches;
  const strain = result.stoppedConsumer;
  const replay = result.replay;
  const first = batches[0];
  const second = batches[1];
  const third = batches[2];
  return [
    { name: '流被拆成真实小块后仍逐批发布，而不是最后一次性读出',
      pass: strain.published.length === 3 && result.plannedFrames.filter(frame => frame.kind === 'recordBatch').length === 3,
      got: { batches: strain.published.map(batch => batch.ordinal) } },
    { name: '每个批次记录独立的来源元数据段与正文段',
      pass: batches.every(batch => batch.metadataRange[1] > batch.metadataRange[0] && batch.bodyRange[1] > batch.bodyRange[0]),
      got: batches.map(batch => ({ ordinal: batch.ordinal, source: batch.sourceRange })) },
    { name: '两个字段引用同一字典，且批次记录所用版本',
      pass: first.dictionaries.length === 2
        && first.dictionaries[0].id === first.dictionaries[1].id
        && first.dictionaries[0].version === first.dictionaries[1].version,
      got: first.dictionaries },
    { name: '字典增量与整份替换各产生新版本，批次各自绑定当时版本',
      pass: second.dictionaries[0].version === 2 && third.dictionaries[0].version === 3
        && first.values[0].category === '合格' && second.values[0].category === '待定'
        && third.values[0].category === '通过',
      got: { versions: [first, second, third].map(batch => batch.dictionaries[0]?.version),
        sampleValues: [first, second, third].map(batch => batch.values[0].category) } },
    { name: '空数组与空值在同一列共存且没有互相吞掉',
      pass: first.values.some(row => Array.isArray(row.tags) && row.tags.length === 0)
        && first.values.some(row => row.tags === null),
      got: first.values.map(row => ({ tags: row.tags })) },
    { name: '下游停住时来源字节不再被接收',
      pass: strain.stall.sourcePaused === true
        && strain.stall.deliveredAfterPause === strain.stall.deliveredAtStall,
      got: strain.stall },
    { name: '恢复后继续拉取并确认后续批次',
      pass: strain.published.length === 3
        && strain.published.every(batch => batch.dictionaries.length > 0),
      got: strain.published },
    { name: '断线时未确认批次被释放，不进入账本',
      pass: replay.releasedBeforeDrop.length > 0
        && replay.ledgerEntriesAfterDrop === replay.confirmedBeforeDrop.length,
      got: { released: replay.releasedBeforeDrop.length,
        ledgerAfterDrop: replay.ledgerEntriesAfterDrop,
        confirmed: replay.confirmedBeforeDrop.length } },
    { name: '重播从 0 开始且跳过已确认批次',
      pass: replay.resumeHint.partnerReplayFrom === 0
        && replay.skippedConfirmed.length === replay.confirmedBeforeDrop.length,
      got: { replayFrom: replay.resumeHint.partnerReplayFrom,
        skipped: replay.skippedConfirmed.length, confirmed: replay.confirmedBeforeDrop.length } },
    { name: '重播重新发布未确认批次，不静默跳过',
      pass: replay.replayedBatches.length === replay.releasedBeforeDrop.length,
      got: { republished: replay.replayedBatches.map(batch => batch.ordinal),
        released: replay.releasedBeforeDrop.map(batch => batch.ordinal) } },
  ];
}

run().catch(error => { console.error(error.message); process.exitCode = 1; });
