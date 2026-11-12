import { mkdir as ensureFixtureDirectory } from 'node:fs/promises';
await ensureFixtureDirectory('fixtures', {recursive:true});

import { writeFile } from 'node:fs/promises';
import { tableFromArrays, tableToIPC, vectorFromArray, Dictionary, Utf8, Int32 } from 'apache-arrow';
const category = vectorFromArray(['合格', '待复核', '合格', null], new Dictionary(new Utf8(), new Int32(), 7));
const table = tableFromArrays({ category, value: [1.5, null, 0, 9.2] });
await writeFile('fixtures/observations.arrow', tableToIPC(table, 'stream'));
await writeFile('fixtures/stream-plan.json', JSON.stringify({ cuts: [1, 3, 8, 21, 64],
  consumer: { pauseAfterBatch: 1 }, replayFrom: 0 }, null, 2));
