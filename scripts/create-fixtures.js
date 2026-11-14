import { mkdir, writeFile } from 'node:fs/promises';
import { buildSessionStream, writeFixturePlan } from '../src/session-stream.js';

await mkdir('fixtures', { recursive: true });
const session = buildSessionStream();
await writeFile('fixtures/session.arrow', session.bytes);
await writeFile('fixtures/stream-plan.json', `${JSON.stringify(writeFixturePlan(session), null, 2)}\n`);
console.log(JSON.stringify({
  stream: 'fixtures/session.arrow',
  bytes: session.bytes.length,
  frames: session.frames,
}, null, 2));
