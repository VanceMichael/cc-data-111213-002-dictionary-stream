import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { inspect, sample } from '../src/domain.js';

test('既有资料查看与本地来源符合领域约定', async () => {
  const result = await inspect(sample);
  assert.equal(result.rows, 4);
  assert.equal(result.preview[0].category, '合格');
  assert.equal(result.preview[3].category, null);
  assert.match(result.columns[0].type, /Dictionary/);
});
