import { readFile } from 'node:fs/promises';
import { tableFromIPC } from 'apache-arrow';
export const sample = 'fixtures/observations.arrow';
export async function inspect(filename) {
  const table = tableFromIPC(await readFile(filename));
  return { rows: table.numRows, columns: table.schema.fields.map(field =>
    ({ name: field.name, type: String(field.type), nullable: field.nullable })),
    preview: Array.from(table).slice(0, 4).map(row => row.toJSON()) };
}
