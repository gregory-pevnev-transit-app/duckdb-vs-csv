import { promisify } from 'util';
import * as stream from 'stream';
import { once } from 'events';
import z from 'zod';
import fs from 'fs-extra';

const finished = promisify(stream.finished);

export async function exportTable<S extends z.ZodRawShape>(
  path: string,
  entries: Iterable<z.output<z.ZodObject<S>>>,
  converter: (entry: z.output<z.ZodObject<S>>) => string,
  header?: string,
): Promise<void> {
  const tempPath = `${path}.temp`;
  const writable = fs.createWriteStream(tempPath);
  if (header) {
    const writeResult = writable.write(`${header}\n`);
    if (!writeResult) {
      await once(writable, 'drain');
    }
  }
  for (const entry of entries) {
    if (!writable.write(`${converter(entry)}\n`)) {
      await once(writable, 'drain');
    }
  }
  writable.end();
  await finished(writable);
  await fs.move(tempPath, path, { overwrite: true });
}

/**
 * extremely rudimentary function to output a valid csv row
 * handles cases that papaparse can't, such as array fields (such as for trips.arrival_times)
 * escapes quotes with "", as per the spec
 * if the string contains a comma or a quote, wraps the field in quotes and escapes the quotes with an extra quote
 * as per https://datatracker.ietf.org/doc/html/rfc4180
 */
export function writeCsvRow(row: unknown[]) {
  return row
    .map((entry) => {
      if (Array.isArray(entry)) {
        return `"${JSON.stringify(entry)}"`;
      }
      if (typeof entry === 'string') {
        return escapeCsvSpecialChars(entry);
      }
      if (entry == null) {
        return '';
      }
      if (typeof entry === 'number') {
        return entry;
      }
      throw new Error('Unexpected value type in CSV row to output');
    })
    .join(',');
}

export function writeCsvRows(rows: unknown[][]) {
  return rows.map((row) => writeCsvRow(row)).join('\n');
}

function escapeCsvSpecialChars(string: string) {
  return /[",\n]/.test(string) ? `"${string.replaceAll('"', '""')}"` : string;
}
