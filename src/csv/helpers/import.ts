import fs from 'fs-extra';
import { type CsvParseState, parseChunk } from './parse-csv.ts';

export async function importFromCsv(
  path: string,
  addEntity: (preEntity: Record<string, unknown>) => void,
  transformHeader?: (header: string) => string,
): Promise<void> {
  if (!(await fs.pathExists(path))) {
    return;
  }
  let parseResult: { state: CsvParseState; remainder: string } | undefined = undefined;
  const csvParseOpts = { ...(transformHeader ? { transformHeader } : {}) };

  const readStream = fs.createReadStream(path, { encoding: 'utf-8' });
  for await (const chunk of readStream) {
    const result = parseChunk(
      [parseResult?.remainder ?? '', chunk].join(''),
      addEntity,
      parseResult?.state,
      csvParseOpts,
    );
    if (result === -1) return;
    parseResult = result;
  }
  // ensure we don't omit the last line of a csv if the file doesn't have a trailing carriage return
  // (since the parser recognizes a line only when a carriage return/newline character is met at the end.)
  // if there is a trailing newline, this won't hurt anyways since the parser just eats newlines.
  parseChunk([parseResult?.remainder ?? '', '\n'].join(''), addEntity, parseResult?.state, csvParseOpts);
}
