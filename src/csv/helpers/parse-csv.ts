import { Readable } from 'stream';
import fs from 'fs-extra';

export type CsvParseState = {
  readingState: 'text' | 'escaped';
  header: string[];
  columns: string[];
  text: string;
};

type CsvRowParser = (row: Record<string, string | undefined>) => void | -1;

/**
 * parse a csv file. return -1 from onRow to stop parsing.
 */
export async function parseCsv(
  pathOrStream: string | Readable,
  onRow: (row: Record<string, string | undefined>) => void,
) {
  let parseResult: { state: CsvParseState; remainder: string } | undefined = undefined;
  const readStream =
    typeof pathOrStream === 'string' ? fs.createReadStream(pathOrStream, { encoding: 'utf-8' }) : pathOrStream;
  for await (const chunk of readStream) {
    const result = parseChunk([parseResult?.remainder ?? '', chunk].join(''), onRow, parseResult?.state);
    if (result === -1) return;
    parseResult = result;
  }
  // ensure we don't omit the last line of a csv if the file doesn't have a trailing carriage return
  // (since the parser recognizes a line only when a carriage return/newline character is met at the end.)
  // if there is a trailing newline, this won't hurt anyways since the parser just eats newlines.
  parseChunk([parseResult?.remainder ?? '', '\n'].join(''), onRow, parseResult?.state);
}

/**
 * parse a string chunk as csv
 * @mutates state
 */
export function parseChunk(
  csv: string,
  parser: CsvRowParser,
  state: CsvParseState = {
    readingState: 'text',
    header: [],
    columns: [],
    text: '',
  },
  options: {
    transformHeader?: (header: string) => string;
  } = {},
) {
  let i = 0;
  const lastIndex = Math.max(csv.lastIndexOf('\n'), csv.lastIndexOf(','));
  while (i <= lastIndex) {
    const nextCommaIndex = csv.indexOf(',', i);
    const nextNewlineIndex = csv.indexOf('\n', i);
    if (nextCommaIndex >= 0 && (nextCommaIndex < nextNewlineIndex || nextNewlineIndex === -1)) {
      const substr = csv.slice(i, nextCommaIndex);
      const quoted = substr.includes('"');
      if (state.readingState !== 'escaped') {
        if (!quoted) {
          state.columns.push(substr.trim());
        } else if (substr.trimStart().startsWith('"')) {
          if (isFullQuote(substr.trimEnd())) {
            state.columns.push(substr.trim().slice(1, -1).replaceAll('""', '"'));
          } else {
            state.text += `${substr.trimStart().slice(1)},`;
            state.readingState = 'escaped';
          }
        } else {
          throw new Error('Unexpected quote in a non-escaped field');
        }
      } else if (hasOddNumberEndQuotes(substr.trimEnd())) {
        state.columns.push((state.text + substr.trimEnd().slice(0, -1)).replaceAll('""', '"'));
        state.readingState = 'text';
        state.text = '';
      } else {
        state.text += `${substr},`;
      }
      i = nextCommaIndex + 1;
    } else if (nextNewlineIndex >= 0) {
      const substr = csv.slice(i, nextNewlineIndex);
      const quoted = substr.includes('"');
      if (state.readingState !== 'escaped') {
        // skip empty lines
        if (!quoted && (state.columns.length > 0 || substr.trim() !== '')) {
          state.columns.push(substr.trim());
          if (state.header.length === 0) {
            state.header = options.transformHeader ? state.columns.map(options.transformHeader) : state.columns;
          } else if (parser(zipRow(state.header, state.columns)) === -1) {
            return -1;
          }
          state.columns = [];
        } else if (substr.trimStart().startsWith('"')) {
          if (isFullQuote(substr.trimEnd())) {
            state.columns.push(substr.trim().slice(1, -1).replaceAll('""', '"'));
            if (state.header.length === 0) {
              state.header = options.transformHeader ? state.columns.map(options.transformHeader) : state.columns;
            } else if (parser(zipRow(state.header, state.columns)) === -1) {
              return -1;
            }
            state.columns = [];
          } else {
            state.text += `${substr.trimStart().slice(1)}\n`;
            state.readingState = 'escaped';
          }
        } else if (!substr.trimStart().startsWith('"') && quoted) {
          throw new Error('Unexpected quote in a non-escaped field');
        }
      } else if (hasOddNumberEndQuotes(substr.trimEnd())) {
        state.columns.push((state.text + substr.trimEnd().slice(0, -1)).replaceAll('""', '"'));
        if (state.header.length === 0) {
          state.header = options.transformHeader ? state.columns.map(options.transformHeader) : state.columns;
        } else if (parser(zipRow(state.header, state.columns)) === -1) {
          return -1;
        }
        state.readingState = 'text';
        state.text = '';
        state.columns = [];
      } else {
        state.text += `${substr}\n`;
      }
      i = nextNewlineIndex + 1;
    }
  }

  return { state, remainder: csv.slice(lastIndex + 1) };
}

const isFullQuote = (str: string) => {
  let i = str.length - 1;
  let quoteCount = 0;
  for (i; i >= 0; i--) {
    if (str[i] === '"') quoteCount += 1;
    else break;
  }
  return i === -1 ? quoteCount % 2 === 0 : quoteCount % 2 === 1;
};

const hasOddNumberEndQuotes = (str: string) => {
  let i = str.length - 1;
  let quoteCount = 0;
  for (i; i >= 0; i--) {
    if (str[i] === '"') quoteCount += 1;
    else break;
  }
  return quoteCount % 2 === 1;
};

const zipRow = (header: string[], row: (string | undefined)[]) => {
  return Object.fromEntries(header.map((fieldName, i) => [fieldName, row.at(i) || undefined]));
};
