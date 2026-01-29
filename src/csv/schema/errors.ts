import { z } from 'zod';

export class TgtfsParsingError extends Error {
  tableName: string;
  override cause: z.ZodError;
  obj: unknown;
  constructor(tableName: string, err: z.ZodError, obj: unknown) {
    super(
      [
        `adding an entity to table ${tableName} failed with the following ${
          err.errors.length > 1 ? 'errors' : 'error'
        }:`,
        err.errors.map((e) => e.message).join('\n'),
        '',
        'for the following object:',
        JSON.stringify(obj, undefined, 2),
      ].join('\n'),
    );
    this.name = this.constructor.name;
    this.tableName = tableName;
    this.cause = err;
    this.obj = obj;
  }
}
