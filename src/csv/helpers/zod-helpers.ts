import { z } from 'zod';

export const requiredString = z.string().min(1);
export const optionalString = z.preprocess(
  (str) => (str === '' || str === null ? undefined : str),
  z.string().min(1).optional(),
);
export const requiredRegexString = (re: RegExp, msg?: string) =>
  msg ? z.string().min(1).regex(re, { message: msg }) : z.string().min(1).regex(re);
export const optionalRegexString = (re: RegExp, msg?: string) =>
  msg
    ? z.preprocess((str) => (str === '' ? undefined : str), z.string().min(1).regex(re, { message: msg })).optional()
    : z.preprocess((str) => (str === '' ? undefined : str), z.string().min(1).regex(re).optional());

export const numberPreprocess = (num: unknown) => {
  switch (typeof num) {
    case 'number':
      return num;
    case 'string':
      if (Number.isNaN(Number(num))) throw new Error(`cannot parse number from string ${num}`);
      return Number(num);
    case 'undefined':
      return undefined;
    default:
      throw new Error(`cannot parse number from type ${typeof num}: ${JSON.stringify(num)} passed`);
  }
};
export const optionalNumber = (schema: z.ZodNumber) => {
  return z.preprocess(numberPreprocess, schema.optional());
};
export const requiredNumber = (schema: z.ZodNumber) => {
  return z.preprocess(numberPreprocess, schema);
};

export const regexMessageBuilder = (re: RegExp) => `must match regex ${re.toString()}`;
