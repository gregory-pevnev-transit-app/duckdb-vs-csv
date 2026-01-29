/**
 * Return a time in GTFS time format as the number of seconds past midnight.
 * Preferable for storage efficiency, data validation, and COM loading.
 * @param {unknown} date
 * @return {number}
 */
export function gtfsTimePreprocess(gtfsTime: unknown): number {
  if (Number.isInteger(Number(gtfsTime))) return Number(gtfsTime);
  const matches = typeof gtfsTime === 'string' && gtfsTime.match(GTFS_TIME_REGEXP);
  if (!matches) {
    throw new Error(`Invalid time in GTFS: ${gtfsTime}`);
  }
  return Number(matches[1]) * 60 * 60 + Number(matches[2]) * 60 + Number(matches[3]);
}

export function gtfsTimeTransform(gtfsTime: string | null | undefined) {
  if (gtfsTime == null) return undefined;
  return gtfsTimePreprocess(gtfsTime);
}

export function gtfsTimeReverseTransform(gtfsTime: number | null | undefined) {
  if (gtfsTime == null) return null;
  const h = Math.floor(gtfsTime / 3600);
  const m = Math.floor((gtfsTime / 60) % 60);
  const s = Math.floor(gtfsTime % 60);

  const hString = h >= 10 ? String(h) : `0${String(h)}`;
  const mString = m >= 10 ? String(m) : `0${String(m)}`;
  const sString = s >= 10 ? String(s) : `0${String(s)}`;

  return `${hString}:${mString}:${sString}`;
}

export const CURRENCY_REGEXP = /^[A-Z]{3}$/;

export const NON_DEFAULT_SUPPORTED_LANGUAGES = ['fr', 'es', 'de', 'pt', 'it', 'nl', 'ro'] as const;

export const DEFAULT_LANGUAGE = 'en';

export const SUPPORTED_LANGUAGES = [...NON_DEFAULT_SUPPORTED_LANGUAGES, DEFAULT_LANGUAGE] as const;
export type SupportedLanguage = (typeof SUPPORTED_LANGUAGES)[number];

export const TGTFS_IMAGE_REGEX = /^[a-z0-9-]+$/;
export const TGTFS_IMAGE_ESCAPED_REGEX = /^<[a-z0-9-]+>$/;

export const GTFS_TIME_REGEXP = /^(\d{1,2}):([012345]\d):([012345]\d)$/;

export const DATE_AS_STRING_WITH_DASHES_REGEXP = /^\d\d\d\d-(0[123456789]|1[012])-(0[123456789]|[12]\d|3[01])$/;

export const DATETIME_AS_STRING_REGEXP =
  /^\d\d\d\d-(0[123456789]|1[012])-(0[123456789]|[12]\d|3[01]) ([01]\d|2[0123]):([012345]\d):([012345]\d)$/;

export const VERSION_NUMBER_REGEXP = /^\d+\.\d+.\d+$/;

export const VERSION = {
  MAJOR: 7,
  MINOR: 0,
  PATCH: 0,
};

export const TgtfsTableName = {
  TRIPS: 'trips',
} as const;
export type TgtfsTableName = (typeof TgtfsTableName)[keyof typeof TgtfsTableName];

/**
 * these are the tables that can be referenced by other tables via foreign key constraints.
 */
export type ForeignKeyTable =
  | typeof TgtfsTableName.TRIPS;

export const TGTFS_FILE_NAMES: Record<TgtfsTableName, string> = {
  [TgtfsTableName.TRIPS]: 'trips.txt',
};

export const TDSN_CHAR_LIMIT = 5;
