export function compositeKey(keys: (string | null | number)[]) {
  // note that if a key is null, this will result in being an empty string
  // this is safe, because tgtfs coalesces empty string to null

  // this function should additionally be moot when the record/tuple proposal
  // makes it into js https://github.com/tc39/proposal-record-tuple
  return keys.join('␟');
}

export function assertNever(value: never): never {
  throw new Error(`Unhandled type: ${JSON.stringify(value)}`);
}

export function coalesceNullToUndefined(obj: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, v === null || v === '' ? undefined : v]));
}
