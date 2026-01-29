export function toMap(value: string | Map<unknown, unknown> | null | undefined) {
  if (value === '' || value === null || value === undefined) {
    return new Map();
  }

  if (value instanceof Map) {
    return value;
  }

  return new Map(JSON.parse(value));
}

export function toJsonObjectFromString(value: unknown) {
  if (value === '' || value === null || value === undefined) {
    return {};
  }

  if (value instanceof Object) {
    return value;
  }

  return JSON.parse(value as string);
}
