export const JSON_TREE_PAGE_SIZE = 100;

export function nodeChildCount(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  if (typeof value === "object" && value !== null) return Object.keys(value).length;
  return 0;
}

export function visibleChildEntries(
  value: Record<string, unknown> | unknown[],
  limit: number,
  offset = 0,
): Array<[string, unknown]> {
  const boundedLimit = Math.max(0, Math.floor(limit));
  const boundedOffset = Math.max(0, Math.floor(offset));
  if (Array.isArray(value)) {
    return value
      .slice(boundedOffset, boundedOffset + boundedLimit)
      .map((child, index) => [String(boundedOffset + index), child]);
  }
  return Object.keys(value)
    .slice(boundedOffset, boundedOffset + boundedLimit)
    .map((key) => [key, value[key]]);
}
