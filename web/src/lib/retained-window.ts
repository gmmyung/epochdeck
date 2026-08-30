/**
 * Retain a bounded, ordered view of a cursor-fed collection.
 *
 * Cursor pages are newest-first. Keeping both ends preserves recent rows and the
 * oldest rows adjacent to the continuation cursor, while pinned rows remain
 * addressable even when they fall in the discarded middle.
 */
export function retainHeadAndTail<T>(
  values: readonly T[],
  maximum: number,
  identity: (value: T) => string,
  pinned = new Set<string>(),
  headSize = Math.ceil(maximum / 2),
): { items: T[]; truncated: boolean } {
  if (!Number.isInteger(maximum) || maximum < 1) {
    throw new RangeError("maximum must be a positive integer");
  }

  const unique: T[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const id = identity(value);
    if (seen.has(id)) continue;
    seen.add(id);
    unique.push(value);
  }
  if (unique.length <= maximum) return { items: unique, truncated: false };

  const retained = new Set<number>();
  for (let index = 0; index < unique.length && retained.size < maximum; index += 1) {
    if (pinned.has(identity(unique[index]))) retained.add(index);
  }
  const boundedHeadSize = Math.max(0, Math.min(headSize, maximum));
  for (let index = 0; index < boundedHeadSize && retained.size < maximum; index += 1) {
    retained.add(index);
  }
  for (let index = unique.length - 1; index >= 0 && retained.size < maximum; index -= 1) {
    retained.add(index);
  }

  return {
    items: [...retained].sort((left, right) => left - right).map((index) => unique[index]),
    truncated: true,
  };
}

/** Insert or touch one record in a small insertion-ordered detail cache. */
export function retainRecord<T>(
  values: Readonly<Record<string, T>>,
  key: string,
  value: T,
  maximum: number,
  pinned = new Set<string>(),
): Record<string, T> {
  if (!Number.isInteger(maximum) || maximum < 1) {
    throw new RangeError("maximum must be a positive integer");
  }

  const entries = Object.entries(values).filter(([candidate]) => candidate !== key);
  entries.push([key, value]);
  if (entries.length <= maximum) return Object.fromEntries(entries);

  const retained = new Set<string>();
  for (const [candidate] of entries) {
    if (pinned.has(candidate) && retained.size < maximum) retained.add(candidate);
  }
  for (let index = entries.length - 1; index >= 0 && retained.size < maximum; index -= 1) {
    retained.add(entries[index][0]);
  }
  return Object.fromEntries(entries.filter(([candidate]) => retained.has(candidate)));
}
