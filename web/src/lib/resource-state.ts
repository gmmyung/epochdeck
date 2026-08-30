export function mergeNewestPage<T>(
  current: readonly T[],
  newest: readonly T[],
  identity: (value: T) => string,
): T[] {
  const seen = new Set<string>();
  const merged: T[] = [];
  for (const value of [...newest, ...current]) {
    const key = identity(value);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(value);
  }
  return merged;
}

export function appendUniquePage<T>(
  current: readonly T[],
  page: readonly T[],
  identity: (value: T) => string,
): T[] {
  const seen = new Set(current.map(identity));
  const appended: T[] = [];
  for (const value of page) {
    const key = identity(value);
    if (seen.has(key)) continue;
    seen.add(key);
    appended.push(value);
  }
  return [...current, ...appended];
}

export function reasonMessage(reason: unknown, fallback = "Unable to reach Runloom"): string {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}

export function formatDurationMs(milliseconds: number): string {
  const value = Math.max(Number.isFinite(milliseconds) ? milliseconds : 0, 0);
  if (value < 1_000) return `${Math.round(value).toLocaleString()} ms`;
  if (value < 60_000) return `${formatCompact(value / 1_000)} s`;
  if (value < 3_600_000) return `${formatCompact(value / 60_000)} min`;
  if (value < 86_400_000) return `${formatCompact(value / 3_600_000)} h`;
  return `${formatCompact(value / 86_400_000)} d`;
}

function formatCompact(value: number): string {
  return value.toLocaleString(undefined, {
    maximumFractionDigits: value < 10 ? 2 : value < 100 ? 1 : 0,
  });
}
