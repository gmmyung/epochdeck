export function filterMetricKeys(keys: string[], query: string): string[] {
  const tokens = query.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return keys;
  return keys.filter((key) => {
    const candidate = key.toLocaleLowerCase();
    return tokens.every((token) => candidate.includes(token));
  });
}
