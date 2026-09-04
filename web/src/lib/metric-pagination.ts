const MAX_METRIC_BACK_HISTORY = 64;

export function pushMetricCursor(
  history: readonly (string | null)[],
  current: string | null,
  maximum = MAX_METRIC_BACK_HISTORY,
): { history: Array<string | null>; truncated: boolean } {
  if (!Number.isInteger(maximum) || maximum < 1) {
    throw new RangeError("maximum must be a positive integer");
  }
  const next = [...history, current];
  return {
    history: next.slice(-maximum),
    truncated: next.length > maximum,
  };
}
