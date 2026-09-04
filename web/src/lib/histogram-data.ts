const HISTOGRAM_CANVAS_BIN_LIMIT = 2_000;

/** Re-bin oversized histograms so canvas work stays constant while preserving total mass. */
export function boundedHistogramCounts(
  values: readonly number[],
  maximum = HISTOGRAM_CANVAS_BIN_LIMIT,
): number[] {
  if (!Number.isInteger(maximum) || maximum < 1) {
    throw new RangeError("maximum must be a positive integer");
  }
  if (values.length <= maximum) return values.map(finiteCount);

  const buckets: number[] = [];
  for (let bucket = 0; bucket < maximum; bucket += 1) {
    const start = Math.floor((bucket * values.length) / maximum);
    const end = Math.floor(((bucket + 1) * values.length) / maximum);
    let count = 0;
    for (let index = start; index < end; index += 1) count += finiteCount(values[index]);
    buckets.push(count);
  }
  return buckets;
}

function finiteCount(value: number): number {
  return Number.isFinite(value) ? Math.max(value, 0) : 0;
}
