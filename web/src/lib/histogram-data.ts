const HISTOGRAM_CANVAS_BIN_LIMIT = 2_000;

export type HistogramBin = {
  lower: number;
  upper: number;
  count: number;
};

/** Re-bin oversized histograms while preserving mass and the represented value ranges. */
export function boundedHistogramBins(
  counts: readonly number[],
  edges: readonly number[],
  maximum = HISTOGRAM_CANVAS_BIN_LIMIT,
): HistogramBin[] {
  if (!Number.isInteger(maximum) || maximum < 1) {
    throw new RangeError("maximum must be a positive integer");
  }
  if (counts.length === 0) return [];

  const normalizedEdges = validEdges(counts, edges)
    ? edges
    : Array.from({ length: counts.length + 1 }, (_, index) => index);
  const binCount = Math.min(counts.length, maximum);
  const bins: HistogramBin[] = [];

  for (let bucket = 0; bucket < binCount; bucket += 1) {
    const start = Math.floor((bucket * counts.length) / binCount);
    const end = Math.floor(((bucket + 1) * counts.length) / binCount);
    let count = 0;
    for (let index = start; index < end; index += 1) count += finiteCount(counts[index]);
    bins.push({ lower: normalizedEdges[start], upper: normalizedEdges[end], count });
  }
  return bins;
}

function finiteCount(value: number): number {
  return Number.isFinite(value) ? Math.max(value, 0) : 0;
}

function validEdges(counts: readonly number[], edges: readonly number[]): boolean {
  return (
    edges.length === counts.length + 1 &&
    edges.every(Number.isFinite) &&
    edges.every((edge, index) => index === 0 || edge > edges[index - 1])
  );
}
