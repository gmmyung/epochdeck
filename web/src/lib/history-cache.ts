import type { ComparisonChartHistory } from "./api";

export const CHART_BUCKET_BUDGET = 2_000;
export const COMPARISON_CACHE_MAX_ENTRIES = 12;
export const COMPARISON_CACHE_MAX_CELLS = 40_000;
export const COMPARISON_CACHE_MAX_ESTIMATED_BYTES = 4 * 1024 * 1024;

export class ComparisonHistoryCache {
  private readonly entries = new Map<
    string,
    { history: ComparisonChartHistory; cells: number; estimatedBytes: number }
  >();
  private totalCells = 0;
  private totalEstimatedBytes = 0;

  constructor(
    private readonly limits = {
      maxEntries: COMPARISON_CACHE_MAX_ENTRIES,
      maxCells: COMPARISON_CACHE_MAX_CELLS,
      maxEstimatedBytes: COMPARISON_CACHE_MAX_ESTIMATED_BYTES,
    },
  ) {
    for (const [name, value] of Object.entries(limits)) {
      if (!Number.isInteger(value) || value < 1) {
        throw new Error(`comparison history cache ${name} must be a positive integer`);
      }
    }
  }

  get(requestKey: string): ComparisonChartHistory | undefined {
    const entry = this.entries.get(requestKey);
    if (!entry) return undefined;
    this.entries.delete(requestKey);
    this.entries.set(requestKey, entry);
    return entry.history;
  }

  set(requestKey: string, history: ComparisonChartHistory): void {
    this.delete(requestKey);
    const cells = comparisonCellCount(history);
    const estimatedBytes = comparisonEstimatedBytes(history);
    if (cells > this.limits.maxCells || estimatedBytes > this.limits.maxEstimatedBytes) return;
    this.entries.set(requestKey, { history, cells, estimatedBytes });
    this.totalCells += cells;
    this.totalEstimatedBytes += estimatedBytes;
    while (
      this.entries.size > this.limits.maxEntries ||
      this.totalCells > this.limits.maxCells ||
      this.totalEstimatedBytes > this.limits.maxEstimatedBytes
    ) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.delete(oldest);
    }
  }

  private delete(requestKey: string): void {
    const entry = this.entries.get(requestKey);
    if (!entry) return;
    this.entries.delete(requestKey);
    this.totalCells -= entry.cells;
    this.totalEstimatedBytes -= entry.estimatedBytes;
  }
}

function comparisonCellCount(history: ComparisonChartHistory): number {
  return history.series.reduce((total, series) => total + series.bucket.length, 0);
}

function comparisonEstimatedBytes(history: ComparisonChartHistory): number {
  let bytes = 256 + history.project.length * 2 + history.runs.length * 48;
  for (const series of history.series) {
    bytes += 192 + (series.run_id.length + series.key.length) * 2;
    bytes +=
      (series.bucket.length +
        series.last_x.length +
        series.last_step.length +
        series.last_timestamp_ms.length +
        series.minimum.length +
        series.maximum.length +
        series.last.length) *
      8;
  }
  return bytes;
}
