import type { ChartHistory } from "./api";

export const CHART_BUCKET_BUDGET = 2_000;

export class ChartHistoryCache {
  private readonly entries = new Map<string, ChartHistory>();

  constructor(private readonly capacity = 128) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new Error("chart history cache capacity must be a positive integer");
    }
  }

  get(
    runId: string,
    metric: string,
    revision: number,
    stepMin?: number,
    stepMax?: number,
  ): ChartHistory | undefined {
    const key = cacheKey(runId, metric, revision, stepMin, stepMax);
    const value = this.entries.get(key);
    if (!value) return undefined;
    this.entries.delete(key);
    this.entries.set(key, value);
    return value;
  }

  set(
    runId: string,
    metric: string,
    revision: number,
    history: ChartHistory,
    stepMin?: number,
    stepMax?: number,
  ): void {
    const key = cacheKey(runId, metric, revision, stepMin, stepMax);
    this.entries.delete(key);
    this.entries.set(key, history);
    while (this.entries.size > this.capacity) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }
}

function cacheKey(
  runId: string,
  metric: string,
  revision: number,
  stepMin?: number,
  stepMax?: number,
): string {
  const viewport = stepMin === undefined || stepMax === undefined ? "all" : `${stepMin}:${stepMax}`;
  return `${runId}\0${metric}\0${revision}\0${viewport}`;
}
