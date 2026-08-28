import type { History } from "./api";

export const CHART_POINT_BUDGET = 2_000;
export const DELTA_POINT_BUDGET = 256;

export class HistoryCache {
  private readonly entries = new Map<string, History>();

  constructor(private readonly capacity = 128) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new Error("history cache capacity must be a positive integer");
    }
  }

  get(runId: string, metric: string, revision: number): History | undefined {
    const key = cacheKey(runId, metric, revision);
    const value = this.entries.get(key);
    if (!value) return undefined;
    this.entries.delete(key);
    this.entries.set(key, value);
    return value;
  }

  set(runId: string, metric: string, revision: number, history: History): void {
    const key = cacheKey(runId, metric, revision);
    this.entries.delete(key);
    this.entries.set(key, history);
    while (this.entries.size > this.capacity) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }
}

export function mergeHistoryDelta(base: History, delta: History, metric: string): History {
  const values = delta.metrics[metric] ?? [];
  const addedSourcePoints = values.reduce<number>(
    (count, value) => count + (value === null ? 0 : 1),
    0,
  );
  return {
    run_id: base.run_id,
    sequence: [...base.sequence, ...delta.sequence],
    step: [...base.step, ...delta.step],
    timestamp_ms: [...base.timestamp_ms, ...delta.timestamp_ms],
    metrics: {
      [metric]: [...(base.metrics[metric] ?? []), ...values],
    },
    next_after: null,
    sampled: base.sampled,
    source_points: (base.source_points ?? base.sequence.length) + addedSourcePoints,
    source_last_sequence: delta.source_last_sequence ?? base.source_last_sequence,
  };
}

function cacheKey(runId: string, metric: string, revision: number): string {
  return `${runId}\0${metric}\0${revision}`;
}
