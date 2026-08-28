import { describe, expect, it } from "vitest";

import type { History } from "./api";
import { HistoryCache, mergeHistoryDelta } from "./history-cache";

function history(sequence: number[], values: Array<number | null>): History {
  return {
    run_id: "run-id",
    sequence,
    step: sequence,
    timestamp_ms: sequence,
    metrics: { loss: values },
    next_after: null,
    sampled: true,
    source_points: values.filter((value) => value !== null).length,
    source_last_sequence: sequence.at(-1) ?? null,
  };
}

describe("HistoryCache", () => {
  it("keys entries by revision and evicts the least recently used value", () => {
    const cache = new HistoryCache(2);
    const first = history([1], [3]);
    const second = history([2], [2]);
    const third = history([3], [1]);
    cache.set("run", "loss", 1, first);
    cache.set("run", "reward", 1, second);
    expect(cache.get("run", "loss", 1)).toBe(first);
    cache.set("run", "accuracy", 1, third);

    expect(cache.get("run", "reward", 1)).toBeUndefined();
    expect(cache.get("run", "loss", 2)).toBeUndefined();
    expect(cache.get("run", "loss", 1)).toBe(first);
  });

  it("merges a bounded delta and advances the source cursor", () => {
    const merged = mergeHistoryDelta(history([1, 2], [3, 2]), history([3, 4], [null, 1]), "loss");

    expect(merged.sequence).toEqual([1, 2, 3, 4]);
    expect(merged.metrics.loss).toEqual([3, 2, null, 1]);
    expect(merged.source_points).toBe(3);
    expect(merged.source_last_sequence).toBe(4);
  });
});
