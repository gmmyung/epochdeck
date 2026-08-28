import { describe, expect, it } from "vitest";

import type { ChartHistory } from "./api";
import { ChartHistoryCache } from "./history-cache";

function history(steps: number[], values: number[]): ChartHistory {
  return {
    run_id: "run-id",
    step_min: steps.at(0) ?? null,
    step_max: steps.at(-1) ?? null,
    bucket_count: steps.length,
    source_points: values.length,
    source_last_sequence: steps.at(-1) ?? null,
    metrics: {
      loss: {
        source_points: values.length,
        bucket: values.map((_, index) => index),
        last_step: steps,
        last_timestamp_ms: steps,
        minimum: values,
        maximum: values,
        last: values,
      },
    },
  };
}

describe("ChartHistoryCache", () => {
  it("keys entries by revision and viewport and evicts the least recently used value", () => {
    const cache = new ChartHistoryCache(2);
    const first = history([1], [3]);
    const second = history([2], [2]);
    const third = history([3], [1]);
    cache.set("run", "loss", 1, first, 10, 20);
    cache.set("run", "reward", 1, second);
    expect(cache.get("run", "loss", 1, 10, 20)).toBe(first);
    cache.set("run", "accuracy", 1, third);

    expect(cache.get("run", "reward", 1)).toBeUndefined();
    expect(cache.get("run", "loss", 2)).toBeUndefined();
    expect(cache.get("run", "loss", 1)).toBeUndefined();
    expect(cache.get("run", "loss", 1, 10, 20)).toBe(first);
  });
});
