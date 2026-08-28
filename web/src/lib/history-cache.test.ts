import { describe, expect, it } from "vitest";

import type { ChartHistory, ComparisonChartHistory } from "./api";
import { ChartHistoryCache, ComparisonHistoryCache } from "./history-cache";

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
        last_x: steps,
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
    cache.set("run", "loss", 1, 2_000, first, 10, 20);
    cache.set("run", "reward", 1, 2_000, second);
    expect(cache.get("run", "loss", 1, 2_000, 10, 20)).toBe(first);
    cache.set("run", "accuracy", 1, 2_000, third);

    expect(cache.get("run", "reward", 1, 2_000)).toBeUndefined();
    expect(cache.get("run", "loss", 2, 2_000)).toBeUndefined();
    expect(cache.get("run", "loss", 1, 1_000, 10, 20)).toBeUndefined();
    expect(cache.get("run", "loss", 1, 2_000)).toBeUndefined();
    expect(cache.get("run", "loss", 1, 2_000, 10, 20)).toBe(first);
  });
});

describe("ComparisonHistoryCache", () => {
  it("is bounded and promotes recently read responses", () => {
    const cache = new ComparisonHistoryCache({
      maxEntries: 2,
      maxCells: 10,
      maxEstimatedBytes: 10_000,
    });
    const response = (project: string): ComparisonChartHistory => ({
      project,
      alignment: "step" as const,
      x_min: null,
      x_max: null,
      bucket_count: 0,
      runs: [],
      series: [],
    });
    cache.set("a", response("a"));
    cache.set("b", response("b"));
    expect(cache.get("a")?.project).toBe("a");
    cache.set("c", response("c"));
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("a")?.project).toBe("a");
    expect(cache.get("c")?.project).toBe("c");
  });

  it("evicts dense responses by cell and estimated-byte weight", () => {
    const dense = (project: string, cells: number): ComparisonChartHistory => {
      const values = Array.from({ length: cells }, (_, index) => index);
      return {
        project,
        alignment: "step",
        x_min: 0,
        x_max: cells - 1,
        bucket_count: cells,
        runs: [{ run_id: "run", source_last_sequence: cells }],
        series: [
          {
            run_id: "run",
            key: "loss",
            source_points: cells,
            bucket: values,
            last_x: values,
            last_step: values,
            last_timestamp_ms: values,
            minimum: values,
            maximum: values,
            last: values,
          },
        ],
      };
    };
    const cellBounded = new ComparisonHistoryCache({
      maxEntries: 8,
      maxCells: 12_000,
      maxEstimatedBytes: 8 * 1024 * 1024,
    });
    cellBounded.set("first", dense("first", 7_000));
    cellBounded.set("second", dense("second", 7_000));
    expect(cellBounded.get("first")).toBeUndefined();
    expect(cellBounded.get("second")?.project).toBe("second");
    expect(cellBounded.cellCount).toBeLessThanOrEqual(12_000);

    const byteBounded = new ComparisonHistoryCache({
      maxEntries: 8,
      maxCells: 100_000,
      maxEstimatedBytes: 600_000,
    });
    byteBounded.set("first", dense("first", 6_000));
    byteBounded.set("second", dense("second", 6_000));
    expect(byteBounded.size).toBe(1);
    expect(byteBounded.get("first")).toBeUndefined();
    expect(byteBounded.estimatedBytes).toBeLessThanOrEqual(600_000);
  });
});
