import { describe, expect, it } from "vitest";

import type { ComparisonChartHistory } from "./api";
import { ComparisonHistoryCache } from "./history-cache";

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

    const byteBounded = new ComparisonHistoryCache({
      maxEntries: 8,
      maxCells: 100_000,
      maxEstimatedBytes: 600_000,
    });
    byteBounded.set("first", dense("first", 6_000));
    byteBounded.set("second", dense("second", 6_000));
    expect(byteBounded.get("first")).toBeUndefined();
    expect(byteBounded.get("second")?.project).toBe("second");
  });
});
