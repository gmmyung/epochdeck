import { describe, expect, it } from "vitest";

import { filterMetricKeys } from "./metric-filter";

describe("metric filtering", () => {
  const keys = ["train/loss", "train/reward mean", "eval/loss", "Throughput"];

  it("matches case-insensitive tokens while preserving server order", () => {
    expect(filterMetricKeys(keys, "TRAIN loss")).toEqual(["train/loss"]);
    expect(filterMetricKeys(keys, " loss ")).toEqual(["train/loss", "eval/loss"]);
    expect(filterMetricKeys(keys, "through")).toEqual(["Throughput"]);
  });

  it("returns the original list for an empty query and an empty list for no match", () => {
    expect(filterMetricKeys(keys, "  ")).toBe(keys);
    expect(filterMetricKeys(keys, "missing")).toEqual([]);
  });
});
