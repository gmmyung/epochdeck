import { describe, expect, it } from "vitest";

import { boundedHistogramBins } from "./histogram-data";

describe("boundedHistogramBins", () => {
  it("rebins oversized histograms while preserving counts and ranges", () => {
    const values = Array.from({ length: 10_000 }, () => 2);
    const edges = Array.from({ length: 10_001 }, (_, index) => index / 10);
    const bounded = boundedHistogramBins(values, edges, 500);

    expect(bounded).toHaveLength(500);
    expect(bounded.reduce((total, bin) => total + bin.count, 0)).toBe(20_000);
    expect(bounded[0]).toEqual({ lower: 0, upper: 2, count: 40 });
    expect(bounded.at(-1)).toEqual({ lower: 998, upper: 1_000, count: 40 });
  });

  it("uses bin indexes when source edges are invalid", () => {
    expect(boundedHistogramBins([2, 3], [4])).toEqual([
      { lower: 0, upper: 1, count: 2 },
      { lower: 1, upper: 2, count: 3 },
    ]);
  });
});
