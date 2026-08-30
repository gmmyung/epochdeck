import { describe, expect, it } from "vitest";

import { boundedHistogramCounts } from "./histogram-data";

describe("boundedHistogramCounts", () => {
  it("rebins oversized histograms into fixed canvas work while preserving counts", () => {
    const values = Array.from({ length: 10_000 }, () => 2);
    const bounded = boundedHistogramCounts(values, 500);

    expect(bounded).toHaveLength(500);
    expect(bounded.reduce((total, count) => total + count, 0)).toBe(20_000);
  });
});
