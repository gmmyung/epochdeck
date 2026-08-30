import { describe, expect, it } from "vitest";

import { axisTicks, closestPointIndex, numericExtent, smoothSeries } from "./chart-data";

describe("chart data transforms", () => {
  it("supports each bounded smoothing mode without filling missing rows", () => {
    const x = [0, 1, 2, 4, 8];
    const values = [0, 10, null, 10, 0];

    expect(smoothSeries(x, values, "none", 1)).toEqual(values);
    expect(smoothSeries(x, values, "running", 2)).toEqual([0, 5, null, 10, 5]);
    expect(smoothSeries(x, values, "ema", 0.5)).toEqual([0, 5, null, 10, 5]);
    expect(smoothSeries(x, values, "time-ema", 2)[2]).toBeNull();
    expect(smoothSeries(x, values, "gaussian", 1)[2]).toBeNull();
    expect(smoothSeries([0, 1, 2, 3], [0, 100, null, 0], "running", 2)).toEqual([0, 50, null, 0]);
    expect(smoothSeries([0, 1, 2, 3], [0, 100, null, 0], "ema", 0.5)).toEqual([0, 50, null, 0]);
    expect(smoothSeries([0, 10], [0, 10], "time-ema", 10)[1]).toBeCloseTo(6.3212, 4);
    expect(smoothSeries([0, 1], [0, 10], "time-ema", 10)[1]).toBeCloseTo(0.9516, 4);
    const longSeries = Array.from({ length: 10_000 }, (_, index) => index);
    expect(smoothSeries(longSeries, longSeries, "running", 500).at(-1)).toBe(9_749.5);
  });

  it("builds linear and logarithmic extents and multiple ticks", () => {
    expect(numericExtent([-1, 1, 100], "log")).toEqual([1, 100]);
    expect(axisTicks(0, 100, 6, "linear")).toEqual([0, 20, 40, 60, 80, 100]);
    expect(axisTicks(1, 100, 3, "log")).toEqual([1, 10, 100]);
  });

  it("selects the nearest non-null point for hover inspection", () => {
    expect(closestPointIndex([0, 10, 20], [1, null, 3], 12)).toBe(2);
    expect(closestPointIndex([0], [null], 0)).toBeNull();
    expect(closestPointIndex([1, 1], [3, 4], 1)).toBe(1);
    expect(closestPointIndex([1, 10, 100], [1, 2, 3], 28, "log", 10, 100)).toBe(1);
  });
});
