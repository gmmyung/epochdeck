import { describe, expect, it } from "vitest";

import { chartViewportKey, metricChartRequestKey, normalizeChartViewport } from "./chart-request";

describe("chart requests", () => {
  it("normalizes a floating chart viewport to an inclusive unsigned step range", () => {
    expect(normalizeChartViewport(10.8, 20.1)).toEqual({ stepMin: 10, stepMax: 21 });
    expect(normalizeChartViewport(20.1, -4)).toEqual({ stepMin: 0, stepMax: 21 });
    expect(normalizeChartViewport(Number.NaN, 10)).toBeNull();
    expect(normalizeChartViewport(null, null)).toBeNull();
  });

  it("separates full, viewport, and revision cache identities", () => {
    const viewport = { stepMin: 10, stepMax: 20 };
    expect(chartViewportKey(null)).toBe("all");
    expect(chartViewportKey(viewport)).toBe("10:20");
    expect(metricChartRequestKey(7, viewport)).toBe("7:10:20");
    expect(metricChartRequestKey(8, viewport)).not.toBe(metricChartRequestKey(7, viewport));
  });
});
