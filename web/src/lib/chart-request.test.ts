import { describe, expect, it } from "vitest";

import type { ChartHistory, ComparisonChartHistory } from "./api";
import {
  chartViewportKey,
  normalizeChartViewport,
  preserveNavigableHistory,
} from "./chart-request";

describe("chart requests", () => {
  it("normalizes a floating chart viewport to an inclusive unsigned step range", () => {
    expect(normalizeChartViewport(10.8, 20.1)).toEqual({ stepMin: 10, stepMax: 21 });
    expect(normalizeChartViewport(20.1, -4)).toEqual({ stepMin: 0, stepMax: 21 });
    expect(normalizeChartViewport(Number.NaN, 10)).toBeNull();
    expect(normalizeChartViewport(null, null)).toBeNull();
  });

  it("separates full and viewport cache identities", () => {
    const viewport = { stepMin: 10, stepMax: 20 };
    expect(chartViewportKey(null)).toBe("all");
    expect(chartViewportKey(viewport)).toBe("10:20");
  });

  it("keeps the last plottable history when a zoomed viewport contains no samples", () => {
    const previous = chartHistory([10], [2]);
    const empty = chartHistory([], []);
    const replacement = chartHistory([20], [1]);
    const viewport = { stepMin: 15, stepMax: 16 };

    expect(preserveNavigableHistory(previous, empty, viewport)).toBe(previous);
    expect(preserveNavigableHistory(previous, replacement, viewport)).toBe(replacement);
    expect(preserveNavigableHistory(previous, empty, null)).toBe(empty);
  });

  it("applies empty-viewport navigation retention to comparison histories", () => {
    const previous = comparisonHistory([10], [2]);
    const empty = comparisonHistory([], []);

    expect(preserveNavigableHistory(previous, empty, { stepMin: 15, stepMax: 16 })).toBe(previous);
  });
});

function chartHistory(steps: number[], values: number[]): ChartHistory {
  return {
    run_id: "run",
    step_min: steps.at(0) ?? null,
    step_max: steps.at(-1) ?? null,
    bucket_count: steps.length,
    source_points: values.length,
    source_last_sequence: steps.at(-1) ?? null,
    metrics: { loss: metricHistory(steps, values) },
  };
}

function comparisonHistory(steps: number[], values: number[]): ComparisonChartHistory {
  return {
    project: "project",
    alignment: "step",
    x_min: steps.at(0) ?? null,
    x_max: steps.at(-1) ?? null,
    bucket_count: steps.length,
    runs: [{ run_id: "run", source_last_sequence: steps.at(-1) ?? null }],
    series: [{ run_id: "run", key: "loss", ...metricHistory(steps, values) }],
  };
}

function metricHistory(steps: number[], values: number[]) {
  return {
    source_points: values.length,
    bucket: values.map((_, index) => index),
    last_x: steps,
    last_step: steps,
    last_timestamp_ms: steps,
    minimum: values,
    maximum: values,
    last: values,
  };
}
