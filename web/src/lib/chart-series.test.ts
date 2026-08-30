import { describe, expect, it } from "vitest";

import type { ChartHistory } from "./api";
import {
  closestSeriesPoints,
  contiguousBucketRanges,
  lineDash,
  metricChartViewportKey,
  prepareMetricSeries,
  runSetIdentity,
  stableSeriesPattern,
  type MetricChartSeries,
} from "./chart-series";

function history(
  runId: string,
  values: number[],
  options: { x?: number[]; steps?: number[]; timestamps?: number[] } = {},
): ChartHistory {
  const steps = options.steps ?? values.map((_, index) => index * 10);
  const timestamps = options.timestamps ?? values.map((_, index) => 1_000 + index * 100);
  return {
    run_id: runId,
    step_min: steps[0] ?? null,
    step_max: steps.at(-1) ?? null,
    bucket_count: values.length,
    source_points: values.length,
    source_last_sequence: values.length - 1,
    metrics: {
      loss: {
        source_points: values.length,
        bucket: values.map((_, index) => index),
        last_step: steps,
        last_timestamp_ms: timestamps,
        minimum: values.map((value) => value - 1),
        maximum: values.map((value) => value + 1),
        last: values,
        last_x: options.x ?? steps,
      },
    },
  } as ChartHistory;
}

describe("multi-run chart series", () => {
  it("uses aligned x coordinates while preserving actual source steps", () => {
    const prepared = prepareMetricSeries(
      {
        runId: "run-a",
        runName: "A",
        color: "#123456",
        available: true,
        history: history("run-a", [5, 4], { x: [0, 1], steps: [100, 200] }),
      },
      "loss",
      "none",
      0.15,
    );

    expect(prepared.x).toEqual([0, 1]);
    expect(prepared.steps).toEqual([100, 200]);
    expect(prepared.status).toBe("ready");
  });

  it("does not silently substitute source steps for a missing aligned x array", () => {
    const malformed = history("run-a", [5, 4]) as unknown as {
      metrics: { loss: Record<string, unknown> };
    };
    delete malformed.metrics.loss.last_x;
    const prepared = prepareMetricSeries(
      {
        runId: "run-a",
        runName: "A",
        color: "#123456",
        available: true,
        history: malformed as unknown as ChartHistory,
      },
      "loss",
      "none",
      0.15,
    );

    expect(prepared.x).toEqual([]);
    expect(prepared.steps).toEqual([0, 10]);
    expect(prepared.status).toBe("no-data");
  });

  it("returns one nearest real point per run without interpolating", () => {
    const inputs: MetricChartSeries[] = [
      {
        runId: "run-a",
        runName: "A",
        color: "#111111",
        available: true,
        history: history("run-a", [1, 2], { x: [0, 10], steps: [0, 10] }),
      },
      {
        runId: "run-b",
        runName: "B",
        color: "#222222",
        available: true,
        history: history("run-b", [8, 9], { x: [2, 20], steps: [20, 200] }),
      },
    ];
    const prepared = inputs.map((input) => prepareMetricSeries(input, "loss", "none", 0.15));

    const points = closestSeriesPoints(prepared, 9, "linear", 0, 20, "linear");
    expect(points.map((point) => [point.series.runId, point.x, point.step, point.raw])).toEqual([
      ["run-a", 10, 10, 2],
      ["run-b", 2, 20, 8],
    ]);
  });

  it("preserves missing gaps and exact bucket envelopes while smoothing centers", () => {
    const prepared = prepareMetricSeries(
      {
        runId: "gapped",
        runName: "Gapped",
        color: "#333333",
        available: true,
        history: history("gapped", [2, Number.NaN, 8]),
      },
      "loss",
      "running",
      3,
    );

    expect(prepared.smoothed).toEqual([2, null, 8]);
    expect(prepared.minimum).toEqual([1, null, 7]);
    expect(prepared.maximum).toEqual([3, null, 9]);
    expect(closestSeriesPoints([prepared], 10, "linear", 0, 20, "linear")[0]?.raw).toBe(8);
  });

  it("keeps missing and not-yet-loaded metrics explicit", () => {
    const unloaded = prepareMetricSeries(
      { runId: "unloaded", runName: "Unloaded", color: "#111111", available: true },
      "loss",
      "none",
      0.15,
    );
    const missing = prepareMetricSeries(
      {
        runId: "missing",
        runName: "Missing",
        color: "#222222",
        available: false,
        history: { ...history("missing", [1]), metrics: {} },
      },
      "loss",
      "none",
      0.15,
    );
    const loadedWithoutPoints = prepareMetricSeries(
      {
        runId: "empty",
        runName: "Empty",
        color: "#333333",
        available: true,
        historyResolved: true,
      },
      "loss",
      "none",
      0.15,
    );

    expect(unloaded.status).toBe("not-loaded");
    expect(missing.status).toBe("no-data");
    expect(loadedWithoutPoints.status).toBe("no-data");
    expect(
      closestSeriesPoints([unloaded, missing, loadedWithoutPoints], 0, "linear", 0, 1, "linear"),
    ).toEqual([]);
  });

  it("assigns deterministic visual patterns and canvas dash arrays", () => {
    expect(stableSeriesPattern("run-a")).toBe(stableSeriesPattern("run-a"));
    expect(lineDash("solid")).toEqual([]);
    expect(lineDash("dash-dot")).toEqual([9, 4, 2, 4]);
  });

  it("identifies a run set independently of display order", () => {
    expect(runSetIdentity([{ runId: "b" }, { runId: "a" }])).toBe(
      runSetIdentity([{ runId: "a" }, { runId: "b" }]),
    );
    expect(runSetIdentity([{ runId: "a" }])).not.toBe(
      runSetIdentity([{ runId: "a" }, { runId: "b" }]),
    );
  });

  it("keys full and bounded parent viewports by value", () => {
    expect(metricChartViewportKey(null)).toBe("full");
    expect(metricChartViewportKey({ minimum: 10, maximum: 20 })).toBe(
      metricChartViewportKey({ minimum: 10, maximum: 20 }),
    );
    expect(metricChartViewportKey({ minimum: 10, maximum: 20 })).not.toBe(
      metricChartViewportKey({ minimum: 11, maximum: 20 }),
    );
  });

  it("preserves sparse bucket indexes and splits paths at bucket gaps", () => {
    const prepared = prepareMetricSeries(
      {
        runId: "sparse",
        runName: "Sparse",
        color: "#111111",
        available: true,
        history: {
          ...history("sparse", [1, 2, 3, 4]),
          metrics: {
            loss: {
              ...history("sparse", [1, 2, 3, 4]).metrics.loss,
              bucket: [0, 1, 7, 8],
            },
          },
        },
      },
      "loss",
      "none",
      0.15,
    );

    expect(prepared.buckets).toEqual([0, 1, 7, 8]);
    expect(contiguousBucketRanges(prepared.buckets, [true, true, true, true])).toEqual([
      { start: 0, end: 2 },
      { start: 2, end: 4 },
    ]);
    expect(contiguousBucketRanges(prepared.buckets, [true, false, true, true])).toEqual([
      { start: 0, end: 1 },
      { start: 2, end: 4 },
    ]);
  });
});
