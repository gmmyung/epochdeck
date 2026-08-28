import { describe, expect, it } from "vitest";

import {
  MAX_SELECTED_RUNS,
  METRIC_CHART_PAGE_SIZE,
  comparisonCacheKey,
  comparisonBucketBudget,
  metricPage,
  metricAvailability,
  normalizeRunSelection,
  planComparisonBatches,
  readComparisonUrl,
  runStyle,
  writeComparisonUrl,
} from "./comparison-state";

describe("comparison state", () => {
  it("keeps an ordered, valid, bounded run selection and repairs its primary run", () => {
    const available = new Set(Array.from({ length: 20 }, (_, index) => `run-${index}`));
    const requested = ["missing", "run-2", "run-2", ...available];
    const normalized = normalizeRunSelection(requested, available, "missing");

    expect(normalized.runIds).toHaveLength(MAX_SELECTED_RUNS);
    expect(normalized.runIds.slice(0, 3)).toEqual(["run-2", "run-0", "run-1"]);
    expect(normalized.primaryRunId).toBe("run-2");
  });

  it("builds union and intersection catalogs with availability counts", () => {
    const keys = { a: ["loss", "reward"], b: ["loss", "speed"], c: ["loss", "reward"] };

    expect(metricAvailability(["a", "b", "c"], keys, "union")).toEqual([
      { key: "loss", available: 3, total: 3 },
      { key: "reward", available: 2, total: 3 },
      { key: "speed", available: 1, total: 3 },
    ]);
    expect(metricAvailability(["a", "b", "c"], keys, "intersection")).toEqual([
      { key: "loss", available: 3, total: 3 },
    ]);
  });

  it("keeps every overlay request inside the server cell budget", () => {
    expect(comparisonBucketBudget(1, 2_000)).toBe(2_000);
    expect(comparisonBucketBudget(12, 2_000)).toBe(1_666);
    expect(() => comparisonBucketBudget(0, 2_000)).toThrow(/positive integer/);
  });

  it("plans multiple visible metrics into bounded multi-series requests", () => {
    const candidates = Array.from({ length: 20 }, (_, index) => ({
      metric: `metric-${index}`,
      runIds: ["run-a", "run-b"],
    }));
    const batches = planComparisonBatches(candidates, 2_000);

    expect(batches).toHaveLength(2);
    expect(batches[0].candidates).toHaveLength(16);
    expect(batches[0].seriesCount).toBe(32);
    expect(batches[0].seriesCount * batches[0].maxBuckets).toBeLessThanOrEqual(20_000);
    expect(batches[1].candidates).toHaveLength(4);
    expect(batches.every((batch) => batch.seriesCount <= 32)).toBe(true);
  });

  it("keys comparison batches by their actual bucket budget", () => {
    const metrics = [{ metric: "loss", revisions: [["run-a", 7] as const] }];
    const coarse = comparisonCacheKey("project", "step", 625, null, metrics);
    const detailed = comparisonCacheKey("project", "step", 2_000, null, metrics);

    expect(coarse).not.toBe(detailed);
  });

  it("paginates instantiated metric charts with a hard 24-chart bound", () => {
    const metrics = Array.from({ length: 55 }, (_, index) => `metric-${index}`);
    const middle = metricPage(metrics, 1);
    const clamped = metricPage(metrics, 99);

    expect(middle.values).toHaveLength(METRIC_CHART_PAGE_SIZE);
    expect(middle.values[0]).toBe("metric-24");
    expect(clamped.page).toBe(2);
    expect(clamped.values).toHaveLength(7);
    expect(metricPage(metrics, Number.NaN).page).toBe(0);
  });

  it("assigns the same visual identity regardless of selection order", () => {
    expect(runStyle("run-a")).toEqual(runStyle("run-a"));
    expect(runStyle("run-a")).not.toEqual(runStyle("run-b"));
  });

  it("round-trips repeated run parameters and validates enum values", () => {
    const tabs = new Set(["summary", "metrics"] as const);
    const written = writeComparisonUrl(new URL("https://runloom.test/?unrelated=kept"), {
      project: "robot learning",
      runIds: ["second", "first"],
      runSelectionSpecified: true,
      primaryRunId: "first",
      tab: "metrics" as const,
      metricMode: "intersection",
      search: "train/loss",
      alignment: "elapsed-time",
      chartMetric: "train/loss",
      chartViewport: { minimum: 12.5, maximum: 87.5 },
    });
    const restored = readComparisonUrl(written, tabs, "metrics");

    expect(restored).toEqual({
      project: "robot learning",
      runIds: ["second", "first"],
      runSelectionSpecified: true,
      primaryRunId: "first",
      tab: "metrics",
      metricMode: "intersection",
      search: "train/loss",
      alignment: "elapsed-time",
      chartMetric: "train/loss",
      chartViewport: { minimum: 12.5, maximum: 87.5 },
    });
    expect(written.searchParams.get("unrelated")).toBe("kept");

    const invalid = readComparisonUrl(
      new URL("https://runloom.test/?tab=nope&metricMode=nope&alignment=nope"),
      tabs,
      "summary",
    );
    expect(invalid.tab).toBe("summary");
    expect(invalid.metricMode).toBe("union");
    expect(invalid.alignment).toBe("step");
    expect(invalid.chartMetric).toBeNull();
    expect(invalid.chartViewport).toBeNull();
    expect(
      readComparisonUrl(new URL("https://runloom.test/?chart=loss&xmin=-1"), tabs, "metrics")
        .chartViewport,
    ).toBeNull();
  });

  it("drops invalid viewports and clears stale viewport parameters", () => {
    const tabs = new Set(["metrics"] as const);
    const invalid = readComparisonUrl(
      new URL("https://runloom.test/?chart=loss&xmin=20&xmax=10"),
      tabs,
      "metrics",
    );
    expect(invalid.chartViewport).toBeNull();

    const cleared = writeComparisonUrl(new URL("https://runloom.test/?chart=loss&xmin=1&xmax=2"), {
      project: "p",
      runIds: [],
      runSelectionSpecified: true,
      primaryRunId: null,
      tab: "metrics",
      metricMode: "union",
      search: "",
      alignment: "step",
      chartMetric: null,
      chartViewport: null,
    });
    expect(cleared.searchParams.has("chart")).toBe(false);
    expect(cleared.searchParams.has("xmin")).toBe(false);
    expect(cleared.searchParams.has("xmax")).toBe(false);
  });

  it("restores viewport history from range A to B and then full range", () => {
    const tabs = new Set(["metrics"] as const);
    const state = {
      project: "p",
      runIds: ["run-a"],
      runSelectionSpecified: true,
      primaryRunId: "run-a",
      tab: "metrics" as const,
      metricMode: "union" as const,
      search: "",
      alignment: "step" as const,
      chartMetric: "loss",
      chartViewport: { minimum: 10, maximum: 20 },
    };
    const rangeA = writeComparisonUrl(new URL("https://runloom.test/"), state);
    const rangeB = writeComparisonUrl(rangeA, {
      ...state,
      chartViewport: { minimum: 30, maximum: 50 },
    });
    const fullRange = writeComparisonUrl(rangeB, {
      ...state,
      chartMetric: null,
      chartViewport: null,
    });

    expect(readComparisonUrl(rangeA, tabs, "metrics").chartViewport).toEqual({
      minimum: 10,
      maximum: 20,
    });
    expect(readComparisonUrl(rangeB, tabs, "metrics").chartViewport).toEqual({
      minimum: 30,
      maximum: 50,
    });
    expect(readComparisonUrl(fullRange, tabs, "metrics").chartViewport).toBeNull();
  });
});
