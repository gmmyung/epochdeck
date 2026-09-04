import { describe, expect, it } from "vitest";

import {
  MAX_SELECTED_RUNS,
  comparisonCacheKey,
  normalizeRunSelection,
  planComparisonBatches,
  readComparisonUrl,
  runStyle,
  writeComparisonUrl,
} from "./comparison-state";

const RUN_A = "00000000-0000-7000-8000-000000000001";
const RUN_B = "00000000-0000-7000-8000-000000000002";
const REPORT = "00000000-0000-7000-8000-000000000003";

describe("comparison state", () => {
  it("keeps an ordered, valid, bounded run selection and repairs its primary run", () => {
    const available = new Set(Array.from({ length: 20 }, (_, index) => `run-${index}`));
    const requested = ["missing", "run-2", "run-2", ...available];
    const normalized = normalizeRunSelection(requested, available, "missing");

    expect(normalized.runIds).toHaveLength(MAX_SELECTED_RUNS);
    expect(normalized.runIds.slice(0, 3)).toEqual(["run-2", "run-0", "run-1"]);
    expect(normalized.primaryRunId).toBe("run-2");
  });

  it("keeps every overlay request inside the server cell budget", () => {
    const singleSeries = planComparisonBatches([{ metric: "loss", runIds: ["run-a"] }], 2_000);
    const twelveSeries = planComparisonBatches(
      [{ metric: "loss", runIds: Array.from({ length: 12 }, (_, index) => `run-${index}`) }],
      2_000,
    );

    expect(singleSeries[0].maxBuckets).toBe(2_000);
    expect(twelveSeries[0].maxBuckets).toBe(1_666);
    expect(() => planComparisonBatches([{ metric: "loss", runIds: ["run-a"] }], 0)).toThrow(
      /positive integer/,
    );
  });

  it("plans multiple visible metrics into bounded multi-series requests", () => {
    const candidates = Array.from({ length: 20 }, (_, index) => ({
      metric: `metric-${index}`,
      runIds: ["run-a", "run-b"],
    }));
    const batches = planComparisonBatches(candidates, 2_000);

    expect(batches).toHaveLength(2);
    expect(batches[0].candidates).toHaveLength(16);
    const seriesCounts = batches.map((batch) =>
      batch.candidates.reduce((total, candidate) => total + candidate.runIds.length, 0),
    );
    expect(seriesCounts[0]).toBe(32);
    expect(seriesCounts[0] * batches[0].maxBuckets).toBeLessThanOrEqual(20_000);
    expect(batches[1].candidates).toHaveLength(4);
    expect(seriesCounts.every((count) => count <= 32)).toBe(true);
  });

  it("keys comparison batches by their actual bucket budget", () => {
    const metrics = [{ metric: "loss", revisions: [["run-a", 7] as const] }];
    const coarse = comparisonCacheKey("project", "step", 625, null, metrics);
    const detailed = comparisonCacheKey("project", "step", 2_000, null, metrics);

    expect(coarse).not.toBe(detailed);
  });

  it("assigns the same visual identity regardless of selection order", () => {
    expect(runStyle("run-a")).toEqual(runStyle("run-a"));
    expect(runStyle("run-a")).not.toEqual(runStyle("run-b"));
  });

  it("round-trips repeated run parameters and validates enum values", () => {
    const tabs = new Set(["summary", "metrics"] as const);
    const written = writeComparisonUrl(new URL("https://epochdeck.test/?unrelated=kept"), {
      project: "robot learning",
      reportId: REPORT,
      runIds: [RUN_B, RUN_A],
      runSelectionSpecified: true,
      primaryRunId: RUN_A,
      tab: "metrics" as const,
      metricMode: "intersection",
      search: "train/loss",
      metricAfter: "train/loss",
      alignment: "elapsed-time",
      chartMetric: "train/loss",
      chartViewport: { minimum: 12.5, maximum: 87.5 },
    });
    const restored = readComparisonUrl(written, tabs, "metrics");

    expect(restored).toEqual({
      project: "robot learning",
      reportId: REPORT,
      runIds: [RUN_B, RUN_A],
      runSelectionSpecified: true,
      primaryRunId: RUN_A,
      tab: "metrics",
      metricMode: "intersection",
      search: "train/loss",
      metricAfter: "train/loss",
      alignment: "elapsed-time",
      chartMetric: "train/loss",
      chartViewport: { minimum: 12.5, maximum: 87.5 },
    });
    expect(written.searchParams.get("metric_after")).toBe("train/loss");
    expect(written.searchParams.has("metricPage")).toBe(false);
    expect(written.searchParams.get("unrelated")).toBe("kept");

    const invalid = readComparisonUrl(
      new URL("https://epochdeck.test/?tab=nope&metricMode=nope&alignment=nope"),
      tabs,
      "summary",
    );
    expect(invalid.tab).toBe("summary");
    expect(invalid.metricMode).toBe("union");
    expect(invalid.alignment).toBe("step");
    expect(invalid.reportId).toBeNull();
    expect(invalid.metricAfter).toBeNull();
    expect(invalid.chartMetric).toBeNull();
    expect(invalid.chartViewport).toBeNull();
    expect(
      readComparisonUrl(new URL("https://epochdeck.test/?chart=loss&xmin=-1"), tabs, "metrics")
        .chartViewport,
    ).toBeNull();
  });

  it("drops invalid viewports and clears stale viewport parameters", () => {
    const tabs = new Set(["metrics"] as const);
    const invalid = readComparisonUrl(
      new URL("https://epochdeck.test/?chart=loss&xmin=20&xmax=10"),
      tabs,
      "metrics",
    );
    expect(invalid.chartViewport).toBeNull();

    const cleared = writeComparisonUrl(
      new URL("https://epochdeck.test/?chart=loss&xmin=1&xmax=2"),
      {
        project: "p",
        reportId: null,
        runIds: [],
        runSelectionSpecified: true,
        primaryRunId: null,
        tab: "metrics",
        metricMode: "union",
        search: "",
        metricAfter: null,
        alignment: "step",
        chartMetric: null,
        chartViewport: null,
      },
    );
    expect(cleared.searchParams.has("chart")).toBe(false);
    expect(cleared.searchParams.has("xmin")).toBe(false);
    expect(cleared.searchParams.has("xmax")).toBe(false);
  });

  it("bounds and validates untrusted deep-link fields before orchestration", () => {
    const url = new URL("https://epochdeck.test/");
    for (let index = 0; index < 30; index += 1) {
      url.searchParams.append("run", `00000000-0000-7000-8000-${String(index).padStart(12, "0")}`);
    }
    url.searchParams.append("run", RUN_A);
    url.searchParams.set("project", "p".repeat(129));
    url.searchParams.set("report", "not-a-uuid");
    url.searchParams.set("search", "s".repeat(257));
    url.searchParams.set("metric_after", `loss\u0085hidden`);

    const state = readComparisonUrl(url, new Set(["metrics"] as const), "metrics");

    expect(state.runIds).toHaveLength(MAX_SELECTED_RUNS);
    expect(new Set(state.runIds).size).toBe(MAX_SELECTED_RUNS);
    expect(state.project).toBeNull();
    expect(state.reportId).toBeNull();
    expect(state.search).toBe("");
    expect(state.metricAfter).toBeNull();
  });

  it("restores viewport history from range A to B and then full range", () => {
    const tabs = new Set(["metrics"] as const);
    const state = {
      project: "p",
      reportId: null,
      runIds: [RUN_A],
      runSelectionSpecified: true,
      primaryRunId: RUN_A,
      tab: "metrics" as const,
      metricMode: "union" as const,
      search: "",
      metricAfter: null,
      alignment: "step" as const,
      chartMetric: "loss",
      chartViewport: { minimum: 10, maximum: 20 },
    };
    const rangeA = writeComparisonUrl(new URL("https://epochdeck.test/"), state);
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
