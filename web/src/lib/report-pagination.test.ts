import { describe, expect, it } from "vitest";

import type { ReportPanel } from "./api";
import {
  MAX_REPORT_CHARTS_PER_PAGE,
  MAX_REPORT_PANELS_PER_PAGE,
  paginateReportPanels,
} from "./report-pagination";

describe("report pagination", () => {
  it("caps markdown-only pages while preserving panel order", () => {
    const panels = Array.from({ length: 25 }, (_, index) => markdownPanel(`markdown-${index}`));

    const pages = paginateReportPanels(panels);

    expect(pages.map((page) => page.panels.length)).toEqual([12, 12, 1]);
    expect(pages.map((page) => page.chartCount)).toEqual([0, 0, 0]);
    expect(pages.flatMap((page) => page.panels.map((panel) => panel.id))).toEqual(
      panels.map((panel) => panel.id),
    );
  });

  it("starts a new page before its mounted chart budget is exceeded", () => {
    const panels = [
      metricPanel("first", 8),
      metricPanel("second", 8),
      metricPanel("third", 8),
      metricPanel("fourth", 1),
    ];

    const pages = paginateReportPanels(panels);

    expect(pages).toHaveLength(2);
    expect(pages[0].panels.map((panel) => panel.id)).toEqual(["first", "second", "third"]);
    expect(pages[0].chartCount).toBe(24);
    expect(pages[1].panels.map((panel) => panel.id)).toEqual(["fourth"]);
    expect(pages[1].chartCount).toBe(1);
  });

  it("keeps mixed panels intact and every page within both budgets", () => {
    const panels = [
      ...Array.from({ length: 10 }, (_, index) => markdownPanel(`intro-${index}`)),
      metricPanel("dense", 20),
      markdownPanel("notes"),
      metricPanel("tail", 5),
    ];

    const pages = paginateReportPanels(panels);

    expect(pages.flatMap((page) => page.panels)).toEqual(panels);
    expect(pages.map((page) => page.panels.map((panel) => panel.id))).toEqual([
      [...Array.from({ length: 10 }, (_, index) => `intro-${index}`), "dense", "notes"],
      ["tail"],
    ]);
    for (const page of pages) {
      expect(page.panels.length).toBeLessThanOrEqual(MAX_REPORT_PANELS_PER_PAGE);
      expect(page.chartCount).toBeLessThanOrEqual(MAX_REPORT_CHARTS_PER_PAGE);
    }
  });

  it("rejects a single panel that cannot fit without splitting it", () => {
    expect(() => paginateReportPanels([metricPanel("oversized", 25)])).toThrow(
      /per-panel limit is 24/,
    );
  });

  it("returns one empty page for an empty report", () => {
    expect(paginateReportPanels([])).toEqual([{ panels: [], chartCount: 0 }]);
  });
});

function metricPanel(id: string, metricCount: number): ReportPanel {
  return {
    id,
    title: id,
    kind: "metric",
    run_id: "run-id",
    metric_keys: Array.from({ length: metricCount }, (_, index) => `metric-${index}`),
    markdown: null,
    width: 1,
    height: 320,
  };
}

function markdownPanel(id: string): ReportPanel {
  return {
    id,
    title: id,
    kind: "markdown",
    run_id: null,
    metric_keys: [],
    markdown: "Notes",
    width: 1,
    height: 200,
  };
}
