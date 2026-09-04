// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import RunMetricsPanel from "./RunMetricsPanel.svelte";
import type { RunListItem } from "./api";

beforeEach(() => {
  class IntersectionObserverMock {
    constructor(private readonly callback: IntersectionObserverCallback) {}

    observe(target: Element): void {
      this.callback(
        [{ target, isIntersecting: false, intersectionRatio: 0 } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      );
    }

    disconnect(): void {}
    unobserve(): void {}
    takeRecords(): IntersectionObserverEntry[] {
      return [];
    }
    readonly root = null;
    readonly rootMargin = "0px";
    readonly thresholds = [0];
  }

  class ResizeObserverMock {
    observe(): void {}
    disconnect(): void {}
    unobserve(): void {}
  }

  vi.stubGlobal("IntersectionObserver", IntersectionObserverMock);
  vi.stubGlobal("ResizeObserver", ResizeObserverMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("metric run counts", () => {
  it("shows one available-run count instead of selected and available totals", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const runs = ["run-a", "run-b", "run-c", "run-d"].map(runSummary);
    const component = mount(RunMetricsPanel, {
      target,
      props: {
        active: true,
        project: "demo",
        runs,
        selectedRunCount: runs.length,
        catalog: [{ key: "train/loss", run_ids: ["run-a", "run-b"] }],
        totalCount: 1,
        catalogLoading: false,
        catalogError: null,
        search: "",
        mode: "union",
        alignment: "step",
        after: null,
        nextAfter: null,
        cursorDepth: 0,
        backHistoryTruncated: false,
        histories: {},
        viewports: {},
        loadingMetrics: new Set<string>(),
        errors: {},
        onsearch: vi.fn(),
        onmodechange: vi.fn(),
        onalignmentchange: vi.fn(),
        onretrycatalog: vi.fn(),
        oncursor: vi.fn(),
        onretrymetric: vi.fn(),
        onvisibilitychange: vi.fn(),
        onviewportchange: vi.fn(),
      },
    });
    await tick();

    const heading = target.querySelector<HTMLElement>(".metric-chart-card .chart-heading")!;
    expect(heading.querySelector("strong")?.textContent).toBe("train/loss");
    expect(heading.querySelector("small")?.textContent?.trim()).toBe("2 runs");
    expect(heading.textContent).not.toContain("2/4 runs");
    expect(heading.textContent).not.toContain("4 runs");
    expect(target.querySelector(".metrics-toolbar")?.textContent?.replace(/\s+/g, " ")).toContain(
      "1 metric",
    );
    expect(target.querySelector(".metric-pagination")?.textContent?.replace(/\s+/g, " ")).toContain(
      "1–1 of 1 metric",
    );

    await unmount(component);
    target.remove();
  });
});

function runSummary(id: string): RunListItem {
  return {
    id,
    project_id: "project-demo",
    project: "demo",
    name: id,
    state: "finished",
    summary_truncated: false,
    document_revision: 1,
    metric_revision: 1,
    rich_data_revision: 0,
    created_at: "2026-08-30T00:00:00Z",
    updated_at: "2026-08-30T00:01:00Z",
    finished_at: "2026-08-30T00:01:00Z",
  };
}
