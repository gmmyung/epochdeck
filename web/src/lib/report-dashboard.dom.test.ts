// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Report, ReportPanel, RunListItem } from "./api";
import ReportDashboard from "./ReportDashboard.svelte";

beforeEach(() => {
  class IntersectionObserverMock {
    constructor(
      private readonly callback: IntersectionObserverCallback,
      _options?: IntersectionObserverInit,
    ) {}

    observe(target: Element): void {
      this.callback(
        [{ target, isIntersecting: true, intersectionRatio: 1 } as IntersectionObserverEntry],
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
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      matches: false,
      media: "",
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(() => true),
    })),
  );
  window.matchMedia = globalThis.matchMedia;
});

describe("ReportDashboard pagination", () => {
  it("mounts at most 24 charts and clears page visibility before mounting the next page", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const visibilityChange = vi.fn();
    const panels = Array.from({ length: 13 }, (_, index) => metricPanel(index));
    const component = mount(ReportDashboard, {
      target,
      props: {
        report: reportWithPanels(panels),
        runs: [runListItem()],
        histories: {},
        viewports: {},
        loadingMetrics: new Set<string>(),
        errors: {},
        onvisibilitychange: visibilityChange,
        onviewportchange: vi.fn(),
        onretry: vi.fn(),
      },
    });
    await tick();

    expect(target.querySelectorAll('[aria-label$="metric comparison panel"]')).toHaveLength(24);
    expect(target.querySelector('[aria-label="panel-11 report panel"]')).not.toBeNull();
    expect(target.querySelector('[aria-label="panel-12 report panel"]')).toBeNull();
    expect(normalizedText(target.querySelector('[role="status"]'))).toBe(
      "Page 1 of 2 · 12 panels · 24 charts",
    );
    expect(visibilityChange.mock.calls.filter((call) => call[2] === true)).toHaveLength(24);

    target.querySelector<HTMLButtonElement>('[aria-label="Next report page"]')!.click();
    await tick();

    expect(target.querySelectorAll('[aria-label$="metric comparison panel"]')).toHaveLength(2);
    expect(target.querySelector('[aria-label="panel-0 report panel"]')).toBeNull();
    expect(target.querySelector('[aria-label="panel-12 report panel"]')).not.toBeNull();
    expect(normalizedText(target.querySelector('[role="status"]'))).toBe(
      "Page 2 of 2 · 1 panel · 2 charts",
    );
    expect(visibilityChange.mock.calls.filter((call) => call[2] === false)).toHaveLength(24);
    expect(
      visibilityChange.mock.calls
        .filter((call) => call[2] === false)
        .map((call) => (call[0] as ReportPanel).id),
    ).not.toContain("panel-12");
    expect(
      target.querySelector<HTMLButtonElement>('[aria-label="Next report page"]')?.disabled,
    ).toBe(true);
    expect(
      target.querySelector<HTMLButtonElement>('[aria-label="Previous report page"]')?.disabled,
    ).toBe(false);

    await unmount(component);
    target.remove();
  });
});

function metricPanel(index: number): ReportPanel {
  return {
    id: `panel-${index}`,
    title: `panel-${index}`,
    kind: "metric",
    run_id: "run-id",
    metric_keys: [`metric-${index}-0`, `metric-${index}-1`],
    markdown: null,
    width: 1,
    height: 320,
  };
}

function reportWithPanels(panels: ReportPanel[]): Report {
  return {
    id: "report-id",
    project_id: "project-id",
    project: "demo",
    name: "Dense report",
    description: null,
    layout: { columns: 4, panels },
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
}

function runListItem(): RunListItem {
  return {
    id: "run-id",
    project_id: "project-id",
    project: "demo",
    name: "Dense run",
    state: "finished",
    summary_truncated: false,
    document_revision: 1,
    metric_revision: 1,
    rich_data_revision: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    finished_at: "2026-01-01T00:00:00Z",
  };
}

function normalizedText(element: Element | null): string {
  return element?.textContent?.replace(/\s+/g, " ").trim() ?? "";
}
