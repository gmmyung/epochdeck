// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import MediaTimeline from "./MediaTimeline.svelte";
import MetricChart from "./MetricChart.svelte";
import MetricChartSettings from "./MetricChartSettings.svelte";
import NavigationSidebar from "./NavigationSidebar.svelte";
import RunDocumentPanels from "./RunDocumentPanels.svelte";
import SelectControl from "./SelectControl.svelte";
import type { ChartHistory, RichValue, Run, RunListItem } from "./api";

beforeEach(() => {
  class IntersectionObserverMock {
    constructor(
      private readonly callback: IntersectionObserverCallback,
      _options?: IntersectionObserverInit,
    ) {}

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

describe("interactive dashboard components", () => {
  it("changes the selected video when the step slider commits", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const values = [videoValue("first", 10), videoValue("second", 20)];
    const component = mount(MediaTimeline, { target, props: { values } });
    await tick();

    expect(target.querySelector("video")?.getAttribute("src")).toContain("second");
    const slider = target.querySelector<HTMLInputElement>('input[type="range"]');
    expect(slider).not.toBeNull();
    slider!.value = "0";
    slider!.dispatchEvent(new Event("input", { bubbles: true }));
    slider!.dispatchEvent(new Event("change", { bubbles: true }));
    await tick();

    expect(target.querySelector("video")?.getAttribute("src")).toContain("first");
    expect(target.textContent).toContain("Step 10");
    await unmount(component);
    target.remove();
  });

  it("keeps chart failures local and exposes a working retry control", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const retry = vi.fn();
    const component = mount(MetricChart, {
      target,
      props: {
        metric: "train/loss",
        series: [],
        loadError: "query timed out",
        onretry: retry,
        onvisibilitychange: vi.fn(),
      },
    });
    await tick();

    const alert = target.querySelector<HTMLElement>('[role="alert"]');
    expect(alert?.textContent).toContain("query timed out");
    expect(target.textContent).toContain("History could not be loaded.");
    expect(target.textContent).not.toContain("Loading bounded histories");
    const retryButton = [...target.querySelectorAll("button")].find(
      (button) => button.textContent === "Retry",
    );
    retryButton?.click();
    expect(retry).toHaveBeenCalledWith("train/loss");
    expect(target.textContent).not.toContain("X alignment");

    await unmount(component);
    target.remove();
  });

  it("renders a terminal empty state after a bounded history response completes", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(MetricChart, {
      target,
      props: {
        metric: "train/loss",
        series: [
          {
            runId: "run-a",
            runName: "empty run",
            color: "#2766ad",
            available: true,
            historyResolved: true,
          },
        ],
        onvisibilitychange: vi.fn(),
      },
    });
    await tick();

    expect(target.textContent).toContain("This metric has no data in the visible runs.");
    expect(target.textContent).not.toContain("Loading bounded histories");
    expect(target.textContent).toContain("no metric");

    await unmount(component);
    target.remove();
  });

  it("keeps simple legend visibility controls and honors an external run highlight", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(MetricChart, {
      target,
      props: {
        metric: "train/loss",
        highlightedRunId: "run-b",
        series: [
          {
            runId: "run-a",
            runName: "Run A",
            color: "#2766ad",
            available: true,
            history: singlePointHistory("run-a", "train/loss", 1.5),
          },
          {
            runId: "run-b",
            runName: "Run B",
            color: "#d05a32",
            available: true,
            history: singlePointHistory("run-b", "train/loss", 2.5),
          },
          {
            runId: "run-without-loss",
            runName: "Run without loss",
            color: "#777777",
            available: false,
            historyResolved: true,
          },
        ],
        onvisibilitychange: vi.fn(),
      },
    });
    await tick();

    const entries = target.querySelectorAll<HTMLElement>(".legend-entry");
    expect(entries).toHaveLength(2);
    expect(entries[1].classList.contains("highlighted")).toBe(true);
    expect(target.querySelector('[aria-label="Hide Run without loss"]')).toBeNull();
    expect(target.textContent).not.toContain("Run without loss");
    expect(target.textContent?.toLocaleLowerCase()).not.toContain("solo");
    expect(target.querySelector(".chart-interaction-canvas")).not.toBeNull();

    const runA = target.querySelector<HTMLButtonElement>('[aria-label="Hide Run A (run-a)"]')!;
    runA.click();
    await tick();
    expect(target.querySelector('[aria-label="Show Run A (run-a)"]')).not.toBeNull();
    const showAll = [...target.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "show all",
    );
    expect(showAll).toBeDefined();
    showAll?.click();
    await tick();
    expect(target.querySelector('[aria-label="Hide Run A (run-a)"]')).not.toBeNull();
    expect(target.textContent).not.toContain("show all");

    await unmount(component);
    target.remove();
  });

  it("renders selector options in the styled dashboard popover", async () => {
    const change = vi.fn();
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(SelectControl, {
      target,
      props: {
        ariaLabel: "Axis scale",
        value: "linear",
        options: [
          { value: "linear", label: "Linear" },
          { value: "log", label: "Log" },
        ],
        onvaluechange: change,
      },
    });
    await tick();

    expect(target.querySelector("select")).toBeNull();
    const trigger = target.querySelector<HTMLButtonElement>('button[aria-haspopup="listbox"]')!;
    const accessibleName = trigger
      .getAttribute("aria-labelledby")!
      .split(" ")
      .map((id) => document.getElementById(id)?.textContent)
      .join(" ");
    expect(accessibleName).toBe("Axis scale Linear");
    trigger.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, button: 0 }));
    await tick();
    const listbox = document.body.querySelector<HTMLElement>('[role="listbox"]')!;
    expect(listbox.classList.contains("select-popover")).toBe(true);
    expect(
      document.body
        .querySelector<HTMLElement>('[role="option"][aria-label="Linear"]')
        ?.getAttribute("aria-selected"),
    ).toBe("true");

    document.body
      .querySelector<HTMLElement>('[role="option"][aria-label="Log"]')!
      .dispatchEvent(new PointerEvent("pointerup", { bubbles: true, button: 0 }));
    await tick();
    expect(change).toHaveBeenCalledWith("log");
    expect(document.body.querySelector('[role="listbox"]')?.getAttribute("data-state")).toBe(
      "closed",
    );
    expect(trigger.textContent).toContain("Log");

    await unmount(component);
    target.remove();
  });

  it("links sidebar hover and run style controls without changing selection", async () => {
    const run = runListItem();
    const hover = vi.fn();
    const styleChange = vi.fn();
    const resetStyle = vi.fn();
    const toggle = vi.fn();
    const choose = vi.fn();
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(NavigationSidebar, {
      target,
      props: {
        visibleProjects: [],
        selectedProject: "demo",
        projectSearch: "",
        projectCursor: null,
        projectWindowTruncated: false,
        loadingMoreProjects: false,
        projectError: null,
        reports: [],
        visibleReports: [],
        selectedReportId: null,
        reportSearch: "",
        reportCursor: null,
        reportWindowTruncated: false,
        loadingMoreReports: false,
        reportError: null,
        runs: [run],
        selectedRunIds: [run.id],
        primaryRunId: run.id,
        runSearch: "",
        runCursor: null,
        runWindowTruncated: false,
        loadingRuns: false,
        runError: null,
        selectionNotice: null,
        onchooseproject: vi.fn(),
        onloadprojects: vi.fn(),
        onchoosereport: vi.fn(),
        onloadreports: vi.fn(),
        onsearchruns: vi.fn(),
        onloadruns: vi.fn(),
        ontogglerun: toggle,
        onchooserun: choose,
        onhoverrun: hover,
        onrunstylechange: styleChange,
        onresetrunstyle: resetStyle,
      },
    });
    await tick();

    const row = target.querySelector<HTMLElement>(".run-list-row")!;
    row.dispatchEvent(new MouseEvent("mouseenter"));
    row.dispatchEvent(new MouseEvent("mouseleave"));
    expect(hover.mock.calls).toEqual([[run.id], [null]]);

    expect(target.querySelector(`[aria-label="Line color for ${run.name}"]`)).toBeNull();
    const styleMenu = target.querySelector<HTMLButtonElement>(
      `[aria-label="Configure chart style for ${run.name} (${run.id.slice(0, 8)})"]`,
    )!;
    styleMenu.click();
    await tick();
    expect(styleMenu.getAttribute("aria-expanded")).toBe("true");

    const color = target.querySelector<HTMLInputElement>(
      `[aria-label="Line color for ${run.name}"]`,
    )!;
    color.value = "#abcdef";
    color.dispatchEvent(new Event("change", { bubbles: true }));
    expect(styleChange).toHaveBeenCalledWith(run.id, expect.objectContaining({ color: "#abcdef" }));

    target.querySelector<HTMLInputElement>(`[aria-label="Dotted line for ${run.name}"]`)!.click();
    expect(styleChange).toHaveBeenCalledWith(run.id, expect.objectContaining({ pattern: "dot" }));
    target
      .querySelector<HTMLButtonElement>(`[aria-label="Reset chart style for ${run.name}"]`)!
      .click();
    expect(resetStyle).toHaveBeenCalledWith(run.id);
    expect(toggle).not.toHaveBeenCalled();
    expect(choose).not.toHaveBeenCalled();

    document.body.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    await tick();
    expect(styleMenu.getAttribute("aria-expanded")).toBe("false");
    expect(target.querySelector(`[aria-label="Line color for ${run.name}"]`)).toBeNull();

    await unmount(component);
    target.remove();
  });

  it("shows compact accessible series, value, and step hover data", async () => {
    class VisibleIntersectionObserver {
      constructor(private readonly callback: IntersectionObserverCallback) {}

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
    const arc = vi.fn();
    const context = new Proxy(
      { arc, measureText: () => ({ width: 16 }) },
      {
        get: (target, property) =>
          property in target ? target[property as keyof typeof target] : () => undefined,
        set: (target, property, value) => {
          Object.assign(target, { [property]: value });
          return true;
        },
      },
    ) as unknown as CanvasRenderingContext2D;
    vi.stubGlobal("IntersectionObserver", VisibleIntersectionObserver);
    const getContext = vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(context);
    const history: ChartHistory = {
      run_id: "run-a",
      step_min: 42,
      step_max: 42,
      bucket_count: 1,
      source_points: 1,
      source_last_sequence: 1,
      metrics: {
        "train/loss": {
          source_points: 1,
          bucket: [0],
          last_x: [42],
          last_step: [42],
          last_timestamp_ms: [1_000],
          minimum: [1],
          maximum: [2],
          last: [1.25],
        },
      },
    };
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(MetricChart, {
      target,
      props: {
        metric: "train/loss",
        series: [
          {
            runId: "run-a",
            runName: "policy baseline",
            color: "#2766ad",
            pattern: "solid",
            available: true,
            history,
          },
        ],
        onvisibilitychange: vi.fn(),
      },
    });
    await tick();
    await tick();

    const canvas = target.querySelector<HTMLCanvasElement>(".chart-interaction-canvas")!;
    canvas.dispatchEvent(new KeyboardEvent("keydown", { key: "Home", bubbles: true }));
    await tick();

    const tooltip = document.querySelector<HTMLElement>(".comparison-tooltip")!;
    const headers = [...tooltip.querySelectorAll("thead th")].map((header) =>
      header.textContent?.trim(),
    );
    expect(headers).toEqual(["Run", "Value", "Step"]);
    expect(tooltip.querySelector("caption")?.textContent).toContain(
      "Nearest value and source step",
    );
    expect(tooltip.querySelector("tbody tr")?.children).toHaveLength(3);
    expect(tooltip.textContent).toContain("policy baseline");
    expect(tooltip.textContent).toContain("1.25");
    expect(tooltip.textContent).toContain("42");
    expect(tooltip.textContent).not.toContain("Timestamp");
    const announcement = target
      .querySelector('[aria-live="polite"]')
      ?.textContent?.replace(/\s+/g, " ");
    expect(announcement).toContain("policy baseline, value 1.25, step 42");
    expect(arc).not.toHaveBeenCalled();

    await unmount(component);
    getContext.mockRestore();
    target.remove();
  });

  it("closes chart settings with Escape and applies axis changes", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const viewChange = vi.fn();
    const component = mount(MetricChartSettings, {
      target,
      props: {
        open: true,
        displayMode: "band",
        smoothingMode: "gaussian",
        smoothingAmount: 2,
        xAlignment: "step",
        xScale: "linear",
        yScale: "linear",
        xMinimum: "",
        xMaximum: "",
        yMinimum: "",
        yMaximum: "",
        axisWarning: null,
        onviewchange: viewChange,
      },
    });
    await tick();

    expect(target.querySelector("details")?.getAttribute("aria-keyshortcuts")).toBe("Escape");

    const xScaleLabel = [...target.querySelectorAll<HTMLElement>(".select-label")].find(
      (label) => label.textContent === "X axis scale",
    )!;
    const xScale = xScaleLabel.parentElement!.querySelector<HTMLButtonElement>(
      'button[aria-haspopup="listbox"]',
    )!;
    xScale.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, button: 0 }));
    await tick();
    const logOption = document.body.querySelector<HTMLElement>(
      '[role="option"][aria-label="Log"]',
    )!;
    logOption.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, button: 0 }));
    await tick();
    expect(target.querySelector("details")?.open).toBe(true);
    logOption.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, button: 0 }));
    await tick();
    expect(target.querySelector("details")?.open).toBe(true);
    expect(viewChange).toHaveBeenCalledOnce();

    const smoothingAmount = target.querySelector<HTMLInputElement>('input[type="number"]')!;
    expect(smoothingAmount.max).toBe("50");
    smoothingAmount.value = "500";
    smoothingAmount.dispatchEvent(new Event("input", { bubbles: true }));
    smoothingAmount.dispatchEvent(new Event("change", { bubbles: true }));
    await tick();
    expect(smoothingAmount.value).toBe("50");

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await tick();
    expect(target.querySelector("details")?.open).toBe(false);

    await unmount(component);
    target.remove();
  });

  it("explains that a truncated summary preview does not truncate metric history", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const run: Run = {
      id: "run-id",
      project_id: "project-id",
      project: "demo",
      name: "dense-run",
      state: "running",
      config: {},
      summary: { loss: 0.25 },
      explicit_summary: {},
      metric_summary: { loss: 0.25 },
      summary_truncated: true,
      document_revision: 0,
      metric_revision: 1,
      rich_data_revision: 0,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:01Z",
      finished_at: null,
    };
    const component = mount(RunDocumentPanels, {
      target,
      props: {
        run,
        activeTab: "summary",
        alerts: [],
        alertCursor: null,
        alertsTruncated: false,
        alertError: undefined,
        loadingMoreTab: null,
        onretryalerts: vi.fn(),
        onloadalerts: vi.fn(),
      },
    });
    await tick();

    expect(target.textContent).toContain("latest-value metric preview is limited to 256 keys");
    expect(target.textContent).toContain("Raw metric history");

    await unmount(component);
    target.remove();
  });
});

function videoValue(id: string, step: number): RichValue {
  return {
    id,
    run_id: "run-id",
    key: "train/rollout",
    kind: "video",
    step,
    timestamp_ms: step * 1_000,
    blob: {
      digest: id,
      size: 10,
      mime_type: "video/mp4",
      file_name: `${id}.mp4`,
    },
    metadata: {},
    created_at: "2026-01-01T00:00:00Z",
  };
}

function singlePointHistory(runId: string, metric: string, value: number): ChartHistory {
  return {
    run_id: runId,
    step_min: 1,
    step_max: 1,
    bucket_count: 1,
    source_points: 1,
    source_last_sequence: 1,
    metrics: {
      [metric]: {
        source_points: 1,
        bucket: [0],
        last_x: [1],
        last_step: [1],
        last_timestamp_ms: [1_000],
        minimum: [value],
        maximum: [value],
        last: [value],
      },
    },
  };
}

function runListItem(): RunListItem {
  return {
    id: "00000000-0000-7000-8000-000000000001",
    project_id: "00000000-0000-7000-8000-000000000010",
    project: "demo",
    name: "policy baseline",
    state: "finished",
    summary_truncated: false,
    document_revision: 1,
    metric_revision: 2,
    rich_data_revision: 3,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:01:00Z",
    finished_at: "2026-01-01T00:01:00Z",
  };
}
