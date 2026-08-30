// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import MediaTimeline from "./MediaTimeline.svelte";
import MetricChart from "./MetricChart.svelte";
import MetricChartSettings from "./MetricChartSettings.svelte";
import RunDocumentPanels from "./RunDocumentPanels.svelte";
import type { ChartHistory, RichValue, Run } from "./api";

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
    const context = new Proxy(
      { measureText: () => ({ width: 16 }) },
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

    const xScale = target.querySelector<HTMLSelectElement>('[aria-label="X axis scale"]')!;
    xScale.value = "log";
    xScale.dispatchEvent(new Event("change", { bubbles: true }));
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
