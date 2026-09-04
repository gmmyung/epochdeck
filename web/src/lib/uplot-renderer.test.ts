// @vitest-environment happy-dom

import { afterEach, describe, expect, it, vi } from "vitest";

import type { PreparedMetricSeries } from "./chart-series";
import type { Viewport } from "./metric-chart-viewport";
import { UPlotRenderer } from "./uplot-renderer";

const plotCalls = vi.hoisted(() => [] as string[]);
const plotData = vi.hoisted(() => ({ value: null as unknown }));
const plotCanvas = vi.hoisted(() => ({ value: null as HTMLCanvasElement | null }));

vi.mock("uplot", () => ({
  default: class MockUPlot {
    static pxRatio = 1;

    width: number;
    height: number;
    series: Array<{ alpha?: number }>;
    ctx: CanvasRenderingContext2D;

    constructor(
      options: {
        width: number;
        height: number;
        series: Array<{ alpha?: number } | null>;
        hooks?: { drawClear?: Array<(plot: MockUPlot) => void> };
      },
      data: unknown,
    ) {
      this.width = options.width;
      this.height = options.height;
      this.series = options.series.map((entry) => ({ ...(entry ?? {}) }));
      const canvas = document.createElement("canvas");
      canvas.width = options.width;
      canvas.height = options.height;
      this.ctx = {
        canvas,
        setTransform: (...values: number[]) => plotCalls.push(`transform:${values.join(":")}`),
      } as unknown as CanvasRenderingContext2D;
      plotData.value = data;
      plotCanvas.value = canvas;
      options.hooks?.drawClear?.forEach((hook) => hook(this));
    }

    batch(operation: () => void): void {
      plotCalls.push("batch:start");
      operation();
      plotCalls.push("batch:end");
    }

    setScale(key: string, domain: { min: number; max: number }): void {
      plotCalls.push(`scale:${key}:${domain.min}:${domain.max}`);
    }

    setSize(size: { width: number; height: number }): void {
      this.width = size.width;
      this.height = size.height;
      plotCalls.push(`size:${size.width}:${size.height}`);
    }

    redraw(): void {
      plotCalls.push("redraw");
    }

    destroy(): void {}
  },
}));

const series: PreparedMetricSeries[] = [
  {
    runId: "run-1",
    runName: "first run",
    color: "#123456",
    pattern: "solid",
    available: true,
    loading: false,
    status: "ready",
    buckets: [0, 7],
    x: [0, 100],
    steps: [0, 100],
    timestamps: [0, 100],
    raw: [1, 2],
    smoothed: [1, 2],
    minimum: [1, 2],
    maximum: [1, 2],
  },
];

const fullViewport: Viewport = {
  x: { minimum: 0, maximum: 100 },
  y: { minimum: 0, maximum: 3 },
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  plotCalls.length = 0;
  plotData.value = null;
  plotCanvas.value = null;
});

describe("uPlot renderer", () => {
  it("commits an x-only viewport change without a redraw that restores the old scale", () => {
    vi.stubGlobal("Path2D", class Path2DMock {});
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(
      {} as CanvasRenderingContext2D,
    );
    const target = document.createElement("div");
    Object.defineProperties(target, {
      clientWidth: { value: 500 },
      clientHeight: { value: 260 },
    });
    const renderer = new UPlotRenderer();
    const render = (viewport: Viewport, highlightedRunId: string | null = null) =>
      renderer.render(target, {
        candidates: series,
        displayMode: "line",
        xScale: "linear",
        yScale: "linear",
        viewport,
        highlightedRunId,
        formatX: String,
        formatY: String,
      });

    render(fullViewport);
    expect(plotData.value).toEqual([
      null,
      [
        [0, 100],
        [1, 2],
      ],
    ]);
    expect(plotCanvas.value?.width).toBe(1_000);
    expect(plotCanvas.value?.height).toBe(520);
    expect(plotCalls).toContain("transform:2:0:0:2:0:0");
    plotCalls.length = 0;
    render({ x: { minimum: 20, maximum: 80 }, y: fullViewport.y });

    expect(plotCalls).toEqual(["batch:start", "scale:x:20:80", "batch:end"]);

    plotCalls.length = 0;
    render({ x: { minimum: 20, maximum: 80 }, y: fullViewport.y }, "run-1");
    expect(plotCalls).toEqual(["redraw"]);
  });
});
