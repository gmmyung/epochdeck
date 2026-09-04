import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import type { ScaleMode } from "./chart-data";
import { lineDash, type PreparedMetricSeries } from "./chart-series";
import type { Frame, Viewport } from "./metric-chart-viewport";

type DisplayMode = "band" | "line";

type RenderOptions = {
  candidates: PreparedMetricSeries[];
  displayMode: DisplayMode;
  xScale: ScaleMode;
  yScale: ScaleMode;
  viewport: Viewport;
  highlightedRunId: string | null;
  formatX: (value: number) => string;
  formatY: (value: number) => string;
};

type FacetedValues = [number[], Array<number | null>];
type FacetedData = Array<null | FacetedValues>;

const PADDING = { top: 12, right: 18, bottom: 32, left: 58 } as const;
const CANVAS_PIXEL_BUDGET = 8_000_000;
const CANVAS_DIMENSION_LIMIT = 4_096;
const CANVAS_DPR_LIMIT = 2;

export function boundedCanvasPixelRatio(width: number, height: number): number {
  const safeWidth = Math.max(width, 1);
  const safeHeight = Math.max(height, 1);
  const requestedRatio = Math.min(window.devicePixelRatio || 1, CANVAS_DPR_LIMIT);
  const pixelBudgetRatio = Math.sqrt(CANVAS_PIXEL_BUDGET / (safeWidth * safeHeight));
  const dimensionRatio = Math.min(
    CANVAS_DIMENSION_LIMIT / safeWidth,
    CANVAS_DIMENSION_LIMIT / safeHeight,
  );
  return Math.max(0.01, Math.min(requestedRatio, pixelBudgetRatio, dimensionRatio));
}

export class UPlotRenderer {
  private plot: uPlot | null = null;
  private candidates: PreparedMetricSeries[] | null = null;
  private renderSignature = "";
  private runIds: Array<string | null> = [];
  private viewport: Viewport | null = null;
  private highlightedRunId: string | null = null;

  render(target: HTMLElement, options: RenderOptions): Frame {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const frame: Frame = {
      width,
      height,
      padding: PADDING,
      x: options.viewport.x,
      y: options.viewport.y,
    };
    const styles = getComputedStyle(target);
    const gridColor = styles.getPropertyValue("--chart-grid").trim() || "#d9dde0";
    const mutedColor = styles.getPropertyValue("--muted").trim() || "#596168";
    const signature = JSON.stringify([
      options.displayMode,
      options.xScale,
      options.yScale,
      gridColor,
      mutedColor,
    ]);

    if (this.candidates !== options.candidates || this.renderSignature !== signature) {
      this.rebuild(target, width, height, options, gridColor, mutedColor);
      this.candidates = options.candidates;
      this.renderSignature = signature;
      this.viewport = copyViewport(options.viewport);
      this.highlightedRunId = options.highlightedRunId;
    } else if (this.plot) {
      const resized = this.plot.width !== width || this.plot.height !== height;
      const xChanged = !sameDomain(this.viewport?.x, options.viewport.x);
      const yChanged = !sameDomain(this.viewport?.y, options.viewport.y);
      const highlightChanged = this.highlightedRunId !== options.highlightedRunId;

      if (highlightChanged) this.updateHighlight(options.highlightedRunId);
      if (resized || xChanged || yChanged) {
        this.plot.batch(() => {
          if (resized) this.plot?.setSize({ width, height });
          if (xChanged) {
            this.plot?.setScale("x", {
              min: options.viewport.x.minimum,
              max: options.viewport.x.maximum,
            });
          }
          if (yChanged) {
            this.plot?.setScale("y", {
              min: options.viewport.y.minimum,
              max: options.viewport.y.maximum,
            });
          }
        });
      } else if (highlightChanged) {
        this.plot.redraw();
      }
      this.viewport = copyViewport(options.viewport);
      this.highlightedRunId = options.highlightedRunId;
    }

    return frame;
  }

  destroy(): void {
    this.plot?.destroy();
    this.plot = null;
    this.candidates = null;
    this.renderSignature = "";
    this.runIds = [];
    this.viewport = null;
    this.highlightedRunId = null;
  }

  private rebuild(
    target: HTMLElement,
    width: number,
    height: number,
    options: RenderOptions,
    gridColor: string,
    mutedColor: string,
  ): void {
    this.destroy();
    const { data, series, bands, runIds } = buildPlot(options);
    this.runIds = runIds;
    const config: uPlot.Options = {
      mode: 2,
      width,
      height,
      padding: [PADDING.top, PADDING.right, 0, 0],
      legend: { show: false },
      cursor: { show: false, drag: { setScale: false, x: false, y: false } },
      scales: {
        x: scaleOptions(options.xScale, options.viewport.x.minimum, options.viewport.x.maximum),
        y: scaleOptions(options.yScale, options.viewport.y.minimum, options.viewport.y.maximum),
      },
      axes: [
        {
          scale: "x",
          side: 2,
          size: PADDING.bottom,
          stroke: mutedColor,
          font: "10px system-ui, sans-serif",
          grid: { stroke: gridColor, width: 1 },
          ticks: { show: false },
          values: (_plot, values) => values.map(options.formatX),
        },
        {
          scale: "y",
          side: 3,
          size: PADDING.left,
          stroke: mutedColor,
          font: "10px system-ui, sans-serif",
          grid: { stroke: gridColor, width: 1 },
          ticks: { show: false },
          values: (_plot, values) => values.map(options.formatY),
        },
      ],
      series: series as uPlot.Series[],
      bands,
    };

    if (!supportsCanvas()) {
      target.replaceChildren();
      return;
    }
    try {
      this.plot = new uPlot(config, data as unknown as uPlot.AlignedData, target);
    } catch (error) {
      target.replaceChildren();
      throw error;
    }
  }

  private updateHighlight(highlightedRunId: string | null): void {
    if (!this.plot) return;
    for (let index = 1; index < this.plot.series.length; index += 1) {
      const emphasized = highlightedRunId === null || this.runIds[index] === highlightedRunId;
      this.plot.series[index].alpha = emphasized ? 1 : 0.16;
    }
  }
}

function sameDomain(left: Viewport["x"] | undefined, right: Viewport["x"]): boolean {
  return left !== undefined && left.minimum === right.minimum && left.maximum === right.maximum;
}

function copyViewport(viewport: Viewport): Viewport {
  return { x: { ...viewport.x }, y: { ...viewport.y } };
}

function buildPlot(options: RenderOptions): {
  data: FacetedData;
  series: Array<uPlot.Series | null>;
  bands: uPlot.Band[];
  runIds: Array<string | null>;
} {
  const data: FacetedData = [null];
  const series: Array<uPlot.Series | null> = [null];
  const bands: uPlot.Band[] = [];
  const runIds: Array<string | null> = [null];

  for (const candidate of options.candidates) {
    const alpha =
      options.highlightedRunId === null || candidate.runId === options.highlightedRunId ? 1 : 0.16;
    if (options.displayMode === "band") {
      const lowerIndex = series.length;
      appendSeries(
        data,
        series,
        runIds,
        candidate,
        candidate.minimum,
        options.xScale,
        options.yScale,
        0,
        [],
        alpha,
      );
      const upperIndex = series.length;
      appendSeries(
        data,
        series,
        runIds,
        candidate,
        candidate.maximum,
        options.xScale,
        options.yScale,
        0,
        [],
        alpha,
      );
      bands.push({
        series: [upperIndex, lowerIndex],
        fill: colorWithAlpha(candidate.color, 0.13),
      });
    }
    appendSeries(
      data,
      series,
      runIds,
      candidate,
      candidate.smoothed,
      options.xScale,
      options.yScale,
      1.7,
      lineDash(candidate.pattern),
      alpha,
    );
  }

  return { data, series, bands, runIds };
}

function appendSeries(
  data: FacetedData,
  series: Array<uPlot.Series | null>,
  runIds: Array<string | null>,
  candidate: PreparedMetricSeries,
  values: Array<number | null>,
  xScale: ScaleMode,
  yScale: ScaleMode,
  width: number,
  dash: number[],
  alpha: number,
): void {
  data.push(facetedValues(candidate.x, values, xScale, yScale));
  series.push({
    label: candidate.runName,
    facets: [
      { scale: "x", auto: false, sorted: 1 },
      { scale: "y", auto: false, sorted: 0 },
    ],
    stroke: width === 0 ? "transparent" : candidate.color,
    width,
    dash,
    points: { show: false },
    spanGaps: false,
    alpha,
  });
  runIds.push(candidate.runId);
}

function facetedValues(
  xValues: number[],
  values: Array<number | null>,
  xScale: ScaleMode,
  yScale: ScaleMode,
): FacetedValues {
  const x: number[] = [];
  const y: Array<number | null> = [];
  for (let index = 0; index < xValues.length; index += 1) {
    const coordinate = xValues[index];
    if (!Number.isFinite(coordinate) || (xScale === "log" && coordinate <= 0)) continue;
    x.push(coordinate);
    const value = values[index];
    y.push(
      value !== null && Number.isFinite(value) && (yScale !== "log" || value > 0) ? value : null,
    );
  }
  return [x, y];
}

function scaleOptions(mode: ScaleMode, minimum: number, maximum: number): uPlot.Scale {
  return {
    time: false,
    auto: false,
    distr: mode === "log" ? 3 : 1,
    range: [minimum, maximum],
  };
}

function colorWithAlpha(color: string, opacity: number): string {
  const match = /^#([0-9a-f]{6})$/i.exec(color);
  if (!match) return color;
  const alpha = Math.round(255 * opacity)
    .toString(16)
    .padStart(2, "0");
  return `#${match[1]}${alpha}`;
}

function supportsCanvas(): boolean {
  return (
    typeof Path2D !== "undefined" && document.createElement("canvas").getContext("2d") !== null
  );
}
