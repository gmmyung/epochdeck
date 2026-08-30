import type { ChartHistory } from "./api";
import { closestPointIndex, smoothSeries, type ScaleMode, type SmoothingMode } from "./chart-data";

export type SeriesPattern = "solid" | "dash" | "dot" | "dash-dot";
export type XAlignment = "step" | "relative-step" | "elapsed-time";

export type MetricChartViewport = {
  minimum: number;
  maximum: number;
};

export type MetricChartSeries = {
  runId: string;
  runName: string;
  color: string;
  available: boolean;
  pattern?: SeriesPattern;
  history?: ChartHistory;
  historyResolved?: boolean;
  loading?: boolean;
};

export type PreparedMetricSeries = {
  runId: string;
  runName: string;
  color: string;
  pattern: SeriesPattern;
  loading: boolean;
  status: "ready" | "loading" | "not-loaded" | "no-data";
  buckets: number[];
  x: number[];
  steps: number[];
  timestamps: number[];
  raw: Array<number | null>;
  smoothed: Array<number | null>;
  minimum: Array<number | null>;
  maximum: Array<number | null>;
};

export type SeriesHoverPoint = {
  series: PreparedMetricSeries;
  index: number;
  x: number;
  step: number;
  timestamp: number;
  raw: number | null;
  smoothed: number;
  minimum: number | null;
  maximum: number | null;
};

const PATTERNS: SeriesPattern[] = ["solid", "dash", "dot", "dash-dot"];

export function prepareMetricSeries(
  input: MetricChartSeries,
  metric: string,
  smoothingMode: SmoothingMode,
  smoothingAmount: number,
): PreparedMetricSeries {
  const history = input.history?.metrics[metric];
  const steps = history?.last_step ?? [];
  const timestamps = history?.last_timestamp_ms ?? [];
  const x = history?.last_x ?? [];
  const buckets = history?.bucket ?? [];
  const raw = finiteValues(history?.last ?? [], x.length);
  const minimum = finiteValues(history?.minimum ?? [], x.length);
  const maximum = finiteValues(history?.maximum ?? [], x.length);
  const smoothingCoordinates =
    smoothingMode === "time-ema"
      ? x.map((_, index) => {
          const timestamp = timestamps[index];
          return Number.isFinite(timestamp) ? timestamp / 1_000 : x[index];
        })
      : x;
  const smoothed = smoothSeries(smoothingCoordinates, raw, smoothingMode, smoothingAmount);
  const hasData = x.some(
    (coordinate, index) => Number.isFinite(coordinate) && smoothed[index] !== null,
  );

  return {
    runId: input.runId,
    runName: input.runName,
    color: input.color,
    pattern: input.pattern ?? stableSeriesPattern(input.runId),
    loading: input.loading ?? false,
    status: hasData
      ? "ready"
      : !input.available
        ? "no-data"
        : input.loading
          ? "loading"
          : input.historyResolved
            ? "no-data"
            : input.history === undefined
              ? "not-loaded"
              : "no-data",
    buckets,
    x,
    steps,
    timestamps,
    raw,
    smoothed,
    minimum,
    maximum,
  };
}

export function contiguousBucketRanges(
  buckets: number[],
  valid: readonly boolean[],
): Array<{ start: number; end: number }> {
  const ranges: Array<{ start: number; end: number }> = [];
  let start: number | null = null;
  for (let index = 0; index < valid.length; index += 1) {
    const bucket = buckets[index];
    const usable = valid[index] && Number.isFinite(bucket);
    if (!usable) {
      if (start !== null) ranges.push({ start, end: index });
      start = null;
      continue;
    }
    if (start === null) {
      start = index;
      continue;
    }
    if (bucket !== buckets[index - 1] + 1) {
      ranges.push({ start, end: index });
      start = index;
    }
  }
  if (start !== null) ranges.push({ start, end: valid.length });
  return ranges;
}

export function closestSeriesPoints(
  series: PreparedMetricSeries[],
  target: number,
  xScale: ScaleMode,
  minimum: number,
  maximum: number,
  yScale: ScaleMode,
): SeriesHoverPoint[] {
  const points: SeriesHoverPoint[] = [];
  for (const candidate of series) {
    const index = closestPointIndex(
      candidate.x,
      candidate.smoothed,
      target,
      xScale,
      minimum,
      maximum,
      yScale,
    );
    if (index === null) continue;
    const smoothed = candidate.smoothed[index];
    if (smoothed === null || !Number.isFinite(smoothed)) continue;
    points.push({
      series: candidate,
      index,
      x: candidate.x[index],
      step: candidate.steps[index],
      timestamp: candidate.timestamps[index],
      raw: candidate.raw[index] ?? null,
      smoothed,
      minimum: candidate.minimum[index] ?? null,
      maximum: candidate.maximum[index] ?? null,
    });
  }
  return points;
}

export function stableSeriesPattern(runId: string): SeriesPattern {
  let hash = 2166136261;
  for (let index = 0; index < runId.length; index += 1) {
    hash ^= runId.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return PATTERNS[(hash >>> 0) % PATTERNS.length];
}

export function runSetIdentity(series: readonly Pick<MetricChartSeries, "runId">[]): string {
  return JSON.stringify(series.map(({ runId }) => runId).sort());
}

export function metricChartViewportKey(viewport: MetricChartViewport | null): string {
  return viewport === null ? "full" : JSON.stringify([viewport.minimum, viewport.maximum]);
}

export function lineDash(pattern: SeriesPattern): number[] {
  if (pattern === "dash") return [8, 5];
  if (pattern === "dot") return [2, 4];
  if (pattern === "dash-dot") return [9, 4, 2, 4];
  return [];
}

function finiteValues(values: number[], length: number): Array<number | null> {
  return Array.from({ length }, (_, index) => {
    const value = values[index];
    return Number.isFinite(value) ? value : null;
  });
}
