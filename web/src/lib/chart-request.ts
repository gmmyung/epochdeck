import type { ChartHistoryViewport } from "./api";

export function normalizeChartViewport(
  stepMin: number | null,
  stepMax: number | null,
): ChartHistoryViewport | null {
  if (stepMin === null || stepMax === null) return null;
  const lower = Math.min(stepMin, stepMax);
  const upper = Math.max(stepMin, stepMax);
  const minimum = Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.floor(lower)));
  const maximum = Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.ceil(upper)));
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum)) return null;
  return { stepMin: minimum, stepMax: maximum };
}

export function chartViewportKey(viewport: ChartHistoryViewport | null): string {
  return viewport ? `${viewport.stepMin}:${viewport.stepMax}` : "all";
}

export function metricChartRequestKey(
  revision: number,
  viewport: ChartHistoryViewport | null,
): string {
  return `${revision}:${chartViewportKey(viewport)}`;
}
