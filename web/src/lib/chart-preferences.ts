import type { ScaleMode, SmoothingMode } from "./chart-data";

export type ChartPreferences = {
  displayMode: "band" | "line";
  smoothingMode: SmoothingMode;
  smoothingAmount: number;
  xScale: ScaleMode;
  yScale: ScaleMode;
  xMinimum: string;
  xMaximum: string;
  yMinimum: string;
  yMaximum: string;
};

const MAX_CACHED_CHART_PREFERENCES = 512;
const preferences = new Map<string, ChartPreferences>();

export function chartPreferenceIdentity(project: string, metric: string): string {
  return JSON.stringify([project, metric]);
}

export function readChartPreferences(identity: string): ChartPreferences | undefined {
  const value = preferences.get(identity);
  if (!value) return undefined;
  preferences.delete(identity);
  preferences.set(identity, value);
  return { ...value };
}

export function rememberChartPreferences(identity: string, value: ChartPreferences): void {
  preferences.delete(identity);
  preferences.set(identity, { ...value });
  while (preferences.size > MAX_CACHED_CHART_PREFERENCES) {
    const oldest = preferences.keys().next().value;
    if (oldest === undefined) break;
    preferences.delete(oldest);
  }
}
