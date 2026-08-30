export const MAX_SELECTED_RUNS = 12;
export const MAX_COMPARISON_CELLS = 20_000;
export const MAX_COMPARISON_SERIES = 32;
export const METRIC_CATALOG_PAGE_SIZE = 24;

export type MetricSetMode = "union" | "intersection";
export type RunAlignment = "step" | "relative-step" | "elapsed-time";

export type RunStyle = {
  color: string;
  pattern: "solid" | "dash" | "dot" | "dash-dot";
};

export type ComparisonBatchCandidate = {
  metric: string;
  runIds: string[];
};

export type ComparisonBatchPlan = {
  candidates: ComparisonBatchCandidate[];
  seriesCount: number;
  maxBuckets: number;
};

export type ComparisonCacheMetric = {
  metric: string;
  revisions: ReadonlyArray<readonly [runId: string, revision: number]>;
};

export type ComparisonUrlState<TTab extends string> = {
  project: string | null;
  reportId: string | null;
  runIds: string[];
  runSelectionSpecified: boolean;
  primaryRunId: string | null;
  tab: TTab;
  metricMode: MetricSetMode;
  search: string;
  metricAfter: string | null;
  alignment: RunAlignment;
  chartMetric: string | null;
  chartViewport: { minimum: number; maximum: number } | null;
};

const RUN_COLORS = [
  "#2766ad",
  "#d05a32",
  "#2f855a",
  "#8a5cad",
  "#bf8b17",
  "#16858c",
  "#c14980",
  "#64748b",
  "#5b6fc8",
  "#9a6b3f",
  "#3f8fbc",
  "#7a8240",
] as const;

const RUN_PATTERNS = ["solid", "dash", "dot", "dash-dot"] as const;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_PROJECT_NAME_BYTES = 128;
const MAX_SEARCH_BYTES = 256;
const MAX_METRIC_KEY_BYTES = 256;

export function normalizeRunSelection(
  requestedRunIds: readonly string[],
  availableRunIds: ReadonlySet<string>,
  requestedPrimaryRunId: string | null,
): { runIds: string[]; primaryRunId: string | null } {
  const seen = new Set<string>();
  const runIds: string[] = [];
  for (const runId of requestedRunIds) {
    if (runIds.length >= MAX_SELECTED_RUNS) break;
    if (!availableRunIds.has(runId) || seen.has(runId)) continue;
    seen.add(runId);
    runIds.push(runId);
  }
  const primaryRunId =
    requestedPrimaryRunId && seen.has(requestedPrimaryRunId)
      ? requestedPrimaryRunId
      : (runIds[0] ?? null);
  return { runIds, primaryRunId };
}

export function comparisonBucketBudget(seriesCount: number, requestedBuckets: number): number {
  if (!Number.isInteger(seriesCount) || seriesCount < 1) {
    throw new Error("comparison series count must be a positive integer");
  }
  if (!Number.isInteger(requestedBuckets) || requestedBuckets < 1) {
    throw new Error("requested bucket count must be a positive integer");
  }
  return Math.min(requestedBuckets, Math.floor(MAX_COMPARISON_CELLS / seriesCount));
}

export function planComparisonBatches(
  candidates: readonly ComparisonBatchCandidate[],
  requestedBuckets: number,
): ComparisonBatchPlan[] {
  const batches: ComparisonBatchPlan[] = [];
  let current: ComparisonBatchCandidate[] = [];
  let seriesCount = 0;
  const publish = () => {
    if (current.length === 0) return;
    batches.push({
      candidates: current,
      seriesCount,
      maxBuckets: comparisonBucketBudget(seriesCount, requestedBuckets),
    });
    current = [];
    seriesCount = 0;
  };
  for (const candidate of candidates) {
    const runIds = [...new Set(candidate.runIds)];
    if (runIds.length < 1 || runIds.length > MAX_COMPARISON_SERIES) {
      throw new Error(`comparison metric ${candidate.metric} has an invalid series count`);
    }
    if (seriesCount + runIds.length > MAX_COMPARISON_SERIES) publish();
    current.push({ metric: candidate.metric, runIds });
    seriesCount += runIds.length;
  }
  publish();
  return batches;
}

export function comparisonCacheKey(
  project: string,
  alignment: RunAlignment,
  maxBuckets: number,
  viewport: { minimum: number; maximum: number } | null,
  metrics: readonly ComparisonCacheMetric[],
): string {
  return JSON.stringify({ project, alignment, maxBuckets, viewport, metrics });
}

export function runStyle(runId: string): RunStyle {
  const hash = stableHash(runId);
  return {
    color: RUN_COLORS[hash % RUN_COLORS.length],
    pattern: RUN_PATTERNS[Math.floor(hash / RUN_COLORS.length) % RUN_PATTERNS.length],
  };
}

export function readComparisonUrl<TTab extends string>(
  url: URL,
  validTabs: ReadonlySet<TTab>,
  defaultTab: TTab,
): ComparisonUrlState<TTab> {
  const requestedTab = url.searchParams.get("tab") as TTab | null;
  const requestedMode = url.searchParams.get("metricMode");
  const requestedAlignment = url.searchParams.get("alignment");
  const chartMetric = cleanBoundedValue(url.searchParams.get("chart"), MAX_METRIC_KEY_BYTES);
  const hasChartBounds = url.searchParams.has("xmin") && url.searchParams.has("xmax");
  const chartMinimum = Number(url.searchParams.get("xmin"));
  const chartMaximum = Number(url.searchParams.get("xmax"));
  const chartViewport =
    chartMetric &&
    hasChartBounds &&
    Number.isFinite(chartMinimum) &&
    Number.isFinite(chartMaximum) &&
    chartMinimum < chartMaximum
      ? { minimum: chartMinimum, maximum: chartMaximum }
      : null;
  return {
    project: cleanBoundedValue(url.searchParams.get("project"), MAX_PROJECT_NAME_BYTES),
    reportId: cleanIdentifier(url.searchParams.get("report")),
    runSelectionSpecified: url.searchParams.has("run"),
    runIds: boundedRunIds(url.searchParams.getAll("run")),
    primaryRunId: cleanIdentifier(url.searchParams.get("primary")),
    tab: requestedTab && validTabs.has(requestedTab) ? requestedTab : defaultTab,
    metricMode: requestedMode === "intersection" ? "intersection" : "union",
    search: cleanBoundedSearch(url.searchParams.get("search")),
    metricAfter: cleanBoundedValue(url.searchParams.get("metric_after"), MAX_METRIC_KEY_BYTES),
    alignment:
      requestedAlignment === "relative-step" || requestedAlignment === "elapsed-time"
        ? requestedAlignment
        : "step",
    chartMetric: chartViewport ? chartMetric : null,
    chartViewport,
  };
}

export function writeComparisonUrl<TTab extends string>(
  url: URL,
  state: ComparisonUrlState<TTab>,
): URL {
  const next = new URL(url);
  for (const key of [
    "project",
    "report",
    "run",
    "primary",
    "tab",
    "metricMode",
    "search",
    "metric_after",
    "alignment",
    "chart",
    "xmin",
    "xmax",
  ]) {
    next.searchParams.delete(key);
  }
  if (state.project) next.searchParams.set("project", state.project);
  if (state.reportId) next.searchParams.set("report", state.reportId);
  if (state.runIds.length === 0) next.searchParams.append("run", "");
  else for (const runId of state.runIds) next.searchParams.append("run", runId);
  if (state.primaryRunId) next.searchParams.set("primary", state.primaryRunId);
  next.searchParams.set("tab", state.tab);
  next.searchParams.set("metricMode", state.metricMode);
  if (state.search) next.searchParams.set("search", state.search);
  if (state.metricAfter) next.searchParams.set("metric_after", state.metricAfter);
  next.searchParams.set("alignment", state.alignment);
  if (state.chartMetric && state.chartViewport) {
    next.searchParams.set("chart", state.chartMetric);
    next.searchParams.set("xmin", String(state.chartViewport.minimum));
    next.searchParams.set("xmax", String(state.chartViewport.maximum));
  }
  return next;
}

function cleanValue(value: string | null): string | null {
  const cleaned = value?.trim();
  return cleaned ? cleaned : null;
}

function cleanBoundedValue(value: string | null, maxBytes: number): string | null {
  const cleaned = cleanValue(value);
  if (!cleaned || utf8Bytes(cleaned) > maxBytes || hasControlCharacter(cleaned)) return null;
  return cleaned;
}

function cleanIdentifier(value: string | null): string | null {
  const cleaned = cleanValue(value);
  return cleaned && UUID_PATTERN.test(cleaned) ? cleaned : null;
}

function boundedRunIds(values: readonly string[]): string[] {
  const unique = new Set<string>();
  for (const value of values) {
    const runId = cleanIdentifier(value);
    if (runId) unique.add(runId);
    if (unique.size >= MAX_SELECTED_RUNS) break;
  }
  return [...unique];
}

function cleanBoundedSearch(value: string | null): string {
  if (value === null || utf8Bytes(value) > MAX_SEARCH_BYTES || hasControlCharacter(value))
    return "";
  return value;
}

function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function hasControlCharacter(value: string): boolean {
  return /[\u0000-\u001f\u007f-\u009f]/.test(value);
}

function stableHash(value: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}
