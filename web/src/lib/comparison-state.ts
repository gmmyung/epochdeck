export const MAX_SELECTED_RUNS = 12;
export const MAX_COMPARISON_CELLS = 20_000;
export const MAX_COMPARISON_SERIES = 32;
export const METRIC_CHART_PAGE_SIZE = 24;

export type MetricSetMode = "union" | "intersection";
export type RunAlignment = "step" | "relative-step" | "elapsed-time";

export type RunStyle = {
  color: string;
  pattern: "solid" | "dash" | "dot" | "dash-dot";
};

export type MetricAvailability = {
  key: string;
  available: number;
  total: number;
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
  runIds: string[];
  runSelectionSpecified: boolean;
  primaryRunId: string | null;
  tab: TTab;
  metricMode: MetricSetMode;
  search: string;
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

export function metricPage<T>(
  values: readonly T[],
  requestedPage: number,
  pageSize = METRIC_CHART_PAGE_SIZE,
): { page: number; pageCount: number; values: T[] } {
  if (!Number.isInteger(pageSize) || pageSize < 1) {
    throw new Error("metric page size must be a positive integer");
  }
  const pageCount = Math.max(1, Math.ceil(values.length / pageSize));
  const normalizedPage = Number.isFinite(requestedPage) ? Math.floor(requestedPage) : 0;
  const page = Math.max(0, Math.min(normalizedPage, pageCount - 1));
  const start = page * pageSize;
  return { page, pageCount, values: values.slice(start, start + pageSize) };
}

export function metricAvailability(
  runIds: readonly string[],
  keysByRun: Readonly<Record<string, readonly string[]>>,
  mode: MetricSetMode,
): MetricAvailability[] {
  if (runIds.length === 0) return [];
  const counts = new Map<string, number>();
  for (const runId of runIds) {
    for (const key of new Set(keysByRun[runId] ?? [])) {
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
  }
  return [...counts]
    .filter(([, available]) => mode === "union" || available === runIds.length)
    .map(([key, available]) => ({ key, available, total: runIds.length }))
    .sort((left, right) => left.key.localeCompare(right.key, undefined, { numeric: true }));
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
  const chartMetric = cleanValue(url.searchParams.get("chart"));
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
    project: cleanValue(url.searchParams.get("project")),
    runSelectionSpecified: url.searchParams.has("run"),
    runIds: url.searchParams.getAll("run").flatMap((runId) => {
      const cleaned = cleanValue(runId);
      return cleaned ? [cleaned] : [];
    }),
    primaryRunId: cleanValue(url.searchParams.get("primary")),
    tab: requestedTab && validTabs.has(requestedTab) ? requestedTab : defaultTab,
    metricMode: requestedMode === "intersection" ? "intersection" : "union",
    search: url.searchParams.get("search") ?? "",
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
    "run",
    "primary",
    "tab",
    "metricMode",
    "search",
    "alignment",
    "chart",
    "xmin",
    "xmax",
  ]) {
    next.searchParams.delete(key);
  }
  if (state.project) next.searchParams.set("project", state.project);
  if (state.runIds.length === 0) next.searchParams.append("run", "");
  else for (const runId of state.runIds) next.searchParams.append("run", runId);
  if (state.primaryRunId) next.searchParams.set("primary", state.primaryRunId);
  next.searchParams.set("tab", state.tab);
  next.searchParams.set("metricMode", state.metricMode);
  if (state.search) next.searchParams.set("search", state.search);
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

function stableHash(value: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}
