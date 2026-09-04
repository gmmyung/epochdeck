import { containsControlCharacter, utf8ByteLength } from "./text-validation";

export type Health = {
  service: string;
  version: string;
  status: "healthy" | "unhealthy";
};

export type DashboardConfig = {
  logo_url: string | null;
  favicon_url: string | null;
  accent_color: string;
};

export type Project = {
  id: string;
  name: string;
  created_at: string;
  run_count: number;
  mutation_token: string;
};

export type Run = {
  id: string;
  project_id: string;
  project: string;
  name: string;
  state: "running" | "finished";
  config: Record<string, unknown>;
  summary: Record<string, unknown>;
  explicit_summary: Record<string, unknown>;
  metric_summary: Record<string, unknown>;
  summary_truncated: boolean;
  document_revision: number;
  metric_revision: number;
  rich_data_revision: number;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
};

export type RunListItem = Omit<Run, "config" | "summary" | "explicit_summary" | "metric_summary">;

type ChartMetricHistory = {
  source_points: number;
  bucket: number[];
  last_x: number[];
  last_step: number[];
  last_timestamp_ms: number[];
  minimum: number[];
  maximum: number[];
  last: number[];
};

export type ChartHistory = {
  run_id: string;
  step_min: number | null;
  step_max: number | null;
  bucket_count: number;
  source_points: number;
  source_last_sequence: number | null;
  metrics: Record<string, ChartMetricHistory>;
};

export type ChartHistoryViewport = {
  stepMin: number;
  stepMax: number;
};

type ChartHistoryOptions = {
  maxBuckets?: number;
  viewport?: ChartHistoryViewport;
  signal?: AbortSignal;
};

type ComparisonAlignment = "step" | "relative_step" | "elapsed_time";

export type MetricCatalogEntry = {
  key: string;
  run_ids: string[];
};

type MetricCatalogPage = {
  items: MetricCatalogEntry[];
  nextAfter: string | null;
  totalCount: number;
};

type ComparisonChartSeriesRequest = {
  run_id: string;
  key: string;
};

type ComparisonChartSeries = ChartMetricHistory & {
  run_id: string;
  key: string;
  last_x: number[];
};

export type ComparisonChartHistory = {
  project: string;
  alignment: ComparisonAlignment;
  x_min: number | null;
  x_max: number | null;
  bucket_count: number;
  runs: Array<{ run_id: string; source_last_sequence: number | null }>;
  series: ComparisonChartSeries[];
};

type ComparisonChartHistoryOptions = {
  alignment: ComparisonAlignment;
  maxBuckets?: number;
  viewport?: { minimum: number; maximum: number };
  signal?: AbortSignal;
};

export type Alert = {
  id: string;
  run_id: string;
  title: string;
  text: string;
  level: "info" | "warn" | "error";
  step: number | null;
  timestamp_ms: number;
  created_at: string;
};

type BlobRef = {
  digest: string;
  size: number;
  mime_type: string;
  file_name: string | null;
};

export type RichValue = {
  id: string;
  run_id: string;
  key: string;
  kind: "image" | "audio" | "video" | "table" | "histogram";
  step: number;
  timestamp_ms: number;
  blob: BlobRef | null;
  metadata: Record<string, unknown>;
  created_at: string;
};

export type RichValueSummary = Omit<RichValue, "metadata">;

export type RichValueKeySummary = {
  key: string;
  count: number;
  latest: RichValueSummary;
};

export type ArtifactEntry = {
  path: string;
  blob: BlobRef;
};

export type Artifact = {
  id: string;
  project_id: string;
  project: string;
  name: string;
  type: string;
  version: number;
  description: string | null;
  metadata: Record<string, unknown>;
  aliases: string[];
  entries: ArtifactEntry[];
  created_by_run: string;
  created_at: string;
};

export type ArtifactSummary = Pick<
  Artifact,
  "id" | "project_id" | "project" | "name" | "type" | "version" | "created_by_run" | "created_at"
> & {
  entry_count: number;
};

export type RunArtifact = {
  artifact: ArtifactSummary;
  relation: "input" | "output";
};

export type CursorPage<T> = {
  items: T[];
  nextBefore: string | null;
};

export type RunArtifactCursor = {
  before: string;
  relation: RunArtifact["relation"];
};

export type RunArtifactPage = {
  items: RunArtifact[];
  nextCursor: RunArtifactCursor | null;
};

export class EpochDeckApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "EpochDeckApiError";
  }
}

export type ReportPanel = {
  id: string;
  title: string;
  kind: "metric" | "markdown";
  run_id: string | null;
  metric_keys: string[];
  markdown: string | null;
  width: number;
  height: number;
};

export type Report = {
  id: string;
  project_id: string;
  project: string;
  name: string;
  description: string | null;
  layout: {
    columns: number;
    panels: ReportPanel[];
  };
  created_at: string;
  updated_at: string;
};

export type ReportSummary = Pick<
  Report,
  "id" | "project_id" | "project" | "name" | "created_at" | "updated_at"
>;

export function getHealth(signal?: AbortSignal): Promise<Health> {
  return getJson<Health>("/api/v1/health", signal);
}

export function getDashboardConfig(signal?: AbortSignal): Promise<DashboardConfig> {
  return getJson<DashboardConfig>("/api/v1/dashboard/config", signal);
}

export async function getProjectPage(
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<Project>> {
  const query = cursorQuery(before);
  const result = await getJson<{ projects: Project[]; next_before: string | null }>(
    `/api/v1/projects?${query}`,
    signal,
  );
  return { items: result.projects, nextBefore: result.next_before };
}

export function getProject(project: string, signal?: AbortSignal): Promise<Project> {
  return getJson<Project>(`/api/v1/projects/${encodeURIComponent(project)}`, signal);
}

export async function getRunPage(
  project: string,
  search = "",
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<RunListItem>> {
  const query = cursorQuery(before);
  const boundedSearch = validatedSearch(search, "run search");
  if (boundedSearch) query.set("q", boundedSearch);
  const result = await getJson<{ runs: RunListItem[]; next_before: string | null }>(
    `/api/v1/projects/${encodeURIComponent(project)}/runs?${query}`,
    signal,
  );
  return { items: result.runs, nextBefore: result.next_before };
}

export async function getRunSummariesByIds(
  project: string,
  runIds: readonly string[],
  signal?: AbortSignal,
): Promise<RunListItem[]> {
  if (runIds.length === 0) return [];
  if (runIds.length > 32 || new Set(runIds).size !== runIds.length) {
    throw new RangeError("run summary queries require 1 to 32 unique run IDs");
  }
  const result = await requestJson<{ runs: RunListItem[]; next_before: null }>(
    "/api/v1/query/runs",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ project, run_ids: runIds, limit: runIds.length }),
      signal,
    },
  );
  return result.runs;
}

export async function getReportPage(
  project: string,
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<ReportSummary>> {
  const query = cursorQuery(before);
  const result = await getJson<{ reports: ReportSummary[]; next_before: string | null }>(
    `/api/v1/projects/${encodeURIComponent(project)}/reports?${query}`,
    signal,
  );
  return { items: result.reports, nextBefore: result.next_before };
}

export function getRun(runId: string, signal?: AbortSignal): Promise<Run> {
  return getJson<Run>(`/api/v1/runs/${encodeURIComponent(runId)}`, signal);
}

export function getReport(reportId: string, signal?: AbortSignal): Promise<Report> {
  return getJson<Report>(`/api/v1/reports/${encodeURIComponent(reportId)}`, signal);
}

export async function getProjectMetricCatalogPage(
  project: string,
  runIds: string[],
  mode: "union" | "intersection",
  search: string,
  after?: string,
  limit = 24,
  signal?: AbortSignal,
): Promise<MetricCatalogPage> {
  const boundedSearch = validatedSearch(search, "metric search");
  const result = await requestJson<{
    keys: MetricCatalogEntry[];
    next_after: string | null;
    total_count: number;
  }>(`/api/v1/projects/${encodeURIComponent(project)}/metrics/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      run_ids: runIds,
      mode,
      ...(boundedSearch ? { search: boundedSearch } : {}),
      ...(after ? { after } : {}),
      limit,
    }),
    signal,
  });
  return {
    items: result.keys,
    nextAfter: result.next_after,
    totalCount: result.total_count,
  };
}

export async function getAlertPage(
  runId: string,
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<Alert>> {
  const query = cursorQuery(before);
  const result = await getJson<{ alerts: Alert[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/alerts?${query}`,
    signal,
  );
  return { items: result.alerts, nextBefore: result.next_before };
}

export async function getRichValueKeyPage(
  runId: string,
  after?: string,
  signal?: AbortSignal,
): Promise<{ items: RichValueKeySummary[]; nextAfter: string | null }> {
  const query = new URLSearchParams({ limit: "100" });
  if (after) query.set("after", after);
  const result = await getJson<{ keys: RichValueKeySummary[]; next_after: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/rich-values/keys?${query}`,
    signal,
  );
  return { items: result.keys, nextAfter: result.next_after };
}

export async function getRichValuePage(
  runId: string,
  key: string,
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<RichValueSummary>> {
  const query = cursorQuery(before);
  query.set("key", key);
  const result = await getJson<{ values: RichValueSummary[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/rich-values?${query}`,
    signal,
  );
  return { items: result.values, nextBefore: result.next_before };
}

export function getRichValue(valueId: string, signal?: AbortSignal): Promise<RichValue> {
  return getJson<RichValue>(`/api/v1/rich-values/${encodeURIComponent(valueId)}`, signal);
}

export function blobUrl(blob: BlobRef): string {
  const query = new URLSearchParams({ mime: blob.mime_type });
  return `/api/v1/blobs/${encodeURIComponent(blob.digest)}?${query}`;
}

export async function getRunArtifactPage(
  runId: string,
  cursor?: RunArtifactCursor,
  signal?: AbortSignal,
): Promise<RunArtifactPage> {
  const query = new URLSearchParams({ limit: "100" });
  if (cursor) {
    query.set("before", cursor.before);
    query.set("before_relation", cursor.relation);
  }
  const result = await getJson<{
    artifacts: RunArtifact[];
    next_before: string | null;
    next_before_relation: RunArtifact["relation"] | null;
  }>(`/api/v1/runs/${encodeURIComponent(runId)}/artifacts?${query}`, signal);
  if ((result.next_before === null) !== (result.next_before_relation === null)) {
    throw new Error("EpochDeck returned an incomplete artifact cursor");
  }
  return {
    items: result.artifacts,
    nextCursor:
      result.next_before && result.next_before_relation
        ? { before: result.next_before, relation: result.next_before_relation }
        : null,
  };
}

export function getArtifact(artifactId: string, signal?: AbortSignal): Promise<Artifact> {
  return getJson<Artifact>(`/api/v1/artifacts/${encodeURIComponent(artifactId)}`, signal);
}

export function artifactFileUrl(artifactId: string, path: string): string {
  const encodedPath = path.split("/").map(encodeURIComponent).join("/");
  return `/api/v1/artifacts/${encodeURIComponent(artifactId)}/files/${encodedPath}`;
}

export function artifactArchiveUrl(artifactId: string): string {
  return `/api/v1/artifacts/${encodeURIComponent(artifactId)}/download`;
}

export function getChartHistory(
  runId: string,
  keys: string[],
  options: ChartHistoryOptions = {},
): Promise<ChartHistory> {
  const query = new URLSearchParams();
  for (const key of keys) query.append("key", key);
  if (options.maxBuckets !== undefined) {
    query.set("max_buckets", String(options.maxBuckets));
  }
  if (options.viewport) {
    query.set("step_min", String(options.viewport.stepMin));
    query.set("step_max", String(options.viewport.stepMax));
  }
  return getJson<ChartHistory>(
    `/api/v1/runs/${encodeURIComponent(runId)}/chart-history?${query}`,
    options.signal,
  );
}

export function getComparisonChartHistory(
  project: string,
  series: ComparisonChartSeriesRequest[],
  options: ComparisonChartHistoryOptions,
): Promise<ComparisonChartHistory> {
  return requestJson<ComparisonChartHistory>(
    `/api/v1/projects/${encodeURIComponent(project)}/chart-history/query`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        series,
        alignment: options.alignment,
        max_buckets: options.maxBuckets ?? 1_000,
        ...(options.viewport ? { viewport: options.viewport } : {}),
      }),
      signal: options.signal,
    },
  );
}

export function comparisonSeriesHistory(
  response: ComparisonChartHistory,
  runId: string,
  key: string,
): ChartHistory | undefined {
  const series = response.series.find(
    (candidate) => candidate.run_id === runId && candidate.key === key,
  );
  if (!series) return undefined;
  return {
    run_id: runId,
    step_min: response.x_min,
    step_max: response.x_max,
    bucket_count: response.bucket_count,
    source_points: series.source_points,
    source_last_sequence:
      response.runs.find((candidate) => candidate.run_id === runId)?.source_last_sequence ?? null,
    metrics: { [key]: series },
  };
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  return requestJson<T>(path, { signal });
}

function cursorQuery(before?: string): URLSearchParams {
  const query = new URLSearchParams({ limit: "100" });
  if (before) query.set("before", before);
  return query;
}

function validatedSearch(value: string, name: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (utf8ByteLength(trimmed) > 256 || containsControlCharacter(trimmed)) {
    throw new RangeError(`${name} cannot exceed 256 non-control bytes`);
  }
  return trimmed;
}

async function requestJson<T>(path: string, init: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  if (!response.ok) {
    const fallback = `EpochDeck request failed with HTTP ${response.status}`;
    let code = "http_error";
    let message = fallback;
    try {
      const body = (await response.json()) as { code?: unknown; message?: unknown };
      if (typeof body.code === "string" && body.code) code = body.code;
      if (typeof body.message === "string" && body.message) message = body.message;
    } catch {
      // A proxy or upstream may return a non-JSON error page.
    }
    throw new EpochDeckApiError(response.status, code, message);
  }
  return (await response.json()) as T;
}
