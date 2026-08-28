export type Health = {
  service: string;
  version: string;
  status: "healthy" | "unhealthy";
};

export type Project = {
  id: string;
  name: string;
  created_at: string;
  run_count: number;
};

export type Run = {
  id: string;
  project_id: string;
  project: string;
  name: string;
  state: "running" | "finished";
  config: Record<string, unknown>;
  summary: Record<string, unknown>;
  metric_revision: number;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
};

export type History = {
  run_id: string;
  sequence: number[];
  step: number[];
  timestamp_ms: number[];
  metrics: Record<string, Array<number | null>>;
  next_after: number | null;
  sampled: boolean;
  source_points: number | null;
  source_last_sequence: number | null;
};

export type ChartMetricHistory = {
  source_points: number;
  bucket: number[];
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

export type ChartHistoryOptions = {
  maxBuckets?: number;
  viewport?: ChartHistoryViewport;
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

export type BlobRef = {
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

export type RunArtifact = {
  artifact: Artifact;
  relation: "input" | "output";
};

export type CursorPage<T> = {
  items: T[];
  nextBefore: string | null;
};

export type TraceSpan = {
  id: string;
  run_id: string;
  trace_id: string;
  parent_span_id: string | null;
  name: string;
  kind: "span" | "llm" | "tool" | "chain" | "agent";
  status: "unset" | "ok" | "error";
  start_time_ms: number;
  end_time_ms: number;
  step: number | null;
  attributes: Record<string, unknown>;
  preview: Record<string, unknown>;
  payload: BlobRef | null;
  created_at: string;
};

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

export function getHealth(signal?: AbortSignal): Promise<Health> {
  return getJson<Health>("/api/v1/health", signal);
}

export async function getProjects(signal?: AbortSignal): Promise<Project[]> {
  const result = await getJson<{ projects: Project[] }>("/api/v1/projects?limit=200", signal);
  return result.projects;
}

export async function getRuns(project: string, signal?: AbortSignal): Promise<Run[]> {
  const result = await getJson<{ runs: Run[] }>(
    `/api/v1/projects/${encodeURIComponent(project)}/runs?limit=200`,
    signal,
  );
  return result.runs;
}

export async function getReports(project: string, signal?: AbortSignal): Promise<Report[]> {
  const result = await getJson<{ reports: Report[] }>(
    `/api/v1/projects/${encodeURIComponent(project)}/reports?limit=100`,
    signal,
  );
  return result.reports;
}

export function getRun(runId: string, signal?: AbortSignal): Promise<Run> {
  return getJson<Run>(`/api/v1/runs/${encodeURIComponent(runId)}`, signal);
}

export async function getMetricKeys(runId: string, signal?: AbortSignal): Promise<string[]> {
  const result = await getJson<{ run_id: string; keys: string[] }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/metrics`,
    signal,
  );
  return result.keys;
}

export async function getAlerts(runId: string, signal?: AbortSignal): Promise<Alert[]> {
  const result = await getJson<{ alerts: Alert[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/alerts?limit=100`,
    signal,
  );
  return result.alerts;
}

export async function getRichValues(runId: string, signal?: AbortSignal): Promise<RichValue[]> {
  return (await getRichValuePage(runId, undefined, signal)).items;
}

export async function getRichValuePage(
  runId: string,
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<RichValue>> {
  const query = new URLSearchParams({ limit: "100" });
  if (before) query.set("before", before);
  const result = await getJson<{ values: RichValue[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/rich-values?${query}`,
    signal,
  );
  return { items: result.values, nextBefore: result.next_before };
}

export function blobUrl(blob: BlobRef): string {
  const query = new URLSearchParams({ mime: blob.mime_type });
  return `/api/v1/blobs/${encodeURIComponent(blob.digest)}?${query}`;
}

export async function getRunArtifacts(runId: string, signal?: AbortSignal): Promise<RunArtifact[]> {
  return (await getRunArtifactPage(runId, undefined, signal)).items;
}

export async function getRunArtifactPage(
  runId: string,
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<RunArtifact>> {
  const query = new URLSearchParams({ limit: "100" });
  if (before) query.set("before", before);
  const result = await getJson<{ artifacts: RunArtifact[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/artifacts?${query}`,
    signal,
  );
  return { items: result.artifacts, nextBefore: result.next_before };
}

export function artifactFileUrl(artifactId: string, path: string): string {
  const encodedPath = path.split("/").map(encodeURIComponent).join("/");
  return `/api/v1/artifacts/${encodeURIComponent(artifactId)}/files/${encodedPath}`;
}

export function artifactArchiveUrl(artifactId: string): string {
  return `/api/v1/artifacts/${encodeURIComponent(artifactId)}/download`;
}

export async function getTraces(
  runId: string,
  query = "",
  signal?: AbortSignal,
): Promise<TraceSpan[]> {
  return (await getTracePage(runId, query, undefined, signal)).items;
}

export async function getTracePage(
  runId: string,
  query = "",
  before?: string,
  signal?: AbortSignal,
): Promise<CursorPage<TraceSpan>> {
  const params = new URLSearchParams({ limit: "100" });
  if (query.trim()) params.set("q", query.trim());
  if (before) params.set("before", before);
  const result = await getJson<{ spans: TraceSpan[]; next_before: string | null }>(
    `/api/v1/runs/${encodeURIComponent(runId)}/traces?${params}`,
    signal,
  );
  return { items: result.spans, nextBefore: result.next_before };
}

export function getHistory(
  runId: string,
  keys: string[],
  limit = 5_000,
  signal?: AbortSignal,
  after?: number,
): Promise<History> {
  const query = new URLSearchParams({ keys: keys.join(","), limit: String(limit) });
  if (after !== undefined) query.set("after", String(after));
  return getJson<History>(`/api/v1/runs/${encodeURIComponent(runId)}/history?${query}`, signal);
}

export function getSampledHistory(
  runId: string,
  keys: string[],
  maxPoints = 2_000,
  signal?: AbortSignal,
): Promise<History> {
  const query = new URLSearchParams({
    keys: keys.join(","),
    max_points: String(maxPoints),
  });
  return getJson<History>(`/api/v1/runs/${encodeURIComponent(runId)}/history?${query}`, signal);
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

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  if (!response.ok) {
    throw new Error(`Runloom request failed with HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}
