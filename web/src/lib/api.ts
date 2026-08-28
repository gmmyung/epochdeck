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

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  if (!response.ok) {
    throw new Error(`Runloom request failed with HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}
