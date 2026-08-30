// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App.svelte";
import type { Project } from "./lib/api";

const HIDDEN_RUN = "00000000-0000-7000-8000-000000000001";
const LISTED_RUN_A = "00000000-0000-7000-8000-000000000002";
const LISTED_RUN_B = "00000000-0000-7000-8000-000000000003";

beforeEach(() => {
  document.body.replaceChildren();
  window.history.replaceState(
    {},
    "",
    `/?project=hidden-project&run=${HIDDEN_RUN}&primary=${HIDDEN_RUN}&tab=configuration&metric_after=train%2Floss`,
  );
  const matchMedia = vi.fn(() => ({
    matches: false,
    media: "",
    onchange: null,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    addListener: vi.fn(),
    removeListener: vi.fn(),
    dispatchEvent: vi.fn(() => true),
  }));
  vi.stubGlobal("matchMedia", matchMedia);
  window.matchMedia = matchMedia;
});

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState({}, "", "/");
});

describe("dashboard orchestration", () => {
  it("deep-links through bounded summaries and hydrates only the selected hidden run", async () => {
    let documentRevision = 2;
    let resolveHealth: (response: Response) => void = () => {};
    const pendingHealth = new Promise<Response>((resolve) => {
      resolveHealth = resolve;
    });
    const fetchMock = vi.fn(async (request: RequestInfo | URL, _init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return pendingHealth;
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("first-project", "project-first", 2)],
          next_before: "project-cursor",
        });
      }
      if (path === "/api/v1/projects/hidden-project") {
        return json(project("hidden-project", "project-hidden", 3));
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({
          runs: [runSummary(LISTED_RUN_A, "Listed A"), runSummary(LISTED_RUN_B, "Listed B")],
          next_before: "run-cursor",
        });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [], next_before: null });
      }
      if (path === `/api/v1/runs/${HIDDEN_RUN}`) {
        return json({
          ...runSummary(HIDDEN_RUN, "Deep linked run", "running"),
          document_revision: documentRevision,
          config: { seed: 42 },
          summary: { result: "complete" },
        });
      }
      if (path === "/api/v1/query/runs") {
        return json({
          runs: [
            {
              ...runSummary(HIDDEN_RUN, "Deep linked run", "running"),
              document_revision: documentRevision,
            },
          ],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        return json({ keys: [], next_after: null });
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });
    await tick();

    await vi.waitFor(() => {
      expect(target.textContent).toContain("Deep linked run");
      expect(target.textContent).toContain("Listed A");
      expect(target.textContent).toContain("seed");
    });

    const paths = fetchMock.mock.calls.map(([request]) => String(request));
    expect(paths).toContain("/api/v1/projects/hidden-project");
    expect(paths.filter((path) => path === `/api/v1/runs/${HIDDEN_RUN}`)).toHaveLength(1);
    expect(paths).not.toContain(`/api/v1/runs/${LISTED_RUN_A}`);
    expect(paths).not.toContain(`/api/v1/runs/${LISTED_RUN_B}`);
    expect(paths.some((path) => path.includes(`/runs/${HIDDEN_RUN}/metrics`))).toBe(false);
    expect(paths.filter((path) => path.endsWith("/runs?limit=100"))).toHaveLength(1);
    const metricCatalogCall = fetchMock.mock.calls.find(
      ([request]) => String(request) === "/api/v1/projects/hidden-project/metrics/query",
    );
    expect(JSON.parse(metricCatalogCall?.[1]?.body as string)).toEqual({
      run_ids: [HIDDEN_RUN],
      mode: "union",
      after: "train/loss",
      limit: 24,
    });
    expect(target.textContent).toContain("connecting");

    expect(
      fetchMock.mock.calls.filter(([request]) => String(request) === "/api/v1/query/runs"),
    ).toHaveLength(1);
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.waitFor(() => {
      expect(
        fetchMock.mock.calls.filter(([request]) => String(request) === "/api/v1/query/runs"),
      ).toHaveLength(2);
    });
    const pollCall = fetchMock.mock.calls.findLast(
      ([request]) => String(request) === "/api/v1/query/runs",
    );
    expect(JSON.parse(pollCall?.[1]?.body as string)).toEqual({
      project: "hidden-project",
      run_ids: [HIDDEN_RUN],
      limit: 1,
    });
    expect(
      fetchMock.mock.calls.filter(([request]) => String(request) === `/api/v1/runs/${HIDDEN_RUN}`),
    ).toHaveLength(1);

    await new Promise((resolve) => window.setTimeout(resolve, 10));
    documentRevision = 3;
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.waitFor(() => {
      expect(
        fetchMock.mock.calls.filter(
          ([request]) => String(request) === `/api/v1/runs/${HIDDEN_RUN}`,
        ),
      ).toHaveLength(2);
    });

    resolveHealth(json({ service: "runloom", version: "0.1.0", status: "healthy" }));
    await vi.waitFor(() => expect(target.textContent).toContain("healthy · v0.1.0"));

    await unmount(component);
    target.remove();
  });

  it("caps crafted repeated-run deep links before issuing detail requests", async () => {
    const requestedRuns = Array.from(
      { length: 30 },
      (_, index) => `00000000-0000-7000-8001-${String(index).padStart(12, "0")}`,
    );
    const url = new URL("http://localhost/?project=hidden-project&tab=configuration");
    for (const runId of requestedRuns) url.searchParams.append("run", runId);
    url.searchParams.set("primary", requestedRuns[0]);
    window.history.replaceState({}, "", `${url.pathname}${url.search}`);

    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "runloom", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 30)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({ runs: [], next_before: null });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [], next_before: null });
      }
      if (path === "/api/v1/query/runs") {
        const body = JSON.parse(init?.body as string) as { run_ids: string[] };
        return json({
          runs: body.run_ids.map((runId) =>
            runSummary(runId, `Run ${requestedRuns.indexOf(runId)}`),
          ),
          next_before: null,
        });
      }
      const runId = path.startsWith("/api/v1/runs/") ? path.slice("/api/v1/runs/".length) : null;
      if (runId && requestedRuns.includes(runId)) {
        return json({
          ...runSummary(runId, `Run ${requestedRuns.indexOf(runId)}`),
          config: {},
          summary: {},
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        return json({ keys: [], next_after: null });
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });
    await vi.waitFor(() => expect(target.textContent).toContain("Run 0"));

    const detailRequests = fetchMock.mock.calls
      .map(([request]) => String(request))
      .filter((path) => path.startsWith("/api/v1/runs/"));
    expect(detailRequests).toEqual([`/api/v1/runs/${requestedRuns[0]}`]);
    const summaryQueries = fetchMock.mock.calls.filter(
      ([request]) => String(request) === "/api/v1/query/runs",
    );
    expect(summaryQueries).toHaveLength(1);
    expect(JSON.parse(summaryQueries[0][1]?.body as string).run_ids).toHaveLength(12);

    await unmount(component);
    target.remove();
  });

  it("renders chart histories that arrive after the metric panels mount", async () => {
    window.history.replaceState(
      {},
      "",
      `/?project=hidden-project&run=${HIDDEN_RUN}&primary=${HIDDEN_RUN}&tab=metrics`,
    );
    class VisibleIntersectionObserver {
      constructor(private readonly callback: IntersectionObserverCallback) {}
      observe(target: Element): void {
        this.callback(
          [{ target, isIntersecting: true, intersectionRatio: 1 } as IntersectionObserverEntry],
          this as unknown as IntersectionObserver,
        );
      }
      disconnect(): void {}
      unobserve(): void {}
      takeRecords(): IntersectionObserverEntry[] {
        return [];
      }
      readonly root = null;
      readonly rootMargin = "0px";
      readonly thresholds = [0];
    }
    class ResizeObserverMock {
      observe(): void {}
      disconnect(): void {}
      unobserve(): void {}
    }
    vi.stubGlobal("IntersectionObserver", VisibleIntersectionObserver);
    vi.stubGlobal("ResizeObserver", ResizeObserverMock);

    let resolveHistory: (response: Response) => void = () => {};
    const pendingHistory = new Promise<Response>((resolve) => {
      resolveHistory = resolve;
    });
    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "runloom", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 1)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({ runs: [runSummary(HIDDEN_RUN, "Visible run")], next_before: null });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [], next_before: null });
      }
      if (path === `/api/v1/runs/${HIDDEN_RUN}`) {
        return json({
          ...runSummary(HIDDEN_RUN, "Visible run"),
          config: {},
          summary: {},
          explicit_summary: {},
          metric_summary: {},
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        return json({ keys: [{ key: "train/loss", run_ids: [HIDDEN_RUN] }], next_after: null });
      }
      if (path === "/api/v1/projects/hidden-project/chart-history/query") {
        expect(JSON.parse(init?.body as string).series).toEqual([
          { run_id: HIDDEN_RUN, key: "train/loss" },
        ]);
        return pendingHistory;
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });
    await vi.waitFor(() => expect(target.textContent).toContain("Loading bounded histories"));
    expect(target.querySelectorAll("canvas")).toHaveLength(0);

    resolveHistory(
      json({
        project: "hidden-project",
        alignment: "step",
        x_min: 0,
        x_max: 1,
        bucket_count: 2,
        runs: [{ run_id: HIDDEN_RUN, source_last_sequence: 2 }],
        series: [
          {
            run_id: HIDDEN_RUN,
            key: "train/loss",
            source_points: 2,
            bucket: [0, 1],
            last_x: [0, 1],
            last_step: [0, 1],
            last_timestamp_ms: [1, 2],
            minimum: [2, 1],
            maximum: [2, 1],
            last: [2, 1],
          },
        ],
      }),
    );
    await vi.waitFor(() => expect(target.querySelectorAll("canvas")).toHaveLength(2));
    expect(target.textContent).not.toContain("Loading bounded histories");

    await unmount(component);
    target.remove();
  });
});

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function project(name: string, id: string, runCount: number): Project {
  return {
    id,
    name,
    created_at: "2026-08-30T00:00:00Z",
    run_count: runCount,
    mutation_token: "900719925474099312345",
  };
}

function runSummary(id: string, name: string, state: "running" | "finished" = "finished") {
  return {
    id,
    project_id: "project-hidden",
    project: "hidden-project",
    name,
    state,
    summary_truncated: false,
    document_revision: 2,
    metric_revision: 7,
    rich_data_revision: 3,
    created_at: "2026-08-30T00:00:00Z",
    updated_at: "2026-08-30T00:01:00Z",
    finished_at: state === "finished" ? "2026-08-30T00:01:00Z" : null,
  };
}
