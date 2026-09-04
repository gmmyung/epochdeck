// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App.svelte";
import type { Project } from "./lib/api";

const HIDDEN_RUN = "00000000-0000-7000-8000-000000000001";
const LISTED_RUN_A = "00000000-0000-7000-8000-000000000002";
const LISTED_RUN_B = "00000000-0000-7000-8000-000000000003";
const MISSING_RUN = "00000000-0000-7000-8000-000000000004";
const MISSING_REPORT = "00000000-0000-7000-8000-000000000099";
const LIVE_REPORT = "00000000-0000-7000-8000-000000000098";

beforeEach(() => {
  document.body.replaceChildren();
  window.localStorage.clear();
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
  vi.useRealTimers();
  vi.unstubAllGlobals();
  window.history.replaceState({}, "", "/");
});

describe("dashboard orchestration", () => {
  it("deep-links through bounded summaries and hydrates only the selected hidden run", async () => {
    let documentRevision = 2;
    const writeClipboard = vi.fn(async () => {});
    vi.stubGlobal("navigator", { clipboard: { writeText: writeClipboard } });
    let resolveHealth: (response: Response) => void = () => {};
    const pendingHealth = new Promise<Response>((resolve) => {
      resolveHealth = resolve;
    });
    const fetchMock = vi.fn(async (request: RequestInfo | URL, _init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return pendingHealth;
      }
      if (path === "/api/v1/dashboard/config") {
        return json({
          logo_url: "/api/v1/dashboard/logo",
          favicon_url: "/api/v1/dashboard/favicon?v=1234",
          accent_color: "#8a31c7",
        });
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
          config: { seed: 42, precise: 1.2345678912 },
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
        return json({ keys: [], next_after: null, total_count: 0 });
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
      expect(target.textContent).toContain("1.2345678912");
    });
    target.querySelector<HTMLButtonElement>('[aria-label="Copy precise"]')!.click();
    await vi.waitFor(() => expect(writeClipboard).toHaveBeenCalledWith("1.2345678912"));
    expect(target.querySelector<HTMLImageElement>('.brand img[alt="EpochDeck"]')?.src).toContain(
      "/api/v1/dashboard/logo",
    );
    expect(
      document.head.querySelector<HTMLLinkElement>(
        'link[rel="icon"][href="/api/v1/dashboard/favicon?v=1234"]',
      ),
    ).not.toBeNull();
    expect(
      document.head.querySelector<HTMLLinkElement>(
        'link[rel="apple-touch-icon"][href="/api/v1/dashboard/favicon?v=1234"]',
      ),
    ).not.toBeNull();
    expect(target.querySelector<HTMLElement>(".app-shell")?.getAttribute("style")).toContain(
      "#8a31c7",
    );

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

    resolveHealth(json({ service: "epochdeck", version: "0.1.0", status: "healthy" }));
    await vi.waitFor(() => expect(target.textContent).toContain("healthy · v0.1.0"));

    const resizer = target.querySelector<HTMLButtonElement>('[aria-label^="Resize run sidebar"]')!;
    expect(resizer.getAttribute("aria-label")).toContain("280 pixels");
    resizer.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }));
    await tick();
    expect(resizer.getAttribute("aria-label")).toContain("296 pixels");

    const setPointerCapture = vi.fn();
    const releasePointerCapture = vi.fn();
    Object.defineProperties(resizer, {
      setPointerCapture: { value: setPointerCapture },
      hasPointerCapture: { value: () => true },
      releasePointerCapture: { value: releasePointerCapture },
    });
    resizer.dispatchEvent(
      new PointerEvent("pointerdown", {
        bubbles: true,
        button: 0,
        clientX: 300,
        pointerId: 7,
      }),
    );
    resizer.dispatchEvent(
      new PointerEvent("pointermove", { bubbles: true, clientX: 360, pointerId: 7 }),
    );
    await tick();
    expect(resizer.getAttribute("aria-label")).toContain("356 pixels");
    expect(setPointerCapture).toHaveBeenCalledWith(7);
    resizer.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, pointerId: 7 }));
    expect(releasePointerCapture).toHaveBeenCalledWith(7);
    expect(window.localStorage.getItem("epochdeck:sidebar-width")).toBe("356");

    target.querySelector<HTMLButtonElement>('[aria-label="Collapse run sidebar"]')!.click();
    await tick();
    expect(target.querySelector(".workspace")?.classList.contains("sidebar-collapsed")).toBe(true);
    expect(target.querySelector(".run-list-row .run-primary-button")).toBeNull();
    expect(target.querySelector('.run-list-row input[type="checkbox"]')).not.toBeNull();
    expect(window.localStorage.getItem("epochdeck:sidebar-collapsed")).toBe("true");

    target.querySelector<HTMLButtonElement>('[aria-label="Expand run sidebar"]')!.click();
    await tick();
    expect(target.querySelector(".workspace")?.classList.contains("sidebar-collapsed")).toBe(false);
    expect(target.querySelector(".run-list-row .run-primary-button")).not.toBeNull();
    expect(window.localStorage.getItem("epochdeck:sidebar-collapsed")).toBe("false");

    await unmount(component);
    target.remove();
  });

  it("recovers stale run, report, and project URLs and replaces them canonically", async () => {
    window.history.replaceState(
      {},
      "",
      `/?project=hidden-project&run=${HIDDEN_RUN}&run=${MISSING_RUN}&primary=${MISSING_RUN}&tab=configuration`,
    );

    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "epochdeck", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/dashboard/config") {
        return json({ logo_url: null, favicon_url: null, accent_color: "#2766ad" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 2)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/missing-project") {
        return json({ code: "not_found", message: "project not found" }, 404);
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({
          runs: [runSummary(LISTED_RUN_A, "Listed A"), runSummary(LISTED_RUN_B, "Listed B")],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [], next_before: null });
      }
      if (path === `/api/v1/reports/${MISSING_REPORT}`) {
        return json({ code: "not_found", message: "report not found" }, 404);
      }
      if (path === "/api/v1/query/runs") {
        const body = JSON.parse(init?.body as string) as { run_ids: string[] };
        if (body.run_ids.length > 1 || body.run_ids[0] === MISSING_RUN) {
          return json({ code: "not_found", message: "run not found" }, 404);
        }
        expect(body.run_ids).toEqual([HIDDEN_RUN]);
        return json({
          runs: [runSummary(HIDDEN_RUN, "Hidden valid run")],
          next_before: null,
        });
      }
      if (path === `/api/v1/runs/${HIDDEN_RUN}`) {
        return json({
          ...runSummary(HIDDEN_RUN, "Hidden valid run"),
          config: { selected: "hidden" },
          summary: {},
          explicit_summary: {},
          metric_summary: {},
        });
      }
      if (path === `/api/v1/runs/${LISTED_RUN_A}`) {
        return json({
          ...runSummary(LISTED_RUN_A, "Listed A"),
          config: { selected: "a" },
          summary: {},
          explicit_summary: {},
          metric_summary: {},
        });
      }
      if (path === `/api/v1/runs/${LISTED_RUN_B}`) {
        return json({
          ...runSummary(LISTED_RUN_B, "Listed B"),
          config: { selected: "b" },
          summary: {},
          explicit_summary: {},
          metric_summary: {},
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        return json({ keys: [], next_after: null, total_count: 0 });
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });

    await vi.waitFor(() => expect(target.textContent).toContain("One unavailable run"));
    expect(new URL(window.location.href).searchParams.getAll("run")).toEqual([HIDDEN_RUN]);
    expect(new URL(window.location.href).searchParams.get("primary")).toBe(HIDDEN_RUN);

    window.history.pushState(
      {},
      "",
      `/?project=hidden-project&run=${MISSING_RUN}&primary=${MISSING_RUN}&tab=configuration`,
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
    await vi.waitFor(() => {
      const url = new URL(window.location.href);
      expect(url.searchParams.getAll("run")).toEqual([LISTED_RUN_A]);
      expect(url.searchParams.get("primary")).toBe(LISTED_RUN_A);
    });

    window.history.pushState(
      {},
      "",
      `/?project=hidden-project&report=${MISSING_REPORT}&run=${LISTED_RUN_B}&primary=${LISTED_RUN_B}&tab=metrics`,
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
    await vi.waitFor(() => expect(target.textContent).toContain("requested report is unavailable"));
    expect(new URL(window.location.href).searchParams.get("report")).toBeNull();
    expect(new URL(window.location.href).searchParams.getAll("run")).toEqual([LISTED_RUN_B]);
    expect(new URL(window.location.href).searchParams.get("primary")).toBe(LISTED_RUN_B);

    window.history.pushState(
      {},
      "",
      `/?project=missing-project&run=${MISSING_RUN}&primary=${MISSING_RUN}&tab=configuration`,
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
    await vi.waitFor(() => {
      const url = new URL(window.location.href);
      expect(url.searchParams.get("project")).toBe("hidden-project");
      expect(url.searchParams.getAll("run")).toEqual([LISTED_RUN_A]);
      expect(url.searchParams.get("primary")).toBe(LISTED_RUN_A);
    });

    expect(
      fetchMock.mock.calls.filter(([request]) => String(request) === "/api/v1/query/runs"),
    ).toHaveLength(5);
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
        return json({ service: "epochdeck", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/dashboard/config") {
        return json({ logo_url: null, favicon_url: null, accent_color: "#2766ad" });
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
        return json({ keys: [], next_after: null, total_count: 0 });
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

  it("renders late chart histories and links sidebar hover and styles without refetching", async () => {
    window.history.replaceState(
      {},
      "",
      `/?project=hidden-project&run=${HIDDEN_RUN}&run=${LISTED_RUN_A}&primary=${HIDDEN_RUN}&tab=metrics`,
    );
    installVisibleChartObservers();

    let resolveHistory: (response: Response) => void = () => {};
    const pendingHistory = new Promise<Response>((resolve) => {
      resolveHistory = resolve;
    });
    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "epochdeck", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/dashboard/config") {
        return json({ logo_url: null, favicon_url: null, accent_color: "#2766ad" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 2)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({
          runs: [runSummary(HIDDEN_RUN, "Visible run"), runSummary(LISTED_RUN_A, "Comparison run")],
          next_before: null,
        });
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
        return json({
          keys: [{ key: "train/loss", run_ids: [HIDDEN_RUN, LISTED_RUN_A] }],
          next_after: null,
          total_count: 1,
        });
      }
      if (path === "/api/v1/projects/hidden-project/chart-history/query") {
        expect(JSON.parse(init?.body as string).series).toEqual([
          { run_id: HIDDEN_RUN, key: "train/loss" },
          { run_id: LISTED_RUN_A, key: "train/loss" },
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
        runs: [
          { run_id: HIDDEN_RUN, source_last_sequence: 2 },
          { run_id: LISTED_RUN_A, source_last_sequence: 2 },
        ],
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
          {
            run_id: LISTED_RUN_A,
            key: "train/loss",
            source_points: 2,
            bucket: [0, 1],
            last_x: [0, 1],
            last_step: [0, 1],
            last_timestamp_ms: [1, 2],
            minimum: [4, 3],
            maximum: [4, 3],
            last: [4, 3],
          },
        ],
      }),
    );
    await vi.waitFor(() =>
      expect(target.querySelectorAll(".chart-interaction-canvas")).toHaveLength(1),
    );
    expect(target.querySelector(".chart-uplot")).not.toBeNull();
    expect(target.textContent).not.toContain("Loading bounded histories");

    const historyCallsBefore = fetchMock.mock.calls.filter(
      ([request]) => String(request) === "/api/v1/projects/hidden-project/chart-history/query",
    ).length;
    const comparisonRow = target.querySelector<HTMLElement>('[aria-label^="Run Comparison run"]')!;
    comparisonRow.dispatchEvent(new MouseEvent("mouseenter"));
    await tick();
    const comparisonLegend = target
      .querySelector('[aria-label^="Hide Comparison run"]')
      ?.closest(".legend-entry");
    expect(comparisonLegend?.classList.contains("highlighted")).toBe(true);

    target
      .querySelector<HTMLButtonElement>('[aria-label^="Configure chart style for Comparison run"]')!
      .click();
    await tick();
    const color = target.querySelector<HTMLInputElement>(
      '[aria-label="Line color for Comparison run"]',
    )!;
    color.value = "#abcdef";
    color.dispatchEvent(new Event("change", { bubbles: true }));
    await tick();
    target
      .querySelector<HTMLInputElement>('[aria-label="Dashed line for Comparison run"]')!
      .click();
    await tick();
    const styledSwatch = target.querySelector<HTMLElement>(
      '[aria-label^="Hide Comparison run"] .series-swatch',
    );
    expect(styledSwatch?.getAttribute("style")).toContain("#abcdef");
    expect(styledSwatch?.classList.contains("pattern-dash")).toBe(true);

    comparisonRow.dispatchEvent(new MouseEvent("mouseleave"));
    await tick();
    expect(comparisonLegend?.classList.contains("highlighted")).toBe(false);
    expect(new URL(window.location.href).searchParams.getAll("run")).toEqual([
      HIDDEN_RUN,
      LISTED_RUN_A,
    ]);
    expect(
      fetchMock.mock.calls.filter(
        ([request]) => String(request) === "/api/v1/projects/hidden-project/chart-history/query",
      ),
    ).toHaveLength(historyCallsBefore);

    await unmount(component);
    target.remove();
  });

  it("coalesces comparison revisions without aborting an active chart and delivers the final run", async () => {
    vi.useFakeTimers({ toFake: ["setInterval"] });
    installVisibleChartObservers();
    window.history.replaceState(
      {},
      "",
      `/?project=hidden-project&run=${HIDDEN_RUN}&primary=${HIDDEN_RUN}&tab=metrics`,
    );
    let revision = 1;
    let state: "running" | "finished" = "running";
    let summaryPolls = 0;
    let chartRequests = 0;
    let metricCatalogRequests = 0;
    const activeRefresh = deferred<Response>();
    const finalRefresh = deferred<Response>();
    const liveCatalogRefresh = deferred<Response>();
    const chartSignals: AbortSignal[] = [];
    const metricCatalogSignals: AbortSignal[] = [];
    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "epochdeck", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/dashboard/config") {
        return json({ logo_url: null, favicon_url: null, accent_color: "#2766ad" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 1)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({
          runs: [{ ...runSummary(HIDDEN_RUN, "Live run", "running"), metric_revision: 1 }],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [], next_before: null });
      }
      if (path === `/api/v1/runs/${HIDDEN_RUN}`) {
        return json({
          ...runSummary(HIDDEN_RUN, "Live run", "running"),
          metric_revision: 1,
          config: {},
          summary: {},
          explicit_summary: {},
          metric_summary: {},
        });
      }
      if (path === "/api/v1/query/runs") {
        summaryPolls += 1;
        return json({
          runs: [{ ...runSummary(HIDDEN_RUN, "Live run", state), metric_revision: revision }],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        metricCatalogRequests += 1;
        if (init?.signal) metricCatalogSignals.push(init.signal);
        if (metricCatalogRequests === 2) return liveCatalogRefresh.promise;
        return json({
          keys: [{ key: "train/loss", run_ids: [HIDDEN_RUN] }],
          next_after: null,
          total_count: 1,
        });
      }
      if (path === "/api/v1/projects/hidden-project/chart-history/query") {
        chartRequests += 1;
        if (init?.signal) chartSignals.push(init.signal);
        if (chartRequests === 1) return json(comparisonHistory(1, 1));
        if (chartRequests === 2) return activeRefresh.promise;
        if (chartRequests === 3) return finalRefresh.promise;
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });
    await vi.waitFor(() => expect(chartRequests).toBe(1));
    await vi.waitFor(() => expect(target.querySelector("canvas")).not.toBeNull());
    const lastGoodCanvas = target.querySelector("canvas");

    revision = 2;
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryPolls).toBe(1));
    await vi.waitFor(() => expect(chartRequests).toBe(2));
    expect(metricCatalogRequests).toBe(2);

    revision = 3;
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryPolls).toBe(2));
    expect(chartRequests).toBe(2);
    expect(chartSignals[1]?.aborted).toBe(false);
    expect(target.querySelector("canvas")).toBe(lastGoodCanvas);

    revision = 4;
    state = "finished";
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryPolls).toBe(3));
    expect(chartRequests).toBe(2);
    expect(chartSignals[1]?.aborted).toBe(false);
    expect(metricCatalogSignals[1]?.aborted).toBe(false);

    activeRefresh.resolve(json(comparisonHistory(2, 2)));
    await vi.waitFor(() => expect(chartRequests).toBe(3));
    expect(target.querySelector("canvas")).toBe(lastGoodCanvas);
    finalRefresh.resolve(json(comparisonHistory(4, 4)));
    await vi.waitFor(() => expect(chartSignals).toHaveLength(3));
    expect(chartSignals.every((signal) => !signal.aborted)).toBe(true);
    expect(metricCatalogSignals[1]?.aborted).toBe(false);
    liveCatalogRefresh.resolve(
      json({
        keys: [{ key: "train/loss", run_ids: [HIDDEN_RUN] }],
        next_after: null,
        total_count: 1,
      }),
    );

    await unmount(component);
    target.remove();
  });

  it("coalesces report chart revisions and guarantees a finished-run refresh", async () => {
    vi.useFakeTimers({ toFake: ["setInterval"] });
    installVisibleChartObservers();
    window.history.replaceState(
      {},
      "",
      `/?project=hidden-project&report=${LIVE_REPORT}&run=${HIDDEN_RUN}&primary=${HIDDEN_RUN}&tab=metrics`,
    );
    let revision = 1;
    let state: "running" | "finished" = "running";
    let summaryQueries = 0;
    let chartRequests = 0;
    const activeRefresh = deferred<Response>();
    const finalRefresh = deferred<Response>();
    const chartSignals: AbortSignal[] = [];
    const report = liveReport();
    const fetchMock = vi.fn(async (request: RequestInfo | URL, init?: RequestInit) => {
      const path = String(request);
      if (path === "/api/v1/health") {
        return json({ service: "epochdeck", version: "0.1.0", status: "healthy" });
      }
      if (path === "/api/v1/dashboard/config") {
        return json({ logo_url: null, favicon_url: null, accent_color: "#2766ad" });
      }
      if (path === "/api/v1/projects?limit=100") {
        return json({
          projects: [project("hidden-project", "project-hidden", 1)],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/runs?limit=100") {
        return json({
          runs: [{ ...runSummary(HIDDEN_RUN, "Report run", "running"), metric_revision: 1 }],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/reports?limit=100") {
        return json({ reports: [reportSummary(report)], next_before: null });
      }
      if (path === `/api/v1/reports/${LIVE_REPORT}`) return json(report);
      if (path === "/api/v1/query/runs") {
        summaryQueries += 1;
        return json({
          runs: [{ ...runSummary(HIDDEN_RUN, "Report run", state), metric_revision: revision }],
          next_before: null,
        });
      }
      if (path === "/api/v1/projects/hidden-project/metrics/query") {
        return json({
          keys: [{ key: "train/loss", run_ids: [HIDDEN_RUN] }],
          next_after: null,
          total_count: 1,
        });
      }
      if (path.startsWith(`/api/v1/runs/${HIDDEN_RUN}/chart-history?`)) {
        chartRequests += 1;
        if (init?.signal) chartSignals.push(init.signal);
        if (chartRequests === 1) return json(singleRunHistory(1, 1));
        if (chartRequests === 2) return activeRefresh.promise;
        if (chartRequests === 3) return finalRefresh.promise;
      }
      return json({ code: "unexpected_request", message: path }, 500);
    });
    vi.stubGlobal("fetch", fetchMock);

    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(App, { target });
    await vi.waitFor(() => expect(chartRequests).toBe(1));
    await vi.waitFor(() => expect(target.querySelector("canvas")).not.toBeNull());
    const lastGoodCanvas = target.querySelector("canvas");
    expect(summaryQueries).toBe(1);

    revision = 2;
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryQueries).toBe(2));
    await vi.waitFor(() => expect(chartRequests).toBe(2));

    revision = 3;
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryQueries).toBe(3));
    expect(chartRequests).toBe(2);
    expect(chartSignals[1]?.aborted).toBe(false);
    expect(target.querySelector("canvas")).toBe(lastGoodCanvas);

    revision = 4;
    state = "finished";
    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(summaryQueries).toBe(4));
    expect(chartRequests).toBe(2);
    expect(chartSignals[1]?.aborted).toBe(false);

    activeRefresh.resolve(json(singleRunHistory(2, 2)));
    await vi.waitFor(() => expect(chartRequests).toBe(3));
    expect(target.querySelector("canvas")).toBe(lastGoodCanvas);
    finalRefresh.resolve(json(singleRunHistory(4, 4)));
    await vi.waitFor(() => expect(chartSignals).toHaveLength(3));
    expect(chartSignals.every((signal) => !signal.aborted)).toBe(true);

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

function comparisonHistory(value: number, sourceLastSequence: number) {
  return {
    project: "hidden-project",
    alignment: "step",
    x_min: 0,
    x_max: sourceLastSequence,
    bucket_count: 1,
    runs: [{ run_id: HIDDEN_RUN, source_last_sequence: sourceLastSequence }],
    series: [
      {
        run_id: HIDDEN_RUN,
        key: "train/loss",
        source_points: sourceLastSequence,
        bucket: [0],
        last_x: [sourceLastSequence],
        last_step: [sourceLastSequence],
        last_timestamp_ms: [sourceLastSequence * 1_000],
        minimum: [value],
        maximum: [value],
        last: [value],
      },
    ],
  };
}

function singleRunHistory(value: number, sourceLastSequence: number) {
  return {
    run_id: HIDDEN_RUN,
    step_min: 0,
    step_max: sourceLastSequence,
    bucket_count: 1,
    source_points: sourceLastSequence,
    source_last_sequence: sourceLastSequence,
    metrics: {
      "train/loss": {
        source_points: sourceLastSequence,
        bucket: [0],
        last_x: [sourceLastSequence],
        last_step: [sourceLastSequence],
        last_timestamp_ms: [sourceLastSequence * 1_000],
        minimum: [value],
        maximum: [value],
        last: [value],
      },
    },
  };
}

function liveReport() {
  return {
    id: LIVE_REPORT,
    project_id: "project-hidden",
    project: "hidden-project",
    name: "Live report",
    description: null,
    layout: {
      columns: 1,
      panels: [
        {
          id: "live-panel",
          title: "Training loss",
          kind: "metric" as const,
          run_id: HIDDEN_RUN,
          metric_keys: ["train/loss"],
          markdown: null,
          width: 1,
          height: 320,
        },
      ],
    },
    created_at: "2026-08-30T00:00:00Z",
    updated_at: "2026-08-30T00:01:00Z",
  };
}

function reportSummary(report: ReturnType<typeof liveReport>) {
  const { id, project_id, project, name, created_at, updated_at } = report;
  return { id, project_id, project, name, created_at, updated_at };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function installVisibleChartObservers(): void {
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
}
