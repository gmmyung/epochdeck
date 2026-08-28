<script lang="ts">
  import { onMount } from "svelte";

  import {
    blobUrl,
    getAlerts,
    getChartHistory,
    getHealth,
    getMetricKeys,
    getProjects,
    getReports,
    getRun,
    getRuns,
    getRichValuePage,
    getRunArtifactPage,
    getTracePage,
    type Alert,
    type ChartHistory,
    type ChartHistoryViewport,
    type Health,
    type Project,
    type Report,
    type ReportPanel,
    type RichValue,
    type Run,
    type RunArtifact,
    type TraceSpan,
  } from "./lib/api";
  import {
    chartViewportKey as viewportKey,
    metricChartRequestKey as metricRequestKey,
    normalizeChartViewport as normalizedViewport,
  } from "./lib/chart-request";
  import { CHART_BUCKET_BUDGET, ChartHistoryCache } from "./lib/history-cache";
  import ArtifactBrowser from "./lib/ArtifactBrowser.svelte";
  import Icon from "./lib/Icon.svelte";
  import JsonTreeNode from "./lib/JsonTreeNode.svelte";
  import MediaTimeline from "./lib/MediaTimeline.svelte";
  import MetricChart from "./lib/MetricChart.svelte";
  import MarkdownPanel from "./lib/MarkdownPanel.svelte";
  import { filterMetricKeys } from "./lib/metric-filter";

  const MAX_CONCURRENT_CHART_REQUESTS = 4;
  const LIVE_REFRESH_MS = 2_000;
  const historyCache = new ChartHistoryCache();
  const RUN_TABS = [
    { id: "summary", label: "Summary", icon: "summary" },
    { id: "configuration", label: "Configuration", icon: "settings" },
    { id: "metrics", label: "Metrics", icon: "chart" },
    { id: "media", label: "Media", icon: "media" },
    { id: "traces", label: "Traces", icon: "trace" },
    { id: "artifacts", label: "Artifacts", icon: "archive" },
  ] as const;
  type RunTab = (typeof RUN_TABS)[number]["id"];

  let health: Health | null = null;
  let projects: Project[] = [];
  let runs: Run[] = [];
  let reports: Report[] = [];
  let selectedProject = "";
  let selectedRun: Run | null = null;
  let selectedReport: Report | null = null;
  let metricKeys: string[] = [];
  let alerts: Alert[] = [];
  let richValues: RichValue[] = [];
  let artifacts: RunArtifact[] = [];
  let traces: TraceSpan[] = [];
  let activeRunTab: RunTab = "metrics";
  let metricSearch = "";
  let loadedRunTabs = new Set<RunTab>();
  let loadingRunTabs = new Set<RunTab>();
  let loadingMoreTab: RunTab | null = null;
  let richValueCursor: string | null = null;
  let artifactCursor: string | null = null;
  let traceCursor: string | null = null;
  let traceSearch = "";
  let traceSearchLoading = false;
  let histories: Record<string, ChartHistory> = {};
  let historyRequestKeys: Record<string, string> = {};
  let metricViewports: Record<string, ChartHistoryViewport | null> = {};
  let loadingMetrics = new Set<string>();
  let visibleMetrics = new Set<string>();
  let pendingMetrics: string[] = [];
  let activeChartRequests = 0;
  let reportHistories: Record<string, ChartHistory> = {};
  let reportHistoryRequestKeys: Record<string, string> = {};
  let reportViewports: Record<string, ChartHistoryViewport | null> = {};
  let loadingReportMetrics = new Set<string>();
  let pendingReportMetrics: Array<{
    identity: string;
    runId: string;
    metric: string;
  }> = [];
  let activeReportRequests = 0;
  let refreshingRun = false;
  let error: string | null = null;
  let loading = true;
  let projectController: AbortController | null = null;
  let runController: AbortController | null = null;
  let selectionGeneration = 0;

  $: filteredMetricKeys = filterMetricKeys(metricKeys, metricSearch);

  onMount(() => {
    const controller = new AbortController();
    const refreshTimer = window.setInterval(refreshSelectedRun, LIVE_REFRESH_MS);
    Promise.all([getHealth(controller.signal), getProjects(controller.signal)])
      .then(async ([healthResult, projectResult]) => {
        health = healthResult;
        projects = projectResult;
        if (projects[0]) await chooseProject(projects[0].name);
      })
      .catch(showError)
      .finally(() => {
        loading = false;
      });
    return () => {
      controller.abort();
      projectController?.abort();
      runController?.abort();
      window.clearInterval(refreshTimer);
    };
  });

  async function chooseProject(name: string): Promise<void> {
    projectController?.abort();
    const controller = new AbortController();
    projectController = controller;
    selectedProject = name;
    resetRunSelection();
    runs = [];
    reports = [];
    error = null;
    try {
      [runs, reports] = await Promise.all([
        getRuns(name, controller.signal),
        getReports(name, controller.signal),
      ]);
      if (reports[0]) chooseReport(reports[0]);
      else if (runs[0]) await chooseRun(runs[0]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function chooseRun(run: Run): Promise<void> {
    selectionGeneration += 1;
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    selectedReport = null;
    resetReportState();
    resetChartState();
    metricKeys = [];
    alerts = [];
    richValues = [];
    artifacts = [];
    traces = [];
    traceSearch = "";
    loadedRunTabs = new Set();
    loadingRunTabs = new Set();
    loadingMoreTab = null;
    richValueCursor = null;
    artifactCursor = null;
    traceCursor = null;
    selectedRun = run;
    activeRunTab = "metrics";
    metricSearch = "";
    error = null;
    try {
      metricKeys = await getMetricKeys(run.id, controller.signal);
      loadedRunTabs = new Set(["configuration", "metrics"]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  function resetRunSelection(): void {
    selectionGeneration += 1;
    runController?.abort();
    selectedRun = null;
    selectedReport = null;
    metricKeys = [];
    alerts = [];
    richValues = [];
    artifacts = [];
    traces = [];
    traceSearch = "";
    activeRunTab = "metrics";
    metricSearch = "";
    loadedRunTabs = new Set();
    loadingRunTabs = new Set();
    loadingMoreTab = null;
    richValueCursor = null;
    artifactCursor = null;
    traceCursor = null;
    resetChartState();
    resetReportState();
  }

  function resetChartState(): void {
    histories = {};
    historyRequestKeys = {};
    metricViewports = {};
    loadingMetrics = new Set();
    visibleMetrics = new Set();
    pendingMetrics = [];
  }

  function resetReportState(): void {
    reportHistories = {};
    reportHistoryRequestKeys = {};
    reportViewports = {};
    loadingReportMetrics = new Set();
    pendingReportMetrics = [];
  }

  function chooseReport(report: Report): void {
    selectionGeneration += 1;
    runController?.abort();
    runController = new AbortController();
    selectedRun = null;
    selectedReport = report;
    metricKeys = [];
    alerts = [];
    richValues = [];
    artifacts = [];
    traces = [];
    traceSearch = "";
    activeRunTab = "metrics";
    metricSearch = "";
    loadedRunTabs = new Set();
    loadingRunTabs = new Set();
    loadingMoreTab = null;
    richValueCursor = null;
    artifactCursor = null;
    traceCursor = null;
    resetChartState();
    resetReportState();
    error = null;
  }

  function reportChartVisibility(panel: ReportPanel, metric: string, visible: boolean): void {
    if (!visible) return;
    const runId = panel.run_id;
    if (!runId) return;
    const identity = `${panel.id}:${runId}:${metric}`;
    queueReportMetric({ identity, runId, metric });
  }

  function reportChartViewport(
    panel: ReportPanel,
    metric: string,
    stepMin: number | null,
    stepMax: number | null,
  ): void {
    const runId = panel.run_id;
    if (!runId) return;
    const identity = `${panel.id}:${runId}:${metric}`;
    const viewport = normalizedViewport(stepMin, stepMax);
    if (viewportKey(reportViewports[identity] ?? null) === viewportKey(viewport)) return;
    reportViewports = { ...reportViewports, [identity]: viewport };
    queueReportMetric({ identity, runId, metric });
  }

  function queueReportMetric(request: { identity: string; runId: string; metric: string }): void {
    const requestKey = viewportKey(reportViewports[request.identity] ?? null);
    if (
      reportHistoryRequestKeys[request.identity] === requestKey ||
      loadingReportMetrics.has(request.identity)
    ) {
      return;
    }
    if (pendingReportMetrics.some((candidate) => candidate.identity === request.identity)) return;
    pendingReportMetrics = [...pendingReportMetrics, request];
    drainReportMetricQueue();
  }

  function drainReportMetricQueue(): void {
    while (
      activeReportRequests < MAX_CONCURRENT_CHART_REQUESTS &&
      pendingReportMetrics.length > 0
    ) {
      const request = pendingReportMetrics[0];
      pendingReportMetrics = pendingReportMetrics.slice(1);
      const report = selectedReport;
      const controller = runController;
      if (!report || !controller) continue;
      const generation = selectionGeneration;
      const viewport = reportViewports[request.identity] ?? null;
      const requestKey = viewportKey(viewport);
      activeReportRequests += 1;
      loadingReportMetrics = new Set([...loadingReportMetrics, request.identity]);
      void getChartHistory(request.runId, [request.metric], {
        maxBuckets: CHART_BUCKET_BUDGET,
        viewport: viewport ?? undefined,
        signal: controller.signal,
      })
        .then((history) => {
          if (generation !== selectionGeneration || selectedReport?.id !== report.id) return;
          if (viewportKey(reportViewports[request.identity] ?? null) !== requestKey) return;
          reportHistories = { ...reportHistories, [request.identity]: history };
          reportHistoryRequestKeys = {
            ...reportHistoryRequestKeys,
            [request.identity]: requestKey,
          };
        })
        .catch((reason) => {
          if (generation === selectionGeneration && !controller.signal.aborted) showError(reason);
        })
        .finally(() => {
          activeReportRequests -= 1;
          if (generation === selectionGeneration) {
            const nextLoading = new Set(loadingReportMetrics);
            nextLoading.delete(request.identity);
            loadingReportMetrics = nextLoading;
            if (
              selectedReport?.id === report.id &&
              viewportKey(reportViewports[request.identity] ?? null) !== requestKey
            ) {
              queueReportMetric(request);
            }
          }
          drainReportMetricQueue();
        });
    }
  }

  function reportMetricIdentity(panel: ReportPanel, metric: string): string {
    return `${panel.id}:${panel.run_id ?? ""}:${metric}`;
  }

  function chartVisibility(metric: string, visible: boolean): void {
    const next = new Set(visibleMetrics);
    if (visible) next.add(metric);
    else next.delete(metric);
    visibleMetrics = next;
    if (visible) queueMetric(metric);
  }

  function chartViewport(metric: string, stepMin: number | null, stepMax: number | null): void {
    const viewport = normalizedViewport(stepMin, stepMax);
    if (viewportKey(metricViewports[metric] ?? null) === viewportKey(viewport)) return;
    metricViewports = { ...metricViewports, [metric]: viewport };
    queueMetric(metric);
  }

  function queueMetric(metric: string): void {
    const run = selectedRun;
    if (!run) return;
    const requestKey = metricRequestKey(run.metric_revision, metricViewports[metric] ?? null);
    if (historyRequestKeys[metric] === requestKey) return;
    if (loadingMetrics.has(metric) || pendingMetrics.includes(metric)) return;
    pendingMetrics = [...pendingMetrics, metric];
    drainMetricQueue();
  }

  function drainMetricQueue(): void {
    while (activeChartRequests < MAX_CONCURRENT_CHART_REQUESTS && pendingMetrics.length > 0) {
      const metric = pendingMetrics[0];
      pendingMetrics = pendingMetrics.slice(1);
      const run = selectedRun;
      const controller = runController;
      if (!run || !controller) continue;
      const generation = selectionGeneration;
      const viewport = metricViewports[metric] ?? null;
      const requestKey = metricRequestKey(run.metric_revision, viewport);
      activeChartRequests += 1;
      loadingMetrics = new Set([...loadingMetrics, metric]);
      void loadMetric(run, metric, viewport, requestKey, generation, controller.signal)
        .catch((reason) => {
          if (generation === selectionGeneration && !controller.signal.aborted) showError(reason);
        })
        .finally(() => {
          activeChartRequests -= 1;
          if (generation === selectionGeneration) {
            const nextLoading = new Set(loadingMetrics);
            nextLoading.delete(metric);
            loadingMetrics = nextLoading;
            if (
              selectedRun?.id === run.id &&
              metricRequestKey(selectedRun.metric_revision, metricViewports[metric] ?? null) !==
                requestKey
            ) {
              queueMetric(metric);
            }
          }
          drainMetricQueue();
        });
    }
  }

  async function loadMetric(
    run: Run,
    metric: string,
    viewport: ChartHistoryViewport | null,
    requestKey: string,
    generation: number,
    signal: AbortSignal,
  ): Promise<void> {
    const revision = run.metric_revision;
    const cached = historyCache.get(run.id, metric, revision, viewport?.stepMin, viewport?.stepMax);
    if (cached) {
      publishHistory(run.id, metric, requestKey, generation, cached);
      return;
    }

    const result = await getChartHistory(run.id, [metric], {
      maxBuckets: CHART_BUCKET_BUDGET,
      viewport: viewport ?? undefined,
      signal,
    });
    historyCache.set(run.id, metric, revision, result, viewport?.stepMin, viewport?.stepMax);
    publishHistory(run.id, metric, requestKey, generation, result);
  }

  function publishHistory(
    runId: string,
    metric: string,
    requestKey: string,
    generation: number,
    result: ChartHistory,
  ): void {
    if (generation !== selectionGeneration || selectedRun?.id !== runId) return;
    if (
      metricRequestKey(selectedRun.metric_revision, metricViewports[metric] ?? null) !== requestKey
    ) {
      return;
    }
    histories = { ...histories, [metric]: result };
    historyRequestKeys = { ...historyRequestKeys, [metric]: requestKey };
  }

  async function refreshSelectedRun(): Promise<void> {
    const run = selectedRun;
    const controller = runController;
    if (!run || !controller || refreshingRun || run.state !== "running") return;
    refreshingRun = true;
    try {
      const latest = await getRun(run.id, controller.signal);
      if (selectedRun?.id !== latest.id) return;
      const revisionChanged = latest.metric_revision !== selectedRun.metric_revision;
      selectedRun = latest;
      runs = runs.map((candidate) => (candidate.id === latest.id ? latest : candidate));
      if (activeRunTab === "summary") alerts = await getAlerts(latest.id, controller.signal);
      if (activeRunTab === "media") {
        const page = await getRichValuePage(latest.id, undefined, controller.signal);
        richValues = page.items;
        richValueCursor = page.nextBefore;
      }
      if (activeRunTab === "artifacts") {
        const page = await getRunArtifactPage(latest.id, undefined, controller.signal);
        artifacts = page.items;
        artifactCursor = page.nextBefore;
      }
      if (activeRunTab === "traces") {
        const page = await getTracePage(latest.id, traceSearch, undefined, controller.signal);
        traces = page.items;
        traceCursor = page.nextBefore;
      }
      if (revisionChanged) {
        metricKeys = await getMetricKeys(latest.id, controller.signal);
        if (activeRunTab === "metrics") {
          for (const metric of visibleMetrics) queueMetric(metric);
        }
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    } finally {
      refreshingRun = false;
    }
  }

  function showError(reason: unknown): void {
    error = reason instanceof Error ? reason.message : "Unable to reach Runloom";
  }

  function formatValue(value: unknown): string {
    if (typeof value === "number") {
      return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
    }
    if (typeof value === "string") return value;
    return JSON.stringify(value);
  }

  function formatAlertTime(timestampMs: number): string {
    return new Date(timestampMs).toLocaleString();
  }

  async function searchTraces(): Promise<void> {
    const run = selectedRun;
    const controller = runController;
    if (!run || !controller || traceSearchLoading) return;
    traceSearchLoading = true;
    try {
      const page = await getTracePage(run.id, traceSearch, undefined, controller.signal);
      traces = page.items;
      traceCursor = page.nextBefore;
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    } finally {
      traceSearchLoading = false;
    }
  }

  function traceDuration(span: TraceSpan): string {
    return `${Math.max(span.end_time_ms - span.start_time_ms, 0).toLocaleString()} ms`;
  }

  function traceMessages(span: TraceSpan): Array<{ role: string; content: string }> {
    const messages = span.preview.messages;
    if (!Array.isArray(messages)) return [];
    return messages.flatMap((message) => {
      if (typeof message !== "object" || message === null) return [];
      const candidate = message as Record<string, unknown>;
      if (typeof candidate.role !== "string" || typeof candidate.content !== "string") return [];
      return [{ role: candidate.role, content: candidate.content }];
    });
  }

  function selectRunTab(tab: RunTab): void {
    activeRunTab = tab;
    void ensureRunTabLoaded(tab);
  }

  async function ensureRunTabLoaded(tab: RunTab): Promise<void> {
    const run = selectedRun;
    const controller = runController;
    if (
      !run ||
      !controller ||
      loadedRunTabs.has(tab) ||
      loadingRunTabs.has(tab) ||
      tab === "configuration" ||
      tab === "metrics"
    ) {
      return;
    }
    loadingRunTabs = new Set([...loadingRunTabs, tab]);
    try {
      if (tab === "summary") alerts = await getAlerts(run.id, controller.signal);
      else if (tab === "media") {
        const page = await getRichValuePage(run.id, undefined, controller.signal);
        richValues = page.items;
        richValueCursor = page.nextBefore;
      } else if (tab === "artifacts") {
        const page = await getRunArtifactPage(run.id, undefined, controller.signal);
        artifacts = page.items;
        artifactCursor = page.nextBefore;
      } else if (tab === "traces") {
        const page = await getTracePage(run.id, traceSearch, undefined, controller.signal);
        traces = page.items;
        traceCursor = page.nextBefore;
      }
      if (selectedRun?.id === run.id) loadedRunTabs = new Set([...loadedRunTabs, tab]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    } finally {
      const next = new Set(loadingRunTabs);
      next.delete(tab);
      loadingRunTabs = next;
    }
  }

  async function loadMore(tab: "media" | "artifacts" | "traces"): Promise<void> {
    const run = selectedRun;
    const controller = runController;
    const cursor =
      tab === "media" ? richValueCursor : tab === "artifacts" ? artifactCursor : traceCursor;
    if (!run || !controller || !cursor || loadingMoreTab) return;
    loadingMoreTab = tab;
    try {
      if (tab === "media") {
        const page = await getRichValuePage(run.id, cursor, controller.signal);
        richValues = [...richValues, ...page.items];
        richValueCursor = page.nextBefore;
      } else if (tab === "artifacts") {
        const page = await getRunArtifactPage(run.id, cursor, controller.signal);
        artifacts = [...artifacts, ...page.items];
        artifactCursor = page.nextBefore;
      } else {
        const page = await getTracePage(run.id, traceSearch, cursor, controller.signal);
        traces = [...traces, ...page.items];
        traceCursor = page.nextBefore;
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    } finally {
      loadingMoreTab = null;
    }
  }

  function handleRunTabKey(event: KeyboardEvent, index: number): void {
    let nextIndex: number | null = null;
    if (event.key === "ArrowRight") nextIndex = (index + 1) % RUN_TABS.length;
    else if (event.key === "ArrowLeft") nextIndex = (index - 1 + RUN_TABS.length) % RUN_TABS.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = RUN_TABS.length - 1;
    if (nextIndex === null) return;
    event.preventDefault();
    const next = RUN_TABS[nextIndex];
    selectRunTab(next.id);
    queueMicrotask(() => document.getElementById(`run-tab-${next.id}`)?.focus());
  }

  function runTabCount(tab: RunTab): number {
    if (tab === "summary") return Object.keys(selectedRun?.summary ?? {}).length;
    if (tab === "configuration") return Object.keys(selectedRun?.config ?? {}).length;
    if (tab === "metrics") return metricKeys.length;
    if (tab === "media") return richValues.length;
    if (tab === "traces") return traces.length;
    return artifacts.length;
  }

  function runTabCountLabel(tab: RunTab): string {
    if (loadingRunTabs.has(tab)) return "…";
    if (["media", "traces", "artifacts"].includes(tab) && !loadedRunTabs.has(tab)) return "—";
    return runTabCount(tab).toLocaleString();
  }
</script>

<svelte:head>
  <title>Runloom</title>
  <meta
    name="description"
    content="Runloom is a lossless, self-hosted experiment tracker built for large histories."
  />
</svelte:head>

<div class="app-shell">
  <header>
    <div class="brand"><h1>Runloom</h1></div>
    <div class="status" class:failed={Boolean(error)}>
      <span class="status-dot" aria-hidden="true"></span>
      {health ? `${health.status} · v${health.version}` : "connecting"}
    </div>
  </header>

  <main class="content">
    {#if error}<div class="error" role="alert">{error}</div>{/if}

    {#if loading}
      <section class="empty">Loading the loom…</section>
    {:else if projects.length === 0}
      <section class="empty">
        <p class="eyebrow">Ready for a first run</p>
        <h1>No experiments yet.</h1>
        <code>runloom.init(project="my-project")</code>
      </section>
    {:else}
      <div class="workspace">
        <aside>
          <label for="project">Project</label>
          <select
            id="project"
            value={selectedProject}
            onchange={(event) => chooseProject(event.currentTarget.value)}
          >
            {#each projects as project (project.id)}
              <option value={project.name}>{project.name} · {project.run_count}</option>
            {/each}
          </select>

          {#if reports.length > 0}<p class="nav-label">Reports</p>{/if}
          <div class="run-list" aria-label="Reports" class:hidden={reports.length === 0}>
            {#each reports as report (report.id)}
              <button
                class:active={selectedReport?.id === report.id}
                onclick={() => chooseReport(report)}
              >
                <span>{report.name}</span>
                <small>{report.layout.panels.length} panels</small>
              </button>
            {/each}
          </div>

          {#if runs.length > 0}<p class="nav-label">Runs</p>{/if}
          <div class="run-list" aria-label="Runs" class:hidden={runs.length === 0}>
            {#each runs as run (run.id)}
              <button class:active={selectedRun?.id === run.id} onclick={() => chooseRun(run)}>
                <span>{run.name}</span>
                <small
                  class="run-list-state"
                  class:live={run.state === "running"}
                  class:finished={run.state === "finished"}
                >
                  <Icon name={run.state === "running" ? "activity" : "check"} size={12} />
                  <span>{run.state}</span>
                  <span>r{run.metric_revision}</span>
                </small>
              </button>
            {/each}
          </div>
        </aside>

        <section class="run-view">
          {#if selectedReport}
            <div class="run-heading">
              <div>
                <p class="eyebrow">{selectedReport.project} / report</p>
                <h1>{selectedReport.name}</h1>
                {#if selectedReport.description}
                  <p class="report-description">{selectedReport.description}</p>
                {/if}
              </div>
              <span class="run-state">{selectedReport.layout.panels.length} panels</span>
            </div>

            <div class="report-grid" style={`--report-columns: ${selectedReport.layout.columns}`}>
              {#each selectedReport.layout.panels as panel (panel.id)}
                {#if panel.kind === "markdown"}
                  <article
                    class="report-markdown-panel"
                    style={`grid-column: span ${Math.min(panel.width, selectedReport.layout.columns)}; min-height: ${panel.height}px`}
                  >
                    <div class="card-heading">
                      <div><small>Markdown</small><strong>{panel.title}</strong></div>
                    </div>
                    <MarkdownPanel source={panel.markdown ?? ""} />
                  </article>
                {:else}
                  <section
                    class="report-metric-panel"
                    style={`grid-column: span ${Math.min(panel.width, selectedReport.layout.columns)}; min-height: ${panel.height}px`}
                    aria-label={`${panel.title} report panel`}
                  >
                    <div class="report-panel-heading">
                      <strong>{panel.title}</strong>
                      <small>run {panel.run_id?.slice(0, 8)}</small>
                    </div>
                    <div class="report-metric-grid">
                      {#each panel.metric_keys as metric (metric)}
                        {@const identity = reportMetricIdentity(panel, metric)}
                        <MetricChart
                          {metric}
                          {identity}
                          title={panel.metric_keys.length === 1 ? metric : metric}
                          history={reportHistories[identity]}
                          loading={loadingReportMetrics.has(identity)}
                          onvisibilitychange={(_, visible) =>
                            reportChartVisibility(panel, metric, visible)}
                          onviewportchange={(_, stepMin, stepMax) =>
                            reportChartViewport(panel, metric, stepMin, stepMax)}
                        />
                      {/each}
                    </div>
                  </section>
                {/if}
              {/each}
            </div>
          {:else if selectedRun}
            <div class="run-heading">
              <div>
                <p class="eyebrow">{selectedRun.project} / {selectedRun.id.slice(0, 8)}</p>
                <h1>{selectedRun.name}</h1>
              </div>
              <span
                class="run-state"
                class:live={selectedRun.state === "running"}
                class:finished={selectedRun.state === "finished"}
              >
                <Icon name={selectedRun.state === "running" ? "activity" : "check"} size={14} />
                {selectedRun.state}
              </span>
            </div>

            <div class="run-tabs" role="tablist" aria-label="Run data">
              {#each RUN_TABS as tab, index (tab.id)}
                <button
                  id={`run-tab-${tab.id}`}
                  type="button"
                  role="tab"
                  aria-selected={activeRunTab === tab.id}
                  aria-controls={`run-panel-${tab.id}`}
                  tabindex={activeRunTab === tab.id ? 0 : -1}
                  class:active={activeRunTab === tab.id}
                  onclick={() => selectRunTab(tab.id)}
                  onkeydown={(event) => handleRunTabKey(event, index)}
                >
                  <Icon name={tab.icon} size={15} />
                  <span>{tab.label}</span>
                  <small>{runTabCountLabel(tab.id)}</small>
                </button>
              {/each}
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-summary"
              role="tabpanel"
              aria-labelledby="run-tab-summary"
              hidden={activeRunTab !== "summary"}
            >
              <div class="section-heading">
                <div>
                  <p class="eyebrow">Final and derived values</p>
                  <h2>Summary</h2>
                </div>
                <span>{Object.keys(selectedRun.summary).length.toLocaleString()} fields</span>
              </div>
              <div class="tree-panel">
                <JsonTreeNode name="" value={selectedRun.summary} root />
              </div>

              {#if alerts.length > 0}
                <div class="section-heading alerts-heading">
                  <h2>Alerts</h2>
                  <span>{alerts.length} most recent</span>
                </div>
                <div class="alert-list">
                  {#each alerts as alert (alert.id)}
                    <div
                      class="alert-row"
                      class:warn={alert.level === "warn"}
                      class:error-level={alert.level === "error"}
                    >
                      <span class="alert-level">{alert.level}</span>
                      <div>
                        <strong>{alert.title}</strong>
                        {#if alert.text}<p>{alert.text}</p>{/if}
                      </div>
                      <small>
                        {alert.step === null ? "no step" : `step ${alert.step}`} · {formatAlertTime(
                          alert.timestamp_ms,
                        )}
                      </small>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-configuration"
              role="tabpanel"
              aria-labelledby="run-tab-configuration"
              hidden={activeRunTab !== "configuration"}
            >
              <div class="section-heading">
                <div>
                  <p class="eyebrow">Expandable run inputs</p>
                  <h2>Configuration</h2>
                </div>
                <span>{Object.keys(selectedRun.config).length.toLocaleString()} fields</span>
              </div>
              <div class="tree-panel">
                <JsonTreeNode name="" value={selectedRun.config} root />
              </div>
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-metrics"
              role="tabpanel"
              aria-labelledby="run-tab-metrics"
              hidden={activeRunTab !== "metrics"}
            >
              <div class="section-heading metrics-toolbar">
                <div>
                  <p class="eyebrow">Exact bucket envelopes · four concurrent queries</p>
                  <h2>Metrics</h2>
                </div>
                <label class="search-control">
                  <Icon name="search" size={15} />
                  <input
                    type="search"
                    aria-label="Search metrics"
                    placeholder="Search metric keys"
                    bind:value={metricSearch}
                  />
                </label>
                <span>
                  {filteredMetricKeys.length.toLocaleString()} of {metricKeys.length.toLocaleString()}
                </span>
              </div>
              {#if filteredMetricKeys.length > 0}
                <div class="metric-grid">
                  {#each filteredMetricKeys as metric (`${selectedRun.id}:${metric}`)}
                    <MetricChart
                      {metric}
                      identity={`${selectedRun.id}:${metric}`}
                      history={histories[metric]}
                      loading={loadingMetrics.has(metric)}
                      onvisibilitychange={chartVisibility}
                      onviewportchange={chartViewport}
                    />
                  {/each}
                </div>
              {:else}
                <section class="metric-empty">
                  {metricKeys.length === 0
                    ? "No scalar metrics logged yet."
                    : "No metric keys match this search."}
                </section>
              {/if}
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-media"
              role="tabpanel"
              aria-labelledby="run-tab-media"
              hidden={activeRunTab !== "media"}
            >
              <div class="section-heading">
                <div>
                  <p class="eyebrow">Native playback and previews</p>
                  <h2>Media & data</h2>
                </div>
                <span>{richValues.length.toLocaleString()} snapshots</span>
              </div>
              {#if loadingRunTabs.has("media")}
                <section class="metric-empty">Loading media…</section>
              {:else}
                <MediaTimeline values={richValues} />
                {#if richValueCursor}
                  <button
                    class="load-more"
                    type="button"
                    disabled={loadingMoreTab !== null}
                    onclick={() => void loadMore("media")}
                    >{loadingMoreTab === "media" ? "Loading…" : "Load 100 more"}</button
                  >
                {/if}
              {/if}
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-traces"
              role="tabpanel"
              aria-labelledby="run-tab-traces"
              hidden={activeRunTab !== "traces"}
            >
              <div class="section-heading trace-heading">
                <div>
                  <p class="eyebrow">Indexed metadata · payloads in object storage</p>
                  <h2>Traces</h2>
                </div>
                <form
                  class="trace-search"
                  onsubmit={(event) => {
                    event.preventDefault();
                    void searchTraces();
                  }}
                >
                  <label class="search-control">
                    <Icon name="search" size={15} />
                    <input
                      aria-label="Search traces"
                      placeholder="Search traces and messages"
                      bind:value={traceSearch}
                    />
                  </label>
                  <button
                    class="icon-button"
                    type="submit"
                    disabled={traceSearchLoading}
                    aria-label="Search traces"><Icon name="search" size={15} /></button
                  >
                </form>
              </div>
              {#if loadingRunTabs.has("traces")}
                <section class="metric-empty">Loading traces…</section>
              {:else if traces.length > 0}
                <div class="trace-list">
                  {#each traces as span (span.id)}
                    <article class="trace-card" class:trace-error={span.status === "error"}>
                      <div class="trace-title">
                        <span>{span.kind}</span>
                        <strong>{span.name}</strong>
                        <small>{span.status} · {traceDuration(span)}</small>
                        {#if span.payload}
                          <a
                            class="icon-button"
                            href={blobUrl(span.payload)}
                            download={span.payload.file_name ?? undefined}
                            aria-label={`Download ${span.name} payload`}
                            ><Icon name="download" size={15} /></a
                          >
                        {/if}
                      </div>
                      <div class="trace-identifiers">
                        <span>trace {span.trace_id}</span>
                        {#if span.parent_span_id}<span>parent {span.parent_span_id}</span>{/if}
                        <span>{span.step === null ? "no step" : `step ${span.step}`}</span>
                      </div>
                      {#if Object.keys(span.attributes).length > 0}
                        <dl class="trace-attributes">
                          {#each Object.entries(span.attributes) as [key, value]}
                            <div>
                              <dt>{key}</dt>
                              <dd>{formatValue(value)}</dd>
                            </div>
                          {/each}
                        </dl>
                      {/if}
                      {#if traceMessages(span).length > 0}
                        <div class="trace-messages">
                          {#each traceMessages(span) as message}
                            <div>
                              <strong>{message.role}</strong>
                              <p>{message.content}</p>
                            </div>
                          {/each}
                        </div>
                      {/if}
                    </article>
                  {/each}
                </div>
              {:else}
                <section class="metric-empty">
                  {traceSearch.trim()
                    ? "No traces match this search."
                    : "No structured traces logged yet."}
                </section>
              {/if}
              {#if traceCursor}
                <button
                  class="load-more"
                  type="button"
                  disabled={loadingMoreTab !== null}
                  onclick={() => void loadMore("traces")}
                  >{loadingMoreTab === "traces" ? "Loading…" : "Load 100 more"}</button
                >
              {/if}
            </div>

            <div
              class="run-tab-panel"
              id="run-panel-artifacts"
              role="tabpanel"
              aria-labelledby="run-tab-artifacts"
              hidden={activeRunTab !== "artifacts"}
            >
              <div class="section-heading">
                <div>
                  <p class="eyebrow">Versioned inputs and outputs</p>
                  <h2>Artifacts</h2>
                </div>
                <span>{artifacts.length.toLocaleString()} lineage links</span>
              </div>
              {#if loadingRunTabs.has("artifacts")}
                <section class="metric-empty">Loading artifacts…</section>
              {:else}
                <ArtifactBrowser {artifacts} />
                {#if artifactCursor}
                  <button
                    class="load-more"
                    type="button"
                    disabled={loadingMoreTab !== null}
                    onclick={() => void loadMore("artifacts")}
                    >{loadingMoreTab === "artifacts" ? "Loading…" : "Load 100 more"}</button
                  >
                {/if}
              {/if}
            </div>
          {:else}
            <section class="empty">This project has no runs.</section>
          {/if}
        </section>
      </div>
    {/if}
  </main>
</div>
