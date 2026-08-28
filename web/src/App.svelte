<script lang="ts">
  import { onMount } from "svelte";

  import {
    blobUrl,
    comparisonSeriesHistory,
    getAlerts,
    getChartHistory,
    getComparisonChartHistory,
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
    type ComparisonChartHistory,
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
    normalizeChartViewport as normalizedViewport,
  } from "./lib/chart-request";
  import { chartPreferenceIdentity } from "./lib/chart-preferences";
  import {
    MAX_SELECTED_RUNS,
    METRIC_CHART_PAGE_SIZE,
    comparisonCacheKey,
    metricAvailability,
    metricPage,
    normalizeRunSelection,
    planComparisonBatches,
    readComparisonUrl,
    runStyle,
    writeComparisonUrl,
    type ComparisonUrlState,
    type MetricSetMode,
    type RunAlignment,
  } from "./lib/comparison-state";
  import {
    CHART_BUCKET_BUDGET,
    COMPARISON_CACHE_MAX_CELLS,
    COMPARISON_CACHE_MAX_ENTRIES,
    COMPARISON_CACHE_MAX_ESTIMATED_BYTES,
    ChartHistoryCache,
    ComparisonHistoryCache,
  } from "./lib/history-cache";
  import ArtifactBrowser from "./lib/ArtifactBrowser.svelte";
  import Icon from "./lib/Icon.svelte";
  import JsonTreeNode from "./lib/JsonTreeNode.svelte";
  import MediaTimeline from "./lib/MediaTimeline.svelte";
  import MetricChart from "./lib/MetricChart.svelte";
  import MarkdownPanel from "./lib/MarkdownPanel.svelte";
  import { filterMetricKeys } from "./lib/metric-filter";
  import { QueryScheduler } from "./lib/query-scheduler";

  const MAX_CONCURRENT_CHART_REQUESTS = 4;
  const LIVE_REFRESH_MS = 2_000;
  const historyCache = new ChartHistoryCache();
  const comparisonHistoryCache = new ComparisonHistoryCache({
    maxEntries: COMPARISON_CACHE_MAX_ENTRIES,
    maxCells: COMPARISON_CACHE_MAX_CELLS,
    maxEstimatedBytes: COMPARISON_CACHE_MAX_ESTIMATED_BYTES,
  });
  const chartScheduler = new QueryScheduler(MAX_CONCURRENT_CHART_REQUESTS);
  const RUN_TABS = [
    { id: "summary", label: "Summary", icon: "summary" },
    { id: "configuration", label: "Configuration", icon: "settings" },
    { id: "metrics", label: "Metrics", icon: "chart" },
    { id: "media", label: "Media", icon: "media" },
    { id: "traces", label: "Traces", icon: "trace" },
    { id: "artifacts", label: "Artifacts", icon: "archive" },
  ] as const;
  type RunTab = (typeof RUN_TABS)[number]["id"];
  const VALID_RUN_TABS = new Set<RunTab>(RUN_TABS.map((tab) => tab.id));

  let health: Health | null = null;
  let projects: Project[] = [];
  let runs: Run[] = [];
  let reports: Report[] = [];
  let selectedProject = "";
  let selectedRun: Run | null = null;
  let selectedRunIds: string[] = [];
  let metricKeysByRun: Record<string, string[]> = {};
  let loadingMetricKeys = new Set<string>();
  let metricMode: MetricSetMode = "union";
  let xAlignment: RunAlignment = "step";
  let selectionNotice: string | null = null;
  let selectedReport: Report | null = null;
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
  let comparisonHistories: Record<string, ComparisonChartHistory> = {};
  let historyRequestKeys: Record<string, string> = {};
  let metricViewports: Record<string, ChartHistoryViewport | null> = {};
  let urlViewportMetric: string | null = null;
  let loadingMetrics = new Set<string>();
  let visibleMetrics = new Set<string>();
  let scheduledMetricStateKeys: Record<string, string> = {};
  let scheduledMetricBatchKeys: Record<string, string> = {};
  let metricPageIndex = 0;
  let instantiatedMetricSignature = "";
  let fullRangeFlushScheduled = false;
  const fullRangeBatchMetrics = new Map<string, Set<string>>();
  let reportHistories: Record<string, ChartHistory> = {};
  let reportHistoryRequestKeys: Record<string, string> = {};
  let reportViewports: Record<string, ChartHistoryViewport | null> = {};
  let loadingReportMetrics = new Set<string>();
  let refreshingRuns = false;
  let error: string | null = null;
  let loading = true;
  let projectController: AbortController | null = null;
  let runController: AbortController | null = null;

  $: comparisonRuns = selectedRunIds.flatMap((runId) => {
    const run = runs.find((candidate) => candidate.id === runId);
    return run ? [run] : [];
  });
  $: metricCatalog = metricAvailability(selectedRunIds, metricKeysByRun, metricMode);
  $: filteredMetricNames = new Set(
    filterMetricKeys(
      metricCatalog.map((entry) => entry.key),
      metricSearch,
    ),
  );
  $: filteredMetricCatalog = metricCatalog.filter((entry) => filteredMetricNames.has(entry.key));
  $: metricPageResult = metricPage(filteredMetricCatalog, metricPageIndex);
  $: pagedMetricCatalog = metricPageResult.values;
  $: pagedMetricSignature = JSON.stringify(pagedMetricCatalog.map((entry) => entry.key));
  $: if (pagedMetricSignature !== instantiatedMetricSignature) {
    instantiatedMetricSignature = pagedMetricSignature;
    evictMetricsOutsidePage(new Set(pagedMetricCatalog.map((entry) => entry.key)));
  }

  onMount(() => {
    const controller = new AbortController();
    const refreshTimer = window.setInterval(refreshSelectedRuns, LIVE_REFRESH_MS);
    const handlePopState = () => void restoreFromLocation();
    window.addEventListener("popstate", handlePopState);
    Promise.all([getHealth(controller.signal), getProjects(controller.signal)])
      .then(async ([healthResult, projectResult]) => {
        health = healthResult;
        projects = projectResult;
        const restored = readComparisonUrl(
          new URL(window.location.href),
          VALID_RUN_TABS,
          "metrics",
        );
        const project =
          projects.find((candidate) => candidate.name === restored.project) ?? projects[0];
        if (project) await chooseProject(project.name, "replace", restored);
      })
      .catch(showError)
      .finally(() => {
        loading = false;
      });
    return () => {
      controller.abort();
      projectController?.abort();
      runController?.abort();
      chartScheduler.cancelAll();
      window.removeEventListener("popstate", handlePopState);
      window.clearInterval(refreshTimer);
    };
  });

  async function chooseProject(
    name: string,
    historyMode: "push" | "replace" | "none" = "push",
    restored?: ComparisonUrlState<RunTab>,
  ): Promise<void> {
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
      const state: ComparisonUrlState<RunTab> = restored ?? {
        project: name,
        runIds: [],
        runSelectionSpecified: false,
        primaryRunId: null,
        tab: "metrics",
        metricMode: "union",
        search: "",
        alignment: "step",
        chartMetric: null,
        chartViewport: null,
      };
      await applyComparisonState(state, !state.runSelectionSpecified, controller.signal);
      if (historyMode !== "none") syncComparisonUrl(historyMode);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function applyComparisonState(
    state: ComparisonUrlState<RunTab>,
    selectDefault: boolean,
    signal: AbortSignal,
  ): Promise<void> {
    const available = new Set(runs.map((run) => run.id));
    const requestedRunIds =
      selectDefault && state.runIds.length === 0 && runs[0] ? [runs[0].id] : state.runIds;
    const normalized = normalizeRunSelection(requestedRunIds, available, state.primaryRunId);
    selectedRunIds = normalized.runIds;
    metricMode = state.metricMode;
    metricSearch = state.search;
    xAlignment = state.alignment;
    activeRunTab = state.tab;
    selectionNotice = null;
    resetChartState(false);
    await activatePrimaryRun(normalized.primaryRunId, true);
    await loadMetricKeysForRuns(normalized.runIds, signal);
    if (
      state.chartMetric &&
      state.chartViewport &&
      metricAvailability(selectedRunIds, metricKeysByRun, "union").some(
        (entry) => entry.key === state.chartMetric,
      )
    ) {
      const viewport = normalizedViewport(state.chartViewport.minimum, state.chartViewport.maximum);
      if (viewport) {
        urlViewportMetric = state.chartMetric;
        metricViewports = { [state.chartMetric]: viewport };
      }
    }
    queueVisibleMetrics();
  }

  async function chooseRun(run: Run): Promise<void> {
    if (!selectedRunIds.includes(run.id)) {
      if (selectedRunIds.length >= MAX_SELECTED_RUNS) {
        selectionNotice = `Up to ${MAX_SELECTED_RUNS} runs can be visible at once.`;
        return;
      }
      selectedRunIds = [...selectedRunIds, run.id];
      await loadMetricKeysForRuns([run.id], projectController?.signal);
      resetChartState(false);
    }
    await activatePrimaryRun(run.id, true);
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  async function activatePrimaryRun(runId: string | null, loadTab: boolean): Promise<void> {
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    selectedReport = null;
    resetReportState();
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
    selectedRun = runs.find((run) => run.id === runId) ?? null;
    error = null;
    if (!selectedRun) return;
    loadedRunTabs = new Set(["configuration", "metrics"]);
    if (loadTab) await ensureRunTabLoaded(activeRunTab);
  }

  async function toggleRun(run: Run, selected: boolean): Promise<void> {
    selectionNotice = null;
    if (selected) {
      if (selectedRunIds.includes(run.id)) return;
      if (selectedRunIds.length >= MAX_SELECTED_RUNS) {
        selectionNotice = `Up to ${MAX_SELECTED_RUNS} runs can be visible at once.`;
        return;
      }
      selectedRunIds = [...selectedRunIds, run.id];
      await loadMetricKeysForRuns([run.id], projectController?.signal);
      if (!selectedRun) await activatePrimaryRun(run.id, true);
    } else {
      selectedRunIds = selectedRunIds.filter((runId) => runId !== run.id);
      if (selectedRun?.id === run.id) await activatePrimaryRun(selectedRunIds[0] ?? null, true);
    }
    resetChartState(false);
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  async function isolateRun(run: Run): Promise<void> {
    selectedRunIds = [run.id];
    selectionNotice = null;
    await loadMetricKeysForRuns([run.id], projectController?.signal);
    if (selectedRun?.id !== run.id) await activatePrimaryRun(run.id, true);
    resetChartState(false);
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  async function clearRunSelection(): Promise<void> {
    selectedRunIds = [];
    selectionNotice = null;
    await activatePrimaryRun(null, false);
    resetChartState();
    syncComparisonUrl("push");
  }

  async function loadMetricKeysForRuns(
    runIds: readonly string[],
    signal?: AbortSignal,
  ): Promise<void> {
    const missing = runIds.filter((runId) => metricKeysByRun[runId] === undefined);
    if (missing.length === 0) return;
    loadingMetricKeys = new Set([...loadingMetricKeys, ...missing]);
    try {
      const results = await Promise.all(
        missing.map(async (runId) => [runId, await getMetricKeys(runId, signal)] as const),
      );
      metricKeysByRun = { ...metricKeysByRun, ...Object.fromEntries(results) };
    } catch (reason) {
      if (!signal?.aborted) showError(reason);
    } finally {
      const next = new Set(loadingMetricKeys);
      for (const runId of missing) next.delete(runId);
      loadingMetricKeys = next;
    }
  }

  async function restoreFromLocation(): Promise<void> {
    const state = readComparisonUrl(new URL(window.location.href), VALID_RUN_TABS, "metrics");
    const requestedProject =
      projects.find((project) => project.name === state.project) ?? projects[0];
    if (!requestedProject) return;
    if (requestedProject.name !== selectedProject) {
      await chooseProject(requestedProject.name, "none", state);
      return;
    }
    const controller = projectController;
    if (!controller) return;
    await applyComparisonState(state, !state.runSelectionSpecified, controller.signal);
  }

  function syncComparisonUrl(mode: "push" | "replace"): void {
    const state: ComparisonUrlState<RunTab> = {
      project: selectedProject || null,
      runIds: selectedRunIds,
      runSelectionSpecified: true,
      primaryRunId: selectedRun?.id ?? null,
      tab: activeRunTab,
      metricMode,
      search: metricSearch,
      alignment: xAlignment,
      chartMetric: urlViewportMetric,
      chartViewport:
        urlViewportMetric && metricViewports[urlViewportMetric]
          ? {
              minimum: metricViewports[urlViewportMetric]!.stepMin,
              maximum: metricViewports[urlViewportMetric]!.stepMax,
            }
          : null,
    };
    const next = writeComparisonUrl(new URL(window.location.href), state);
    if (next.href === window.location.href) return;
    window.history[mode === "push" ? "pushState" : "replaceState"]({}, "", next);
  }

  function resetRunSelection(): void {
    runController?.abort();
    selectedRun = null;
    selectedRunIds = [];
    selectedReport = null;
    metricKeysByRun = {};
    loadingMetricKeys = new Set();
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

  function resetChartState(resetVisibility = true): void {
    chartScheduler.cancelAll();
    comparisonHistories = {};
    historyRequestKeys = {};
    metricViewports = {};
    urlViewportMetric = null;
    loadingMetrics = new Set();
    scheduledMetricStateKeys = {};
    scheduledMetricBatchKeys = {};
    metricPageIndex = 0;
    fullRangeFlushScheduled = false;
    fullRangeBatchMetrics.clear();
    if (resetVisibility) visibleMetrics = new Set();
  }

  function resetReportState(): void {
    reportHistories = {};
    reportHistoryRequestKeys = {};
    reportViewports = {};
    loadingReportMetrics = new Set();
  }

  function chooseReport(report: Report): void {
    runController?.abort();
    runController = new AbortController();
    selectedRun = null;
    selectedReport = report;
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
    chartScheduler.cancelAll();
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
    const report = selectedReport;
    if (!report) return;
    const viewport = reportViewports[request.identity] ?? null;
    const revision = runs.find((run) => run.id === request.runId)?.metric_revision ?? 0;
    const requestKey = `${CHART_BUCKET_BUDGET}:${revision}:${viewportKey(viewport)}`;
    if (reportHistoryRequestKeys[request.identity] === requestKey) return;
    loadingReportMetrics = new Set([...loadingReportMetrics, request.identity]);
    chartScheduler.schedule({
      identity: `report:${request.identity}`,
      requestKey,
      request: async (signal) => {
        const cached = historyCache.get(
          request.runId,
          request.metric,
          revision,
          CHART_BUCKET_BUDGET,
          viewport?.stepMin,
          viewport?.stepMax,
        );
        if (cached) return cached;
        const history = await getChartHistory(request.runId, [request.metric], {
          maxBuckets: CHART_BUCKET_BUDGET,
          viewport: viewport ?? undefined,
          signal,
        });
        historyCache.set(
          request.runId,
          request.metric,
          revision,
          CHART_BUCKET_BUDGET,
          history,
          viewport?.stepMin,
          viewport?.stepMax,
        );
        return history;
      },
      publish: (history, publishedKey) => {
        if (selectedReport?.id !== report.id) return;
        if (reportRequestKey(request) !== publishedKey) return;
        reportHistories = { ...reportHistories, [request.identity]: history };
        reportHistoryRequestKeys = {
          ...reportHistoryRequestKeys,
          [request.identity]: publishedKey,
        };
        finishReportLoading(request.identity);
      },
      reject: (reason) => {
        finishReportLoading(request.identity);
        showError(reason);
      },
    });
  }

  function reportRequestKey(request: { identity: string; runId: string }): string {
    const revision = runs.find((run) => run.id === request.runId)?.metric_revision ?? 0;
    return `${CHART_BUCKET_BUDGET}:${revision}:${viewportKey(reportViewports[request.identity] ?? null)}`;
  }

  function finishReportLoading(identity: string): void {
    const next = new Set(loadingReportMetrics);
    next.delete(identity);
    loadingReportMetrics = next;
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
    else evictMetricHistory(metric, false);
  }

  function chartViewport(metric: string, stepMin: number | null, stepMax: number | null): void {
    if (!visibleMetrics.has(metric)) return;
    const viewport = normalizedViewport(stepMin, stepMax);
    if (viewportKey(metricViewports[metric] ?? null) === viewportKey(viewport)) return;
    metricViewports = { ...metricViewports, [metric]: viewport };
    urlViewportMetric = viewport ? metric : null;
    queueMetric(metric);
    syncComparisonUrl("replace");
  }

  function queueMetric(metric: string): void {
    const viewport = metricViewports[metric] ?? null;
    if (!viewport) {
      scheduleFullRangeFlush();
      return;
    }
    const plan = deterministicComparisonPlans().find((candidatePlan) =>
      candidatePlan.candidates.some((candidate) => candidate.metric === metric),
    );
    if (!plan) return;
    const stateKey = comparisonMetricStateKey(metric, plan, viewport);
    if (historyRequestKeys[metric] === stateKey || scheduledMetricStateKeys[metric] === stateKey) {
      return;
    }
    scheduleComparisonPlan(
      plan,
      viewport,
      `comparison:${chartPreferenceIdentity(selectedProject, metric)}`,
      new Set([metric]),
    );
  }

  function scheduleFullRangeFlush(): void {
    if (fullRangeFlushScheduled) return;
    fullRangeFlushScheduled = true;
    queueMicrotask(() => {
      if (!fullRangeFlushScheduled) return;
      fullRangeFlushScheduled = false;
      flushFullRangeMetrics();
    });
  }

  function flushFullRangeMetrics(): void {
    for (const plan of deterministicComparisonPlans()) {
      const trackedMetrics = new Set(
        plan.candidates.flatMap((candidate) => {
          const metric = candidate.metric;
          if (!visibleMetrics.has(metric) || metricViewports[metric]) return [];
          const stateKey = comparisonMetricStateKey(metric, plan, null);
          return historyRequestKeys[metric] === stateKey ||
            scheduledMetricStateKeys[metric] === stateKey
            ? []
            : [metric];
        }),
      );
      if (trackedMetrics.size === 0) continue;
      const metrics = plan.candidates.map((candidate) => candidate.metric);
      const identity = `comparison-batch:${selectedProject}:${JSON.stringify(metrics)}`;
      scheduleComparisonPlan(plan, null, identity, trackedMetrics);
    }
  }

  function deterministicComparisonPlans() {
    const candidates = metricAvailability(selectedRunIds, metricKeysByRun, "union").flatMap(
      ({ key }) => {
        const candidate = comparisonCandidate(key);
        return candidate ? [candidate] : [];
      },
    );
    return planComparisonBatches(candidates, CHART_BUCKET_BUDGET);
  }

  function scheduleComparisonPlan(
    plan: ReturnType<typeof planComparisonBatches>[number],
    viewport: ChartHistoryViewport | null,
    identity: string,
    trackedMetrics = new Set(plan.candidates.map((candidate) => candidate.metric)),
  ): void {
    const project = selectedProject;
    const alignment = xAlignment;
    const stateKeys = Object.fromEntries(
      plan.candidates.map((candidate) => [
        candidate.metric,
        comparisonMetricStateKey(candidate.metric, plan, viewport),
      ]),
    );
    const requestKey = comparisonBatchRequestKey(plan, viewport);
    const nextLoading = new Set(loadingMetrics);
    const nextStateKeys = { ...scheduledMetricStateKeys };
    const nextBatchKeys = { ...scheduledMetricBatchKeys };
    for (const metric of trackedMetrics) {
      nextLoading.add(metric);
      nextStateKeys[metric] = stateKeys[metric];
      nextBatchKeys[metric] = requestKey;
    }
    loadingMetrics = nextLoading;
    scheduledMetricStateKeys = nextStateKeys;
    scheduledMetricBatchKeys = nextBatchKeys;
    if (!viewport) {
      fullRangeBatchMetrics.set(
        identity,
        new Set([...(fullRangeBatchMetrics.get(identity) ?? []), ...trackedMetrics]),
      );
    }
    chartScheduler.schedule({
      identity,
      requestKey,
      request: async (signal) => {
        const cached = comparisonHistoryCache.get(requestKey);
        if (cached) return cached;
        const response = await getComparisonChartHistory(
          project,
          plan.candidates.flatMap((candidate) =>
            candidate.runIds.map((runId) => ({ run_id: runId, key: candidate.metric })),
          ),
          {
            alignment: alignmentForApi(alignment),
            maxBuckets: plan.maxBuckets,
            viewport: viewport
              ? { minimum: viewport.stepMin, maximum: viewport.stepMax }
              : undefined,
            signal,
          },
        );
        comparisonHistoryCache.set(requestKey, response);
        return response;
      },
      publish: (response, publishedKey) => {
        if (project !== selectedProject || alignment !== xAlignment) return;
        const nextHistories = { ...comparisonHistories };
        const nextHistoryKeys = { ...historyRequestKeys };
        for (const candidate of plan.candidates) {
          const metric = candidate.metric;
          if (scheduledMetricBatchKeys[metric] !== publishedKey) continue;
          if (viewportKey(metricViewports[metric] ?? null) === viewportKey(viewport)) {
            nextHistories[metric] = comparisonResponseForMetric(response, metric);
            nextHistoryKeys[metric] = stateKeys[metric];
          }
          finishMetricRequest(metric, publishedKey);
        }
        comparisonHistories = nextHistories;
        historyRequestKeys = nextHistoryKeys;
        fullRangeBatchMetrics.delete(identity);
      },
      reject: (reason) => {
        for (const candidate of plan.candidates) {
          finishMetricRequest(candidate.metric, requestKey);
        }
        fullRangeBatchMetrics.delete(identity);
        showError(reason);
      },
    });
  }

  function comparisonCandidate(metric: string): { metric: string; runIds: string[] } | null {
    const runIds = currentComparisonRuns()
      .filter((run) => metricKeysByRun[run.id]?.includes(metric))
      .map((run) => run.id);
    return runIds.length > 0 ? { metric, runIds } : null;
  }

  function comparisonResponseForMetric(
    response: ComparisonChartHistory,
    metric: string,
  ): ComparisonChartHistory {
    const series = response.series.filter((candidate) => candidate.key === metric);
    const runIds = new Set(series.map((candidate) => candidate.run_id));
    return {
      ...response,
      runs: response.runs.filter((run) => runIds.has(run.run_id)),
      series,
    };
  }

  function comparisonMetricStateKey(
    metric: string,
    plan: ReturnType<typeof planComparisonBatches>[number],
    viewport: ChartHistoryViewport | null,
  ): string {
    return JSON.stringify({
      metric,
      batch: comparisonBatchRequestKey(plan, viewport),
    });
  }

  function comparisonBatchRequestKey(
    plan: ReturnType<typeof planComparisonBatches>[number],
    viewport: ChartHistoryViewport | null,
  ): string {
    return comparisonCacheKey(
      selectedProject,
      xAlignment,
      plan.maxBuckets,
      viewport ? { minimum: viewport.stepMin, maximum: viewport.stepMax } : null,
      plan.candidates.map((candidate) => ({
        metric: candidate.metric,
        revisions: candidate.runIds.map(
          (runId) => [runId, runs.find((run) => run.id === runId)?.metric_revision ?? -1] as const,
        ),
      })),
    );
  }

  function finishMetricRequest(metric: string, batchKey: string): void {
    if (scheduledMetricBatchKeys[metric] !== batchKey) return;
    const nextLoading = new Set(loadingMetrics);
    nextLoading.delete(metric);
    loadingMetrics = nextLoading;
    const nextStateKeys = { ...scheduledMetricStateKeys };
    const nextBatchKeys = { ...scheduledMetricBatchKeys };
    delete nextStateKeys[metric];
    delete nextBatchKeys[metric];
    scheduledMetricStateKeys = nextStateKeys;
    scheduledMetricBatchKeys = nextBatchKeys;
  }

  function evictMetricHistory(metric: string, removeViewport: boolean): void {
    chartScheduler.cancel(`comparison:${chartPreferenceIdentity(selectedProject, metric)}`);
    for (const [identity, metrics] of fullRangeBatchMetrics) {
      metrics.delete(metric);
      if (metrics.size === 0) {
        chartScheduler.cancel(identity);
        fullRangeBatchMetrics.delete(identity);
      }
    }
    const nextHistories = { ...comparisonHistories };
    const nextHistoryKeys = { ...historyRequestKeys };
    const nextStateKeys = { ...scheduledMetricStateKeys };
    const nextBatchKeys = { ...scheduledMetricBatchKeys };
    delete nextHistories[metric];
    delete nextHistoryKeys[metric];
    delete nextStateKeys[metric];
    delete nextBatchKeys[metric];
    comparisonHistories = nextHistories;
    historyRequestKeys = nextHistoryKeys;
    scheduledMetricStateKeys = nextStateKeys;
    scheduledMetricBatchKeys = nextBatchKeys;
    if (removeViewport && urlViewportMetric !== metric) {
      const nextViewports = { ...metricViewports };
      delete nextViewports[metric];
      metricViewports = nextViewports;
    }
    const nextLoading = new Set(loadingMetrics);
    nextLoading.delete(metric);
    loadingMetrics = nextLoading;
  }

  function evictMetricsOutsidePage(metrics: ReadonlySet<string>): void {
    for (const metric of new Set([
      ...Object.keys(comparisonHistories),
      ...Object.keys(scheduledMetricStateKeys),
      ...visibleMetrics,
    ])) {
      if (!metrics.has(metric)) evictMetricHistory(metric, true);
    }
    visibleMetrics = new Set([...visibleMetrics].filter((metric) => metrics.has(metric)));
  }

  function queueVisibleMetrics(): void {
    for (const metric of visibleMetrics) queueMetric(metric);
  }

  function comparisonSeries(
    metric: string,
    selectedRuns: readonly Run[],
    keysByRun: Readonly<Record<string, readonly string[]>>,
    histories: Readonly<Record<string, ComparisonChartHistory>>,
    activeLoadingMetrics: ReadonlySet<string>,
  ) {
    const response = histories[metric];
    return selectedRuns.map((run) => {
      const keys = keysByRun[run.id];
      const available = keys === undefined || keys.includes(metric);
      return {
        runId: run.id,
        runName: run.name,
        ...runStyle(run.id),
        available,
        history: response ? comparisonSeriesHistory(response, run.id, metric) : undefined,
        loading: keys === undefined || (available && activeLoadingMetrics.has(metric)),
      };
    });
  }

  function reportSeries(
    panel: ReportPanel,
    metric: string,
    availableRuns: readonly Run[],
    histories: Readonly<Record<string, ChartHistory>>,
    activeLoadingMetrics: ReadonlySet<string>,
  ) {
    const identity = reportMetricIdentity(panel, metric);
    const run = availableRuns.find((candidate) => candidate.id === panel.run_id);
    if (!run) return [];
    return [
      {
        runId: run.id,
        runName: run.name,
        ...runStyle(run.id),
        available: true,
        history: histories[identity],
        loading: activeLoadingMetrics.has(identity),
      },
    ];
  }

  function alignmentForApi(alignment: RunAlignment): "step" | "relative_step" | "elapsed_time" {
    if (alignment === "relative-step") return "relative_step";
    if (alignment === "elapsed-time") return "elapsed_time";
    return "step";
  }

  function changeAlignment(_: string, alignment: RunAlignment): void {
    if (xAlignment === alignment) return;
    xAlignment = alignment;
    comparisonHistories = {};
    historyRequestKeys = {};
    metricViewports = {};
    urlViewportMetric = null;
    chartScheduler.cancelAll();
    scheduledMetricStateKeys = {};
    scheduledMetricBatchKeys = {};
    fullRangeBatchMetrics.clear();
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  function changeMetricMode(mode: MetricSetMode): void {
    if (metricMode === mode) return;
    metricMode = mode;
    metricPageIndex = 0;
    syncComparisonUrl("push");
  }

  async function refreshSelectedRuns(): Promise<void> {
    const controller = projectController;
    const running = currentComparisonRuns().filter((run) => run.state === "running");
    if (!controller || refreshingRuns || running.length === 0) return;
    refreshingRuns = true;
    try {
      const latestRuns = await Promise.all(running.map((run) => getRun(run.id, controller.signal)));
      const stillSelected = new Set(selectedRunIds);
      const revisionsChanged = latestRuns.filter((latest) => {
        const previous = runs.find((run) => run.id === latest.id);
        return stillSelected.has(latest.id) && previous?.metric_revision !== latest.metric_revision;
      });
      runs = runs.map(
        (candidate) => latestRuns.find((latest) => latest.id === candidate.id) ?? candidate,
      );
      if (selectedRun) selectedRun = runs.find((run) => run.id === selectedRun?.id) ?? null;
      if (revisionsChanged.length > 0) {
        const updatedKeys = await Promise.all(
          revisionsChanged.map(
            async (run) => [run.id, await getMetricKeys(run.id, controller.signal)] as const,
          ),
        );
        metricKeysByRun = { ...metricKeysByRun, ...Object.fromEntries(updatedKeys) };
        metricPageIndex = 0;
        queueVisibleMetrics();
      }
      const primary = selectedRun;
      const detailController = runController;
      if (!primary || !detailController) return;
      if (activeRunTab === "summary") alerts = await getAlerts(primary.id, detailController.signal);
      if (activeRunTab === "media") {
        const page = await getRichValuePage(primary.id, undefined, detailController.signal);
        richValues = page.items;
        richValueCursor = page.nextBefore;
      }
      if (activeRunTab === "artifacts") {
        const page = await getRunArtifactPage(primary.id, undefined, detailController.signal);
        artifacts = page.items;
        artifactCursor = page.nextBefore;
      }
      if (activeRunTab === "traces") {
        const page = await getTracePage(
          primary.id,
          traceSearch,
          undefined,
          detailController.signal,
        );
        traces = page.items;
        traceCursor = page.nextBefore;
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    } finally {
      refreshingRuns = false;
    }
  }

  function currentComparisonRuns(): Run[] {
    return selectedRunIds.flatMap((runId) => {
      const run = runs.find((candidate) => candidate.id === runId);
      return run ? [run] : [];
    });
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
    syncComparisonUrl("push");
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
    if (tab === "metrics") return metricCatalog.length;
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
              {@const style = runStyle(run.id)}
              <div
                class="run-list-row"
                class:selected={selectedRunIds.includes(run.id)}
                class:primary={selectedRun?.id === run.id}
                style={`--run-color: ${style.color}`}
              >
                <label class="run-checkbox" aria-label={`Compare ${run.name}`}>
                  <input
                    type="checkbox"
                    checked={selectedRunIds.includes(run.id)}
                    disabled={!selectedRunIds.includes(run.id) &&
                      selectedRunIds.length >= MAX_SELECTED_RUNS}
                    onchange={(event) => void toggleRun(run, event.currentTarget.checked)}
                  />
                  <span class={`run-swatch pattern-${style.pattern}`} aria-hidden="true"></span>
                </label>
                <button
                  class="run-primary-button"
                  class:active={selectedRun?.id === run.id}
                  onclick={() => void chooseRun(run)}
                >
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
              </div>
            {/each}
          </div>
          {#if selectionNotice}
            <p class="selection-notice" role="status">{selectionNotice}</p>
          {/if}
          {#if runs.length > 0}
            <p class="run-limit">{selectedRunIds.length} / {MAX_SELECTED_RUNS} visible</p>
          {/if}
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
                          series={reportSeries(
                            panel,
                            metric,
                            runs,
                            reportHistories,
                            loadingReportMetrics,
                          )}
                          parentViewport={reportViewports[identity]
                            ? {
                                minimum: reportViewports[identity]!.stepMin,
                                maximum: reportViewports[identity]!.stepMax,
                              }
                            : null}
                          xAlignment="step"
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
                <p class="eyebrow">
                  {selectedRun.project} / primary run / {selectedRun.id.slice(0, 8)}
                </p>
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

            <div class="comparison-bar" aria-label="Compared runs">
              <div class="comparison-chips">
                {#each comparisonRuns as run (run.id)}
                  {@const style = runStyle(run.id)}
                  <div
                    class="run-chip"
                    class:primary={selectedRun.id === run.id}
                    style={`--run-color: ${style.color}`}
                  >
                    <span class={`run-swatch pattern-${style.pattern}`} aria-hidden="true"></span>
                    <button
                      class="chip-name"
                      type="button"
                      aria-label={`Use ${run.name} as primary run`}
                      onclick={() => void chooseRun(run)}>{run.name}</button
                    >
                    <button
                      class="chip-action"
                      type="button"
                      aria-label={`Show only ${run.name}`}
                      title={`Show only ${run.name}`}
                      onclick={() => void isolateRun(run)}>◎</button
                    >
                    <button
                      class="chip-action"
                      type="button"
                      aria-label={`Remove ${run.name} from comparison`}
                      title={`Remove ${run.name} from comparison`}
                      onclick={() => void toggleRun(run, false)}>×</button
                    >
                  </div>
                {/each}
              </div>
              <button
                class="clear-comparison"
                type="button"
                onclick={() => void clearRunSelection()}
              >
                Clear
              </button>
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
                  <p class="eyebrow">
                    {comparisonRuns.length} runs · exact buckets · four concurrent queries
                  </p>
                  <h2>Metrics</h2>
                </div>
                <label class="search-control">
                  <Icon name="search" size={15} />
                  <input
                    type="search"
                    aria-label="Search metrics"
                    placeholder="Search metric keys"
                    value={metricSearch}
                    oninput={(event) => {
                      metricSearch = event.currentTarget.value;
                      metricPageIndex = 0;
                      syncComparisonUrl("replace");
                    }}
                  />
                </label>
                <span
                  >{filteredMetricCatalog.length.toLocaleString()} of {metricCatalog.length.toLocaleString()}</span
                >
              </div>
              <div class="comparison-controls">
                <div class="segmented-control" aria-label="Metric availability mode">
                  <button
                    type="button"
                    class:active={metricMode === "union"}
                    aria-pressed={metricMode === "union"}
                    onclick={() => changeMetricMode("union")}>Any run</button
                  >
                  <button
                    type="button"
                    class:active={metricMode === "intersection"}
                    aria-pressed={metricMode === "intersection"}
                    onclick={() => changeMetricMode("intersection")}>All runs</button
                  >
                </div>
                <label class="alignment-control">
                  <span>Align x-axis</span>
                  <select
                    value={xAlignment}
                    onchange={(event) =>
                      changeAlignment("", event.currentTarget.value as RunAlignment)}
                  >
                    <option value="step">Absolute step</option>
                    <option value="relative-step">Relative step</option>
                    <option value="elapsed-time">Elapsed time</option>
                  </select>
                </label>
                <span class="availability-hint">Availability is shown on each chart.</span>
              </div>
              {#if filteredMetricCatalog.length > 0}
                <nav class="metric-pagination" aria-label="Metric chart pages">
                  <button
                    type="button"
                    disabled={metricPageResult.page === 0}
                    onclick={() => (metricPageIndex = metricPageResult.page - 1)}>Previous</button
                  >
                  <span>
                    Page {(metricPageResult.page + 1).toLocaleString()} of {metricPageResult.pageCount.toLocaleString()}
                    · up to {METRIC_CHART_PAGE_SIZE} charts
                  </span>
                  <button
                    type="button"
                    disabled={metricPageResult.page + 1 >= metricPageResult.pageCount}
                    onclick={() => (metricPageIndex = metricPageResult.page + 1)}>Next</button
                  >
                </nav>
                <div class="metric-grid">
                  {#each pagedMetricCatalog as entry (`${selectedProject}:${entry.key}`)}
                    <MetricChart
                      metric={entry.key}
                      identity={chartPreferenceIdentity(selectedProject, entry.key)}
                      title={entry.total === 1
                        ? entry.key
                        : `${entry.key} · ${entry.available}/${entry.total} runs`}
                      series={comparisonSeries(
                        entry.key,
                        comparisonRuns,
                        metricKeysByRun,
                        comparisonHistories,
                        loadingMetrics,
                      )}
                      parentViewport={metricViewports[entry.key]
                        ? {
                            minimum: metricViewports[entry.key]!.stepMin,
                            maximum: metricViewports[entry.key]!.stepMax,
                          }
                        : null}
                      {xAlignment}
                      onalignmentchange={changeAlignment}
                      onvisibilitychange={chartVisibility}
                      onviewportchange={chartViewport}
                    />
                  {/each}
                </div>
              {:else}
                <section class="metric-empty">
                  {selectedRunIds.length === 0
                    ? "Select one or more runs to compare metrics."
                    : loadingMetricKeys.size > 0
                      ? "Loading metric catalogs…"
                      : metricCatalog.length === 0
                        ? "No scalar metrics are available in this mode."
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
            <section class="empty">
              {runs.length === 0
                ? "This project has no runs."
                : "Select one or more runs from the sidebar."}
            </section>
          {/if}
        </section>
      </div>
    {/if}
  </main>
</div>
