<script lang="ts">
  import { onMount } from "svelte";

  import {
    getChartHistory,
    getComparisonChartHistory,
    getDashboardConfig,
    getHealth,
    getProject,
    getProjectMetricCatalogPage,
    getProjectPage,
    getReport,
    getReportPage,
    getRun,
    getRunPage,
    getRunSummariesByIds,
    EpochDeckApiError,
    type ChartHistory,
    type ChartHistoryViewport,
    type ComparisonChartHistory,
    type DashboardConfig,
    type Health,
    type MetricCatalogEntry,
    type Project,
    type Report,
    type ReportPanel,
    type ReportSummary,
    type Run,
    type RunListItem,
  } from "./lib/api";
  import {
    chartViewportKey as viewportKey,
    normalizeChartViewport as normalizedViewport,
  } from "./lib/chart-request";
  import { BoundedRequestScheduler } from "./lib/bounded-request-scheduler";
  import { chartPreferenceIdentity } from "./lib/chart-preferences";
  import {
    MAX_SELECTED_RUNS,
    METRIC_CATALOG_PAGE_SIZE,
    comparisonCacheKey,
    normalizeRunSelection,
    planComparisonBatches,
    readComparisonUrl,
    writeComparisonUrl,
    type ComparisonUrlState,
    type MetricSetMode,
    type RunAlignment,
    type RunStyle,
  } from "./lib/comparison-state";
  import {
    CHART_BUCKET_BUDGET,
    COMPARISON_CACHE_MAX_CELLS,
    COMPARISON_CACHE_MAX_ENTRIES,
    COMPARISON_CACHE_MAX_ESTIMATED_BYTES,
    ChartHistoryCache,
    ComparisonHistoryCache,
  } from "./lib/history-cache";
  import Icon from "./lib/Icon.svelte";
  import { LiveRefreshCoordinator } from "./lib/live-refresh-coordinator";
  import { pushMetricCursor } from "./lib/metric-pagination";
  import NavigationSidebar from "./lib/NavigationSidebar.svelte";
  import ReportDashboard from "./lib/ReportDashboard.svelte";
  import RunArtifactPanel from "./lib/RunArtifactPanel.svelte";
  import RunDocumentPanels from "./lib/RunDocumentPanels.svelte";
  import RunHeaderTabs from "./lib/RunHeaderTabs.svelte";
  import RunMediaPanel from "./lib/RunMediaPanel.svelte";
  import RunMetricsPanel from "./lib/RunMetricsPanel.svelte";
  import RunTracePanel from "./lib/RunTracePanel.svelte";
  import { QueryScheduler } from "./lib/query-scheduler";
  import { appendUniquePage, reasonMessage } from "./lib/resource-state";
  import { retainHeadAndTail, retainRecord } from "./lib/retained-window";
  import {
    DEFAULT_SIDEBAR_WIDTH,
    MIN_SIDEBAR_WIDTH,
    clampSidebarWidth,
    forgetRunStylePreference,
    maximumSidebarWidth,
    readSidebarCollapsed,
    readRunStylePreferences,
    readSidebarWidth,
    rememberRunStylePreference,
    rememberSidebarCollapsed,
    rememberSidebarWidth,
  } from "./lib/sidebar-preferences";
  import {
    mergeCurrentRunListFields,
    retainNewestRunDetail,
    runDocumentIsCurrent,
    runRevisionsAtLeast,
  } from "./lib/run-snapshot";
  import {
    RUN_TABS,
    RunResourceController,
    emptyRunResourceState,
    type PaginatedRunTab,
    type RunResourceContext,
    type RunTab,
  } from "./lib/run-resources";

  const MAX_CONCURRENT_CHART_REQUESTS = 4;
  const MAX_PENDING_CHART_REQUESTS = 256;
  const MAX_CONCURRENT_RUN_DETAILS = 2;
  const MAX_PENDING_RUN_DETAILS = 4;
  const LIVE_STATUS_REFRESH_MS = 2_000;
  const LIVE_CHART_REFRESH_COOLDOWN_MS = 10_000;
  const MAX_LIVE_REFRESH_IDENTITIES = 2;
  const MAX_RETAINED_PROJECTS = 200;
  const MAX_RETAINED_REPORTS = 200;
  const MAX_RETAINED_RUNS = 300;
  const MAX_RETAINED_RUN_DETAILS = 16;
  const historyCache = new ChartHistoryCache();
  const comparisonHistoryCache = new ComparisonHistoryCache({
    maxEntries: COMPARISON_CACHE_MAX_ENTRIES,
    maxCells: COMPARISON_CACHE_MAX_CELLS,
    maxEstimatedBytes: COMPARISON_CACHE_MAX_ESTIMATED_BYTES,
  });
  const chartScheduler = new QueryScheduler(
    MAX_CONCURRENT_CHART_REQUESTS,
    MAX_PENDING_CHART_REQUESTS,
  );
  const liveChartRefresh = new LiveRefreshCoordinator(
    LIVE_CHART_REFRESH_COOLDOWN_MS,
    MAX_LIVE_REFRESH_IDENTITIES,
  );
  const runDetailScheduler = new BoundedRequestScheduler(
    MAX_CONCURRENT_RUN_DETAILS,
    MAX_PENDING_RUN_DETAILS,
  );
  const VALID_RUN_TABS = new Set<RunTab>(RUN_TABS.map((tab) => tab.id));
  const DEFAULT_DASHBOARD_CONFIG: DashboardConfig = {
    logo_url: null,
    accent_color: "#2766ad",
  };
  type ReportSelectionResult = "selected" | "missing" | "failed" | "cancelled";
  type ChartSchedulingPolicy = "abort-active" | "coalesce-pending";

  let health: Health | null = null;
  let dashboardConfig = DEFAULT_DASHBOARD_CONFIG;
  let dashboardLogoFailed = false;
  let projects: Project[] = [];
  let projectCursor: string | null = null;
  let projectWindowTruncated = false;
  let projectSearch = "";
  let loadingMoreProjects = false;
  let projectNavigationError: string | null = null;
  let runs: RunListItem[] = [];
  let navigationRuns: RunListItem[] = [];
  let runCursor: string | null = null;
  let runWindowTruncated = false;
  let runSearch = "";
  let loadingRunNavigation = false;
  let runNavigationError: string | null = null;
  let reports: ReportSummary[] = [];
  let reportCursor: string | null = null;
  let reportWindowTruncated = false;
  let reportSearch = "";
  let loadingMoreReports = false;
  let reportNavigationError: string | null = null;
  let selectedProject = "";
  let selectedRun: Run | null = null;
  let runDetailsById: Record<string, Run> = {};
  let selectedRunIds: string[] = [];
  let hoveredRunId: string | null = null;
  let runStylePreferences = readRunStylePreferences();
  let sidebarViewportWidth = typeof window === "undefined" ? 1440 : window.innerWidth;
  let sidebarWidth = readSidebarWidth(sidebarViewportWidth);
  let sidebarCollapsed = readSidebarCollapsed();
  let sidebarResizing = false;
  let sidebarResizeStart: {
    pointerId: number;
    x: number;
    width: number;
    target: HTMLElement;
  } | null = null;
  let metricCatalog: MetricCatalogEntry[] = [];
  let metricCatalogNextAfter: string | null = null;
  let metricAfter: string | null = null;
  let metricCursorStack: Array<string | null> = [];
  let metricBackHistoryTruncated = false;
  let metricCatalogLoading = false;
  let metricCatalogError: string | null = null;
  let metricMode: MetricSetMode = "union";
  let xAlignment: RunAlignment = "step";
  let selectionNotice: string | null = null;
  let selectedReport: Report | null = null;
  let activeRunTab: RunTab = "metrics";
  let metricSearch = "";
  let traceSearch = "";
  let runResources = emptyRunResourceState();
  let {
    alerts,
    richValues,
    artifacts,
    traces,
    loadedTabs: loadedRunTabs,
    loadingTabs: loadingRunTabs,
    errors: runTabErrors,
    loadingMoreTab,
  } = runResources;
  const runResourceController = new RunResourceController((state) => {
    runResources = state;
  });
  let comparisonHistories: Record<string, ComparisonChartHistory> = {};
  let historyRequestKeys: Record<string, string> = {};
  let metricViewports: Record<string, ChartHistoryViewport | null> = {};
  let urlViewportMetric: string | null = null;
  let loadingMetrics = new Set<string>();
  let metricErrors: Record<string, string> = {};
  let visibleMetrics = new Set<string>();
  let scheduledMetricStateKeys: Record<string, string> = {};
  let scheduledMetricBatchKeys: Record<string, string> = {};
  let instantiatedMetricSignature = "";
  let fullRangeFlushScheduled = false;
  let fullRangeFlushFrame: number | null = null;
  let fullRangeFlushPolicy: ChartSchedulingPolicy = "abort-active";
  const fullRangeBatchMetrics = new Map<string, Set<string>>();
  let reportHistories: Record<string, ChartHistory> = {};
  let reportHistoryRequestKeys: Record<string, string> = {};
  let scheduledReportRequestKeys: Record<string, string> = {};
  let reportViewports: Record<string, ChartHistoryViewport | null> = {};
  let loadingReportMetrics = new Set<string>();
  let reportErrors: Record<string, string> = {};
  let visibleReportMetrics = new Set<string>();
  let refreshingRuns = false;
  let error: string | null = null;
  let refreshError: string | null = null;
  let healthError: string | null = null;
  let loading = true;
  let projectController: AbortController | null = null;
  let runNavigationController: AbortController | null = null;
  let runController: AbortController | null = null;
  let appController: AbortController | null = null;
  let metricCatalogController: AbortController | null = null;
  let metricSearchTimer: number | null = null;

  $: ({
    alerts,
    richValues,
    artifacts,
    traces,
    loadedTabs: loadedRunTabs,
    loadingTabs: loadingRunTabs,
    errors: runTabErrors,
    loadingMoreTab,
  } = runResources);

  $: comparisonRuns = selectedRunIds.flatMap((runId) => {
    const run = runs.find((candidate) => candidate.id === runId);
    return run ? [run] : [];
  });
  $: filteredProjects = projects.filter(
    (project) =>
      project.name === selectedProject ||
      project.name.toLocaleLowerCase().includes(projectSearch.trim().toLocaleLowerCase()),
  );
  $: filteredReports = reports.filter((report) =>
    report.name.toLocaleLowerCase().includes(reportSearch.trim().toLocaleLowerCase()),
  );
  $: pagedMetricSignature = JSON.stringify(metricCatalog.map((entry) => entry.key));
  $: if (pagedMetricSignature !== instantiatedMetricSignature) {
    instantiatedMetricSignature = pagedMetricSignature;
    evictMetricsOutsidePage(new Set(metricCatalog.map((entry) => entry.key)));
  }

  onMount(() => {
    const controller = new AbortController();
    appController = controller;
    const refreshTimer = window.setInterval(refreshSelectedRuns, LIVE_STATUS_REFRESH_MS);
    const handlePopState = () => void restoreFromLocation();
    const handleVisibility = () => {
      if (document.visibilityState === "visible") {
        if (selectedReport) queueVisibleReportMetrics();
        else queueVisibleMetrics();
        void refreshSelectedRuns();
      } else {
        pauseChartRequests();
      }
    };
    const handleWindowResize = () => {
      sidebarViewportWidth = window.innerWidth;
      sidebarWidth = clampSidebarWidth(sidebarWidth, sidebarViewportWidth);
    };
    window.addEventListener("popstate", handlePopState);
    window.addEventListener("resize", handleWindowResize);
    document.addEventListener("visibilitychange", handleVisibility);
    void getDashboardConfig(controller.signal)
      .then((result) => {
        if (!/^#[0-9a-f]{6}$/i.test(result.accent_color)) {
          throw new Error("EpochDeck dashboard config returned an invalid accent color");
        }
        if (
          result.logo_url !== null &&
          (!result.logo_url.startsWith("/") || result.logo_url.startsWith("//"))
        ) {
          throw new Error("EpochDeck dashboard config returned an invalid logo URL");
        }
        dashboardConfig = result;
        dashboardLogoFailed = false;
      })
      .catch((reason) => {
        if (!controller.signal.aborted) showError(reason);
      });
    void getHealth(controller.signal)
      .then((result) => {
        health = result;
        healthError = null;
      })
      .catch((reason) => {
        if (!controller.signal.aborted) healthError = reasonMessage(reason);
      });
    getProjectPage(undefined, controller.signal)
      .then(async (projectPage) => {
        projects = projectPage.items;
        projectCursor = projectPage.nextBefore;
        const restored = readComparisonUrl(
          new URL(window.location.href),
          VALID_RUN_TABS,
          "metrics",
        );
        const project = await resolveProject(restored.project, controller.signal);
        if (project) await chooseProject(project.name, "replace", restored);
      })
      .catch(showError)
      .finally(() => {
        loading = false;
      });
    return () => {
      controller.abort();
      appController = null;
      projectController?.abort();
      runNavigationController?.abort();
      runController?.abort();
      metricCatalogController?.abort();
      if (metricSearchTimer !== null) window.clearTimeout(metricSearchTimer);
      chartScheduler.cancelAll();
      liveChartRefresh.clear();
      cancelFullRangeFlush();
      runDetailScheduler.cancelAll();
      window.removeEventListener("popstate", handlePopState);
      window.removeEventListener("resize", handleWindowResize);
      document.removeEventListener("visibilitychange", handleVisibility);
      window.clearInterval(refreshTimer);
    };
  });

  async function chooseProject(
    name: string,
    historyMode: "push" | "replace" | "none" = "push",
    restored?: ComparisonUrlState<RunTab>,
  ): Promise<void> {
    projectController?.abort();
    runNavigationController?.abort();
    runNavigationController = null;
    metricCatalogController?.abort();
    if (metricSearchTimer !== null) window.clearTimeout(metricSearchTimer);
    metricSearchTimer = null;
    const controller = new AbortController();
    projectController = controller;
    selectedProject = name;
    resetRunSelection();
    runs = [];
    navigationRuns = [];
    reports = [];
    runCursor = null;
    reportCursor = null;
    runWindowTruncated = false;
    reportWindowTruncated = false;
    runSearch = "";
    reportSearch = "";
    runNavigationError = null;
    loadingRunNavigation = false;
    reportNavigationError = null;
    error = null;
    try {
      const [runPage, reportPage] = await Promise.all([
        getRunPage(name, "", undefined, controller.signal),
        getReportPage(name, undefined, controller.signal),
      ]);
      if (controller.signal.aborted || projectController !== controller) return;
      runs = runPage.items;
      navigationRuns = runPage.items;
      runCursor = runPage.nextBefore;
      reports = reportPage.items;
      reportCursor = reportPage.nextBefore;
      const state: ComparisonUrlState<RunTab> = restored ?? {
        project: name,
        reportId: null,
        runIds: [],
        runSelectionSpecified: false,
        primaryRunId: null,
        tab: "metrics",
        metricMode: "union",
        search: "",
        metricAfter: null,
        alignment: "step",
        chartMetric: null,
        chartViewport: null,
      };
      const applied = await applyComparisonState(
        state,
        !state.runSelectionSpecified,
        controller.signal,
      );
      if (
        applied &&
        historyMode !== "none" &&
        !controller.signal.aborted &&
        projectController === controller
      ) {
        syncComparisonUrl(historyMode);
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function loadMoreProjects(): Promise<void> {
    const signal = appController?.signal;
    if (!projectCursor || !signal || loadingMoreProjects) return;
    loadingMoreProjects = true;
    projectNavigationError = null;
    try {
      const page = await getProjectPage(projectCursor, signal);
      const retained = retainHeadAndTail(
        appendUniquePage(projects, page.items, (project) => project.id),
        MAX_RETAINED_PROJECTS,
        (project) => project.id,
        pinnedProjectIds(),
      );
      projects = retained.items;
      projectWindowTruncated ||= retained.truncated;
      projectCursor = page.nextBefore;
    } catch (reason) {
      if (!signal.aborted) projectNavigationError = reasonMessage(reason);
    } finally {
      loadingMoreProjects = false;
    }
  }

  async function searchRuns(): Promise<void> {
    const project = projectController;
    if (!project) return;
    runNavigationController?.abort();
    const controller = new AbortController();
    runNavigationController = controller;
    const signal = AbortSignal.any([project.signal, controller.signal]);
    const search = runSearch;
    loadingRunNavigation = true;
    runNavigationError = null;
    try {
      const page = await getRunPage(selectedProject, search, undefined, signal);
      if (signal.aborted || projectController !== project || runNavigationController !== controller)
        return;
      navigationRuns = page.items;
      runWindowTruncated = false;
      runCursor = page.nextBefore;
      const pinnedIds = new Set([
        ...selectedRunIds,
        ...(selectedRun ? [selectedRun.id] : []),
        ...(selectedReport
          ? selectedReport.layout.panels.flatMap((panel) => (panel.run_id ? [panel.run_id] : []))
          : []),
      ]);
      runs = upsertRuns(
        runs.filter((run) => pinnedIds.has(run.id)),
        page.items,
      );
    } catch (reason) {
      if (!signal.aborted) runNavigationError = reasonMessage(reason);
    } finally {
      if (runNavigationController === controller) {
        runNavigationController = null;
        loadingRunNavigation = false;
      }
    }
  }

  async function loadMoreRuns(): Promise<void> {
    const project = projectController;
    if (!project || !runCursor || loadingRunNavigation) return;
    const controller = new AbortController();
    runNavigationController = controller;
    const signal = AbortSignal.any([project.signal, controller.signal]);
    loadingRunNavigation = true;
    runNavigationError = null;
    try {
      const page = await getRunPage(selectedProject, runSearch, runCursor, signal);
      if (signal.aborted || projectController !== project || runNavigationController !== controller)
        return;
      navigationRuns = retainRuns(
        appendUniquePage(navigationRuns, page.items, (run) => run.id),
        true,
      );
      runs = upsertRuns(runs, page.items);
      runCursor = page.nextBefore;
    } catch (reason) {
      if (!signal.aborted) runNavigationError = reasonMessage(reason);
    } finally {
      if (runNavigationController === controller) {
        runNavigationController = null;
        loadingRunNavigation = false;
      }
    }
  }

  async function loadMoreReports(): Promise<void> {
    const controller = projectController;
    if (!controller || !reportCursor || loadingMoreReports) return;
    loadingMoreReports = true;
    reportNavigationError = null;
    try {
      const page = await getReportPage(selectedProject, reportCursor, controller.signal);
      if (controller.signal.aborted || projectController !== controller) return;
      reports = retainReports(appendUniquePage(reports, page.items, (report) => report.id));
      reportCursor = page.nextBefore;
    } catch (reason) {
      if (!controller.signal.aborted) reportNavigationError = reasonMessage(reason);
    } finally {
      loadingMoreReports = false;
    }
  }

  async function applyComparisonState(
    state: ComparisonUrlState<RunTab>,
    selectDefault: boolean,
    signal: AbortSignal,
  ): Promise<boolean> {
    const requestedRunIds =
      selectDefault && state.runIds.length === 0 && navigationRuns[0]
        ? [navigationRuns[0].id]
        : [...new Set(state.runIds)].slice(0, MAX_SELECTED_RUNS);
    const unavailableRunIds = await ensureKnownRuns(requestedRunIds, signal);
    if (signal.aborted) return false;
    const available = new Set(runs.map((run) => run.id));
    let normalized = normalizeRunSelection(requestedRunIds, available, state.primaryRunId);
    if (
      normalized.runIds.length === 0 &&
      navigationRuns[0] &&
      (requestedRunIds.length > 0 || state.reportId !== null)
    ) {
      normalized = normalizeRunSelection([navigationRuns[0].id], available, navigationRuns[0].id);
    }
    selectedRunIds = normalized.runIds;
    metricMode = state.metricMode;
    metricSearch = state.search;
    xAlignment = state.alignment;
    activeRunTab = state.tab;
    selectionNotice = unavailableRunNotice(unavailableRunIds.size);
    resetChartState(false);
    const requestedReport = state.reportId;
    let reportSelected = false;
    if (requestedReport) {
      const result = await chooseReport(requestedReport, false);
      if (result === "cancelled" || result === "failed") return false;
      reportSelected = result === "selected";
      if (!reportSelected) {
        await activatePrimaryRun(normalized.primaryRunId, true);
        if (signal.aborted) return false;
        selectionNotice = selectedRun
          ? `The requested report is unavailable. Showing ${selectedRun.name}.`
          : "The requested report is unavailable.";
      }
    } else {
      await activatePrimaryRun(normalized.primaryRunId, true);
    }
    metricBackHistoryTruncated = false;
    await loadMetricCatalog(state.metricAfter, [], signal);
    if (signal.aborted) return false;
    if (reportSelected) return true;
    if (
      state.chartMetric &&
      state.chartViewport &&
      metricCatalog.some((entry) => entry.key === state.chartMetric)
    ) {
      const viewport = normalizedViewport(state.chartViewport.minimum, state.chartViewport.maximum);
      if (viewport) {
        urlViewportMetric = state.chartMetric;
        metricViewports = { [state.chartMetric]: viewport };
      }
    }
    queueVisibleMetrics();
    return true;
  }

  async function chooseRun(run: RunListItem): Promise<void> {
    if (!selectedRunIds.includes(run.id)) {
      if (selectedRunIds.length >= MAX_SELECTED_RUNS) {
        selectionNotice = `Up to ${MAX_SELECTED_RUNS} runs can be visible at once.`;
        return;
      }
      selectedRunIds = [...selectedRunIds, run.id];
      await loadMetricCatalog(null, []);
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
    traceSearch = "";
    selectedRun = null;
    error = null;
    runResourceController.reset();
    if (!runId) return;
    const cached = runDetailsById[runId];
    const summary = runs.find((run) => run.id === runId);
    const reuseCached =
      cached !== undefined && runDocumentIsCurrent(cached, summary, activeRunTab === "summary");
    const detail = reuseCached
      ? cached
      : await runDetailScheduler.run({
          identity: `run:${runId}`,
          parentSignal: controller.signal,
          request: (signal) => getRun(runId, signal),
        });
    if (!detail) return;
    if (controller.signal.aborted || runController !== controller) return;
    storeRunDetails([detail], new Set([runId]));
    const retainedDetail = runDetailsById[runId] ?? detail;
    runs = upsertRuns(runs, [runListItem(retainedDetail)]);
    const currentSummary = runs.find((run) => run.id === runId);
    selectedRun = mergeCurrentRunListFields(
      retainedDetail,
      currentSummary,
      activeRunTab === "summary",
    );
    runResourceController.reset(["configuration", "metrics"]);
    if (loadTab) await ensureRunTabLoaded(activeRunTab);
  }

  async function hydrateSelectedRunDocument(signal: AbortSignal): Promise<string | null> {
    const runId = selectedRun?.id;
    if (!runId) return null;
    const summary = runs.find((run) => run.id === runId);
    const cached = runDetailsById[runId];
    if (cached && runDocumentIsCurrent(cached, summary, activeRunTab === "summary")) {
      selectedRun = mergeCurrentRunListFields(cached, summary, activeRunTab === "summary");
      return null;
    }
    try {
      const detail = await runDetailScheduler.run({
        identity: `run:${runId}`,
        parentSignal: signal,
        request: (requestSignal) => getRun(runId, requestSignal),
      });
      if (!detail) return null;
      if (signal.aborted || selectedRun?.id !== runId) return null;
      storeRunDetails([detail], new Set([...pinnedRunIds(), runId]));
      const retainedDetail = runDetailsById[runId] ?? detail;
      runs = upsertRuns(runs, [runListItem(retainedDetail)]);
      navigationRuns = upsertRuns(navigationRuns, [runListItem(retainedDetail)], true);
      const currentSummary = runs.find((run) => run.id === runId);
      selectedRun = mergeCurrentRunListFields(
        retainedDetail,
        currentSummary,
        activeRunTab === "summary",
      );
      return null;
    } catch (reason) {
      return signal.aborted ? null : reasonMessage(reason);
    }
  }

  async function toggleRun(run: RunListItem, selected: boolean): Promise<void> {
    selectionNotice = null;
    if (selected) {
      if (selectedRunIds.includes(run.id)) return;
      if (selectedRunIds.length >= MAX_SELECTED_RUNS) {
        selectionNotice = `Up to ${MAX_SELECTED_RUNS} runs can be visible at once.`;
        return;
      }
      selectedRunIds = [...selectedRunIds, run.id];
      await loadMetricCatalog(null, []);
      if (!selectedRun) await activatePrimaryRun(run.id, true);
    } else {
      selectedRunIds = selectedRunIds.filter((runId) => runId !== run.id);
      await loadMetricCatalog(null, []);
      if (selectedRun?.id === run.id) await activatePrimaryRun(selectedRunIds[0] ?? null, true);
    }
    resetChartState(false);
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  function hoverRun(runId: string | null): void {
    hoveredRunId = runId;
  }

  function updateRunStyle(runId: string, style: RunStyle): void {
    runStylePreferences = rememberRunStylePreference(runStylePreferences, runId, style);
  }

  function resetRunStyle(runId: string): void {
    runStylePreferences = forgetRunStylePreference(runStylePreferences, runId);
  }

  function startSidebarResize(event: PointerEvent): void {
    if (event.button !== 0 || !(event.currentTarget instanceof HTMLElement)) return;
    event.preventDefault();
    sidebarResizeStart = {
      pointerId: event.pointerId,
      x: event.clientX,
      width: sidebarWidth,
      target: event.currentTarget,
    };
    sidebarResizing = true;
    event.currentTarget.setPointerCapture?.(event.pointerId);
  }

  function moveSidebarResize(event: PointerEvent): void {
    const start = sidebarResizeStart;
    if (!start || event.pointerId !== start.pointerId) return;
    sidebarWidth = clampSidebarWidth(start.width + event.clientX - start.x, sidebarViewportWidth);
  }

  function finishSidebarResize(event: PointerEvent): void {
    const start = sidebarResizeStart;
    if (!start || event.pointerId !== start.pointerId) return;
    if (start.target.hasPointerCapture?.(event.pointerId)) {
      start.target.releasePointerCapture(event.pointerId);
    }
    sidebarResizeStart = null;
    sidebarResizing = false;
    sidebarWidth = rememberSidebarWidth(sidebarWidth, sidebarViewportWidth);
  }

  function resizeSidebarWithKeyboard(event: KeyboardEvent): void {
    const increment = event.shiftKey ? 48 : 16;
    let next: number | null = null;
    if (event.key === "ArrowLeft") next = sidebarWidth - increment;
    else if (event.key === "ArrowRight") next = sidebarWidth + increment;
    else if (event.key === "Home") next = MIN_SIDEBAR_WIDTH;
    else if (event.key === "End") next = maximumSidebarWidth(sidebarViewportWidth);
    if (next === null) return;
    event.preventDefault();
    sidebarWidth = rememberSidebarWidth(next, sidebarViewportWidth);
  }

  function resetSidebarWidth(): void {
    sidebarWidth = rememberSidebarWidth(DEFAULT_SIDEBAR_WIDTH, sidebarViewportWidth);
  }

  function toggleSidebarCollapsed(): void {
    sidebarCollapsed = rememberSidebarCollapsed(!sidebarCollapsed);
  }

  async function ensureKnownRuns(
    runIds: readonly string[],
    signal: AbortSignal,
  ): Promise<Set<string>> {
    const known = new Set(runs.map((run) => run.id));
    const missing = [...new Set(runIds)].filter((runId) => runId && !known.has(runId));
    if (missing.length === 0) return new Set();
    let summaries: RunListItem[];
    try {
      summaries = await getRunSummariesByIds(selectedProject, missing, signal);
    } catch (reason) {
      if (!isNotFound(reason)) throw reason;
      if (missing.length === 1) return new Set(missing);
      summaries = [];
      for (const runId of missing) {
        if (signal.aborted) return new Set(missing);
        try {
          summaries.push(...(await getRunSummariesByIds(selectedProject, [runId], signal)));
        } catch (candidateReason) {
          if (!isNotFound(candidateReason)) throw candidateReason;
        }
      }
    }
    if (signal.aborted) return new Set(missing);
    runs = upsertRuns(runs, summaries);
    navigationRuns = upsertRuns(navigationRuns, summaries, true);
    const resolved = new Set(summaries.map((run) => run.id));
    return new Set(missing.filter((runId) => !resolved.has(runId)));
  }

  function unavailableRunNotice(count: number): string | null {
    if (count === 0) return null;
    return count === 1
      ? "One unavailable run was removed from this view."
      : `${count} unavailable runs were removed from this view.`;
  }

  function runListItem(run: Run): RunListItem {
    const {
      config: _config,
      summary: _summary,
      explicit_summary: _explicitSummary,
      metric_summary: _metricSummary,
      ...item
    } = run;
    return item;
  }

  function upsertRuns(
    current: readonly RunListItem[],
    additions: readonly RunListItem[],
    trackNavigation = false,
  ): RunListItem[] {
    const updates = new Map(additions.map((run) => [run.id, run]));
    const merged = current.map((run) => {
      const update = updates.get(run.id);
      return update && runRevisionsAtLeast(update, run) ? update : run;
    });
    const existing = new Set(current.map((run) => run.id));
    return retainRuns(
      [...merged, ...additions.filter((run) => !existing.has(run.id))],
      trackNavigation,
    );
  }

  function pinnedProjectIds(): Set<string> {
    return new Set(
      projects.filter((project) => project.name === selectedProject).map((project) => project.id),
    );
  }

  function pinnedRunIds(): Set<string> {
    return new Set([
      ...selectedRunIds,
      ...(selectedRun ? [selectedRun.id] : []),
      ...(selectedReport ? reportPanelRunIds(selectedReport) : []),
    ]);
  }

  function retainRuns(values: readonly RunListItem[], trackNavigation = false): RunListItem[] {
    const retained = retainHeadAndTail(values, MAX_RETAINED_RUNS, (run) => run.id, pinnedRunIds());
    if (trackNavigation) runWindowTruncated ||= retained.truncated;
    return retained.items;
  }

  function retainReports(values: readonly ReportSummary[]): ReportSummary[] {
    const retained = retainHeadAndTail(
      values,
      MAX_RETAINED_REPORTS,
      (report) => report.id,
      selectedReport ? new Set([selectedReport.id]) : new Set(),
    );
    reportWindowTruncated ||= retained.truncated;
    return retained.items;
  }

  function storeRunDetails(details: readonly Run[], pinned = pinnedRunIds()): void {
    let retained = runDetailsById;
    for (const detail of details) {
      const existing = retained[detail.id];
      const selected = retainNewestRunDetail(existing, detail);
      if (selected === existing) continue;
      retained = retainRecord(retained, detail.id, selected, MAX_RETAINED_RUN_DETAILS, pinned);
    }
    runDetailsById = retained;
  }

  function reportPanelRunIds(report: Report): string[] {
    return [
      ...new Set(report.layout.panels.flatMap((panel) => (panel.run_id ? [panel.run_id] : []))),
    ].slice(0, 32);
  }

  async function loadMetricCatalog(
    requestedAfter: string | null,
    cursorStack: Array<string | null>,
    parentSignal?: AbortSignal,
  ): Promise<string | null> {
    if (metricSearchTimer !== null) window.clearTimeout(metricSearchTimer);
    metricSearchTimer = null;
    metricCatalogController?.abort();
    if (selectedRunIds.length === 0) {
      metricCatalogController = null;
      metricCatalog = [];
      metricCatalogNextAfter = null;
      metricAfter = null;
      metricCursorStack = [];
      metricBackHistoryTruncated = false;
      metricCatalogError = null;
      metricCatalogLoading = false;
      return null;
    }
    const controller = new AbortController();
    metricCatalogController = controller;
    const abortFromParent = () => controller.abort();
    if (parentSignal?.aborted) controller.abort();
    else parentSignal?.addEventListener("abort", abortFromParent, { once: true });
    const project = selectedProject;
    const runIds = [...selectedRunIds];
    const mode = metricMode;
    const search = metricSearch;
    metricCatalogLoading = true;
    metricCatalogError = null;
    try {
      const page = await getProjectMetricCatalogPage(
        project,
        runIds,
        mode,
        search,
        requestedAfter ?? undefined,
        METRIC_CATALOG_PAGE_SIZE,
        controller.signal,
      );
      if (controller.signal.aborted || metricCatalogController !== controller) return null;
      metricCatalog = page.items;
      metricCatalogNextAfter = page.nextAfter;
      metricAfter = requestedAfter;
      metricCursorStack = [...cursorStack];
      if (requestedAfter === null && cursorStack.length === 0) {
        metricBackHistoryTruncated = false;
      }
      return null;
    } catch (reason) {
      if (controller.signal.aborted || metricCatalogController !== controller) return null;
      const message = reasonMessage(reason);
      metricCatalogError = message;
      return message;
    } finally {
      parentSignal?.removeEventListener("abort", abortFromParent);
      if (metricCatalogController === controller) {
        metricCatalogLoading = false;
        metricCatalogController = null;
      }
    }
  }

  function retryMetricCatalog(): void {
    void loadMetricCatalog(metricAfter, metricCursorStack);
  }

  function changeMetricSearch(value: string): void {
    metricSearch = value;
    metricAfter = null;
    metricCursorStack = [];
    metricBackHistoryTruncated = false;
    metricCatalog = [];
    metricCatalogNextAfter = null;
    metricCatalogError = null;
    metricCatalogLoading = selectedRunIds.length > 0;
    if (metricSearchTimer !== null) window.clearTimeout(metricSearchTimer);
    metricSearchTimer = window.setTimeout(() => {
      metricSearchTimer = null;
      void loadMetricCatalog(null, []);
    }, 180);
    syncComparisonUrl("replace");
  }

  async function changeMetricCursor(direction: "previous" | "next"): Promise<void> {
    if (direction === "next") {
      if (!metricCatalogNextAfter) return;
      const previous = pushMetricCursor(metricCursorStack, metricAfter);
      if ((await loadMetricCatalog(metricCatalogNextAfter, previous.history)) !== null) return;
      metricBackHistoryTruncated ||= previous.truncated;
    } else {
      if (metricCursorStack.length === 0) {
        if (!metricAfter || (await loadMetricCatalog(null, [])) !== null) return;
      } else {
        const previous = metricCursorStack.at(-1) ?? null;
        if ((await loadMetricCatalog(previous, metricCursorStack.slice(0, -1))) !== null) return;
      }
    }
    syncComparisonUrl("push");
  }

  async function restoreFromLocation(): Promise<void> {
    const state = readComparisonUrl(new URL(window.location.href), VALID_RUN_TABS, "metrics");
    const signal = appController?.signal;
    if (!signal) return;
    let requestedProject: Project | undefined;
    try {
      requestedProject = await resolveProject(state.project, signal);
    } catch (reason) {
      if (!signal.aborted) showError(reason);
      return;
    }
    if (!requestedProject) return;
    if (requestedProject.name !== selectedProject) {
      await chooseProject(requestedProject.name, "replace", state);
      return;
    }
    const controller = projectController;
    if (!controller) return;
    try {
      const applied = await applyComparisonState(
        state,
        !state.runSelectionSpecified,
        controller.signal,
      );
      if (applied && !controller.signal.aborted && projectController === controller) {
        syncComparisonUrl("replace");
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function resolveProject(
    requestedName: string | null,
    signal: AbortSignal,
  ): Promise<Project | undefined> {
    if (!requestedName) return projects[0];
    const loaded = projects.find((project) => project.name === requestedName);
    if (loaded) return loaded;
    let project: Project;
    try {
      project = await getProject(requestedName, signal);
    } catch (reason) {
      if (isNotFound(reason)) return projects[0];
      throw reason;
    }
    if (signal.aborted) return undefined;
    const retained = retainHeadAndTail(
      appendUniquePage(projects, [project], (candidate) => candidate.id),
      MAX_RETAINED_PROJECTS,
      (candidate) => candidate.id,
      new Set([...pinnedProjectIds(), project.id]),
    );
    projects = retained.items;
    projectWindowTruncated ||= retained.truncated;
    return project;
  }

  function syncComparisonUrl(mode: "push" | "replace"): void {
    const state: ComparisonUrlState<RunTab> = {
      project: selectedProject || null,
      reportId: selectedReport?.id ?? null,
      runIds: selectedRunIds,
      runSelectionSpecified: true,
      primaryRunId: selectedRun?.id ?? null,
      tab: activeRunTab,
      metricMode,
      search: metricSearch,
      metricAfter,
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
    runDetailScheduler.cancelAll();
    selectedRun = null;
    selectedRunIds = [];
    hoveredRunId = null;
    runDetailsById = {};
    selectedReport = null;
    metricCatalogController?.abort();
    metricCatalog = [];
    metricCatalogNextAfter = null;
    metricAfter = null;
    metricCursorStack = [];
    metricBackHistoryTruncated = false;
    metricCatalogLoading = false;
    metricCatalogError = null;
    traceSearch = "";
    activeRunTab = "metrics";
    metricSearch = "";
    runResourceController.reset();
    resetChartState();
    resetReportState();
  }

  function resetChartState(resetVisibility = true): void {
    liveChartRefresh.forget("comparison");
    chartScheduler.cancelAll();
    comparisonHistories = {};
    historyRequestKeys = {};
    metricViewports = {};
    urlViewportMetric = null;
    loadingMetrics = new Set();
    metricErrors = {};
    scheduledMetricStateKeys = {};
    scheduledMetricBatchKeys = {};
    cancelFullRangeFlush();
    fullRangeBatchMetrics.clear();
    if (resetVisibility) visibleMetrics = new Set();
  }

  function pauseChartRequests(): void {
    liveChartRefresh.clear();
    chartScheduler.cancelAll();
    loadingMetrics = new Set();
    loadingReportMetrics = new Set();
    scheduledReportRequestKeys = {};
    scheduledMetricStateKeys = {};
    scheduledMetricBatchKeys = {};
    cancelFullRangeFlush();
    fullRangeBatchMetrics.clear();
  }

  function resetReportState(): void {
    liveChartRefresh.forget("report");
    reportHistories = {};
    reportHistoryRequestKeys = {};
    scheduledReportRequestKeys = {};
    reportViewports = {};
    loadingReportMetrics = new Set();
    reportErrors = {};
    visibleReportMetrics = new Set();
  }

  async function chooseReport(
    report: ReportSummary | string,
    updateHistory = true,
  ): Promise<ReportSelectionResult> {
    runController?.abort();
    runDetailScheduler.cancelAll();
    const controller = new AbortController();
    runController = controller;
    selectedRun = null;
    selectedReport = null;
    traceSearch = "";
    runResourceController.reset();
    liveChartRefresh.clear();
    chartScheduler.cancelAll();
    resetReportState();
    error = null;
    try {
      const reportId = typeof report === "string" ? report : report.id;
      const detail = await getReport(reportId, controller.signal);
      if (controller.signal.aborted || runController !== controller) return "cancelled";
      if (detail.project !== selectedProject) return "missing";
      selectedReport = detail;
      reports = retainReports(
        appendUniquePage(reports, [reportSummary(detail)], (candidate) => candidate.id),
      );
      const reportRunIds = reportPanelRunIds(detail);
      if (reportRunIds.length > 0) {
        const summaries = await getRunSummariesByIds(
          selectedProject,
          reportRunIds,
          controller.signal,
        );
        if (controller.signal.aborted || runController !== controller) return "cancelled";
        runs = upsertRuns(runs, summaries);
      }
      if (updateHistory) syncComparisonUrl("push");
      return "selected";
    } catch (reason) {
      if (controller.signal.aborted || runController !== controller) return "cancelled";
      if (isNotFound(reason)) {
        if (updateHistory) showError(reason);
        return "missing";
      }
      showError(reason);
      return "failed";
    }
  }

  function reportSummary(report: Report): ReportSummary {
    const { id, project_id, project, name, created_at, updated_at } = report;
    return { id, project_id, project, name, created_at, updated_at };
  }

  function reportChartVisibility(panel: ReportPanel, metric: string, visible: boolean): void {
    const runId = panel.run_id;
    if (!runId) return;
    const identity = `${panel.id}:${runId}:${metric}`;
    const nextVisible = new Set(visibleReportMetrics);
    if (visible) nextVisible.add(identity);
    else nextVisible.delete(identity);
    visibleReportMetrics = nextVisible;
    if (!visible) {
      chartScheduler.cancel(`report:${identity}`);
      evictReportMetric(identity);
      return;
    }
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

  function queueReportMetric(
    request: { identity: string; runId: string; metric: string },
    schedulingPolicy: ChartSchedulingPolicy = "abort-active",
  ): void {
    const report = selectedReport;
    if (!report || !visibleReportMetrics.has(request.identity) || pageIsHidden()) return;
    const viewport = reportViewports[request.identity] ?? null;
    const revision = runs.find((run) => run.id === request.runId)?.metric_revision ?? 0;
    const requestKey = `${CHART_BUCKET_BUDGET}:${revision}:${viewportKey(viewport)}`;
    if (reportHistoryRequestKeys[request.identity] === requestKey) return;
    const nextErrors = { ...reportErrors };
    delete nextErrors[request.identity];
    reportErrors = nextErrors;
    loadingReportMetrics = new Set([...loadingReportMetrics, request.identity]);
    scheduledReportRequestKeys = {
      ...scheduledReportRequestKeys,
      [request.identity]: requestKey,
    };
    chartScheduler.schedule({
      identity: `report:${request.identity}`,
      requestKey,
      schedulingPolicy,
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
        if (scheduledReportRequestKeys[request.identity] !== publishedKey) return;
        if (reportRequestKey(request) !== publishedKey) return;
        reportHistories = { ...reportHistories, [request.identity]: history };
        reportHistoryRequestKeys = {
          ...reportHistoryRequestKeys,
          [request.identity]: publishedKey,
        };
        finishReportLoading(request.identity);
      },
      reject: (reason) => {
        if (scheduledReportRequestKeys[request.identity] !== requestKey) return;
        finishReportLoading(request.identity);
        reportErrors = { ...reportErrors, [request.identity]: reasonMessage(reason) };
      },
      discard: () => {
        if (scheduledReportRequestKeys[request.identity] === requestKey) {
          finishReportLoading(request.identity);
        }
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
    const requests = { ...scheduledReportRequestKeys };
    delete requests[identity];
    scheduledReportRequestKeys = requests;
  }

  function evictReportMetric(identity: string): void {
    const histories = { ...reportHistories };
    const requests = { ...reportHistoryRequestKeys };
    const viewports = { ...reportViewports };
    const errors = { ...reportErrors };
    const scheduled = { ...scheduledReportRequestKeys };
    delete histories[identity];
    delete requests[identity];
    delete viewports[identity];
    delete errors[identity];
    delete scheduled[identity];
    reportHistories = histories;
    reportHistoryRequestKeys = requests;
    reportViewports = viewports;
    reportErrors = errors;
    scheduledReportRequestKeys = scheduled;
    finishReportLoading(identity);
  }

  function reportMetricIdentity(panel: ReportPanel, metric: string): string {
    return `${panel.id}:${panel.run_id ?? ""}:${metric}`;
  }

  function queueVisibleReportMetrics(
    schedulingPolicy: ChartSchedulingPolicy = "abort-active",
  ): void {
    const report = selectedReport;
    if (!report) return;
    for (const panel of report.layout.panels) {
      if (!panel.run_id) continue;
      for (const metric of panel.metric_keys) {
        const identity = reportMetricIdentity(panel, metric);
        if (visibleReportMetrics.has(identity)) {
          queueReportMetric({ identity, runId: panel.run_id, metric }, schedulingPolicy);
        }
      }
    }
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

  function queueMetric(
    metric: string,
    schedulingPolicy: ChartSchedulingPolicy = "abort-active",
  ): void {
    if (pageIsHidden()) return;
    const viewport = metricViewports[metric] ?? null;
    if (!viewport) {
      scheduleFullRangeFlush(schedulingPolicy);
      return;
    }
    const candidate = comparisonCandidate(metric);
    const plan = candidate ? planComparisonBatches([candidate], CHART_BUCKET_BUDGET)[0] : undefined;
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
      schedulingPolicy,
    );
  }

  function scheduleFullRangeFlush(schedulingPolicy: ChartSchedulingPolicy = "abort-active"): void {
    if (schedulingPolicy === "abort-active") fullRangeFlushPolicy = schedulingPolicy;
    if (fullRangeFlushScheduled) return;
    fullRangeFlushPolicy = schedulingPolicy;
    fullRangeFlushScheduled = true;
    fullRangeFlushFrame = window.requestAnimationFrame(() => {
      fullRangeFlushFrame = null;
      if (!fullRangeFlushScheduled) return;
      fullRangeFlushScheduled = false;
      const policy = fullRangeFlushPolicy;
      fullRangeFlushPolicy = "abort-active";
      flushFullRangeMetrics(policy);
    });
  }

  function cancelFullRangeFlush(): void {
    fullRangeFlushScheduled = false;
    fullRangeFlushPolicy = "abort-active";
    if (fullRangeFlushFrame === null) return;
    window.cancelAnimationFrame(fullRangeFlushFrame);
    fullRangeFlushFrame = null;
  }

  function flushFullRangeMetrics(schedulingPolicy: ChartSchedulingPolicy): void {
    if (pageIsHidden()) return;
    for (const plan of deterministicComparisonPlans(visibleMetrics)) {
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
      scheduleComparisonPlan(plan, null, identity, trackedMetrics, schedulingPolicy);
    }
  }

  function deterministicComparisonPlans(metrics: ReadonlySet<string>) {
    const candidates = metricCatalog.flatMap(({ key }) => {
      if (!metrics.has(key)) return [];
      const candidate = comparisonCandidate(key);
      return candidate ? [candidate] : [];
    });
    return planComparisonBatches(candidates, CHART_BUCKET_BUDGET);
  }

  function scheduleComparisonPlan(
    plan: ReturnType<typeof planComparisonBatches>[number],
    viewport: ChartHistoryViewport | null,
    identity: string,
    trackedMetrics = new Set(plan.candidates.map((candidate) => candidate.metric)),
    schedulingPolicy: ChartSchedulingPolicy = "abort-active",
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
    const nextErrors = { ...metricErrors };
    for (const metric of trackedMetrics) delete nextErrors[metric];
    metricErrors = nextErrors;
    scheduledMetricStateKeys = nextStateKeys;
    scheduledMetricBatchKeys = nextBatchKeys;
    chartScheduler.schedule({
      identity,
      requestKey,
      schedulingPolicy,
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
          if (!visibleMetrics.has(metric)) continue;
          if (scheduledMetricBatchKeys[metric] !== publishedKey) continue;
          if (viewportKey(metricViewports[metric] ?? null) === viewportKey(viewport)) {
            nextHistories[metric] = comparisonResponseForMetric(response, metric);
            nextHistoryKeys[metric] = stateKeys[metric];
          }
          finishMetricRequest(metric, publishedKey);
        }
        comparisonHistories = nextHistories;
        historyRequestKeys = nextHistoryKeys;
        finishFullRangeBatch(identity);
      },
      reject: (reason) => {
        const message = reasonMessage(reason);
        const nextErrors = { ...metricErrors };
        for (const { metric } of plan.candidates) {
          if (scheduledMetricBatchKeys[metric] !== requestKey) continue;
          finishMetricRequest(metric, requestKey);
          if (visibleMetrics.has(metric)) nextErrors[metric] = message;
        }
        metricErrors = nextErrors;
        finishFullRangeBatch(identity);
      },
      discard: () => {
        for (const { metric } of plan.candidates) finishMetricRequest(metric, requestKey);
        finishFullRangeBatch(identity);
      },
    });
    if (!viewport) {
      fullRangeBatchMetrics.set(
        identity,
        new Set([...(fullRangeBatchMetrics.get(identity) ?? []), ...trackedMetrics]),
      );
    }
  }

  function finishFullRangeBatch(identity: string): void {
    const metrics = fullRangeBatchMetrics.get(identity);
    if (!metrics) return;
    if ([...metrics].some((metric) => scheduledMetricBatchKeys[metric] !== undefined)) return;
    fullRangeBatchMetrics.delete(identity);
  }

  function comparisonCandidate(metric: string): { metric: string; runIds: string[] } | null {
    const runIds = metricCatalog.find((entry) => entry.key === metric)?.run_ids ?? [];
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
    const nextErrors = { ...metricErrors };
    delete nextHistories[metric];
    delete nextHistoryKeys[metric];
    delete nextStateKeys[metric];
    delete nextBatchKeys[metric];
    delete nextErrors[metric];
    comparisonHistories = nextHistories;
    historyRequestKeys = nextHistoryKeys;
    scheduledMetricStateKeys = nextStateKeys;
    scheduledMetricBatchKeys = nextBatchKeys;
    metricErrors = nextErrors;
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

  function queueVisibleMetrics(schedulingPolicy: ChartSchedulingPolicy = "abort-active"): void {
    for (const metric of visibleMetrics) queueMetric(metric, schedulingPolicy);
  }

  function scheduleLiveComparisonRefresh(finalRefresh: boolean): void {
    const project = selectedProject;
    liveChartRefresh.invalidate(
      "comparison",
      () => void refreshLiveComparisonCharts(project),
      finalRefresh,
    );
  }

  async function refreshLiveComparisonCharts(project: string): Promise<void> {
    const controller = projectController;
    if (
      !controller ||
      controller.signal.aborted ||
      project !== selectedProject ||
      selectedReport ||
      pageIsHidden()
    ) {
      return;
    }
    queueVisibleMetrics("coalesce-pending");
    if (metricCatalogLoading) return;
    try {
      const catalogFailure = await loadMetricCatalog(
        metricAfter,
        metricCursorStack,
        controller.signal,
      );
      if (
        controller.signal.aborted ||
        projectController !== controller ||
        project !== selectedProject ||
        selectedReport
      ) {
        return;
      }
      if (catalogFailure) refreshError = `Metric catalog refresh: ${catalogFailure}`;
      queueVisibleMetrics("coalesce-pending");
    } catch (reason) {
      if (!controller.signal.aborted && projectController === controller) {
        refreshError = `Metric catalog refresh: ${reasonMessage(reason)}`;
      }
    }
  }

  function scheduleLiveReportRefresh(finalRefresh: boolean): void {
    const reportId = selectedReport?.id;
    if (!reportId) return;
    liveChartRefresh.invalidate(
      "report",
      () => {
        if (selectedReport?.id === reportId && !pageIsHidden()) {
          queueVisibleReportMetrics("coalesce-pending");
        }
      },
      finalRefresh,
    );
  }

  function retryMetric(metric: string): void {
    const nextErrors = { ...metricErrors };
    delete nextErrors[metric];
    metricErrors = nextErrors;
    const nextRequests = { ...historyRequestKeys };
    delete nextRequests[metric];
    historyRequestKeys = nextRequests;
    queueMetric(metric);
  }

  function alignmentForApi(alignment: RunAlignment): "step" | "relative_step" | "elapsed_time" {
    if (alignment === "relative-step") return "relative_step";
    if (alignment === "elapsed-time") return "elapsed_time";
    return "step";
  }

  function changeAlignment(_: string, alignment: RunAlignment): void {
    if (xAlignment === alignment) return;
    liveChartRefresh.forget("comparison");
    xAlignment = alignment;
    comparisonHistories = {};
    historyRequestKeys = {};
    metricViewports = {};
    urlViewportMetric = null;
    chartScheduler.cancelAll();
    scheduledMetricStateKeys = {};
    scheduledMetricBatchKeys = {};
    cancelFullRangeFlush();
    fullRangeBatchMetrics.clear();
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  async function changeMetricMode(mode: MetricSetMode): Promise<void> {
    if (metricMode === mode) return;
    liveChartRefresh.forget("comparison");
    metricMode = mode;
    metricAfter = null;
    metricCursorStack = [];
    metricBackHistoryTruncated = false;
    metricCatalog = [];
    metricCatalogNextAfter = null;
    await loadMetricCatalog(null, []);
    queueVisibleMetrics();
    syncComparisonUrl("push");
  }

  async function refreshSelectedRuns(): Promise<void> {
    const controller = projectController;
    const running = currentPollingRuns().filter((run) => run.state === "running");
    if (
      !controller ||
      refreshingRuns ||
      running.length === 0 ||
      document.visibilityState !== "visible"
    ) {
      return;
    }
    refreshingRuns = true;
    try {
      const previousRuns = new Map(runs.map((run) => [run.id, run]));
      const primaryRunId = selectedRun?.id ?? null;
      const latestRuns = await getRunSummariesByIds(
        selectedProject,
        running.map((run) => run.id),
        controller.signal,
      );
      if (controller.signal.aborted || projectController !== controller) return;
      refreshError = null;
      if (latestRuns.length === 0) return;
      const stillSelected = new Set(selectedRunIds);
      const revisionsChanged = latestRuns.filter((latest) => {
        const previous = previousRuns.get(latest.id);
        return (
          stillSelected.has(latest.id) && latest.metric_revision > (previous?.metric_revision ?? -1)
        );
      });
      const primaryLatest = latestRuns.find((run) => run.id === primaryRunId);
      const primaryPrevious = primaryRunId ? previousRuns.get(primaryRunId) : undefined;
      const richDataChanged =
        primaryLatest !== undefined &&
        primaryLatest.rich_data_revision > (primaryPrevious?.rich_data_revision ?? -1);
      const anyMetricRevisionChanged = latestRuns.some((latest) => {
        const previous = previousRuns.get(latest.id);
        return latest.metric_revision > (previous?.metric_revision ?? -1);
      });
      const finishedRunIds = new Set(
        latestRuns.flatMap((latest) => {
          const previous = previousRuns.get(latest.id);
          return previous?.state === "running" && latest.state === "finished" ? [latest.id] : [];
        }),
      );
      const comparisonFinished = [...finishedRunIds].some((runId) => stillSelected.has(runId));
      const reportFinished = selectedReport !== null && finishedRunIds.size > 0;
      runs = upsertRuns(runs, latestRuns);
      navigationRuns = upsertRuns(navigationRuns, latestRuns, true);
      const primaryCurrent = runs.find((run) => run.id === primaryRunId);
      if (selectedRun && primaryCurrent) {
        selectedRun = { ...selectedRun, ...primaryCurrent };
        const cached = runDetailsById[primaryCurrent.id];
        if (
          (activeRunTab === "summary" || activeRunTab === "configuration") &&
          (!cached || !runDocumentIsCurrent(cached, primaryCurrent, activeRunTab === "summary"))
        ) {
          const documentFailure = await hydrateSelectedRunDocument(controller.signal);
          if (documentFailure) refreshError = `Run detail refresh: ${documentFailure}`;
        }
      }
      if (!selectedReport && (revisionsChanged.length > 0 || comparisonFinished)) {
        scheduleLiveComparisonRefresh(comparisonFinished);
      }
      if (selectedReport && (anyMetricRevisionChanged || reportFinished)) {
        scheduleLiveReportRefresh(reportFinished);
      }
      const resourceContext = activeResourceContext();
      if (!resourceContext || !richDataChanged) return;
      const resourceFailure = await runResourceController.applyResourceRevision(
        activeRunTab,
        resourceContext,
      );
      if (resourceFailure) refreshError = `Live refresh: ${resourceFailure}`;
    } catch (reason) {
      if (!controller.signal.aborted) {
        refreshError = `Live refresh: ${reasonMessage(reason)}`;
      }
    } finally {
      refreshingRuns = false;
    }
  }

  function currentComparisonRuns(): RunListItem[] {
    return selectedRunIds.flatMap((runId) => {
      const run = runs.find((candidate) => candidate.id === runId);
      return run ? [run] : [];
    });
  }

  function currentPollingRuns(): RunListItem[] {
    const ids = selectedReport ? reportPanelRunIds(selectedReport) : selectedRunIds;
    const selected = new Set(ids.slice(0, 32));
    return runs.filter((run) => selected.has(run.id));
  }

  function pageIsHidden(): boolean {
    return typeof document !== "undefined" && document.visibilityState === "hidden";
  }

  function showError(reason: unknown): void {
    error = reasonMessage(reason);
  }

  function isNotFound(reason: unknown): reason is EpochDeckApiError {
    return reason instanceof EpochDeckApiError && reason.status === 404;
  }

  function activeResourceContext(): RunResourceContext | null {
    if (!selectedRun || !runController) return null;
    return { runId: selectedRun.id, signal: runController.signal, traceSearch };
  }

  function retryRunTab(tab: RunTab): void {
    const context = activeResourceContext();
    if (context) void runResourceController.retry(tab, context);
  }

  function searchTraces(): void {
    const context = activeResourceContext();
    if (context) void runResourceController.searchTraces(context);
  }

  async function selectRunTab(tab: RunTab): Promise<void> {
    activeRunTab = tab;
    if ((tab === "summary" || tab === "configuration") && runController) {
      const documentFailure = await hydrateSelectedRunDocument(runController.signal);
      if (documentFailure) error = `Run detail: ${documentFailure}`;
    }
    await ensureRunTabLoaded(tab);
    syncComparisonUrl("push");
  }

  function ensureRunTabLoaded(tab: RunTab): Promise<void> {
    const context = activeResourceContext();
    return context ? runResourceController.ensureLoaded(tab, context) : Promise.resolve();
  }

  function loadMore(tab: PaginatedRunTab): Promise<void> {
    const context = activeResourceContext();
    return context ? runResourceController.loadMore(tab, context) : Promise.resolve();
  }

  function selectRichKey(key: string): void {
    const context = activeResourceContext();
    if (context) void runResourceController.selectRichKey(key, context);
  }

  function loadMoreRichKeys(): void {
    const context = activeResourceContext();
    if (context) void runResourceController.loadMoreRichKeys(context);
  }

  function loadRichDetail(valueId: string): void {
    const context = activeResourceContext();
    if (context) void runResourceController.loadRichDetail(valueId, context);
  }

  function loadArtifactDetail(artifactId: string): void {
    const context = activeResourceContext();
    if (context) void runResourceController.loadArtifactDetail(artifactId, context);
  }

  function loadTraceDetail(spanId: string): void {
    const context = activeResourceContext();
    if (context) void runResourceController.loadTraceDetail(spanId, context);
  }

  function runTabCount(tab: RunTab): number {
    if (tab === "summary") return Object.keys(selectedRun?.summary ?? {}).length;
    if (tab === "configuration") return Object.keys(selectedRun?.config ?? {}).length;
    if (tab === "metrics") return metricCatalog.length;
    if (tab === "media") {
      return runResources.richKeys.reduce((total, key) => total + key.count, 0);
    }
    if (tab === "traces") return traces.length;
    return artifacts.length;
  }

  function runTabCountLabel(tab: RunTab): string {
    if (loadingRunTabs.has(tab) || (tab === "metrics" && metricCatalogLoading)) return "…";
    if (["media", "traces", "artifacts"].includes(tab) && !loadedRunTabs.has(tab)) return "—";
    const hasMore =
      (tab === "media" && Boolean(runResources.richKeyCursor || runResources.truncatedRichKeys)) ||
      (tab === "traces" &&
        Boolean(runResources.traceCursor || runResources.truncatedTabs.has("traces"))) ||
      (tab === "artifacts" &&
        Boolean(runResources.artifactCursor || runResources.truncatedTabs.has("artifacts"))) ||
      (tab === "metrics" && Boolean(metricAfter || metricCatalogNextAfter));
    return `${runTabCount(tab).toLocaleString()}${hasMore ? "+" : ""}`;
  }
</script>

<svelte:head>
  <title>EpochDeck</title>
  <meta
    name="description"
    content="EpochDeck is a lossless, self-hosted experiment tracker built for large histories."
  />
  {#if dashboardConfig.logo_url && !dashboardLogoFailed}
    <link rel="icon" href={dashboardConfig.logo_url} />
  {/if}
</svelte:head>

<div
  class="app-shell"
  style={`--configured-accent: ${dashboardConfig.accent_color}; --series-accent: ${dashboardConfig.accent_color}`}
>
  <header>
    <div class="brand">
      <h1>
        {#if dashboardConfig.logo_url && !dashboardLogoFailed}
          <img
            src={dashboardConfig.logo_url}
            alt="EpochDeck"
            onerror={() => (dashboardLogoFailed = true)}
          />
        {:else}
          EpochDeck
        {/if}
      </h1>
    </div>
    <div class="status" class:failed={Boolean(error || refreshError || healthError)}>
      <span class="status-dot" aria-hidden="true"></span>
      {health
        ? `${health.status} · v${health.version}`
        : healthError
          ? "status unavailable"
          : "connecting"}
    </div>
  </header>

  <main class="content">
    {#if error}
      <div class="error" role="alert">
        <span>{error}</span>
        <button type="button" aria-label="Dismiss error" onclick={() => (error = null)}>
          <Icon name="close" size={15} />
        </button>
      </div>
    {/if}
    {#if refreshError}
      <div class="error refresh-error" role="status">
        <span>{refreshError}</span>
        <button
          type="button"
          aria-label="Dismiss live refresh error"
          onclick={() => (refreshError = null)}
        >
          <Icon name="close" size={15} />
        </button>
      </div>
    {/if}

    {#if loading}
      <section class="empty">Loading EpochDeck…</section>
    {:else if projects.length === 0}
      <section class="empty">
        <p class="eyebrow">Ready for a first run</p>
        <h1>No experiments yet.</h1>
        <code>import epochdeck as ed; ed.init(project="my-project")</code>
      </section>
    {:else}
      <div
        class="workspace"
        class:resizing-sidebar={sidebarResizing}
        class:sidebar-collapsed={sidebarCollapsed}
        style={`--sidebar-width: ${sidebarWidth}px`}
      >
        <NavigationSidebar
          visibleProjects={filteredProjects}
          {selectedProject}
          bind:projectSearch
          {projectCursor}
          {projectWindowTruncated}
          {loadingMoreProjects}
          projectError={projectNavigationError}
          {reports}
          visibleReports={filteredReports}
          selectedReportId={selectedReport?.id ?? null}
          bind:reportSearch
          {reportCursor}
          {reportWindowTruncated}
          {loadingMoreReports}
          reportError={reportNavigationError}
          runs={navigationRuns}
          {selectedRunIds}
          {runStylePreferences}
          primaryRunId={selectedRun?.id ?? null}
          collapsed={sidebarCollapsed}
          bind:runSearch
          {runCursor}
          {runWindowTruncated}
          loadingRuns={loadingRunNavigation}
          runError={runNavigationError}
          {selectionNotice}
          onchooseproject={(project) => void chooseProject(project)}
          onloadprojects={() => void loadMoreProjects()}
          onchoosereport={(report) => void chooseReport(report)}
          onloadreports={() => void loadMoreReports()}
          onsearchruns={() => void searchRuns()}
          onloadruns={() => void loadMoreRuns()}
          ontogglerun={(run, selected) => void toggleRun(run, selected)}
          onchooserun={(run) => void chooseRun(run)}
          onhoverrun={hoverRun}
          onrunstylechange={updateRunStyle}
          onresetrunstyle={resetRunStyle}
          ontogglecollapsed={toggleSidebarCollapsed}
        />

        {#if !sidebarCollapsed}
          <button
            type="button"
            class="sidebar-resizer"
            class:active={sidebarResizing}
            aria-label={`Resize run sidebar, ${sidebarWidth} pixels`}
            title="Drag to resize the run sidebar. Double-click to reset."
            onpointerdown={startSidebarResize}
            onpointermove={moveSidebarResize}
            onpointerup={finishSidebarResize}
            onpointercancel={finishSidebarResize}
            onlostpointercapture={finishSidebarResize}
            onkeydown={resizeSidebarWithKeyboard}
            ondblclick={resetSidebarWidth}
          ></button>
        {/if}

        <section class="run-view">
          {#if selectedReport}
            <ReportDashboard
              report={selectedReport}
              {runs}
              {runStylePreferences}
              highlightedRunId={hoveredRunId}
              histories={reportHistories}
              viewports={reportViewports}
              loadingMetrics={loadingReportMetrics}
              errors={reportErrors}
              onretry={(panel, metric) =>
                queueReportMetric({
                  identity: reportMetricIdentity(panel, metric),
                  runId: panel.run_id!,
                  metric,
                })}
              onvisibilitychange={reportChartVisibility}
              onviewportchange={reportChartViewport}
            />
          {:else if selectedRun}
            <RunHeaderTabs
              run={selectedRun}
              activeTab={activeRunTab}
              countLabel={runTabCountLabel}
              onselect={selectRunTab}
            />

            <RunDocumentPanels
              run={selectedRun}
              activeTab={activeRunTab}
              {alerts}
              alertCursor={runResources.alertCursor}
              alertsTruncated={runResources.truncatedTabs.has("summary")}
              alertError={runTabErrors.summary}
              {loadingMoreTab}
              onretryalerts={() => retryRunTab("summary")}
              onloadalerts={() => void loadMore("summary")}
            />

            <RunMetricsPanel
              active={activeRunTab === "metrics"}
              project={selectedProject}
              runs={comparisonRuns}
              {runStylePreferences}
              highlightedRunId={hoveredRunId}
              selectedRunCount={selectedRunIds.length}
              catalog={metricCatalog}
              catalogLoading={metricCatalogLoading}
              catalogError={metricCatalogError}
              search={metricSearch}
              mode={metricMode}
              alignment={xAlignment}
              after={metricAfter}
              nextAfter={metricCatalogNextAfter}
              cursorDepth={metricCursorStack.length}
              backHistoryTruncated={metricBackHistoryTruncated}
              histories={comparisonHistories}
              viewports={metricViewports}
              {loadingMetrics}
              errors={metricErrors}
              onsearch={changeMetricSearch}
              onmodechange={(mode) => void changeMetricMode(mode)}
              onalignmentchange={(alignment) => changeAlignment("", alignment)}
              onretrycatalog={retryMetricCatalog}
              oncursor={(direction) => void changeMetricCursor(direction)}
              onretrymetric={retryMetric}
              onvisibilitychange={chartVisibility}
              onviewportchange={chartViewport}
            />

            <RunMediaPanel
              active={activeRunTab === "media"}
              state={runResources}
              error={runTabErrors.media}
              loading={loadingRunTabs.has("media")}
              {loadingMoreTab}
              onretry={() => retryRunTab("media")}
              onselectkey={selectRichKey}
              onloadkeys={loadMoreRichKeys}
              onselectdetail={loadRichDetail}
              onloadmore={() => void loadMore("media")}
            />

            <RunTracePanel
              active={activeRunTab === "traces"}
              state={runResources}
              bind:search={traceSearch}
              error={runTabErrors.traces}
              loading={loadingRunTabs.has("traces")}
              {loadingMoreTab}
              onsearch={searchTraces}
              onretry={() => retryRunTab("traces")}
              onselectdetail={loadTraceDetail}
              onloadmore={() => void loadMore("traces")}
            />

            <RunArtifactPanel
              active={activeRunTab === "artifacts"}
              state={runResources}
              error={runTabErrors.artifacts}
              loading={loadingRunTabs.has("artifacts")}
              {loadingMoreTab}
              onretry={() => retryRunTab("artifacts")}
              onselectdetail={loadArtifactDetail}
              onloadmore={() => void loadMore("artifacts")}
            />
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
