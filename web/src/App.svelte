<script lang="ts">
  import { onMount } from "svelte";

  import {
    artifactFileUrl,
    blobUrl,
    getAlerts,
    getHealth,
    getHistory,
    getMetricKeys,
    getProjects,
    getReports,
    getRun,
    getRuns,
    getRichValues,
    getRunArtifacts,
    getSampledHistory,
    getTraces,
    type Alert,
    type Health,
    type History,
    type Project,
    type Report,
    type ReportPanel,
    type RichValue,
    type Run,
    type RunArtifact,
    type TraceSpan,
  } from "./lib/api";
  import {
    CHART_POINT_BUDGET,
    DELTA_POINT_BUDGET,
    HistoryCache,
    mergeHistoryDelta,
  } from "./lib/history-cache";
  import MetricChart from "./lib/MetricChart.svelte";
  import HistogramChart from "./lib/HistogramChart.svelte";
  import MarkdownPanel from "./lib/MarkdownPanel.svelte";

  const MAX_CONCURRENT_CHART_REQUESTS = 4;
  const LIVE_REFRESH_MS = 2_000;
  const historyCache = new HistoryCache();

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
  let traceSearch = "";
  let traceSearchLoading = false;
  let histories: Record<string, History> = {};
  let historyRevisions: Record<string, number> = {};
  let loadingMetrics = new Set<string>();
  let visibleMetrics = new Set<string>();
  let pendingMetrics: string[] = [];
  let activeChartRequests = 0;
  let reportHistories: Record<string, History> = {};
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
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    selectedReport = null;
    resetReportState();
    resetChartState();
    selectedRun = run;
    error = null;
    try {
      [metricKeys, alerts, richValues, artifacts, traces] = await Promise.all([
        getMetricKeys(run.id, controller.signal),
        getAlerts(run.id, controller.signal),
        getRichValues(run.id, controller.signal),
        getRunArtifacts(run.id, controller.signal),
        getTraces(run.id, "", controller.signal),
      ]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  function resetRunSelection(): void {
    runController?.abort();
    selectedRun = null;
    selectedReport = null;
    metricKeys = [];
    alerts = [];
    richValues = [];
    artifacts = [];
    traces = [];
    traceSearch = "";
    resetChartState();
    resetReportState();
  }

  function resetChartState(): void {
    histories = {};
    historyRevisions = {};
    loadingMetrics = new Set();
    visibleMetrics = new Set();
    pendingMetrics = [];
  }

  function resetReportState(): void {
    reportHistories = {};
    loadingReportMetrics = new Set();
    pendingReportMetrics = [];
  }

  function chooseReport(report: Report): void {
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
    resetChartState();
    resetReportState();
    error = null;
  }

  function reportChartVisible(panel: ReportPanel, metric: string): void {
    const runId = panel.run_id;
    if (!runId) return;
    const identity = `${panel.id}:${runId}:${metric}`;
    if (reportHistories[identity] || loadingReportMetrics.has(identity)) return;
    if (pendingReportMetrics.some((candidate) => candidate.identity === identity)) return;
    pendingReportMetrics = [...pendingReportMetrics, { identity, runId, metric }];
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
      activeReportRequests += 1;
      loadingReportMetrics = new Set([...loadingReportMetrics, request.identity]);
      void getSampledHistory(request.runId, [request.metric], CHART_POINT_BUDGET, controller.signal)
        .then((history) => {
          if (selectedReport?.id !== report.id) return;
          reportHistories = { ...reportHistories, [request.identity]: history };
        })
        .catch((reason) => {
          if (!controller.signal.aborted) showError(reason);
        })
        .finally(() => {
          activeReportRequests -= 1;
          const nextLoading = new Set(loadingReportMetrics);
          nextLoading.delete(request.identity);
          loadingReportMetrics = nextLoading;
          drainReportMetricQueue();
        });
    }
  }

  function reportMetricIdentity(panel: ReportPanel, metric: string): string {
    return `${panel.id}:${panel.run_id ?? ""}:${metric}`;
  }

  function chartVisible(metric: string): void {
    if (!visibleMetrics.has(metric)) visibleMetrics = new Set([...visibleMetrics, metric]);
    queueMetric(metric);
  }

  function queueMetric(metric: string): void {
    const run = selectedRun;
    if (!run || historyRevisions[metric] === run.metric_revision) return;
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
      activeChartRequests += 1;
      loadingMetrics = new Set([...loadingMetrics, metric]);
      void loadMetric(run, metric, controller.signal)
        .catch((reason) => {
          if (!controller.signal.aborted) showError(reason);
        })
        .finally(() => {
          activeChartRequests -= 1;
          const nextLoading = new Set(loadingMetrics);
          nextLoading.delete(metric);
          loadingMetrics = nextLoading;
          drainMetricQueue();
        });
    }
  }

  async function loadMetric(run: Run, metric: string, signal: AbortSignal): Promise<void> {
    const revision = run.metric_revision;
    const cached = historyCache.get(run.id, metric, revision);
    if (cached) {
      publishHistory(run.id, metric, revision, cached);
      return;
    }

    const current = histories[metric];
    let result: History | undefined;
    if (current?.source_last_sequence !== null && current?.source_last_sequence !== undefined) {
      const delta = await getHistory(
        run.id,
        [metric],
        DELTA_POINT_BUDGET + 1,
        signal,
        current.source_last_sequence,
      );
      if (
        delta.next_after === null &&
        delta.sequence.length <= DELTA_POINT_BUDGET &&
        current.sequence.length + delta.sequence.length <= CHART_POINT_BUDGET + DELTA_POINT_BUDGET
      ) {
        result = mergeHistoryDelta(current, delta, metric);
      }
    }
    if (!result) {
      result = await getSampledHistory(run.id, [metric], CHART_POINT_BUDGET, signal);
    }
    historyCache.set(run.id, metric, revision, result);
    publishHistory(run.id, metric, revision, result);
  }

  function publishHistory(runId: string, metric: string, revision: number, result: History): void {
    if (selectedRun?.id !== runId) return;
    histories = { ...histories, [metric]: result };
    historyRevisions = { ...historyRevisions, [metric]: revision };
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
      [alerts, richValues, artifacts, traces] = await Promise.all([
        getAlerts(latest.id, controller.signal),
        getRichValues(latest.id, controller.signal),
        getRunArtifacts(latest.id, controller.signal),
        getTraces(latest.id, traceSearch, controller.signal),
      ]);
      if (revisionChanged) {
        metricKeys = await getMetricKeys(latest.id, controller.signal);
        for (const metric of visibleMetrics) queueMetric(metric);
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

  function metadataString(value: RichValue, key: string): string | undefined {
    const result = value.metadata[key];
    return typeof result === "string" ? result : undefined;
  }

  function histogramCounts(value: RichValue): number[] {
    const counts = value.metadata.counts;
    return Array.isArray(counts)
      ? counts.filter((item): item is number => typeof item === "number")
      : [];
  }

  function tableColumns(value: RichValue): string[] {
    const columns = value.metadata.columns;
    return Array.isArray(columns)
      ? columns.filter((item): item is string => typeof item === "string")
      : [];
  }

  function tablePreview(value: RichValue): unknown[][] {
    const preview = value.metadata.preview;
    return Array.isArray(preview)
      ? preview.filter((item): item is unknown[] => Array.isArray(item))
      : [];
  }

  function formatBytes(value: number): string {
    if (value < 1024) return `${value} B`;
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let size = value;
    let unit = -1;
    do {
      size /= 1024;
      unit += 1;
    } while (size >= 1024 && unit < units.length - 1);
    return `${size.toLocaleString(undefined, { maximumFractionDigits: 1 })} ${units[unit]}`;
  }

  async function searchTraces(): Promise<void> {
    const run = selectedRun;
    const controller = runController;
    if (!run || !controller || traceSearchLoading) return;
    traceSearchLoading = true;
    try {
      traces = await getTraces(run.id, traceSearch, controller.signal);
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
                <small>{run.state} · r{run.metric_revision}</small>
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
                          title={panel.metric_keys.length === 1 ? metric : metric}
                          history={reportHistories[identity]}
                          loading={loadingReportMetrics.has(identity)}
                          onvisible={() => reportChartVisible(panel, metric)}
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
              <span class="run-state" class:live={selectedRun.state === "running"}
                >{selectedRun.state}</span
              >
            </div>

            <div class="document-cards">
              <article>
                <div class="card-heading"><strong>Summary</strong></div>
                <dl>
                  {#each Object.entries(selectedRun.summary) as [key, value]}
                    <div>
                      <dt>{key}</dt>
                      <dd>{formatValue(value)}</dd>
                    </div>
                  {/each}
                </dl>
              </article>

              <article>
                <div class="card-heading"><strong>Configuration</strong></div>
                <dl>
                  {#each Object.entries(selectedRun.config) as [key, value]}
                    <div>
                      <dt>{key}</dt>
                      <dd>{formatValue(value)}</dd>
                    </div>
                  {/each}
                </dl>
              </article>
            </div>

            {#if alerts.length > 0}
              <article class="alerts-card">
                <div class="card-heading">
                  <strong>Alerts</strong>
                  <small>{alerts.length} most recent</small>
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
              </article>
            {/if}

            {#if richValues.length > 0}
              <div class="metrics-heading">
                <div>
                  <p class="eyebrow">Native playback and previews</p>
                  <h2>Media & data</h2>
                </div>
                <span>{richValues.length} most recent</span>
              </div>
              <div class="rich-grid">
                {#each richValues as value (value.id)}
                  <article class="rich-card">
                    <div class="card-heading">
                      <div>
                        <small>{value.kind} · step {value.step}</small><strong>{value.key}</strong>
                      </div>
                      {#if value.blob}
                        <a href={blobUrl(value.blob)} download={value.blob.file_name ?? undefined}
                          >download</a
                        >
                      {/if}
                    </div>
                    {#if value.kind === "image" && value.blob}
                      <img
                        loading="lazy"
                        src={blobUrl(value.blob)}
                        alt={metadataString(value, "caption") ?? value.key}
                      />
                    {:else if value.kind === "audio" && value.blob}
                      <audio controls preload="metadata" src={blobUrl(value.blob)}></audio>
                    {:else if value.kind === "video" && value.blob}
                      <!-- svelte-ignore a11y_media_has_caption -->
                      <video controls preload="metadata" src={blobUrl(value.blob)}></video>
                    {:else if value.kind === "histogram"}
                      <HistogramChart counts={histogramCounts(value)} label={value.key} />
                    {:else if value.kind === "table"}
                      <div class="table-preview">
                        <table>
                          <thead
                            ><tr
                              >{#each tableColumns(value) as column}<th>{column}</th>{/each}</tr
                            ></thead
                          >
                          <tbody>
                            {#each tablePreview(value) as row}
                              <tr
                                >{#each row as cell}<td>{formatValue(cell)}</td>{/each}</tr
                              >
                            {/each}
                          </tbody>
                        </table>
                      </div>
                    {/if}
                    {#if metadataString(value, "caption")}<p class="media-caption">
                        {metadataString(value, "caption")}
                      </p>{/if}
                  </article>
                {/each}
              </div>
            {/if}

            {#if artifacts.length > 0}
              <div class="metrics-heading">
                <div>
                  <p class="eyebrow">Versioned inputs and outputs</p>
                  <h2>Artifacts</h2>
                </div>
                <span>{artifacts.length} lineage links</span>
              </div>
              <div class="artifact-list">
                {#each artifacts as linked (`${linked.artifact.id}:${linked.relation}`)}
                  <article class="artifact-card">
                    <div class="artifact-title">
                      <span class:artifact-input={linked.relation === "input"}
                        >{linked.relation}</span
                      >
                      <strong>{linked.artifact.name}:v{linked.artifact.version}</strong>
                      <small>{linked.artifact.type}</small>
                    </div>
                    {#if linked.artifact.aliases.length > 0}
                      <div class="artifact-aliases">
                        {#each linked.artifact.aliases as alias}<span>{alias}</span>{/each}
                      </div>
                    {/if}
                    {#if linked.artifact.description}<p>{linked.artifact.description}</p>{/if}
                    <div class="artifact-files">
                      {#each linked.artifact.entries as entry}
                        <a
                          href={artifactFileUrl(linked.artifact.id, entry.path)}
                          download={entry.blob.file_name ?? entry.path}
                        >
                          <span>{entry.path}</span><small>{formatBytes(entry.blob.size)}</small>
                        </a>
                      {/each}
                    </div>
                  </article>
                {/each}
              </div>
            {/if}

            <div class="metrics-heading trace-heading">
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
                <input
                  aria-label="Search traces"
                  placeholder="Search traces and messages"
                  bind:value={traceSearch}
                />
                <button type="submit" disabled={traceSearchLoading}
                  >{traceSearchLoading ? "Searching" : "Search"}</button
                >
              </form>
            </div>
            {#if traces.length > 0}
              <div class="trace-list">
                {#each traces as span (span.id)}
                  <article class="trace-card" class:trace-error={span.status === "error"}>
                    <div class="trace-title">
                      <span>{span.kind}</span>
                      <strong>{span.name}</strong>
                      <small>{span.status} · {traceDuration(span)}</small>
                      {#if span.payload}<a href={blobUrl(span.payload)}>payload</a>{/if}
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

            <div class="metrics-heading">
              <div>
                <p class="eyebrow">Bounded histories</p>
                <h2>Metrics</h2>
              </div>
              <span>{metricKeys.length.toLocaleString()} keys · four concurrent queries</span>
            </div>
            {#if metricKeys.length > 0}
              <div class="metric-grid">
                {#each metricKeys as metric (metric)}
                  <MetricChart
                    {metric}
                    history={histories[metric]}
                    loading={loadingMetrics.has(metric)}
                    onvisible={chartVisible}
                  />
                {/each}
              </div>
            {:else}
              <section class="metric-empty">No scalar metrics logged yet.</section>
            {/if}
          {:else}
            <section class="empty">This project has no runs.</section>
          {/if}
        </section>
      </div>
    {/if}
  </main>
</div>
