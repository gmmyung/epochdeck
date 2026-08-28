<script lang="ts">
  import { onMount } from "svelte";

  import {
    getAlerts,
    getHealth,
    getHistory,
    getMetricKeys,
    getProjects,
    getRun,
    getRuns,
    getSampledHistory,
    type Alert,
    type Health,
    type History,
    type Project,
    type Run,
  } from "./lib/api";
  import {
    CHART_POINT_BUDGET,
    DELTA_POINT_BUDGET,
    HistoryCache,
    mergeHistoryDelta,
  } from "./lib/history-cache";
  import MetricChart from "./lib/MetricChart.svelte";

  const MAX_CONCURRENT_CHART_REQUESTS = 4;
  const LIVE_REFRESH_MS = 2_000;
  const historyCache = new HistoryCache();

  let health: Health | null = null;
  let projects: Project[] = [];
  let runs: Run[] = [];
  let selectedProject = "";
  let selectedRun: Run | null = null;
  let metricKeys: string[] = [];
  let alerts: Alert[] = [];
  let histories: Record<string, History> = {};
  let historyRevisions: Record<string, number> = {};
  let loadingMetrics = new Set<string>();
  let visibleMetrics = new Set<string>();
  let pendingMetrics: string[] = [];
  let activeChartRequests = 0;
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
    error = null;
    try {
      runs = await getRuns(name, controller.signal);
      if (runs[0]) await chooseRun(runs[0]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function chooseRun(run: Run): Promise<void> {
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    resetChartState();
    selectedRun = run;
    error = null;
    try {
      [metricKeys, alerts] = await Promise.all([
        getMetricKeys(run.id, controller.signal),
        getAlerts(run.id, controller.signal),
      ]);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  function resetRunSelection(): void {
    runController?.abort();
    selectedRun = null;
    metricKeys = [];
    alerts = [];
    resetChartState();
  }

  function resetChartState(): void {
    histories = {};
    historyRevisions = {};
    loadingMetrics = new Set();
    visibleMetrics = new Set();
    pendingMetrics = [];
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
      alerts = await getAlerts(latest.id, controller.signal);
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

          <div class="run-list" aria-label="Runs">
            {#each runs as run (run.id)}
              <button class:active={selectedRun?.id === run.id} onclick={() => chooseRun(run)}>
                <span>{run.name}</span>
                <small>{run.state} · r{run.metric_revision}</small>
              </button>
            {/each}
          </div>
        </aside>

        <section class="run-view">
          {#if selectedRun}
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
