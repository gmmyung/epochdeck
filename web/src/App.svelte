<script lang="ts">
  import { onMount } from "svelte";

  import {
    getHealth,
    getMetricKeys,
    getProjects,
    getRuns,
    getSampledHistory,
    type Health,
    type History,
    type Project,
    type Run,
  } from "./lib/api";

  let health: Health | null = null;
  let projects: Project[] = [];
  let runs: Run[] = [];
  let selectedProject = "";
  let selectedRun: Run | null = null;
  let metricKeys: string[] = [];
  let selectedMetric = "";
  let history: History | null = null;
  let error: string | null = null;
  let loading = true;
  let canvas: HTMLCanvasElement;
  let chartRevision = 0;
  let projectController: AbortController | null = null;
  let runController: AbortController | null = null;

  onMount(() => {
    const controller = new AbortController();
    const resize = () => (chartRevision += 1);
    const theme = window.matchMedia("(prefers-color-scheme: dark)");
    window.addEventListener("resize", resize);
    theme.addEventListener("change", resize);
    Promise.all([getHealth(controller.signal), getProjects(controller.signal)])
      .then(async ([healthResult, projectResult]) => {
        health = healthResult;
        projects = projectResult;
        if (projects[0]) {
          await chooseProject(projects[0].name);
        }
      })
      .catch(showError)
      .finally(() => {
        loading = false;
      });
    return () => {
      controller.abort();
      projectController?.abort();
      runController?.abort();
      window.removeEventListener("resize", resize);
      theme.removeEventListener("change", resize);
    };
  });

  $: if (canvas && history && selectedMetric && chartRevision >= 0) {
    drawChart(canvas, history.step, history.metrics[selectedMetric] ?? []);
  }

  async function chooseProject(name: string): Promise<void> {
    projectController?.abort();
    const controller = new AbortController();
    projectController = controller;
    selectedProject = name;
    selectedRun = null;
    metricKeys = [];
    selectedMetric = "";
    history = null;
    error = null;
    try {
      runs = await getRuns(name, controller.signal);
      if (runs[0]) {
        await chooseRun(runs[0]);
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function chooseRun(run: Run): Promise<void> {
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    selectedRun = run;
    history = null;
    selectedMetric = "";
    error = null;
    try {
      metricKeys = await getMetricKeys(run.id, controller.signal);
      if (metricKeys[0]) {
        selectedMetric = metricKeys[0];
        await loadMetric(metricKeys[0], controller.signal);
      }
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function metricChanged(event: Event): Promise<void> {
    const metric = (event.currentTarget as HTMLSelectElement).value;
    selectedMetric = metric;
    runController?.abort();
    const controller = new AbortController();
    runController = controller;
    try {
      await loadMetric(metric, controller.signal);
    } catch (reason) {
      if (!controller.signal.aborted) showError(reason);
    }
  }

  async function loadMetric(metric: string, signal: AbortSignal): Promise<void> {
    if (!selectedRun) return;
    history = await getSampledHistory(selectedRun.id, [metric], 2_000, signal);
  }

  function showError(reason: unknown): void {
    error = reason instanceof Error ? reason.message : "Unable to reach Runloom";
  }

  function drawChart(
    target: HTMLCanvasElement,
    steps: number[],
    values: Array<number | null>,
  ): void {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const ratio = window.devicePixelRatio || 1;
    target.width = Math.floor(width * ratio);
    target.height = Math.floor(height * ratio);
    const context = target.getContext("2d");
    if (!context) return;
    context.scale(ratio, ratio);
    context.clearRect(0, 0, width, height);
    const points = values
      .map((value, index) => ({ x: steps[index], y: value }))
      .filter((point): point is { x: number; y: number } => point.y !== null);
    if (points.length === 0) return;

    const padding = 24;
    const minX = Math.min(...points.map((point) => point.x));
    const maxX = Math.max(...points.map((point) => point.x));
    const minY = Math.min(...points.map((point) => point.y));
    const maxY = Math.max(...points.map((point) => point.y));
    const xRange = maxX - minX || 1;
    const yRange = maxY - minY || 1;

    const styles = getComputedStyle(target);
    context.strokeStyle = styles.getPropertyValue("--chart-grid").trim() || "#d9dde0";
    context.lineWidth = 1;
    for (let line = 0; line <= 4; line += 1) {
      const y = padding + ((height - padding * 2) * line) / 4;
      context.beginPath();
      context.moveTo(padding, y);
      context.lineTo(width - padding, y);
      context.stroke();
    }

    context.strokeStyle = styles.getPropertyValue("--accent").trim() || "#2766ad";
    context.lineWidth = 1.75;
    context.lineJoin = "round";
    context.beginPath();
    points.forEach((point, index) => {
      const x = padding + ((point.x - minX) / xRange) * (width - padding * 2);
      const y = height - padding - ((point.y - minY) / yRange) * (height - padding * 2);
      if (index === 0) context.moveTo(x, y);
      else context.lineTo(x, y);
    });
    context.stroke();
  }

  function formatValue(value: unknown): string {
    if (typeof value === "number") {
      return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
    }
    if (typeof value === "string") return value;
    return JSON.stringify(value);
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
    {#if error}
      <div class="error" role="alert">{error}</div>
    {/if}

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

            <div class="cards">
              <article class="chart-card">
                <div class="card-heading">
                  <div>
                    <small>Metric history</small>
                    <strong>{selectedMetric || "No metrics logged"}</strong>
                  </div>
                  {#if metricKeys.length > 0}
                    <select aria-label="Metric" value={selectedMetric} onchange={metricChanged}>
                      {#each metricKeys as metric}
                        <option value={metric}>{metric}</option>
                      {/each}
                    </select>
                  {/if}
                </div>
                <canvas bind:this={canvas} aria-label={`${selectedMetric} history chart`}></canvas>
                <div class="chart-footer">
                  <span
                    >{history?.sequence.length ?? 0} extrema from
                    {(history?.source_points ?? 0).toLocaleString()} source points</span
                  >
                  <span>min/max sampled · 2,000 point budget</span>
                </div>
              </article>

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
          {:else}
            <section class="empty">This project has no runs.</section>
          {/if}
        </section>
      </div>
    {/if}
  </main>
</div>
