<script lang="ts">
  import { onDestroy } from "svelte";

  import type { ChartHistory, ChartHistoryViewport, Report, ReportPanel, RunListItem } from "./api";
  import type { MetricChartSeries } from "./chart-series";
  import { runStyle } from "./comparison-state";
  import MarkdownPanel from "./MarkdownPanel.svelte";
  import MetricChart from "./MetricChart.svelte";
  import { paginateReportPanels } from "./report-pagination";

  export let report: Report;
  export let runs: RunListItem[];
  export let histories: Record<string, ChartHistory>;
  export let viewports: Record<string, ChartHistoryViewport | null>;
  export let loadingMetrics: Set<string>;
  export let errors: Record<string, string>;
  export let onvisibilitychange: (panel: ReportPanel, metric: string, visible: boolean) => void;
  export let onviewportchange: (
    panel: ReportPanel,
    metric: string,
    minimum: number | null,
    maximum: number | null,
  ) => void;
  export let onretry: (panel: ReportPanel, metric: string) => void;

  let pageIndex = 0;
  let paginationReportKey = "";
  let pages = paginateReportPanels(report.layout.panels);
  let currentPage = pages[0];
  const visibleCharts = new Map<string, { panel: ReportPanel; metric: string }>();

  $: reportKey = `${report.id}:${report.updated_at}`;
  $: pages = paginateReportPanels(report.layout.panels);
  $: if (reportKey !== paginationReportKey) {
    clearVisibleCharts();
    paginationReportKey = reportKey;
    pageIndex = 0;
  }
  $: if (pageIndex >= pages.length) {
    clearVisibleCharts();
    pageIndex = pages.length - 1;
  }
  $: currentPage = pages[pageIndex];

  function identity(panel: ReportPanel, metric: string): string {
    return `${panel.id}:${panel.run_id ?? ""}:${metric}`;
  }

  function series(
    panel: ReportPanel,
    metric: string,
    currentRuns: RunListItem[],
    history: ChartHistory | undefined,
    loading: boolean,
  ): MetricChartSeries[] {
    const runId = panel.run_id;
    if (!runId) return [];
    const run = currentRuns.find((candidate) => candidate.id === runId);
    return [
      {
        runId,
        runName: run?.name ?? runId.slice(0, 8),
        ...runStyle(runId),
        available: true,
        history,
        historyResolved: history !== undefined,
        loading,
      },
    ];
  }

  function handleChartVisibility(panel: ReportPanel, metric: string, visible: boolean): void {
    const chartIdentity = identity(panel, metric);
    if (visible) {
      visibleCharts.set(chartIdentity, { panel, metric });
      onvisibilitychange(panel, metric, true);
      return;
    }
    if (!visibleCharts.delete(chartIdentity)) return;
    onvisibilitychange(panel, metric, false);
  }

  function clearVisibleCharts(): void {
    const activeCharts = [...visibleCharts.values()];
    visibleCharts.clear();
    for (const { panel, metric } of activeCharts) {
      onvisibilitychange(panel, metric, false);
    }
  }

  function selectPage(nextPage: number): void {
    const boundedPage = Math.max(0, Math.min(nextPage, pages.length - 1));
    if (boundedPage === pageIndex) return;
    clearVisibleCharts();
    pageIndex = boundedPage;
  }

  onDestroy(clearVisibleCharts);
</script>

<div class="run-heading">
  <div>
    <p class="eyebrow">{report.project} / report</p>
    <h1>{report.name}</h1>
    {#if report.description}<p class="report-description">{report.description}</p>{/if}
  </div>
  <span class="run-state">{report.layout.panels.length} panels</span>
</div>

<nav class="report-pagination" aria-label="Report pagination">
  <button
    type="button"
    aria-label="Previous report page"
    disabled={pageIndex === 0}
    onclick={() => selectPage(pageIndex - 1)}>Previous</button
  >
  <span class="report-pagination-status" role="status" aria-live="polite" aria-atomic="true">
    Page {pageIndex + 1} of {pages.length} · {currentPage.panels.length}
    {currentPage.panels.length === 1 ? "panel" : "panels"} · {currentPage.chartCount}
    {currentPage.chartCount === 1 ? "chart" : "charts"}
  </span>
  <button
    type="button"
    aria-label="Next report page"
    disabled={pageIndex === pages.length - 1}
    onclick={() => selectPage(pageIndex + 1)}>Next</button
  >
</nav>

{#key `${reportKey}:${pageIndex}`}
  <div
    class="report-grid"
    style={`--report-columns: ${report.layout.columns}`}
    role="region"
    aria-label={`Report page ${pageIndex + 1} of ${pages.length}`}
  >
    {#each currentPage.panels as panel (panel.id)}
      {#if panel.kind === "markdown"}
        <article
          class="report-markdown-panel"
          style={`grid-column: span ${Math.min(panel.width, report.layout.columns)}; min-height: ${panel.height}px`}
        >
          <div class="card-heading">
            <div><small>Markdown</small><strong>{panel.title}</strong></div>
          </div>
          <MarkdownPanel source={panel.markdown ?? ""} />
        </article>
      {:else}
        <section
          class="report-metric-panel"
          style={`grid-column: span ${Math.min(panel.width, report.layout.columns)}; min-height: ${panel.height}px`}
          aria-label={`${panel.title} report panel`}
        >
          <div class="report-panel-heading">
            <strong>{panel.title}</strong>
            <small>run {panel.run_id?.slice(0, 8)}</small>
          </div>
          <div class="report-metric-grid">
            {#each panel.metric_keys as metric (metric)}
              {@const chartIdentity = identity(panel, metric)}
              <MetricChart
                {metric}
                identity={chartIdentity}
                title={metric}
                series={series(
                  panel,
                  metric,
                  runs,
                  histories[chartIdentity],
                  loadingMetrics.has(chartIdentity),
                )}
                parentViewport={viewports[chartIdentity]
                  ? {
                      minimum: viewports[chartIdentity]!.stepMin,
                      maximum: viewports[chartIdentity]!.stepMax,
                    }
                  : null}
                xAlignment="step"
                loadError={errors[chartIdentity] ?? null}
                onretry={() => onretry(panel, metric)}
                onvisibilitychange={(_, visible) => handleChartVisibility(panel, metric, visible)}
                onviewportchange={(_, minimum, maximum) =>
                  onviewportchange(panel, metric, minimum, maximum)}
              />
            {/each}
          </div>
        </section>
      {/if}
    {/each}
  </div>
{/key}
