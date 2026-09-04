<script lang="ts">
  import {
    comparisonSeriesHistory,
    type ChartHistoryViewport,
    type ComparisonChartHistory,
    type MetricCatalogEntry,
    type RunListItem,
  } from "./api";
  import { chartPreferenceIdentity } from "./chart-preferences";
  import {
    METRIC_CATALOG_PAGE_SIZE,
    type MetricSetMode,
    type RunAlignment,
  } from "./comparison-state";
  import Icon from "./Icon.svelte";
  import MetricChart from "./MetricChart.svelte";
  import {
    readMetricColumnCount,
    rememberMetricColumnCount,
    type MetricColumnCount,
  } from "./metric-layout";
  import SelectControl from "./SelectControl.svelte";
  import { resolveRunStyle, type RunStylePreferences } from "./sidebar-preferences";

  export let active: boolean;
  export let project: string;
  export let runs: RunListItem[];
  export let runStylePreferences: RunStylePreferences = {};
  export let highlightedRunId: string | null = null;
  export let selectedRunCount: number;
  export let catalog: MetricCatalogEntry[];
  export let totalCount: number;
  export let catalogLoading: boolean;
  export let catalogError: string | null;
  export let search: string;
  export let mode: MetricSetMode;
  export let alignment: RunAlignment;
  export let after: string | null;
  export let nextAfter: string | null;
  export let cursorDepth: number;
  export let backHistoryTruncated: boolean;
  export let histories: Record<string, ComparisonChartHistory>;
  export let viewports: Record<string, ChartHistoryViewport | null>;
  export let loadingMetrics: Set<string>;
  export let errors: Record<string, string>;
  export let onsearch: (value: string) => void;
  export let onmodechange: (mode: MetricSetMode) => void;
  export let onalignmentchange: (alignment: RunAlignment) => void;
  export let onretrycatalog: () => void;
  export let oncursor: (direction: "previous" | "next") => void;
  export let onretrymetric: (metric: string) => void;
  export let onvisibilitychange: (metric: string, visible: boolean) => void;
  export let onviewportchange: (
    metric: string,
    minimum: number | null,
    maximum: number | null,
  ) => void;

  let metricColumnCount = readMetricColumnCount();

  function setMetricColumnCount(value: string): void {
    metricColumnCount = rememberMetricColumnCount(value as MetricColumnCount);
  }

  function chartSeries(
    metric: string,
    entry: MetricCatalogEntry,
    response: ComparisonChartHistory | undefined,
    currentRuns: RunListItem[],
    styles: RunStylePreferences,
    loading: boolean,
  ) {
    const historyResolved = response !== undefined;
    return currentRuns.map((run) => {
      const available = entry.run_ids.includes(run.id);
      return {
        runId: run.id,
        runName: run.name,
        ...resolveRunStyle(run.id, styles),
        available,
        history: response ? comparisonSeriesHistory(response, run.id, metric) : undefined,
        historyResolved,
        loading: available && loading,
      };
    });
  }
</script>

<div
  class="run-tab-panel"
  id="run-panel-metrics"
  role="tabpanel"
  aria-labelledby="run-tab-metrics"
  hidden={!active}
>
  <div class="section-heading metrics-toolbar">
    <div>
      <p class="eyebrow">{runs.length} runs · exact buckets · four concurrent queries</p>
      <h2>Metrics</h2>
    </div>
    <label class="search-control">
      <Icon name="search" size={15} />
      <input
        type="search"
        name="metric-search"
        maxlength="256"
        aria-label="Search metrics"
        placeholder="Search metric keys"
        value={search}
        oninput={(event) => onsearch(event.currentTarget.value)}
      />
    </label>
    <span>
      {totalCount.toLocaleString()}
      {search.trim() ? "matching " : ""}{totalCount === 1 ? "metric" : "metrics"}
    </span>
  </div>
  <div class="comparison-controls">
    <div class="segmented-control" aria-label="Metric availability mode">
      <button
        type="button"
        class:active={mode === "union"}
        aria-pressed={mode === "union"}
        onclick={() => onmodechange("union")}>Any run</button
      >
      <button
        type="button"
        class:active={mode === "intersection"}
        aria-pressed={mode === "intersection"}
        onclick={() => onmodechange("intersection")}>All runs</button
      >
    </div>
    <label class="alignment-control">
      <span>Align x-axis</span>
      <SelectControl
        ariaLabel="Align x-axis"
        compact
        fit
        value={alignment}
        options={[
          { value: "step", label: "Absolute step" },
          { value: "relative-step", label: "Relative step" },
          { value: "elapsed-time", label: "Elapsed time" },
        ]}
        onvaluechange={(value) => onalignmentchange(value as RunAlignment)}
      />
    </label>
    <label class="alignment-control">
      <span>Columns</span>
      <SelectControl
        ariaLabel="Chart columns"
        compact
        fit
        value={metricColumnCount}
        options={[
          { value: "auto", label: "Auto" },
          { value: "1", label: "1" },
          { value: "2", label: "2" },
          { value: "3", label: "3" },
          { value: "4", label: "4" },
        ]}
        onvaluechange={setMetricColumnCount}
      />
    </label>
    {#if catalog.length > 0}
      <nav class="metric-pagination" aria-label="Metric chart pages">
        <button
          type="button"
          disabled={cursorDepth === 0 && !after}
          aria-label={cursorDepth === 0 && after ? "First metric page" : "Previous metric page"}
          title={cursorDepth === 0 && after ? "First metric page" : "Previous metric page"}
          onclick={() => oncursor("previous")}
        >
          <Icon name="chevron-left" size={15} />
        </button>
        <span>
          {(cursorDepth * METRIC_CATALOG_PAGE_SIZE + 1).toLocaleString()}–{Math.min(
            cursorDepth * METRIC_CATALOG_PAGE_SIZE + catalog.length,
            totalCount,
          ).toLocaleString()} of {totalCount.toLocaleString()}
          {totalCount === 1 ? "metric" : "metrics"}{backHistoryTruncated
            ? " · back history limited"
            : ""}
        </span>
        <button
          type="button"
          disabled={!nextAfter}
          aria-label="Next metric page"
          title="Next metric page"
          onclick={() => oncursor("next")}
        >
          <Icon name="chevron-right" size={15} />
        </button>
      </nav>
    {/if}
  </div>
  {#if catalogError}
    <section class="resource-error" role="alert">
      <span>{catalogError}</span>
      <button type="button" onclick={onretrycatalog}>Retry metric catalog</button>
    </section>
  {/if}
  {#if catalog.length > 0}
    <div
      class="metric-grid"
      class:fixed-columns={metricColumnCount !== "auto"}
      data-columns={metricColumnCount}
      style={`--metric-columns: ${metricColumnCount}`}
    >
      {#each catalog as entry (`${project}:${entry.key}`)}
        <MetricChart
          metric={entry.key}
          identity={chartPreferenceIdentity(project, entry.key)}
          title={entry.key}
          series={chartSeries(
            entry.key,
            entry,
            histories[entry.key],
            runs,
            runStylePreferences,
            loadingMetrics.has(entry.key),
          )}
          {highlightedRunId}
          parentViewport={viewports[entry.key]
            ? {
                minimum: viewports[entry.key]!.stepMin,
                maximum: viewports[entry.key]!.stepMax,
              }
            : null}
          xAlignment={alignment}
          loadError={errors[entry.key] ?? null}
          onretry={onretrymetric}
          {onvisibilitychange}
          {onviewportchange}
        />
      {/each}
    </div>
  {:else}
    <section class="metric-empty">
      {selectedRunCount === 0
        ? "Select one or more runs to compare metrics."
        : catalogLoading
          ? "Loading metric catalogs…"
          : search.trim()
            ? "No metric keys match this search."
            : "No scalar metrics are available in this mode."}
    </section>
  {/if}
</div>
