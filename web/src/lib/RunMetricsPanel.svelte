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
    runStyle,
    type MetricSetMode,
    type RunAlignment,
  } from "./comparison-state";
  import Icon from "./Icon.svelte";
  import MetricChart from "./MetricChart.svelte";

  export let active: boolean;
  export let project: string;
  export let runs: RunListItem[];
  export let selectedRunCount: number;
  export let catalog: MetricCatalogEntry[];
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

  function chartSeries(
    metric: string,
    entry: MetricCatalogEntry,
    response: ComparisonChartHistory | undefined,
    currentRuns: RunListItem[],
    loading: boolean,
  ) {
    const historyResolved = response !== undefined;
    return currentRuns.map((run) => {
      const available = entry.run_ids.includes(run.id);
      return {
        runId: run.id,
        runName: run.name,
        ...runStyle(run.id),
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
        maxlength="256"
        aria-label="Search metrics"
        placeholder="Search metric keys"
        value={search}
        oninput={(event) => onsearch(event.currentTarget.value)}
      />
    </label>
    <span>{catalog.length.toLocaleString()} on this page</span>
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
      <select
        value={alignment}
        onchange={(event) => onalignmentchange(event.currentTarget.value as RunAlignment)}
      >
        <option value="step">Absolute step</option>
        <option value="relative-step">Relative step</option>
        <option value="elapsed-time">Elapsed time</option>
      </select>
    </label>
    <span class="availability-hint">Availability is shown on each chart.</span>
  </div>
  {#if catalogError}
    <section class="resource-error" role="alert">
      <span>{catalogError}</span>
      <button type="button" onclick={onretrycatalog}>Retry metric catalog</button>
    </section>
  {/if}
  {#if catalog.length > 0}
    <nav class="metric-pagination" aria-label="Metric chart pages">
      <button
        type="button"
        disabled={cursorDepth === 0 && !after}
        onclick={() => oncursor("previous")}
        >{cursorDepth === 0 && after ? "First" : "Previous"}</button
      >
      <span>
        {catalog.length.toLocaleString()} loaded · up to {METRIC_CATALOG_PAGE_SIZE}
        charts{backHistoryTruncated ? " · back history limited" : ""}
      </span>
      <button type="button" disabled={!nextAfter} onclick={() => oncursor("next")}>Next</button>
    </nav>
    <div class="metric-grid">
      {#each catalog as entry (`${project}:${entry.key}`)}
        <MetricChart
          metric={entry.key}
          identity={chartPreferenceIdentity(project, entry.key)}
          title={selectedRunCount === 1
            ? entry.key
            : `${entry.key} · ${entry.run_ids.length}/${selectedRunCount} runs`}
          series={chartSeries(
            entry.key,
            entry,
            histories[entry.key],
            runs,
            loadingMetrics.has(entry.key),
          )}
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
