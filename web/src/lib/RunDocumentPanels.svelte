<script lang="ts">
  import type { Alert, Run } from "./api";
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import {
    JSON_TREE_SEARCH_MAX_LENGTH,
    normalizeJsonTreeSearch,
    searchJsonTree,
    type JsonTreeSearchMatch,
  } from "./json-tree";
  import type { PaginatedRunTab, RunTab } from "./run-resources";

  export let run: Run;
  export let activeTab: RunTab;
  export let alerts: Alert[];
  export let alertCursor: string | null;
  export let alertsTruncated: boolean;
  export let alertError: string | undefined;
  export let loadingMoreTab: PaginatedRunTab | null;
  export let onretryalerts: () => void;
  export let onloadalerts: () => void;

  let summarySearch = "";
  let configurationSearch = "";
  let summaryQuery = "";
  let configurationQuery = "";
  let summarySearchResult: JsonTreeSearchMatch | null = null;
  let configurationSearchResult: JsonTreeSearchMatch | null = null;

  $: summaryQuery = normalizeJsonTreeSearch(summarySearch);
  $: configurationQuery = normalizeJsonTreeSearch(configurationSearch);
  $: summarySearchResult = summaryQuery ? searchJsonTree(run.summary, summaryQuery) : null;
  $: configurationSearchResult = configurationQuery
    ? searchJsonTree(run.config, configurationQuery)
    : null;

  function formatAlertTime(timestampMs: number): string {
    return new Date(timestampMs).toLocaleString();
  }

  function boundedSearch(value: string): string {
    return value.slice(0, JSON_TREE_SEARCH_MAX_LENGTH);
  }

  function matchLabel(result: JsonTreeSearchMatch | null): string {
    const count = result?.matchCount ?? 0;
    return `${count.toLocaleString()} ${count === 1 ? "match" : "matches"}`;
  }
</script>

<div
  class="run-tab-panel"
  id="run-panel-summary"
  role="tabpanel"
  aria-labelledby="run-tab-summary"
  hidden={activeTab !== "summary"}
>
  <div class="section-heading document-heading">
    <div>
      <p class="eyebrow">Final and derived values</p>
      <h2>Summary</h2>
    </div>
    <label class="search-control document-search">
      <Icon name="search" size={15} />
      <input
        type="search"
        name="summary-search"
        maxlength={JSON_TREE_SEARCH_MAX_LENGTH}
        aria-label="Search summary"
        placeholder="Search summary keys and values"
        value={summarySearch}
        oninput={(event) => (summarySearch = boundedSearch(event.currentTarget.value))}
      />
    </label>
    <span aria-live="polite">
      {summaryQuery
        ? matchLabel(summarySearchResult)
        : `${Object.keys(run.summary).length.toLocaleString()} fields`}
    </span>
  </div>
  {#if run.summary_truncated}
    <p class="bounded-window-note" role="status">
      The latest-value metric preview is limited to 256 keys. Raw metric history and explicit
      summary fields remain complete.
    </p>
  {/if}
  {#if summaryQuery && !summarySearchResult}
    <div class="document-empty" role="status">No summary keys or values match this search.</div>
  {:else if Object.keys(run.summary).length === 0}
    <div class="document-empty" role="status">No summary values have been logged yet.</div>
  {:else}
    <div class="tree-panel">
      <JsonTreeNode
        name=""
        value={run.summary}
        root
        searchQuery={summaryQuery}
        searchResult={summarySearchResult ?? undefined}
      />
    </div>
  {/if}

  {#if alertError}
    <div class="resource-error" role="alert">
      <span>{alertError}</span>
      <button type="button" onclick={onretryalerts}>Retry alerts</button>
    </div>
  {/if}

  {#if alerts.length > 0}
    <div class="section-heading alerts-heading">
      <h2>Alerts</h2>
      <span>
        {alerts.length.toLocaleString()} loaded{alertCursor
          ? " · older available"
          : alertsTruncated
            ? " · bounded window"
            : ""}
      </span>
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
    {#if alertCursor}
      <button
        class="load-more"
        type="button"
        disabled={loadingMoreTab !== null}
        onclick={onloadalerts}
      >
        {loadingMoreTab === "summary" ? "Loading…" : "Load 100 older alerts"}
      </button>
    {/if}
  {/if}
</div>

<div
  class="run-tab-panel"
  id="run-panel-configuration"
  role="tabpanel"
  aria-labelledby="run-tab-configuration"
  hidden={activeTab !== "configuration"}
>
  <div class="section-heading document-heading">
    <div>
      <p class="eyebrow">Expandable run inputs</p>
      <h2>Configuration</h2>
    </div>
    <label class="search-control document-search">
      <Icon name="search" size={15} />
      <input
        type="search"
        name="configuration-search"
        maxlength={JSON_TREE_SEARCH_MAX_LENGTH}
        aria-label="Search configuration"
        placeholder="Search configuration keys and values"
        value={configurationSearch}
        oninput={(event) => (configurationSearch = boundedSearch(event.currentTarget.value))}
      />
    </label>
    <span aria-live="polite">
      {configurationQuery
        ? matchLabel(configurationSearchResult)
        : `${Object.keys(run.config).length.toLocaleString()} fields`}
    </span>
  </div>
  {#if configurationQuery && !configurationSearchResult}
    <div class="document-empty" role="status">
      No configuration keys or values match this search.
    </div>
  {:else if Object.keys(run.config).length === 0}
    <div class="document-empty" role="status">No configuration values were recorded.</div>
  {:else}
    <div class="tree-panel">
      <JsonTreeNode
        name=""
        value={run.config}
        root
        searchQuery={configurationQuery}
        searchResult={configurationSearchResult ?? undefined}
      />
    </div>
  {/if}
</div>

<style>
  .section-heading.document-heading {
    display: grid;
    grid-template-columns: minmax(140px, 1fr) minmax(220px, 420px) auto;
    align-items: end;
  }

  .document-search {
    width: 100%;
    min-width: 0;
  }

  .document-empty {
    min-height: 120px;
    display: grid;
    place-content: center;
    border-top: 1px solid var(--line);
    border-bottom: 1px solid var(--line);
    background: var(--panel);
    color: var(--muted);
    font-size: 11px;
    text-align: center;
  }

  @media (max-width: 680px) {
    .section-heading.document-heading {
      grid-template-columns: minmax(0, 1fr) auto;
    }

    .document-heading > span {
      grid-column: 2;
      grid-row: 1;
    }

    .document-search {
      grid-column: 1 / -1;
      grid-row: 2;
    }
  }
</style>
