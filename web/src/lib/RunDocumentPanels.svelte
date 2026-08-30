<script lang="ts">
  import type { Alert, Run } from "./api";
  import JsonTreeNode from "./JsonTreeNode.svelte";
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

  function formatAlertTime(timestampMs: number): string {
    return new Date(timestampMs).toLocaleString();
  }
</script>

<div
  class="run-tab-panel"
  id="run-panel-summary"
  role="tabpanel"
  aria-labelledby="run-tab-summary"
  hidden={activeTab !== "summary"}
>
  <div class="section-heading">
    <div>
      <p class="eyebrow">Final and derived values</p>
      <h2>Summary</h2>
    </div>
    <span>{Object.keys(run.summary).length.toLocaleString()} fields</span>
  </div>
  {#if run.summary_truncated}
    <p class="bounded-window-note" role="status">
      The latest-value metric preview is limited to 256 keys. Raw metric history and explicit
      summary fields remain complete.
    </p>
  {/if}
  <div class="tree-panel">
    <JsonTreeNode name="" value={run.summary} root />
  </div>

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
  <div class="section-heading">
    <div>
      <p class="eyebrow">Expandable run inputs</p>
      <h2>Configuration</h2>
    </div>
    <span>{Object.keys(run.config).length.toLocaleString()} fields</span>
  </div>
  <div class="tree-panel">
    <JsonTreeNode name="" value={run.config} root />
  </div>
</div>
