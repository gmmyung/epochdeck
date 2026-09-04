<script lang="ts">
  import ArtifactBrowser from "./ArtifactBrowser.svelte";
  import type { RunListItem } from "./api";
  import type { PaginatedRunTab, RunResourceState } from "./run-resources";

  export let active: boolean;
  export let state: RunResourceState;
  export let runs: RunListItem[];
  export let error: string | undefined;
  export let loading: boolean;
  export let loadingMoreTab: PaginatedRunTab | null;
  export let onretry: () => void;
  export let onselectdetail: (artifactId: string) => void;
  export let onloadmore: () => void;

  $: hasMore = Object.values(state.artifactCursors).some(Boolean);
  $: runNames = Object.fromEntries(runs.map((run) => [run.id, run.name]));
</script>

<div
  class="run-tab-panel"
  id="run-panel-artifacts"
  role="tabpanel"
  aria-labelledby="run-tab-artifacts"
  aria-busy={loading}
  hidden={!active}
>
  <div class="section-heading">
    <div>
      <p class="eyebrow">
        Versioned inputs and outputs · {state.artifactRunIds.length.toLocaleString()} selected
        {state.artifactRunIds.length === 1 ? "run" : "runs"}
      </p>
      <h2>Artifacts</h2>
    </div>
    <span
      >{state.artifacts.length.toLocaleString()}{hasMore || state.truncatedTabs.has("artifacts")
        ? "+"
        : ""} artifacts</span
    >
  </div>
  {#if error}
    <section class="resource-error" role="alert">
      <span>{error}</span>
      <button type="button" onclick={onretry}>Retry artifacts</button>
    </section>
  {/if}
  {#if loading && state.artifacts.length === 0}
    <section class="metric-empty">Loading artifacts…</section>
  {:else}
    <ArtifactBrowser
      artifacts={state.artifacts}
      {runNames}
      details={state.artifactDetails}
      detailLoading={state.artifactDetailLoading}
      detailErrors={state.artifactDetailErrors}
      onselect={onselectdetail}
    />
    {#if state.truncatedTabs.has("artifacts")}
      <p class="bounded-window-note" role="status">
        Bounded window · recent and oldest loaded artifact links kept
      </p>
    {/if}
    {#if hasMore}
      <button
        class="load-more"
        type="button"
        disabled={loadingMoreTab !== null}
        onclick={onloadmore}
      >
        {loadingMoreTab === "artifacts" ? "Loading…" : "Load 100 more"}
      </button>
    {/if}
  {/if}
</div>
