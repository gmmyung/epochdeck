<script lang="ts">
  import MediaTimeline from "./MediaTimeline.svelte";
  import type { PaginatedRunTab, RunResourceState } from "./run-resources";
  import SelectControl from "./SelectControl.svelte";

  export let active: boolean;
  export let state: RunResourceState;
  export let error: string | undefined;
  export let loading: boolean;
  export let loadingMoreTab: PaginatedRunTab | null;
  export let onretry: () => void;
  export let onselectkey: (key: string) => void;
  export let onloadkeys: () => void;
  export let onselectdetail: (valueId: string) => void;
  export let onloadmore: () => void;

  $: snapshotCount = state.richKeys.reduce((total, key) => total + key.count, 0);
  $: totalByKey = Object.fromEntries(state.richKeys.map((key) => [key.key, key.count]));
</script>

<div
  class="run-tab-panel"
  id="run-panel-media"
  role="tabpanel"
  aria-labelledby="run-tab-media"
  aria-busy={loading || state.loadingRichTimeline || state.loadingRichKeys}
  hidden={!active}
>
  <div class="section-heading">
    <div>
      <p class="eyebrow">Native playback and previews</p>
      <h2>Media & data</h2>
    </div>
    <span>
      {snapshotCount.toLocaleString()}{state.richKeyCursor || state.truncatedRichKeys ? "+" : ""} snapshots
      · {state.richKeys.length.toLocaleString()}{state.richKeyCursor || state.truncatedRichKeys
        ? "+"
        : ""} keys
    </span>
  </div>
  {#if error}
    <section class="resource-error" role="alert">
      <span>{error}</span>
      <button type="button" onclick={onretry}>Retry media</button>
    </section>
  {/if}
  {#if loading && state.richValues.length === 0}
    <section class="metric-empty">Loading media…</section>
  {:else}
    {#if state.richKeys.length > 0}
      <div class="media-key-toolbar">
        <label class="media-key-field">
          <span class="media-key-label">Media key</span>
          <SelectControl
            ariaLabel="Media key"
            value={state.selectedRichKey ?? ""}
            options={state.richKeys.map((key) => ({
              value: key.key,
              label: `${key.key} · ${key.count.toLocaleString()}`,
            }))}
            compact
            onvaluechange={onselectkey}
          />
        </label>
        {#if state.richKeyCursor}
          <button type="button" disabled={state.loadingRichKeys} onclick={onloadkeys}>
            {state.loadingRichKeys ? "Loading…" : "Load more keys"}
          </button>
        {/if}
      </div>
    {/if}
    {#if state.loadingRichTimeline && state.richValues.length === 0}
      <section class="metric-empty">Loading selected timeline…</section>
    {:else}
      <MediaTimeline
        values={state.richValues}
        {totalByKey}
        details={state.richValueDetails}
        detailLoading={state.richDetailLoading}
        detailErrors={state.richDetailErrors}
        onselect={onselectdetail}
      />
      {#if state.truncatedTabs.has("media") || state.truncatedRichKeys}
        <p class="bounded-window-note" role="status">
          Bounded window · recent and oldest loaded media entries kept
        </p>
      {/if}
    {/if}
    {#if state.richValueCursor}
      <button
        class="load-more"
        type="button"
        disabled={loadingMoreTab !== null}
        onclick={onloadmore}
      >
        {loadingMoreTab === "media" ? "Loading…" : "Load 100 more"}
      </button>
    {/if}
  {/if}
</div>

<style>
  .media-key-toolbar {
    display: flex;
    gap: 8px;
    align-items: flex-end;
    margin: 8px 0 18px;
  }

  .media-key-field {
    width: min(420px, 100%);
    display: grid;
    gap: 5px;
  }

  .media-key-label {
    color: var(--muted);
    font-size: 10px;
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }

  .media-key-toolbar > button {
    min-height: 30px;
    padding: 0 9px;
    border: 1px solid var(--line);
    background: transparent;
    color: var(--muted);
    font-size: 10px;
  }

  .media-key-toolbar > button:hover:not(:disabled) {
    border-color: var(--line-strong);
    background: var(--button-hover);
    color: var(--text);
  }

  @media (max-width: 760px) {
    .media-key-toolbar {
      align-items: stretch;
      flex-direction: column;
    }

    .media-key-field {
      width: 100%;
    }
  }
</style>
