<script lang="ts">
  import { blobUrl, type TraceSpan, type TraceSpanSummary } from "./api";
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import { formatDurationMs } from "./resource-state";
  import type { PaginatedRunTab, RunResourceState } from "./run-resources";

  const MAX_INLINE_MESSAGES = 100;
  const MAX_INLINE_MESSAGE_CHARACTERS = 4_000;
  const MAX_EXPANDED_TRACES = 24;

  export let active: boolean;
  export let state: RunResourceState;
  export let search: string;
  export let error: string | undefined;
  export let loading: boolean;
  export let loadingMoreTab: PaginatedRunTab | null;
  export let onsearch: () => void;
  export let onretry: () => void;
  export let onselectdetail: (spanId: string) => void;
  export let onloadmore: () => void;

  let expandedSpanIds = new Set<string>();

  function toggleDetail(spanId: string, detail: TraceSpan | undefined): void {
    const next = new Set(expandedSpanIds);
    if (next.has(spanId)) next.delete(spanId);
    else {
      next.add(spanId);
      if (!detail) onselectdetail(spanId);
    }
    expandedSpanIds = new Set([...next].slice(-MAX_EXPANDED_TRACES));
  }

  function duration(span: TraceSpanSummary): string {
    return formatDurationMs(span.end_time_ms - span.start_time_ms);
  }

  function messages(span: TraceSpan): Array<{ role: string; content: string }> {
    const value = span.preview.messages;
    if (!Array.isArray(value)) return [];
    return value.slice(0, MAX_INLINE_MESSAGES).flatMap((message) => {
      if (typeof message !== "object" || message === null) return [];
      const candidate = message as Record<string, unknown>;
      if (typeof candidate.role !== "string" || typeof candidate.content !== "string") return [];
      const content =
        candidate.content.length > MAX_INLINE_MESSAGE_CHARACTERS
          ? `${candidate.content.slice(0, MAX_INLINE_MESSAGE_CHARACTERS)}…`
          : candidate.content;
      return [{ role: candidate.role, content }];
    });
  }
</script>

<div
  class="run-tab-panel"
  id="run-panel-traces"
  role="tabpanel"
  aria-labelledby="run-tab-traces"
  aria-busy={loading || state.traceSearchLoading}
  hidden={!active}
>
  <div class="section-heading trace-heading">
    <div>
      <p class="eyebrow">Indexed metadata · payloads in object storage</p>
      <h2>Traces</h2>
    </div>
    <form
      class="trace-search"
      onsubmit={(event) => {
        event.preventDefault();
        onsearch();
      }}
    >
      <label class="search-control">
        <Icon name="search" size={15} />
        <input
          name="trace-search"
          aria-label="Search traces"
          placeholder="Search traces and messages"
          maxlength="256"
          bind:value={search}
        />
      </label>
      <button
        class="icon-button"
        type="submit"
        disabled={state.traceSearchLoading}
        aria-label="Search traces"><Icon name="search" size={15} /></button
      >
    </form>
  </div>
  {#if error}
    <section class="resource-error" role="alert">
      <span>{error}</span>
      <button type="button" onclick={onretry}>Retry traces</button>
    </section>
  {/if}
  {#if loading && state.traces.length === 0}
    <section class="metric-empty">Loading traces…</section>
  {:else if state.traces.length > 0}
    <div class="trace-list">
      {#each state.traces as span (span.id)}
        {@const detail = state.traceDetails[span.id]}
        {@const expanded = expandedSpanIds.has(span.id)}
        <article class="trace-card" class:trace-error={span.status === "error"}>
          <div class="trace-title">
            <span>{span.kind}</span>
            <strong>{span.name}</strong>
            <small>{span.status} · {duration(span)}</small>
            {#if span.payload}
              <a
                class="icon-button"
                href={blobUrl(span.payload)}
                download={span.payload.file_name ?? undefined}
                aria-label={`Download ${span.name} payload`}><Icon name="download" size={15} /></a
              >
            {/if}
          </div>
          <div class="trace-identifiers">
            <span>trace {span.trace_id}</span>
            {#if span.parent_span_id}<span>parent {span.parent_span_id}</span>{/if}
            <span>{span.step === null ? "no step" : `step ${span.step}`}</span>
          </div>
          <button
            class="trace-detail-toggle"
            type="button"
            aria-expanded={expanded}
            aria-controls={`trace-detail-${span.id}`}
            disabled={state.traceDetailLoading.has(span.id)}
            onclick={() => toggleDetail(span.id, detail)}
          >
            {state.traceDetailLoading.has(span.id)
              ? "Loading details…"
              : expanded
                ? "Hide details"
                : detail
                  ? "Show details"
                  : "Load details"}
          </button>
          {#if state.traceDetailErrors[span.id]}
            <div class="resource-error" role="alert">
              <span>{state.traceDetailErrors[span.id]}</span>
              <button type="button" onclick={() => onselectdetail(span.id)}>Retry details</button>
            </div>
          {/if}
          {#if expanded && detail}
            {@const visibleMessages = messages(detail)}
            <div id={`trace-detail-${span.id}`}>
              {#if Object.keys(detail.attributes).length > 0}
                <div class="trace-attributes">
                  <JsonTreeNode name="Attributes" value={detail.attributes} />
                </div>
              {/if}
              {#if visibleMessages.length > 0}
                <div class="trace-messages">
                  {#each visibleMessages as message}
                    <div>
                      <strong>{message.role}</strong>
                      <p>{message.content}</p>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
          {/if}
        </article>
      {/each}
    </div>
    {#if state.truncatedTabs.has("traces")}
      <p class="bounded-window-note" role="status">
        Bounded window · recent and oldest loaded traces kept
      </p>
    {/if}
  {:else}
    <section class="metric-empty">
      {search.trim() ? "No traces match this search." : "No structured traces logged yet."}
    </section>
  {/if}
  {#if state.traceCursor}
    <button class="load-more" type="button" disabled={loadingMoreTab !== null} onclick={onloadmore}>
      {loadingMoreTab === "traces" ? "Loading…" : "Load 100 more"}
    </button>
  {/if}
</div>
