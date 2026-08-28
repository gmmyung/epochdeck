<script lang="ts">
  import { onDestroy } from "svelte";

  import HistogramChart from "./HistogramChart.svelte";
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import { blobUrl, type RichValue } from "./api";
  import { groupRichValues, selectedMediaIndex, type MediaGroup } from "./media-timeline";

  export let values: RichValue[];

  let groups: MediaGroup[] = [];
  let selectedIds: Record<string, string> = {};
  let pendingIndexes: Record<string, number> = {};
  const commitTimers = new Map<string, number>();

  $: groups = groupRichValues(values);

  function selection(
    group: MediaGroup,
    selectedId: string | undefined,
  ): { index: number; value: RichValue } {
    const index = selectedMediaIndex(group, selectedId);
    return { index, value: group.values[index] };
  }

  function select(group: MediaGroup, index: number): void {
    const value = group.values[Math.max(0, Math.min(Math.round(index), group.values.length - 1))];
    if (!value) return;
    selectedIds = { ...selectedIds, [group.id]: value.id };
  }

  function sliderIndex(group: MediaGroup, selectedIndex: number): number {
    return pendingIndexes[group.id] ?? selectedIndex;
  }

  function scrub(group: MediaGroup, index: number): void {
    const bounded = Math.max(0, Math.min(Math.round(index), group.values.length - 1));
    pendingIndexes = { ...pendingIndexes, [group.id]: bounded };
    const existing = commitTimers.get(group.id);
    if (existing !== undefined) window.clearTimeout(existing);
    commitTimers.set(
      group.id,
      window.setTimeout(() => commitSelection(group), 120),
    );
  }

  function commitSelection(group: MediaGroup): void {
    const index = pendingIndexes[group.id];
    if (index === undefined) return;
    const timer = commitTimers.get(group.id);
    if (timer !== undefined) window.clearTimeout(timer);
    commitTimers.delete(group.id);
    select(group, index);
    const next = { ...pendingIndexes };
    delete next[group.id];
    pendingIndexes = next;
  }

  onDestroy(() => {
    for (const timer of commitTimers.values()) window.clearTimeout(timer);
  });

  function metadataString(value: RichValue, key: string): string | undefined {
    const result = value.metadata[key];
    return typeof result === "string" ? result : undefined;
  }

  function histogramCounts(value: RichValue): number[] {
    const counts = value.metadata.counts;
    return Array.isArray(counts)
      ? counts.filter((item): item is number => typeof item === "number" && Number.isFinite(item))
      : [];
  }

  function tableColumns(value: RichValue): string[] {
    const columns = value.metadata.columns;
    return Array.isArray(columns)
      ? columns.filter((item): item is string => typeof item === "string")
      : [];
  }

  function tablePreview(value: RichValue): unknown[][] {
    const preview = value.metadata.preview;
    return Array.isArray(preview)
      ? preview.filter((item): item is unknown[] => Array.isArray(item))
      : [];
  }

  function formatValue(value: unknown): string {
    if (value === null) return "null";
    if (typeof value === "number") {
      return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
    }
    if (typeof value === "string") return value;
    if (typeof value === "boolean") return value ? "true" : "false";
    return JSON.stringify(value) ?? String(value);
  }

  function autoplayWhenVisible(video: HTMLVideoElement): { destroy: () => void } {
    video.muted = true;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) void video.play().catch(() => undefined);
        else video.pause();
      },
      { threshold: 0.45 },
    );
    observer.observe(video);
    return {
      destroy: () => {
        observer.disconnect();
        video.pause();
      },
    };
  }
</script>

{#if groups.length > 0}
  <div class="media-timeline">
    {#each groups as group (group.id)}
      {@const selected = selection(group, selectedIds[group.id])}
      {@const rangeIndex = sliderIndex(group, selected.index)}
      {@const rangeValue = group.values[rangeIndex]}
      <section class="media-group" aria-label={`${group.key} ${group.kind} timeline`}>
        <header class="media-heading">
          <div class="media-title">
            <span class="kind">{group.kind}</span>
            <strong>{group.key}</strong>
            <small>{group.values.length.toLocaleString()} snapshots</small>
          </div>
          {#if selected.value.blob}
            <a
              class="icon-button"
              href={blobUrl(selected.value.blob)}
              download={selected.value.blob.file_name ?? undefined}
              aria-label={`Download ${group.key} at step ${selected.value.step}`}
            >
              <Icon name="download" size={16} />
            </a>
          {/if}
        </header>

        <div class="preview">
          {#key selected.value.id}
            {#if selected.value.kind === "image" && selected.value.blob}
              <img
                loading="lazy"
                src={blobUrl(selected.value.blob)}
                alt={metadataString(selected.value, "caption") ?? `${selected.value.key} preview`}
              />
            {:else if selected.value.kind === "audio" && selected.value.blob}
              <audio controls preload="metadata" src={blobUrl(selected.value.blob)}></audio>
            {:else if selected.value.kind === "video" && selected.value.blob}
              <!-- svelte-ignore a11y_media_has_caption -->
              <video
                use:autoplayWhenVisible
                controls
                muted
                playsinline
                preload="metadata"
                src={blobUrl(selected.value.blob)}
              ></video>
            {:else if selected.value.kind === "histogram"}
              {@const counts = histogramCounts(selected.value)}
              {#if counts.length > 0}
                <HistogramChart {counts} label={selected.value.key} />
              {:else}
                <div class="unavailable">No histogram preview is available.</div>
              {/if}
            {:else if selected.value.kind === "table"}
              {@const columns = tableColumns(selected.value)}
              {@const rows = tablePreview(selected.value)}
              {#if columns.length > 0 || rows.length > 0}
                <div class="table-preview">
                  <table>
                    {#if columns.length > 0}
                      <thead>
                        <tr
                          >{#each columns as column}<th>{column}</th>{/each}</tr
                        >
                      </thead>
                    {/if}
                    <tbody>
                      {#each rows as row}
                        <tr
                          >{#each row as cell}<td>{formatValue(cell)}</td>{/each}</tr
                        >
                      {/each}
                    </tbody>
                  </table>
                </div>
              {:else}
                <div class="unavailable">
                  No inline table preview. Use download to open the data.
                </div>
              {/if}
            {:else}
              <div class="unavailable">No native preview is available for this snapshot.</div>
            {/if}
          {/key}
        </div>

        <div class="timeline-controls">
          <div class="step-row">
            <strong>Step {rangeValue.step.toLocaleString()}</strong>
            <span>{rangeIndex + 1} of {group.values.length}</span>
          </div>
          <input
            type="range"
            min="0"
            max={Math.max(group.values.length - 1, 0)}
            step="1"
            value={rangeIndex}
            disabled={group.values.length < 2}
            aria-label={`Select ${group.key} ${group.kind} snapshot`}
            aria-valuetext={`Step ${rangeValue.step}, snapshot ${rangeIndex + 1} of ${group.values.length}`}
            oninput={(event) => scrub(group, event.currentTarget.valueAsNumber)}
            onchange={() => commitSelection(group)}
          />
          {#if metadataString(selected.value, "caption")}
            <p class="caption">{metadataString(selected.value, "caption")}</p>
          {/if}
        </div>
        {#if Object.keys(selected.value.metadata).length > 0}
          <details class="media-metadata">
            <summary>Metadata</summary>
            <div class="metadata-tree">
              <JsonTreeNode name="" value={selected.value.metadata} root />
            </div>
          </details>
        {/if}
      </section>
    {/each}
  </div>
{:else}
  <div class="empty-media">No media or rich data logged yet.</div>
{/if}

<style>
  .media-timeline {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(min(100%, 480px), 720px));
    gap: 18px;
  }

  .media-group {
    min-width: 0;
    padding-bottom: 14px;
    border-bottom: 1px solid var(--line);
  }

  .media-heading,
  .media-title,
  .step-row {
    display: flex;
    align-items: center;
  }

  .media-heading {
    min-height: 38px;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 8px;
  }

  .media-title {
    min-width: 0;
    gap: 8px;
  }

  .media-title strong {
    overflow: hidden;
    font-size: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .media-title small,
  .step-row span {
    flex: 0 0 auto;
    color: var(--muted);
    font-size: 10px;
  }

  .kind {
    flex: 0 0 auto;
    color: var(--accent);
    font-size: 9px;
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }

  .icon-button {
    width: 30px;
    height: 30px;
    flex: 0 0 auto;
    display: grid;
    place-items: center;
    border: 1px solid transparent;
    color: var(--muted);
    text-decoration: none;
  }

  .icon-button:hover {
    border-color: var(--line);
    background: var(--button-hover);
    color: var(--text);
  }

  .icon-button:focus-visible,
  input:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }

  .preview {
    min-height: 250px;
    display: grid;
    place-items: center;
    overflow: hidden;
    background: var(--surface);
  }

  .preview img,
  .preview video {
    width: 100%;
    max-height: 430px;
    display: block;
    object-fit: contain;
  }

  .preview audio {
    width: calc(100% - 24px);
  }

  .preview :global(canvas) {
    border: 0;
  }

  .table-preview {
    width: 100%;
    max-height: 360px;
    align-self: stretch;
    overflow: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 10px;
    white-space: nowrap;
  }

  th,
  td {
    padding: 7px 9px;
    border-bottom: 1px solid var(--line);
    text-align: left;
  }

  th {
    position: sticky;
    top: 0;
    background: var(--panel-alt);
  }

  .timeline-controls {
    padding-top: 9px;
  }

  .step-row {
    justify-content: space-between;
    gap: 12px;
  }

  .step-row strong {
    font-size: 11px;
  }

  input[type="range"] {
    width: 100%;
    margin: 8px 0 0;
    accent-color: var(--accent);
  }

  input[type="range"]:disabled {
    opacity: 0.45;
  }

  .caption {
    margin: 7px 0 0;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.5;
  }

  .media-metadata {
    margin-top: 8px;
    border-top: 1px solid var(--line);
  }

  .media-metadata summary {
    padding: 8px 0;
    color: var(--muted);
    cursor: pointer;
    font-size: 10px;
  }

  .metadata-tree {
    max-height: 240px;
    overflow: auto;
  }

  .unavailable,
  .empty-media {
    min-height: 160px;
    display: grid;
    place-content: center;
    padding: 20px;
    color: var(--muted);
    font-size: 11px;
    text-align: center;
  }

  @media (max-width: 620px) {
    .media-timeline {
      grid-template-columns: 1fr;
    }

    .media-title small {
      display: none;
    }
  }
</style>
