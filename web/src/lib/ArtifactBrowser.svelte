<script lang="ts">
  import { onMount } from "svelte";

  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import { artifactArchiveUrl, artifactFileUrl, type Artifact, type ArtifactEntry } from "./api";
  import {
    ARTIFACT_ITEM_PAGE_SIZE,
    artifactBreadcrumbs,
    artifactDirectoryItems,
    artifactItemPage,
    artifactTotalSize,
    type ArtifactBrowserItem,
    type ArtifactBreadcrumb,
  } from "./artifact-browser";
  import type { SelectedRunArtifact } from "./run-resources";

  export let artifacts: SelectedRunArtifact[];
  export let runNames: Record<string, string> = {};
  export let details: Record<string, Artifact> = {};
  export let detailLoading = new Set<string>();
  export let detailErrors: Record<string, string> = {};
  export let onselect: (artifactId: string) => void = () => {};

  let activeKey = "";
  let orderedArtifacts: SelectedRunArtifact[] = [];
  let previousActiveKey = "";
  let currentDirectory = "";
  let tabRail: HTMLElement;
  let selectedLink: SelectedRunArtifact | undefined;
  let selectedArtifact: Artifact | undefined;
  let items: ArtifactBrowserItem[] = [];
  let breadcrumbs: ArtifactBreadcrumb[] = [];
  let selectedEntry: ArtifactEntry | undefined;
  let narrowLayout = false;
  let visibleItemOffset = 0;
  let itemLocation = "";
  let visibleItems: ArtifactBrowserItem[] = [];
  const requestedArtifactIds = new Set<string>();

  $: orderedArtifacts = [...artifacts].sort(compareArtifactLinks);
  $: {
    const visibleIds = new Set(artifacts.map((linked) => linked.artifact.id));
    for (const artifactId of requestedArtifactIds) {
      if (
        !visibleIds.has(artifactId) ||
        details[artifactId] ||
        detailErrors[artifactId] ||
        !detailLoading.has(artifactId)
      ) {
        requestedArtifactIds.delete(artifactId);
      }
    }
  }
  $: if (orderedArtifacts.length === 0) activeKey = "";
  $: if (
    orderedArtifacts.length > 0 &&
    !orderedArtifacts.some((linked) => linkKey(linked) === activeKey)
  ) {
    activeKey = linkKey(orderedArtifacts[0]);
  }
  $: selectedLink = orderedArtifacts.find((linked) => linkKey(linked) === activeKey);
  $: selectedArtifact = selectedLink ? details[selectedLink.artifact.id] : undefined;
  $: if (selectedLink) requestDetail(selectedLink.artifact.id);
  $: if (activeKey !== previousActiveKey) {
    previousActiveKey = activeKey;
    currentDirectory = "";
    selectedEntry = undefined;
  }
  $: items = selectedArtifact
    ? artifactDirectoryItems(selectedArtifact.entries, currentDirectory)
    : [];
  $: breadcrumbs = artifactBreadcrumbs(currentDirectory);
  $: if (`${activeKey}\0${currentDirectory}` !== itemLocation) {
    itemLocation = `${activeKey}\0${currentDirectory}`;
    visibleItemOffset = 0;
    selectedEntry = undefined;
  }
  $: visibleItems = artifactItemPage(items, ARTIFACT_ITEM_PAGE_SIZE, visibleItemOffset);

  onMount(() => {
    const media = window.matchMedia("(max-width: 760px)");
    const update = () => (narrowLayout = media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  });

  function linkKey(linked: SelectedRunArtifact): string {
    return linked.artifact.id;
  }

  function tabId(linked: SelectedRunArtifact): string {
    return `artifact-tab-${linked.artifact.id}`;
  }

  function chooseArtifact(linked: SelectedRunArtifact): void {
    activeKey = linkKey(linked);
  }

  function requestDetail(artifactId: string): void {
    if (
      details[artifactId] ||
      detailLoading.has(artifactId) ||
      requestedArtifactIds.has(artifactId)
    ) {
      return;
    }
    requestedArtifactIds.add(artifactId);
    onselect(artifactId);
  }

  function handleTabKey(event: KeyboardEvent, index: number): void {
    let nextIndex: number | undefined;
    if (["ArrowDown", "ArrowRight"].includes(event.key)) {
      nextIndex = (index + 1) % orderedArtifacts.length;
    } else if (["ArrowUp", "ArrowLeft"].includes(event.key)) {
      nextIndex = (index - 1 + orderedArtifacts.length) % orderedArtifacts.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = orderedArtifacts.length - 1;
    }
    if (nextIndex === undefined) return;
    event.preventDefault();
    chooseArtifact(orderedArtifacts[nextIndex]);
    queueMicrotask(() => {
      const tabs = tabRail.querySelectorAll<HTMLElement>("[role='tab']");
      tabs[nextIndex]?.focus();
    });
  }

  function openItem(item: ArtifactBrowserItem): void {
    if (item.kind === "directory") {
      currentDirectory = item.path;
      selectedEntry = undefined;
    }
  }

  function changeItemPage(offset: number): void {
    visibleItemOffset = Math.max(0, offset);
    selectedEntry = undefined;
  }

  function formatBytes(value: number): string {
    if (value < 1024) return `${value.toLocaleString()} B`;
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let size = value;
    let unit = -1;
    do {
      size /= 1024;
      unit += 1;
    } while (size >= 1024 && unit < units.length - 1);
    return `${size.toLocaleString(undefined, { maximumFractionDigits: 1 })} ${units[unit]}`;
  }

  function archiveName(artifact: Pick<Artifact, "name" | "version">): string {
    return `${artifact.name}-v${artifact.version}.zip`;
  }

  function compareArtifactLinks(left: SelectedRunArtifact, right: SelectedRunArtifact): number {
    return (
      right.artifact.name.localeCompare(left.artifact.name, undefined, { numeric: true }) ||
      right.artifact.version - left.artifact.version ||
      right.artifact.created_at.localeCompare(left.artifact.created_at)
    );
  }

  function relations(linked: SelectedRunArtifact): string {
    return [...new Set(linked.links.map((link) => link.relation))].join(" + ");
  }

  function linkedRuns(linked: SelectedRunArtifact): string {
    const names = [...new Set(linked.links.map((link) => runNames[link.runId] ?? link.runId))];
    if (names.length <= 2) return names.join(", ");
    return `${names.slice(0, 2).join(", ")} +${names.length - 2}`;
  }
</script>

{#if orderedArtifacts.length > 0 && selectedLink}
  <div class="artifact-browser">
    <div
      class="artifact-tabs"
      role="tablist"
      aria-label="Run artifacts"
      aria-orientation={narrowLayout ? "horizontal" : "vertical"}
      bind:this={tabRail}
    >
      {#each orderedArtifacts as linked, index (linkKey(linked))}
        <button
          id={tabId(linked)}
          type="button"
          role="tab"
          aria-selected={linkKey(linked) === activeKey}
          aria-controls="artifact-panel"
          tabindex={linkKey(linked) === activeKey ? 0 : -1}
          class:active={linkKey(linked) === activeKey}
          onclick={() => chooseArtifact(linked)}
          onkeydown={(event) => handleTabKey(event, index)}
        >
          <Icon name="archive" size={15} />
          <span>
            <strong>{linked.artifact.name}:v{linked.artifact.version}</strong>
            <small>{relations(linked)} · {linkedRuns(linked)}</small>
          </span>
        </button>
      {/each}
    </div>

    <div
      class="artifact-panel"
      id="artifact-panel"
      role="tabpanel"
      aria-labelledby={tabId(selectedLink)}
    >
      <header class="artifact-heading">
        <div>
          <div class="artifact-title">
            <strong>{selectedLink.artifact.name}:v{selectedLink.artifact.version}</strong>
            <span>{relations(selectedLink)}</span>
            <span>{selectedLink.artifact.type}</span>
          </div>
          <p>
            {selectedLink.artifact.entry_count.toLocaleString()} files · {linkedRuns(
              selectedLink,
            )}{selectedArtifact
              ? ` · ${formatBytes(artifactTotalSize(selectedArtifact.entries))}`
              : ""}
          </p>
        </div>
        <a
          class="icon-button"
          href={artifactArchiveUrl(selectedLink.artifact.id)}
          download={archiveName(selectedLink.artifact)}
          aria-label={`Download ${selectedLink.artifact.name} version ${selectedLink.artifact.version} as ZIP`}
        >
          <Icon name="download" size={17} />
        </a>
      </header>

      {#if detailErrors[selectedLink.artifact.id]}
        <div class="resource-error" role="alert">
          <span>{detailErrors[selectedLink.artifact.id]}</span>
          <button type="button" onclick={() => onselect(selectedLink.artifact.id)}
            >Retry manifest</button
          >
        </div>
      {:else if detailLoading.has(selectedLink.artifact.id) && !selectedArtifact}
        <div class="artifact-loading" role="status">Loading artifact manifest…</div>
      {/if}

      {#if selectedArtifact && selectedArtifact.aliases.length > 0}
        <div class="aliases" aria-label="Artifact aliases">
          {#each selectedArtifact.aliases as alias}<span>{alias}</span>{/each}
        </div>
      {/if}
      {#if selectedArtifact?.description}
        <p class="description">{selectedArtifact.description}</p>
      {/if}
      {#if selectedArtifact && Object.keys(selectedArtifact.metadata).length > 0}
        <details class="artifact-metadata">
          <summary>Metadata</summary>
          <div class="metadata-tree">
            <JsonTreeNode name="" value={selectedArtifact.metadata} root />
          </div>
        </details>
      {/if}

      {#if selectedArtifact}
        <nav class="breadcrumbs" aria-label="Artifact file path">
          {#each breadcrumbs as breadcrumb, index (breadcrumb.path)}
            {#if index > 0}<Icon name="chevron-right" size={12} />{/if}
            <button
              type="button"
              aria-current={index === breadcrumbs.length - 1 ? "page" : undefined}
              onclick={() => (currentDirectory = breadcrumb.path)}
            >
              {breadcrumb.label}
            </button>
          {/each}
        </nav>

        <div class="file-table-wrap">
          <table class="file-table">
            <thead>
              <tr><th>Name</th><th>Size</th><th><span class="sr-only">Actions</span></th></tr>
            </thead>
            <tbody>
              {#each visibleItems as item (item.path)}
                <tr>
                  <td>
                    {#if item.kind === "directory"}
                      <button
                        class="file-name directory"
                        type="button"
                        onclick={() => openItem(item)}
                      >
                        <Icon name="folder" size={16} />
                        <span>{item.name}</span>
                        <small>{item.fileCount.toLocaleString()} files</small>
                      </button>
                    {:else}
                      <button
                        class="file-name file"
                        class:active={selectedEntry?.path === item.entry.path}
                        type="button"
                        aria-pressed={selectedEntry?.path === item.entry.path}
                        onclick={() => (selectedEntry = item.entry)}
                      >
                        <Icon name="file" size={16} />
                        <span>{item.name}</span>
                      </button>
                    {/if}
                  </td>
                  <td class="size">{formatBytes(item.size)}</td>
                  <td class="actions">
                    {#if item.kind === "file"}
                      <a
                        class="icon-button file-download"
                        href={artifactFileUrl(selectedLink.artifact.id, item.entry.path)}
                        download={item.entry.blob.file_name ?? item.name}
                        aria-label={`Download ${item.entry.path}`}
                      >
                        <Icon name="download" size={15} />
                      </a>
                    {/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
          {#if items.length === 0}
            <div class="empty-directory">This directory is empty.</div>
          {:else if items.length > ARTIFACT_ITEM_PAGE_SIZE}
            <nav class="file-pagination" aria-label="Artifact file pages">
              <button
                type="button"
                disabled={visibleItemOffset === 0}
                onclick={() => changeItemPage(visibleItemOffset - ARTIFACT_ITEM_PAGE_SIZE)}
                >Previous</button
              >
              <span>
                {(visibleItemOffset + 1).toLocaleString()}–{Math.min(
                  visibleItemOffset + visibleItems.length,
                  items.length,
                ).toLocaleString()} of {items.length.toLocaleString()}
              </span>
              <button
                type="button"
                disabled={visibleItemOffset + ARTIFACT_ITEM_PAGE_SIZE >= items.length}
                onclick={() => changeItemPage(visibleItemOffset + ARTIFACT_ITEM_PAGE_SIZE)}
                >Next</button
              >
            </nav>
          {/if}
        </div>
        {#if selectedEntry}
          <div class="file-details">
            <strong>{selectedEntry.path}</strong>
            <span>{selectedEntry.blob.mime_type}</span>
            <span>{formatBytes(selectedEntry.blob.size)}</span>
            <code>{selectedEntry.blob.digest}</code>
          </div>
        {/if}
      {/if}
    </div>
  </div>
{:else}
  <div class="empty-artifacts">No artifacts linked to the selected runs.</div>
{/if}

<style>
  .artifact-browser {
    min-width: 0;
    display: grid;
    grid-template-columns: minmax(220px, 290px) minmax(0, 1fr);
    border-top: 1px solid var(--line);
  }

  .artifact-tabs {
    max-height: min(72vh, 760px);
    overflow: auto;
    border-right: 1px solid var(--line);
  }

  .artifact-tabs button {
    width: 100%;
    min-width: 0;
    display: grid;
    grid-template-columns: auto minmax(0, 1fr);
    gap: 9px;
    align-items: start;
    padding: 10px 12px;
    border: 0;
    border-bottom: 1px solid var(--line);
    background: transparent;
    color: var(--muted);
    text-align: left;
  }

  .artifact-tabs button:hover {
    background: var(--button-hover);
    color: var(--text);
  }

  .artifact-tabs button.active {
    box-shadow: inset 3px 0 var(--accent);
    background: var(--accent-bg);
    color: var(--accent-text);
  }

  .artifact-tabs button:focus-visible,
  .breadcrumbs button:focus-visible,
  .file-name.directory:focus-visible,
  .icon-button:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }

  .artifact-tabs span {
    min-width: 0;
    display: grid;
    gap: 3px;
  }

  .artifact-tabs strong,
  .artifact-tabs small {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .artifact-tabs strong {
    font-size: 11px;
  }

  .artifact-tabs small {
    font-size: 11px;
  }

  .artifact-panel {
    min-width: 0;
    padding: 14px 16px 18px;
  }

  .artifact-heading,
  .artifact-title,
  .breadcrumbs,
  .aliases,
  .file-name {
    display: flex;
    align-items: center;
  }

  .artifact-heading {
    justify-content: space-between;
    gap: 16px;
  }

  .artifact-heading > div {
    min-width: 0;
  }

  .artifact-title {
    flex-wrap: wrap;
    gap: 7px;
  }

  .artifact-title strong {
    font-size: 14px;
    overflow-wrap: anywhere;
  }

  .artifact-title span,
  .aliases span {
    color: var(--muted);
    font-size: 11px;
    letter-spacing: 0.03em;
    text-transform: uppercase;
  }

  .artifact-heading p,
  .description {
    margin: 5px 0 0;
    color: var(--muted);
    font-size: 10px;
  }

  .artifact-metadata {
    margin-top: 9px;
    border-top: 1px solid var(--line);
  }

  .artifact-metadata summary {
    padding: 8px 0;
    color: var(--muted);
    cursor: pointer;
    font-size: 10px;
  }

  .metadata-tree {
    max-height: 240px;
    overflow: auto;
  }

  .description {
    line-height: 1.5;
  }

  .aliases {
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 10px;
  }

  .aliases span {
    padding-right: 8px;
    border-right: 1px solid var(--line);
  }

  .icon-button {
    width: 32px;
    height: 32px;
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

  .breadcrumbs {
    min-height: 36px;
    gap: 2px;
    margin-top: 12px;
    overflow-x: auto;
    border-bottom: 1px solid var(--line);
  }

  .breadcrumbs button {
    min-height: 28px;
    padding: 0 6px;
    border: 0;
    background: transparent;
    color: var(--muted);
    font-size: 10px;
    white-space: nowrap;
  }

  .breadcrumbs button:hover,
  .breadcrumbs button[aria-current="page"] {
    color: var(--text);
  }

  .file-table-wrap {
    max-height: min(62vh, 680px);
    overflow: auto;
  }

  .file-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 10px;
  }

  .file-table th,
  .file-table td {
    height: 38px;
    padding: 4px 8px;
    border-bottom: 1px solid var(--line);
    text-align: left;
  }

  .file-table th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--panel);
    color: var(--muted);
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
  }

  .file-table th:nth-child(2),
  .file-table td.size {
    width: 110px;
    text-align: right;
    white-space: nowrap;
  }

  .file-table th:last-child,
  .file-table td.actions {
    width: 42px;
    text-align: right;
  }

  .file-name {
    min-width: 0;
    gap: 8px;
    color: var(--text);
  }

  .file-name > span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .file-name small {
    flex: 0 0 auto;
    color: var(--muted);
    font-size: 11px;
  }

  .file-name.directory {
    max-width: 100%;
    padding: 4px 2px;
    border: 0;
    background: transparent;
    font: inherit;
    text-align: left;
  }

  .file-name.file {
    width: 100%;
    padding: 4px 2px;
    border: 0;
    background: transparent;
    font: inherit;
    text-align: left;
  }

  .file-name.file:hover,
  .file-name.file.active {
    color: var(--accent);
  }

  .file-name.directory:hover {
    color: var(--accent);
  }

  .file-download {
    width: 28px;
    height: 28px;
    margin-left: auto;
  }

  .empty-directory,
  .empty-artifacts {
    min-height: 160px;
    display: grid;
    place-content: center;
    padding: 20px;
    color: var(--muted);
    font-size: 11px;
  }

  .file-pagination {
    min-height: 36px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    margin: 10px auto;
    color: var(--muted);
    font-size: 11px;
  }

  .file-pagination button {
    padding: 0 12px;
    border: 1px solid var(--line);
    background: transparent;
    color: var(--muted);
    font-size: 11px;
  }

  .file-details {
    min-width: 0;
    display: flex;
    flex-wrap: wrap;
    gap: 6px 12px;
    align-items: baseline;
    padding: 10px 8px;
    border-top: 1px solid var(--line);
    color: var(--muted);
    font-size: 11px;
  }

  .file-details strong {
    color: var(--text);
    font-size: 10px;
  }

  .file-details code {
    min-width: 0;
    overflow: hidden;
    color: var(--faint);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }

  @media (max-width: 760px) {
    .artifact-browser {
      grid-template-columns: 1fr;
    }

    .artifact-tabs {
      display: grid;
      grid-auto-columns: minmax(210px, 1fr);
      grid-auto-flow: column;
      max-height: none;
      overflow-x: auto;
      border-right: 0;
      border-bottom: 1px solid var(--line);
    }

    .artifact-tabs button {
      border-right: 1px solid var(--line);
      border-bottom: 0;
    }

    .file-table th:nth-child(2),
    .file-table td.size {
      width: 86px;
    }
  }
</style>
