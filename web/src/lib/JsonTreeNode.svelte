<script lang="ts">
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import {
    JSON_TREE_PAGE_SIZE,
    jsonTreeScalarText,
    normalizeJsonTreeSearch,
    nodeChildCount,
    searchJsonTree,
    visibleChildEntries,
    type JsonTreeSearchMatch,
  } from "./json-tree";

  export let name: string;
  export let value: unknown;
  export let depth = 0;
  export let root = false;
  export let searchQuery = "";
  export let searchResult: JsonTreeSearchMatch | undefined = undefined;

  const initialSearchQuery = normalizeJsonTreeSearch(searchQuery);
  let expanded =
    initialSearchQuery.length > 0 || (depth < 2 && nodeChildCount(value) <= JSON_TREE_PAGE_SIZE);
  let childOffset = 0;
  let copyStatus: "idle" | "copied" | "failed" = "idle";
  let observedValue = value;
  let observedSearchQuery = initialSearchQuery;

  $: branch = isBranch(value);
  $: normalizedSearchQuery = normalizeJsonTreeSearch(searchQuery);
  $: resolvedSearchResult =
    normalizedSearchQuery.length > 0
      ? (searchResult ?? searchJsonTree(value, normalizedSearchQuery, name) ?? undefined)
      : undefined;
  $: searchActive = normalizedSearchQuery.length > 0;
  $: if (value !== observedValue) {
    observedValue = value;
    childOffset = 0;
    expanded =
      normalizedSearchQuery.length > 0 ||
      (depth < 2 && nodeChildCount(value) <= JSON_TREE_PAGE_SIZE);
  }
  $: if (normalizedSearchQuery !== observedSearchQuery) {
    observedSearchQuery = normalizedSearchQuery;
    childOffset = 0;
    expanded =
      normalizedSearchQuery.length > 0 ||
      (depth < 2 && nodeChildCount(value) <= JSON_TREE_PAGE_SIZE);
  }
  $: totalChildren = branch ? nodeChildCount(value) : 0;
  $: displayedChildCount =
    searchActive && !resolvedSearchResult?.keyMatches
      ? (resolvedSearchResult?.children.length ?? 0)
      : totalChildren;
  $: children =
    branch && expanded
      ? childEntries(
          value as Record<string, unknown> | unknown[],
          resolvedSearchResult,
          searchActive,
          childOffset,
        )
      : [];
  $: kind = Array.isArray(value) ? "array" : branch ? "object" : valueKind(value);

  function isBranch(candidate: unknown): candidate is Record<string, unknown> | unknown[] {
    return typeof candidate === "object" && candidate !== null;
  }

  function valueKind(candidate: unknown): string {
    if (candidate === null) return "null";
    return typeof candidate;
  }

  function leafValue(candidate: unknown): string {
    return jsonTreeScalarText(candidate);
  }

  function childEntries(
    candidate: Record<string, unknown> | unknown[],
    match: JsonTreeSearchMatch | undefined,
    searching: boolean,
    offset: number,
  ): Array<{ name: string; value: unknown; searchResult: JsonTreeSearchMatch | undefined }> {
    if (searching && !match?.keyMatches) {
      return (match?.children ?? []).slice(offset, offset + JSON_TREE_PAGE_SIZE).map((child) => ({
        name: child.name,
        value: child.value,
        searchResult: child.match,
      }));
    }

    const matchingChildren = new Map(
      (match?.children ?? []).map((child) => [child.name, child.match]),
    );
    return visibleChildEntries(candidate, JSON_TREE_PAGE_SIZE, offset).map(
      ([childName, childValue]) => ({
        name: childName,
        value: childValue,
        searchResult: matchingChildren.get(childName),
      }),
    );
  }

  async function copyLeaf(): Promise<void> {
    try {
      if (!navigator.clipboard) throw new Error("Clipboard access is unavailable");
      await navigator.clipboard.writeText(leafValue(value));
      copyStatus = "copied";
    } catch {
      copyStatus = "failed";
    }
    window.setTimeout(() => (copyStatus = "idle"), 1_200);
  }
</script>

{#if branch}
  <div class:tree-root={root} class="tree-node tree-branch" style={`--tree-depth: ${depth}`}>
    <button
      type="button"
      class="tree-toggle"
      aria-expanded={expanded}
      onclick={() => {
        expanded = !expanded;
        if (!expanded) childOffset = 0;
      }}
    >
      <Icon name={expanded ? "chevron-down" : "chevron-right"} size={13} />
      {#if name}
        <span class="tree-key" class:tree-search-match={resolvedSearchResult?.keyMatches}
          >{name}</span
        >
      {/if}
      <span class="tree-kind">{kind} · {totalChildren.toLocaleString()}</span>
    </button>
    {#if expanded}
      <div class="tree-children">
        {#each children as child (child.name)}
          <JsonTreeNode
            name={child.name}
            value={child.value}
            depth={depth + 1}
            searchQuery={child.searchResult ? normalizedSearchQuery : ""}
            searchResult={child.searchResult}
          />
        {/each}
        {#if displayedChildCount > JSON_TREE_PAGE_SIZE}
          <nav class="tree-pagination" aria-label={`${name || "Root"} child pages`}>
            <button
              type="button"
              disabled={childOffset === 0}
              onclick={() => (childOffset = Math.max(0, childOffset - JSON_TREE_PAGE_SIZE))}
              >Previous</button
            >
            <span>
              {(childOffset + 1).toLocaleString()}–{Math.min(
                childOffset + JSON_TREE_PAGE_SIZE,
                displayedChildCount,
              ).toLocaleString()} of {displayedChildCount.toLocaleString()}
            </span>
            <button
              type="button"
              disabled={childOffset + JSON_TREE_PAGE_SIZE >= displayedChildCount}
              onclick={() => (childOffset += JSON_TREE_PAGE_SIZE)}>Next</button
            >
          </nav>
        {/if}
      </div>
    {/if}
  </div>
{:else}
  <div class="tree-node tree-leaf" style={`--tree-depth: ${depth}`}>
    <span class="tree-spacer"></span>
    <span class="tree-key" class:tree-search-match={resolvedSearchResult?.keyMatches}>{name}</span>
    <button
      type="button"
      class={`tree-value tree-${kind}`}
      class:tree-search-match={resolvedSearchResult?.valueMatches}
      aria-label={copyStatus === "copied"
        ? `Copied ${name}`
        : copyStatus === "failed"
          ? `Could not copy ${name}`
          : `Copy ${name}`}
      onclick={() => void copyLeaf()}>{leafValue(value)}</button
    >
    {#if copyStatus !== "idle"}
      <span class="copy-status" role="status">
        {copyStatus === "copied" ? "Copied" : "Copy failed"}
      </span>
    {/if}
  </div>
{/if}

<style>
  .tree-pagination {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 3px 0 5px calc((var(--tree-depth, 0) + 1) * 18px);
    color: var(--muted);
    font-size: 11px;
  }

  .tree-pagination button {
    padding: 5px 8px;
    border: 1px solid var(--line);
    background: transparent;
    color: var(--muted);
    font-size: 11px;
  }

  .copy-status {
    margin-left: 8px;
    color: var(--muted);
    font-size: 11px;
  }

  .tree-search-match {
    background: var(--accent-bg);
    box-shadow: 0 0 0 2px var(--accent-bg);
    color: var(--accent-text);
  }
</style>
