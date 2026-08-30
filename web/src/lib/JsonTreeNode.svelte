<script lang="ts">
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";
  import { JSON_TREE_PAGE_SIZE, nodeChildCount, visibleChildEntries } from "./json-tree";

  export let name: string;
  export let value: unknown;
  export let depth = 0;
  export let root = false;

  let expanded = depth < 2 && nodeChildCount(value) <= JSON_TREE_PAGE_SIZE;
  let childOffset = 0;
  let copyStatus: "idle" | "copied" | "failed" = "idle";
  let observedValue = value;

  $: branch = isBranch(value);
  $: if (value !== observedValue) {
    observedValue = value;
    childOffset = 0;
    expanded = depth < 2 && nodeChildCount(value) <= JSON_TREE_PAGE_SIZE;
  }
  $: totalChildren = branch ? nodeChildCount(value) : 0;
  $: children =
    branch && expanded
      ? visibleChildEntries(
          value as Record<string, unknown> | unknown[],
          JSON_TREE_PAGE_SIZE,
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
    if (candidate === null) return "null";
    if (typeof candidate === "string") return candidate;
    if (typeof candidate === "number") {
      return candidate.toLocaleString(undefined, { maximumFractionDigits: 8 });
    }
    return String(candidate);
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
      {#if name}<span class="tree-key">{name}</span>{/if}
      <span class="tree-kind">{kind} · {totalChildren.toLocaleString()}</span>
    </button>
    {#if expanded}
      <div class="tree-children">
        {#each children as [childName, childValue] (childName)}
          <JsonTreeNode name={childName} value={childValue} depth={depth + 1} />
        {/each}
        {#if totalChildren > JSON_TREE_PAGE_SIZE}
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
                totalChildren,
              ).toLocaleString()} of {totalChildren.toLocaleString()}
            </span>
            <button
              type="button"
              disabled={childOffset + JSON_TREE_PAGE_SIZE >= totalChildren}
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
    <span class="tree-key">{name}</span>
    <button
      type="button"
      class={`tree-value tree-${kind}`}
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
</style>
