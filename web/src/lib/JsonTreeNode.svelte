<script lang="ts">
  import Icon from "./Icon.svelte";
  import JsonTreeNode from "./JsonTreeNode.svelte";

  export let name: string;
  export let value: unknown;
  export let depth = 0;
  export let root = false;

  let expanded = depth < 2;
  let copied = false;

  $: branch = isBranch(value);
  $: children = branch ? childEntries(value as Record<string, unknown> | unknown[]) : [];
  $: kind = Array.isArray(value) ? "array" : branch ? "object" : valueKind(value);

  function isBranch(candidate: unknown): candidate is Record<string, unknown> | unknown[] {
    return typeof candidate === "object" && candidate !== null;
  }

  function childEntries(candidate: Record<string, unknown> | unknown[]): Array<[string, unknown]> {
    return Array.isArray(candidate)
      ? candidate.map((child, index) => [String(index), child])
      : Object.entries(candidate);
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

  function copyLeaf(): void {
    void navigator.clipboard?.writeText(leafValue(value));
    copied = true;
    window.setTimeout(() => (copied = false), 1_200);
  }
</script>

{#if branch}
  <div class:tree-root={root} class="tree-node tree-branch" style={`--tree-depth: ${depth}`}>
    <button
      type="button"
      class="tree-toggle"
      aria-expanded={expanded}
      onclick={() => (expanded = !expanded)}
    >
      <Icon name={expanded ? "chevron-down" : "chevron-right"} size={13} />
      {#if name}<span class="tree-key">{name}</span>{/if}
      <span class="tree-kind">{kind} · {children.length}</span>
    </button>
    {#if expanded}
      <div class="tree-children">
        {#each children as [childName, childValue] (childName)}
          <JsonTreeNode name={childName} value={childValue} depth={depth + 1} />
        {/each}
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
      aria-label={copied ? `Copied ${name}` : `Copy ${name}`}
      onclick={copyLeaf}>{leafValue(value)}</button
    >
  </div>
{/if}
