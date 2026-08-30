<script lang="ts">
  import type { Run } from "./api";
  import Icon from "./Icon.svelte";
  import { RUN_TABS, type RunTab } from "./run-resources";

  export let run: Run;
  export let activeTab: RunTab;
  export let countLabel: (tab: RunTab) => string;
  export let onselect: (tab: RunTab) => void;

  function handleKey(event: KeyboardEvent, index: number): void {
    let nextIndex: number | null = null;
    if (event.key === "ArrowRight") nextIndex = (index + 1) % RUN_TABS.length;
    else if (event.key === "ArrowLeft") nextIndex = (index - 1 + RUN_TABS.length) % RUN_TABS.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = RUN_TABS.length - 1;
    if (nextIndex === null) return;
    event.preventDefault();
    const next = RUN_TABS[nextIndex];
    onselect(next.id);
    queueMicrotask(() => document.getElementById(`run-tab-${next.id}`)?.focus());
  }
</script>

<div class="run-heading">
  <div>
    <p class="eyebrow">{run.project} / primary run / {run.id.slice(0, 8)}</p>
    <h1>{run.name}</h1>
  </div>
  <span
    class="run-state"
    class:live={run.state === "running"}
    class:finished={run.state === "finished"}
  >
    <Icon name={run.state === "running" ? "activity" : "check"} size={14} />
    {run.state}
  </span>
</div>

<div class="run-tabs" role="tablist" aria-label="Run data">
  {#each RUN_TABS as tab, index (tab.id)}
    <button
      id={`run-tab-${tab.id}`}
      type="button"
      role="tab"
      aria-selected={activeTab === tab.id}
      aria-controls={`run-panel-${tab.id}`}
      tabindex={activeTab === tab.id ? 0 : -1}
      class:active={activeTab === tab.id}
      onclick={() => onselect(tab.id)}
      onkeydown={(event) => handleKey(event, index)}
    >
      <Icon name={tab.icon} size={15} />
      <span>{tab.label}</span>
      <small>{countLabel(tab.id)}</small>
    </button>
  {/each}
</div>
