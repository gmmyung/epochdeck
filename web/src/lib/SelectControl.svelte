<script context="module" lang="ts">
  export type SelectOption = {
    value: string;
    label: string;
    disabled?: boolean;
  };

  let selectControlSequence = 0;
</script>

<script lang="ts">
  import { Select } from "bits-ui";

  import Icon from "./Icon.svelte";

  export let value: string;
  export let options: readonly SelectOption[];
  export let ariaLabel: string;
  export let disabled = false;
  export let compact = false;
  export let fit = false;
  export let onvaluechange: (value: string) => void = () => {};

  const controlId = `select-control-${++selectControlSequence}`;
  const labelId = `${controlId}-label`;
  const valueId = `${controlId}-value`;
  let open = false;
  let selectedOption: SelectOption | undefined;
  let unavailable = false;

  $: selectedOption = options.find((option) => option.value === value);
  $: unavailable = disabled || !options.some((option) => !option.disabled);
</script>

<Select.Root
  type="single"
  items={[...options]}
  bind:value
  bind:open
  disabled={unavailable}
  loop
  onValueChange={onvaluechange}
>
  <div class="select-control" class:compact class:fit>
    <span id={labelId} class="select-label">{ariaLabel}</span>
    <Select.Trigger
      class="select-trigger"
      aria-labelledby={`${labelId} ${valueId}`}
      title={selectedOption?.label ?? value}
    >
      <span id={valueId}>{selectedOption?.label ?? value}</span>
      <span class="select-chevron" aria-hidden="true">
        <Icon name="chevron-down" size={14} />
      </span>
    </Select.Trigger>
  </div>
  <Select.Portal>
    <Select.Content class="select-popover" sideOffset={4} collisionPadding={8}>
      <Select.Viewport class="select-viewport">
        {#each options as option (`${option.value}:${option.label}`)}
          <Select.Item
            class="select-option"
            value={option.value}
            label={option.label}
            disabled={option.disabled}
            aria-label={option.label}
            title={option.label}
          >
            <span>{option.label}</span>
            {#if option.value === value}<Icon name="check" size={14} />{/if}
          </Select.Item>
        {/each}
      </Select.Viewport>
    </Select.Content>
  </Select.Portal>
</Select.Root>

<style>
  .select-control {
    width: 100%;
    min-width: 0;
  }

  .select-label {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }

  .select-control.fit {
    width: auto;
    min-width: 150px;
  }

  :global(.select-trigger) {
    width: 100%;
    min-width: 0;
    min-height: 36px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) 18px;
    gap: 8px;
    align-items: center;
    padding: 0 8px 0 10px;
    border: 1px solid var(--line);
    background: var(--control-bg);
    color: var(--text);
    text-align: left;
    font-size: 12px;
    transition:
      border-color 120ms ease,
      background-color 120ms ease;
  }

  .compact :global(.select-trigger) {
    min-height: 30px;
    padding-left: 7px;
    font-size: 11px;
  }

  :global(.select-trigger:hover:not(:disabled)),
  :global(.select-trigger[data-state="open"]) {
    border-color: var(--line-strong);
    background: var(--surface);
  }

  :global(.select-trigger[data-state="open"]) {
    box-shadow: inset 0 -2px var(--accent);
  }

  :global(.select-trigger:disabled) {
    cursor: not-allowed;
    opacity: 0.55;
  }

  :global(.select-trigger) > span:first-child {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .select-chevron {
    display: grid;
    place-items: center;
    color: var(--muted);
    transition: transform 120ms ease;
  }

  :global(.select-trigger[data-state="open"]) .select-chevron {
    transform: rotate(180deg);
  }

  :global(.select-popover) {
    z-index: 2000;
    width: max(var(--bits-select-anchor-width), 180px);
    max-width: calc(100vw - 16px);
    max-height: min(264px, var(--bits-select-content-available-height));
    padding: 4px;
    overflow: hidden;
    border: 1px solid var(--line-strong);
    background: var(--panel);
    box-shadow: 0 10px 30px rgb(0 0 0 / 20%);
  }

  :global(.select-viewport) {
    max-height: inherit;
    display: grid;
    gap: 2px;
    overflow-y: auto;
    overscroll-behavior: contain;
  }

  :global(.select-option) {
    width: 100%;
    min-width: 0;
    min-height: 32px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) 18px;
    gap: 8px;
    align-items: center;
    padding: 6px 8px;
    outline: none;
    color: var(--text);
    font-size: 11px;
    cursor: default;
  }

  :global(.select-option[data-highlighted]) {
    background: var(--button-hover);
  }

  :global(.select-option[data-selected]) {
    box-shadow: inset 2px 0 var(--accent);
    background: var(--accent-bg);
    color: var(--accent-text);
    font-weight: 650;
  }

  :global(.select-option[data-disabled]) {
    opacity: 0.45;
  }

  :global(.select-option) > span {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
