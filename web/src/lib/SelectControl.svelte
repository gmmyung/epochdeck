<script context="module" lang="ts">
  export type SelectOption = {
    value: string;
    label: string;
    disabled?: boolean;
  };

  let selectControlSequence = 0;
</script>

<script lang="ts">
  import { onMount, tick } from "svelte";

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
  const listboxId = `${controlId}-listbox`;
  let root: HTMLDivElement;
  let trigger: HTMLButtonElement;
  let listbox: HTMLDivElement;
  let open = false;
  let activeIndex = -1;
  let popoverStyle = "";
  let selectedOption: SelectOption | undefined;
  let unavailable = false;

  $: selectedOption = options.find((option) => option.value === value);
  $: unavailable = disabled || firstEnabledIndex() < 0;
  $: if (!open) activeIndex = selectedIndex();
  $: if (open && unavailable) open = false;

  onMount(() => {
    const pointerdown = (event: PointerEvent) => {
      if (open && !root.contains(event.target as Node)) close(false);
    };
    const reposition = () => {
      if (open) positionPopover();
    };
    document.addEventListener("pointerdown", pointerdown);
    document.addEventListener("scroll", reposition, true);
    window.addEventListener("resize", reposition);
    return () => {
      document.removeEventListener("pointerdown", pointerdown);
      document.removeEventListener("scroll", reposition, true);
      window.removeEventListener("resize", reposition);
    };
  });

  function selectedIndex(): number {
    const index = options.findIndex((option) => option.value === value && !option.disabled);
    return index >= 0 ? index : firstEnabledIndex();
  }

  function firstEnabledIndex(): number {
    return options.findIndex((option) => !option.disabled);
  }

  function lastEnabledIndex(): number {
    for (let index = options.length - 1; index >= 0; index -= 1) {
      if (!options[index].disabled) return index;
    }
    return -1;
  }

  function adjacentEnabledIndex(start: number, direction: 1 | -1): number {
    if (options.length === 0) return -1;
    for (let offset = 1; offset <= options.length; offset += 1) {
      const index = (start + direction * offset + options.length) % options.length;
      if (!options[index].disabled) return index;
    }
    return -1;
  }

  async function openMenu(preferredIndex = selectedIndex()): Promise<void> {
    if (unavailable) return;
    open = true;
    activeIndex = preferredIndex >= 0 ? preferredIndex : firstEnabledIndex();
    await tick();
    positionPopover();
    focusActiveOption();
  }

  function close(restoreFocus: boolean): void {
    open = false;
    if (restoreFocus) window.requestAnimationFrame(() => trigger?.focus());
  }

  function toggle(): void {
    if (open) close(false);
    else void openMenu();
  }

  function choose(index: number): void {
    const option = options[index];
    if (!option || option.disabled) return;
    value = option.value;
    onvaluechange(option.value);
    close(true);
  }

  function handleTriggerKeydown(event: KeyboardEvent): void {
    if (unavailable) return;
    if (event.key === "Escape" && open) {
      event.preventDefault();
      event.stopPropagation();
      close(true);
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      void openMenu(selectedIndex());
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      void openMenu(selectedIndex() >= 0 ? selectedIndex() : lastEnabledIndex());
    } else if (event.key === "Home") {
      event.preventDefault();
      void openMenu(firstEnabledIndex());
    } else if (event.key === "End") {
      event.preventDefault();
      void openMenu(lastEnabledIndex());
    }
  }

  function handleListboxKeydown(event: KeyboardEvent): void {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      close(true);
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      activeIndex = adjacentEnabledIndex(activeIndex, event.key === "ArrowDown" ? 1 : -1);
      focusActiveOption();
    } else if (event.key === "Home" || event.key === "End") {
      event.preventDefault();
      activeIndex = event.key === "Home" ? firstEnabledIndex() : lastEnabledIndex();
      focusActiveOption();
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      choose(activeIndex);
    } else if (event.key === "Tab") {
      window.setTimeout(() => close(false), 0);
    } else if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
      const query = event.key.toLocaleLowerCase();
      const match = options.findIndex(
        (option, index) =>
          index !== activeIndex &&
          !option.disabled &&
          option.label.toLocaleLowerCase().startsWith(query),
      );
      if (match >= 0) {
        event.preventDefault();
        activeIndex = match;
        focusActiveOption();
      }
    }
  }

  function focusActiveOption(): void {
    window.requestAnimationFrame(() => {
      const option = listbox?.querySelector<HTMLElement>(`[data-option-index="${activeIndex}"]`);
      option?.focus();
      option?.scrollIntoView?.({ block: "nearest" });
    });
  }

  function positionPopover(): void {
    if (!trigger) return;
    const rect = trigger.getBoundingClientRect();
    const viewportPadding = 8;
    const measurable = rect.width > 0 || rect.height > 0;
    if (
      measurable &&
      (rect.bottom < viewportPadding || rect.top > window.innerHeight - viewportPadding)
    ) {
      close(true);
      return;
    }
    const estimatedHeight = Math.min(options.length * 34 + 8, 264);
    const renderedHeight = listbox?.getBoundingClientRect().height || estimatedHeight;
    const desiredHeight = Math.min(Math.max(renderedHeight, 32), 264);
    const spaceBelow = window.innerHeight - rect.bottom - viewportPadding;
    const spaceAbove = rect.top - viewportPadding;
    const placeAbove = spaceBelow < Math.min(desiredHeight, 160) && spaceAbove > spaceBelow;
    const availableHeight = Math.max(96, placeAbove ? spaceAbove - 4 : spaceBelow - 4);
    const maxHeight = Math.min(264, availableHeight);
    const width = Math.min(
      Math.max(rect.width, 180),
      Math.max(180, window.innerWidth - viewportPadding * 2),
    );
    const left = Math.max(
      viewportPadding,
      Math.min(rect.left, window.innerWidth - width - viewportPadding),
    );
    const menuHeight = Math.min(desiredHeight, maxHeight);
    const top = placeAbove
      ? Math.max(viewportPadding, rect.top - menuHeight - 4)
      : Math.min(rect.bottom + 4, window.innerHeight - menuHeight - viewportPadding);
    popoverStyle = `top: ${Math.round(top)}px; left: ${Math.round(left)}px; width: ${Math.round(width)}px; max-height: ${Math.round(maxHeight)}px`;
  }
</script>

<div bind:this={root} class="select-control" class:compact class:fit>
  <span id={labelId} class="select-label">{ariaLabel}</span>
  <button
    bind:this={trigger}
    class="select-trigger"
    type="button"
    disabled={unavailable}
    aria-labelledby={`${labelId} ${valueId}`}
    aria-haspopup="listbox"
    aria-controls={listboxId}
    aria-expanded={open}
    onclick={toggle}
    onkeydown={handleTriggerKeydown}
  >
    <span id={valueId} title={selectedOption?.label ?? value}>{selectedOption?.label ?? value}</span
    >
    <span class="select-chevron" class:open aria-hidden="true">
      <Icon name="chevron-down" size={14} />
    </span>
  </button>
  {#if open}
    <div
      bind:this={listbox}
      id={listboxId}
      class="select-popover"
      role="listbox"
      tabindex="-1"
      aria-labelledby={labelId}
      style={popoverStyle}
      onkeydown={handleListboxKeydown}
    >
      {#each options as option, index (`${option.value}:${index}`)}
        <button
          type="button"
          role="option"
          class="select-option"
          class:active={index === activeIndex}
          aria-label={option.label}
          aria-selected={option.value === value}
          aria-disabled={option.disabled ?? false}
          disabled={option.disabled}
          tabindex={index === activeIndex ? 0 : -1}
          data-option-index={index}
          title={option.label}
          onmouseenter={() => {
            if (!option.disabled) activeIndex = index;
          }}
          onclick={() => choose(index)}
        >
          <span>{option.label}</span>
          {#if option.value === value}<Icon name="check" size={14} />{/if}
        </button>
      {/each}
    </div>
  {/if}
</div>

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

  .select-trigger {
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

  .compact .select-trigger {
    min-height: 30px;
    padding-left: 7px;
    font-size: 11px;
  }

  .select-trigger:hover:not(:disabled),
  .select-trigger[aria-expanded="true"] {
    border-color: var(--line-strong);
    background: var(--surface);
  }

  .select-trigger[aria-expanded="true"] {
    box-shadow: inset 0 -2px var(--accent);
  }

  .select-trigger:disabled {
    cursor: not-allowed;
    opacity: 0.55;
  }

  .select-trigger > span:first-child {
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

  .select-chevron.open {
    transform: rotate(180deg);
  }

  .select-popover {
    position: fixed;
    z-index: 2000;
    display: grid;
    gap: 2px;
    padding: 4px;
    overflow-y: auto;
    overscroll-behavior: contain;
    border: 1px solid var(--line-strong);
    background: var(--panel);
    box-shadow: 0 10px 30px rgb(0 0 0 / 20%);
  }

  .select-option {
    width: 100%;
    min-width: 0;
    min-height: 32px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) 18px;
    gap: 8px;
    align-items: center;
    padding: 6px 8px;
    border: 0;
    background: transparent;
    color: var(--text);
    text-align: left;
    font-size: 11px;
  }

  .select-option:hover,
  .select-option.active {
    background: var(--button-hover);
  }

  .select-option[aria-selected="true"] {
    box-shadow: inset 2px 0 var(--accent);
    background: var(--accent-bg);
    color: var(--accent-text);
    font-weight: 650;
  }

  .select-option:disabled {
    cursor: not-allowed;
    opacity: 0.45;
  }

  .select-option > span {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
