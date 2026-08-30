<script lang="ts">
  import { onMount } from "svelte";

  import {
    MAX_GAUSSIAN_SIGMA,
    MAX_SMOOTHING_WINDOW,
    type ScaleMode,
    type SmoothingMode,
  } from "./chart-data";
  import type { XAlignment } from "./chart-series";
  import Icon from "./Icon.svelte";
  import SelectControl from "./SelectControl.svelte";

  const DISPLAY_OPTIONS = [
    { value: "band", label: "Band" },
    { value: "line", label: "Line" },
  ];
  const SMOOTHING_OPTIONS = [
    { value: "none", label: "None" },
    { value: "time-ema", label: "Time-weighted EMA" },
    { value: "running", label: "Running average" },
    { value: "gaussian", label: "Gaussian" },
    { value: "ema", label: "EMA" },
  ];
  const SCALE_OPTIONS = [
    { value: "linear", label: "Linear" },
    { value: "log", label: "Log" },
  ];

  export let open = false;
  export let displayMode: "band" | "line";
  export let smoothingMode: SmoothingMode;
  export let smoothingAmount: number;
  export let xAlignment: XAlignment;
  export let xScale: ScaleMode;
  export let yScale: ScaleMode;
  export let xMinimum: string;
  export let xMaximum: string;
  export let yMinimum: string;
  export let yMaximum: string;
  export let axisWarning: string | null;
  export let onviewchange: () => void;

  let settings: HTMLDetailsElement;
  let summary: HTMLElement;

  onMount(() => {
    const keydown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || !open) return;
      if (
        event.target instanceof Element &&
        settings.contains(event.target) &&
        event.target.closest('[role="listbox"]')
      ) {
        return;
      }
      event.preventDefault();
      event.stopImmediatePropagation();
      close(true);
    };
    const pointerdown = (event: PointerEvent) => {
      if (open && !settings.contains(event.target as Node)) close(false);
    };
    document.addEventListener("keydown", keydown, true);
    document.addEventListener("pointerdown", pointerdown);
    return () => {
      document.removeEventListener("keydown", keydown, true);
      document.removeEventListener("pointerdown", pointerdown);
    };
  });

  function close(restoreFocus: boolean): void {
    open = false;
    if (restoreFocus) window.requestAnimationFrame(() => summary?.focus());
  }

  function changeSmoothing(mode: SmoothingMode): void {
    smoothingMode = mode;
    if (mode === "ema") smoothingAmount = 0.15;
    else if (mode === "time-ema") smoothingAmount = 25;
    else if (mode === "running") smoothingAmount = 20;
    else if (mode === "gaussian") smoothingAmount = 2;
  }

  function normalizeSmoothingAmount(): void {
    if (!Number.isFinite(smoothingAmount)) {
      smoothingAmount = smoothingMode === "ema" ? 0.15 : smoothingMode === "gaussian" ? 2 : 20;
    }
    const minimum = smoothingMode === "ema" ? 0.001 : 1;
    const maximum =
      smoothingMode === "ema"
        ? 1
        : smoothingMode === "gaussian"
          ? MAX_GAUSSIAN_SIGMA
          : MAX_SMOOTHING_WINDOW;
    smoothingAmount = Math.max(minimum, Math.min(maximum, smoothingAmount));
  }

  function smoothingAmountLabel(mode: SmoothingMode): string {
    if (mode === "time-ema") return "Time constant (seconds)";
    if (mode === "running") return "Window (points)";
    if (mode === "gaussian") return "Sigma (points)";
    return "Alpha";
  }

  function alignmentLabel(alignment: XAlignment): string {
    if (alignment === "relative-step") return "Relative step";
    if (alignment === "elapsed-time") return "Elapsed time";
    return "Absolute step";
  }
</script>

<details bind:this={settings} bind:open class="chart-settings" aria-keyshortcuts="Escape">
  <summary bind:this={summary} aria-label="Chart settings" aria-expanded={open}
    ><Icon name="settings" size={14} /></summary
  >
  <div class="chart-settings-popover" role="group" aria-label="Chart display settings">
    <label>
      Display
      <SelectControl
        ariaLabel="Chart display"
        compact
        value={displayMode}
        options={DISPLAY_OPTIONS}
        onvaluechange={(value) => (displayMode = value as "band" | "line")}
      />
    </label>
    <label>
      Smoothing
      <SelectControl
        ariaLabel="Smoothing"
        compact
        value={smoothingMode}
        options={SMOOTHING_OPTIONS}
        onvaluechange={(value) => changeSmoothing(value as SmoothingMode)}
      />
    </label>
    {#if smoothingMode !== "none"}
      <label>
        {smoothingAmountLabel(smoothingMode)}
        <input
          type="number"
          name="smoothing-amount"
          min={smoothingMode === "ema" ? 0.001 : 1}
          max={smoothingMode === "ema"
            ? 1
            : smoothingMode === "gaussian"
              ? MAX_GAUSSIAN_SIGMA
              : MAX_SMOOTHING_WINDOW}
          step={smoothingMode === "ema" ? 0.001 : 1}
          bind:value={smoothingAmount}
          onchange={normalizeSmoothingAmount}
        />
      </label>
    {/if}
    <fieldset>
      <legend>X axis · {alignmentLabel(xAlignment)}</legend>
      <SelectControl
        ariaLabel="X axis scale"
        compact
        value={xScale}
        options={SCALE_OPTIONS}
        onvaluechange={(value) => {
          xScale = value as ScaleMode;
          onviewchange();
        }}
      />
      <input
        name="x-minimum"
        aria-label="X axis minimum"
        placeholder="Auto min"
        bind:value={xMinimum}
        onchange={onviewchange}
      />
      <input
        name="x-maximum"
        aria-label="X axis maximum"
        placeholder="Auto max"
        bind:value={xMaximum}
        onchange={onviewchange}
      />
    </fieldset>
    <fieldset>
      <legend>Y axis</legend>
      <SelectControl
        ariaLabel="Y axis scale"
        compact
        value={yScale}
        options={SCALE_OPTIONS}
        onvaluechange={(value) => {
          yScale = value as ScaleMode;
          onviewchange();
        }}
      />
      <input
        name="y-minimum"
        aria-label="Y axis minimum"
        placeholder="Auto min"
        bind:value={yMinimum}
        onchange={onviewchange}
      />
      <input
        name="y-maximum"
        aria-label="Y axis maximum"
        placeholder="Auto max"
        bind:value={yMaximum}
        onchange={onviewchange}
      />
    </fieldset>
    {#if axisWarning}<p class="chart-warning">{axisWarning}</p>{/if}
  </div>
</details>
