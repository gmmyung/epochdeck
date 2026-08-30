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

  function changeSmoothing(event: Event): void {
    const mode = (event.currentTarget as HTMLSelectElement).value as SmoothingMode;
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
      <select bind:value={displayMode}>
        <option value="band">Band</option>
        <option value="line">Line</option>
      </select>
    </label>
    <label>
      Smoothing
      <select value={smoothingMode} onchange={changeSmoothing}>
        <option value="none">None</option>
        <option value="time-ema">Time-weighted EMA</option>
        <option value="running">Running average</option>
        <option value="gaussian">Gaussian</option>
        <option value="ema">EMA</option>
      </select>
    </label>
    {#if smoothingMode !== "none"}
      <label>
        {smoothingAmountLabel(smoothingMode)}
        <input
          type="number"
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
      <select aria-label="X axis scale" bind:value={xScale} onchange={onviewchange}>
        <option value="linear">Linear</option>
        <option value="log">Log</option>
      </select>
      <input
        aria-label="X axis minimum"
        placeholder="Auto min"
        bind:value={xMinimum}
        onchange={onviewchange}
      />
      <input
        aria-label="X axis maximum"
        placeholder="Auto max"
        bind:value={xMaximum}
        onchange={onviewchange}
      />
    </fieldset>
    <fieldset>
      <legend>Y axis</legend>
      <select aria-label="Y axis scale" bind:value={yScale} onchange={onviewchange}>
        <option value="linear">Linear</option>
        <option value="log">Log</option>
      </select>
      <input
        aria-label="Y axis minimum"
        placeholder="Auto min"
        bind:value={yMinimum}
        onchange={onviewchange}
      />
      <input
        aria-label="Y axis maximum"
        placeholder="Auto max"
        bind:value={yMaximum}
        onchange={onviewchange}
      />
    </fieldset>
    {#if axisWarning}<p class="chart-warning">{axisWarning}</p>{/if}
  </div>
</details>
