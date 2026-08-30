<script lang="ts">
  import { onMount } from "svelte";
  import { boundedHistogramCounts } from "./histogram-data";

  export let counts: number[];
  export let label: string;

  let canvas: HTMLCanvasElement;
  let revision = 0;
  const ACCESSIBLE_BIN_LIMIT = 500;

  $: visibleCounts = counts.slice(0, ACCESSIBLE_BIN_LIMIT);
  $: drawableCounts = boundedHistogramCounts(counts);

  onMount(() => {
    const observer = new ResizeObserver(() => (revision += 1));
    observer.observe(canvas);
    return () => observer.disconnect();
  });

  $: if (canvas && revision >= 0) draw(canvas, drawableCounts);

  function draw(target: HTMLCanvasElement, values: number[]): void {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const ratio = window.devicePixelRatio || 1;
    target.width = Math.floor(width * ratio);
    target.height = Math.floor(height * ratio);
    const context = target.getContext("2d");
    if (!context) return;
    context.scale(ratio, ratio);
    context.clearRect(0, 0, width, height);
    let maximum = 1;
    for (const value of values) maximum = Math.max(maximum, value);
    const gap = values.length > 128 ? 0 : 1;
    const barWidth = width / Math.max(values.length, 1);
    const styles = getComputedStyle(target);
    context.fillStyle = styles.getPropertyValue("--accent").trim() || "#2766ad";
    for (let index = 0; index < values.length; index += 1) {
      const barHeight = (Math.max(values[index], 0) / maximum) * (height - 4);
      context.fillRect(
        index * barWidth,
        height - barHeight,
        Math.max(barWidth - gap, 1),
        barHeight,
      );
    }
  }
</script>

<figure class="histogram">
  <canvas
    bind:this={canvas}
    class="histogram-canvas"
    aria-label={`${label} histogram with ${counts.length.toLocaleString()} bins`}
  ></canvas>
  <details class="histogram-data">
    <summary>View histogram values · {counts.length.toLocaleString()} bins</summary>
    <div>
      <table>
        <thead><tr><th>Bin</th><th>Count</th></tr></thead>
        <tbody>
          {#each visibleCounts as count, index}
            <tr><th>{index.toLocaleString()}</th><td>{count.toLocaleString()}</td></tr>
          {/each}
        </tbody>
      </table>
      {#if visibleCounts.length < counts.length}
        <p>Showing the first {ACCESSIBLE_BIN_LIMIT.toLocaleString()} bins.</p>
      {/if}
    </div>
  </details>
</figure>

<style>
  .histogram {
    width: 100%;
    margin: 0;
  }

  .histogram-data {
    margin-top: 6px;
    color: var(--muted);
    font-size: 11px;
  }

  .histogram-data summary {
    cursor: pointer;
  }

  .histogram-data > div {
    max-height: 240px;
    margin-top: 6px;
    overflow: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 4px 7px;
    border-bottom: 1px solid var(--line);
    text-align: right;
  }
</style>
