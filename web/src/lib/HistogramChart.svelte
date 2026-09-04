<script lang="ts">
  import { onMount } from "svelte";

  import { boundedCanvasPixelRatio, snapCanvasCoordinate } from "./canvas-resolution";
  import { boundedHistogramBins, type HistogramBin } from "./histogram-data";

  export let counts: number[];
  export let edges: number[] = [];
  export let label: string;

  const ACCESSIBLE_BIN_LIMIT = 500;
  const PLOT_LEFT = 42;
  const PLOT_RIGHT = 8;
  const PLOT_TOP = 8;
  const PLOT_BOTTOM = 25;

  let canvas: HTMLCanvasElement;
  let revision = 0;
  let hoveredIndex: number | null = null;
  let hoverX = 0;

  $: bins = boundedHistogramBins(counts, edges);
  $: exactBins = boundedHistogramBins(counts, edges, Math.max(counts.length, 1));
  $: visibleBins = exactBins.slice(0, ACCESSIBLE_BIN_LIMIT);
  $: total = exactBins.reduce((sum, bin) => sum + bin.count, 0);
  $: hoveredBin = hoveredIndex === null ? null : bins[hoveredIndex];

  onMount(() => {
    const observer = new ResizeObserver(() => (revision += 1));
    observer.observe(canvas);
    return () => observer.disconnect();
  });

  $: if (canvas && revision >= 0) draw(canvas, bins, hoveredIndex);

  function updateHover(event: PointerEvent): void {
    if (bins.length === 0) return clearHover();
    const bounds = canvas.getBoundingClientRect();
    const plotWidth = Math.max(bounds.width - PLOT_LEFT - PLOT_RIGHT, 1);
    const localX = event.clientX - bounds.left;
    if (localX < PLOT_LEFT || localX > PLOT_LEFT + plotWidth) return clearHover();
    hoveredIndex = Math.min(
      Math.floor(((localX - PLOT_LEFT) / plotWidth) * bins.length),
      bins.length - 1,
    );
    hoverX = localX;
  }

  function clearHover(): void {
    hoveredIndex = null;
  }

  function draw(target: HTMLCanvasElement, values: HistogramBin[], highlight: number | null): void {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const ratio = boundedCanvasPixelRatio(width, height);
    target.width = Math.round(width * ratio);
    target.height = Math.round(height * ratio);
    const context = target.getContext("2d");
    if (!context) return;
    context.scale(ratio, ratio);
    context.clearRect(0, 0, width, height);
    if (values.length === 0) return;

    const plotWidth = Math.max(width - PLOT_LEFT - PLOT_RIGHT, 1);
    const plotHeight = Math.max(height - PLOT_TOP - PLOT_BOTTOM, 1);
    const maximum = niceCountMaximum(Math.max(1, ...values.map((bin) => bin.count)));
    const styles = getComputedStyle(target);
    const accent = styles.getPropertyValue("--series-accent").trim() || "#2766ad";
    const grid = styles.getPropertyValue("--chart-grid").trim() || "#d9dde0";
    const muted = styles.getPropertyValue("--muted").trim() || "#596168";
    const surface = styles.getPropertyValue("--surface").trim() || "#f7f8f9";

    context.font = "10px system-ui, sans-serif";
    context.textBaseline = "middle";
    context.fillStyle = muted;
    context.strokeStyle = grid;
    context.lineWidth = 1;
    for (let tick = 0; tick <= 4; tick += 1) {
      const fraction = tick / 4;
      const y = snapCanvasCoordinate(PLOT_TOP + plotHeight * (1 - fraction), ratio);
      context.beginPath();
      context.moveTo(PLOT_LEFT, y);
      context.lineTo(PLOT_LEFT + plotWidth, y);
      context.stroke();
      context.textAlign = "right";
      context.fillText(formatNumber(maximum * fraction), PLOT_LEFT - 6, y);
    }

    const gap = values.length > 128 ? 0 : 1;
    const barWidth = plotWidth / values.length;
    for (let index = 0; index < values.length; index += 1) {
      const bin = values[index];
      const barHeight = (bin.count / maximum) * plotHeight;
      context.fillStyle = index === highlight ? muted : accent;
      const left = snapCanvasCoordinate(PLOT_LEFT + index * barWidth, ratio);
      const right = snapCanvasCoordinate(PLOT_LEFT + (index + 1) * barWidth - gap, ratio);
      const top = snapCanvasCoordinate(PLOT_TOP + plotHeight - barHeight, ratio);
      const bottom = snapCanvasCoordinate(PLOT_TOP + plotHeight, ratio);
      context.fillRect(left, top, Math.max(right - left, 1 / ratio), bottom - top);
    }

    const xTicks = Math.min(4, values.length);
    context.fillStyle = muted;
    context.textBaseline = "bottom";
    for (let tick = 0; tick <= xTicks; tick += 1) {
      const index = Math.min(Math.floor((tick * values.length) / xTicks), values.length - 1);
      const value = tick === xTicks ? values.at(-1)!.upper : values[index].lower;
      const x = PLOT_LEFT + (tick / xTicks) * plotWidth;
      context.textAlign = tick === 0 ? "left" : tick === xTicks ? "right" : "center";
      context.fillText(formatNumber(value), x, height);
    }

    if (highlight !== null) {
      const x = PLOT_LEFT + (highlight + 0.5) * barWidth;
      context.strokeStyle = surface;
      context.beginPath();
      context.moveTo(x, PLOT_TOP);
      context.lineTo(x, PLOT_TOP + plotHeight);
      context.stroke();
    }
  }

  function formatRange(bin: HistogramBin): string {
    return `${formatNumber(bin.lower)} – ${formatNumber(bin.upper)}`;
  }

  function formatNumber(value: number): string {
    return value.toLocaleString(undefined, { maximumSignificantDigits: 5 });
  }

  function formatPercent(count: number): string {
    return total > 0 ? `${((count / total) * 100).toFixed(1)}%` : "0%";
  }

  function niceCountMaximum(value: number): number {
    const magnitude = 10 ** Math.floor(Math.log10(value));
    const normalized = value / magnitude;
    const ceiling = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
    return ceiling * magnitude;
  }
</script>

<figure class="histogram">
  <div class="histogram-plot" style={`--hover-x: ${hoverX}px`}>
    <canvas
      bind:this={canvas}
      class="histogram-canvas"
      aria-label={`${label} histogram with ${counts.length.toLocaleString()} bins`}
      onpointermove={updateHover}
      onpointerleave={clearHover}
    ></canvas>
    {#if hoveredBin}
      <div class="histogram-tooltip" role="status">
        <strong>{formatRange(hoveredBin)}</strong>
        <span>{formatNumber(hoveredBin.count)} · {formatPercent(hoveredBin.count)}</span>
      </div>
    {/if}
  </div>
  <details class="histogram-data">
    <summary>Exact bin data · {counts.length.toLocaleString()} bins</summary>
    <div>
      <table>
        <thead><tr><th>Range</th><th>Count</th><th>Share</th></tr></thead>
        <tbody>
          {#each visibleBins as bin}
            <tr>
              <th>{formatRange(bin)}</th>
              <td>{formatNumber(bin.count)}</td>
              <td>{formatPercent(bin.count)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
      {#if visibleBins.length < exactBins.length}
        <p>Showing the first {ACCESSIBLE_BIN_LIMIT.toLocaleString()} bins.</p>
      {/if}
    </div>
  </details>
</figure>

<style>
  .histogram {
    width: 100%;
    margin: 0;
    padding: 8px 10px 7px;
  }

  .histogram-plot {
    position: relative;
    width: 100%;
  }

  .histogram-canvas {
    width: 100%;
    height: 190px;
    display: block;
    touch-action: none;
  }

  .histogram-tooltip {
    position: absolute;
    top: 10px;
    left: clamp(76px, var(--hover-x), calc(100% - 76px));
    min-width: 128px;
    display: grid;
    gap: 3px;
    padding: 6px 8px;
    border: 1px solid var(--line-strong);
    background: color-mix(in srgb, var(--panel) 94%, transparent);
    box-shadow: 0 5px 14px rgb(0 0 0 / 16%);
    color: var(--text);
    font-size: 10px;
    pointer-events: none;
    transform: translateX(-50%);
  }

  .histogram-tooltip span {
    color: var(--muted);
  }

  .histogram-data {
    margin-top: 5px;
    color: var(--muted);
    font-size: 10px;
  }

  .histogram-data summary {
    width: max-content;
    cursor: pointer;
  }

  .histogram-data > div {
    max-height: 220px;
    margin-top: 6px;
    overflow: auto;
    border-top: 1px solid var(--line);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-variant-numeric: tabular-nums;
  }

  th,
  td {
    padding: 5px 7px;
    border-bottom: 1px solid var(--line);
    text-align: right;
  }

  thead th {
    position: sticky;
    top: 0;
    background: var(--surface);
  }

  th:first-child {
    text-align: left;
  }
</style>
