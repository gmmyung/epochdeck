<script lang="ts">
  import { onMount } from "svelte";

  import type { History } from "./api";

  export let metric: string;
  export let title: string | undefined = undefined;
  export let history: History | undefined;
  export let loading = false;
  export let onvisible: (metric: string) => void;

  let card: HTMLElement;
  let canvas: HTMLCanvasElement;
  let visible = false;
  let chartRevision = 0;

  onMount(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        visible = entries.some((entry) => entry.isIntersecting);
        if (visible) onvisible(metric);
      },
      { rootMargin: "500px 0px" },
    );
    const resizeObserver = new ResizeObserver(() => (chartRevision += 1));
    const theme = window.matchMedia("(prefers-color-scheme: dark)");
    const redraw = () => (chartRevision += 1);
    observer.observe(card);
    resizeObserver.observe(card);
    theme.addEventListener("change", redraw);
    return () => {
      observer.disconnect();
      resizeObserver.disconnect();
      theme.removeEventListener("change", redraw);
    };
  });

  $: if (canvas && visible && history && chartRevision >= 0) {
    drawChart(canvas, history.step, history.metrics[metric] ?? []);
  }

  function drawChart(
    target: HTMLCanvasElement,
    steps: number[],
    values: Array<number | null>,
  ): void {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const ratio = window.devicePixelRatio || 1;
    target.width = Math.floor(width * ratio);
    target.height = Math.floor(height * ratio);
    const context = target.getContext("2d");
    if (!context) return;
    context.scale(ratio, ratio);
    context.clearRect(0, 0, width, height);

    let minX = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    let pointCount = 0;
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index];
      if (value === null) continue;
      const step = steps[index];
      minX = Math.min(minX, step);
      maxX = Math.max(maxX, step);
      minY = Math.min(minY, value);
      maxY = Math.max(maxY, value);
      pointCount += 1;
    }
    if (pointCount === 0) return;

    const padding = { top: 18, right: 18, bottom: 28, left: 48 };
    const plotWidth = Math.max(width - padding.left - padding.right, 1);
    const plotHeight = Math.max(height - padding.top - padding.bottom, 1);
    const xRange = maxX - minX || 1;
    const yRange = maxY - minY || 1;
    const styles = getComputedStyle(target);
    context.font = "10px system-ui, sans-serif";
    context.lineWidth = 1;
    context.strokeStyle = styles.getPropertyValue("--chart-grid").trim() || "#d9dde0";
    context.fillStyle = styles.getPropertyValue("--muted").trim() || "#596168";
    for (let line = 0; line <= 4; line += 1) {
      const y = padding.top + (plotHeight * line) / 4;
      const value = maxY - (yRange * line) / 4;
      context.beginPath();
      context.moveTo(padding.left, y);
      context.lineTo(width - padding.right, y);
      context.stroke();
      context.fillText(formatAxis(value), 5, y + 3);
    }

    context.fillText(formatAxis(minX), padding.left, height - 8);
    const maxXLabel = formatAxis(maxX);
    context.fillText(
      maxXLabel,
      width - padding.right - context.measureText(maxXLabel).width,
      height - 8,
    );
    context.strokeStyle = styles.getPropertyValue("--accent").trim() || "#2766ad";
    context.lineWidth = 1.5;
    context.lineJoin = "round";
    context.beginPath();
    let drawing = false;
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index];
      if (value === null) {
        drawing = false;
        continue;
      }
      const x = padding.left + ((steps[index] - minX) / xRange) * plotWidth;
      const y = padding.top + ((maxY - value) / yRange) * plotHeight;
      if (drawing) context.lineTo(x, y);
      else context.moveTo(x, y);
      drawing = true;
    }
    context.stroke();
  }

  function formatAxis(value: number): string {
    if (Math.abs(value) >= 10_000 || (Math.abs(value) > 0 && Math.abs(value) < 0.001)) {
      return value.toExponential(1);
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
</script>

<article bind:this={card} class="metric-chart-card" aria-label={`${title ?? metric} metric panel`}>
  <div class="card-heading">
    <div>
      <small>Metric history</small>
      <strong>{title ?? metric}</strong>
    </div>
    {#if loading}<span class="loading-label">updating</span>{/if}
  </div>
  {#if history}
    <canvas bind:this={canvas} aria-label={`${metric} history chart`}></canvas>
    <div class="chart-footer">
      <span>{history.sequence.length.toLocaleString()} displayed points</span>
      <span>{(history.source_points ?? history.sequence.length).toLocaleString()} source</span>
    </div>
  {:else if loading}
    <div class="chart-placeholder">Loading bounded history…</div>
  {:else}
    <div class="chart-placeholder">Scroll near this chart to load it.</div>
  {/if}
</article>
