<script lang="ts">
  import { onMount } from "svelte";

  export let counts: number[];
  export let label: string;

  let canvas: HTMLCanvasElement;
  let revision = 0;

  onMount(() => {
    const observer = new ResizeObserver(() => (revision += 1));
    observer.observe(canvas);
    return () => observer.disconnect();
  });

  $: if (canvas && revision >= 0) draw(canvas, counts);

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
    const maximum = Math.max(...values, 1);
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

<canvas bind:this={canvas} class="histogram-canvas" aria-label={`${label} histogram`}></canvas>
