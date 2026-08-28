<script lang="ts">
  import { onMount } from "svelte";

  import { getHealth, type Health } from "./lib/api";

  let health: Health | null = null;
  let error: string | null = null;

  onMount(() => {
    const controller = new AbortController();
    getHealth(controller.signal)
      .then((result) => {
        health = result;
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          error = reason instanceof Error ? reason.message : "Unable to reach Runloom";
        }
      });
    return () => controller.abort();
  });
</script>

<svelte:head>
  <meta
    name="description"
    content="Runloom is a lossless, self-hosted experiment tracker built for large histories."
  />
</svelte:head>

<main>
  <section class="hero">
    <div class="wordmark" aria-label="Runloom">
      <span class="mark" aria-hidden="true"></span>
      <span>RUNLOOM</span>
    </div>

    <div class="copy">
      <p class="eyebrow">Every run, woven together.</p>
      <h1>Complete histories.<br />Considerate performance.</h1>
      <p class="summary">
        A standalone experiment tracker with a W&amp;B-compatible API, lossless columnar storage,
        native rich data, and no hosted-service dependency.
      </p>
    </div>

    <div class="status" class:failed={Boolean(error)}>
      <span class="status-dot" aria-hidden="true"></span>
      {#if health}
        API {health.status} · v{health.version}
      {:else if error}
        {error}
      {:else}
        Connecting to the local API…
      {/if}
    </div>
  </section>
</main>
