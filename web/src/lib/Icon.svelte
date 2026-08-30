<script lang="ts">
  type IconName =
    | "activity"
    | "archive"
    | "chart"
    | "check"
    | "close"
    | "chevron-down"
    | "chevron-left"
    | "chevron-right"
    | "download"
    | "expand"
    | "file"
    | "folder"
    | "hand"
    | "info"
    | "media"
    | "minimize"
    | "more"
    | "reset"
    | "search"
    | "select"
    | "settings"
    | "summary"
    | "trace";

  export let name: IconName;
  export let size = 16;
</script>

<svg
  class="icon"
  width={size}
  height={size}
  viewBox="0 0 24 24"
  fill="none"
  stroke="currentColor"
  stroke-width="1.8"
  stroke-linecap="round"
  stroke-linejoin="round"
  aria-hidden="true"
>
  {#if name === "activity"}
    <path d="M3 12h4l2.5-7 5 14 2.5-7h4" />
  {:else if name === "archive"}
    <path d="M4 7h16v13H4zM3 4h18v3H3zM9 11h6" />
  {:else if name === "chart"}
    <path d="M4 19V5M4 19h16M7 15l4-5 3 2 5-7" />
  {:else if name === "check"}
    <circle cx="12" cy="12" r="9" /><path d="m8 12 3 3 5-6" />
  {:else if name === "close"}
    <path d="m6 6 12 12M18 6 6 18" />
  {:else if name === "chevron-down"}
    <path d="m7 9 5 5 5-5" />
  {:else if name === "chevron-left"}
    <path d="m14 7-5 5 5 5" />
  {:else if name === "chevron-right"}
    <path d="m9 7 5 5-5 5" />
  {:else if name === "download"}
    <path d="M12 3v12m-4-4 4 4 4-4M4 20h16" />
  {:else if name === "expand"}
    <path d="M8 3H3v5M16 3h5v5M21 16v5h-5M3 16v5h5M3 8l6-6M15 3l6 6M21 16l-6 6M9 21l-6-6" />
  {:else if name === "file"}
    <path d="M6 3h8l4 4v14H6zM14 3v5h5" />
  {:else if name === "folder"}
    <path d="M3 6h7l2 2h9v11H3z" />
  {:else if name === "hand"}
    <path
      d="M7 11V7a2 2 0 0 1 4 0v4-5a2 2 0 0 1 4 0v5-3a2 2 0 0 1 4 0v6c0 4-2 7-7 7-3 0-5-2-7-5l-2-3a2 2 0 0 1 3-2l1 1"
    />
  {:else if name === "info"}
    <circle cx="12" cy="12" r="9" /><path d="M12 11v6M12 7h.01" />
  {:else if name === "media"}
    <rect x="3" y="5" width="18" height="14" rx="1" /><path d="m10 9 5 3-5 3z" />
  {:else if name === "minimize"}
    <path d="M9 4v5H4M15 4v5h5M20 15h-5v5M4 15h5v5M9 9 3 3M15 9l6-6M15 15l6 6M9 15l-6 6" />
  {:else if name === "more"}
    <path d="M5 12h.01M12 12h.01M19 12h.01" stroke-width="3" />
  {:else if name === "reset"}
    <path d="M4 4v6h6M5.5 9A8 8 0 1 1 4 14" />
  {:else if name === "search"}
    <circle cx="11" cy="11" r="7" /><path d="m16 16 5 5" />
  {:else if name === "select"}
    <path d="M4 8V4h4M16 4h4v4M20 16v4h-4M8 20H4v-4M8 12h8" />
  {:else if name === "settings"}
    <path d="M4 6h10M18 6h2M4 12h2M10 12h10M4 18h7M15 18h5" />
    <circle cx="16" cy="6" r="2" /><circle cx="8" cy="12" r="2" /><circle cx="13" cy="18" r="2" />
  {:else if name === "summary"}
    <path d="M5 4h14v16H5zM8 8h8M8 12h8M8 16h5" />
  {:else if name === "trace"}
    <circle cx="6" cy="6" r="2" /><circle cx="18" cy="8" r="2" /><circle cx="8" cy="18" r="2" />
    <path d="M8 6h4a4 4 0 0 1 4 4v2a4 4 0 0 1-4 4h-2" />
  {/if}
</svg>
