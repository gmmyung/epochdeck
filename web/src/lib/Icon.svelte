<script context="module" lang="ts">
  import type { LucideIcon } from "@lucide/svelte";
  import {
    Activity,
    Archive,
    ChartNoAxesCombined,
    ChevronDown,
    ChevronLeft,
    ChevronRight,
    CircleCheck,
    Download,
    Ellipsis,
    Eye,
    EyeOff,
    File,
    Folder,
    Hand,
    Info,
    List,
    Maximize2,
    Minimize2,
    RotateCcw,
    Scan,
    Search,
    SlidersHorizontal,
    Video,
    X,
  } from "@lucide/svelte";

  export type IconName =
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
    | "hide"
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
    | "show"
    | "settings"
    | "summary";

  const icons: Record<IconName, LucideIcon> = {
    activity: Activity,
    archive: Archive,
    chart: ChartNoAxesCombined,
    check: CircleCheck,
    close: X,
    "chevron-down": ChevronDown,
    "chevron-left": ChevronLeft,
    "chevron-right": ChevronRight,
    download: Download,
    expand: Maximize2,
    hide: EyeOff,
    file: File,
    folder: Folder,
    hand: Hand,
    info: Info,
    media: Video,
    minimize: Minimize2,
    more: Ellipsis,
    reset: RotateCcw,
    search: Search,
    select: Scan,
    show: Eye,
    settings: SlidersHorizontal,
    summary: List,
  };
</script>

<script lang="ts">
  export let name: IconName;
  export let size = 16;

  $: icon = icons[name];
</script>

<svelte:component this={icon} {size} strokeWidth={1.8} aria-hidden="true" />
