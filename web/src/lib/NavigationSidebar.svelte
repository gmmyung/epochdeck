<script lang="ts">
  import { onMount, tick } from "svelte";

  import type { Project, ReportSummary, RunListItem } from "./api";
  import { MAX_SELECTED_RUNS, type RunStyle } from "./comparison-state";
  import Icon from "./Icon.svelte";
  import SelectControl from "./SelectControl.svelte";
  import { resolveRunStyle, type RunStylePreferences } from "./sidebar-preferences";

  const LINE_STYLE_OPTIONS: ReadonlyArray<{
    value: RunStyle["pattern"];
    label: string;
  }> = [
    { value: "solid", label: "Solid" },
    { value: "dash", label: "Dashed" },
    { value: "dot", label: "Dotted" },
    { value: "dash-dot", label: "Dash-dot" },
  ];

  export let visibleProjects: Project[];
  export let selectedProject: string;
  export let projectSearch: string;
  export let projectCursor: string | null;
  export let projectWindowTruncated: boolean;
  export let loadingMoreProjects: boolean;
  export let projectError: string | null;
  export let reports: ReportSummary[];
  export let visibleReports: ReportSummary[];
  export let selectedReportId: string | null;
  export let reportSearch: string;
  export let reportCursor: string | null;
  export let reportWindowTruncated: boolean;
  export let loadingMoreReports: boolean;
  export let reportError: string | null;
  export let runs: RunListItem[];
  export let selectedRunIds: string[];
  export let runStylePreferences: RunStylePreferences = {};
  export let primaryRunId: string | null;
  export let collapsed = false;
  export let runSearch: string;
  export let runCursor: string | null;
  export let runWindowTruncated: boolean;
  export let loadingRuns: boolean;
  export let runError: string | null;
  export let selectionNotice: string | null;
  export let logoUrl: string | null = null;
  export let statusText = "connecting";
  export let statusFailed = false;
  export let onlogofailure: () => void = () => {};
  export let onchooseproject: (project: string) => void;
  export let onloadprojects: () => void;
  export let onchoosereport: (report: ReportSummary) => void;
  export let onloadreports: () => void;
  export let onsearchruns: () => void;
  export let onloadruns: () => void;
  export let ontogglerun: (run: RunListItem, selected: boolean) => void;
  export let onchooserun: (run: RunListItem) => void;
  export let onhoverrun: (runId: string | null) => void = () => {};
  export let onrunstylechange: (runId: string, style: RunStyle) => void = () => {};
  export let onresetrunstyle: (runId: string) => void = () => {};
  export let ontogglecollapsed: () => void = () => {};

  let styleMenuRunId: string | null = null;
  let styleMenuTrigger: HTMLButtonElement | null = null;
  let styleMenuPopover: HTMLDivElement | null = null;
  let styleMenuFirstControl: HTMLInputElement | null = null;
  let styleMenuPopoverStyle = "";

  $: if (styleMenuRunId && !runs.some((run) => run.id === styleMenuRunId)) {
    styleMenuRunId = null;
  }
  $: if (collapsed && styleMenuRunId) styleMenuRunId = null;

  onMount(() => {
    const pointerdown = (event: PointerEvent) => {
      if (
        styleMenuRunId &&
        (!(event.target instanceof Element) || !event.target.closest(".run-style-menu-shell"))
      ) {
        closeStyleMenu(false);
      }
    };
    const keydown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || !styleMenuRunId) return;
      event.preventDefault();
      event.stopPropagation();
      closeStyleMenu(true);
    };
    const reposition = () => {
      if (styleMenuRunId) positionStyleMenu();
    };
    document.addEventListener("pointerdown", pointerdown);
    document.addEventListener("keydown", keydown);
    document.addEventListener("scroll", reposition, true);
    window.addEventListener("resize", reposition);
    return () => {
      document.removeEventListener("pointerdown", pointerdown);
      document.removeEventListener("keydown", keydown);
      document.removeEventListener("scroll", reposition, true);
      window.removeEventListener("resize", reposition);
    };
  });

  async function toggleStyleMenu(event: MouseEvent, runId: string): Promise<void> {
    if (!(event.currentTarget instanceof HTMLButtonElement)) return;
    if (styleMenuRunId === runId) {
      closeStyleMenu(false);
      return;
    }
    styleMenuTrigger = event.currentTarget;
    styleMenuPopoverStyle = "";
    styleMenuRunId = runId;
    await tick();
    positionStyleMenu();
    styleMenuFirstControl?.focus();
  }

  function closeStyleMenu(restoreFocus: boolean): void {
    styleMenuRunId = null;
    if (restoreFocus) window.requestAnimationFrame(() => styleMenuTrigger?.focus());
  }

  function positionStyleMenu(): void {
    if (!styleMenuTrigger || !styleMenuPopover) return;
    const triggerRect = styleMenuTrigger.getBoundingClientRect();
    const viewportPadding = 8;
    const measurable = triggerRect.width > 0 || triggerRect.height > 0;
    if (
      measurable &&
      (triggerRect.bottom < viewportPadding ||
        triggerRect.top > window.innerHeight - viewportPadding)
    ) {
      closeStyleMenu(true);
      return;
    }
    const width = Math.min(232, window.innerWidth - viewportPadding * 2);
    const renderedHeight = styleMenuPopover.getBoundingClientRect().height;
    const desiredHeight = Math.min(Math.max(renderedHeight, 180), window.innerHeight - 16);
    const spaceBelow = window.innerHeight - triggerRect.bottom - viewportPadding;
    const spaceAbove = triggerRect.top - viewportPadding;
    const placeAbove = spaceBelow < desiredHeight && spaceAbove > spaceBelow;
    const maxHeight = Math.max(48, Math.min(desiredHeight, placeAbove ? spaceAbove : spaceBelow));
    const height = Math.min(desiredHeight, maxHeight);
    const left = Math.max(
      viewportPadding,
      Math.min(triggerRect.right - width, window.innerWidth - width - viewportPadding),
    );
    const top = placeAbove
      ? Math.max(viewportPadding, triggerRect.top - height - 4)
      : Math.min(triggerRect.bottom + 4, window.innerHeight - height - viewportPadding);
    styleMenuPopoverStyle = `top: ${Math.round(top)}px; left: ${Math.round(left)}px; width: ${Math.round(width)}px; max-height: ${Math.round(maxHeight)}px`;
  }
</script>

<aside class:collapsed>
  <div class="sidebar-header">
    {#if !collapsed}
      <div class="brand">
        <h1>
          {#if logoUrl}
            <img class="brand-logo" src={logoUrl} alt="EpochDeck" onerror={onlogofailure} />
          {:else}
            <span class="default-brand">
              <img src="/epochdeck-mark.svg" alt="" aria-hidden="true" />
              <span>EpochDeck</span>
            </span>
          {/if}
        </h1>
      </div>
    {/if}
    <button
      type="button"
      aria-label={collapsed ? "Expand run sidebar" : "Collapse run sidebar"}
      title={collapsed ? "Expand run sidebar" : "Collapse to run checkboxes"}
      onclick={ontogglecollapsed}
    >
      <Icon name={collapsed ? "chevron-right" : "chevron-left"} size={15} />
    </button>
    {#if !collapsed}
      <div class="status" class:failed={statusFailed} title={statusText}>
        <span class="status-dot" aria-hidden="true"></span>
        <span>{statusText}</span>
      </div>
    {/if}
  </div>
  {#if !collapsed}
    <label class="nav-search">
      <span>Projects</span>
      <span class="search-control">
        <Icon name="search" size={14} />
        <input
          type="search"
          name="project-filter"
          placeholder="Filter loaded projects"
          maxlength="256"
          bind:value={projectSearch}
        />
      </span>
    </label>
    <SelectControl
      ariaLabel="Project"
      value={selectedProject}
      options={visibleProjects.map((project) => ({
        value: project.name,
        label: `${project.name} · ${project.run_count}`,
      }))}
      onvaluechange={onchooseproject}
    />
    {#if projectError}<p class="nav-error" role="alert">{projectError}</p>{/if}
    {#if projectCursor}
      <button
        class="nav-load-more"
        type="button"
        disabled={loadingMoreProjects}
        onclick={onloadprojects}>{loadingMoreProjects ? "Loading…" : "Load more projects"}</button
      >
    {/if}
    {#if projectWindowTruncated}
      <p class="window-notice" role="status">
        Bounded window · recent and oldest loaded projects kept
      </p>
    {/if}

    {#if reports.length > 0 || reportError || reportCursor}
      <div class="nav-section-heading">
        <p class="nav-label">Reports</p>
        <label class="compact-search">
          <Icon name="search" size={13} />
          <input
            type="search"
            name="report-filter"
            aria-label="Filter loaded reports"
            maxlength="256"
            bind:value={reportSearch}
          />
        </label>
      </div>
      {#if reportError}<p class="nav-error" role="alert">{reportError}</p>{/if}
      <div class="run-list" aria-label="Reports">
        {#each visibleReports as report (report.id)}
          <button
            type="button"
            class:active={selectedReportId === report.id}
            aria-pressed={selectedReportId === report.id}
            onclick={() => onchoosereport(report)}
          >
            <span>{report.name}</span>
            <small>{new Date(report.updated_at).toLocaleDateString()}</small>
          </button>
        {/each}
      </div>
      {#if reportCursor}
        <button
          class="nav-load-more"
          type="button"
          disabled={loadingMoreReports}
          onclick={onloadreports}
        >
          {loadingMoreReports ? "Loading…" : "Load more reports"}
        </button>
      {/if}
      {#if reportWindowTruncated}
        <p class="window-notice" role="status">
          Bounded window · recent and oldest loaded reports kept
        </p>
      {/if}
    {/if}

    <form
      class="run-search-form"
      onsubmit={(event) => {
        event.preventDefault();
        onsearchruns();
      }}
    >
      <span class="nav-label">Runs</span>
      <div class="run-search-control">
        <label class="search-control">
          <Icon name="search" size={14} />
          <input
            type="search"
            name="run-search"
            placeholder="Search all runs"
            maxlength="256"
            bind:value={runSearch}
          />
        </label>
        <button class="icon-button" type="submit" disabled={loadingRuns} aria-label="Search runs">
          <Icon name="search" size={14} />
        </button>
      </div>
    </form>
    {#if runError}<p class="nav-error" role="alert">{runError}</p>{/if}
  {/if}
  <div class="run-list" aria-label="Runs" class:hidden={runs.length === 0}>
    {#each runs as run (run.id)}
      {@const selected = selectedRunIds.includes(run.id)}
      {@const style = resolveRunStyle(run.id, runStylePreferences)}
      <div
        class="run-list-row"
        class:selected
        class:primary={primaryRunId === run.id}
        class:collapsed
        role="group"
        aria-label={`Run ${run.name} (${run.id.slice(0, 8)})`}
        style={`--run-color: ${style.color}`}
        onmouseenter={() => onhoverrun(run.id)}
        onmouseleave={() => onhoverrun(null)}
        onfocusin={() => onhoverrun(run.id)}
        onfocusout={(event) => {
          if (
            !(event.relatedTarget instanceof Node) ||
            !event.currentTarget.contains(event.relatedTarget)
          ) {
            onhoverrun(null);
          }
        }}
      >
        <label
          class="run-checkbox"
          aria-label={`Compare ${run.name} (${run.id.slice(0, 8)})`}
          title={collapsed ? run.name : undefined}
        >
          <input
            type="checkbox"
            name="comparison-runs"
            value={run.id}
            checked={selected}
            disabled={!selected && selectedRunIds.length >= MAX_SELECTED_RUNS}
            onchange={(event) => ontogglerun(run, event.currentTarget.checked)}
          />
          <span class={`run-swatch pattern-${style.pattern}`} aria-hidden="true"></span>
        </label>
        {#if !collapsed}
          <div class="run-row-content">
            <button
              class="run-primary-button"
              class:active={primaryRunId === run.id}
              onclick={() => onchooserun(run)}
            >
              <span>{run.name}</span>
              <small
                class="run-list-state"
                class:live={run.state === "running"}
                class:finished={run.state === "finished"}
              >
                <Icon name={run.state === "running" ? "activity" : "check"} size={12} />
                <span>{run.state}</span>
                <span>r{run.metric_revision}</span>
              </small>
            </button>
            <div class="run-style-menu-shell">
              <button
                class="run-style-menu-trigger"
                class:active={styleMenuRunId === run.id}
                type="button"
                aria-label={`Configure chart style for ${run.name} (${run.id.slice(0, 8)})`}
                aria-controls={`run-style-${run.id}`}
                aria-expanded={styleMenuRunId === run.id}
                aria-haspopup="dialog"
                title={`Chart style for ${run.name}`}
                onclick={(event) => void toggleStyleMenu(event, run.id)}
              >
                <Icon name="more" size={15} />
              </button>
              {#if styleMenuRunId === run.id}
                <div
                  bind:this={styleMenuPopover}
                  id={`run-style-${run.id}`}
                  class="run-style-popover"
                  role="dialog"
                  aria-labelledby={`run-style-heading-${run.id}`}
                  style={styleMenuPopoverStyle}
                >
                  <div class="run-style-popover-heading">
                    <strong id={`run-style-heading-${run.id}`}>Run appearance</strong>
                    <small title={run.name}>{run.name}</small>
                  </div>
                  <label class="run-color-control">
                    <span>Color</span>
                    <span class="run-color-picker">
                      <input
                        bind:this={styleMenuFirstControl}
                        type="color"
                        name={`run-color-${run.id}`}
                        aria-label={`Line color for ${run.name}`}
                        title={`Line color for ${run.name}`}
                        value={style.color}
                        onchange={(event) =>
                          onrunstylechange(run.id, { ...style, color: event.currentTarget.value })}
                      />
                      <code>{style.color.toUpperCase()}</code>
                    </span>
                  </label>
                  <fieldset class="run-line-style-control">
                    <legend>Line</legend>
                    <div class="run-line-style-options">
                      {#each LINE_STYLE_OPTIONS as option (option.value)}
                        <label
                          class:active={style.pattern === option.value}
                          title={`${option.label} line`}
                        >
                          <input
                            type="radio"
                            name={`run-line-style-${run.id}`}
                            value={option.value}
                            checked={style.pattern === option.value}
                            aria-label={`${option.label} line for ${run.name}`}
                            onchange={() =>
                              onrunstylechange(run.id, { ...style, pattern: option.value })}
                          />
                          <span
                            class={`line-style-preview pattern-${option.value}`}
                            aria-hidden="true"
                          ></span>
                          <span>{option.label}</span>
                        </label>
                      {/each}
                    </div>
                  </fieldset>
                  <button
                    class="run-style-reset"
                    type="button"
                    aria-label={`Reset chart style for ${run.name}`}
                    title={`Reset chart style for ${run.name}`}
                    onclick={() => onresetrunstyle(run.id)}
                  >
                    <Icon name="reset" size={13} />
                    <span>Reset appearance</span>
                  </button>
                </div>
              {/if}
            </div>
          </div>
        {/if}
      </div>
    {/each}
  </div>
  {#if !collapsed}
    {#if runCursor}
      <button class="nav-load-more" type="button" disabled={loadingRuns} onclick={onloadruns}
        >{loadingRuns ? "Loading…" : "Load 100 more runs"}</button
      >
    {/if}
    {#if runWindowTruncated}
      <p class="window-notice" role="status">Bounded window · recent and oldest loaded runs kept</p>
    {/if}
    {#if selectionNotice}<p class="selection-notice" role="status">{selectionNotice}</p>{/if}
    {#if runs.length > 0}
      <p class="run-limit">{selectedRunIds.length} / {MAX_SELECTED_RUNS} visible</p>
    {/if}
  {/if}
</aside>

<style>
  aside {
    min-width: 0;
  }

  .sidebar-header {
    min-height: 51px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) 29px;
    gap: 7px 10px;
    align-items: center;
    margin-bottom: 16px;
    padding-bottom: 13px;
    border-bottom: 1px solid var(--divider);
  }

  .sidebar-header button {
    width: 29px;
    height: 29px;
    display: grid;
    place-items: center;
    padding: 0;
    border: 1px solid transparent;
    background: transparent;
    color: var(--muted);
  }

  .sidebar-header button:hover,
  .sidebar-header button:focus-visible {
    border-color: var(--line);
    background: var(--button-hover);
    color: var(--text);
  }

  .sidebar-header .status {
    grid-column: 1 / -1;
    min-width: 0;
  }

  .sidebar-header .status > span:last-child {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .default-brand {
    display: flex;
    align-items: center;
    gap: 9px;
  }

  .default-brand img {
    width: 26px;
    height: 26px;
    flex: none;
  }

  aside.collapsed .sidebar-header {
    min-height: 34px;
    display: flex;
    justify-content: center;
    margin-bottom: 10px;
    padding-bottom: 10px;
  }

  @media (max-width: 760px) {
    .sidebar-header button :global(svg) {
      transform: rotate(90deg);
    }
  }

  .nav-search,
  .nav-section-heading {
    display: grid;
    gap: 6px;
  }

  .nav-search > span:first-child,
  .nav-label {
    color: var(--muted);
    font-size: 10px;
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }

  .nav-section-heading {
    grid-template-columns: auto minmax(0, 1fr);
    align-items: center;
    margin-top: 18px;
  }

  .nav-label {
    margin: 0;
  }

  .compact-search {
    display: flex;
    align-items: center;
  }

  .compact-search {
    gap: 5px;
    padding: 3px 6px;
    border-bottom: 1px solid var(--line);
    color: var(--muted);
  }

  .compact-search input {
    width: 100%;
    min-width: 0;
    border: 0;
    background: transparent;
    color: var(--text);
    outline: none;
  }

  .run-search-form {
    display: grid;
    gap: 6px;
    margin-top: 18px;
  }

  .run-search-control {
    min-width: 0;
    display: flex;
    align-items: center;
    border-bottom: 1px solid var(--line-strong);
  }

  .run-search-control .search-control {
    min-width: 0;
    flex: 1;
    margin: 0;
    border-bottom: 0;
  }

  .run-search-control .icon-button {
    width: 34px;
    height: 34px;
    flex: none;
  }

  .nav-load-more {
    width: 100%;
    padding: 7px;
    border: 0;
    border-bottom: 1px solid var(--line);
    background: transparent;
    color: var(--muted);
  }

  .nav-load-more:hover:not(:disabled) {
    background: var(--button-hover);
    color: var(--text);
  }

  .nav-error {
    margin: 6px 0;
    color: var(--danger);
    font-size: 10px;
  }

  .window-notice {
    margin: 6px 0 0;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.35;
  }
</style>
