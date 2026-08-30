<script lang="ts">
  import type { Project, ReportSummary, RunListItem } from "./api";
  import { MAX_SELECTED_RUNS, runStyle } from "./comparison-state";
  import Icon from "./Icon.svelte";

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
  export let primaryRunId: string | null;
  export let runSearch: string;
  export let runCursor: string | null;
  export let runWindowTruncated: boolean;
  export let loadingRuns: boolean;
  export let runError: string | null;
  export let selectionNotice: string | null;
  export let onchooseproject: (project: string) => void;
  export let onloadprojects: () => void;
  export let onchoosereport: (report: ReportSummary) => void;
  export let onloadreports: () => void;
  export let onsearchruns: () => void;
  export let onloadruns: () => void;
  export let ontogglerun: (run: RunListItem, selected: boolean) => void;
  export let onchooserun: (run: RunListItem) => void;
</script>

<aside>
  <label class="nav-search">
    <span>Projects</span>
    <span class="search-control">
      <Icon name="search" size={14} />
      <input
        type="search"
        placeholder="Filter loaded projects"
        maxlength="256"
        bind:value={projectSearch}
      />
    </span>
  </label>
  <select
    id="project"
    aria-label="Project"
    value={selectedProject}
    onchange={(event) => onchooseproject(event.currentTarget.value)}
  >
    {#each visibleProjects as project (project.id)}
      <option value={project.name}>{project.name} · {project.run_count}</option>
    {/each}
  </select>
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

  <div class="nav-section-heading">
    <p class="nav-label">Reports</p>
    <label class="compact-search">
      <Icon name="search" size={13} />
      <input
        type="search"
        aria-label="Filter loaded reports"
        maxlength="256"
        bind:value={reportSearch}
      />
    </label>
  </div>
  {#if reportError}<p class="nav-error" role="alert">{reportError}</p>{/if}
  <div class="run-list" aria-label="Reports" class:hidden={reports.length === 0}>
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
      onclick={onloadreports}>{loadingMoreReports ? "Loading…" : "Load more reports"}</button
    >
  {/if}
  {#if reportWindowTruncated}
    <p class="window-notice" role="status">
      Bounded window · recent and oldest loaded reports kept
    </p>
  {/if}

  <form
    class="run-search-form"
    onsubmit={(event) => {
      event.preventDefault();
      onsearchruns();
    }}
  >
    <label class="nav-search">
      <span>Runs</span>
      <span class="search-control">
        <Icon name="search" size={14} />
        <input type="search" placeholder="Search all runs" maxlength="256" bind:value={runSearch} />
      </span>
    </label>
    <button class="icon-button" type="submit" disabled={loadingRuns} aria-label="Search runs">
      <Icon name="search" size={14} />
    </button>
  </form>
  {#if runError}<p class="nav-error" role="alert">{runError}</p>{/if}
  <div class="run-list" aria-label="Runs" class:hidden={runs.length === 0}>
    {#each runs as run (run.id)}
      {@const style = runStyle(run.id)}
      <div
        class="run-list-row"
        class:selected={selectedRunIds.includes(run.id)}
        class:primary={primaryRunId === run.id}
        style={`--run-color: ${style.color}`}
      >
        <label class="run-checkbox" aria-label={`Compare ${run.name}`}>
          <input
            type="checkbox"
            checked={selectedRunIds.includes(run.id)}
            disabled={!selectedRunIds.includes(run.id) &&
              selectedRunIds.length >= MAX_SELECTED_RUNS}
            onchange={(event) => ontogglerun(run, event.currentTarget.checked)}
          />
          <span class={`run-swatch pattern-${style.pattern}`} aria-hidden="true"></span>
        </label>
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
      </div>
    {/each}
  </div>
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
</aside>

<style>
  aside {
    min-width: 0;
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
    margin-top: 15px;
  }

  .nav-label {
    margin: 0;
  }

  .compact-search,
  .run-search-form {
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
    gap: 5px;
    align-items: end;
    margin-top: 15px;
  }

  .run-search-form .nav-search {
    min-width: 0;
    flex: 1;
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
