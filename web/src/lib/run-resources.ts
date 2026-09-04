import {
  getAlertPage,
  getArtifact,
  getRichValue,
  getRichValueKeyPage,
  getRichValuePage,
  getRunArtifactPage,
  type Alert,
  type Artifact,
  type ArtifactSummary,
  type CursorPage,
  type RichValue,
  type RichValueKeySummary,
  type RichValueSummary,
  type RunArtifact,
  type RunArtifactCursor,
  type RunArtifactPage,
} from "./api";
import { BoundedRequestScheduler } from "./bounded-request-scheduler";
import { MAX_SELECTED_RUNS } from "./comparison-state";
import { appendUniquePage, mergeNewestPage, reasonMessage } from "./resource-state";
import { retainHeadAndTail, retainRecord } from "./retained-window";

export const RUN_TABS = [
  { id: "summary", label: "Summary", icon: "summary" },
  { id: "configuration", label: "Configuration", icon: "settings" },
  { id: "metrics", label: "Metrics", icon: "chart" },
  { id: "media", label: "Media", icon: "media" },
  { id: "artifacts", label: "Artifacts", icon: "archive" },
] as const;

export type RunTab = (typeof RUN_TABS)[number]["id"];
export type PaginatedRunTab = Extract<RunTab, "summary" | "media" | "artifacts">;

export type SelectedRunArtifact = {
  artifact: ArtifactSummary;
  links: Array<{ runId: string; relation: RunArtifact["relation"] }>;
};

const MAX_RETAINED_ALERTS = 500;
const MAX_RETAINED_RICH_KEYS = 300;
const MAX_RETAINED_RICH_VALUES = 500;
const MAX_RETAINED_ARTIFACTS = 500;
const MAX_RETAINED_DETAILS = 24;
const MAX_ACTIVE_DETAIL_REQUESTS = 4;
const MAX_PENDING_DETAIL_REQUESTS = 24;

export type RunResourceState = {
  alerts: Alert[];
  alertCursor: string | null;
  richKeys: RichValueKeySummary[];
  selectedRichKey: string | null;
  richValues: RichValueSummary[];
  richValueDetails: Record<string, RichValue>;
  richKeyCursor: string | null;
  richValueCursor: string | null;
  loadingRichTimeline: boolean;
  loadingRichKeys: boolean;
  loadedMoreRichKeys: boolean;
  truncatedRichKeys: boolean;
  richDetailLoading: Set<string>;
  richDetailErrors: Record<string, string>;
  artifacts: SelectedRunArtifact[];
  artifactDetails: Record<string, Artifact>;
  artifactDetailLoading: Set<string>;
  artifactDetailErrors: Record<string, string>;
  artifactRunIds: string[];
  artifactCursors: Record<string, RunArtifactCursor | null>;
  loadedTabs: Set<RunTab>;
  loadingTabs: Set<RunTab>;
  errors: Partial<Record<RunTab, string>>;
  loadedMoreTabs: Set<RunTab>;
  truncatedTabs: Set<PaginatedRunTab>;
  loadingMoreTab: PaginatedRunTab | null;
};

export type RunResourceContext = {
  runId: string;
  artifactRunIds: string[];
  signal: AbortSignal;
};

type RunResourceApi = {
  getAlertPage(runId: string, before?: string, signal?: AbortSignal): Promise<CursorPage<Alert>>;
  getRichValueKeyPage(
    runId: string,
    after?: string,
    signal?: AbortSignal,
  ): Promise<{ items: RichValueKeySummary[]; nextAfter: string | null }>;
  getRichValuePage(
    runId: string,
    key: string,
    before?: string,
    signal?: AbortSignal,
  ): Promise<CursorPage<RichValueSummary>>;
  getRichValue(valueId: string, signal?: AbortSignal): Promise<RichValue>;
  getRunArtifactPage(
    runId: string,
    cursor?: RunArtifactCursor,
    signal?: AbortSignal,
  ): Promise<RunArtifactPage>;
  getArtifact(artifactId: string, signal?: AbortSignal): Promise<Artifact>;
};

const DEFAULT_API: RunResourceApi = {
  getAlertPage,
  getRichValueKeyPage,
  getRichValuePage,
  getRichValue,
  getRunArtifactPage,
  getArtifact,
};

export function emptyRunResourceState(loadedTabs: readonly RunTab[] = []): RunResourceState {
  return {
    alerts: [],
    alertCursor: null,
    richKeys: [],
    selectedRichKey: null,
    richValues: [],
    richValueDetails: {},
    richKeyCursor: null,
    richValueCursor: null,
    loadingRichTimeline: false,
    loadingRichKeys: false,
    loadedMoreRichKeys: false,
    truncatedRichKeys: false,
    richDetailLoading: new Set(),
    richDetailErrors: {},
    artifacts: [],
    artifactDetails: {},
    artifactDetailLoading: new Set(),
    artifactDetailErrors: {},
    artifactRunIds: [],
    artifactCursors: {},
    loadedTabs: new Set(loadedTabs),
    loadingTabs: new Set(),
    errors: {},
    loadedMoreTabs: new Set(),
    truncatedTabs: new Set(),
    loadingMoreTab: null,
  };
}

/** Owns bounded summary pages and selected full records for the active run. */
export class RunResourceController {
  private state = emptyRunResourceState();
  private generation = 0;
  private richSelectionGeneration = 0;
  private requestController = new AbortController();
  private artifactRequestController = new AbortController();
  private readonly detailScheduler = new BoundedRequestScheduler(
    MAX_ACTIVE_DETAIL_REQUESTS,
    MAX_PENDING_DETAIL_REQUESTS,
  );

  constructor(
    private readonly publish: (state: RunResourceState) => void,
    private readonly api: RunResourceApi = DEFAULT_API,
  ) {}

  reset(loadedTabs: readonly RunTab[] = []): void {
    this.restartRequests();
    this.restartArtifactRequests();
    this.generation += 1;
    this.richSelectionGeneration += 1;
    this.state = emptyRunResourceState(loadedTabs);
    this.emit();
  }

  async updateArtifactSelection(context: RunResourceContext, reload: boolean): Promise<void> {
    const runIds = boundedRunIds(context.artifactRunIds);
    if (sameValues(runIds, this.state.artifactRunIds)) return;
    this.restartArtifactRequests();
    this.state = {
      ...this.state,
      artifacts: [],
      artifactRunIds: runIds,
      artifactCursors: {},
      artifactDetailLoading: new Set(),
      artifactDetailErrors: {},
      loadedTabs: withoutValue(this.state.loadedTabs, "artifacts"),
      loadingTabs: withoutValue(this.state.loadingTabs, "artifacts"),
      loadedMoreTabs: withoutValue(this.state.loadedMoreTabs, "artifacts"),
      truncatedTabs: withoutValue(this.state.truncatedTabs, "artifacts"),
      loadingMoreTab: this.state.loadingMoreTab === "artifacts" ? null : this.state.loadingMoreTab,
      errors: withoutKey(this.state.errors, "artifacts"),
    };
    this.emit();
    if (reload && runIds.length > 0) {
      await this.ensureLoaded("artifacts", { ...context, artifactRunIds: runIds });
    }
  }

  async retry(tab: RunTab, context: RunResourceContext): Promise<void> {
    this.state = {
      ...this.state,
      loadedTabs: withoutValue(this.state.loadedTabs, tab),
      errors: withoutKey(this.state.errors, tab),
    };
    this.emit();
    await this.ensureLoaded(tab, context);
  }

  async ensureLoaded(tab: RunTab, context: RunResourceContext): Promise<void> {
    if (
      tab === "configuration" ||
      tab === "metrics" ||
      this.state.loadedTabs.has(tab) ||
      this.state.loadingTabs.has(tab)
    ) {
      return;
    }
    context = this.requestContext(context, tab === "artifacts");
    const generation = this.generation;
    this.state = {
      ...this.state,
      loadingTabs: withValue(this.state.loadingTabs, tab),
      errors: withoutKey(this.state.errors, tab),
    };
    this.emit();
    try {
      const patch = await this.loadNewest(tab, context);
      if (!this.isCurrent(generation, context.signal)) return;
      this.state = {
        ...this.state,
        ...patch,
        loadedTabs: withValue(this.state.loadedTabs, tab),
      };
    } catch (reason) {
      if (this.isCurrent(generation, context.signal)) this.setError(tab, reasonMessage(reason));
    } finally {
      if (this.isCurrent(generation, context.signal)) {
        this.state = {
          ...this.state,
          loadingTabs: withoutValue(this.state.loadingTabs, tab),
        };
        this.emit();
      }
    }
  }

  /** Marks hidden rich-resource tabs stale and refreshes the visible loaded tab in place. */
  async applyResourceRevision(tab: RunTab, context: RunResourceContext): Promise<string | null> {
    const revisionTabs = new Set<RunTab>(["summary", "media", "artifacts"]);
    const reloadActive =
      revisionTabs.has(tab) && (this.state.loadedTabs.has(tab) || this.state.loadingTabs.has(tab));
    this.restartRequests();
    this.generation += 1;
    this.richSelectionGeneration += 1;
    this.state = {
      ...this.state,
      loadedTabs: new Set([...this.state.loadedTabs].filter((loaded) => !revisionTabs.has(loaded))),
      loadingTabs: new Set(
        [...this.state.loadingTabs].filter((loading) => !revisionTabs.has(loading)),
      ),
      loadingRichTimeline: false,
      loadingRichKeys: false,
      richDetailLoading: new Set(),
      artifactDetailLoading: new Set(),
      loadingMoreTab: null,
    };
    this.emit();
    if (!reloadActive) return null;
    await this.ensureLoaded(tab, context);
    return this.state.errors[tab] ?? null;
  }

  async selectRichKey(key: string, context: RunResourceContext): Promise<void> {
    if (key === this.state.selectedRichKey && this.state.richValues.length > 0) return;
    context = this.requestContext(context);
    const generation = this.generation;
    const selectionGeneration = ++this.richSelectionGeneration;
    this.state = {
      ...this.state,
      selectedRichKey: key,
      richValues: [],
      richValueCursor: null,
      loadingRichTimeline: true,
      errors: withoutKey(this.state.errors, "media"),
      loadedMoreTabs: withoutValue(this.state.loadedMoreTabs, "media"),
      truncatedTabs: withoutValue(this.state.truncatedTabs, "media"),
    };
    this.emit();
    try {
      const page = await this.api.getRichValuePage(context.runId, key, undefined, context.signal);
      if (!this.isRichSelectionCurrent(generation, selectionGeneration, context.signal)) return;
      const retained = retainHeadAndTail(page.items, MAX_RETAINED_RICH_VALUES, (value) => value.id);
      this.state = {
        ...this.state,
        richValues: retained.items,
        richValueCursor: page.nextBefore,
        truncatedTabs: setTruncated(this.state.truncatedTabs, "media", retained.truncated),
      };
      this.emit();
      const latest = page.items[0];
      if (latest) void this.loadRichDetail(latest.id, context);
    } catch (reason) {
      if (this.isRichSelectionCurrent(generation, selectionGeneration, context.signal)) {
        this.setError("media", reasonMessage(reason));
      }
    } finally {
      if (this.isRichSelectionCurrent(generation, selectionGeneration, context.signal)) {
        this.state = { ...this.state, loadingRichTimeline: false };
        this.emit();
      }
    }
  }

  async loadMoreRichKeys(context: RunResourceContext): Promise<void> {
    const after = this.state.richKeyCursor;
    if (!after || this.state.loadingRichKeys) return;
    context = this.requestContext(context);
    const generation = this.generation;
    this.state = { ...this.state, loadingRichKeys: true };
    this.emit();
    try {
      const page = await this.api.getRichValueKeyPage(context.runId, after, context.signal);
      if (!this.isCurrent(generation, context.signal)) return;
      const retained = retainHeadAndTail(
        appendUniquePage(this.state.richKeys, page.items, (item) => item.key),
        MAX_RETAINED_RICH_KEYS,
        (item) => item.key,
        this.state.selectedRichKey ? new Set([this.state.selectedRichKey]) : new Set(),
      );
      this.state = {
        ...this.state,
        richKeys: retained.items,
        richKeyCursor: page.nextAfter,
        loadedMoreRichKeys: true,
        truncatedRichKeys: this.state.truncatedRichKeys || retained.truncated,
      };
    } catch (reason) {
      if (this.isCurrent(generation, context.signal)) {
        this.setError("media", reasonMessage(reason));
      }
    } finally {
      if (this.isCurrent(generation, context.signal)) {
        this.state = { ...this.state, loadingRichKeys: false };
        this.emit();
      }
    }
  }

  async loadRichDetail(valueId: string, context: RunResourceContext): Promise<void> {
    if (this.state.richValueDetails[valueId] || this.state.richDetailLoading.has(valueId)) return;
    context = this.requestContext(context);
    const generation = this.generation;
    this.state = {
      ...this.state,
      richDetailLoading: withValue(this.state.richDetailLoading, valueId),
      richDetailErrors: withoutStringKey(this.state.richDetailErrors, valueId),
    };
    this.emit();
    try {
      const value = await this.detailScheduler.run({
        identity: `rich:${valueId}`,
        parentSignal: context.signal,
        request: (signal) => this.api.getRichValue(valueId, signal),
      });
      if (!value) return;
      if (!this.isCurrent(generation, context.signal)) return;
      this.state = {
        ...this.state,
        richValueDetails: retainRecord(
          this.state.richValueDetails,
          valueId,
          value,
          MAX_RETAINED_DETAILS,
        ),
      };
    } catch (reason) {
      if (this.isCurrent(generation, context.signal)) {
        this.state = {
          ...this.state,
          richDetailErrors: retainRecord(
            this.state.richDetailErrors,
            valueId,
            reasonMessage(reason),
            MAX_RETAINED_DETAILS,
          ),
        };
      }
    } finally {
      if (this.isCurrent(generation, context.signal)) {
        this.state = {
          ...this.state,
          richDetailLoading: withoutValue(this.state.richDetailLoading, valueId),
        };
        this.emit();
      }
    }
  }

  async loadArtifactDetail(artifactId: string, context: RunResourceContext): Promise<void> {
    if (
      this.state.artifactDetails[artifactId] ||
      this.state.artifactDetailLoading.has(artifactId)
    ) {
      return;
    }
    context = this.requestContext(context);
    const generation = this.generation;
    this.state = {
      ...this.state,
      artifactDetailLoading: withValue(this.state.artifactDetailLoading, artifactId),
      artifactDetailErrors: withoutStringKey(this.state.artifactDetailErrors, artifactId),
    };
    this.emit();
    try {
      const artifact = await this.detailScheduler.run({
        identity: `artifact:${artifactId}`,
        parentSignal: context.signal,
        request: (signal) => this.api.getArtifact(artifactId, signal),
      });
      if (!artifact) return;
      if (!this.isCurrent(generation, context.signal)) return;
      this.state = {
        ...this.state,
        artifactDetails: retainRecord(
          this.state.artifactDetails,
          artifactId,
          artifact,
          MAX_RETAINED_DETAILS,
        ),
      };
    } catch (reason) {
      if (this.isCurrent(generation, context.signal)) {
        this.state = {
          ...this.state,
          artifactDetailErrors: retainRecord(
            this.state.artifactDetailErrors,
            artifactId,
            reasonMessage(reason),
            MAX_RETAINED_DETAILS,
          ),
        };
      }
    } finally {
      if (this.isCurrent(generation, context.signal)) {
        this.state = {
          ...this.state,
          artifactDetailLoading: withoutValue(this.state.artifactDetailLoading, artifactId),
        };
        this.emit();
      }
    }
  }

  async loadMore(tab: PaginatedRunTab, context: RunResourceContext): Promise<void> {
    const cursor = this.cursor(tab);
    if (!cursor || this.state.loadingMoreTab) return;
    context = this.requestContext(context, tab === "artifacts");
    const generation = this.generation;
    this.state = { ...this.state, loadingMoreTab: tab, errors: withoutKey(this.state.errors, tab) };
    this.emit();
    try {
      const patch =
        tab === "artifacts"
          ? await this.loadArtifactPages(context, false)
          : await this.loadPage(tab, cursor, context);
      if (!this.isCurrent(generation, context.signal)) return;
      this.state = {
        ...this.state,
        ...patch,
        loadedMoreTabs: withValue(this.state.loadedMoreTabs, tab),
      };
    } catch (reason) {
      if (this.isCurrent(generation, context.signal)) this.setError(tab, reasonMessage(reason));
    } finally {
      if (this.isCurrent(generation, context.signal)) {
        this.state = { ...this.state, loadingMoreTab: null };
        this.emit();
      }
    }
  }

  private async loadNewest(
    tab: Exclude<RunTab, "configuration" | "metrics">,
    context: RunResourceContext,
  ): Promise<Partial<RunResourceState>> {
    if (tab === "media") return this.loadNewestMedia(context);
    if (tab === "artifacts") return this.loadArtifactPages(context, true);
    return this.loadPage(tab, undefined, context, true);
  }

  private async loadNewestMedia(context: RunResourceContext): Promise<Partial<RunResourceState>> {
    const keyPage = await this.api.getRichValueKeyPage(context.runId, undefined, context.signal);
    const selectedKey =
      this.state.selectedRichKey &&
      [...keyPage.items, ...this.state.richKeys].some(
        (item) => item.key === this.state.selectedRichKey,
      )
        ? this.state.selectedRichKey
        : (keyPage.items[0]?.key ?? null);
    if (!selectedKey) {
      const retainedKeys = retainHeadAndTail(
        keyPage.items,
        MAX_RETAINED_RICH_KEYS,
        (item) => item.key,
      );
      return {
        richKeys: retainedKeys.items,
        richKeyCursor: keyPage.nextAfter,
        selectedRichKey: null,
        richValues: [],
        richValueCursor: null,
        truncatedRichKeys: retainedKeys.truncated,
        truncatedTabs: withoutValue(this.state.truncatedTabs, "media"),
      };
    }
    const valuePage = await this.api.getRichValuePage(
      context.runId,
      selectedKey,
      undefined,
      context.signal,
    );
    const sameSelection = selectedKey === this.state.selectedRichKey;
    const mergedKeys =
      this.state.richKeys.length > 0
        ? mergeNewestPage(this.state.richKeys, keyPage.items, (item) => item.key)
        : keyPage.items;
    const retainedKeys = retainHeadAndTail(
      mergedKeys,
      MAX_RETAINED_RICH_KEYS,
      (item) => item.key,
      new Set([selectedKey]),
    );
    const mergedValues =
      sameSelection && this.state.richValues.length > 0
        ? mergeNewestPage(this.state.richValues, valuePage.items, (value) => value.id)
        : valuePage.items;
    const retainedValues = retainHeadAndTail(
      mergedValues,
      MAX_RETAINED_RICH_VALUES,
      (value) => value.id,
    );
    return {
      richKeys: retainedKeys.items,
      richKeyCursor: this.state.loadedMoreRichKeys ? this.state.richKeyCursor : keyPage.nextAfter,
      truncatedRichKeys: this.state.truncatedRichKeys || retainedKeys.truncated,
      selectedRichKey: selectedKey,
      richValues: retainedValues.items,
      richValueCursor:
        sameSelection && this.state.loadedMoreTabs.has("media")
          ? this.state.richValueCursor
          : valuePage.nextBefore,
      truncatedTabs: setTruncated(
        this.state.truncatedTabs,
        "media",
        retainedValues.truncated || (sameSelection && this.state.truncatedTabs.has("media")),
      ),
    };
  }

  private async loadPage(
    tab: PaginatedRunTab,
    before: string | true | undefined,
    context: RunResourceContext,
    newest = false,
  ): Promise<Partial<RunResourceState>> {
    if (tab === "summary") {
      const page = await this.api.getAlertPage(
        context.runId,
        typeof before === "string" ? before : undefined,
        context.signal,
      );
      const retained = retainHeadAndTail(
        mergePage(this.state.alerts, page.items, (alert) => alert.id, newest),
        MAX_RETAINED_ALERTS,
        (alert) => alert.id,
      );
      return {
        alerts: retained.items,
        alertCursor:
          newest && this.state.loadedMoreTabs.has(tab) ? this.state.alertCursor : page.nextBefore,
        truncatedTabs: setTruncated(
          this.state.truncatedTabs,
          tab,
          retained.truncated || this.state.truncatedTabs.has(tab),
        ),
      };
    }
    if (tab === "media") {
      const key = this.state.selectedRichKey;
      if (!key) return {};
      const page = await this.api.getRichValuePage(
        context.runId,
        key,
        typeof before === "string" ? before : undefined,
        context.signal,
      );
      const retained = retainHeadAndTail(
        mergePage(this.state.richValues, page.items, (value) => value.id, newest),
        MAX_RETAINED_RICH_VALUES,
        (value) => value.id,
      );
      return {
        richValues: retained.items,
        richValueCursor:
          newest && this.state.loadedMoreTabs.has(tab)
            ? this.state.richValueCursor
            : page.nextBefore,
        truncatedTabs: setTruncated(
          this.state.truncatedTabs,
          tab,
          retained.truncated || this.state.truncatedTabs.has(tab),
        ),
      };
    }
    throw new Error(`unsupported paginated tab: ${tab}`);
  }

  private async loadArtifactPages(
    context: RunResourceContext,
    newest: boolean,
  ): Promise<Partial<RunResourceState>> {
    const runIds = boundedRunIds(context.artifactRunIds);
    const requestedRunIds = newest
      ? runIds
      : runIds.filter((runId) => this.state.artifactCursors[runId]);
    const pages = await Promise.all(
      requestedRunIds.map(async (runId) => ({
        runId,
        page: await this.api.getRunArtifactPage(
          runId,
          newest ? undefined : (this.state.artifactCursors[runId] ?? undefined),
          context.signal,
        ),
      })),
    );
    const incoming = groupArtifactLinks(
      pages.flatMap(({ runId, page }) => page.items.map((linked) => ({ runId, linked }))),
    );
    const sameSelection = sameValues(runIds, this.state.artifactRunIds);
    const merged =
      newest && !sameSelection ? incoming : mergeArtifactGroups(this.state.artifacts, incoming);
    const retained = retainHeadAndTail(
      merged,
      MAX_RETAINED_ARTIFACTS,
      (group) => group.artifact.id,
    );
    const artifactCursors = sameSelection ? { ...this.state.artifactCursors } : {};
    for (const { runId, page } of pages) {
      if (!(newest && sameSelection && this.state.loadedMoreTabs.has("artifacts"))) {
        artifactCursors[runId] = page.nextCursor;
      }
    }
    for (const runId of runIds) artifactCursors[runId] ??= null;
    return {
      artifacts: retained.items,
      artifactRunIds: runIds,
      artifactCursors,
      truncatedTabs: setTruncated(
        this.state.truncatedTabs,
        "artifacts",
        retained.truncated || (sameSelection && this.state.truncatedTabs.has("artifacts")),
      ),
    };
  }

  private cursor(tab: PaginatedRunTab): string | true | null {
    if (tab === "summary") return this.state.alertCursor;
    if (tab === "media") return this.state.richValueCursor;
    return Object.values(this.state.artifactCursors).some(Boolean) ? true : null;
  }

  private setError(tab: RunTab, message: string): void {
    this.state = { ...this.state, errors: { ...this.state.errors, [tab]: message } };
    this.emit();
  }

  private isCurrent(generation: number, signal: AbortSignal): boolean {
    return generation === this.generation && !signal.aborted;
  }

  private requestContext(context: RunResourceContext, artifacts = false): RunResourceContext {
    const signals = [context.signal, this.requestController.signal];
    if (artifacts) signals.push(this.artifactRequestController.signal);
    return {
      ...context,
      signal: AbortSignal.any(signals),
    };
  }

  private restartRequests(): void {
    this.requestController.abort();
    this.requestController = new AbortController();
    this.detailScheduler.cancelAll();
  }

  private restartArtifactRequests(): void {
    this.artifactRequestController.abort();
    this.artifactRequestController = new AbortController();
  }

  private isRichSelectionCurrent(
    generation: number,
    selectionGeneration: number,
    signal: AbortSignal,
  ): boolean {
    return (
      this.isCurrent(generation, signal) && selectionGeneration === this.richSelectionGeneration
    );
  }

  private emit(): void {
    this.publish(this.state);
  }
}

function mergePage<T>(
  current: readonly T[],
  page: readonly T[],
  identity: (value: T) => string,
  newest: boolean,
): T[] {
  if (newest) return current.length > 0 ? mergeNewestPage(current, page, identity) : [...page];
  return appendUniquePage(current, page, identity);
}

function groupArtifactLinks(
  values: ReadonlyArray<{ runId: string; linked: RunArtifact }>,
): SelectedRunArtifact[] {
  const groups = new Map<string, SelectedRunArtifact>();
  for (const { runId, linked } of values) {
    const group = groups.get(linked.artifact.id) ?? {
      artifact: linked.artifact,
      links: [],
    };
    if (!group.links.some((link) => link.runId === runId && link.relation === linked.relation)) {
      group.links.push({ runId, relation: linked.relation });
    }
    groups.set(linked.artifact.id, group);
  }
  return [...groups.values()].sort(compareArtifactGroups);
}

function mergeArtifactGroups(
  current: readonly SelectedRunArtifact[],
  incoming: readonly SelectedRunArtifact[],
): SelectedRunArtifact[] {
  const groups = new Map(current.map((group) => [group.artifact.id, group]));
  for (const next of incoming) {
    const existing = groups.get(next.artifact.id);
    if (!existing) {
      groups.set(next.artifact.id, next);
      continue;
    }
    const links = [...existing.links];
    for (const link of next.links) {
      if (
        !links.some(
          (candidate) => candidate.runId === link.runId && candidate.relation === link.relation,
        )
      ) {
        links.push(link);
      }
    }
    groups.set(next.artifact.id, { artifact: next.artifact, links });
  }
  return [...groups.values()].sort(compareArtifactGroups);
}

function compareArtifactGroups(left: SelectedRunArtifact, right: SelectedRunArtifact): number {
  return (
    right.artifact.created_at.localeCompare(left.artifact.created_at) ||
    right.artifact.id.localeCompare(left.artifact.id)
  );
}

function boundedRunIds(values: readonly string[]): string[] {
  return [...new Set(values)].slice(0, MAX_SELECTED_RUNS);
}

function sameValues(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function withValue<T>(values: ReadonlySet<T>, value: T): Set<T> {
  return new Set([...values, value]);
}

function withoutValue<T>(values: ReadonlySet<T>, value: T): Set<T> {
  const next = new Set(values);
  next.delete(value);
  return next;
}

function withoutKey<K extends PropertyKey, V>(
  values: Partial<Record<K, V>>,
  key: K,
): Partial<Record<K, V>> {
  const next = { ...values };
  delete next[key];
  return next;
}

function withoutStringKey<V>(values: Record<string, V>, key: string): Record<string, V> {
  const next = { ...values };
  delete next[key];
  return next;
}

function setTruncated(
  values: ReadonlySet<PaginatedRunTab>,
  tab: PaginatedRunTab,
  truncated: boolean,
): Set<PaginatedRunTab> {
  return truncated ? withValue(values, tab) : withoutValue(values, tab);
}
