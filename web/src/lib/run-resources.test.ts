import { describe, expect, it, vi } from "vitest";

import type {
  Alert,
  Artifact,
  CursorPage,
  RichValue,
  RichValueKeySummary,
  RichValueSummary,
  RunArtifactCursor,
  RunArtifactPage,
} from "./api";
import { RunResourceController, type RunResourceState } from "./run-resources";

describe("RunResourceController", () => {
  it("loads a tab once and exposes local loading state", async () => {
    const states: RunResourceState[] = [];
    const getAlertPage = vi.fn(async () => ({ items: [alert("alert-1")], nextBefore: null }));
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage }),
    );
    const signal = new AbortController().signal;

    await controller.ensureLoaded("summary", { runId: "run-1", artifactRunIds: ["run-1"], signal });
    await controller.ensureLoaded("summary", { runId: "run-1", artifactRunIds: ["run-1"], signal });

    expect(getAlertPage).toHaveBeenCalledOnce();
    expect(states.some((state) => state.loadingTabs.has("summary"))).toBe(true);
    expect(states.at(-1)?.alerts.map((value) => value.id)).toEqual(["alert-1"]);
    expect(states.at(-1)?.loadedTabs.has("summary")).toBe(true);
  });

  it("keeps older alert pages reachable while merging a live newest page", async () => {
    const states: RunResourceState[] = [];
    const getAlertPage = vi
      .fn<() => Promise<CursorPage<Alert>>>()
      .mockResolvedValueOnce({ items: [alert("current")], nextBefore: "older-page" })
      .mockResolvedValueOnce({ items: [alert("older")], nextBefore: null })
      .mockResolvedValueOnce({
        items: [alert("newest"), alert("current")],
        nextBefore: "ignored-live-cursor",
      });
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    await controller.ensureLoaded("summary", context);
    await controller.loadMore("summary", context);
    await controller.applyResourceRevision("summary", context);

    expect(states.at(-1)?.alerts.map((value) => value.id)).toEqual(["newest", "current", "older"]);
    expect(states.at(-1)?.alertCursor).toBeNull();
  });

  it("keeps older cursor pages while merging a live newest page", async () => {
    const states: RunResourceState[] = [];
    const first = richValue("first", 2);
    const older = richValue("older", 1);
    const newest = richValue("newest", 3);
    const image = { ...richValue("image", 1), key: "image" };
    const getRichValueKeyPage = vi
      .fn<() => Promise<{ items: RichValueKeySummary[]; nextAfter: string | null }>>()
      .mockResolvedValueOnce({ items: [richKey(first)], nextAfter: "video" })
      .mockResolvedValueOnce({ items: [richKey(image)], nextAfter: null })
      .mockResolvedValueOnce({ items: [richKey(newest)], nextAfter: "video" });
    const getRichValuePage = vi
      .fn<() => Promise<CursorPage<RichValueSummary>>>()
      .mockResolvedValueOnce({ items: [first], nextBefore: "older-page" })
      .mockResolvedValueOnce({ items: [older], nextBefore: null })
      .mockResolvedValueOnce({ items: [newest, first], nextBefore: "ignored-live-cursor" });
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getRichValueKeyPage, getRichValuePage }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    await controller.ensureLoaded("media", context);
    await controller.loadMoreRichKeys(context);
    await controller.loadMore("media", context);
    await controller.applyResourceRevision("media", context);

    const current = states.at(-1)!;
    expect(current.richValues.map((value) => value.id)).toEqual(["newest", "first", "older"]);
    expect(current.richValueCursor).toBeNull();
    expect(current.richKeyCursor).toBeNull();
    expect(current.loadedMoreRichKeys).toBe(true);
    expect(current.richKeys.map((value) => value.key)).toEqual(["video", "image"]);
    expect(current.loadedMoreTabs.has("media")).toBe(true);
  });

  it("marks hidden resource tabs stale after the unified resource revision changes", async () => {
    const states: RunResourceState[] = [];
    const getAlertPage = vi.fn(async () => ({
      items: [alert(`alert-${getAlertPage.mock.calls.length}`)],
      nextBefore: null,
    }));
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    await controller.ensureLoaded("summary", context);
    await controller.applyResourceRevision("metrics", context);
    expect(states.at(-1)?.loadedTabs.has("summary")).toBe(false);

    await controller.ensureLoaded("summary", context);
    expect(getAlertPage).toHaveBeenCalledTimes(2);
    expect(states.at(-1)?.loadedTabs.has("summary")).toBe(true);
  });

  it("filters and deduplicates artifacts across the selected runs", async () => {
    const states: RunResourceState[] = [];
    const shared = artifact("shared");
    const getRunArtifactPage = vi.fn(async (runId: string) => ({
      items:
        runId === "run-a"
          ? [artifactLink(shared, "output"), artifactLink(artifact("only-a"), "output")]
          : [artifactLink(shared, "input"), artifactLink(artifact("only-b"), "output")],
      nextCursor: null,
    }));
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getRunArtifactPage }),
    );
    const signal = new AbortController().signal;

    await controller.ensureLoaded("artifacts", {
      runId: "run-a",
      artifactRunIds: ["run-a", "run-b"],
      signal,
    });

    const sharedGroup = states.at(-1)?.artifacts.find((group) => group.artifact.id === "shared");
    expect(getRunArtifactPage.mock.calls.map(([runId]) => runId)).toEqual(["run-a", "run-b"]);
    expect(states.at(-1)?.artifacts).toHaveLength(3);
    expect(sharedGroup?.links).toEqual([
      { runId: "run-a", relation: "output" },
      { runId: "run-b", relation: "input" },
    ]);

    await controller.updateArtifactSelection(
      { runId: "run-b", artifactRunIds: ["run-b"], signal },
      true,
    );
    expect(states.at(-1)?.artifactRunIds).toEqual(["run-b"]);
    expect(
      states
        .at(-1)
        ?.artifacts.map((group) => group.artifact.id)
        .sort(),
    ).toEqual(["only-b", "shared"]);
  });

  it("loads full records only for an explicitly selected summary", async () => {
    const states: RunResourceState[] = [];
    const value = richValue("value-1", 4);
    const getRichValue = vi.fn(async () => ({ ...value, metadata: { caption: "preview" } }));
    const getArtifact = vi.fn(async () => artifact("artifact-1"));
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getRichValue, getArtifact }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    await controller.loadRichDetail("value-1", context);
    await controller.loadArtifactDetail("artifact-1", context);

    expect(getRichValue).toHaveBeenCalledOnce();
    expect(getArtifact).toHaveBeenCalledOnce();
    expect(states.at(-1)?.richValueDetails["value-1"]?.metadata).toEqual({
      caption: "preview",
    });
    expect(states.at(-1)?.artifactDetails["artifact-1"]?.entries).toEqual([]);
  });

  it("bounds cursor-fed rows and selected-detail caches", async () => {
    const states: RunResourceState[] = [];
    let alertPage = 0;
    const getAlertPage = vi.fn(async () => {
      const page = alertPage++;
      return {
        items: Array.from({ length: 100 }, (_, index) => alert(`alert-${page}-${index}`)),
        nextBefore: page < 5 ? `cursor-${page + 1}` : null,
      };
    });
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    await controller.ensureLoaded("summary", context);
    for (let page = 0; page < 5; page += 1) await controller.loadMore("summary", context);
    for (let index = 0; index < 30; index += 1) {
      await controller.loadRichDetail(`value-${index}`, context);
      await controller.loadArtifactDetail(`artifact-${index}`, context);
    }

    const state = states.at(-1)!;
    expect(state.alerts).toHaveLength(500);
    expect(state.truncatedTabs.has("summary")).toBe(true);
    expect(Object.keys(state.richValueDetails)).toHaveLength(24);
    expect(Object.keys(state.artifactDetails)).toHaveLength(24);
    expect(state.richValueDetails["value-0"]).toBeUndefined();
    expect(state.richValueDetails["value-29"]).toBeDefined();
  });

  it("does not publish a request completed after the active run resets", async () => {
    const states: RunResourceState[] = [];
    let resolveAlerts: ((value: Alert[]) => void) | undefined;
    const pendingAlerts = new Promise<Alert[]>((resolve) => {
      resolveAlerts = resolve;
    });
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage: async () => ({ items: await pendingAlerts, nextBefore: null }) }),
    );
    const context = {
      runId: "old-run",
      artifactRunIds: ["old-run"],
      signal: new AbortController().signal,
    };

    const request = controller.ensureLoaded("summary", context);
    controller.reset(["configuration", "metrics"]);
    resolveAlerts?.([alert("stale")]);
    await request;

    expect(states.at(-1)?.alerts).toEqual([]);
    expect(states.at(-1)?.loadedTabs).toEqual(new Set(["configuration", "metrics"]));
  });

  it("restarts an in-flight active resource tab when its revision advances", async () => {
    const states: RunResourceState[] = [];
    let resolveStale!: (value: CursorPage<Alert>) => void;
    const stalePage = new Promise<CursorPage<Alert>>((resolve) => {
      resolveStale = resolve;
    });
    const getAlertPage = vi
      .fn<() => Promise<CursorPage<Alert>>>()
      .mockImplementationOnce(() => stalePage)
      .mockResolvedValueOnce({ items: [alert("fresh")], nextBefore: null });
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getAlertPage }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    const initial = controller.ensureLoaded("summary", context);
    await vi.waitFor(() => expect(getAlertPage).toHaveBeenCalledOnce());
    await controller.applyResourceRevision("summary", context);
    resolveStale({ items: [alert("stale")], nextBefore: null });
    await initial;

    expect(getAlertPage).toHaveBeenCalledTimes(2);
    expect(states.at(-1)?.alerts.map((value) => value.id)).toEqual(["fresh"]);
    expect(states.at(-1)?.loadedTabs.has("summary")).toBe(true);
  });

  it("bounds active and pending full-detail requests", async () => {
    const states: RunResourceState[] = [];
    const getRichValue = vi.fn(() => new Promise<RichValue>(() => {}));
    const controller = new RunResourceController(
      (state) => states.push(state),
      fakeApi({ getRichValue }),
    );
    const context = {
      runId: "run-1",
      artifactRunIds: ["run-1"],
      signal: new AbortController().signal,
    };

    for (let index = 0; index < 40; index += 1) {
      void controller.loadRichDetail(`value-${index}`, context);
    }

    await vi.waitFor(() => expect(getRichValue).toHaveBeenCalledTimes(4));
    await vi.waitFor(() => expect(states.at(-1)?.richDetailLoading.size).toBe(28));
    controller.reset();
    expect(states.at(-1)?.richDetailLoading.size).toBe(0);
  });
});

function fakeApi(
  overrides: Partial<{
    getAlertPage(runId: string, before?: string, signal?: AbortSignal): Promise<CursorPage<Alert>>;
    getRichValuePage(
      runId: string,
      key: string,
      before?: string,
      signal?: AbortSignal,
    ): Promise<CursorPage<RichValueSummary>>;
    getRichValueKeyPage(
      runId: string,
      after?: string,
      signal?: AbortSignal,
    ): Promise<{ items: RichValueKeySummary[]; nextAfter: string | null }>;
    getRichValue(valueId: string, signal?: AbortSignal): Promise<RichValue>;
    getRunArtifactPage(
      runId: string,
      cursor?: RunArtifactCursor,
      signal?: AbortSignal,
    ): Promise<RunArtifactPage>;
    getArtifact(artifactId: string, signal?: AbortSignal): Promise<Artifact>;
  }> = {},
) {
  return {
    getAlertPage: async () => ({ items: [], nextBefore: null }),
    getRichValueKeyPage: async () => ({ items: [], nextAfter: null }),
    getRichValuePage: async () => ({ items: [], nextBefore: null }),
    getRichValue: async (valueId: string) => ({ ...richValue(valueId, 0), metadata: {} }),
    getRunArtifactPage: async () => ({ items: [], nextCursor: null }),
    getArtifact: async (artifactId: string) => artifact(artifactId),
    ...overrides,
  };
}

function alert(id: string): Alert {
  return {
    id,
    run_id: "run-1",
    title: "Alert",
    text: "Details",
    level: "info",
    step: 1,
    timestamp_ms: 1,
    created_at: "2026-08-30T00:00:00Z",
  };
}

function richValue(id: string, step: number): RichValueSummary {
  return {
    id,
    run_id: "run-1",
    key: "video",
    kind: "video",
    step,
    timestamp_ms: step,
    blob: null,
    created_at: "2026-08-30T00:00:00Z",
  };
}

function richKey(latest: RichValueSummary): RichValueKeySummary {
  return { key: latest.key, count: 3, latest };
}

function artifact(id: string): Artifact {
  return {
    id,
    project_id: "project-1",
    project: "demo",
    name: "checkpoint",
    type: "model",
    version: 1,
    description: null,
    metadata: {},
    aliases: [],
    entries: [],
    created_by_run: "run-1",
    created_at: "2026-08-30T00:00:00Z",
  };
}

function artifactLink(value: Artifact, relation: "input" | "output") {
  return {
    relation,
    artifact: {
      id: value.id,
      project_id: value.project_id,
      project: value.project,
      name: value.name,
      type: value.type,
      version: value.version,
      entry_count: value.entries.length,
      created_by_run: value.created_by_run,
      created_at: value.created_at,
    },
  };
}
