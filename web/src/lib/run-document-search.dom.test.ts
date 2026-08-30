// @vitest-environment happy-dom

import { mount, tick, unmount } from "svelte";
import { describe, expect, it, vi } from "vitest";

import RunDocumentPanels from "./RunDocumentPanels.svelte";
import type { Run } from "./api";
import { JSON_TREE_SEARCH_MAX_LENGTH } from "./json-tree";

describe("run document search", () => {
  it("filters summary scalar matches, expands their ancestors, and reports no matches locally", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(RunDocumentPanels, {
      target,
      props: panelProps(runDocument(), "summary"),
    });
    await tick();

    const panel = target.querySelector<HTMLElement>("#run-panel-summary")!;
    const input = panel.querySelector<HTMLInputElement>('[aria-label="Search summary"]')!;
    expect(input.maxLength).toBe(JSON_TREE_SEARCH_MAX_LENGTH);
    input.value = "COSINE";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();

    expect(panel.textContent).toContain("training");
    expect(panel.textContent).toContain("optimizer");
    expect(panel.textContent).toContain("schedule");
    expect(panel.textContent).toContain("Cosine annealing");
    expect(panel.textContent).not.toContain("finished");
    expect(panel.textContent).not.toContain("batch_size");
    expect(
      [...panel.querySelectorAll('[aria-expanded="true"]')].map((node) => node.textContent),
    ).toHaveLength(4);
    expect(panel.querySelector(".tree-search-match")?.textContent).toContain("Cosine annealing");
    expect(fetchMock).not.toHaveBeenCalled();

    input.value = "not-present";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();
    expect(panel.querySelector('[role="status"]')?.textContent).toContain(
      "No summary keys or values match this search.",
    );
    expect(panel.querySelector(".tree-panel")).toBeNull();

    await unmount(component);
    target.remove();
    vi.unstubAllGlobals();
  });

  it("searches configuration keys and values and clamps programmatic input", async () => {
    const target = document.createElement("div");
    document.body.append(target);
    const component = mount(RunDocumentPanels, {
      target,
      props: panelProps(runDocument(), "configuration"),
    });
    await tick();

    const panel = target.querySelector<HTMLElement>("#run-panel-configuration")!;
    const input = panel.querySelector<HTMLInputElement>('[aria-label="Search configuration"]')!;
    input.value = "optimizer";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();

    expect(panel.textContent).toContain("optimizer");
    expect(panel.textContent).toContain("algorithm");
    expect(panel.textContent).toContain("AdamW");
    expect(panel.textContent).not.toContain("dataset");
    expect(panel.querySelector(".tree-search-match")?.textContent).toBe("optimizer");

    input.value = "0.001";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();
    expect(panel.textContent).toContain("learning_rate");
    expect(panel.textContent).toContain("0.001");
    expect(panel.textContent).not.toContain("algorithm");

    input.value = "x".repeat(JSON_TREE_SEARCH_MAX_LENGTH + 40);
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();
    expect(input.value).toHaveLength(JSON_TREE_SEARCH_MAX_LENGTH);
    expect(panel.textContent).toContain("No configuration keys or values match this search.");

    await unmount(component);
    target.remove();
  });
});

function panelProps(run: Run, activeTab: "summary" | "configuration") {
  return {
    run,
    activeTab,
    alerts: [],
    alertCursor: null,
    alertsTruncated: false,
    alertError: undefined,
    loadingMoreTab: null,
    onretryalerts: vi.fn(),
    onloadalerts: vi.fn(),
  };
}

function runDocument(): Run {
  return {
    id: "run-id",
    project_id: "project-id",
    project: "demo",
    name: "searchable-run",
    state: "finished",
    config: {
      optimizer: {
        algorithm: "AdamW",
        learning_rate: 0.001,
      },
      dataset: "imagenet",
    },
    summary: {
      training: {
        optimizer: {
          schedule: {
            policy: "Cosine annealing",
            warmup_steps: 100,
          },
          algorithm: "AdamW",
        },
        batch_size: 64,
      },
      final_state: "finished",
    },
    explicit_summary: {},
    metric_summary: {},
    summary_truncated: false,
    document_revision: 1,
    metric_revision: 1,
    rich_data_revision: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:01:00Z",
    finished_at: "2026-01-01T00:01:00Z",
  };
}
