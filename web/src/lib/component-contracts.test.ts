import { render } from "svelte/server";
import { describe, expect, it } from "vitest";

import ArtifactBrowser from "./ArtifactBrowser.svelte";
import HistogramChart from "./HistogramChart.svelte";
import JsonTreeNode from "./JsonTreeNode.svelte";
import type { Artifact, RunArtifact } from "./api";

describe("dashboard component contracts", () => {
  it("keeps very wide JSON roots collapsed instead of instantiating every leaf", () => {
    const value = Object.fromEntries(
      Array.from({ length: 150 }, (_, index) => [`oversized-key-${index}`, index]),
    );
    const { body } = render(JsonTreeNode, { props: { name: "", value, root: true } });

    expect(body).toContain("object · 150");
    expect(body).not.toContain("oversized-key-149");
    expect(body).toContain('aria-expanded="false"');
  });

  it("offers a semantic table alongside the histogram canvas", () => {
    const { body } = render(HistogramChart, {
      props: { label: "reward distribution", counts: [2, 5, 3] },
    });

    expect(body).toContain("reward distribution histogram with 3 bins");
    expect(body).toContain("View histogram values · 3 bins");
    expect(body).toContain("<table");
  });

  it("bounds artifact file rows while preserving an explicit continuation control", () => {
    const detail: Artifact = {
      id: "artifact-id",
      project_id: "project-id",
      project: "demo",
      name: "checkpoint",
      type: "model",
      version: 1,
      description: null,
      metadata: {},
      aliases: [],
      entries: Array.from({ length: 250 }, (_, index) => ({
        path: `file-${String(index).padStart(3, "0")}.bin`,
        blob: {
          digest: `digest-${index}`,
          size: index,
          mime_type: "application/octet-stream",
          file_name: null,
        },
      })),
      created_by_run: "run-id",
      created_at: "2026-01-01T00:00:00Z",
    };
    const linked: RunArtifact = {
      relation: "output",
      artifact: {
        id: detail.id,
        project_id: detail.project_id,
        project: detail.project,
        name: detail.name,
        type: detail.type,
        version: detail.version,
        entry_count: detail.entries.length,
        created_by_run: detail.created_by_run,
        created_at: detail.created_at,
      },
    };
    const { body } = render(ArtifactBrowser, {
      props: {
        artifacts: [
          { artifact: linked.artifact, links: [{ runId: "run-id", relation: "output" }] },
        ],
        runNames: { "run-id": "training-run" },
        details: { "artifact-id": detail },
      },
    });

    expect(body).toContain("file-199.bin");
    expect(body).not.toContain("file-249.bin");
    expect(body).toMatch(/1–200 of 250/);
    expect(body).toContain('aria-pressed="false"');
  });
});
