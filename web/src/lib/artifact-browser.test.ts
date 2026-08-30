import { describe, expect, it } from "vitest";

import type { ArtifactEntry } from "./api";
import {
  ARTIFACT_ITEM_PAGE_SIZE,
  artifactBreadcrumbs,
  artifactDirectoryItems,
  artifactItemPage,
  artifactTotalSize,
} from "./artifact-browser";

function entry(path: string, size: number): ArtifactEntry {
  return {
    path,
    blob: {
      digest: `${path}-digest`,
      size,
      mime_type: "application/octet-stream",
      file_name: path.split("/").at(-1) ?? null,
    },
  };
}

describe("artifactDirectoryItems", () => {
  const entries = [
    entry("checkpoint/params/a.bin", 10),
    entry("checkpoint/params/b.bin", 20),
    entry("checkpoint/state.json", 5),
    entry("README.md", 2),
  ];

  it("returns immediate directories before files with recursive counts and sizes", () => {
    expect(artifactDirectoryItems(entries, "")).toEqual([
      { kind: "directory", name: "checkpoint", path: "checkpoint", fileCount: 3, size: 35 },
      { kind: "file", name: "README.md", path: "README.md", entry: entries[3], size: 2 },
    ]);
    expect(artifactDirectoryItems(entries, "checkpoint")).toEqual([
      {
        kind: "directory",
        name: "params",
        path: "checkpoint/params",
        fileCount: 2,
        size: 30,
      },
      {
        kind: "file",
        name: "state.json",
        path: "checkpoint/state.json",
        entry: entries[2],
        size: 5,
      },
    ]);
  });

  it("builds root-to-directory breadcrumbs and totals bytes", () => {
    expect(artifactBreadcrumbs("/checkpoint//params/")).toEqual([
      { label: "Files", path: "" },
      { label: "checkpoint", path: "checkpoint" },
      { label: "params", path: "checkpoint/params" },
    ]);
    expect(artifactTotalSize(entries)).toBe(37);
  });

  it("bounds the number of file rows instantiated at once", () => {
    const items = artifactDirectoryItems(
      Array.from({ length: ARTIFACT_ITEM_PAGE_SIZE + 50 }, (_, index) =>
        entry(`file-${index}.bin`, index),
      ),
      "",
    );

    expect(artifactItemPage(items, ARTIFACT_ITEM_PAGE_SIZE)).toHaveLength(ARTIFACT_ITEM_PAGE_SIZE);
    expect(artifactItemPage(items, ARTIFACT_ITEM_PAGE_SIZE, ARTIFACT_ITEM_PAGE_SIZE)).toHaveLength(
      50,
    );
  });
});
