import { describe, expect, it } from "vitest";

import { retainHeadAndTail, retainRecord } from "./retained-window";

describe("retainHeadAndTail", () => {
  it("keeps recent rows, cursor-adjacent rows, and an explicitly pinned middle row", () => {
    const values = Array.from({ length: 20 }, (_, index) => ({ id: `row-${index}` }));

    const retained = retainHeadAndTail(values, 8, (value) => value.id, new Set(["row-10"]), 3);

    expect(retained.truncated).toBe(true);
    expect(retained.items.map((value) => value.id)).toEqual([
      "row-0",
      "row-1",
      "row-2",
      "row-10",
      "row-16",
      "row-17",
      "row-18",
      "row-19",
    ]);
  });

  it("deduplicates without reporting truncation when the unique view fits", () => {
    const retained = retainHeadAndTail(
      [{ id: "a" }, { id: "a" }, { id: "b" }],
      2,
      (value) => value.id,
    );

    expect(retained).toEqual({ items: [{ id: "a" }, { id: "b" }], truncated: false });
  });
});

describe("retainRecord", () => {
  it("uses insertion order as a small LRU while preserving pinned records", () => {
    const retained = retainRecord(
      { pinned: 0, old: 1, recent: 2 },
      "new",
      3,
      3,
      new Set(["pinned"]),
    );

    expect(retained).toEqual({ pinned: 0, recent: 2, new: 3 });
  });
});
