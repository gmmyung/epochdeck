import { describe, expect, it } from "vitest";

import { JSON_TREE_PAGE_SIZE, nodeChildCount, visibleChildEntries } from "./json-tree";

describe("bounded JSON tree", () => {
  it("counts branches without treating scalar values as collections", () => {
    expect(nodeChildCount([1, 2, 3])).toBe(3);
    expect(nodeChildCount({ a: 1, b: 2 })).toBe(2);
    expect(nodeChildCount("value")).toBe(0);
  });

  it("materializes only the requested child page", () => {
    const value = Object.fromEntries(
      Array.from({ length: JSON_TREE_PAGE_SIZE + 50 }, (_, index) => [`key-${index}`, index]),
    );
    const entries = visibleChildEntries(value, JSON_TREE_PAGE_SIZE);

    expect(entries).toHaveLength(JSON_TREE_PAGE_SIZE);
    expect(entries.at(-1)).toEqual(["key-99", 99]);
    expect(visibleChildEntries(value, 2, 100)).toEqual([
      ["key-100", 100],
      ["key-101", 101],
    ]);
  });
});
