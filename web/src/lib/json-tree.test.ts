import { describe, expect, it } from "vitest";

import {
  JSON_TREE_PAGE_SIZE,
  JSON_TREE_SEARCH_MAX_LENGTH,
  jsonTreeScalarText,
  nodeChildCount,
  normalizeJsonTreeSearch,
  searchJsonTree,
  visibleChildEntries,
} from "./json-tree";

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

  it("finds keys and scalar values while retaining their ancestor paths", () => {
    const value = {
      training: {
        optimizer: {
          algorithm: "AdamW",
          learning_rate: 0.001,
        },
        batch_size: 64,
      },
      state: "finished",
    };

    const valueMatch = searchJsonTree(value, "ADAMW");
    expect(valueMatch?.matchCount).toBe(1);
    expect(valueMatch?.children.map((child) => child.name)).toEqual(["training"]);
    expect(valueMatch?.children[0].match.children.map((child) => child.name)).toEqual([
      "optimizer",
    ]);
    expect(
      valueMatch?.children[0].match.children[0].match.children.map((child) => child.name),
    ).toEqual(["algorithm"]);
    expect(valueMatch?.children[0].match.children[0].match.children[0].match.valueMatches).toBe(
      true,
    );

    const keyMatch = searchJsonTree(value, "learning_rate");
    expect(keyMatch?.matchCount).toBe(1);
    expect(keyMatch?.children[0].match.children[0].match.children[0].match.keyMatches).toBe(true);
    expect(searchJsonTree(value, "object object")).toBeNull();
  });

  it("normalizes and bounds document searches to the dashboard input limit", () => {
    const oversized = `  ${"A".repeat(JSON_TREE_SEARCH_MAX_LENGTH + 20)}  `;
    expect(normalizeJsonTreeSearch(oversized)).toBe("a".repeat(JSON_TREE_SEARCH_MAX_LENGTH));
    expect(normalizeJsonTreeSearch("   ")).toBe("");
  });

  it("preserves exact JavaScript numeric values for display, copy, and search", () => {
    expect(jsonTreeScalarText(1e-10)).toBe("1e-10");
    expect(jsonTreeScalarText(1.2345678912)).toBe("1.2345678912");
    expect(jsonTreeScalarText(9_007_199_254_740_991)).toBe("9007199254740991");

    const document = { tiny: 1e-10, precise: 1.2345678912 };
    expect(searchJsonTree(document, "1e-10")?.children[0].name).toBe("tiny");
    expect(searchJsonTree(document, "1.2345678912")?.children[0].name).toBe("precise");
  });
});
