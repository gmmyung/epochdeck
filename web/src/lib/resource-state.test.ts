import { describe, expect, it } from "vitest";

import {
  appendUniquePage,
  formatDurationMs,
  mergeNewestPage,
  reasonMessage,
} from "./resource-state";

describe("resource state", () => {
  it("refreshes the newest page without dropping previously loaded records", () => {
    const current = [{ id: "new" }, { id: "middle" }, { id: "old" }];
    const refreshed = [{ id: "latest" }, { id: "new" }];

    expect(mergeNewestPage(current, refreshed, (value) => value.id)).toEqual([
      { id: "latest" },
      { id: "new" },
      { id: "middle" },
      { id: "old" },
    ]);
  });

  it("deduplicates overlapping cursor pages", () => {
    expect(
      appendUniquePage([{ id: "a" }, { id: "b" }], [{ id: "b" }, { id: "c" }], (value) => value.id),
    ).toEqual([{ id: "a" }, { id: "b" }, { id: "c" }]);
  });

  it("formats elapsed durations with units", () => {
    expect(formatDurationMs(250)).toBe("250 ms");
    expect(formatDurationMs(1_500)).toBe("1.5 s");
    expect(formatDurationMs(90_000)).toBe("1.5 min");
    expect(formatDurationMs(7_200_000)).toBe("2 h");
  });

  it("uses concrete errors and a stable fallback", () => {
    expect(reasonMessage(new Error("broken"))).toBe("broken");
    expect(reasonMessage(null)).toBe("Unable to reach EpochDeck");
  });
});
