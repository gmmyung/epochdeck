import { describe, expect, it } from "vitest";

import { pushMetricCursor } from "./metric-pagination";

describe("pushMetricCursor", () => {
  it("keeps a bounded back-history and reports when older cursors were dropped", () => {
    let history: Array<string | null> = [];
    let truncated = false;
    for (let index = 0; index < 7; index += 1) {
      const next = pushMetricCursor(history, index === 0 ? null : `metric-${index}`, 4);
      history = next.history;
      truncated ||= next.truncated;
    }

    expect(history).toEqual(["metric-3", "metric-4", "metric-5", "metric-6"]);
    expect(truncated).toBe(true);
  });
});
