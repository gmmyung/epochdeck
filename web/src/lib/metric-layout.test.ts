import { describe, expect, it } from "vitest";

import { readMetricColumnCount, rememberMetricColumnCount } from "./metric-layout";

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

describe("metric layout preferences", () => {
  it("persists supported column counts and rejects malformed storage", () => {
    const storage = new MemoryStorage();
    expect(readMetricColumnCount(storage)).toBe("auto");
    expect(rememberMetricColumnCount("3", storage)).toBe("3");
    expect(readMetricColumnCount(storage)).toBe("3");

    storage.setItem("epochdeck:metric-columns", "100");
    expect(readMetricColumnCount(storage)).toBe("auto");
  });
});
