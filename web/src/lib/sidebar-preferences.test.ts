import { describe, expect, it } from "vitest";

import { runStyle } from "./comparison-state";
import {
  DEFAULT_SIDEBAR_WIDTH,
  MAX_SIDEBAR_WIDTH,
  MIN_SIDEBAR_WIDTH,
  clampSidebarWidth,
  forgetRunStylePreference,
  readSidebarCollapsed,
  readRunStylePreferences,
  readSidebarWidth,
  rememberRunStylePreference,
  rememberSidebarCollapsed,
  rememberSidebarWidth,
  resolveRunStyle,
} from "./sidebar-preferences";

const RUN_A = "00000000-0000-7000-8000-000000000001";

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

describe("sidebar preferences", () => {
  it("persists validated run styles and resets to the deterministic default", () => {
    const storage = new MemoryStorage();
    const preferred = { color: "#abcdef", pattern: "dash-dot" as const };
    const saved = rememberRunStylePreference({}, RUN_A, preferred, storage);
    preferred.color = "#000000";

    expect(resolveRunStyle(RUN_A, saved)).toEqual({
      color: "#abcdef",
      pattern: "dash-dot",
    });
    expect(readRunStylePreferences(storage)).toEqual(saved);

    const reset = forgetRunStylePreference(saved, RUN_A, storage);
    expect(reset).toEqual({});
    expect(resolveRunStyle(RUN_A, reset)).toEqual(runStyle(RUN_A));
    expect(readRunStylePreferences(storage)).toEqual({});
  });

  it("rejects malformed storage and bounds retained run styles", () => {
    const storage = new MemoryStorage();
    storage.setItem(
      "epochdeck:run-styles",
      JSON.stringify([
        ["not-a-run", { color: "#123456", pattern: "solid" }],
        [RUN_A, { color: "red", pattern: "solid" }],
      ]),
    );
    expect(readRunStylePreferences(storage)).toEqual({});

    const entries = Array.from({ length: 300 }, (_, index) => [
      `00000000-0000-7000-8000-${index.toString(16).padStart(12, "0")}`,
      { color: "#123456", pattern: "dash" },
    ]);
    storage.setItem("epochdeck:run-styles", JSON.stringify(entries));
    const bounded = readRunStylePreferences(storage);
    expect(Object.keys(bounded)).toHaveLength(256);
    expect(bounded[entries[0][0] as string]).toBeUndefined();
    expect(bounded[entries.at(-1)![0] as string]).toEqual({
      color: "#123456",
      pattern: "dash",
    });

    storage.setItem("epochdeck:run-styles", " ".repeat(64 * 1024 + 1));
    expect(readRunStylePreferences(storage)).toEqual({});
  });

  it("clamps and remembers sidebar width while preserving chart space", () => {
    const storage = new MemoryStorage();
    expect(readSidebarWidth(1440, storage)).toBe(DEFAULT_SIDEBAR_WIDTH);
    expect(clampSidebarWidth(10, 1440)).toBe(MIN_SIDEBAR_WIDTH);
    expect(clampSidebarWidth(2_000, 4_000)).toBe(MAX_SIDEBAR_WIDTH);
    expect(clampSidebarWidth(600, 800)).toBe(320);

    expect(rememberSidebarWidth(412, 1440, storage)).toBe(412);
    expect(readSidebarWidth(1440, storage)).toBe(412);
    expect(readSidebarWidth(800, storage)).toBe(320);

    expect(readSidebarCollapsed(storage)).toBe(false);
    expect(rememberSidebarCollapsed(true, storage)).toBe(true);
    expect(readSidebarCollapsed(storage)).toBe(true);
    expect(rememberSidebarCollapsed(false, storage)).toBe(false);
    expect(readSidebarCollapsed(storage)).toBe(false);
  });
});
