import { describe, expect, it } from "vitest";

import type { RichValue } from "./api";
import { groupRichValues, selectedMediaIndex } from "./media-timeline";

function value(
  id: string,
  key: string,
  kind: RichValue["kind"],
  step: number,
  timestampMs: number,
): RichValue {
  return {
    id,
    run_id: "run-id",
    key,
    kind,
    step,
    timestamp_ms: timestampMs,
    blob: null,
    metadata: {},
    created_at: "2026-08-28 00:00:00",
  };
}

describe("groupRichValues", () => {
  it("groups by kind and key and orders each timeline by step, time, and ID", () => {
    const groups = groupRichValues([
      value("z", "train/rollout", "video", 20, 100),
      value("b", "train/rollout", "video", 10, 200),
      value("a", "train/rollout", "video", 10, 200),
      value("image", "train/rollout", "image", 5, 50),
      value("reward", "eval/reward", "histogram", 1, 10),
    ]);

    expect(groups.map((group) => [group.key, group.kind])).toEqual([
      ["eval/reward", "histogram"],
      ["train/rollout", "image"],
      ["train/rollout", "video"],
    ]);
    expect(groups[2].values.map((item) => item.id)).toEqual(["a", "b", "z"]);
  });

  it("selects the latest item by default and preserves an existing selection", () => {
    const [group] = groupRichValues([
      value("first", "rollout", "video", 10, 10),
      value("last", "rollout", "video", 30, 30),
    ]);

    expect(selectedMediaIndex(group, undefined)).toBe(1);
    expect(selectedMediaIndex(group, "first")).toBe(0);
    expect(selectedMediaIndex(group, "removed")).toBe(1);
  });
});
