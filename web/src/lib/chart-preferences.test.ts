import { describe, expect, it } from "vitest";

import {
  chartPreferenceIdentity,
  readChartPreferences,
  rememberChartPreferences,
} from "./chart-preferences";

describe("chart preferences", () => {
  it("restores an isolated copy for a project and metric identity", () => {
    const value = {
      displayMode: "band" as const,
      smoothingMode: "ema" as const,
      smoothingAmount: 0.2,
      xScale: "linear" as const,
      yScale: "log" as const,
      xMinimum: "",
      xMaximum: "100",
      yMinimum: "0.01",
      yMaximum: "",
    };
    const identity = chartPreferenceIdentity("robotics", "train/loss");
    rememberChartPreferences(identity, value);
    const restored = readChartPreferences(identity);
    expect(restored).toEqual(value);
    if (restored) restored.smoothingAmount = 99;
    expect(readChartPreferences(identity)?.smoothingAmount).toBe(0.2);
    expect(readChartPreferences(chartPreferenceIdentity("other", "train/loss"))).toBeUndefined();
  });

  it("does not encode a selected run set in the preference identity", () => {
    expect(chartPreferenceIdentity("a:b", "c")).not.toBe(chartPreferenceIdentity("a", "b:c"));
    expect(chartPreferenceIdentity("project", "loss")).toBe('["project","loss"]');
  });
});
