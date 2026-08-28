import { describe, expect, it } from "vitest";

import { readChartPreferences, rememberChartPreferences } from "./chart-preferences";

describe("chart preferences", () => {
  it("restores an isolated copy for a run and metric identity", () => {
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
    rememberChartPreferences("run:loss", value);
    const restored = readChartPreferences("run:loss");
    expect(restored).toEqual(value);
    if (restored) restored.smoothingAmount = 99;
    expect(readChartPreferences("run:loss")?.smoothingAmount).toBe(0.2);
    expect(readChartPreferences("other:loss")).toBeUndefined();
  });
});
