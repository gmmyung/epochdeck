import { describe, expect, it } from "vitest";

import {
  configuredChartViewport,
  clampDomainToBounds,
  clampPannedDomainToBounds,
  fromScreenX,
  toScreenX,
  validateChartAxes,
  viewportFromSelection,
  zoomHorizontalViewport,
  type Frame,
} from "./metric-chart-viewport";

const frame: Frame = {
  width: 500,
  height: 300,
  padding: { top: 10, right: 20, bottom: 30, left: 50 },
  x: { minimum: 10, maximum: 110 },
  y: { minimum: -2, maximum: 8 },
};

describe("metric chart viewport", () => {
  it("builds a bounded manual viewport and validates invalid axes", () => {
    expect(
      configuredChartViewport([10, 20], [1, 2], "linear", "linear", "12", "18", "", ""),
    ).toMatchObject({ x: { minimum: 12, maximum: 18 }, y: { minimum: 1, maximum: 2 } });
    expect(validateChartAxes("20", "10", "linear", "", "", "linear", [1, 2], [1, 2])).toBe(
      "X minimum must be smaller than its maximum.",
    );
    expect(validateChartAxes("", "", "log", "", "", "linear", [-2, 0], [1, 2])).toBe(
      "X logarithmic scale requires positive coordinates.",
    );
  });

  it("round-trips screen coordinates and clips selection to the plot", () => {
    const x = toScreenX(60, frame, "linear");
    expect(fromScreenX(x, frame, "linear")).toBeCloseTo(60);
    const viewport = viewportFromSelection(
      {
        start: { x: -100, y: 40 },
        current: { x: 300, y: 500 },
        viewport: { x: frame.x, y: frame.y },
      },
      frame,
      "linear",
      "linear",
    );
    expect(viewport.x.minimum).toBeCloseTo(10);
    expect(viewport.x.maximum).toBeCloseTo(68.1395348837);
    expect(viewport.y.minimum).toBeCloseTo(-2);
    expect(viewport.y.maximum).toBeCloseTo(6.8461538462);
  });

  it("zooms the horizontal domain without changing the vertical domain", () => {
    const viewport = zoomHorizontalViewport(frame, toScreenX(60, frame, "linear"), "linear", 0.5);

    expect(viewport.x.minimum).toBeCloseTo(35);
    expect(viewport.x.maximum).toBeCloseTo(85);
    expect(viewport.y).toEqual(frame.y);
  });

  it("clamps zoomed domains to the observed full-data extent", () => {
    expect(
      clampDomainToBounds({ minimum: -50, maximum: 150 }, { minimum: 0, maximum: 100 }),
    ).toEqual({ minimum: 0, maximum: 100 });
    expect(
      clampDomainToBounds({ minimum: 25, maximum: 150 }, { minimum: 0, maximum: 100 }),
    ).toEqual({ minimum: 25, maximum: 100 });
  });

  it("stops panning at either boundary without changing the viewport width", () => {
    const bounds = { minimum: 0, maximum: 100 };
    expect(clampPannedDomainToBounds({ minimum: -20, maximum: 40 }, bounds, "linear")).toEqual({
      minimum: 0,
      maximum: 60,
    });
    expect(clampPannedDomainToBounds({ minimum: 80, maximum: 140 }, bounds, "linear")).toEqual({
      minimum: 40,
      maximum: 100,
    });
    expect(clampPannedDomainToBounds({ minimum: -20, maximum: 140 }, bounds, "linear")).toEqual(
      bounds,
    );
    expect(
      clampPannedDomainToBounds(
        { minimum: 0.1, maximum: 10 },
        { minimum: 1, maximum: 1_000 },
        "log",
      ),
    ).toEqual({ minimum: 1, maximum: 100 });
  });
});
