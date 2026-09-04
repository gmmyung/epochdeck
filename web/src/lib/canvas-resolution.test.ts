import { describe, expect, it } from "vitest";

import { boundedCanvasPixelRatio, snapCanvasCoordinate } from "./canvas-resolution";

describe("canvas resolution", () => {
  it("oversamples a one-times display without exceeding the requested device ratio", () => {
    expect(boundedCanvasPixelRatio(500, 260, 1)).toBe(2);
    expect(boundedCanvasPixelRatio(500, 260, 3)).toBe(3);
  });

  it("bounds oversized backing buffers", () => {
    const ratio = boundedCanvasPixelRatio(4_000, 2_000, 2);
    expect(ratio).toBe(1);
    expect(4_000 * 2_000 * ratio ** 2).toBeLessThanOrEqual(8_000_000);
  });

  it("snaps drawing coordinates to physical pixels", () => {
    expect(snapCanvasCoordinate(10.24, 2)).toBe(10);
    expect(snapCanvasCoordinate(10.26, 2)).toBe(10.5);
  });
});
