const TARGET_CANVAS_RATIO = 2;
const CANVAS_PIXEL_BUDGET = 8_000_000;
const CANVAS_DIMENSION_LIMIT = 8_192;

/** Prefer a 2x backing buffer while bounding the memory used by very large canvases. */
export function boundedCanvasPixelRatio(
  width: number,
  height: number,
  deviceRatio = window.devicePixelRatio || 1,
): number {
  const safeWidth = Math.max(width, 1);
  const safeHeight = Math.max(height, 1);
  const requestedRatio = Math.max(deviceRatio, TARGET_CANVAS_RATIO);
  const pixelBudgetRatio = Math.sqrt(CANVAS_PIXEL_BUDGET / (safeWidth * safeHeight));
  const dimensionRatio = Math.min(
    CANVAS_DIMENSION_LIMIT / safeWidth,
    CANVAS_DIMENSION_LIMIT / safeHeight,
  );
  return Math.max(0.01, Math.min(requestedRatio, pixelBudgetRatio, dimensionRatio));
}

export function snapCanvasCoordinate(value: number, ratio: number): number {
  return Math.round(value * ratio) / ratio;
}
