import { numericExtent, type ScaleMode } from "./chart-data";

export type Domain = { minimum: number; maximum: number };
export type Viewport = { x: Domain; y: Domain };
export type Point = { x: number; y: number };
export type Frame = {
  width: number;
  height: number;
  padding: { top: number; right: number; bottom: number; left: number };
  x: Domain;
  y: Domain;
};
export type Drag = {
  start: Point;
  current: Point;
  viewport: Viewport;
};

export function configuredChartViewport(
  horizontalValues: Array<number | null>,
  values: Array<number | null>,
  horizontalScale: ScaleMode,
  verticalScale: ScaleMode,
  horizontalMinimum: string,
  horizontalMaximum: string,
  verticalMinimum: string,
  verticalMaximum: string,
): Viewport | null {
  const xExtent = numericExtent(horizontalValues, horizontalScale);
  const yExtent = numericExtent(values, verticalScale);
  if (!xExtent || !yExtent) return null;
  const rawX = manualDomain(xExtent, horizontalMinimum, horizontalMaximum, horizontalScale);
  const rawY = manualDomain(yExtent, verticalMinimum, verticalMaximum, verticalScale);
  return {
    x: boundedDomain(
      transformScale(rawX.minimum, horizontalScale),
      transformScale(rawX.maximum, horizontalScale),
      horizontalScale,
    ),
    y: boundedDomain(
      transformScale(rawY.minimum, verticalScale),
      transformScale(rawY.maximum, verticalScale),
      verticalScale,
    ),
  };
}

export function validateChartAxes(
  horizontalMinimum: string,
  horizontalMaximum: string,
  horizontalScale: ScaleMode,
  verticalMinimum: string,
  verticalMaximum: string,
  verticalScale: ScaleMode,
  horizontalValues: Array<number | null>,
  values: Array<number | null>,
): string | null {
  for (const [label, minimumInput, maximumInput, scale, extent] of [
    [
      "X",
      horizontalMinimum,
      horizontalMaximum,
      horizontalScale,
      numericExtent(horizontalValues, horizontalScale),
    ],
    ["Y", verticalMinimum, verticalMaximum, verticalScale, numericExtent(values, verticalScale)],
  ] as const) {
    const minimum = parseOptionalNumber(minimumInput);
    const maximum = parseOptionalNumber(maximumInput);
    if (minimumInput.trim() && minimum === null) return `${label} minimum must be a number.`;
    if (maximumInput.trim() && maximum === null) return `${label} maximum must be a number.`;
    if (minimum !== null && maximum !== null && minimum >= maximum) {
      return `${label} minimum must be smaller than its maximum.`;
    }
    if (extent && (minimum ?? extent[0]) >= (maximum ?? extent[1])) {
      return `${label} range does not overlap the automatic data range.`;
    }
    if (
      extent &&
      (!Number.isFinite(
        transformScale(maximum ?? extent[1], scale) - transformScale(minimum ?? extent[0], scale),
      ) ||
        Math.abs(transformScale(minimum ?? extent[0], scale)) > (scale === "log" ? 300 : 1e150) ||
        Math.abs(transformScale(maximum ?? extent[1], scale)) > (scale === "log" ? 300 : 1e150))
    ) {
      return `${label} range exceeds the supported chart domain.`;
    }
    if (
      scale === "log" &&
      ((minimum !== null && minimum <= 0) || (maximum !== null && maximum <= 0))
    ) {
      return `${label} logarithmic ranges must be positive.`;
    }
  }
  if (horizontalScale === "log" && !numericExtent(horizontalValues, horizontalScale)) {
    return "X logarithmic scale requires positive coordinates.";
  }
  if (verticalScale === "log" && !numericExtent(values, verticalScale)) {
    return "Y logarithmic scale requires positive values.";
  }
  return null;
}

export function transformScale(value: number, scale: ScaleMode): number {
  return scale === "log" ? Math.log10(value) : value;
}

function restoreScale(value: number, scale: ScaleMode): number {
  return scale === "log" ? 10 ** value : value;
}

export function toScreenX(value: number, frame: Frame, scale: ScaleMode): number {
  const plotWidth = frame.width - frame.padding.left - frame.padding.right;
  const minimum = transformScale(frame.x.minimum, scale);
  const maximum = transformScale(frame.x.maximum, scale);
  return (
    frame.padding.left +
    ((transformScale(value, scale) - minimum) / Math.max(maximum - minimum, Number.EPSILON)) *
      plotWidth
  );
}

export function toScreenY(value: number, frame: Frame, scale: ScaleMode): number {
  const plotHeight = frame.height - frame.padding.top - frame.padding.bottom;
  const minimum = transformScale(frame.y.minimum, scale);
  const maximum = transformScale(frame.y.maximum, scale);
  return (
    frame.padding.top +
    ((maximum - transformScale(value, scale)) / Math.max(maximum - minimum, Number.EPSILON)) *
      plotHeight
  );
}

export function fromScreenX(value: number, frame: Frame, scale: ScaleMode): number {
  const plotWidth = frame.width - frame.padding.left - frame.padding.right;
  const ratio = (value - frame.padding.left) / plotWidth;
  const minimum = transformScale(frame.x.minimum, scale);
  const maximum = transformScale(frame.x.maximum, scale);
  return restoreScale(minimum + ratio * (maximum - minimum), scale);
}

export function fromScreenY(value: number, frame: Frame, scale: ScaleMode): number {
  const plotHeight = frame.height - frame.padding.top - frame.padding.bottom;
  const ratio = (value - frame.padding.top) / plotHeight;
  const minimum = transformScale(frame.y.minimum, scale);
  const maximum = transformScale(frame.y.maximum, scale);
  return restoreScale(maximum - ratio * (maximum - minimum), scale);
}

export function boundedDomain(minimum: number, maximum: number, scale: ScaleMode): Domain {
  const limit = scale === "log" ? 300 : 1e150;
  let lower = finiteClamp(minimum, limit);
  let upper = finiteClamp(maximum, limit);
  if (lower > upper) [lower, upper] = [upper, lower];
  let center = lower / 2 + upper / 2;
  const minimumSpan = Math.max(1e-12, Math.abs(center) * 1e-12);
  let span = Math.min(Math.max(upper - lower, minimumSpan), limit * 2);
  if (!Number.isFinite(span)) span = limit * 2;
  const halfSpan = span / 2;
  center = Math.max(-limit + halfSpan, Math.min(limit - halfSpan, center));
  return {
    minimum: restoreScale(center - halfSpan, scale),
    maximum: restoreScale(center + halfSpan, scale),
  };
}

export function viewportFromSelection(
  drag: Drag,
  frame: Frame,
  horizontalScale: ScaleMode,
  verticalScale: ScaleMode,
): Viewport {
  const left = Math.max(frame.padding.left, Math.min(drag.start.x, drag.current.x));
  const right = Math.min(frame.width - frame.padding.right, Math.max(drag.start.x, drag.current.x));
  const top = Math.max(frame.padding.top, Math.min(drag.start.y, drag.current.y));
  const bottom = Math.min(
    frame.height - frame.padding.bottom,
    Math.max(drag.start.y, drag.current.y),
  );
  return {
    x: boundedDomain(
      transformScale(fromScreenX(left, frame, horizontalScale), horizontalScale),
      transformScale(fromScreenX(right, frame, horizontalScale), horizontalScale),
      horizontalScale,
    ),
    y: boundedDomain(
      transformScale(fromScreenY(bottom, frame, verticalScale), verticalScale),
      transformScale(fromScreenY(top, frame, verticalScale), verticalScale),
      verticalScale,
    ),
  };
}

export function zoomHorizontalViewport(
  frame: Frame,
  anchorX: number,
  scale: ScaleMode,
  factor: number,
): Viewport {
  const anchor = transformScale(fromScreenX(anchorX, frame, scale), scale);
  const minimum = transformScale(frame.x.minimum, scale);
  const maximum = transformScale(frame.x.maximum, scale);
  const boundedFactor = Math.max(0.05, Math.min(20, factor));
  return {
    x: boundedDomain(
      anchor + (minimum - anchor) * boundedFactor,
      anchor + (maximum - anchor) * boundedFactor,
      scale,
    ),
    y: frame.y,
  };
}

export function clampDomainToBounds(domain: Domain, bounds: Domain): Domain {
  const minimum = Math.max(domain.minimum, bounds.minimum);
  const maximum = Math.min(domain.maximum, bounds.maximum);
  return minimum < maximum ? { minimum, maximum } : { ...bounds };
}

export function clampPannedDomainToBounds(
  domain: Domain,
  bounds: Domain,
  scale: ScaleMode,
): Domain {
  const boundsMinimum = transformScale(bounds.minimum, scale);
  const boundsMaximum = transformScale(bounds.maximum, scale);
  let minimum = transformScale(domain.minimum, scale);
  let maximum = transformScale(domain.maximum, scale);
  const span = maximum - minimum;
  if (span >= boundsMaximum - boundsMinimum) return { ...bounds };
  if (minimum < boundsMinimum) {
    minimum = boundsMinimum;
    maximum = boundsMinimum + span;
  } else if (maximum > boundsMaximum) {
    maximum = boundsMaximum;
    minimum = boundsMaximum - span;
  }
  return boundedDomain(minimum, maximum, scale);
}

export function insidePlot(point: Point, frame: Frame): boolean {
  return (
    point.x >= frame.padding.left &&
    point.x <= frame.width - frame.padding.right &&
    point.y >= frame.padding.top &&
    point.y <= frame.height - frame.padding.bottom
  );
}

function manualDomain(
  extent: [number, number],
  minimumInput: string,
  maximumInput: string,
  scale: ScaleMode,
): Domain {
  const parsedMinimum = parseOptionalNumber(minimumInput);
  const parsedMaximum = parseOptionalNumber(maximumInput);
  const minimum = parsedMinimum ?? extent[0];
  const maximum = parsedMaximum ?? extent[1];
  if (
    !Number.isFinite(minimum) ||
    !Number.isFinite(maximum) ||
    minimum >= maximum ||
    (scale === "log" && minimum <= 0)
  ) {
    return { minimum: extent[0], maximum: extent[1] };
  }
  return { minimum, maximum };
}

function parseOptionalNumber(input: string): number | null {
  if (!input.trim()) return null;
  const value = Number(input);
  return Number.isFinite(value) ? value : null;
}

function finiteClamp(value: number, limit: number): number {
  if (Number.isFinite(value)) return Math.max(-limit, Math.min(limit, value));
  return value < 0 ? -limit : limit;
}
