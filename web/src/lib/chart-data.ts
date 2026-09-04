export type SmoothingMode = "none" | "ema" | "time-ema" | "running" | "gaussian";
export type ScaleMode = "linear" | "log";

export const MAX_GAUSSIAN_SIGMA = 50;
export const MAX_SMOOTHING_WINDOW = 500;

export function smoothSeries(
  x: number[],
  values: Array<number | null>,
  mode: SmoothingMode,
  amount: number,
): Array<number | null> {
  if (mode === "none") return [...values];
  const safeAmount = Number.isFinite(amount)
    ? amount
    : mode === "ema"
      ? 0.15
      : mode === "gaussian"
        ? 2
        : 20;
  if (mode === "running") return runningAverage(values, safeAmount);
  if (mode === "gaussian") return gaussianSmooth(values, safeAmount);
  return exponentialSmooth(x, values, mode, safeAmount);
}

export function numericExtent(
  values: Array<number | null>,
  scale: ScaleMode,
): [number, number] | null {
  let minimum = Number.POSITIVE_INFINITY;
  let maximum = Number.NEGATIVE_INFINITY;
  for (const value of values) {
    if (value === null || !Number.isFinite(value) || (scale === "log" && value <= 0)) continue;
    minimum = Math.min(minimum, value);
    maximum = Math.max(maximum, value);
  }
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum)) return null;
  if (minimum === maximum) {
    const padding = Math.abs(minimum) * 0.05 || 1;
    return scale === "log"
      ? [Math.max(minimum / 10, Number.MIN_VALUE), minimum * 10]
      : [minimum - padding, maximum + padding];
  }
  return [minimum, maximum];
}

export function axisTicks(
  minimum: number,
  maximum: number,
  count: number,
  scale: ScaleMode,
): number[] {
  const safeCount = Math.max(2, Math.min(12, Math.round(count)));
  if (scale === "log") {
    const minPower = Math.log10(minimum);
    const maxPower = Math.log10(maximum);
    return Array.from(
      { length: safeCount },
      (_, index) => 10 ** (minPower + ((maxPower - minPower) * index) / (safeCount - 1)),
    );
  }
  return Array.from(
    { length: safeCount },
    (_, index) => minimum + ((maximum - minimum) * index) / (safeCount - 1),
  );
}

export function closestPointIndex(
  x: number[],
  values: Array<number | null>,
  target: number,
  scale: ScaleMode = "linear",
  minimum = Number.NEGATIVE_INFINITY,
  maximum = Number.POSITIVE_INFINITY,
  valueScale: ScaleMode = "linear",
): number | null {
  if (x.length === 0 || !Number.isFinite(target) || (scale === "log" && target <= 0)) return null;
  let closest: number | null = null;
  let distance = Number.POSITIVE_INFINITY;
  const transformedTarget = transformValue(target, scale);
  let right = lowerBound(x, target);
  let left = right - 1;
  while (left >= 0 || right < x.length) {
    const leftDistance = coordinateDistance(x[left], transformedTarget, scale);
    const rightDistance = coordinateDistance(x[right], transformedTarget, scale);
    if (leftDistance > distance && rightDistance > distance) break;
    const chooseLeft = left >= 0 && (right >= x.length || leftDistance <= rightDistance);
    const index = chooseLeft ? left-- : right++;
    const coordinate = x[index];
    const value = values[index];
    if (
      value === null ||
      !Number.isFinite(value) ||
      !Number.isFinite(coordinate) ||
      coordinate < minimum ||
      coordinate > maximum ||
      (scale === "log" && coordinate <= 0) ||
      (valueScale === "log" && value <= 0)
    ) {
      continue;
    }
    const candidate = Math.abs(transformValue(coordinate, scale) - transformedTarget);
    if (candidate <= distance) {
      closest = index;
      distance = candidate;
    }
  }
  return closest;
}

function lowerBound(values: number[], target: number): number {
  let minimum = 0;
  let maximum = values.length;
  while (minimum < maximum) {
    const middle = minimum + Math.floor((maximum - minimum) / 2);
    if (values[middle] < target) minimum = middle + 1;
    else maximum = middle;
  }
  return minimum;
}

function coordinateDistance(
  coordinate: number | undefined,
  target: number,
  scale: ScaleMode,
): number {
  if (
    coordinate === undefined ||
    !Number.isFinite(coordinate) ||
    (scale === "log" && coordinate <= 0)
  ) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.abs(transformValue(coordinate, scale) - target);
}

function runningAverage(values: Array<number | null>, amount: number): Array<number | null> {
  const window = Math.max(1, Math.min(MAX_SMOOTHING_WINDOW, Math.round(amount)));
  const output: Array<number | null> = [];
  const ring = Array.from({ length: window }, () => 0);
  let count = 0;
  let writeIndex = 0;
  let total = 0;
  for (const value of values) {
    if (value === null || !Number.isFinite(value)) {
      output.push(null);
      count = 0;
      writeIndex = 0;
      total = 0;
      continue;
    }
    if (count === window) total -= ring[writeIndex];
    else count += 1;
    ring[writeIndex] = value;
    writeIndex = (writeIndex + 1) % window;
    total += value;
    output.push(total / count);
  }
  return output;
}

function exponentialSmooth(
  x: number[],
  values: Array<number | null>,
  mode: "ema" | "time-ema",
  amount: number,
): Array<number | null> {
  const output: Array<number | null> = [];
  const fixedAlpha = Math.max(0.001, Math.min(1, amount));
  const timeConstant = Math.max(Number.EPSILON, amount);
  let previous: number | null = null;
  let previousX: number | null = null;
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (value === null || !Number.isFinite(value)) {
      output.push(null);
      previous = null;
      previousX = null;
      continue;
    }
    if (previous === null) {
      previous = value;
    } else {
      const delta = previousX === null ? 1 : Math.max(Math.abs(x[index] - previousX), 0);
      const alpha = mode === "ema" ? fixedAlpha : 1 - Math.exp(-delta / timeConstant);
      previous += Math.max(alpha, Number.EPSILON) * (value - previous);
    }
    previousX = x[index];
    output.push(previous);
  }
  return output;
}

function gaussianSmooth(values: Array<number | null>, amount: number): Array<number | null> {
  const sigma = Math.max(0.25, Math.min(MAX_GAUSSIAN_SIGMA, amount));
  const radius = Math.ceil(sigma * 3);
  return values.map((value, index) => {
    if (value === null) return null;
    let weighted = 0;
    let totalWeight = 0;
    const [start, end] = contiguousWindow(values, index, radius);
    for (let neighbor = start; neighbor <= end; neighbor += 1) {
      const candidate = values[neighbor];
      if (candidate === null || !Number.isFinite(candidate)) continue;
      const distance = neighbor - index;
      const weight = Math.exp(-(distance * distance) / (2 * sigma * sigma));
      weighted += candidate * weight;
      totalWeight += weight;
    }
    return totalWeight > 0 ? weighted / totalWeight : null;
  });
}

function contiguousWindow(
  values: Array<number | null>,
  index: number,
  radius: number,
): [number, number] {
  let start = index;
  let end = index;
  const minimum = Math.max(0, index - radius);
  const maximum = Math.min(values.length - 1, index + radius);
  while (start > minimum && values[start - 1] !== null) start -= 1;
  while (end < maximum && values[end + 1] !== null) end += 1;
  return [start, end];
}

function transformValue(value: number, scale: ScaleMode): number {
  return scale === "log" ? Math.log10(value) : value;
}
