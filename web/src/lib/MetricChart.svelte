<script module lang="ts">
  let modalBodyLockCount = 0;

  function acquireModalBodyLock(): void {
    modalBodyLockCount += 1;
    document.body.classList.add("chart-modal-open");
  }

  function releaseModalBodyLock(): void {
    modalBodyLockCount = Math.max(0, modalBodyLockCount - 1);
    if (modalBodyLockCount === 0) document.body.classList.remove("chart-modal-open");
  }
</script>

<script lang="ts">
  import { onMount } from "svelte";

  import type { ChartHistory } from "./api";
  import { readChartPreferences, rememberChartPreferences } from "./chart-preferences";
  import {
    axisTicks,
    closestPointIndex,
    numericExtent,
    smoothSeries,
    type ScaleMode,
    type SmoothingMode,
  } from "./chart-data";
  import Icon from "./Icon.svelte";

  export let metric: string;
  export let identity = metric;
  export let title: string | undefined = undefined;
  export let history: ChartHistory | undefined;
  export let loading = false;
  export let onvisibilitychange: (metric: string, visible: boolean) => void;
  export let onviewportchange: (
    metric: string,
    stepMin: number | null,
    stepMax: number | null,
  ) => void = () => {};

  type InteractionMode = "pan" | "select";
  type DisplayMode = "band" | "line";
  type Domain = { minimum: number; maximum: number };
  type Viewport = { x: Domain; y: Domain };
  type Point = { x: number; y: number };
  type Frame = {
    width: number;
    height: number;
    padding: { top: number; right: number; bottom: number; left: number };
    x: Domain;
    y: Domain;
  };
  type Drag = {
    start: Point;
    current: Point;
    viewport: Viewport;
  };

  const CANVAS_PIXEL_BUDGET = 8_000_000;
  const CANVAS_DIMENSION_LIMIT = 4_096;
  const CANVAS_DPR_LIMIT = 2;

  let card: HTMLElement;
  let canvas: HTMLCanvasElement;
  let settings: HTMLDetailsElement;
  let settingsSummary: HTMLElement;
  let expandButton: HTMLButtonElement;
  let visible = false;
  let chartRevision = 0;
  let interactionMode: InteractionMode = "pan";
  let displayMode: DisplayMode = "band";
  let smoothingMode: SmoothingMode = "none";
  let smoothingAmount = 0.15;
  let xScale: ScaleMode = "linear";
  let yScale: ScaleMode = "linear";
  let xMinimum = "";
  let xMaximum = "";
  let yMinimum = "";
  let yMaximum = "";
  let viewport: Viewport | null = null;
  let frame: Frame | null = null;
  let drag: Drag | null = null;
  let hoverIndex: number | null = null;
  let hoverPosition: Point | null = null;
  let pendingPointer: Point | null = null;
  let pointerFrame: number | null = null;
  let viewportRequestTimer: number | null = null;
  let preferenceIdentity = "";
  let expanded = false;
  let settingsOpen = false;
  let mounted = false;
  let layerListenersActive = false;
  let focusTrapActive = false;
  let ownsModalBodyLock = false;
  let restoreFocusElement: HTMLElement | null = null;
  let inertedElements: Array<{ element: HTMLElement; wasInert: boolean }> = [];

  $: updateLayerListeners(settingsOpen, expanded);

  $: if (identity !== preferenceIdentity) {
    if (viewportRequestTimer !== null) {
      window.clearTimeout(viewportRequestTimer);
      viewportRequestTimer = null;
    }
    const saved = readChartPreferences(identity);
    preferenceIdentity = identity;
    if (saved) {
      displayMode = saved.displayMode;
      smoothingMode = saved.smoothingMode;
      smoothingAmount = saved.smoothingAmount;
      xScale = saved.xScale;
      yScale = saved.yScale;
      xMinimum = saved.xMinimum;
      xMaximum = saved.xMaximum;
      yMinimum = saved.yMinimum;
      yMaximum = saved.yMaximum;
    }
    viewport = null;
    drag = null;
    hoverIndex = null;
    hoverPosition = null;
  }

  $: if (identity === preferenceIdentity) {
    rememberChartPreferences(identity, {
      displayMode,
      smoothingMode,
      smoothingAmount,
      xScale,
      yScale,
      xMinimum,
      xMaximum,
      yMinimum,
      yMaximum,
    });
  }

  $: series = history?.metrics[metric];
  $: steps = series?.last_step ?? [];
  $: timestamps = series?.last_timestamp_ms ?? [];
  $: rawValues = series?.last ?? [];
  $: bandLower = series?.minimum ?? [];
  $: bandUpper = series?.maximum ?? [];
  $: smoothingCoordinates =
    smoothingMode === "time-ema" ? timestamps.map((timestamp) => timestamp / 1_000) : steps;
  $: smoothedValues = history
    ? smoothSeries(smoothingCoordinates, rawValues, smoothingMode, smoothingAmount)
    : [];
  $: domainValues =
    displayMode === "band" ? [...bandLower, ...bandUpper, ...smoothedValues] : smoothedValues;
  $: axisWarning = validateAxes(
    xMinimum,
    xMaximum,
    xScale,
    yMinimum,
    yMaximum,
    yScale,
    steps,
    domainValues,
  );
  $: validHoverIndex =
    hoverIndex !== null &&
    history !== undefined &&
    hoverIndex >= 0 &&
    hoverIndex < steps.length &&
    hoverIndex < smoothedValues.length &&
    Number.isFinite(steps[hoverIndex]) &&
    (xScale !== "log" || steps[hoverIndex] > 0) &&
    smoothedValues[hoverIndex] !== null &&
    Number.isFinite(smoothedValues[hoverIndex]) &&
    (yScale !== "log" || (smoothedValues[hoverIndex] as number) > 0)
      ? hoverIndex
      : null;
  $: hoverValue = validHoverIndex === null ? null : smoothedValues[validHoverIndex];
  $: hoverRawValue = validHoverIndex === null ? null : rawValues[validHoverIndex];

  onMount(() => {
    mounted = true;
    const observer = new IntersectionObserver(
      (entries) => {
        const nextVisible = entries.some((entry) => entry.isIntersecting);
        if (nextVisible === visible) return;
        visible = nextVisible;
        onvisibilitychange(metric, visible);
      },
      { rootMargin: "500px 0px" },
    );
    const resizeObserver = new ResizeObserver(() => (chartRevision += 1));
    const theme = window.matchMedia("(prefers-color-scheme: dark)");
    const redraw = () => (chartRevision += 1);
    observer.observe(card);
    resizeObserver.observe(card);
    theme.addEventListener("change", redraw);
    updateLayerListeners(settingsOpen, expanded);
    return () => {
      mounted = false;
      updateLayerListeners(false, false);
      visible = false;
      onvisibilitychange(metric, false);
      observer.disconnect();
      resizeObserver.disconnect();
      theme.removeEventListener("change", redraw);
      if (pointerFrame !== null) window.cancelAnimationFrame(pointerFrame);
      if (viewportRequestTimer !== null) window.clearTimeout(viewportRequestTimer);
      setSurroundingContentInert(false);
      setModalBodyLock(false);
    };
  });

  $: if (canvas && visible && history && chartRevision >= 0) {
    drawChart(
      canvas,
      steps,
      rawValues,
      smoothedValues,
      bandLower,
      bandUpper,
      displayMode,
      xScale,
      yScale,
      configuredDomain(steps, domainValues, xScale, yScale, xMinimum, xMaximum, yMinimum, yMaximum),
      viewport,
      validHoverIndex,
      drag,
    );
  }

  function configuredDomain(
    steps: number[],
    values: Array<number | null>,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    horizontalMinimum: string,
    horizontalMaximum: string,
    verticalMinimum: string,
    verticalMaximum: string,
  ): Viewport | null {
    const xExtent = numericExtent(steps, horizontalScale);
    const yExtent = numericExtent(values, verticalScale);
    if (!xExtent || !yExtent) return null;
    const rawX = manualDomain(xExtent, horizontalMinimum, horizontalMaximum, horizontalScale);
    const rawY = manualDomain(yExtent, verticalMinimum, verticalMaximum, verticalScale);
    return {
      x: boundedDomain(
        transformed(rawX.minimum, horizontalScale),
        transformed(rawX.maximum, horizontalScale),
        horizontalScale,
      ),
      y: boundedDomain(
        transformed(rawY.minimum, verticalScale),
        transformed(rawY.maximum, verticalScale),
        verticalScale,
      ),
    };
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

  function validateAxes(
    horizontalMinimum: string,
    horizontalMaximum: string,
    horizontalScale: ScaleMode,
    verticalMinimum: string,
    verticalMaximum: string,
    verticalScale: ScaleMode,
    steps: number[],
    values: Array<number | null>,
  ): string | null {
    for (const [label, minimumInput, maximumInput, scale, extent] of [
      [
        "X",
        horizontalMinimum,
        horizontalMaximum,
        horizontalScale,
        numericExtent(steps, horizontalScale),
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
          transformed(maximum ?? extent[1], scale) - transformed(minimum ?? extent[0], scale),
        ) ||
          Math.abs(transformed(minimum ?? extent[0], scale)) > (scale === "log" ? 300 : 1e150) ||
          Math.abs(transformed(maximum ?? extent[1], scale)) > (scale === "log" ? 300 : 1e150))
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
    if (horizontalScale === "log" && !numericExtent(steps, horizontalScale)) {
      return "X logarithmic scale requires positive steps.";
    }
    if (verticalScale === "log" && !numericExtent(values, verticalScale)) {
      return "Y logarithmic scale requires positive values.";
    }
    return null;
  }

  function parseOptionalNumber(input: string): number | null {
    if (!input.trim()) return null;
    const value = Number(input);
    return Number.isFinite(value) ? value : null;
  }

  function drawChart(
    target: HTMLCanvasElement,
    steps: number[],
    values: Array<number | null>,
    smoothed: Array<number | null>,
    lower: Array<number | null>,
    upper: Array<number | null>,
    display: DisplayMode,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    configured: Viewport | null,
    activeViewport: Viewport | null,
    activeHover: number | null,
    activeDrag: Drag | null,
  ): void {
    const width = Math.max(target.clientWidth, 1);
    const height = Math.max(target.clientHeight, 1);
    const requestedRatio = Math.min(window.devicePixelRatio || 1, CANVAS_DPR_LIMIT);
    const pixelBudgetRatio = Math.sqrt(CANVAS_PIXEL_BUDGET / (width * height));
    const dimensionRatio = Math.min(
      CANVAS_DIMENSION_LIMIT / width,
      CANVAS_DIMENSION_LIMIT / height,
    );
    const ratio = Math.max(0.01, Math.min(requestedRatio, pixelBudgetRatio, dimensionRatio));
    const pixelWidth = Math.floor(width * ratio);
    const pixelHeight = Math.floor(height * ratio);
    if (target.width !== pixelWidth) target.width = pixelWidth;
    if (target.height !== pixelHeight) target.height = pixelHeight;
    const context = target.getContext("2d");
    if (!context) return;
    context.setTransform(1, 0, 0, 1, 0, 0);
    context.clearRect(0, 0, target.width, target.height);
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    const current = activeViewport ?? configured;
    if (!current) {
      frame = null;
      return;
    }

    const padding = { top: 12, right: 18, bottom: 32, left: 58 };
    const currentFrame: Frame = { width, height, padding, x: current.x, y: current.y };
    frame = currentFrame;
    const styles = getComputedStyle(target);
    const gridColor = styles.getPropertyValue("--chart-grid").trim() || "#d9dde0";
    const mutedColor = styles.getPropertyValue("--muted").trim() || "#596168";
    const accentColor = styles.getPropertyValue("--accent").trim() || "#2766ad";
    const surfaceColor = styles.getPropertyValue("--surface").trim() || "#ffffff";
    const plotWidth = Math.max(width - padding.left - padding.right, 1);
    const plotHeight = Math.max(height - padding.top - padding.bottom, 1);

    context.font = "10px system-ui, sans-serif";
    context.lineWidth = 1;
    context.strokeStyle = gridColor;
    context.fillStyle = mutedColor;
    const yTicks = axisTicks(current.y.minimum, current.y.maximum, 5, verticalScale);
    for (const tick of yTicks) {
      const y = toScreenY(tick, currentFrame, verticalScale);
      context.beginPath();
      context.moveTo(padding.left, y);
      context.lineTo(width - padding.right, y);
      context.stroke();
      const label = formatAxis(tick);
      context.fillText(label, padding.left - context.measureText(label).width - 8, y + 3);
    }

    const tickCount = Math.max(4, Math.min(10, Math.floor(plotWidth / 100)));
    const xTicks = axisTicks(current.x.minimum, current.x.maximum, tickCount, horizontalScale);
    for (const [index, tick] of xTicks.entries()) {
      const x = toScreenX(tick, currentFrame, horizontalScale);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      const label = formatAxis(tick);
      const measured = context.measureText(label).width;
      const labelX =
        index === 0
          ? Math.max(x, padding.left)
          : index === xTicks.length - 1
            ? Math.min(x - measured, width - padding.right - measured)
            : x - measured / 2;
      context.fillText(label, labelX, height - 9);
    }

    context.save();
    context.beginPath();
    context.rect(padding.left, padding.top, plotWidth, plotHeight);
    context.clip();

    if (display === "band") {
      drawBand(
        context,
        steps,
        lower,
        upper,
        currentFrame,
        horizontalScale,
        verticalScale,
        accentColor,
      );
    }
    drawLine(context, steps, smoothed, currentFrame, horizontalScale, verticalScale, accentColor);

    if (activeDrag && interactionMode !== "pan") {
      const left = Math.min(activeDrag.start.x, activeDrag.current.x);
      const top = Math.min(activeDrag.start.y, activeDrag.current.y);
      const dragWidth = Math.abs(activeDrag.current.x - activeDrag.start.x);
      const dragHeight = Math.abs(activeDrag.current.y - activeDrag.start.y);
      context.fillStyle = `${accentColor}22`;
      context.strokeStyle = accentColor;
      context.setLineDash([4, 3]);
      context.fillRect(left, top, dragWidth, dragHeight);
      context.strokeRect(left, top, dragWidth, dragHeight);
      context.setLineDash([]);
    }

    if (
      activeHover !== null &&
      validPoint(steps[activeHover], smoothed[activeHover], horizontalScale, verticalScale)
    ) {
      const x = toScreenX(steps[activeHover], currentFrame, horizontalScale);
      const y = toScreenY(smoothed[activeHover] as number, currentFrame, verticalScale);
      context.strokeStyle = mutedColor;
      context.setLineDash([3, 3]);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      context.setLineDash([]);
      context.fillStyle = surfaceColor;
      context.strokeStyle = accentColor;
      context.lineWidth = 2;
      context.beginPath();
      context.arc(x, y, 4, 0, Math.PI * 2);
      context.fill();
      context.stroke();
    }
    context.restore();
  }

  function drawBand(
    context: CanvasRenderingContext2D,
    steps: number[],
    lower: Array<number | null>,
    upper: Array<number | null>,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    color: string,
  ): void {
    let index = 0;
    context.fillStyle = `${color}28`;
    while (index < steps.length) {
      while (
        index < steps.length &&
        (!validPoint(steps[index], lower[index], horizontalScale, verticalScale) ||
          !validPoint(steps[index], upper[index], horizontalScale, verticalScale))
      ) {
        index += 1;
      }
      const start = index;
      while (
        index < steps.length &&
        validPoint(steps[index], lower[index], horizontalScale, verticalScale) &&
        validPoint(steps[index], upper[index], horizontalScale, verticalScale)
      ) {
        index += 1;
      }
      if (index <= start) continue;
      context.beginPath();
      for (let point = start; point < index; point += 1) {
        const x = toScreenX(steps[point], activeFrame, horizontalScale);
        const y = toScreenY(upper[point] as number, activeFrame, verticalScale);
        if (point === start) context.moveTo(x, y);
        else context.lineTo(x, y);
      }
      for (let point = index - 1; point >= start; point -= 1) {
        context.lineTo(
          toScreenX(steps[point], activeFrame, horizontalScale),
          toScreenY(lower[point] as number, activeFrame, verticalScale),
        );
      }
      context.closePath();
      context.fill();
    }
  }

  function drawLine(
    context: CanvasRenderingContext2D,
    steps: number[],
    values: Array<number | null>,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    color: string,
  ): void {
    context.strokeStyle = color;
    context.lineWidth = 1.5;
    context.lineJoin = "round";
    context.beginPath();
    let drawing = false;
    let segmentLength = 0;
    let segmentPoint: Point | null = null;
    const singletonPoints: Point[] = [];
    for (let index = 0; index < values.length; index += 1) {
      if (!validPoint(steps[index], values[index], horizontalScale, verticalScale)) {
        if (segmentLength === 1 && segmentPoint) singletonPoints.push(segmentPoint);
        drawing = false;
        segmentLength = 0;
        segmentPoint = null;
        continue;
      }
      const x = toScreenX(steps[index], activeFrame, horizontalScale);
      const y = toScreenY(values[index] as number, activeFrame, verticalScale);
      if (drawing) {
        context.lineTo(x, y);
        segmentLength += 1;
      } else {
        context.moveTo(x, y);
        segmentLength = 1;
        segmentPoint = { x, y };
      }
      drawing = true;
    }
    if (segmentLength === 1 && segmentPoint) singletonPoints.push(segmentPoint);
    context.stroke();
    if (singletonPoints.length === 0) return;
    context.fillStyle = color;
    context.beginPath();
    for (const point of singletonPoints) {
      context.moveTo(point.x + 3, point.y);
      context.arc(point.x, point.y, 3, 0, Math.PI * 2);
    }
    context.fill();
  }

  function validPoint(
    x: number | undefined,
    y: number | null | undefined,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
  ): boolean {
    return (
      x !== undefined &&
      Number.isFinite(x) &&
      y !== null &&
      y !== undefined &&
      Number.isFinite(y) &&
      (horizontalScale !== "log" || x > 0) &&
      (verticalScale !== "log" || y > 0)
    );
  }

  function transformed(value: number, scale: ScaleMode): number {
    return scale === "log" ? Math.log10(value) : value;
  }

  function restored(value: number, scale: ScaleMode): number {
    return scale === "log" ? 10 ** value : value;
  }

  function toScreenX(value: number, activeFrame: Frame, scale: ScaleMode): number {
    const plotWidth = activeFrame.width - activeFrame.padding.left - activeFrame.padding.right;
    const minimum = transformed(activeFrame.x.minimum, scale);
    const maximum = transformed(activeFrame.x.maximum, scale);
    return (
      activeFrame.padding.left +
      ((transformed(value, scale) - minimum) / Math.max(maximum - minimum, Number.EPSILON)) *
        plotWidth
    );
  }

  function toScreenY(value: number, activeFrame: Frame, scale: ScaleMode): number {
    const plotHeight = activeFrame.height - activeFrame.padding.top - activeFrame.padding.bottom;
    const minimum = transformed(activeFrame.y.minimum, scale);
    const maximum = transformed(activeFrame.y.maximum, scale);
    return (
      activeFrame.padding.top +
      ((maximum - transformed(value, scale)) / Math.max(maximum - minimum, Number.EPSILON)) *
        plotHeight
    );
  }

  function fromScreenX(value: number, activeFrame: Frame): number {
    const plotWidth = activeFrame.width - activeFrame.padding.left - activeFrame.padding.right;
    const ratio = (value - activeFrame.padding.left) / plotWidth;
    const minimum = transformed(activeFrame.x.minimum, xScale);
    const maximum = transformed(activeFrame.x.maximum, xScale);
    return restored(minimum + ratio * (maximum - minimum), xScale);
  }

  function fromScreenY(value: number, activeFrame: Frame): number {
    const plotHeight = activeFrame.height - activeFrame.padding.top - activeFrame.padding.bottom;
    const ratio = (value - activeFrame.padding.top) / plotHeight;
    const minimum = transformed(activeFrame.y.minimum, yScale);
    const maximum = transformed(activeFrame.y.maximum, yScale);
    return restored(maximum - ratio * (maximum - minimum), yScale);
  }

  function canvasPoint(event: PointerEvent | WheelEvent): Point {
    const bounds = canvas.getBoundingClientRect();
    return {
      x: Math.max(0, Math.min(bounds.width, event.clientX - bounds.left)),
      y: Math.max(0, Math.min(bounds.height, event.clientY - bounds.top)),
    };
  }

  function pointerDown(event: PointerEvent): void {
    if (!frame || event.button !== 0 || !insidePlot(canvasPoint(event), frame)) return;
    canvas.setPointerCapture(event.pointerId);
    const point = canvasPoint(event);
    drag = {
      start: point,
      current: point,
      viewport: { x: { ...frame.x }, y: { ...frame.y } },
    };
  }

  function pointerMove(event: PointerEvent): void {
    pendingPointer = canvasPoint(event);
    if (pointerFrame !== null) return;
    pointerFrame = window.requestAnimationFrame(() => {
      pointerFrame = null;
      const point = pendingPointer;
      pendingPointer = null;
      if (point) applyPointerMove(point);
    });
  }

  function applyPointerMove(point: Point): void {
    if (!frame) return;
    hoverPosition = point;
    if (drag) {
      drag = { ...drag, current: point };
      if (interactionMode === "pan") panTo(point);
      chartRevision += 1;
      return;
    }
    if (!insidePlot(point, frame) || !history) {
      hoverIndex = null;
      return;
    }
    hoverIndex = closestPointIndex(
      steps,
      smoothedValues,
      fromScreenX(point.x, frame),
      xScale,
      frame.x.minimum,
      frame.x.maximum,
      yScale,
    );
    chartRevision += 1;
  }

  function pointerUp(event: PointerEvent): void {
    if (!frame || !drag) return;
    const releasePoint = canvasPoint(event);
    const completed = { ...drag, current: releasePoint };
    if (interactionMode === "pan") panTo(releasePoint);
    drag = null;
    if (canvas.hasPointerCapture(event.pointerId)) canvas.releasePointerCapture(event.pointerId);
    const distanceX = Math.abs(completed.current.x - completed.start.x);
    const distanceY = Math.abs(completed.current.y - completed.start.y);
    if (interactionMode === "select" && distanceX >= 6 && distanceY >= 6) {
      viewport = viewportFromDrag(completed, frame);
    }
    if (interactionMode === "pan" || viewport !== null) scheduleViewportRequest(viewport);
    chartRevision += 1;
  }

  function pointerLeave(): void {
    if (drag) return;
    pendingPointer = null;
    hoverIndex = null;
    hoverPosition = null;
    chartRevision += 1;
  }

  function panTo(point: Point): void {
    if (!frame || !drag) return;
    const plotWidth = frame.width - frame.padding.left - frame.padding.right;
    const plotHeight = frame.height - frame.padding.top - frame.padding.bottom;
    const xMinimum = transformed(drag.viewport.x.minimum, xScale);
    const xMaximum = transformed(drag.viewport.x.maximum, xScale);
    const yMinimum = transformed(drag.viewport.y.minimum, yScale);
    const yMaximum = transformed(drag.viewport.y.maximum, yScale);
    const xShift = (-(point.x - drag.start.x) / plotWidth) * (xMaximum - xMinimum);
    const yShift = ((point.y - drag.start.y) / plotHeight) * (yMaximum - yMinimum);
    viewport = {
      x: boundedDomain(xMinimum + xShift, xMaximum + xShift, xScale),
      y: boundedDomain(yMinimum + yShift, yMaximum + yShift, yScale),
    };
  }

  function viewportFromDrag(completed: Drag, activeFrame: Frame): Viewport {
    const left = Math.max(
      activeFrame.padding.left,
      Math.min(completed.start.x, completed.current.x),
    );
    const right = Math.min(
      activeFrame.width - activeFrame.padding.right,
      Math.max(completed.start.x, completed.current.x),
    );
    const top = Math.max(activeFrame.padding.top, Math.min(completed.start.y, completed.current.y));
    const bottom = Math.min(
      activeFrame.height - activeFrame.padding.bottom,
      Math.max(completed.start.y, completed.current.y),
    );
    return {
      x: boundedDomain(
        transformed(fromScreenX(left, activeFrame), xScale),
        transformed(fromScreenX(right, activeFrame), xScale),
        xScale,
      ),
      y: boundedDomain(
        transformed(fromScreenY(bottom, activeFrame), yScale),
        transformed(fromScreenY(top, activeFrame), yScale),
        yScale,
      ),
    };
  }

  function wheel(event: WheelEvent): void {
    if (!frame || !insidePlot(canvasPoint(event), frame)) return;
    event.preventDefault();
    zoomAt(canvasPoint(event), Math.exp(Math.max(-1, Math.min(1, event.deltaY / 240))));
  }

  function zoomAt(point: Point, factor: number): void {
    if (!frame) return;
    const anchorX = transformed(fromScreenX(point.x, frame), xScale);
    const anchorY = transformed(fromScreenY(point.y, frame), yScale);
    const xMinimum = transformed(frame.x.minimum, xScale);
    const xMaximum = transformed(frame.x.maximum, xScale);
    const yMinimum = transformed(frame.y.minimum, yScale);
    const yMaximum = transformed(frame.y.maximum, yScale);
    const boundedFactor = Math.max(0.05, Math.min(20, factor));
    viewport = {
      x: boundedDomain(
        anchorX + (xMinimum - anchorX) * boundedFactor,
        anchorX + (xMaximum - anchorX) * boundedFactor,
        xScale,
      ),
      y: boundedDomain(
        anchorY + (yMinimum - anchorY) * boundedFactor,
        anchorY + (yMaximum - anchorY) * boundedFactor,
        yScale,
      ),
    };
    scheduleViewportRequest(viewport);
    chartRevision += 1;
  }

  function boundedDomain(minimum: number, maximum: number, scale: ScaleMode): Domain {
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
      minimum: restored(center - halfSpan, scale),
      maximum: restored(center + halfSpan, scale),
    };
  }

  function finiteClamp(value: number, limit: number): number {
    if (Number.isFinite(value)) return Math.max(-limit, Math.min(limit, value));
    return value < 0 ? -limit : limit;
  }

  function pointerCancel(event: PointerEvent): void {
    pendingPointer = null;
    if (interactionMode === "pan" && drag) viewport = drag.viewport;
    drag = null;
    if (canvas.hasPointerCapture(event.pointerId)) canvas.releasePointerCapture(event.pointerId);
    chartRevision += 1;
  }

  function chartKeydown(event: KeyboardEvent): void {
    if (event.key === "Escape" && dismissChartLayer()) {
      event.preventDefault();
      event.stopPropagation();
      return;
    }
    if (!frame) return;
    const center = {
      x: (frame.padding.left + frame.width - frame.padding.right) / 2,
      y: (frame.padding.top + frame.height - frame.padding.bottom) / 2,
    };
    if (["+", "="].includes(event.key)) zoomAt(center, 0.5);
    else if (event.key === "-") zoomAt(center, 2);
    else if (["0", "Escape"].includes(event.key)) resetView();
    else return;
    event.preventDefault();
  }

  function insidePlot(point: Point, activeFrame: Frame): boolean {
    return (
      point.x >= activeFrame.padding.left &&
      point.x <= activeFrame.width - activeFrame.padding.right &&
      point.y >= activeFrame.padding.top &&
      point.y <= activeFrame.height - activeFrame.padding.bottom
    );
  }

  function resetView(): void {
    viewport = null;
    drag = null;
    scheduleViewportRequest(null, true);
    chartRevision += 1;
  }

  function scheduleViewportRequest(next: Viewport | null, immediate = false): void {
    if (viewportRequestTimer !== null) {
      window.clearTimeout(viewportRequestTimer);
      viewportRequestTimer = null;
    }
    const notify = () => {
      viewportRequestTimer = null;
      if (!next) {
        onviewportchange(metric, null, null);
        return;
      }
      onviewportchange(metric, next.x.minimum, next.x.maximum);
    };
    if (immediate) notify();
    else viewportRequestTimer = window.setTimeout(notify, 220);
  }

  function setInteraction(mode: InteractionMode): void {
    interactionMode = mode;
    drag = null;
    chartRevision += 1;
  }

  function changeSmoothing(event: Event): void {
    const mode = (event.currentTarget as HTMLSelectElement).value as SmoothingMode;
    smoothingMode = mode;
    if (mode === "ema") smoothingAmount = 0.15;
    else if (mode === "time-ema") smoothingAmount = 25;
    else if (mode === "running") smoothingAmount = 20;
    else if (mode === "gaussian") smoothingAmount = 2;
  }

  function normalizeChartNumbers(): void {
    if (!Number.isFinite(smoothingAmount)) {
      smoothingAmount = smoothingMode === "ema" ? 0.15 : smoothingMode === "gaussian" ? 2 : 20;
    }
    const minimum = smoothingMode === "ema" ? 0.001 : 1;
    const maximum = smoothingMode === "ema" ? 1 : 500;
    smoothingAmount = Math.max(minimum, Math.min(maximum, smoothingAmount));
  }

  function toggleExpanded(): void {
    const nextExpanded = !expanded;
    closeSettings(false);
    if (nextExpanded) {
      restoreFocusElement =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
    }
    expanded = nextExpanded;
    setModalBodyLock(expanded);
    setSurroundingContentInert(expanded);
    window.requestAnimationFrame(() => {
      chartRevision += 1;
      if (expanded) {
        expandButton?.focus();
      } else {
        const target = restoreFocusElement;
        restoreFocusElement = null;
        if (target?.isConnected) target.focus();
      }
    });
  }

  function cardKeydown(event: KeyboardEvent): void {
    if (event.key === "Tab" && expanded) {
      trapFocus(event);
      return;
    }
    if (event.key !== "Escape" || !dismissChartLayer()) return;
    event.preventDefault();
    event.stopPropagation();
  }

  function dismissChartLayer(): boolean {
    if (settingsOpen) {
      closeSettings(true);
      return true;
    }
    if (!expanded) return false;
    toggleExpanded();
    return true;
  }

  function closeSettings(restoreFocus: boolean): void {
    if (!settingsOpen) return;
    settingsOpen = false;
    if (restoreFocus) window.requestAnimationFrame(() => settingsSummary?.focus());
  }

  function documentPointerDown(event: PointerEvent): void {
    if (!settingsOpen || settings.contains(event.target as Node)) return;
    closeSettings(false);
  }

  function updateLayerListeners(open: boolean, enlarged: boolean): void {
    const active = mounted && (open || enlarged);
    if (active !== layerListenersActive) {
      layerListenersActive = active;
      if (active) {
        window.addEventListener("keydown", cardKeydown);
        document.addEventListener("pointerdown", documentPointerDown);
      } else {
        window.removeEventListener("keydown", cardKeydown);
        document.removeEventListener("pointerdown", documentPointerDown);
      }
    }
    const trapActive = mounted && enlarged;
    if (trapActive !== focusTrapActive) {
      focusTrapActive = trapActive;
      if (trapActive) document.addEventListener("focusin", documentFocusIn);
      else document.removeEventListener("focusin", documentFocusIn);
    }
  }

  function trapFocus(event: KeyboardEvent): void {
    const focusable = focusableElements();
    if (focusable.length === 0) {
      event.preventDefault();
      expandButton?.focus();
      return;
    }
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    const active = document.activeElement;
    if (!card.contains(active) || active === card) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus();
    } else if (event.shiftKey && active === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && active === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function documentFocusIn(event: FocusEvent): void {
    if (!expanded || card.contains(event.target as Node)) return;
    (focusableElements()[0] ?? expandButton)?.focus();
  }

  function focusableElements(): HTMLElement[] {
    return Array.from(
      card.querySelectorAll<HTMLElement>(
        'button:not([disabled]):not([tabindex="-1"]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
      ),
    ).filter((element) => element.getClientRects().length > 0);
  }

  function setModalBodyLock(locked: boolean): void {
    if (locked === ownsModalBodyLock) return;
    ownsModalBodyLock = locked;
    if (locked) acquireModalBodyLock();
    else releaseModalBodyLock();
  }

  function setSurroundingContentInert(inert: boolean): void {
    for (const { element, wasInert } of inertedElements) element.inert = wasInert;
    inertedElements = [];
    if (!inert) return;

    let branch: HTMLElement = card;
    while (branch.parentElement && branch.parentElement !== document.body) {
      const parent = branch.parentElement;
      for (const sibling of parent.children) {
        if (
          sibling === branch ||
          !(sibling instanceof HTMLElement) ||
          sibling.classList.contains("chart-modal-backdrop")
        ) {
          continue;
        }
        inertedElements.push({ element: sibling, wasInert: sibling.inert });
        sibling.inert = true;
      }
      branch = parent;
    }
  }

  function smoothingAmountLabel(mode: SmoothingMode): string {
    if (mode === "time-ema") return "Time constant (seconds)";
    if (mode === "running") return "Window (points)";
    if (mode === "gaussian") return "Sigma (points)";
    return "Alpha";
  }

  function formatAxis(value: number): string {
    if (Math.abs(value) >= 10_000 || (Math.abs(value) > 0 && Math.abs(value) < 0.001)) {
      return value.toExponential(1);
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
  }
</script>

{#if expanded}
  <button
    class="chart-modal-backdrop"
    type="button"
    aria-hidden="true"
    tabindex="-1"
    onclick={toggleExpanded}
  ></button>
{/if}
<article
  bind:this={card}
  class="metric-chart-card"
  class:expanded
  role={expanded ? "dialog" : undefined}
  aria-modal={expanded ? "true" : undefined}
  aria-label={`${title ?? metric} metric panel`}
>
  <div class="chart-heading">
    <div class="chart-title">
      <strong>{title ?? metric}</strong>
      {#if loading}<span class="loading-label">updating</span>{/if}
    </div>
    <div class="chart-actions" role="toolbar" aria-label="Chart mouse actions">
      <button
        type="button"
        class:active={interactionMode === "pan"}
        aria-label="Pan chart"
        aria-pressed={interactionMode === "pan"}
        onclick={() => setInteraction("pan")}><Icon name="hand" size={14} /></button
      >
      <button
        type="button"
        class:active={interactionMode === "select"}
        aria-label="Zoom to selected region"
        aria-pressed={interactionMode === "select"}
        onclick={() => setInteraction("select")}><Icon name="select" size={14} /></button
      >
      <button type="button" aria-label="Reset chart view" onclick={resetView}
        ><Icon name="reset" size={14} /></button
      >
      <button
        bind:this={expandButton}
        type="button"
        aria-label={expanded ? "Collapse chart" : "Enlarge chart"}
        onclick={toggleExpanded}><Icon name={expanded ? "minimize" : "expand"} size={14} /></button
      >
      <details bind:this={settings} bind:open={settingsOpen} class="chart-settings">
        <summary
          bind:this={settingsSummary}
          aria-label="Chart settings"
          aria-expanded={settingsOpen}><Icon name="settings" size={14} /></summary
        >
        <div class="chart-settings-popover" role="group" aria-label="Chart display settings">
          <label>
            Display
            <select bind:value={displayMode}>
              <option value="band">Band</option>
              <option value="line">Line</option>
            </select>
          </label>
          <label>
            Smoothing
            <select value={smoothingMode} onchange={changeSmoothing}>
              <option value="none">None</option>
              <option value="time-ema">Time-weighted EMA</option>
              <option value="running">Running average</option>
              <option value="gaussian">Gaussian</option>
              <option value="ema">EMA</option>
            </select>
          </label>
          {#if smoothingMode !== "none"}
            <label>
              {smoothingAmountLabel(smoothingMode)}
              <input
                type="number"
                min={smoothingMode === "ema" ? 0.001 : 1}
                max={smoothingMode === "ema" ? 1 : 500}
                step={smoothingMode === "ema" ? 0.001 : 1}
                bind:value={smoothingAmount}
                onchange={normalizeChartNumbers}
              />
            </label>
          {/if}
          <fieldset>
            <legend>X axis</legend>
            <select aria-label="X axis scale" bind:value={xScale} onchange={resetView}>
              <option value="linear">Linear</option>
              <option value="log">Log</option>
            </select>
            <input
              aria-label="X axis minimum"
              placeholder="Auto min"
              bind:value={xMinimum}
              onchange={resetView}
            />
            <input
              aria-label="X axis maximum"
              placeholder="Auto max"
              bind:value={xMaximum}
              onchange={resetView}
            />
          </fieldset>
          <fieldset>
            <legend>Y axis</legend>
            <select aria-label="Y axis scale" bind:value={yScale} onchange={resetView}>
              <option value="linear">Linear</option>
              <option value="log">Log</option>
            </select>
            <input
              aria-label="Y axis minimum"
              placeholder="Auto min"
              bind:value={yMinimum}
              onchange={resetView}
            />
            <input
              aria-label="Y axis maximum"
              placeholder="Auto max"
              bind:value={yMaximum}
              onchange={resetView}
            />
          </fieldset>
          {#if axisWarning}<p class="chart-warning">{axisWarning}</p>{/if}
        </div>
      </details>
    </div>
  </div>
  {#if history}
    <div class={`chart-canvas-wrap chart-mode-${interactionMode}`}>
      <canvas
        bind:this={canvas}
        tabindex="0"
        aria-label={`${metric} history chart. Use plus and minus to zoom and zero to reset.`}
        onpointerdown={pointerDown}
        onpointermove={pointerMove}
        onpointerup={pointerUp}
        onpointercancel={pointerCancel}
        onpointerleave={pointerLeave}
        onwheel={wheel}
        onkeydown={chartKeydown}
      ></canvas>
      <span class="visually-hidden" aria-live="polite">
        {#if validHoverIndex !== null && hoverValue !== null}
          {metric}, step {steps[validHoverIndex]}, value {hoverValue}
        {/if}
      </span>
      {#if validHoverIndex !== null && hoverPosition && hoverValue !== null}
        <div
          class="chart-tooltip"
          class:flip={hoverPosition.x > (frame?.width ?? 0) * 0.68}
          style={`--tooltip-x: ${hoverPosition.x}px; --tooltip-y: ${hoverPosition.y}px`}
        >
          <strong>{metric}</strong>
          <span>step {steps[validHoverIndex].toLocaleString()}</span>
          <span>value {formatAxis(hoverValue)}</span>
          {#if hoverRawValue !== hoverValue && hoverRawValue !== null}
            <span>raw {formatAxis(hoverRawValue)}</span>
          {/if}
          {#if displayMode === "band" && Number.isFinite(bandLower[validHoverIndex]) && Number.isFinite(bandUpper[validHoverIndex])}
            <span
              >band {formatAxis(bandLower[validHoverIndex])}–{formatAxis(
                bandUpper[validHoverIndex],
              )}</span
            >
          {/if}
        </div>
      {/if}
    </div>
  {:else if loading}
    <div class="chart-placeholder">Loading bounded history…</div>
  {:else}
    <div class="chart-placeholder">Scroll near this chart to load it.</div>
  {/if}
</article>
