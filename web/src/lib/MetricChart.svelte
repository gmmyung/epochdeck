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

  import { readChartPreferences, rememberChartPreferences } from "./chart-preferences";
  import { axisTicks, type ScaleMode, type SmoothingMode } from "./chart-data";
  import { formatDurationMs } from "./resource-state";
  import {
    closestSeriesPoints,
    contiguousBucketRanges,
    lineDash,
    metricChartViewportKey,
    prepareMetricSeries,
    runSetIdentity,
    type MetricChartViewport,
    type MetricChartSeries,
    type PreparedMetricSeries,
    type SeriesHoverPoint,
    type SeriesPattern,
    type XAlignment,
  } from "./chart-series";
  import Icon from "./Icon.svelte";
  import {
    boundedDomain,
    configuredChartViewport as configuredDomain,
    fromScreenX,
    fromScreenY,
    insidePlot,
    toScreenX,
    toScreenY,
    transformScale as transformed,
    validateChartAxes as validateAxes,
    viewportFromSelection,
    type Domain,
    type Drag,
    type Frame,
    type Point,
    type Viewport,
  } from "./metric-chart-viewport";
  import MetricChartSettings from "./MetricChartSettings.svelte";

  export let metric: string;
  export let identity = metric;
  export let title: string | undefined = undefined;
  export let series: MetricChartSeries[] = [];
  export let xAlignment: XAlignment = "step";
  export let parentViewport: MetricChartViewport | null = null;
  export let highlightedRunId: string | null = null;
  export let onvisibilitychange: (metric: string, visible: boolean) => void;
  export let onviewportchange: (
    metric: string,
    xMinimum: number | null,
    xMaximum: number | null,
  ) => void = () => {};
  export let loadError: string | null = null;
  export let onretry: (metric: string) => void = () => {};

  type InteractionMode = "pan" | "select";
  type DisplayMode = "band" | "line";

  const CANVAS_PIXEL_BUDGET = 8_000_000;
  const CANVAS_DIMENSION_LIMIT = 4_096;
  const CANVAS_DPR_LIMIT = 2;
  const DEFAULT_PREFERENCES = {
    displayMode: "band" as const,
    smoothingMode: "none" as const,
    smoothingAmount: 0.15,
    xScale: "linear" as const,
    yScale: "linear" as const,
    xMinimum: "",
    xMaximum: "",
    yMinimum: "",
    yMaximum: "",
  };

  let card: HTMLElement;
  let canvas: HTMLCanvasElement;
  let plotCanvas: HTMLCanvasElement;
  let expandButton: HTMLButtonElement;
  let visible = false;
  let layoutRevision = 0;
  let overlayRevision = 0;
  let interactionMode: InteractionMode = "pan";
  let displayMode: DisplayMode = DEFAULT_PREFERENCES.displayMode;
  let smoothingMode: SmoothingMode = DEFAULT_PREFERENCES.smoothingMode;
  let smoothingAmount = DEFAULT_PREFERENCES.smoothingAmount;
  let xScale: ScaleMode = DEFAULT_PREFERENCES.xScale;
  let yScale: ScaleMode = DEFAULT_PREFERENCES.yScale;
  let xMinimum = DEFAULT_PREFERENCES.xMinimum;
  let xMaximum = DEFAULT_PREFERENCES.xMaximum;
  let yMinimum = DEFAULT_PREFERENCES.yMinimum;
  let yMaximum = DEFAULT_PREFERENCES.yMaximum;
  let viewport: Viewport | null = null;
  let configuredViewport: Viewport | null = null;
  let frame: Frame | null = null;
  let drag: Drag | null = null;
  let hoverX: number | null = null;
  let hoverPosition: Point | null = null;
  let pendingPointer: Point | null = null;
  let pointerFrame: number | null = null;
  let viewportRequestTimer: number | null = null;
  let tooltip: HTMLElement | null = null;
  let tooltipPositionFrame: number | null = null;
  let tooltipHideTimer: number | null = null;
  let tooltipHovered = false;
  let tooltipPositioned = false;
  let preferenceIdentity = "";
  let synchronizedParentViewportKey = "";
  let activeAlignment = xAlignment;
  let seriesIdentity = "";
  let hiddenRunIds = new Set<string>();
  let legendHighlightedRunId: string | null = null;
  let activeHighlightedRunId: string | null = null;
  let preparedSeries: PreparedMetricSeries[] = [];
  let availableRunCount = 0;
  let legendSeries: PreparedMetricSeries[] = [];
  let visibleSeries: PreparedMetricSeries[] = [];
  let renderableSeries: PreparedMetricSeries[] = [];
  let hoverPoints: SeriesHoverPoint[] = [];
  let xValues: Array<number | null> = [];
  let domainValues: Array<number | null> = [];
  let axisWarning: string | null = null;
  let anyLoading = false;
  let anyPending = false;
  let expanded = false;
  let settingsOpen = false;
  let mounted = false;
  let focusTrapActive = false;
  let ownsModalBodyLock = false;
  let restoreFocusElement: HTMLElement | null = null;
  let inertedElements: Array<{ element: HTMLElement; wasInert: boolean }> = [];

  $: updateLayerListeners(expanded);

  $: if (identity !== preferenceIdentity) {
    clearViewportRequest();
    const saved = readChartPreferences(identity) ?? DEFAULT_PREFERENCES;
    preferenceIdentity = identity;
    displayMode = saved.displayMode;
    smoothingMode = saved.smoothingMode;
    smoothingAmount = saved.smoothingAmount;
    xScale = saved.xScale;
    yScale = saved.yScale;
    xMinimum = saved.xMinimum;
    xMaximum = saved.xMaximum;
    yMinimum = saved.yMinimum;
    yMaximum = saved.yMaximum;
    resetTransientState();
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

  $: if (xAlignment !== activeAlignment) {
    activeAlignment = xAlignment;
    resetView();
  }

  $: {
    const nextIdentity = runSetIdentity(series);
    if (nextIdentity !== seriesIdentity) {
      seriesIdentity = nextIdentity;
      const currentRunIds = new Set(series.map(({ runId }) => runId));
      hiddenRunIds = new Set([...hiddenRunIds].filter((runId) => currentRunIds.has(runId)));
      if (legendHighlightedRunId && !currentRunIds.has(legendHighlightedRunId)) {
        legendHighlightedRunId = null;
      }
      clearViewportRequest();
      resetTransientState();
    }
  }

  $: preparedSeries = series.map((item) =>
    prepareMetricSeries(item, metric, smoothingMode, smoothingAmount),
  );
  $: availableRunCount = series.filter((item) => item.available).length;
  $: legendSeries = preparedSeries.filter((item) => item.available);
  $: activeHighlightedRunId =
    (legendHighlightedRunId && legendSeries.some((item) => item.runId === legendHighlightedRunId)
      ? legendHighlightedRunId
      : null) ??
    (highlightedRunId && legendSeries.some((item) => item.runId === highlightedRunId)
      ? highlightedRunId
      : null);
  $: visibleSeries = preparedSeries.filter((item) => !hiddenRunIds.has(item.runId));
  $: renderableSeries = visibleSeries.filter((item) => item.status === "ready");
  $: xValues = renderableSeries.flatMap((item) => item.x);
  $: domainValues = renderableSeries.flatMap((item) =>
    displayMode === "band" ? [...item.minimum, ...item.maximum, ...item.smoothed] : item.smoothed,
  );
  $: configuredViewport = configuredDomain(
    xValues,
    domainValues,
    xScale,
    yScale,
    xMinimum,
    xMaximum,
    yMinimum,
    yMaximum,
  );
  $: synchronizeParentViewport(
    JSON.stringify([
      identity,
      seriesIdentity,
      activeAlignment,
      metricChartViewportKey(parentViewport),
    ]),
    parentViewport,
    configuredViewport,
  );
  $: axisWarning = validateAxes(
    xMinimum,
    xMaximum,
    xScale,
    yMinimum,
    yMaximum,
    yScale,
    xValues,
    domainValues,
  );
  $: anyLoading = series.some((item) => item.loading);
  $: anyPending = preparedSeries.some(
    (item) => item.status === "loading" || item.status === "not-loaded",
  );
  $: hoverPoints =
    hoverX !== null && frame
      ? closestSeriesPoints(
          renderableSeries,
          hoverX,
          xScale,
          frame.x.minimum,
          frame.x.maximum,
          yScale,
        )
      : [];
  $: if (tooltip && hoverPosition && hoverPoints.length > 0) {
    queueTooltipPosition(tooltip, hoverPosition, overlayRevision);
  }

  onMount(() => {
    mounted = true;
    const observer = new IntersectionObserver(
      (entries) => {
        const nextVisible = entries.some((entry) => entry.isIntersecting);
        if (nextVisible === visible) return;
        visible = nextVisible;
        if (!visible) clearViewportRequest();
        onvisibilitychange(metric, visible);
      },
      { rootMargin: "500px 0px" },
    );
    const resizeObserver = new ResizeObserver(redrawAll);
    const theme = window.matchMedia("(prefers-color-scheme: dark)");
    const redraw = redrawAll;
    const repositionTooltip = () => {
      if (tooltip && hoverPosition && hoverPoints.length > 0) {
        queueTooltipPosition(tooltip, hoverPosition, overlayRevision);
      }
    };
    observer.observe(card);
    resizeObserver.observe(card);
    theme.addEventListener("change", redraw);
    window.addEventListener("resize", repositionTooltip);
    window.addEventListener("scroll", repositionTooltip, true);
    updateLayerListeners(expanded);
    return () => {
      mounted = false;
      updateLayerListeners(false);
      visible = false;
      onvisibilitychange(metric, false);
      observer.disconnect();
      resizeObserver.disconnect();
      theme.removeEventListener("change", redraw);
      window.removeEventListener("resize", repositionTooltip);
      window.removeEventListener("scroll", repositionTooltip, true);
      if (pointerFrame !== null) window.cancelAnimationFrame(pointerFrame);
      clearTooltipTimers();
      clearViewportRequest();
      setSurroundingContentInert(false);
      setModalBodyLock(false);
    };
  });

  $: if (plotCanvas && visible && renderableSeries.length > 0 && layoutRevision >= 0) {
    drawChart(
      plotCanvas,
      renderableSeries,
      displayMode,
      xScale,
      yScale,
      configuredViewport,
      viewport,
      activeHighlightedRunId,
    );
  }

  $: if (canvas && visible && frame && renderableSeries.length > 0 && overlayRevision >= 0) {
    drawInteraction(canvas, frame, xScale, yScale, hoverX, drag);
  }

  function redrawAll(): void {
    layoutRevision += 1;
    overlayRevision += 1;
  }

  function drawChart(
    target: HTMLCanvasElement,
    candidates: PreparedMetricSeries[],
    display: DisplayMode,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    configured: Viewport | null,
    activeViewport: Viewport | null,
    highlighted: string | null,
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
    const plotWidth = Math.max(width - padding.left - padding.right, 1);
    const plotHeight = Math.max(height - padding.top - padding.bottom, 1);

    drawAxes(context, currentFrame, horizontalScale, verticalScale, gridColor, mutedColor);

    context.save();
    context.beginPath();
    context.rect(padding.left, padding.top, plotWidth, plotHeight);
    context.clip();

    for (const candidate of candidates) {
      const emphasized = highlighted === null || highlighted === candidate.runId;
      if (display === "band") {
        drawBand(
          context,
          candidate.buckets,
          candidate.x,
          candidate.minimum,
          candidate.maximum,
          currentFrame,
          horizontalScale,
          verticalScale,
          candidate.color,
          emphasized ? 0.13 : 0.025,
        );
      }
      drawLine(
        context,
        candidate.buckets,
        candidate.x,
        candidate.smoothed,
        currentFrame,
        horizontalScale,
        verticalScale,
        candidate.color,
        candidate.pattern,
        emphasized ? 1 : 0.16,
        emphasized ? 1.7 : 1.2,
      );
    }

    context.globalAlpha = 1;
    context.restore();
  }

  function drawInteraction(
    target: HTMLCanvasElement,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    activeHoverX: number | null,
    activeDrag: Drag | null,
  ): void {
    const { width, height, padding } = activeFrame;
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
    const styles = getComputedStyle(target);
    const mutedColor = styles.getPropertyValue("--muted").trim() || "#596168";
    const accentColor = styles.getPropertyValue("--accent").trim() || "#2766ad";
    const plotWidth = Math.max(width - padding.left - padding.right, 1);
    const plotHeight = Math.max(height - padding.top - padding.bottom, 1);
    context.save();
    context.beginPath();
    context.rect(padding.left, padding.top, plotWidth, plotHeight);
    context.clip();

    if (activeDrag && interactionMode !== "pan") {
      const left = Math.min(activeDrag.start.x, activeDrag.current.x);
      const top = Math.min(activeDrag.start.y, activeDrag.current.y);
      const dragWidth = Math.abs(activeDrag.current.x - activeDrag.start.x);
      const dragHeight = Math.abs(activeDrag.current.y - activeDrag.start.y);
      context.fillStyle = accentColor;
      context.globalAlpha = 0.12;
      context.fillRect(left, top, dragWidth, dragHeight);
      context.globalAlpha = 1;
      context.strokeStyle = accentColor;
      context.setLineDash([4, 3]);
      context.strokeRect(left, top, dragWidth, dragHeight);
      context.setLineDash([]);
    }

    if (
      activeHoverX !== null &&
      Number.isFinite(activeHoverX) &&
      (horizontalScale !== "log" || activeHoverX > 0)
    ) {
      const x = toScreenX(activeHoverX, activeFrame, horizontalScale);
      context.globalAlpha = 1;
      context.strokeStyle = mutedColor;
      context.setLineDash([3, 3]);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      context.setLineDash([]);
    }
    context.globalAlpha = 1;
    context.restore();
  }

  function drawAxes(
    context: CanvasRenderingContext2D,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    gridColor: string,
    mutedColor: string,
  ): void {
    const { width, height, padding } = activeFrame;
    const plotWidth = Math.max(width - padding.left - padding.right, 1);
    context.font = "10px system-ui, sans-serif";
    context.lineWidth = 1;
    context.strokeStyle = gridColor;
    context.fillStyle = mutedColor;
    const yTicks = axisTicks(activeFrame.y.minimum, activeFrame.y.maximum, 5, verticalScale);
    for (const tick of yTicks) {
      const y = toScreenY(tick, activeFrame, verticalScale);
      context.beginPath();
      context.moveTo(padding.left, y);
      context.lineTo(width - padding.right, y);
      context.stroke();
      const label = formatAxis(tick);
      context.fillText(label, padding.left - context.measureText(label).width - 8, y + 3);
    }

    const tickCount = Math.max(4, Math.min(10, Math.floor(plotWidth / 100)));
    const xTicks = axisTicks(
      activeFrame.x.minimum,
      activeFrame.x.maximum,
      tickCount,
      horizontalScale,
    );
    for (const [index, tick] of xTicks.entries()) {
      const x = toScreenX(tick, activeFrame, horizontalScale);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      const label = formatHorizontalAxis(tick);
      const measured = context.measureText(label).width;
      const labelX =
        index === 0
          ? Math.max(x, padding.left)
          : index === xTicks.length - 1
            ? Math.min(x - measured, width - padding.right - measured)
            : x - measured / 2;
      context.fillText(label, labelX, height - 9);
    }
  }

  function drawBand(
    context: CanvasRenderingContext2D,
    buckets: number[],
    xValues: number[],
    lower: Array<number | null>,
    upper: Array<number | null>,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    color: string,
    opacity: number,
  ): void {
    context.fillStyle = color;
    context.globalAlpha = opacity;
    const valid = xValues.map(
      (x, index) =>
        validPoint(x, lower[index], horizontalScale, verticalScale) &&
        validPoint(x, upper[index], horizontalScale, verticalScale),
    );
    for (const { start, end } of contiguousBucketRanges(buckets, valid)) {
      context.beginPath();
      for (let point = start; point < end; point += 1) {
        const x = toScreenX(xValues[point], activeFrame, horizontalScale);
        const y = toScreenY(upper[point] as number, activeFrame, verticalScale);
        if (point === start) context.moveTo(x, y);
        else context.lineTo(x, y);
      }
      for (let point = end - 1; point >= start; point -= 1) {
        context.lineTo(
          toScreenX(xValues[point], activeFrame, horizontalScale),
          toScreenY(lower[point] as number, activeFrame, verticalScale),
        );
      }
      context.closePath();
      context.fill();
    }
    context.globalAlpha = 1;
  }

  function drawLine(
    context: CanvasRenderingContext2D,
    buckets: number[],
    xValues: number[],
    values: Array<number | null>,
    activeFrame: Frame,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    color: string,
    pattern: SeriesPattern,
    opacity: number,
    width: number,
  ): void {
    context.strokeStyle = color;
    context.lineWidth = width;
    context.lineJoin = "round";
    context.globalAlpha = opacity;
    context.setLineDash(lineDash(pattern));
    context.beginPath();
    const valid = xValues.map((x, index) =>
      validPoint(x, values[index], horizontalScale, verticalScale),
    );
    for (const { start, end } of contiguousBucketRanges(buckets, valid)) {
      for (let index = start; index < end; index += 1) {
        const x = toScreenX(xValues[index], activeFrame, horizontalScale);
        const y = toScreenY(values[index] as number, activeFrame, verticalScale);
        if (index === start) context.moveTo(x, y);
        else context.lineTo(x, y);
      }
    }
    context.stroke();
    context.setLineDash([]);
    context.globalAlpha = 1;
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

  function topLayerTooltip(node: HTMLElement): { destroy: () => void } {
    tooltipPositioned = false;
    let opened = false;
    try {
      node.showPopover();
      opened = true;
    } catch {
      // The tooltip-open card class removes paint containment for older browsers.
    }
    return {
      destroy: () => {
        if (opened && node.matches(":popover-open")) node.hidePopover();
      },
    };
  }

  function queueTooltipPosition(target: HTMLElement, point: Point, _revision: number): void {
    if (tooltipPositionFrame !== null) window.cancelAnimationFrame(tooltipPositionFrame);
    tooltipPositionFrame = window.requestAnimationFrame(() => {
      tooltipPositionFrame = null;
      if (!target.isConnected || !canvas?.isConnected) return;
      const canvasBounds = canvas.getBoundingClientRect();
      const anchorX = canvasBounds.left + point.x;
      const anchorY = canvasBounds.top + point.y;
      const viewportWidth = document.documentElement.clientWidth;
      const viewportHeight = document.documentElement.clientHeight;
      const margin = 8;
      const gap = 12;
      const width = Math.max(240, Math.min(420, viewportWidth - margin * 2));
      const maximumHeight = Math.max(120, viewportHeight - margin * 2);
      target.style.width = `${width}px`;
      target.style.maxHeight = `${maximumHeight}px`;
      target.style.setProperty(
        "--tooltip-table-max-height",
        `${Math.max(80, maximumHeight - 42)}px`,
      );
      const height = Math.min(target.getBoundingClientRect().height, maximumHeight);
      const fitsRight = anchorX + gap + width <= viewportWidth - margin;
      const fitsLeft = anchorX - gap - width >= margin;
      const placeRight = fitsRight || (!fitsLeft && viewportWidth - anchorX >= anchorX);
      const desiredLeft = placeRight ? anchorX + gap : anchorX - gap - width;
      const left = Math.max(margin, Math.min(viewportWidth - width - margin, desiredLeft));
      const top = Math.max(
        margin,
        Math.min(viewportHeight - height - margin, anchorY - height / 2),
      );
      target.style.left = `${left}px`;
      target.style.top = `${top}px`;
      tooltipPositioned = true;
    });
  }

  function clearTooltipTimers(): void {
    if (tooltipPositionFrame !== null) {
      window.cancelAnimationFrame(tooltipPositionFrame);
      tooltipPositionFrame = null;
    }
    if (tooltipHideTimer !== null) {
      window.clearTimeout(tooltipHideTimer);
      tooltipHideTimer = null;
    }
  }

  function cancelTooltipHide(): void {
    if (tooltipHideTimer === null) return;
    window.clearTimeout(tooltipHideTimer);
    tooltipHideTimer = null;
  }

  function scheduleTooltipHide(): void {
    cancelTooltipHide();
    tooltipHideTimer = window.setTimeout(() => {
      tooltipHideTimer = null;
      if (tooltipHovered || tooltip?.contains(document.activeElement)) return;
      clearTooltip();
    }, 140);
  }

  function clearTooltip(): void {
    tooltipPositioned = false;
    pendingPointer = null;
    hoverX = null;
    hoverPosition = null;
    overlayRevision += 1;
  }

  function tooltipEnter(): void {
    tooltipHovered = true;
    cancelTooltipHide();
  }

  function tooltipLeave(): void {
    tooltipHovered = false;
    scheduleTooltipHide();
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
    cancelTooltipHide();
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
      overlayRevision += 1;
      return;
    }
    if (!insidePlot(point, frame) || renderableSeries.length === 0) {
      hoverX = null;
      return;
    }
    hoverX = fromScreenX(point.x, frame, xScale);
    overlayRevision += 1;
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
    if (interactionMode === "select" && distanceX >= 6) {
      const selectedViewport = viewportFromSelection(completed, frame, xScale, yScale);
      viewport = {
        x: selectedViewport.x,
        y: distanceY >= 6 ? selectedViewport.y : completed.viewport.y,
      };
    }
    if (interactionMode === "pan" || viewport !== null) scheduleViewportRequest(viewport);
    overlayRevision += 1;
  }

  function pointerLeave(): void {
    if (drag) return;
    pendingPointer = null;
    scheduleTooltipHide();
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

  function wheel(event: WheelEvent): void {
    if (!frame || !insidePlot(canvasPoint(event), frame)) return;
    event.preventDefault();
    zoomAt(canvasPoint(event), Math.exp(Math.max(-1, Math.min(1, event.deltaY / 240))));
  }

  function zoomAt(point: Point, factor: number): void {
    if (!frame) return;
    const anchorX = transformed(fromScreenX(point.x, frame, xScale), xScale);
    const anchorY = transformed(fromScreenY(point.y, frame, yScale), yScale);
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
    overlayRevision += 1;
  }

  function pointerCancel(event: PointerEvent): void {
    pendingPointer = null;
    if (interactionMode === "pan" && drag) viewport = drag.viewport;
    drag = null;
    if (canvas.hasPointerCapture(event.pointerId)) canvas.releasePointerCapture(event.pointerId);
    overlayRevision += 1;
  }

  function chartKeydown(event: KeyboardEvent): void {
    if (event.key === "Escape" && expanded) {
      toggleExpanded();
      event.preventDefault();
      event.stopPropagation();
      return;
    }
    if (!frame) return;
    const center = {
      x: (frame.padding.left + frame.width - frame.padding.right) / 2,
      y: (frame.padding.top + frame.height - frame.padding.bottom) / 2,
    };
    if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      moveKeyboardCrosshair(event.key);
    } else if (["+", "="].includes(event.key)) zoomAt(center, 0.5);
    else if (event.key === "-") zoomAt(center, 2);
    else if (["0", "Escape"].includes(event.key)) resetView();
    else return;
    event.preventDefault();
  }

  function moveKeyboardCrosshair(key: string): void {
    if (!frame) return;
    const coordinates = [
      ...new Set(
        renderableSeries.flatMap((candidate) =>
          candidate.x.filter(
            (coordinate) =>
              Number.isFinite(coordinate) &&
              coordinate >= frame!.x.minimum &&
              coordinate <= frame!.x.maximum &&
              (xScale !== "log" || coordinate > 0),
          ),
        ),
      ),
    ].sort((left, right) => left - right);
    if (coordinates.length === 0) return;
    let index: number;
    if (key === "Home") index = 0;
    else if (key === "End") index = coordinates.length - 1;
    else if (hoverX === null) index = key === "ArrowLeft" ? coordinates.length - 1 : 0;
    else if (key === "ArrowLeft") {
      const previous = coordinates.findLastIndex((coordinate) => coordinate < hoverX!);
      index = previous < 0 ? 0 : previous;
    } else {
      const next = coordinates.findIndex((coordinate) => coordinate > hoverX!);
      index = next < 0 ? coordinates.length - 1 : next;
    }
    hoverX = coordinates[index];
    hoverPosition = {
      x: toScreenX(hoverX, frame, xScale),
      y: frame.padding.top + (frame.height - frame.padding.top - frame.padding.bottom) / 2,
    };
    cancelTooltipHide();
    overlayRevision += 1;
  }

  function resetTransientState(): void {
    clearTooltipTimers();
    tooltipHovered = false;
    tooltipPositioned = false;
    viewport = null;
    drag = null;
    hoverX = null;
    hoverPosition = null;
    overlayRevision += 1;
  }

  function synchronizeParentViewport(
    synchronizationKey: string,
    next: MetricChartViewport | null,
    configured: Viewport | null,
  ): void {
    if (synchronizationKey === synchronizedParentViewportKey) return;
    if (next !== null && !configured) return;
    if (
      next !== null &&
      (!Number.isFinite(next.minimum) ||
        !Number.isFinite(next.maximum) ||
        next.minimum >= next.maximum ||
        (xScale === "log" && next.minimum <= 0))
    ) {
      return;
    }

    synchronizedParentViewportKey = synchronizationKey;
    clearViewportRequest();
    if (next === null) {
      if (viewport !== null) {
        viewport = null;
        resetPointerState();
        overlayRevision += 1;
      }
      return;
    }

    const nextX = boundedDomain(
      transformed(next.minimum, xScale),
      transformed(next.maximum, xScale),
      xScale,
    );
    if (viewport && domainsEqual(viewport.x, nextX)) return;
    viewport = { x: nextX, y: viewport?.y ?? configured!.y };
    resetPointerState();
    overlayRevision += 1;
  }

  function domainsEqual(left: Domain, right: Domain): boolean {
    return left.minimum === right.minimum && left.maximum === right.maximum;
  }

  function resetPointerState(): void {
    drag = null;
    hoverX = null;
    hoverPosition = null;
    pendingPointer = null;
    clearTooltipTimers();
  }

  function resetView(): void {
    resetTransientState();
    scheduleViewportRequest(null, true);
  }

  function clearViewportRequest(): void {
    if (viewportRequestTimer === null) return;
    window.clearTimeout(viewportRequestTimer);
    viewportRequestTimer = null;
  }

  function scheduleViewportRequest(next: Viewport | null, immediate = false): void {
    clearViewportRequest();
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
    overlayRevision += 1;
  }

  function toggleRun(runId: string): void {
    const next = new Set(hiddenRunIds);
    if (next.has(runId)) next.delete(runId);
    else next.add(runId);
    hiddenRunIds = next;
    hoverX = null;
    overlayRevision += 1;
  }

  function showAllRuns(): void {
    hiddenRunIds = new Set<string>();
    hoverX = null;
    overlayRevision += 1;
  }

  function highlightRun(runId: string | null): void {
    legendHighlightedRunId = runId;
    overlayRevision += 1;
  }

  function toggleExpanded(): void {
    const nextExpanded = !expanded;
    settingsOpen = false;
    if (nextExpanded) {
      restoreFocusElement =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
    }
    expanded = nextExpanded;
    setModalBodyLock(expanded);
    setSurroundingContentInert(expanded);
    window.requestAnimationFrame(() => {
      redrawAll();
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
    if (event.key !== "Escape" || !expanded) return;
    toggleExpanded();
    event.preventDefault();
    event.stopPropagation();
  }

  function updateLayerListeners(enlarged: boolean): void {
    const trapActive = mounted && enlarged;
    if (trapActive !== focusTrapActive) {
      focusTrapActive = trapActive;
      if (trapActive) {
        window.addEventListener("keydown", cardKeydown);
        document.addEventListener("focusin", documentFocusIn);
      } else {
        window.removeEventListener("keydown", cardKeydown);
        document.removeEventListener("focusin", documentFocusIn);
      }
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
    if (!chartLayerContains(active) || active === card) {
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
    if (!expanded || chartLayerContains(event.target)) return;
    (focusableElements()[0] ?? expandButton)?.focus();
  }

  function focusableElements(): HTMLElement[] {
    const cardElements = Array.from(
      card.querySelectorAll<HTMLElement>(
        'button:not([disabled]):not([tabindex="-1"]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
      ),
    );
    const tooltipElements = tooltip
      ? Array.from(
          tooltip.querySelectorAll<HTMLElement>(
            'button:not([disabled]):not([tabindex="-1"]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
          ),
        )
      : [];
    return [...cardElements, ...tooltipElements].filter(
      (element) => element.getClientRects().length > 0,
    );
  }

  function chartLayerContains(target: EventTarget | null): boolean {
    return target instanceof Node && (card.contains(target) || Boolean(tooltip?.contains(target)));
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

  function alignmentLabel(alignment: XAlignment): string {
    if (alignment === "relative-step") return "Relative step";
    if (alignment === "elapsed-time") return "Elapsed time";
    return "Absolute step";
  }

  function statusLabel(item: PreparedMetricSeries): string | null {
    if (item.status === "loading") return "loading";
    if (item.status === "no-data") return "no metric";
    if (item.loading) return "updating";
    return null;
  }

  function formatAxis(value: number): string {
    if (!Number.isFinite(value)) return "—";
    if (Math.abs(value) >= 10_000 || (Math.abs(value) > 0 && Math.abs(value) < 0.001)) {
      return value.toExponential(2);
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }

  function formatHorizontalAxis(value: number): string {
    return xAlignment === "elapsed-time" ? formatDurationMs(value) : formatAxis(value);
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
  class="metric-chart-card comparison-chart"
  class:expanded
  class:tooltip-open={hoverPoints.length > 0 && hoverPosition !== null}
  role={expanded ? "dialog" : undefined}
  aria-modal={expanded ? "true" : undefined}
  aria-keyshortcuts={expanded ? "Escape" : undefined}
  aria-label={`${title ?? metric} metric comparison panel`}
>
  <div class="chart-heading">
    <div class="chart-title">
      <strong>{title ?? metric}</strong>
      <small>{availableRunCount} {availableRunCount === 1 ? "run" : "runs"}</small>
      {#if anyLoading}<span class="loading-label">updating</span>{/if}
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
      <MetricChartSettings
        bind:open={settingsOpen}
        bind:displayMode
        bind:smoothingMode
        bind:smoothingAmount
        {xAlignment}
        bind:xScale
        bind:yScale
        bind:xMinimum
        bind:xMaximum
        bind:yMinimum
        bind:yMaximum
        {axisWarning}
        onviewchange={resetView}
      />
    </div>
  </div>

  {#if legendSeries.length > 0}
    <div class="chart-legend" role="list" aria-label="Compared runs">
      {#each legendSeries as item (item.runId)}
        {@const status = statusLabel(item)}
        <div
          class="legend-entry"
          class:hidden={hiddenRunIds.has(item.runId)}
          class:highlighted={activeHighlightedRunId === item.runId}
          role="listitem"
          onmouseenter={() => highlightRun(item.runId)}
          onmouseleave={() => highlightRun(null)}
          onfocusin={() => highlightRun(item.runId)}
          onfocusout={(event) => {
            if (
              !(event.relatedTarget instanceof Node) ||
              !event.currentTarget.contains(event.relatedTarget)
            ) {
              highlightRun(null);
            }
          }}
        >
          <button
            class="legend-toggle"
            type="button"
            aria-label={`${hiddenRunIds.has(item.runId) ? "Show" : "Hide"} ${item.runName} (${item.runId.slice(0, 8)})`}
            aria-pressed={!hiddenRunIds.has(item.runId)}
            onclick={() => toggleRun(item.runId)}
          >
            <span
              class={`series-swatch pattern-${item.pattern}`}
              style={`--series-color: ${item.color}`}
              aria-hidden="true"
            ></span>
            <span class="legend-name" title={item.runName}>{item.runName}</span>
            {#if status}<small class:no-data={item.status === "no-data"}>{status}</small>{/if}
          </button>
        </div>
      {/each}
      {#if legendSeries.some((item) => hiddenRunIds.has(item.runId))}
        <button class="legend-show-all" type="button" onclick={showAllRuns}>show all</button>
      {/if}
    </div>
  {/if}

  {#if loadError}
    <div class="chart-load-error" role="alert">
      <span>{loadError}</span>
      <button type="button" onclick={() => onretry(metric)}>Retry</button>
    </div>
  {/if}

  {#if renderableSeries.length > 0}
    <div class={`chart-canvas-wrap chart-mode-${interactionMode}`}>
      <canvas bind:this={plotCanvas} class="chart-plot-canvas" aria-hidden="true"></canvas>
      <canvas
        bind:this={canvas}
        class="chart-interaction-canvas"
        tabindex="0"
        aria-label={`${metric} comparison history chart. Use left and right arrows to inspect points, plus and minus to zoom, and zero to reset.`}
        onpointerdown={pointerDown}
        onpointermove={pointerMove}
        onpointerup={pointerUp}
        onpointercancel={pointerCancel}
        onpointerleave={pointerLeave}
        onwheel={wheel}
        onkeydown={chartKeydown}
      ></canvas>
      <span class="visually-hidden" aria-live="polite">
        {#if hoverPoints.length > 0}
          {metric}, {hoverPoints.length} runs near x {formatHorizontalAxis(hoverX ?? 0)}.
          {#each hoverPoints as point}
            {point.series.runName}, value {formatAxis(point.smoothed)}, step
            {formatAxis(point.step)}.
          {/each}
        {/if}
      </span>
      {#if hoverPoints.length > 0 && hoverPosition}
        <div
          bind:this={tooltip}
          use:topLayerTooltip
          class="chart-tooltip comparison-tooltip"
          class:positioned={tooltipPositioned}
          popover="manual"
          role="tooltip"
          onpointerenter={tooltipEnter}
          onpointerleave={tooltipLeave}
          onfocusin={tooltipEnter}
          onfocusout={tooltipLeave}
        >
          <div class="tooltip-heading">
            <strong>{metric}</strong>
            <span>{alignmentLabel(xAlignment)} {formatHorizontalAxis(hoverX ?? 0)}</span>
          </div>
          <!-- svelte-ignore a11y_no_noninteractive_tabindex (scrollable comparison data needs keyboard focus) -->
          <div class="tooltip-table-wrap" role="region" tabindex="0" aria-label="Comparison values">
            <table>
              <caption class="visually-hidden">
                Nearest value and source step for each visible run
              </caption>
              <thead>
                <tr>
                  <th scope="col">Run</th>
                  <th scope="col">Value</th>
                  <th scope="col">Step</th>
                </tr>
              </thead>
              <tbody>
                {#each hoverPoints as point (point.series.runId)}
                  <tr
                    class:deemphasized={activeHighlightedRunId &&
                      activeHighlightedRunId !== point.series.runId}
                  >
                    <th scope="row" title={point.series.runName}>
                      <span class="tooltip-series">
                        <span
                          class={`series-swatch pattern-${point.series.pattern}`}
                          style={`--series-color: ${point.series.color}`}
                          aria-hidden="true"
                        ></span>
                        <span class="tooltip-run-name">{point.series.runName}</span>
                      </span>
                    </th>
                    <td>{formatAxis(point.smoothed)}</td>
                    <td>{formatAxis(point.step)}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </div>
      {/if}
    </div>
  {:else if loadError}
    <div class="chart-placeholder">History could not be loaded.</div>
  {:else if series.length === 0}
    <div class="chart-placeholder">Select at least one run to compare.</div>
  {:else if anyPending}
    <div class="chart-placeholder">Loading bounded histories…</div>
  {:else}
    <div class="chart-placeholder">This metric has no data in the visible runs.</div>
  {/if}
</article>

<style>
  .comparison-chart {
    display: flex;
    flex-direction: column;
  }

  .comparison-chart.tooltip-open:not(.expanded) {
    position: relative;
    z-index: 20;
    content-visibility: visible;
    contain: none;
  }

  .chart-title {
    min-width: 0;
    flex-wrap: wrap;
  }

  .chart-title small {
    color: var(--muted);
    font-size: 11px;
    font-weight: 500;
  }

  .chart-legend {
    min-height: 35px;
    display: flex;
    gap: 4px;
    align-items: center;
    padding: 4px 0 7px;
    overflow-x: auto;
    scrollbar-width: thin;
  }

  .legend-entry {
    min-width: 0;
    display: flex;
    flex: 0 1 250px;
    align-items: stretch;
    border: 1px solid transparent;
    background: var(--control-bg);
  }

  .legend-entry:hover,
  .legend-entry.highlighted {
    border-color: var(--line);
  }

  .legend-entry.hidden {
    opacity: 0.48;
  }

  .legend-toggle,
  .legend-show-all {
    min-width: 0;
    border: 0;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
  }

  .legend-toggle {
    width: 100%;
    display: grid;
    grid-template-columns: 28px minmax(40px, 1fr) auto;
    gap: 6px;
    align-items: center;
    padding: 5px 7px;
    text-align: left;
  }

  .legend-toggle:hover,
  .legend-show-all:hover {
    background: var(--button-hover);
    color: var(--text);
  }

  .legend-name {
    overflow: hidden;
    color: var(--text);
    font-size: 11px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .legend-toggle small {
    color: var(--muted);
    font-size: 11px;
    white-space: nowrap;
  }

  .legend-toggle small.no-data {
    color: var(--faint);
  }

  .legend-show-all {
    flex: none;
    padding: 6px 8px;
    font-size: 11px;
    white-space: nowrap;
  }

  .series-swatch {
    --series-color: var(--series-accent);
    width: 26px;
    height: 4px;
    display: inline-block;
    flex: none;
    background: var(--series-color);
  }

  .series-swatch.pattern-dash {
    background: repeating-linear-gradient(90deg, var(--series-color) 0 7px, transparent 7px 11px);
  }

  .series-swatch.pattern-dot {
    background: repeating-linear-gradient(90deg, var(--series-color) 0 2px, transparent 2px 6px);
  }

  .series-swatch.pattern-dash-dot {
    background: repeating-linear-gradient(
      90deg,
      var(--series-color) 0 8px,
      transparent 8px 11px,
      var(--series-color) 11px 13px,
      transparent 13px 17px
    );
  }

  .chart-canvas-wrap {
    position: relative;
    min-height: 0;
    flex: 1;
  }

  .chart-interaction-canvas {
    position: absolute;
    inset: 0;
    background: transparent;
  }

  .comparison-tooltip {
    position: fixed;
    z-index: 1100;
    inset: auto;
    width: min(420px, calc(100vw - 16px));
    max-width: calc(100vw - 16px);
    max-height: calc(100dvh - 16px);
    display: block;
    overflow: hidden;
    padding: 0;
    margin: 0;
    pointer-events: auto;
    transform: none;
    visibility: hidden;
  }

  .comparison-tooltip.positioned {
    visibility: visible;
  }

  .tooltip-heading {
    display: flex;
    gap: 10px;
    justify-content: space-between;
    padding: 6px 8px;
    border-bottom: 1px solid var(--line);
  }

  .tooltip-heading strong {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .tooltip-heading span {
    flex: none;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .tooltip-table-wrap {
    max-height: var(--tooltip-table-max-height, calc(100dvh - 58px));
    overflow: auto;
    overscroll-behavior: contain;
    outline: none;
  }

  .tooltip-table-wrap:focus-visible {
    box-shadow: inset 0 0 0 1px var(--accent);
  }

  .comparison-tooltip table {
    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;
    color: var(--muted);
    font-size: 11px;
    white-space: nowrap;
  }

  .comparison-tooltip th,
  .comparison-tooltip td {
    padding: 4px 6px;
    border-bottom: 1px solid var(--line);
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .comparison-tooltip thead th {
    position: sticky;
    z-index: 1;
    top: 0;
    background: var(--panel);
    color: var(--faint);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
  }

  .comparison-tooltip th:first-child {
    text-align: left;
  }

  .comparison-tooltip th:nth-child(2),
  .comparison-tooltip td:nth-child(2) {
    width: 96px;
  }

  .comparison-tooltip th:nth-child(3),
  .comparison-tooltip td:nth-child(3) {
    width: 80px;
  }

  .tooltip-series {
    display: flex;
    gap: 6px;
    align-items: center;
    min-width: 0;
  }

  .comparison-tooltip tbody th {
    overflow: hidden;
    color: var(--text);
    font-weight: 500;
  }

  .tooltip-run-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .comparison-tooltip .series-swatch {
    flex: 0 0 16px;
    width: 16px;
    transform: scale(0.8);
  }

  .comparison-tooltip tr.deemphasized {
    opacity: 0.34;
  }

  .chart-load-error {
    min-height: 34px;
    display: flex;
    gap: 12px;
    align-items: center;
    justify-content: space-between;
    padding: 6px 8px;
    border-left: 3px solid var(--danger);
    background: var(--error-bg);
    color: var(--error-text);
    font-size: 11px;
  }

  .chart-load-error button {
    min-height: 26px;
    padding: 0 9px;
    border: 1px solid var(--error-line);
    background: transparent;
  }

  .expanded .chart-legend {
    flex-wrap: wrap;
    overflow-x: visible;
  }

  .expanded .legend-entry {
    flex-basis: 280px;
  }

  @media (max-width: 720px) {
    .legend-entry {
      flex-basis: 210px;
    }

    .legend-toggle {
      grid-template-columns: 24px minmax(40px, 1fr);
    }

    .legend-toggle small {
      display: none;
    }
  }
</style>
