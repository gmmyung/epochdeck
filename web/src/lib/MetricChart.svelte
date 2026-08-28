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
  import { axisTicks, numericExtent, type ScaleMode, type SmoothingMode } from "./chart-data";
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

  export let metric: string;
  export let identity = metric;
  export let title: string | undefined = undefined;
  export let series: MetricChartSeries[] = [];
  export let xAlignment: XAlignment = "step";
  export let parentViewport: MetricChartViewport | null = null;
  export let onvisibilitychange: (metric: string, visible: boolean) => void;
  export let onviewportchange: (
    metric: string,
    xMinimum: number | null,
    xMaximum: number | null,
  ) => void = () => {};
  export let onalignmentchange: (metric: string, alignment: XAlignment) => void = () => {};

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
  let settings: HTMLDetailsElement;
  let settingsSummary: HTMLElement;
  let expandButton: HTMLButtonElement;
  let visible = false;
  let chartRevision = 0;
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
  let hiddenBeforeSolo: Set<string> | null = null;
  let soloRunId: string | null = null;
  let highlightedRunId: string | null = null;
  let preparedSeries: PreparedMetricSeries[] = [];
  let visibleSeries: PreparedMetricSeries[] = [];
  let renderableSeries: PreparedMetricSeries[] = [];
  let hoverPoints: SeriesHoverPoint[] = [];
  let xValues: Array<number | null> = [];
  let domainValues: Array<number | null> = [];
  let axisWarning: string | null = null;
  let anyLoading = false;
  let anyHistory = false;
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
      if (soloRunId && currentRunIds.has(soloRunId)) {
        hiddenRunIds = new Set(
          series.filter(({ runId }) => runId !== soloRunId).map(({ runId }) => runId),
        );
      } else {
        hiddenRunIds = new Set(
          [...(soloRunId ? (hiddenBeforeSolo ?? []) : hiddenRunIds)].filter((runId) =>
            currentRunIds.has(runId),
          ),
        );
        soloRunId = null;
        hiddenBeforeSolo = null;
      }
      if (highlightedRunId && !currentRunIds.has(highlightedRunId)) highlightedRunId = null;
      if (hiddenBeforeSolo) {
        hiddenBeforeSolo = new Set(
          [...hiddenBeforeSolo].filter((runId) => currentRunIds.has(runId)),
        );
      }
      clearViewportRequest();
      resetTransientState();
    }
  }

  $: preparedSeries = series.map((item) =>
    prepareMetricSeries(item, metric, smoothingMode, smoothingAmount),
  );
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
  $: anyHistory = series.some((item) => item.history !== undefined);
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
    queueTooltipPosition(tooltip, hoverPosition, hoverPoints.length, expanded, chartRevision);
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
    const resizeObserver = new ResizeObserver(() => (chartRevision += 1));
    const theme = window.matchMedia("(prefers-color-scheme: dark)");
    const redraw = () => (chartRevision += 1);
    const repositionTooltip = () => {
      if (tooltip && hoverPosition && hoverPoints.length > 0) {
        queueTooltipPosition(tooltip, hoverPosition, hoverPoints.length, expanded, chartRevision);
      }
    };
    observer.observe(card);
    resizeObserver.observe(card);
    theme.addEventListener("change", redraw);
    window.addEventListener("resize", repositionTooltip);
    window.addEventListener("scroll", repositionTooltip, true);
    updateLayerListeners(settingsOpen, expanded);
    return () => {
      mounted = false;
      updateLayerListeners(false, false);
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

  $: if (canvas && visible && renderableSeries.length > 0 && chartRevision >= 0) {
    drawChart(
      canvas,
      renderableSeries,
      displayMode,
      xScale,
      yScale,
      configuredViewport,
      viewport,
      hoverX,
      hoverPoints,
      drag,
      highlightedRunId,
    );
  }

  function configuredDomain(
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
    if (horizontalScale === "log" && !numericExtent(horizontalValues, horizontalScale)) {
      return "X logarithmic scale requires positive coordinates.";
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
    candidates: PreparedMetricSeries[],
    display: DisplayMode,
    horizontalScale: ScaleMode,
    verticalScale: ScaleMode,
    configured: Viewport | null,
    activeViewport: Viewport | null,
    activeHoverX: number | null,
    activeHoverPoints: SeriesHoverPoint[],
    activeDrag: Drag | null,
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
    const surfaceColor = styles.getPropertyValue("--surface").trim() || "#ffffff";
    const accentColor = styles.getPropertyValue("--accent").trim() || "#2766ad";
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

    if (activeDrag && interactionMode !== "pan") {
      const left = Math.min(activeDrag.start.x, activeDrag.current.x);
      const top = Math.min(activeDrag.start.y, activeDrag.current.y);
      const dragWidth = Math.abs(activeDrag.current.x - activeDrag.start.x);
      const dragHeight = Math.abs(activeDrag.current.y - activeDrag.start.y);
      context.globalAlpha = 1;
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
      const x = toScreenX(activeHoverX, currentFrame, horizontalScale);
      context.globalAlpha = 1;
      context.strokeStyle = mutedColor;
      context.setLineDash([3, 3]);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      context.setLineDash([]);

      for (const point of activeHoverPoints) {
        const emphasized = highlighted === null || highlighted === point.series.runId;
        context.globalAlpha = emphasized ? 1 : 0.2;
        drawMarker(
          context,
          toScreenX(point.x, currentFrame, horizontalScale),
          toScreenY(point.smoothed, currentFrame, verticalScale),
          point.series.color,
          point.series.pattern,
          surfaceColor,
          4,
        );
      }
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
    const markers: Point[] = [];
    const valid = xValues.map((x, index) =>
      validPoint(x, values[index], horizontalScale, verticalScale),
    );
    for (const { start, end } of contiguousBucketRanges(buckets, valid)) {
      let lastMarkerX = Number.NEGATIVE_INFINITY;
      for (let index = start; index < end; index += 1) {
        const x = toScreenX(xValues[index], activeFrame, horizontalScale);
        const y = toScreenY(values[index] as number, activeFrame, verticalScale);
        if (index === start) context.moveTo(x, y);
        else context.lineTo(x, y);
        if (!Number.isFinite(lastMarkerX) || Math.abs(x - lastMarkerX) >= 88) {
          markers.push({ x, y });
          lastMarkerX = x;
        }
      }
    }
    context.stroke();
    context.setLineDash([]);
    for (const point of markers) {
      drawMarker(context, point.x, point.y, color, pattern, "transparent", 2.5);
    }
    context.globalAlpha = 1;
  }

  function drawMarker(
    context: CanvasRenderingContext2D,
    x: number,
    y: number,
    color: string,
    pattern: SeriesPattern,
    fill: string,
    radius: number,
  ): void {
    context.save();
    context.setLineDash([]);
    context.strokeStyle = color;
    context.fillStyle = fill === "transparent" ? color : fill;
    context.lineWidth = 1.5;
    context.beginPath();
    if (pattern === "dash") {
      context.rect(x - radius, y - radius, radius * 2, radius * 2);
    } else if (pattern === "dot") {
      context.moveTo(x, y - radius - 0.5);
      context.lineTo(x + radius + 0.5, y);
      context.lineTo(x, y + radius + 0.5);
      context.lineTo(x - radius - 0.5, y);
      context.closePath();
    } else if (pattern === "dash-dot") {
      context.moveTo(x, y - radius - 0.8);
      context.lineTo(x + radius + 0.8, y + radius);
      context.lineTo(x - radius - 0.8, y + radius);
      context.closePath();
    } else {
      context.arc(x, y, radius, 0, Math.PI * 2);
    }
    context.fill();
    if (fill !== "transparent") context.stroke();
    context.restore();
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

  function queueTooltipPosition(
    target: HTMLElement,
    point: Point,
    _rowCount: number,
    _expanded: boolean,
    _revision: number,
  ): void {
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
      const width = Math.max(160, Math.min(790, viewportWidth - margin * 2));
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
    chartRevision += 1;
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
      chartRevision += 1;
      return;
    }
    if (!insidePlot(point, frame) || renderableSeries.length === 0) {
      hoverX = null;
      return;
    }
    hoverX = fromScreenX(point.x, frame);
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

  function resetTransientState(): void {
    clearTooltipTimers();
    tooltipHovered = false;
    tooltipPositioned = false;
    viewport = null;
    drag = null;
    hoverX = null;
    hoverPosition = null;
    chartRevision += 1;
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
        chartRevision += 1;
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
    chartRevision += 1;
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
    chartRevision += 1;
  }

  function changeAlignment(event: Event): void {
    const alignment = (event.currentTarget as HTMLSelectElement).value as XAlignment;
    if (alignment === xAlignment) return;
    onalignmentchange(metric, alignment);
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

  function toggleRun(runId: string): void {
    exitSolo();
    const next = new Set(hiddenRunIds);
    if (next.has(runId)) next.delete(runId);
    else next.add(runId);
    hiddenRunIds = next;
    hoverX = null;
    chartRevision += 1;
  }

  function toggleSolo(runId: string): void {
    if (soloRunId === runId) {
      exitSolo();
    } else {
      if (soloRunId === null) hiddenBeforeSolo = new Set(hiddenRunIds);
      soloRunId = runId;
      hiddenRunIds = new Set(
        series.filter((item) => item.runId !== runId).map((item) => item.runId),
      );
    }
    hoverX = null;
    chartRevision += 1;
  }

  function exitSolo(): void {
    if (soloRunId === null) return;
    hiddenRunIds = hiddenBeforeSolo ?? new Set<string>();
    hiddenBeforeSolo = null;
    soloRunId = null;
  }

  function showAllRuns(): void {
    hiddenRunIds = new Set<string>();
    hiddenBeforeSolo = null;
    soloRunId = null;
    hoverX = null;
    chartRevision += 1;
  }

  function highlightRun(runId: string | null): void {
    highlightedRunId = runId;
    chartRevision += 1;
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

  function smoothingAmountLabel(mode: SmoothingMode): string {
    if (mode === "time-ema") return "Time constant (seconds)";
    if (mode === "running") return "Window (points)";
    if (mode === "gaussian") return "Sigma (points)";
    return "Alpha";
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

  function formatNullable(value: number | null): string {
    return value === null ? "—" : formatAxis(value);
  }

  function formatRange(minimum: number | null, maximum: number | null): string {
    if (minimum === null || maximum === null) return "—";
    return `${formatAxis(minimum)}–${formatAxis(maximum)}`;
  }

  function formatTimestamp(timestamp: number): string {
    if (!Number.isFinite(timestamp)) return "—";
    return new Date(timestamp).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    });
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
  aria-label={`${title ?? metric} metric comparison panel`}
>
  <div class="chart-heading">
    <div class="chart-title">
      <strong>{title ?? metric}</strong>
      <small>{series.length} {series.length === 1 ? "run" : "runs"}</small>
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
      <details bind:this={settings} bind:open={settingsOpen} class="chart-settings">
        <summary
          bind:this={settingsSummary}
          aria-label="Chart settings"
          aria-expanded={settingsOpen}><Icon name="settings" size={14} /></summary
        >
        <div class="chart-settings-popover" role="group" aria-label="Chart display settings">
          <label>
            X alignment
            <select value={xAlignment} onchange={changeAlignment}>
              <option value="step">Absolute step</option>
              <option value="relative-step">Relative step</option>
              <option value="elapsed-time">Elapsed time</option>
            </select>
          </label>
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
            <legend>X axis · {alignmentLabel(xAlignment)}</legend>
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

  {#if series.length > 0}
    <div class="chart-legend" role="list" aria-label="Compared runs">
      {#each preparedSeries as item (item.runId)}
        {@const status = statusLabel(item)}
        <div
          class="legend-entry"
          class:hidden={hiddenRunIds.has(item.runId)}
          class:highlighted={highlightedRunId === item.runId}
          role="listitem"
          onmouseenter={() => highlightRun(item.runId)}
          onmouseleave={() => highlightRun(null)}
        >
          <button
            class="legend-toggle"
            type="button"
            aria-label={`${hiddenRunIds.has(item.runId) ? "Show" : "Hide"} ${item.runName}`}
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
          <button
            class="legend-solo"
            class:active={soloRunId === item.runId}
            type="button"
            aria-label={`${soloRunId === item.runId ? "Restore all runs after soloing" : "Solo"} ${item.runName}`}
            aria-pressed={soloRunId === item.runId}
            onclick={() => toggleSolo(item.runId)}>solo</button
          >
        </div>
      {/each}
      {#if hiddenRunIds.size > 0}
        <button class="legend-show-all" type="button" onclick={showAllRuns}>show all</button>
      {/if}
    </div>
  {/if}

  {#if renderableSeries.length > 0}
    <div class={`chart-canvas-wrap chart-mode-${interactionMode}`}>
      <canvas
        bind:this={canvas}
        tabindex="0"
        aria-label={`${metric} comparison history chart. Use plus and minus to zoom and zero to reset.`}
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
          {metric}, {hoverPoints.length} runs near x {formatAxis(hoverX ?? 0)}
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
            <span>{alignmentLabel(xAlignment)} {formatAxis(hoverX ?? 0)}</span>
          </div>
          <!-- svelte-ignore a11y_no_noninteractive_tabindex (scrollable comparison data needs keyboard focus) -->
          <div class="tooltip-table-wrap" role="region" tabindex="0" aria-label="Comparison values">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>X</th>
                  <th>Step</th>
                  <th>Raw</th>
                  <th>Smoothed</th>
                  <th>Min–max</th>
                  <th>Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {#each hoverPoints as point (point.series.runId)}
                  <tr
                    class:deemphasized={highlightedRunId && highlightedRunId !== point.series.runId}
                  >
                    <th title={point.series.runName}>
                      <span
                        class={`series-swatch pattern-${point.series.pattern}`}
                        style={`--series-color: ${point.series.color}`}
                        aria-hidden="true"
                      ></span>
                      <span>{point.series.runName}</span>
                    </th>
                    <td>{formatAxis(point.x)}</td>
                    <td>{formatAxis(point.step)}</td>
                    <td>{formatNullable(point.raw)}</td>
                    <td>{formatAxis(point.smoothed)}</td>
                    <td>{formatRange(point.minimum, point.maximum)}</td>
                    <td>{formatTimestamp(point.timestamp)}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </div>
      {/if}
    </div>
  {:else if series.length === 0}
    <div class="chart-placeholder">Select at least one run to compare.</div>
  {:else if anyLoading || !anyHistory}
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
    font-size: 9px;
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
  .legend-solo,
  .legend-show-all {
    min-width: 0;
    border: 0;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
  }

  .legend-toggle {
    display: grid;
    grid-template-columns: 28px minmax(40px, 1fr) auto;
    gap: 6px;
    align-items: center;
    padding: 5px 7px;
    text-align: left;
  }

  .legend-toggle:hover,
  .legend-solo:hover,
  .legend-solo.active,
  .legend-show-all:hover {
    background: var(--button-hover);
    color: var(--text);
  }

  .legend-name {
    overflow: hidden;
    color: var(--text);
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .legend-toggle small {
    color: var(--muted);
    font-size: 8px;
    white-space: nowrap;
  }

  .legend-toggle small.no-data {
    color: var(--faint);
  }

  .legend-solo {
    padding: 0 7px;
    border-left: 1px solid var(--line);
    font-size: 8px;
    text-transform: uppercase;
  }

  .legend-show-all {
    flex: none;
    padding: 6px 8px;
    font-size: 9px;
    white-space: nowrap;
  }

  .series-swatch {
    --series-color: var(--accent);
    position: relative;
    width: 26px;
    height: 10px;
    display: inline-block;
    flex: none;
  }

  .series-swatch::before {
    position: absolute;
    top: 4px;
    right: 0;
    left: 0;
    border-top: 2px solid var(--series-color);
    content: "";
  }

  .series-swatch.pattern-dash::before,
  .series-swatch.pattern-dash-dot::before {
    border-top-style: dashed;
  }

  .series-swatch.pattern-dot::before {
    border-top-style: dotted;
  }

  .series-swatch::after {
    position: absolute;
    top: 1px;
    left: 10px;
    width: 7px;
    height: 7px;
    border: 1px solid var(--series-color);
    background: var(--panel);
    border-radius: 50%;
    content: "";
  }

  .series-swatch.pattern-dash::after {
    border-radius: 0;
  }

  .series-swatch.pattern-dot::after {
    border-radius: 0;
    transform: rotate(45deg);
  }

  .series-swatch.pattern-dash-dot::after {
    top: 0;
    width: 0;
    height: 0;
    border-width: 0 4px 8px;
    border-color: transparent transparent var(--series-color);
    background: transparent;
    border-radius: 0;
  }

  .chart-canvas-wrap {
    min-height: 0;
    flex: 1;
  }

  .comparison-tooltip {
    position: fixed;
    z-index: 1100;
    inset: auto;
    width: min(790px, calc(100vw - 16px));
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
    gap: 14px;
    justify-content: space-between;
    padding: 8px 10px;
    border-bottom: 1px solid var(--line);
  }

  .tooltip-heading span {
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
    border-collapse: collapse;
    color: var(--muted);
    font-size: 9px;
    white-space: nowrap;
  }

  .comparison-tooltip th,
  .comparison-tooltip td {
    padding: 5px 7px;
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
    font-size: 8px;
    font-weight: 600;
    text-transform: uppercase;
  }

  .comparison-tooltip th:first-child {
    max-width: 220px;
    text-align: left;
  }

  .comparison-tooltip tbody th {
    display: flex;
    gap: 6px;
    align-items: center;
    overflow: hidden;
    color: var(--text);
    font-weight: 500;
  }

  .comparison-tooltip tbody th > span:last-child {
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .comparison-tooltip .series-swatch {
    width: 20px;
    transform: scale(0.8);
  }

  .comparison-tooltip tr.deemphasized {
    opacity: 0.34;
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
