<script lang="ts">
  import { Dialog } from "bits-ui";
  import { onMount } from "svelte";

  import { readChartPreferences, rememberChartPreferences } from "./chart-preferences";
  import { type ScaleMode, type SmoothingMode } from "./chart-data";
  import { formatDurationMs } from "./resource-state";
  import {
    closestSeriesPoints,
    hasDrawableSeriesInRange,
    metricChartViewportKey,
    prepareMetricSeries,
    runSetIdentity,
    type MetricChartViewport,
    type MetricChartSeries,
    type PreparedMetricSeries,
    type SeriesHoverPoint,
    type XAlignment,
  } from "./chart-series";
  import Icon from "./Icon.svelte";
  import {
    boundedDomain,
    clampDomainToBounds,
    clampPannedDomainToBounds,
    configuredChartViewport as configuredDomain,
    fromScreenX,
    fromScreenY,
    insidePlot,
    toScreenX,
    toScreenY,
    transformScale as transformed,
    validateChartAxes as validateAxes,
    viewportFromSelection,
    zoomHorizontalViewport,
    type Domain,
    type Drag,
    type Frame,
    type Point,
    type Viewport,
  } from "./metric-chart-viewport";
  import MetricChartSettings from "./MetricChartSettings.svelte";
  import { boundedCanvasPixelRatio, UPlotRenderer } from "./uplot-renderer";

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
  let plotTarget: HTMLDivElement;
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
  let pendingWheel: { point: Point; exponent: number } | null = null;
  let wheelFrame: number | null = null;
  let viewportRequestTimer: number | null = null;
  let tooltip: HTMLElement | null = null;
  let tooltipPositionFrame: number | null = null;
  let tooltipHideTimer: number | null = null;
  let tooltipFocused = false;
  let tooltipPositioned = false;
  let preferenceIdentity = "";
  let synchronizedParentViewportKey = "";
  let navigationDomainKey = "";
  let navigationX: Domain | null = null;
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
  let restoreFocusElement: HTMLElement | null = null;
  const plotRenderer = new UPlotRenderer();

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
  $: synchronizeNavigationDomain(
    JSON.stringify([identity, seriesIdentity, activeAlignment, xScale, xMinimum, xMaximum]),
    parentViewport,
    configuredViewport,
    series.some((item) => item.loading),
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
    return () => {
      visible = false;
      onvisibilitychange(metric, false);
      observer.disconnect();
      resizeObserver.disconnect();
      theme.removeEventListener("change", redraw);
      window.removeEventListener("resize", repositionTooltip);
      window.removeEventListener("scroll", repositionTooltip, true);
      if (pointerFrame !== null) window.cancelAnimationFrame(pointerFrame);
      clearPendingWheel();
      clearTooltipTimers();
      clearViewportRequest();
      plotRenderer.destroy();
    };
  });

  $: if (plotTarget && visible && renderableSeries.length > 0 && layoutRevision >= 0) {
    const currentViewport = viewport ?? configuredViewport;
    frame = currentViewport
      ? plotRenderer.render(plotTarget, {
          candidates: renderableSeries,
          displayMode,
          xScale,
          yScale,
          viewport: currentViewport,
          highlightedRunId: activeHighlightedRunId,
          formatX: formatHorizontalAxis,
          formatY: formatAxis,
        })
      : null;
  }

  $: if (canvas && visible && frame && renderableSeries.length > 0 && overlayRevision >= 0) {
    drawInteraction(canvas, frame, xScale, yScale, hoverX, drag);
  }

  function redrawAll(): void {
    layoutRevision += 1;
    overlayRevision += 1;
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
    const ratio = boundedCanvasPixelRatio(width, height);
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
      context.globalAlpha = 0.72;
      context.strokeStyle = accentColor;
      context.lineWidth = 1.25;
      context.setLineDash([4, 3]);
      context.beginPath();
      context.moveTo(x, padding.top);
      context.lineTo(x, height - padding.bottom);
      context.stroke();
      context.setLineDash([]);
    }
    context.globalAlpha = 1;
    context.restore();
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
      const width = Math.max(220, Math.min(320, viewportWidth - margin * 2));
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
      if (tooltipFocused || tooltip?.contains(document.activeElement)) return;
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

  function tooltipFocusIn(): void {
    tooltipFocused = true;
    cancelTooltipHide();
  }

  function tooltipFocusOut(): void {
    tooltipFocused = false;
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
    if (viewport !== null) scheduleViewportRequest(viewport);
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
    const shiftedX = boundedDomain(xMinimum + xShift, xMaximum + xShift, xScale);
    const nextViewport = {
      x: navigationX ? clampPannedDomainToBounds(shiftedX, navigationX, xScale) : shiftedX,
      y: boundedDomain(yMinimum + yShift, yMaximum + yShift, yScale),
    };
    if (domainsEqual(nextViewport.x, frame.x) && domainsEqual(nextViewport.y, frame.y)) return;
    viewport = nextViewport;
  }

  function wheel(event: WheelEvent): void {
    const point = canvasPoint(event);
    if (!frame || !insidePlot(point, frame)) return;
    event.preventDefault();
    const exponent = Math.max(-1, Math.min(1, event.deltaY / 240));
    pendingWheel = {
      point,
      exponent: (pendingWheel?.exponent ?? 0) + exponent,
    };
    if (wheelFrame !== null) return;
    wheelFrame = window.requestAnimationFrame(() => {
      wheelFrame = null;
      const pending = pendingWheel;
      pendingWheel = null;
      if (pending) zoomAt(pending.point, Math.exp(pending.exponent));
    });
  }

  function zoomAt(point: Point, factor: number): void {
    if (!frame) return;
    let nextViewport = zoomHorizontalViewport(frame, point.x, xScale, factor);
    if (
      factor < 1 &&
      !hasDrawableSeriesInRange(
        renderableSeries,
        nextViewport.x.minimum,
        nextViewport.x.maximum,
        yScale,
      )
    ) {
      let invalidFactor = factor;
      let validFactor = 1;
      nextViewport = { x: frame.x, y: frame.y };
      for (let attempt = 0; attempt < 10; attempt += 1) {
        const candidateFactor = (invalidFactor + validFactor) / 2;
        const candidate = zoomHorizontalViewport(frame, point.x, xScale, candidateFactor);
        if (
          hasDrawableSeriesInRange(
            renderableSeries,
            candidate.x.minimum,
            candidate.x.maximum,
            yScale,
          )
        ) {
          nextViewport = candidate;
          validFactor = candidateFactor;
        } else {
          invalidFactor = candidateFactor;
        }
      }
    }
    if (factor > 1 && navigationX) {
      nextViewport = {
        x: clampDomainToBounds(nextViewport.x, navigationX),
        y: nextViewport.y,
      };
    }
    if (domainsEqual(nextViewport.x, frame.x)) return;
    viewport = nextViewport;
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
    clearPendingWheel();
    clearTooltipTimers();
    tooltipFocused = false;
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

  function synchronizeNavigationDomain(
    key: string,
    parent: MetricChartViewport | null,
    configured: Viewport | null,
    loading: boolean,
  ): void {
    if (key !== navigationDomainKey) {
      navigationDomainKey = key;
      navigationX = null;
    }
    if (parent !== null || configured === null || loading) return;
    navigationX = navigationX
      ? {
          minimum: Math.min(navigationX.minimum, configured.x.minimum),
          maximum: Math.max(navigationX.maximum, configured.x.maximum),
        }
      : { ...configured.x };
  }

  function domainsEqual(left: Domain, right: Domain): boolean {
    return left.minimum === right.minimum && left.maximum === right.maximum;
  }

  function resetPointerState(): void {
    clearPendingWheel();
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

  function clearPendingWheel(): void {
    pendingWheel = null;
    if (wheelFrame === null) return;
    window.cancelAnimationFrame(wheelFrame);
    wheelFrame = null;
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

  function setExpanded(nextExpanded: boolean): void {
    if (nextExpanded === expanded) return;
    settingsOpen = false;
    if (nextExpanded) {
      restoreFocusElement =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
    }
    expanded = nextExpanded;
    window.requestAnimationFrame(() => {
      redrawAll();
      if (!expanded) {
        const target = restoreFocusElement;
        restoreFocusElement = null;
        if (target?.isConnected) target.focus();
      }
    });
  }

  function toggleExpanded(): void {
    setExpanded(!expanded);
  }

  function focusExpandButton(event: Event): void {
    event.preventDefault();
    expandButton?.focus();
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

<Dialog.Root open={expanded} onOpenChange={setExpanded}>
  {#if expanded}
    <Dialog.Portal>
      <Dialog.Overlay class="chart-modal-backdrop" />
    </Dialog.Portal>
  {/if}
  <Dialog.Content
    forceMount
    onOpenAutoFocus={focusExpandButton}
    onCloseAutoFocus={(event) => event.preventDefault()}
  >
    {#snippet child({ props, open })}
      <article
        {...open ? props : {}}
        bind:this={card}
        class="metric-chart-card comparison-chart"
        class:expanded
        class:tooltip-open={hoverPoints.length > 0 && hoverPosition !== null}
        aria-keyshortcuts={expanded ? "Escape" : undefined}
        aria-label={`${title ?? metric} metric comparison panel`}
      >
        <div class="chart-heading">
          <div class="chart-title">
            <Dialog.Title level={3}>
              {#snippet child({ props: titleProps })}
                <strong {...titleProps}>{title ?? metric}</strong>
              {/snippet}
            </Dialog.Title>
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
              onclick={toggleExpanded}
              ><Icon name={expanded ? "minimize" : "expand"} size={14} /></button
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
            <div bind:this={plotTarget} class="chart-uplot" aria-hidden="true"></div>
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
                onfocusin={tooltipFocusIn}
                onfocusout={tooltipFocusOut}
              >
                <div class="tooltip-heading">
                  <strong>{metric}</strong>
                  <span>{alignmentLabel(xAlignment)} {formatHorizontalAxis(hoverX ?? 0)}</span>
                </div>
                <!-- svelte-ignore a11y_no_noninteractive_tabindex (scrollable comparison data needs keyboard focus) -->
                <div
                  class="tooltip-table-wrap"
                  role="region"
                  tabindex="0"
                  aria-label="Comparison values"
                >
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
    {/snippet}
  </Dialog.Content>
</Dialog.Root>

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

  .chart-actions {
    z-index: 1;
    opacity: 0;
    pointer-events: none;
    transform: translateY(-2px);
    transition:
      opacity 120ms ease,
      transform 120ms ease;
  }

  .comparison-chart:hover .chart-actions,
  .comparison-chart:focus-within .chart-actions,
  .comparison-chart.expanded .chart-actions,
  .comparison-chart:has(:global(.chart-settings[open])) .chart-actions {
    opacity: 1;
    pointer-events: auto;
    transform: translateY(0);
  }

  @media (hover: none), (pointer: coarse) {
    .chart-actions {
      opacity: 1;
      pointer-events: auto;
      transform: none;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    .chart-actions {
      transition: none;
    }
  }

  .chart-legend {
    min-height: 32px;
    display: flex;
    gap: 4px;
    align-items: center;
    padding: 2px 0 8px;
    overflow-x: auto;
    scrollbar-width: thin;
  }

  .legend-entry {
    min-width: 0;
    display: flex;
    flex: 0 1 250px;
    align-items: stretch;
    background: transparent;
  }

  .legend-entry:hover,
  .legend-entry.highlighted {
    background: var(--button-hover);
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

  .chart-uplot {
    width: 100%;
    height: 260px;
    overflow: hidden;
    background: transparent;
  }

  .expanded .chart-uplot {
    height: 100%;
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
    width: min(320px, calc(100vw - 16px));
    max-width: calc(100vw - 16px);
    max-height: calc(100dvh - 16px);
    display: block;
    overflow: hidden;
    padding: 0;
    margin: 0;
    pointer-events: none;
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
    width: 76px;
  }

  .comparison-tooltip th:nth-child(3),
  .comparison-tooltip td:nth-child(3) {
    width: 64px;
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
