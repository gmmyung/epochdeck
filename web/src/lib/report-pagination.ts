import type { ReportPanel } from "./api";

export const MAX_REPORT_PANELS_PER_PAGE = 12;
export const MAX_REPORT_CHARTS_PER_PAGE = 24;

export type ReportPanelPage = {
  panels: ReportPanel[];
  chartCount: number;
};

export function paginateReportPanels(panels: readonly ReportPanel[]): ReportPanelPage[] {
  const pages: ReportPanelPage[] = [];
  let currentPanels: ReportPanel[] = [];
  let currentChartCount = 0;

  for (const panel of panels) {
    const panelChartCount = panel.kind === "metric" ? panel.metric_keys.length : 0;
    if (panelChartCount > MAX_REPORT_CHARTS_PER_PAGE) {
      throw new RangeError(
        `report panel ${panel.id} has ${panelChartCount} charts; the per-panel limit is ${MAX_REPORT_CHARTS_PER_PAGE}`,
      );
    }

    const pageIsFull =
      currentPanels.length >= MAX_REPORT_PANELS_PER_PAGE ||
      currentChartCount + panelChartCount > MAX_REPORT_CHARTS_PER_PAGE;
    if (currentPanels.length > 0 && pageIsFull) {
      pages.push({ panels: currentPanels, chartCount: currentChartCount });
      currentPanels = [];
      currentChartCount = 0;
    }

    currentPanels.push(panel);
    currentChartCount += panelChartCount;
  }

  if (currentPanels.length > 0 || pages.length === 0) {
    pages.push({ panels: currentPanels, chartCount: currentChartCount });
  }
  return pages;
}
