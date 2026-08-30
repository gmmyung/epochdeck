import type { Run, RunListItem } from "./api";

type RunRevisions = Pick<
  RunListItem,
  "document_revision" | "metric_revision" | "rich_data_revision"
>;

export function runRevisionsAtLeast(candidate: RunRevisions, existing: RunRevisions): boolean {
  return (
    candidate.document_revision >= existing.document_revision &&
    candidate.metric_revision >= existing.metric_revision &&
    candidate.rich_data_revision >= existing.rich_data_revision
  );
}

export function runDocumentIsCurrent(
  detail: Run,
  summary: RunListItem | undefined,
  includeMetricSummary: boolean,
): boolean {
  return (
    summary === undefined ||
    (detail.document_revision >= summary.document_revision &&
      (!includeMetricSummary || detail.metric_revision >= summary.metric_revision))
  );
}

export function retainNewestRunDetail(existing: Run | undefined, candidate: Run): Run {
  return existing && !runRevisionsAtLeast(candidate, existing) ? existing : candidate;
}

/** Overlay list-only fields only when the detail document satisfies its revision contract. */
export function mergeCurrentRunListFields(
  detail: Run,
  summary: RunListItem | undefined,
  includeMetricSummary: boolean,
): Run {
  return runDocumentIsCurrent(detail, summary, includeMetricSummary) &&
    summary &&
    runRevisionsAtLeast(summary, detail)
    ? { ...detail, ...summary }
    : detail;
}
