import { describe, expect, it } from "vitest";

import type { Run, RunListItem } from "./api";
import {
  mergeCurrentRunListFields,
  retainNewestRunDetail,
  runDocumentIsCurrent,
} from "./run-snapshot";

describe("run snapshot ordering", () => {
  it("never replaces a newer cached detail with an out-of-order response", () => {
    const newest = runDetail({ document_revision: 5, metric_revision: 8, rich_data_revision: 3 });
    const stale = runDetail({ document_revision: 4, metric_revision: 7, rich_data_revision: 2 });

    expect(retainNewestRunDetail(newest, stale)).toBe(newest);
  });

  it("merges newer list-only resource state when the document remains current", () => {
    const detail = runDetail({ document_revision: 5, metric_revision: 8, rich_data_revision: 2 });
    const summary = runSummary({
      document_revision: 5,
      metric_revision: 8,
      rich_data_revision: 3,
      state: "finished",
    });

    expect(runDocumentIsCurrent(detail, summary, true)).toBe(true);
    expect(mergeCurrentRunListFields(detail, summary, true)).toMatchObject({
      state: "finished",
      rich_data_revision: 3,
      config: { seed: 42 },
    });
  });

  it("does not make a stale metric summary look current", () => {
    const detail = runDetail({ document_revision: 5, metric_revision: 7, rich_data_revision: 2 });
    const summary = runSummary({
      document_revision: 5,
      metric_revision: 8,
      rich_data_revision: 3,
    });

    expect(runDocumentIsCurrent(detail, summary, true)).toBe(false);
    expect(mergeCurrentRunListFields(detail, summary, true)).toBe(detail);
  });

  it("does not downgrade a detail that is newer than its list snapshot", () => {
    const detail = runDetail({ document_revision: 6, metric_revision: 9, rich_data_revision: 4 });
    const staleSummary = runSummary({
      document_revision: 5,
      metric_revision: 8,
      rich_data_revision: 3,
      state: "finished",
    });

    expect(runDocumentIsCurrent(detail, staleSummary, true)).toBe(true);
    expect(mergeCurrentRunListFields(detail, staleSummary, true)).toBe(detail);
  });
});

function runDetail(revisions: Partial<Run> = {}): Run {
  return {
    id: "run-1",
    project_id: "project-1",
    project: "demo",
    name: "run",
    state: "running",
    config: { seed: 42 },
    summary: { loss: 1 },
    explicit_summary: {},
    metric_summary: { loss: 1 },
    summary_truncated: false,
    document_revision: 1,
    metric_revision: 1,
    rich_data_revision: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    finished_at: null,
    ...revisions,
  };
}

function runSummary(values: Partial<RunListItem> = {}): RunListItem {
  const {
    config: _config,
    summary: _summary,
    explicit_summary: _explicit,
    metric_summary: _metric,
    ...item
  } = runDetail(values);
  return item;
}
