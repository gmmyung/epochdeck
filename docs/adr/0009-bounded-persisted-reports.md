# ADR 0009: Bounded persisted reports

- Status: Accepted
- Date: 2026-08-28

## Context

Users need stable multi-run dashboards, but persisting chart data or issuing an
eager request for every panel would duplicate lossless histories and make report
load cost proportional to raw experiment length. Report definitions also need
referential integrity when they compare runs.

## Decision

Store reports as project-scoped SQLite records containing a typed, bounded JSON
layout. A layout has one to four columns and at most 32 metric or Markdown
panels. Metric panels reference one run in the same project and at most eight
metric keys. Markdown and total layout bytes have independent limits.

Creation validates all run references in the catalog transaction. Updates
replace the current pre-alpha definition directly. Reports never store history,
downsampled results, or blob content.

The dashboard turns visible metric panels into ordinary exact-bucket chart
history calls. It cancels requests when selection changes and admits at most
four concurrently. Markdown is parsed into a small safe block model and
rendered as Svelte text nodes rather than injected HTML.

## Consequences

Report storage remains small and backup-friendly. Metric source data has one
owner, and dashboard memory and response sizes stay independent of run length.
Deleting a report cannot delete experiments. The initial report language is
intentionally narrower than W&B's rich report document model.

## Rejected alternatives

### Persist sampled chart data

Samples become stale, duplicate metric state, and weaken the lossless source of
truth without making the raw history cheaper to retain.

### Eagerly fetch every panel

The bounded panel count would still allow 256 simultaneous metric queries and
would make opening a report contend with ingestion.

### Render arbitrary Markdown HTML

HTML injection expands the security surface. The current safe block renderer is
enough for report notes while richer semantics remain an explicit future task.
