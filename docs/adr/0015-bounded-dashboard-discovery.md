# ADR 0015: Bounded dashboard discovery and lightweight list records

- Status: Accepted
- Date: 2026-08-30

## Context

Dashboard navigation previously reused complete resource records for lists. A
project page could therefore repeat run configuration and summary documents,
artifact manifests and report layouts before the user
selected any of them. Sweep lists similarly repeated parameter definitions and
trial configurations. Metric discovery compounded that cost by draining every
selected run's complete key catalog into the browser. These costs grew with
stored metadata and metric cardinality even though the visible page was bounded.

Several imported resources use deterministic UUIDs rather than time-ordered
UUIDv7 values. Treating an identifier as a chronology cursor can consequently
skip or repeat records. Likewise, polling only scalar metric revisions leaves
already-open media, artifact, and alert tabs stale.

## Decision

List APIs return lightweight summaries and expose a separate detail endpoint
for the selected resource. Sweep summaries retain scheduler progress but omit
the parameter document; trial summaries retain lease and result state but omit
the configuration. Their existing sweep detail route and dedicated trial detail
route return the complete records. All newest-first lists use keyset order
`(created_at DESC, id DESC)` (or the resource's explicit event time plus ID).
The public cursor remains the record ID; the catalog resolves it to the complete
ordering tuple, rejects foreign or missing cursors, requests `limit + 1` rows,
and returns a continuation only when another row exists.

Projects maintain `run_count` transactionally. Rich-value ingestion maintains
one row per `(run_id, key)` containing its count and latest value reference.
These denormalized values replace list-time scans of all runs or all rich-value
rows. They are derived catalog state and are updated in the same transaction as
their source mutation.

Multi-run metric discovery uses one project-scoped endpoint. A request names at
most 32 unique project runs, chooses union or intersection semantics, and may
provide a bounded search string, lexicographic `after` cursor, and page size.
The response contains only a bounded page of keys plus the selected runs in
which each key is available. Chart history remains a separate, column-projected
request and is never used for discovery.

Each run has `document_revision`, `metric_revision`, and a unified
`rich_data_revision`. Real config, explicit-summary, and finish mutations
increment the first exactly once, including multiple changes within SQLite's
one-second timestamp resolution. Scalar ingest increments the second. Every
successful non-scalar mutation that can change an already-open dashboard tab
increments the third. Idempotent retries and document no-ops do not manufacture
revision churn. Lightweight run summaries carry all three revisions, while
complete config and summary documents are fetched only for the selected run.

Explicit user summary values and the automatic latest-metric preview are
separate catalog documents. The explicit document keeps its independent 256
KiB budget. The preview retains and updates the lexicographically smallest 256
non-system metric keys across all accepted batches. Observing another key sets
a sticky `summary_truncated` flag, but never rejects ingest or removes that key
from Parquet or metric discovery. A complete run record exposes
`explicit_summary`, `metric_summary`, the flag, and a merged `summary` in which
the explicit layer wins. Summary equality applies the same precedence.

These tables, indexes, counters, and triggers are folded directly into the one
current disposable pre-alpha catalog definition. This ADR records the current
design rather than a storage generation or upgrade path.

## Consequences

Navigation memory and response size depend on page limits rather than complete
resource documents. Opening a deep link costs one summary lookup and one detail
lookup instead of draining preceding list pages. Metric search and comparison
selection remain bounded even when a project has many runs or keys.

Catalog writes perform small additional counter, preview, and revision updates.
The preview has a constant key bound, so high metric cardinality cannot recreate
an unbounded SQLite summary or poison an otherwise valid batch. Cursor
resolution adds one indexed lookup, but ordering remains stable for arbitrary
caller-supplied IDs and concurrent inserts. A crash cannot separate derived
counts or preview state from their source rows because they live in one SQLite
transaction.

When the catalog definition changes, pre-alpha operators archive the complete
old storage root set and start the replacement build with empty roots. Physical
backup preserves the point-in-time root set but does not translate it into a
later internal layout.

## Rejected alternatives

### Drain per-run metric catalogs in the browser

Client-side union is simple, but its requests, memory, and latency grow with the
complete key set multiplied by selected runs. It also duplicates work across
tabs and browsers.

### Paginate by UUID comparison alone

UUIDv5 and caller-selected IDs do not encode insertion time. Identifier-only
ordering is deterministic but is not a valid newest-first chronology.

### Return complete records in bounded lists

A row count limit does not bound bytes when each row embeds configuration,
summary, sweep parameters, trial configurations, layouts, or manifests.
Summary/detail separation is the explicit payload bound.

### Keep one revision per rich resource type

More counters make every list record and polling path wider without improving
cache correctness. One non-scalar invalidation token is sufficient because the
selected tab still fetches its own paginated resource.

### Merge every latest metric into the user summary document

This makes a nominally valid metric batch fail once cumulative key names and
values exceed the summary-document byte limit. Raising that limit only moves an
implicit metric-cardinality quota and makes every selected-run detail heavier.
The separate bounded preview preserves useful convenience values without
coupling metric retention to a JSON document budget.
