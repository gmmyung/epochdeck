# ADR 0006: Versioned artifacts and explicit lineage

- Status: Accepted
- Date: 2026-08-28

## Context

Checkpoints and datasets need stable immutable versions, friendly aliases, and
reproducible relationships between producing and consuming runs. Inferring
lineage from filenames is ambiguous, while copying artifact bytes per version
would defeat content deduplication. Version allocation and alias moves must be
safe under concurrent and replayed requests.

## Decision

Artifact manifests are project-scoped immutable versions in SQLite. A
collection name has one stable type. Creation allocates the next integer version,
stores the bounded ordered entry manifest, moves requested aliases, and inserts
the producing run's output edge in one transaction. A client-assigned UUIDv7 and
canonical request document make exact response-loss retries idempotent.

Entries reference the shared SHA-256 blob store. The server verifies existence
and byte size before opening the catalog transaction. Artifact file delivery
resolves an immutable manifest path and delegates range streaming to the blob
file service.

Input use is a separate idempotent lineage operation. Lineage rows identify the
artifact version, run, and input/output relation; no edge is inferred from run
configuration or artifact names. Alias resolution always returns the currently
targeted immutable version.

## Consequences

Creating a new version or moving `latest` never mutates prior manifests.
Identical files across versions occupy one blob. Runs can display both sides of
their lineage, and scripts can pin an artifact UUID/version or deliberately use
a movable alias.

Manifest entry count and JSON size are bounded independently from blob size.
The initial implementation supports 4,096 paths and a 2 MiB manifest per
version; large directory trees should be packed or split into collections.

## Rejected alternatives

### Put aliases directly on versions

Aliases move. Storing only a historical alias list cannot resolve the current
target atomically and makes concurrent `latest` updates ambiguous.

### Derive versions in the SDK

Multiple writers can race. The catalog transaction is the only authority that
can allocate a unique next version.

### Infer lineage from downloads

Downloading a file does not prove that a run consumed it. Explicit `use`
operations make lineage intentional, replay-safe, and auditable.
