# ADR 0003: Durable and idempotent run lifecycle

- Status: Accepted
- Date: 2026-08-28

## Context

Background metric delivery crosses three failure boundaries: the local process
can stop after journaling, an ingest response can disappear after the server
commits a batch, and a finish response can disappear after the run becomes
immutable. Resume must also continue from the last server-accepted sequence and
step without loading a complete run history.

Using only a journal acknowledgement is insufficient. If new points are
appended while an earlier request is in flight, reconstructing a retry from the
current acknowledgement can change the original batch contents. Likewise,
guessing a remote resume position can overwrite or conflict with existing
history.

## Decision

The Python spool is a versioned lifecycle record. Format version 2 stores run
identity, config, the explicit summary, the bounded metric-derived summary
preview and truncation flag, batch size, finish intent, and metric events. The
summary snapshot carries a `summary_event_offset` that must be an exact journal
record boundary. The SDK checkpoints the current preview and offset every 128
metric records or 512 KiB of journal growth, and whenever explicit summary or
finish state changes. Recovery never uses the delivery acknowledgement as a
summary cursor: it validates the snapshot boundary and scans at most the
bounded crash tail after that offset. Missing, malformed, unbounded, or
non-record-boundary offsets fail visibly; there is no older spool fallback.

Before a metric HTTP upload, the spool atomically records the selected journal
byte range, first sequence, canonical request-body size, and SHA-256 digest.
Selection is bounded by both the configured point count and an exact 1,750,000
byte canonical-JSON body budget below the server's 2 MiB limit. Retries read the
fixed range and must reproduce the stored body identity. The acknowledgement
advances only after a successful response, then the delivery record is removed.
A stale delivery record whose range is already acknowledged is safe to discard.

Metric points are validated before the fsynced append against the HTTP
contract: 1 to 256 values, UTF-8 keys of 1 to 256 bytes without Unicode control
characters, and finite numeric values. The automatic summary is not a retention
path. It deterministically keeps the lexicographically smallest 256 distinct
non-system metric keys and a sticky truncation signal, while metric history and
key discovery remain lossless. Explicit summary values have their own 256 KiB
budget, override the preview in the merged read view, and are the only summary
values sent by the SDK's finish request.

Run creation responses contain authoritative `next_sequence` and `next_step`
fields. On resume, the server projects sequence and step from the final row of
the latest segment's final Parquet row group under the metric snapshot barrier
and the bounded query-worker semaphore. This recovery query cannot grow with
total run history. The SDK combines that server cursor with any durable local
tail and rejects responses that omit or invalidate the cursor.

Finish is idempotent. Repeating it with matching summary values returns the
existing finished run; attempting to mutate a finished summary returns a
conflict. The SDK persists finish intent before flushing or sending the finish
request. After a restart, a conflict followed by a matching finished run proves
that a previously lost response committed successfully.

## Consequences

Metric delivery is at least once at the transport boundary and exactly once in
logical storage for identical retries. Appending new local points cannot alter
an in-flight batch. Offline runs restore explicit summary state and a bounded
metric-derived preview without scanning the complete journal, and repeated sync
of a finished spool is safe.

The spool format is now an explicit compatibility boundary. Unknown versions,
identity mismatches, corrupt delivery ranges, and missing server resume cursors
fail visibly. The server performs one bounded Parquet-tail read on remote
resume; ordinary creation and ingestion do not pay that cost.

## Rejected alternatives

### Rebuild every retry from the current acknowledgement

Concurrent appends can enlarge the request after a response is lost. Reusing
the same batch sequence with different contents correctly conflicts on the
server and leaves the run unable to progress.

### Derive remote progress from the local spool alone

The server may have committed a request whose acknowledgement was never stored,
or another process may have advanced the same run. Only server state is
authoritative for the remote cursor.

### Scan the complete metric history on resume

Total history is unbounded. The latest immutable segment's final row contains
the required tail position and can be selected directly.
