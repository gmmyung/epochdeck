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

The Python spool is a versioned lifecycle record. It stores run identity,
config, summary, batch size, finish intent, and metric events. Before an HTTP
upload, it atomically records the selected journal byte range and batch
sequence. Retries read that fixed range exactly. The acknowledgement advances
only after a successful response, then the delivery record is removed. A stale
delivery record whose range is already acknowledged is safe to discard.

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
an in-flight batch. Offline runs restore their document state and metric-derived
summary, and repeated sync of a finished spool is safe.

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
