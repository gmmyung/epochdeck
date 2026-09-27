# ADR 0018: Exclusive ownership and bounded journal-tail recovery

## Status

Accepted for 0.1.0-alpha.2.

## Context

Thread locks inside independent SDK instances do not prevent two processes from
resuming the same local spool. They can allocate the same sequence and overwrite
delivery state. A process exit during an append can also leave a final record
without its newline; rejecting that tail prevents replay of earlier valid data.

## Decision

- Acquire a nonblocking OS lock on `spool.lock` before reading or mutating the
  spool. Keep the lock through active delivery, including cancellation after a
  caller's timeout. Process exit releases the lock automatically.
- Add `Run.close()` to stop local activity without finalizing the remote run.
  Successful finish and initialization failure also release ownership once
  workers have stopped. Closed handles reject mutations.
- At open, inspect each journal's final record using a bounded read. Truncate
  only an incomplete final append that lies beyond acknowledgement, in-flight
  delivery, and metric-summary checkpoint boundaries. Sync the truncation and
  emit a recovery warning. Oversized tails and completed corrupt records fail
  explicitly instead of being silently rewritten.
- Keep delivery scheduling in its own SDK module and retain the existing
  record and request budgets. This introduces no stored-data generations or
  migration scaffolding.

## Validation

Public SDK tests cover conflicting processes, recovery after abrupt process
exit, close/resume, all four journal tails, and preservation of committed or
corrupt bytes. A cancellation test holds a request open and verifies that spool
ownership is unavailable until delivery settles. The real-server contract test
covers scalar/media/artifact delivery, restart, and physical backup/restore.

## Consequences

Distributed jobs must use separate run IDs/spools or one designated logging
process. The lock is advisory and requires a filesystem with working native
file-lock semantics. Records returned successfully by `log` remain fsynced;
an interrupted call's incomplete final append is explicitly reported and removed.
