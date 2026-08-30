# ADR 0008: Durable bounded sweep scheduling

- Status: Accepted
- Date: 2026-08-28

## Context

Sweep agents need to coordinate concurrent workers, avoid eagerly expanding
large grids, recover abandoned claims, bind scheduled configurations to runs,
and communicate early termination without a second unbounded event system.

## Decision

Sweep definitions and trials are transactional SQLite records. Each sweep owns a
monotonic `next_index`. Grid scheduling interprets that index as mixed-radix
digits over sorted parameter names; random scheduling hashes the sweep ID,
index, and parameter name. Both select directly from finite typed value lists
and use constant memory per claim. A definition accepts at most 64 parameters,
256 values per parameter, and 256 KiB of parameter JSON. The SDK enforces those
bounds while iterating, rejects unknown definition fields, and normalizes every
value through the shared depth, node, and JSON-safe-integer contract before the
request is materialized.

An agent claim has a renewable 60-second lease before and after it binds a run.
Run creation verifies project ownership and atomically changes the trial from
claimed to running. The owning agent heartbeats while training. Expired claimed
or running trials are reassigned with their original configuration and any bound
run ID, allowing the replacement agent to resume the same Runloom run. Heartbeat
and terminal updates include the agent ID and reject stale owners. Terminal
completion accepts only completed, failed, or stopped and is idempotent for the
current owner.

Optional early termination uses a median rule after a configured step and peer
count. Metric ingestion updates the linked trial and returns a stop bit in the
existing idempotent batch acknowledgement. The Python delivery worker exposes
that bit as `run.should_stop` and raises `SweepEarlyStop` on the next log call.

## Consequences

Multiple agent processes can share a scheduler without an external queue. A
search space can be combinatorially large without consuming proportional memory
or catalog rows. Random schedules are reproducible for a definition and index.
Stop delivery follows the same retry semantics as metrics and adds no polling
load. Heartbeats add one bounded request per active agent lease interval; the
Python agent persists its current trial so process restarts do not rely solely on
lease expiry.

The current search-space contract deliberately supports finite `values` only.
Continuous distributions and Hyperband need explicit sampling and rung
semantics; accepting their fields without implementing those semantics would be
misleading, so the SDK rejects them.

## Rejected alternatives

### Materialize every configuration

Grid size grows multiplicatively and would turn definition creation into an
unbounded memory and transaction operation.

### Keep scheduling in each agent

Independent agents race and duplicate work without one transactional allocation
authority.

### Add a separate stop polling loop

It adds request load and lifecycle complexity. Batch acknowledgements already
form a durable ordered path from the server to active training code.
