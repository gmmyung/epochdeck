# ADR 0004: System telemetry and durable alerts

- Status: Accepted
- Date: 2026-08-28

## Context

Monitoring data must remain useful during network interruption without adding
latency to training. Host telemetry has different step and summary semantics
from user metrics, while alerts are sparse records with text and severity that
do not belong in Parquet scalar columns. Neither path may introduce an
unbounded queue or depend on a hosted notification service.

## Decision

The Python SDK samples host telemetry on one daemon thread at a configurable,
positive interval. Collection begins after the first user metric, associates
each sample with the most recently completed user step, and writes ordinary
scalar points under the reserved `system/` prefix. This keeps the existing
columnar query and chart path while ensuring telemetry never advances automatic
steps. System keys are excluded from automatic run summaries. NVIDIA data is
queried only when `nvidia-smi` is present, with a two-second timeout and a
sixteen-device result bound.

Alerts use a separate append-only, fsynced SDK journal and acknowledgement
cursor. Each record receives a time-sortable UUIDv7 before delivery. The
background worker alternates metric batches and alerts when both are pending,
so sustained metric ingestion cannot starve alerts. The server stores bounded
alert metadata in SQLite, accepts exact retries idempotently, and exposes a
bounded UUID cursor list for the dashboard.

## Consequences

Telemetry uses the same lossless storage and bounded history queries as scalar
metrics without contaminating user summaries. It adds one lightweight sampling
thread per active run and an optional bounded subprocess call. Alerts survive
offline operation and lost responses, but Runloom does not yet send email,
webhook, or mobile notifications.

System metrics are absolute host observations rather than a separate resource
schema. Rich device-specific telemetry can later add namespaced scalar keys
without a catalog migration.

## Rejected alternatives

### Store telemetry in SQLite

Long-running samples would make the control-plane database a metric store and
recreate the dashboard performance problem Runloom's columnar data plane is
designed to avoid.

### Encode alerts as scalar metrics

Titles and text are sparse structured data. Encoding them in metric histories
would either lose semantics or introduce string columns into every scalar query.

### Use one mixed journal acknowledgement

A mixed record stream complicates metric batch idempotency and lets large metric
backlogs delay alerts. Separate cursors keep recovery explicit and allow fair
delivery.
