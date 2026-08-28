# ADR 0007: Indexed traces and content-addressed payloads

- Status: Accepted
- Date: 2026-08-28

## Context

LLM and agent traces need parent/child structure, timing, attributes, messages,
inputs, outputs, and text search. Complete prompts, responses, and tool payloads
can be large. Putting them directly in SQLite would make catalog backups,
working sets, and list queries grow with payload volume. Storing only opaque
files would make run navigation and search unusable.

## Decision

SQLite stores one bounded row per span: stable UUIDv7 ID, run, trace and parent
IDs, name, kind, status, timing, step, attributes, bounded previews, and an
optional blob reference. An FTS5 index covers trace identity, name, attributes,
and at most 16 KiB of serialized preview text.

Complete JSON inputs, outputs, and messages are serialized once into the shared
SHA-256 content-addressed blob store. The Python SDK installs that payload in
its durable spool, appends the span to a separate fsynced journal, and uploads
the blob before creating the catalog row. A stable client-assigned span ID makes
the operation replay-safe after a lost response.

Search accepts a bounded token query and returns the same bounded newest-first
span records as ordinary listing. Payload retrieval remains an explicit blob
request and does not enter list or search response memory.

## Consequences

Catalog scans, dashboard polling, and backups of transactional metadata remain
small relative to trace payload volume. Trace payloads can live on HDD/ZFS while
SQLite remains on SSD. Search covers intentionally bounded previews rather than
arbitrary full payloads; callers should place useful model, tool, and message
context in attributes or previews.

The first implementation stores trace metadata in SQLite rather than separate
columnar trace segments. This matches the expected span record shape and keeps
tree and FTS queries simple. A later storage change can be justified by measured
catalog volume without changing the `/api/v1` resource contract.

## Rejected alternatives

### Store complete trace documents in SQLite

Large prompts and outputs would inflate the transactional working set and make
metadata backup cost depend on payload size.

### Search complete blobs on demand

Opening and decoding every payload makes latency proportional to total trace
volume and cannot meet a bounded interactive query contract.

### Reuse scalar metric history

Trace trees and documents are not numeric time series. Encoding them as metric
keys would damage both query paths and complicate retention-free storage.
