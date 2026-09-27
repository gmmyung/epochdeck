# Production readiness

EpochDeck 0.1.0-alpha.2 is suitable for controlled single-user evaluation with
disposable or independently backed-up experiment data. It is not a general
multi-user production service or a stable archival system.

## Evidence

The repository checks include Rust formatting, Clippy and tests; Python linting,
typing and tests; dashboard formatting, typing and tests; dependency policy,
release metadata and license checks; and a real SDK/server contract test.

The contract test logs metrics, media, an artifact and an alert, kills the server
after acknowledged delivery, verifies the restarted history, and restores a
physical backup into separate roots. Focused tests cover idempotent replay,
concurrent admission, cancellation, exact projected chart queries, compaction,
exclusive spool ownership, and interrupted journal tails.

Alpha 2 fixes overlapping SDK spool writers, blocked resume after an incomplete
append, two dashboard scheduling edge cases, and stale branding expectations in
the release smoke test. Chart caches, Rust test modules, the catalog SQL
definition, and SDK delivery scheduling now have separate source files.

## Remaining release gates

| Area | Current limitation | Gate for broader production use |
| ---- | ------------------ | ------------------------------- |
| Security | No native authentication, scoped credentials, ownership, or multi-user authorization | Define and test the authorization boundary; retain authenticated HTTPS proxy protection meanwhile |
| Data lifecycle | Pre-alpha stored data and APIs can change without migrations | Establish stable retention, migration, upgrade and rollback contracts |
| Capacity | Single-machine storage benchmark; no sustained concurrent-load acceptance gate | Measure ingestion/query p95/p99 latency and RSS under representative concurrency on the deployment target |
| Recovery | Focused failure tests and one real process-kill/restore contract | Exercise disk-full, repeated crashes during publication/compaction, and longer recovery/restore drills |
| Client disk usage | Journals and copied media remain local for the lifetime of active runs | Monitor disk usage and define safe reclamation/backpressure behavior for long runs |
| Compatibility | Several public Python, query, table, and import behaviors remain partial | Expand boundary contracts for supported workflows; preserve explicit exclusions |

The JavaScript dependency audit reported no known vulnerabilities after
updating Vitest to 4.1.11. This does not establish that every dependency is free
of vulnerabilities.

The assessment is based on source review and local checks, not a penetration
test, comprehensive cross-ecosystem vulnerability audit, multi-day soak, or independent validation
of every supported operating system. Native prerelease jobs validate their own
builds and packaged binaries.

Use the [security policy](../SECURITY.md), [operations guide](operations.md),
[benchmark scope](benchmarks.md), and [compatibility matrix](compatibility.md)
when deciding whether a specific workload fits these limits.
