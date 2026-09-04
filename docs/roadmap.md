# Roadmap

The [compatibility matrix](compatibility.md) records what works today. This page
lists remaining priorities; it does not duplicate completed feature history.

## Release foundation

- Publish and smoke-test the first public prerelease.
- Make installation and upgrades predictable on every supported platform.
- Add representative concurrent-ingestion and dashboard performance gates.

## Security and collaboration

- Add native authentication and scoped API credentials.
- Add multi-user authorization and project ownership.
- Define safe public-network and multi-tenant deployment profiles.

## Compatibility depth

- Expand project, run, filter, and file APIs.
- Complete table mutation and media-sequence semantics.
- Add groups, jobs, tags, notes, and ownership metadata.
- Expand sweeps beyond finite value sets and median stopping.
- Broaden W&B import coverage without weakening bounded-memory behavior.

## Stable release gates

- Define the public compatibility and deprecation policy.
- Introduce stored-data migrations only when stable retention requires them.
- Establish long-running upgrade, rollback, and recovery test suites.

Prior decisions live in the [ADRs](adr/), and shipped changes live in the
[changelog](../CHANGELOG.md).
