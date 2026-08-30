# Contributing

Enter the pinned environment before running repository commands:

```bash
nix develop
```

Then bootstrap and validate:

```bash
just bootstrap
just check
```

Keep commits focused. Changes to storage formats, public compatibility,
resource budgets, or deployment topology require an architecture decision
record under `docs/adr/`.

Generated databases, Parquet files, Arrow journals, blobs, coverage reports,
package caches, and build artifacts do not belong in Git.

Release candidates follow the gated process in [docs/releasing.md](docs/releasing.md).
