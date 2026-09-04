# Contributing

## Set up

Enter the pinned environment, install locked dependencies, and run the complete
validation suite:

```bash
nix develop
just bootstrap
just check
```

Run `just bootstrap` again after changing a lockfile.

## Common tasks

| Command              | Purpose                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `just dev`           | Start the API and dashboard development servers.                   |
| `just check`         | Run all required formatting, lint, test, build, and policy checks. |
| `just format`        | Format Rust, Python, and web sources.                              |
| `just single-binary` | Build the server with the dashboard embedded.                      |

`just dev` serves the API on `127.0.0.1:8787` and proxies dashboard API calls
from Vite. Ctrl-C stops both processes.

## Repository layout

| Path      | Purpose                                                  |
| --------- | -------------------------------------------------------- |
| `crates/` | Rust protocol, catalog, storage, and server crates.      |
| `python/` | Python SDK, CLI, importer, and tests.                    |
| `web/`    | Svelte dashboard.                                        |
| `docs/`   | User, operator, architecture, and release documentation. |
| `deploy/` | Service and environment templates.                       |

## Change discipline

- Keep commits focused.
- Update the canonical documentation page for user-visible changes.
- Add an ADR for changes to storage, public compatibility, resource budgets, or
  deployment topology.
- Keep runtime databases, Parquet files, journals, blobs, caches, coverage, and
  build output out of Git.

See the [documentation index](docs/index.md) for topic ownership. Maintainer
releases follow [the release process](docs/releasing.md).
