# EpochDeck

EpochDeck is a standalone, self-hosted experiment tracker for long-running,
high-dimensional workloads. Its Python SDK provides a W&B-compatible API, while
its Rust server owns ingestion, storage, querying, and the Svelte dashboard.
Trackio feature parity is the immediate milestone; practical W&B compatibility
is the longer-term contract.

EpochDeck is pre-alpha. Scalar metrics, host telemetry, alerts, rich media,
versioned artifacts, traces, finite sweeps, persisted reports, and streaming
W&B import/export are usable end to end. Authentication, multi-user
authorization, and the wider compatibility surface are still in development.
For remote access, place the server behind an authenticated HTTPS reverse proxy
and review [SECURITY.md](SECURITY.md).

## Design

- Dashboard sampling is bounded and never deletes raw metric history.
- Ingestion, queries, caches, queues, and responses have explicit limits and
  backpressure.
- Numeric histories use Arrow/Parquet; SQLite holds catalog and transactional
  metadata; rich data uses content-addressed storage.
- Metrics, metadata, and blobs have independently configurable storage roots.
- Compatibility is tested at the public Python and HTTP boundaries.
- EpochDeck has no Hugging Face, Hub, Spaces, Buckets, Datasets, or Gradio
  runtime dependency.

```text
Python SDK and importers
  durable local spool -> batched HTTP REST (/api/v1)
                              |
                              v
                         Rust server
                    |-- SQLite catalog
                    |-- Arrow/Parquet metrics
                    `-- content-addressed rich data
                              |
Browser dashboard <- bounded query endpoints
```

The Python SDK talks to `/api/v1` over HTTP REST. Logging first writes to a
durable local spool; a bounded background worker batches uploads, retries them
idempotently, and advances acknowledgements only after the server commits a
batch. Training therefore does not wait for each network request, and a restart
can replay unacknowledged work.

## Documentation

- [System architecture](docs/architecture.md)
- [Compatibility matrix](docs/compatibility.md)
- [HTTP API](docs/api.md)
- [Deployment](docs/deployment.md) and [operations](docs/operations.md)
- [Performance benchmarks](docs/benchmarks.md)
- [Export format](docs/export-format.md)
- [Roadmap](docs/roadmap.md)

## Install a prerelease

GitHub prereleases include native server archives for Linux x86_64/ARM64,
macOS Intel/Apple Silicon, and Windows x86_64, plus a Python wheel, source
distribution, and `SHA256SUMS`. Linux archives are static musl builds; every
server is built and smoke-tested with ordinary Cargo on its target platform.

For Linux, download the matching `.tar.gz` archive and verify it with the files
from the same release:

```bash
sha256sum --ignore-missing --check --strict SHA256SUMS
tar -xzf epochdeck-server-<version>-<target>.tar.gz
sudo install -m 0755 \
  epochdeck-server-<version>-<target>/epochdeck-server \
  /usr/local/bin/epochdeck-server
```

The checksum command verifies every release asset present in the current
directory and fails if it finds none. Keep only assets from the same release in
that directory; files for architectures you did not download are intentionally
ignored.

macOS and Windows server archives are ZIP files. The
[deployment guide](docs/deployment.md) lists the exact target names, checksum
commands, and extraction steps for each platform.

Install the wheel with `uv add` in each training project that imports the SDK.
Use `uv tool install ./epochdeck-*.whl` only for an isolated administration CLI.
Prereleases are not published to PyPI, crates.io, or npm.

Start the server locally:

```bash
epochdeck-server
```

It listens on `127.0.0.1:8787` by default. Before making it remotely reachable,
put it behind an authenticated HTTPS reverse proxy. Pre-alpha stored data may
be incompatible between builds, so start incompatible builds with fresh data
directories. See the [deployment guide](docs/deployment.md) for configuration.

## Log a run

```python
import epochdeck as ed

run = ed.init(
    project="robot-locomotion",
    name="baseline",
    config={"seed": 42, "learning_rate": 3e-4},
    server_url="http://127.0.0.1:8787",
)

for step in range(1_000):
    run.log(
        {
            "train": {"loss": 1 / (step + 1)},
            "reward": step * 0.1,
        }
    )

run.log({"rollout": ed.Video("rollout.mp4", caption="latest policy")})
run.finish(summary={"result": "complete"})
```

The SDK also supports `Image`, `Audio`, `Table`, and `Histogram` values,
versioned artifacts and lineage, structured traces, alerts, lazy history
queries, reports, sweeps, and W&B imports. See the
[compatibility matrix](docs/compatibility.md) for the current contract.

For disconnected work, initialize with `mode="offline"`, then upload the
durable spool later:

```bash
epochdeck sync ~/.local/share/epochdeck/spool/<run-id>
```

## Development

```bash
nix develop
just bootstrap
just check
just dev
```

Run `just bootstrap` after cloning or changing a lockfile. `just dev` starts the
API on `127.0.0.1:8787` and the Vite dashboard with `/api` proxied to it; Ctrl-C
stops both. Run `just single-binary` to build the server with the dashboard
embedded. Release archives contain that single executable.

## Repository layout

```text
crates/
  epochdeck-catalog/   catalog and transactional metadata
  epochdeck-protocol/  shared API types
  epochdeck-server/    HTTP server and process lifecycle
  epochdeck-storage/   columnar metric and rich-data storage
python/                W&B-compatible Python SDK and CLI
web/                   Svelte dashboard
docs/                  architecture, compatibility, and operations
```

Trackio and W&B are compatibility references, not runtime dependencies.

EpochDeck is licensed under [Apache-2.0](LICENSE).
