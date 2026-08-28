# Runloom

Runloom is a standalone, self-hosted experiment tracking library and server.
Its public Python API targets W&B compatibility, while its storage and dashboard
are designed for long-running, high-dimensional workloads.

The immediate compatibility milestone is Trackio feature parity. The long-term
contract is practical W&B feature parity across logging, run management, rich
media, artifacts, querying, and the dashboard.

Runloom is pre-alpha. This repository currently establishes the product
contract, architecture, reproducible development environment, and first
runnable vertical slice.

## Non-negotiable properties

- Lossless storage: dashboard sampling never deletes raw history.
- Bounded resources: ingestion, queries, caches, and responses have explicit
  memory limits and backpressure.
- Columnar metrics: numeric histories live in Arrow/Parquet rather than
  repeated JSON objects.
- Local-first operation: one service and local disks are sufficient.
- W&B-compatible Python API: compatibility is verified with contract tests.
- Independent implementation: no Hugging Face services, Hub, Spaces, Buckets,
  Datasets, Gradio, or related client dependencies.
- Storage separation: metrics and metadata can live on SSD while media and
  artifact blobs live on high-capacity disks.

## Architecture

```text
W&B-compatible Python SDK / importers
                  |
                  v
          Rust ingestion service
            |-- SQLite catalog and journal       SSD
            |-- Arrow/Parquet metric segments    SSD
            `-- content-addressed rich-data CAS  HDD/ZFS

Browser -> metric-aware query API -> columnar response -> virtualized charts
```

The backend is Rust with Tokio and Axum. Apache Arrow and Parquet form the
metric data plane, while SQLite is limited to the catalog and transactional
control plane. The dashboard is Svelte 5 and TypeScript. The Python SDK is
managed with uv. Nix pins all development tools.

See [System architecture](docs/architecture.md), the
[compatibility matrix](docs/compatibility.md), and the
[initial architecture decision](docs/adr/0001-system-architecture.md).

## Development

```bash
nix develop
just bootstrap
just check
```

Run the API and dashboard in separate terminals:

```bash
just dev-server
just dev-web
```

The API listens on `127.0.0.1:8787` by default. The Vite development server
proxies `/api` to it.

## Repository layout

```text
crates/
  runloom-catalog/   SQLite catalog and transactional metadata
  runloom-protocol/  shared API types
  runloom-server/    HTTP server and process lifecycle
  runloom-storage/   storage roots and columnar data plane
python/              W&B-compatible Python SDK and CLI
web/                 standalone Svelte dashboard
docs/                architecture, compatibility, and roadmap
```

Trackio and W&B are compatibility references, not runtime dependencies.

License is intentionally undecided until the project is ready to publish.
