# Runloom

Runloom is a standalone, self-hosted experiment tracking library and server.
Its public Python API targets W&B compatibility, while its storage and dashboard
are designed for long-running, high-dimensional workloads.

The immediate compatibility milestone is Trackio feature parity. The long-term
contract is practical W&B feature parity across logging, run management, rich
media, artifacts, querying, and the dashboard.

Runloom is pre-alpha. Scalar run tracking, host telemetry, durable alerts,
bounded dashboard sampling, and background metric compaction are usable end to
end; rich media, artifacts, authentication, sweeps, and the wider compatibility
surface remain under active development.

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
[compatibility matrix](docs/compatibility.md), the [HTTP API](docs/api.md), and
[metric benchmarks](docs/benchmarks.md).

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

Log a run from Python:

```python
import runloom as wandb

run = wandb.init(
    project="bello-mujoco",
    config={"seed": 42, "learning_rate": 3e-4},
    server_url="http://127.0.0.1:8787",
)
for step in range(1_000):
    run.log({"train": {"loss": 1 / (step + 1)}, "reward": step * 0.1})
run.alert("Checkpoint saved", "Validation improved", level="info")
run.config.update({"optimizer": "adam"})
run.summary["result"] = "complete"
run.finish(summary={"tags": ["baseline", "mujoco"]})
```

`log` fsyncs to a local journal and returns without waiting for HTTP. A bounded
background worker uploads batches, persists each in-flight byte range, and only
advances its acknowledgement after a successful response. Restarts therefore
replay the exact same batch, while the server accepts identical duplicates.
Resume restores the local config and summary and obtains the next sequence and
step from the server. Finish is idempotent and recoverable when its response is
lost after the server commits it.

The SDK samples host and process metrics every 15 seconds after the first user
metric. These `system/` histories do not advance training steps or enter the
summary. Set `RUNLOOM_SYSTEM_METRICS_INTERVAL=0` to disable collection. Alerts
are also fsynced locally and delivered idempotently without blocking training.
Use `mode="offline"`, then upload later with:

```bash
runloom sync ~/.local/share/runloom/spool/<run-id>
```

Metric history accepts finite numbers and booleans. Config and summary documents
accept bounded JSON values, including strings, booleans, nulls, arrays, and
nested objects. Unsupported rich metric values fail explicitly until their
native implementations land.

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
