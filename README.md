# Runloom

Runloom is a standalone, self-hosted experiment tracking library and server.
Its public Python API targets W&B compatibility, while its storage and dashboard
are designed for long-running, high-dimensional workloads.

The immediate compatibility milestone is Trackio feature parity. The long-term
contract is practical W&B feature parity across logging, run management, rich
media, artifacts, querying, and the dashboard.

Runloom is pre-alpha. Scalar run tracking, native rich media, host telemetry,
durable alerts, bounded dashboard sampling, background metric compaction,
versioned artifacts, and structured traces are usable end to end. Finite sweeps
and persisted reports are also usable. Authentication, import/export,
multi-user authorization and the wider compatibility surface remain under
active development.

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
[metric benchmarks](docs/benchmarks.md). Production setup is covered by the
[Tailnet deployment](docs/deployment.md) and [operations](docs/operations.md)
runbooks.

## Development

```bash
nix develop
just bootstrap
just check
just single-binary
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
run.log({"rollout": wandb.Video("rollout.mp4", caption="latest policy")})
run.alert("Checkpoint saved", "Validation improved", level="info")
with run.trace("policy-evaluation", kind="agent", inputs={"episode": 7}) as span:
    span.add_message("assistant", "The rollout completed successfully")
    span.set_outputs({"reward": 42.0})
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
nested objects. Unsupported metric object types fail explicitly; native rich
values use the types below.

Rich log values use `Image`, `Audio`, `Video`, `Table`, and `Histogram`.
Runloom copies their bytes into the durable local spool, streams uploads without
buffering complete files, and deduplicates server content by SHA-256. Put
`RUNLOOM_BLOBS_DIR` on HDD/ZFS while leaving the catalog and metric roots on SSD.
The dashboard renders each type natively and video delivery supports byte ranges.

Version checkpoints and datasets independently from run media:

```python
artifact = wandb.Artifact("policy", type="model", metadata={"step": 100_000})
artifact.add_file("checkpoint.bin", name="weights/checkpoint.bin")
run.log_artifact(artifact, aliases=["latest", "best"])
run.finish()
downstream = wandb.init(project="bello-mujoco")
downstream.use_artifact(artifact)  # explicit input lineage
```

Artifact versions and aliases are catalog transactions; file entries reuse the
CAS, downloads stream with byte ranges, and input/output relationships appear in
the dashboard.

Log LLM, tool, chain, or agent execution as structured spans:

```python
with run.trace("answer", kind="llm", attributes={"model": "local-model"}) as span:
    span.set_inputs({"prompt": "Summarize the rollout"})
    span.add_message("user", "Summarize the rollout")
    span.add_message("assistant", "The policy remained stable.")
    span.set_outputs({"tokens": 6})
```

Trace IDs and parent span IDs preserve call trees. Complete JSON inputs,
outputs, and messages use content-addressed blob storage; SQLite indexes only
bounded metadata and previews for responsive dashboard search.

Query runs without loading an entire project:

```python
api = wandb.Api(server_url="http://127.0.0.1:8787")
runs = api.runs(
    "bello-mujoco",
    filters={"state": "finished", "config.seed": 42, "summary.result": "complete"},
    per_page=100,
)
for stored_run in runs:  # cursor pages are fetched lazily
    for row in stored_run.scan_history(keys=["train/loss"], page_size=1_000):
        consume(row)
api.close()
```

The matching CLI commands are `runloom projects`, `runloom runs`, `runloom get`,
and `runloom history`. All list and history operations require bounded pages.

Schedule a finite parameter search with durable agents:

```python
sweep_id = wandb.sweep(
    {
        "method": "grid",
        "metric": {"name": "validation/loss", "goal": "minimize"},
        "parameters": {
            "learning_rate": {"values": [1e-3, 3e-4]},
            "seed": {"values": [1, 2, 3]},
        },
        "early_terminate": {"type": "median", "min_iter": 100, "min_trials": 3},
    },
    project="bello-mujoco",
)


def train():
    run = wandb.init(project="bello-mujoco")  # claimed values populate run.config
    while not run.should_stop:
        run.log(train_step(run.config))
    run.finish()


wandb.agent(sweep_id, train)
```

Claims and trial results survive agent restarts. Search spaces are generated by
index and remain memory-bounded even when the Cartesian grid is large.

Persist a multi-run dashboard without copying metric history into the report:

```python
api = wandb.Api(server_url="http://127.0.0.1:8787")
report = api.create_report(
    "bello-mujoco",
    name="Training comparison",
    description="Loss curves for the current baselines.",
    layout={
        "columns": 2,
        "panels": [
            {
                "id": "notes",
                "title": "Notes",
                "kind": "markdown",
                "run_id": None,
                "metric_keys": [],
                "markdown": "## Baselines\n- seed 1\n- seed 2",
                "width": 2,
                "height": 220,
            },
            {
                "id": "loss",
                "title": "Training loss",
                "kind": "metric",
                "run_id": "<run-id>",
                "metric_keys": ["train/loss"],
                "markdown": None,
                "width": 2,
                "height": 380,
            },
        ],
    },
)
api.close()
```

Reports persist only bounded layout definitions. Their metric panels issue the
same lazy, sampled, column-projected queries as ordinary run charts, so opening
a report does not duplicate or scan complete histories.

Import a W&B project with four run workers and a durable checkpoint:

```bash
uv run --project python --with wandb runloom import-wandb gyungmin bello-mujoco \
  --server-url http://127.0.0.1:8787 \
  --checkpoint bello-mujoco.import.json \
  --workers 4
```

The importer derives stable Runloom IDs from W&B run paths and sends scalar
history in deterministic batches. Sparse high-step histories adapt their scan
window to avoid issuing millions of empty W&B requests. A response lost after
commit is replayed exactly; completed runs are skipped on the next invocation.
W&B run files and logged artifacts, including checkpoints, are streamed into
CAS with durable progress and a fixed four-artifact transfer window. Image,
audio, and video history references become native Runloom rich values while
their original files remain preserved.
Unsupported non-scalar history cells are counted in the imported summary rather
than silently presented as scalar metrics. W&B registry-wide and input-artifact
lineage remain outside this importer surface.

Create a complete portable project bundle:

```bash
uv run --project python runloom export bello-mujoco ./bello-mujoco.runloom-export
```

The exporter scans raw metrics in cursor pages, downloads CAS content once,
includes alerts, rich values, traces, artifact links, reports, sweeps, and
trials, and atomically publishes the directory only after every digest verifies.
See [Export format](docs/export-format.md).

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
