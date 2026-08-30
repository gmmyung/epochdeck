# EpochDeck

EpochDeck is a standalone, self-hosted experiment tracking library and server.
Its public Python API targets W&B compatibility, while its storage and dashboard
are designed for long-running, high-dimensional workloads.

The immediate compatibility milestone is Trackio feature parity. The long-term
contract is practical W&B feature parity across logging, run management, rich
media, artifacts, querying, and the dashboard.

EpochDeck is pre-alpha. Scalar run tracking, native rich media, host telemetry,
durable alerts, bounded dashboard sampling, background metric compaction,
versioned artifacts, and structured traces are usable end to end. Finite sweeps
and persisted reports are also usable. Authentication, multi-user
authorization, and the wider compatibility surface remain under active
development.

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
            |-- SQLite catalog and manifests     SSD
            |-- Arrow/Parquet metric segments    SSD
            `-- content-addressed rich-data CAS  HDD/ZFS

Browser -> metric-aware query API -> columnar response -> lazy paged Canvas charts
```

The backend is Rust with Tokio and Axum. Apache Arrow and Parquet form the
metric data plane, while SQLite is limited to the catalog and transactional
control plane. The dashboard is Svelte 5 and TypeScript. The Python SDK is
managed with uv. Nix pins all development tools.

See [System architecture](docs/architecture.md), the
[compatibility matrix](docs/compatibility.md), the [HTTP API](docs/api.md), and
[metric benchmarks](docs/benchmarks.md). Production setup is covered by the
[Tailnet deployment](docs/deployment.md) and [operations](docs/operations.md)
runbooks. See [SECURITY.md](SECURITY.md) before exposing a server.

## Installing a prerelease

GitHub prereleases attach static Linux server archives for x86_64 and arm64, a
Python wheel and source distribution, and `SHA256SUMS`. Download all required
files from the same release, verify them together, and install the archive for
your machine:

```bash
sha256sum --ignore-missing --check --strict SHA256SUMS
tar -xzf epochdeck-server-0.1.0-alpha.1-x86_64-unknown-linux-musl.tar.gz
sudo install -m 0755 \
  epochdeck-server-0.1.0-alpha.1-x86_64-unknown-linux-musl/epochdeck-server \
  /usr/local/bin/epochdeck-server
uv add ./epochdeck-*.whl
```

The checksum command verifies every release asset present in the current
directory and fails if it finds none. Keep only files from that one release in
the directory; missing assets for other architectures are intentionally
ignored.

Install the wheel with `uv add` in each training project that imports the SDK.
Use `uv tool install ./epochdeck-*.whl` instead only when you want the isolated
administration CLI. This release channel does not publish packages to PyPI,
crates.io, or npm. EpochDeck remains pre-alpha: deploy only behind Tailnet
policy and start each incompatible build with empty storage roots.

## Development

```bash
nix develop
just bootstrap
just check
just single-binary
```

Run `just bootstrap` once after cloning and whenever a lockfile changes. It
fetches the locked Rust dependencies and installs the locked Python and
dashboard dependencies; subsequent incremental checks and single-binary builds
do not reinstall them.

Run the API and dashboard together:

```bash
just dev
```

The API listens on `127.0.0.1:8787` by default. The Vite development server
proxies `/api` to it. `just dev` stops both processes when either one exits or
when you press Ctrl-C. This two-process setup is only for contributor hot
reload; release archives contain one server executable with the dashboard
embedded.

Log a run from Python:

```python
import epochdeck as ed

run = ed.init(
    project="robot-locomotion",
    config={"seed": 42, "learning_rate": 3e-4},
    server_url="http://127.0.0.1:8787",
)
for step in range(1_000):
    run.log({"train": {"loss": 1 / (step + 1)}, "reward": step * 0.1})
run.log({"rollout": ed.Video("rollout.mp4", caption="latest policy")})
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
summary. Set `EPOCHDECK_SYSTEM_METRICS_INTERVAL=0` to disable collection. Alerts
are also fsynced locally and delivered idempotently without blocking training.
Use `mode="offline"`, then upload later with:

```bash
epochdeck sync ~/.local/share/epochdeck/spool/<run-id>
```

Metric history accepts finite numbers and booleans. Config and summary documents
accept bounded JSON values, including strings, booleans, nulls, arrays, and
nested objects. Each Python SDK document is capped at 256 KiB, 64 nesting
levels, and 65,536 JSON value nodes; invalid input fails before durable
journaling. Integer document values must stay within the exact signed JSON-safe
range `-9007199254740991` through `9007199254740991`. Unsupported metric object
types fail explicitly; native rich values use the types below. One log call
accepts at most 256 scalar metrics and 256 rich values across 64 nested mapping
levels and 65,536 traversed values. Flattened paths must be unique string keys;
booleans are stored canonically as `0.0` or `1.0`.

Rich log values use `Image`, `Audio`, `Video`, `Table`, and `Histogram`.
EpochDeck copies their bytes into the durable local spool, streams uploads without
buffering complete files, and deduplicates server content by SHA-256. Put
`EPOCHDECK_BLOBS_DIR` on HDD/ZFS while leaving the catalog and metric roots on SSD.
The full-width dashboard renders each type natively, groups repeated media keys
into step timelines, and supports byte-range video playback. Summary,
configuration, metrics, media, traces, and artifacts have separate tabs;
summary and configuration documents are searchable, expandable trees. Metric
charts are searchable and support hover values, pan, zoom, region selection,
axis ranges and log scales, multiple smoothing modes, and line or exact min/max
band display. Wheel and region selection zoom without a separate zoom mode.

Version checkpoints and datasets independently from run media:

```python
import epochdeck as ed

artifact = ed.Artifact("policy", type="model", metadata={"step": 100_000})
artifact.add_file("checkpoint.bin", name="weights/checkpoint.bin")
run.log_artifact(artifact, aliases=["latest", "best"])
run.finish()
downstream = ed.init(project="robot-locomotion")
downstream.use_artifact(artifact)  # explicit input lineage
```

Artifact versions and aliases are catalog transactions; file entries reuse the
CAS, individual downloads stream with byte ranges, whole-artifact ZIPs stream
with bounded memory, and input/output relationships appear in the dashboard's
tabbed file browser.

Log LLM, tool, chain, or agent execution as structured spans:

```python
import epochdeck as ed

with run.trace("answer", kind="llm", attributes={"model": "local-model"}) as span:
    span.set_inputs({"prompt": "Summarize the rollout"})
    span.add_message("user", "Summarize the rollout")
    span.add_message("assistant", "The policy remained stable.")
    span.set_outputs({"tokens": 6})
```

Trace IDs and parent span IDs preserve call trees. Complete JSON inputs,
outputs, and messages use content-addressed blob storage; SQLite indexes only
bounded metadata and previews for responsive dashboard search. The Python SDK
accepts at most 256 KiB of trace attributes and 16 MiB of complete JSON payload
per span. Attributes and the aggregate payload also allow at most 64 nesting
levels and 65,536 JSON value nodes, rejecting invalid documents before durable
journaling.

Query runs without loading an entire project:

```python
import epochdeck as ed

api = ed.Api(server_url="http://127.0.0.1:8787")
runs = api.runs(
    "robot-locomotion",
    filters={"state": "finished", "config.seed": 42, "summary.result": "complete"},
    per_page=100,
)
for stored_run in runs:  # cursor pages are fetched lazily
    for row in stored_run.scan_history(keys=["train/loss"], page_size=1_000):
        consume(row)
api.close()
```

The matching CLI commands are `epochdeck projects`, `epochdeck runs`, `epochdeck get`,
and `epochdeck history`. All list and history operations require bounded pages.

Schedule a finite parameter search with durable agents:

```python
import epochdeck as ed

sweep_id = ed.sweep(
    {
        "method": "grid",
        "metric": {"name": "validation/loss", "goal": "minimize"},
        "parameters": {
            "learning_rate": {"values": [1e-3, 3e-4]},
            "seed": {"values": [1, 2, 3]},
        },
        "early_terminate": {"type": "median", "min_iter": 100, "min_trials": 3},
    },
    project="robot-locomotion",
)


def train():
    run = ed.init(project="robot-locomotion")  # claimed values populate run.config
    while not run.should_stop:
        run.log(train_step(run.config))
    run.finish()


ed.agent(sweep_id, train)
```

Claims and trial results survive agent restarts. Search spaces are generated by
index and remain memory-bounded even when the Cartesian grid is large.

Persist a multi-run dashboard without copying metric history into the report:

```python
import epochdeck as ed

api = ed.Api(server_url="http://127.0.0.1:8787")
report = api.create_report(
    "robot-locomotion",
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
same lazy, exact-bucket, column-projected queries as ordinary run charts, so
opening a report does not duplicate or return complete histories.

Import a W&B project with four run workers and a durable checkpoint:

```bash
uv run --project python --extra wandb epochdeck import-wandb your-entity your-project \
  --server-url http://127.0.0.1:8787 \
  --checkpoint your-project.import.json \
  --workers 4
```

The importer derives stable EpochDeck IDs from terminal W&B runs and sends scalar
history in deterministic batches. Sparse high-step histories adapt their scan
window to avoid issuing millions of empty W&B requests. A response lost after
commit is replayed exactly; completed runs are skipped on the next invocation.
Because W&B materializes a complete step window before yielding it, the current
importer requires an authoritative `historyLineCount` of at most 100,000 rows;
larger or unbounded histories fail explicitly instead of risking an unbounded
response. The checkpoint also binds the original file-inclusion choice.
One process owns the checkpoint, Ctrl-C cancels queued work, and only already
active W&B SDK calls may still finish during shutdown. Re-run the same command
to resume contiguous acknowledged watermarks.

W&B run files and logged artifacts, including checkpoints, are streamed into
CAS with durable progress and bounded parallel transfer windows. Logged
artifacts retain their canonical W&B `vN` version; run-file shards use ordinary
automatic EpochDeck versions. Image, audio, and video history references are
downloaded and uploaded in parallel as native EpochDeck rich values while their
original files remain preserved. Temporary disk usage is bounded by active
transfers rather than the complete artifact.
Unsupported non-scalar history cells are counted in the imported summary rather
than silently presented as scalar metrics. W&B registry-wide and input-artifact
lineage remain outside this importer surface.

Create a complete portable project bundle:

```bash
uv run --project python epochdeck export robot-locomotion ./robot-locomotion.epochdeck-export
```

Before exporting, finish every selected run and quiesce all project writers. The
exporter captures the opaque project mutation token before traversal and verifies
it afterward; any project-visible change aborts without publishing. It scans raw
metrics in cursor pages, downloads CAS content once,
includes alerts, rich values, traces, artifact links, reports, sweeps, and
trials, then fsyncs and atomically publishes the private directory only after
every digest verifies.
See [Export format](docs/export-format.md).

## Repository layout

```text
crates/
  epochdeck-catalog/   SQLite catalog and transactional metadata
  epochdeck-protocol/  shared API types
  epochdeck-server/    HTTP server and process lifecycle
  epochdeck-storage/   storage roots and columnar data plane
python/              W&B-compatible Python SDK and CLI
web/                 standalone Svelte dashboard
docs/                architecture, compatibility, and roadmap
```

Trackio and W&B are compatibility references, not runtime dependencies.

EpochDeck is licensed under [Apache-2.0](LICENSE).
