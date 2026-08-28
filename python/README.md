# Runloom Python SDK

The Python client for Runloom, a standalone experiment tracker designed for
W&B-compatible workflows without hosted-service or Hugging Face dependencies.

The SDK supports online, offline, and disabled scalar runs with durable
background delivery, host telemetry, alerts, controlled config mutation, and
JSON summary documents.

```python
import runloom as wandb

run = wandb.init(project="demo", config={"seed": 42})
assert wandb.run is run
run.config.update({"optimizer": "adam"})
run.config.update({"seed": 7}, allow_val_change=True)
wandb.log({"loss": 0.25})
wandb.log({"rollout": wandb.Video("rollout.mp4", caption="latest policy")})
wandb.alert("Checkpoint saved", "Validation improved", level="info")
wandb.summary["status"] = "complete"
wandb.finish(summary={"tags": ["baseline", None]})
```

Changing an existing config value requires `allow_val_change=True`. Config and
summary values must be JSON-compatible and fit within the documented server
document budget.

Run IDs can be resumed explicitly with `resume="allow"` or `resume="must"`.
The durable spool restores config, summary, steps, sequences, and the exact
in-flight batch after a restart. Online resume requires a server that returns
the authoritative `next_sequence` and `next_step` lifecycle fields; the SDK
fails explicitly instead of guessing when those fields are unavailable.

CPU, memory, disk, network, process, load-average, and available NVIDIA GPU
metrics are recorded under `system/` every 15 seconds after the first user
metric. Set `RUNLOOM_SYSTEM_METRICS_INTERVAL=0` to disable collection or a
positive number of seconds to change the interval. System metrics do not change
automatic steps or the user summary. Alerts accept `info`, `warn`, and `error`
levels and use a separate durable delivery journal.

Native rich values can be mixed with scalars in the same step:

```python
run.log(
    {
        "frame": wandb.Image("frame.png", caption="camera 0"),
        "audio": wandb.Audio("episode.wav", sample_rate=48_000),
        "video": wandb.Video("episode.mp4"),
        "scores": wandb.Table(columns=["step", "score"], data=rows),
        "rewards": wandb.Histogram(reward_values, num_bins=64),
    }
)
```

Media and tables are serialized into a local SHA-256 spool using bounded copy
buffers before upload. Table iterables are consumed once and store a bounded
dashboard preview; histogram iterables spill to a temporary file while exact
bins are computed, so neither requires retaining a complete generator in memory.

Artifacts reuse the same durable blob spool:

```python
artifact = wandb.Artifact("policy", type="model", metadata={"step": 100_000})
artifact.add_file("checkpoint.bin", name="weights/checkpoint.bin")
run.log_artifact(artifact, aliases=["latest", "best"])

downstream = wandb.init(project="demo")
downstream.use_artifact(artifact)
```

`add_dir` walks deterministically without following symlinked directories and
manifests accept up to 4,096 unique POSIX paths. Offline use requires a concrete
artifact object or ID; online runs may also resolve `name:alias`.

Structured traces use the same durable delivery path:

```python
with run.trace("answer", kind="llm", inputs={"prompt": "hello"}) as span:
    span.add_message("assistant", "hello back")
    span.set_outputs({"tokens": 2})

with run.trace("lookup", kind="tool", parent=span) as child:
    child.set_inputs({"metric": "reward"})
    child.set_outputs({"value": 12.5})
```

Kinds are `span`, `llm`, `tool`, `chain`, and `agent`. A context manager marks a
successful span `ok` or captures an escaping exception as `error`. Inputs,
outputs, and messages must be JSON-compatible; complete payloads are stored in
the blob spool while bounded previews remain searchable.

The public read API performs server-side filtering and lazy cursor pagination:

```python
with wandb.Api() as api:
    runs = api.runs(
        "demo",
        filters={"state": "finished", "config.seed": 7},
        per_page=100,
    )
    for stored_run in runs:
        rows = stored_run.scan_history(keys=["loss"], page_size=1_000)
        for row in rows:
            print(row)
```

Supported filters are project, state, exact name, literal name substring, and
typed equality on top-level `config.*` or `summary.*` keys. Unsupported
operators and sort orders fail explicitly.
