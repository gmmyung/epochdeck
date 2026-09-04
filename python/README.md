# EpochDeck Python SDK

The Python SDK logs training runs to a self-hosted EpochDeck server. Use the
conventional alias `import epochdeck as ed`.

## Install

Add the wheel from the matching GitHub prerelease to each training project:

```bash
uv add ./epochdeck-*.whl
```

The package is not yet published to PyPI.

## Configure a server

```bash
export EPOCHDECK_SERVER_URL=https://epochdeck.example.com
export EPOCHDECK_HTTP_USERNAME=epochdeck
read -rs EPOCHDECK_HTTP_PASSWORD
export EPOCHDECK_HTTP_PASSWORD
```

Username and password are needed only when the reverse proxy uses HTTP Basic
authentication. They are sent in the request header and never written to the
durable spool.

## Log a run

```python
import epochdeck as ed

run = ed.init(
    project="demo",
    name="baseline",
    config={"seed": 42, "optimizer": "adam"},
)

for step in range(1_000):
    run.log({"loss": 1 / (step + 1), "reward": step * 0.1})

run.summary["status"] = "complete"
run.finish()
```

Changing an existing config value requires
`run.config.update(values, allow_val_change=True)`.

## Choose a run mode

| Mode       | Behavior                                             |
| ---------- | ---------------------------------------------------- |
| `online`   | Spool locally and deliver in the background.         |
| `offline`  | Spool locally without contacting the server.         |
| `disabled` | Return an inert run without persistence or delivery. |

Resume a known run with `resume="allow"` or `resume="must"`. The local spool
recovers unacknowledged work after interruption and replays it idempotently.

Upload an offline spool later:

```bash
epochdeck sync <spool-directory>/<run-id>
```

Set `EPOCHDECK_SPOOL_DIR` to choose the spool root. Otherwise EpochDeck uses the
operating system's application-data directory.

## Log media and tables

Rich values can share a step with scalar metrics:

```python
run.log(
    {
        "frame": ed.Image("frame.png", caption="camera 0"),
        "audio": ed.Audio("episode.wav", sample_rate=48_000),
        "video": ed.Video("episode.mp4"),
        "scores": ed.Table(columns=["step", "score"], data=rows),
        "rewards": ed.Histogram(reward_values, num_bins=64),
    }
)
```

Files are copied into the durable local spool before upload. Large iterables are
processed with bounded memory.

## Track artifacts

```python
artifact = ed.Artifact("policy", type="model", metadata={"step": 100_000})
artifact.add_file("checkpoint.bin", name="weights/checkpoint.bin")
run.log_artifact(artifact, aliases=["latest", "best"])

downstream = ed.init(project="demo")
downstream.use_artifact(artifact)
```

Artifacts are immutable and versioned. Files are content-addressed and reused
across artifacts.

## Query runs

```python
with ed.Api() as api:
    runs = api.runs(
        "demo",
        filters={"state": "finished", "config.seed": 42},
        per_page=100,
    )
    for stored_run in runs:
        for row in stored_run.scan_history(keys=["loss"], page_size=1_000):
            print(row)
```

Collections and histories are lazy. Consume them while the `Api` context is
open.

## Run a finite sweep

```python
sweep_id = ed.sweep(
    {
        "method": "random",
        "metric": {"name": "loss", "goal": "minimize"},
        "parameters": {"learning_rate": {"values": [1e-2, 1e-3, 1e-4]}},
        "run_cap": 12,
    },
    project="demo",
)
ed.agent(sweep_id, train, count=12)
```

Grid and random sweeps currently accept finite typed value sets. Unsupported
distributions fail explicitly.

## Administration and imports

Install the CLI separately when it is not a training dependency:

```bash
uv tool install ./epochdeck-*.whl
```

Add the supported W&B SDK only when using the importer:

```bash
uv tool install --with 'wandb>=0.29,<0.30' ./epochdeck-*.whl
epochdeck import-wandb --help
```

See the [compatibility matrix](../docs/compatibility.md) for supported behavior
and the [HTTP API](../docs/api.md) for exact request limits.
