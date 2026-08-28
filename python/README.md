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
