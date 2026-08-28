# Runloom Python SDK

The Python client for Runloom, a standalone experiment tracker designed for
W&B-compatible workflows without hosted-service or Hugging Face dependencies.

The SDK supports online, offline, and disabled scalar runs with durable
background delivery, controlled config mutation, and JSON summary documents.

```python
import runloom as wandb

run = wandb.init(project="demo", config={"seed": 42})
run.config.update({"optimizer": "adam"})
run.config.update({"seed": 7}, allow_val_change=True)
run.log({"loss": 0.25})
run.summary["status"] = "complete"
run.finish(summary={"tags": ["baseline", None]})
```

Changing an existing config value requires `allow_val_change=True`. Config and
summary values must be JSON-compatible and fit within the documented server
document budget.
