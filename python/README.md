# EpochDeck Python SDK

The Python client for EpochDeck, a standalone experiment tracker designed for
W&B-compatible workflows without hosted-service or Hugging Face dependencies.

The SDK supports online, offline, and disabled scalar runs with durable
background delivery, host telemetry, alerts, controlled config mutation, and
JSON summary documents.

Add the wheel attached to the matching GitHub prerelease to the project that
imports the SDK; the distribution is not published to PyPI:

```bash
uv add ./epochdeck-*.whl
export EPOCHDECK_SERVER_URL=https://epochdeck.<tailnet>.ts.net
```

Use the conventional `import epochdeck as ed` alias in Python; the CLI command is
`epochdeck`. Install the administrative CLI in an isolated tool environment when
it is not needed as a project dependency. The optional W&B importer supports W&B
SDK `>=0.29,<0.30`; add it to that tool environment when needed:

```bash
uv tool install --with 'wandb>=0.29,<0.30' ./epochdeck-*.whl
```

```python
import epochdeck as ed

run = ed.init(project="demo", config={"seed": 42})
assert ed.run is run
run.config.update({"optimizer": "adam"})
run.config.update({"seed": 7}, allow_val_change=True)
ed.log({"loss": 0.25})
ed.log({"rollout": ed.Video("rollout.mp4", caption="latest policy")})
ed.alert("Checkpoint saved", "Validation improved", level="info")
ed.summary["status"] = "complete"
ed.finish(summary={"tags": ["baseline", None]})
```

Changing an existing config value requires `allow_val_change=True`. Config and
summary values must be JSON-compatible. The SDK enforces the 256 KiB document
budget during normalization, with at most 64 nesting levels and 65,536 JSON
value nodes, before writing durable state. Integer document values must stay in
the signed JSON-safe range `-9007199254740991` through `9007199254740991`.

Run IDs can be resumed explicitly with `resume="allow"` or `resume="must"`.
The durable spool restores config, separate explicit and metric-derived
summaries, steps, sequences, and the exact in-flight batch after a restart. A
summary snapshot records its exact metric-journal boundary every
128 records or 512 KiB and after explicit summary or finish changes; recovery
validates that boundary and scans only the bounded crash tail, independently of
delivery acknowledgements. Online resume requires a server that returns the
authoritative `next_sequence` and `next_step` lifecycle fields; the SDK fails
explicitly instead of guessing when those fields are unavailable.

Each scalar point contains 1 to 256 finite numeric or boolean values. Flattened
keys must contain 1 to 256 UTF-8 bytes and no Unicode control characters.
Booleans are normalized to `0.0` or `1.0`. One log call traverses at most 64
nested mapping levels and 65,536 values, rejects non-string or colliding
flattened keys, and accepts at most 256 rich values alongside the scalars.
Validation runs before the fsynced journal append, so an invalid point cannot
block later delivery. Metric requests are split by both point count and an exact
1,750,000 byte canonical-JSON body budget below the server's 2 MiB request
limit. The spool fixes the selected journal range plus the body size and SHA-256
digest before the first attempt, so retries replay identical bytes.
`run.summary` merges the bounded metric preview with explicit values, explicit
values win on key collisions, and `run.summary_truncated` reports that the
preview omitted keys; omitted metrics remain present in lossless history and
key discovery.

CPU, memory, disk, network, process, load-average, and available NVIDIA GPU
metrics are recorded under `system/` every 15 seconds after the first user
metric. Set `EPOCHDECK_SYSTEM_METRICS_INTERVAL=0` to disable collection or a
positive number of seconds to change the interval. System metrics do not change
automatic steps or the user summary. Alerts accept `info`, `warn`, and `error`
levels and use a separate durable delivery journal.

Native rich values can be mixed with scalars in the same step:

```python
import epochdeck as ed

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

Media and tables are serialized into a local SHA-256 spool using bounded copy
buffers before upload. Table iterables are consumed once and store a bounded
dashboard preview; histogram iterables spill to a temporary file while exact
bins are computed, so neither requires retaining a complete generator in memory.

Artifacts reuse the same durable blob spool:

```python
import epochdeck as ed

artifact = ed.Artifact("policy", type="model", metadata={"step": 100_000})
artifact.add_file("checkpoint.bin", name="weights/checkpoint.bin")
run.log_artifact(artifact, aliases=["latest", "best"])

downstream = ed.init(project="demo")
downstream.use_artifact(artifact)
```

`add_dir` walks deterministically without following symlinked directories and
manifests accept up to 4,096 unique POSIX paths. Offline use requires a concrete
artifact object or ID; online runs may also resolve `name:alias`.

Structured traces use the same durable delivery path:

```python
import epochdeck as ed

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
the blob spool while bounded previews remain searchable. The SDK rejects trace
attributes larger than 256 KiB and complete per-span input/output/message
payloads larger than 16 MiB before they enter the durable spool. Attributes and
each complete aggregate payload are additionally limited to 64 nesting levels
and 65,536 JSON value nodes.

The public read API performs server-side filtering and lazy cursor pagination:

```python
import epochdeck as ed

with ed.Api() as api:
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
operators and sort orders fail explicitly. Project, report, run, and run-artifact
collections are lazy iterators; consume them while the `Api` context is open.

Grid and random sweeps use finite typed value sets:

```python
import epochdeck as ed

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

The agent injects claimed parameters into `init`, binds the resulting run, and
reports completion. A median early-stop signal is available as
`run.should_stop`; logging after the signal raises `SweepEarlyStop` so the agent
can finish the trial as stopped. Unsupported distributions fail before any
remote request. Definitions accept at most 64 parameters, 256 values per
parameter, and 256 KiB of JSON-safe parameter data.
