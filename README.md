<p align="center">
  <img src="web/public/epochdeck-mark.svg" width="96" height="96" alt="EpochDeck logo">
</p>

<h1 align="center">EpochDeck</h1>

<p align="center">
  <a href="https://github.com/gmmyung/epochdeck/actions/workflows/ci.yml"><img src="https://github.com/gmmyung/epochdeck/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI status"></a>
  <a href="https://github.com/gmmyung/epochdeck/releases"><img src="https://img.shields.io/github/v/release/gmmyung/epochdeck?include_prereleases&amp;sort=semver&amp;label=release" alt="Latest release"></a>
</p>

EpochDeck is a high-performance, self-hosted experiment tracker for logging,
comparing, and exploring training runs, metrics, traces, videos, and artifacts.
It stays fast and responsive even with long runs and very large metric
histories.

> [!WARNING]
> EpochDeck is pre-alpha. Scalar metrics, host telemetry, alerts, rich media,
> versioned artifacts, traces, finite sweeps, persisted reports, and streaming
> W&B import/export are usable end to end. Authentication, multi-user
> authorization, and the wider compatibility surface are still in development.
> For remote access, place the server behind an authenticated HTTPS reverse
> proxy and review [SECURITY.md](SECURITY.md).

## Quick start

Download the server archive and Python wheel from the
[latest release](https://github.com/gmmyung/epochdeck/releases). Install the
wheel in your training project and start the server:

```bash
uv add ./epochdeck-*.whl
epochdeck-server
```

Log a run with the conventional `ed` alias:

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

Open [http://127.0.0.1:8787](http://127.0.0.1:8787) to compare runs and inspect
metrics, media, artifacts, traces, configuration, and summaries.

## Features

- Durable, nonblocking online and offline logging.
- Scalar and system metrics with bounded, spike-preserving chart queries.
- Native images, audio, video, tables, histograms, traces, and alerts.
- Versioned artifacts, lineage, reports, sweeps, and multi-run comparison.
- Resumable W&B imports and lossless EpochDeck project exports.
- One self-contained server binary with no hosted service dependency.

See the [compatibility matrix](docs/compatibility.md) for the exact pre-alpha
feature surface.

## Documentation

- [Python SDK](python/README.md)
- [Self-hosting](docs/deployment.md)
- [Backup, upgrades, and diagnostics](docs/operations.md)
- [Documentation index](docs/index.md)
- [Contributing](CONTRIBUTING.md)

EpochDeck has no Hugging Face, Hub, Spaces, Buckets, Datasets, or Gradio runtime
dependency. Trackio and W&B are compatibility references only.

## AI assistance

EpochDeck is developed with substantial AI assistance. Changes are reviewed and
tested, but AI-generated code can still contain mistakes. Evaluate the software
independently before relying on it.

EpochDeck is licensed under [Apache-2.0](LICENSE).
