from typing import Any

from runloom.api import alert, current_run, finish, init, log, log_artifact, use_artifact
from runloom.artifact import Artifact
from runloom.client import Health, RunloomApiError, RunloomClient
from runloom.rich import Audio, Histogram, Image, Table, Video
from runloom.run import DeliveryError, Run, RunConfig, RunSummary, sync_spool

run: Run | None = current_run()

__all__ = [
    "Artifact",
    "Audio",
    "DeliveryError",
    "Health",
    "Histogram",
    "Image",
    "Run",
    "RunConfig",
    "RunSummary",
    "RunloomApiError",
    "RunloomClient",
    "Table",
    "Video",
    "alert",
    "current_run",
    "finish",
    "init",
    "log",
    "log_artifact",
    "run",
    "sync_spool",
    "use_artifact",
]
__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    active_run = current_run()
    if name in {"config", "summary"}:
        if active_run is None:
            raise AttributeError(f"runloom.{name} is unavailable before runloom.init()")
        return getattr(active_run, name)
    raise AttributeError(f"module 'runloom' has no attribute {name!r}")
