from typing import TYPE_CHECKING, Any

from epochdeck._run import DeliveryError, Run, RunConfig, RunSummary, SweepEarlyStop, sync_spool
from epochdeck.api import alert, current_run, finish, init, log, log_artifact, trace, use_artifact
from epochdeck.artifact import Artifact
from epochdeck.client import EpochDeckApiError, EpochDeckClient, Health
from epochdeck.public_api import Api
from epochdeck.rich import Audio, Histogram, Image, Table, Video
from epochdeck.sweep import agent, sweep
from epochdeck.trace import Trace

if TYPE_CHECKING:
    run: Run | None

__all__ = [
    "Api",
    "Artifact",
    "Audio",
    "DeliveryError",
    "EpochDeckApiError",
    "EpochDeckClient",
    "Health",
    "Histogram",
    "Image",
    "Run",
    "RunConfig",
    "RunSummary",
    "SweepEarlyStop",
    "Table",
    "Trace",
    "Video",
    "agent",
    "alert",
    "current_run",
    "finish",
    "init",
    "log",
    "log_artifact",
    "run",
    "sweep",
    "sync_spool",
    "trace",
    "use_artifact",
]
__version__ = "0.1.0a1"


def __getattr__(name: str) -> Any:
    active_run = current_run()
    if name == "run":
        return active_run
    if name in {"config", "summary"}:
        if active_run is None:
            raise AttributeError(f"epochdeck.{name} is unavailable before epochdeck.init()")
        return getattr(active_run, name)
    raise AttributeError(f"module 'epochdeck' has no attribute {name!r}")
