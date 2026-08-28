from typing import Any

from runloom.api import alert, current_run, finish, init, log
from runloom.client import Health, RunloomApiError, RunloomClient
from runloom.run import DeliveryError, Run, RunConfig, RunSummary, sync_spool

run: Run | None = current_run()

__all__ = [
    "DeliveryError",
    "Health",
    "Run",
    "RunConfig",
    "RunSummary",
    "RunloomApiError",
    "RunloomClient",
    "alert",
    "current_run",
    "finish",
    "init",
    "log",
    "run",
    "sync_spool",
]
__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    active_run = current_run()
    if name in {"config", "summary"}:
        if active_run is None:
            raise AttributeError(f"runloom.{name} is unavailable before runloom.init()")
        return getattr(active_run, name)
    raise AttributeError(f"module 'runloom' has no attribute {name!r}")
