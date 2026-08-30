from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class SweepRunContext:
    trial_id: str
    config: dict[str, Any]
    metric_name: str
    run_id: str | None = None
    run_id_callback: Callable[[str], None] | None = None


current_sweep_context: ContextVar[SweepRunContext | None] = ContextVar(
    "runloom_sweep_context",
    default=None,
)
