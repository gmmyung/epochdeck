from __future__ import annotations

import os
import sys
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from runloom._sweep_context import current_sweep_context
from runloom.artifact import Artifact
from runloom.run import Mode, Resume, Run, create_run
from runloom.trace import Trace, TraceKind

_current_run: Run | None = None
_current_run_lock = threading.Lock()


def init(
    *,
    project: str,
    name: str | None = None,
    id: str | None = None,
    config: Mapping[str, Any] | None = None,
    resume: bool | str | None = None,
    mode: Mode = "online",
    dir: str | Path | None = None,
    server_url: str | None = None,
) -> Run:
    """Create a run using the common W&B-style lifecycle arguments."""
    global _current_run

    with _current_run_lock:
        if _current_run is not None and not _current_run.finished:
            raise RuntimeError(
                "a Runloom run is already active; finish it before calling init again"
            )
        sweep_context = current_sweep_context.get()
        selected_config = dict(config or {})
        if sweep_context is not None:
            conflicts = {
                key
                for key, value in selected_config.items()
                if key in sweep_context.config and sweep_context.config[key] != value
            }
            if conflicts:
                raise ValueError(
                    "run config conflicts with sweep parameters: " + ", ".join(sorted(conflicts))
                )
            selected_config = {**selected_config, **sweep_context.config}
        spool_root = Path(dir) / ".runloom" / "spool" if dir is not None else None
        new_run = create_run(
            project=project,
            name=name,
            run_id=id,
            config=selected_config,
            mode=mode,
            resume=_normalize_resume(resume),
            server_url=server_url or os.environ.get("RUNLOOM_SERVER_URL", "http://127.0.0.1:8787"),
            spool_root=spool_root,
            sweep_trial_id=sweep_context.trial_id if sweep_context is not None else None,
        )
        if sweep_context is not None:
            sweep_context.run_id = new_run.id
        new_run._set_finish_callback(_clear_current_run)
        _current_run = None if new_run.finished else new_run
        _publish_current_run(_current_run)
        return new_run


def log(data: Mapping[str, Any], *, step: int | None = None) -> None:
    """Log scalar metrics to the active run."""
    run = _require_current_run()
    run.log(data, step=step)


def alert(title: str, text: str = "", *, level: str = "info") -> None:
    """Record a durable alert on the active run."""
    run = _require_current_run()
    run.alert(title, text, level=level)


def log_artifact(
    artifact: Artifact,
    *,
    aliases: list[str] | tuple[str, ...] | None = None,
) -> Artifact:
    """Durably log an output artifact on the active run."""
    return _require_current_run().log_artifact(artifact, aliases=aliases)


def use_artifact(artifact: Artifact | str) -> str:
    """Record an input-artifact lineage edge on the active run."""
    return _require_current_run().use_artifact(artifact)


def trace(
    name: str,
    *,
    kind: TraceKind = "span",
    trace_id: str | None = None,
    parent: Trace | str | None = None,
    attributes: Mapping[str, Any] | None = None,
    inputs: Any = None,
    start_time_ms: int | None = None,
) -> Trace:
    """Create a durable structured trace span on the active run."""
    return _require_current_run().trace(
        name,
        kind=kind,
        trace_id=trace_id,
        parent=parent,
        attributes=attributes,
        inputs=inputs,
        start_time_ms=start_time_ms,
    )


def finish(*, summary: Mapping[str, Any] | None = None, timeout: float = 30.0) -> None:
    """Flush and finish the active run."""
    run = _require_current_run()
    run.finish(summary=summary, timeout=timeout)


def _clear_current_run(run: Run) -> None:
    global _current_run

    with _current_run_lock:
        if _current_run is run:
            _current_run = None
            _publish_current_run(None)


def current_run() -> Run | None:
    """Return the active run without creating compatibility state."""
    with _current_run_lock:
        return _current_run


def _require_current_run() -> Run:
    run = current_run()
    if run is None:
        raise RuntimeError("no active Runloom run; call runloom.init first")
    return run


def _publish_current_run(run: Run | None) -> None:
    package = sys.modules.get("runloom")
    if package is not None:
        package.__dict__["run"] = run


def _normalize_resume(value: bool | str | None) -> Resume:
    if value in {None, False, "never"}:
        return "never"
    if value in {True, "allow", "auto"}:
        return "allow"
    if value == "must":
        return "must"
    raise ValueError("resume must be None, bool, 'never', 'allow', 'auto', or 'must'")
