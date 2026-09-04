from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from epochdeck._ids import uuid7
from epochdeck._json_normalization import (
    DEFAULT_MAX_JSON_DEPTH,
    DEFAULT_MAX_JSON_NODES,
    normalize_json_object,
    normalize_json_value_with_stats,
)
from epochdeck._limits import MAX_SAFE_INTEGER
from epochdeck._platform_fs import open_regular_file_descriptor, sync_directory, verify_directory
from epochdeck._sweep_context import SweepRunContext, current_sweep_context
from epochdeck.api import current_run
from epochdeck.client import EpochDeckApiError, EpochDeckClient
from epochdeck.run import SweepEarlyStop

_MAX_AGENT_STATE_BYTES = 512 * 1024
_MAX_SWEEP_PARAMETERS = 64
_MAX_SWEEP_VALUES = 256
_MAX_SWEEP_RUNS = 100_000
_MAX_SWEEP_PARAMETERS_BYTES = 256 * 1024


def sweep(
    configuration: Mapping[str, Any],
    *,
    project: str,
    server_url: str | None = None,
) -> str:
    request = _normalize_sweep(configuration)
    with EpochDeckClient(_server_url(server_url)) as client:
        response = client.create_sweep(project, request)
    return str(response["sweep"]["id"])


def agent(
    sweep_id: str,
    function: Callable[[], Any],
    *,
    count: int | None = None,
    agent_id: str | None = None,
    server_url: str | None = None,
    raise_on_error: bool = False,
    state_dir: str | Path | None = None,
) -> None:
    if not callable(function):
        raise TypeError("sweep agent function must be callable")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count <= 0):
        raise ValueError("sweep agent count must be a positive integer or None")
    selected_agent = agent_id or f"{socket.gethostname()}-{os.getpid()}-{uuid7()[:8]}"
    selected_server = _server_url(server_url)
    agent_state = _AgentState(
        _agent_state_path(state_dir, selected_server, sweep_id, selected_agent),
        sweep_id=sweep_id,
        agent_id=selected_agent,
    )
    completed = 0
    with EpochDeckClient(selected_server, timeout=30.0) as client:
        pending = agent_state.read()
        if pending is not None and pending.get("phase") == "terminal":
            _complete_pending_trial(client, pending)
            agent_state.clear()
            completed += 1
        while count is None or completed < count:
            recovered = agent_state.read()
            trial: dict[str, Any] | None = None
            sweep_record: dict[str, Any] | None = None
            if recovered is not None and recovered.get("phase") == "running":
                try:
                    renewed = client.heartbeat_sweep_trial(
                        str(recovered["trial_id"]),
                        selected_agent,
                    )
                except EpochDeckApiError as error:
                    if error.status_code not in {404, 409}:
                        raise
                    agent_state.clear()
                else:
                    trial = renewed
                    sweep_record = {"metric": {"name": str(recovered["metric_name"])}}
            if trial is None:
                claim = client.claim_sweep_trial(sweep_id, selected_agent)
                claimed = claim.get("trial")
                if claimed is None:
                    return
                if not isinstance(claimed, dict):
                    raise TypeError("sweep claim response has an invalid trial")
                record = claim.get("sweep")
                if not isinstance(record, dict):
                    raise TypeError("sweep claim response has an invalid sweep")
                trial = claimed
                sweep_record = record
            assert sweep_record is not None
            trial_id = str(trial["id"])
            trial_config = deepcopy(trial["config"])
            metric_name = str(sweep_record["metric"]["name"])
            bound_run_id = trial.get("run_id")
            if bound_run_id is not None:
                bound_run_id = str(bound_run_id)
            agent_state.write(
                {
                    "phase": "running",
                    "trial_id": trial_id,
                    "run_id": bound_run_id,
                    "config": trial_config,
                    "metric_name": metric_name,
                }
            )

            def persist_run_id(
                run_id: str,
                *,
                _trial_id: str = trial_id,
                _trial_config: dict[str, Any] = trial_config,
                _metric_name: str = metric_name,
            ) -> None:
                agent_state.write(
                    {
                        "phase": "running",
                        "trial_id": _trial_id,
                        "run_id": run_id,
                        "config": _trial_config,
                        "metric_name": _metric_name,
                    }
                )

            context = SweepRunContext(
                trial_id=trial_id,
                config=trial_config,
                metric_name=metric_name,
                run_id=bound_run_id,
                run_id_callback=persist_run_id,
            )
            token = current_sweep_context.set(context)
            trial_state = "completed"
            failure: Exception | None = None
            heartbeat = _Heartbeat(
                server_url=selected_server,
                trial_id=trial_id,
                agent_id=selected_agent,
            )
            heartbeat.start()
            try:
                function()
            except SweepEarlyStop:
                trial_state = "stopped"
            except Exception as error:
                trial_state = "failed"
                failure = error
            finally:
                active = current_run()
                try:
                    if active is not None and active.should_stop and trial_state == "completed":
                        trial_state = "stopped"
                    if active is not None and not active.finished:
                        active.finish(summary={"sweep_state": trial_state})
                except Exception as finish_error:
                    trial_state = "failed"
                    if failure is None:
                        failure = finish_error
                finally:
                    heartbeat.stop()
                    heartbeat.join(10)
                    current_sweep_context.reset(token)
            if heartbeat.is_alive():
                raise RuntimeError("timed out stopping the sweep heartbeat")
            if heartbeat.lease_lost:
                raise RuntimeError(f"sweep trial lease was lost: {heartbeat.last_error}")
            if context.run_id is None:
                trial_state = "failed"
                metric = None
            else:
                run = client.get_run(context.run_id)
                raw_metric = run.get("summary", {}).get(context.metric_name)
                metric = float(raw_metric) if isinstance(raw_metric, (int, float)) else None
            terminal = {
                "phase": "terminal",
                "trial_id": context.trial_id,
                "agent_id": selected_agent,
                "state": trial_state,
                "metric": metric,
            }
            # Persist before the idempotent terminal request so a lost response is retryable.
            agent_state.write(terminal)
            _complete_pending_trial(client, terminal)
            agent_state.clear()
            completed += 1
            if failure is not None and raise_on_error:
                raise failure


class _Heartbeat(threading.Thread):
    def __init__(self, *, server_url: str, trial_id: str, agent_id: str) -> None:
        super().__init__(name=f"epochdeck-sweep-{trial_id[:8]}", daemon=True)
        self._server_url = server_url
        self._trial_id = trial_id
        self._agent_id = agent_id
        self._stopping = threading.Event()
        self.last_error: Exception | None = None
        self.lease_lost = False

    def stop(self) -> None:
        self._stopping.set()

    def run(self) -> None:
        last_success = time.monotonic()
        with EpochDeckClient(self._server_url, timeout=5.0) as client:
            while not self._stopping.wait(20.0):
                try:
                    client.heartbeat_sweep_trial(self._trial_id, self._agent_id)
                except Exception as error:
                    self.last_error = error
                    if time.monotonic() - last_success >= 40.0:
                        self.lease_lost = True
                        return
                else:
                    self.last_error = None
                    last_success = time.monotonic()


class _AgentState:
    def __init__(self, path: Path, *, sweep_id: str, agent_id: str) -> None:
        self.path = path
        self._sweep_id = sweep_id
        self._agent_id = agent_id

    def read(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            with self.path.open("rb") as stream:
                encoded = stream.read(_MAX_AGENT_STATE_BYTES + 1)
            if len(encoded) > _MAX_AGENT_STATE_BYTES:
                raise RuntimeError(
                    f"sweep agent state exceeds {_MAX_AGENT_STATE_BYTES} bytes: {self.path}"
                )
            value = json.loads(encoded)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"invalid sweep agent state: {self.path}") from error
        if (
            not isinstance(value, dict)
            or value.get("sweep_id") != self._sweep_id
            or value.get("agent_id") != self._agent_id
            or value.get("phase") not in {"running", "terminal"}
        ):
            raise RuntimeError(f"invalid sweep agent state: {self.path}")
        return value

    def write(self, value: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        verify_directory(self.path.parent, private_mode=0o700)
        payload = {
            "sweep_id": self._sweep_id,
            "agent_id": self._agent_id,
            **deepcopy(dict(value)),
        }
        encoded = json.dumps(
            payload,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(encoded) + 1 > _MAX_AGENT_STATE_BYTES:
            raise RuntimeError(
                f"sweep agent state exceeds {_MAX_AGENT_STATE_BYTES} bytes: {self.path}"
            )
        temporary = self.path.with_name(f".{self.path.name}.{uuid7()}.tmp")
        descriptor = open_regular_file_descriptor(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            private_mode=0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            _fsync_directory(self.path.parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)
        if self.path.parent.exists():
            _fsync_directory(self.path.parent)


def _agent_state_path(
    state_dir: str | Path | None,
    server_url: str,
    sweep_id: str,
    agent_id: str,
) -> Path:
    root = Path(
        state_dir
        or os.environ.get("EPOCHDECK_SWEEP_STATE_DIR")
        or Path.home() / ".local" / "share" / "epochdeck" / "sweep-agents"
    ).expanduser()
    identity = hashlib.sha256(f"{server_url}\0{sweep_id}\0{agent_id}".encode()).hexdigest()
    return root / f"{identity}.json"


def _complete_pending_trial(client: EpochDeckClient, pending: Mapping[str, Any]) -> None:
    trial_id = pending.get("trial_id")
    agent_id = pending.get("agent_id")
    state = pending.get("state")
    metric = pending.get("metric")
    if (
        not isinstance(trial_id, str)
        or not trial_id
        or not isinstance(agent_id, str)
        or not agent_id
        or state not in {"completed", "failed", "stopped"}
        or (
            metric is not None
            and (isinstance(metric, bool) or not isinstance(metric, (int, float)))
        )
    ):
        raise RuntimeError("invalid terminal sweep agent state")
    client.complete_sweep_trial(
        trial_id,
        agent_id=agent_id,
        state=state,
        metric=float(metric) if metric is not None else None,
    )


def _fsync_directory(path: Path) -> None:
    sync_directory(path)


def _normalize_sweep(configuration: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(configuration, Mapping):
        raise TypeError("sweep configuration must be a mapping")
    _reject_unknown_fields(
        configuration,
        {"name", "method", "metric", "parameters", "early_terminate", "run_cap"},
        "sweep configuration",
    )
    method = configuration.get("method")
    if method not in {"grid", "random"}:
        raise ValueError("sweep method must be 'grid' or 'random'")
    sweep_name = configuration.get("name")
    if sweep_name is not None:
        _validate_sweep_text(sweep_name, "sweep name", 256)
    metric = configuration.get("metric")
    if not isinstance(metric, Mapping) or not isinstance(metric.get("name"), str):
        raise ValueError("sweep metric requires a string name")
    _reject_unknown_fields(metric, {"name", "goal"}, "sweep metric")
    _validate_sweep_text(metric["name"], "sweep metric name", 256)
    goal = metric.get("goal")
    if goal not in {"minimize", "maximize"}:
        raise ValueError("sweep metric goal must be 'minimize' or 'maximize'")
    raw_parameters = configuration.get("parameters")
    if not isinstance(raw_parameters, Mapping) or not raw_parameters:
        raise ValueError("sweep parameters must be a non-empty mapping")
    parameters: dict[str, dict[str, list[Any]]] = {}
    parameters_size = 2
    parameters_nodes = 1
    for parameter_index, (name, parameter) in enumerate(raw_parameters.items()):
        if parameter_index >= _MAX_SWEEP_PARAMETERS:
            raise ValueError(f"sweeps cannot contain more than {_MAX_SWEEP_PARAMETERS} parameters")
        if not isinstance(name, str) or not isinstance(parameter, Mapping):
            raise TypeError("sweep parameter names and definitions must be mappings")
        _validate_sweep_text(name, "sweep parameter name", 256)
        _reject_unknown_fields(parameter, {"values"}, f"sweep parameter {name!r}")
        values = parameter.get("values")
        if not isinstance(values, (list, tuple)) or not values:
            raise ValueError(f"sweep parameter {name!r} requires a non-empty values list")
        key_size = len(json.dumps(name, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        entry_base = key_size + len(b':{"values":[]}')
        candidate_size = parameters_size + (1 if parameters else 0) + entry_base
        candidate_nodes = parameters_nodes + 2
        if candidate_size > _MAX_SWEEP_PARAMETERS_BYTES:
            raise ValueError(
                f"serialized sweep parameters exceed {_MAX_SWEEP_PARAMETERS_BYTES} bytes"
            )
        normalized_values: list[Any] = []
        for value_index, raw_value in enumerate(values):
            if value_index >= _MAX_SWEEP_VALUES:
                raise ValueError(
                    f"sweep parameter {name!r} cannot contain more than {_MAX_SWEEP_VALUES} values"
                )
            separator_size = 1 if normalized_values else 0
            remaining_bytes = _MAX_SWEEP_PARAMETERS_BYTES - candidate_size - separator_size
            remaining_nodes = DEFAULT_MAX_JSON_NODES - candidate_nodes
            if remaining_bytes < 0 or remaining_nodes <= 0:
                raise ValueError("sweep parameters exceed their JSON construction budget")
            normalized = normalize_json_value_with_stats(
                raw_value,
                f"sweep parameter {name!r} value {value_index}",
                remaining_bytes,
                maximum_depth=DEFAULT_MAX_JSON_DEPTH - 3,
                maximum_nodes=remaining_nodes,
            )
            candidate_size += separator_size + normalized.size
            candidate_nodes += normalized.nodes
            normalized_values.append(normalized.value)
        parameters[name] = {"values": normalized_values}
        parameters_size = candidate_size
        parameters_nodes = candidate_nodes
    parameters = normalize_json_object(
        parameters,
        "sweep parameters",
        _MAX_SWEEP_PARAMETERS_BYTES,
        maximum_nodes=DEFAULT_MAX_JSON_NODES,
    )
    early = configuration.get("early_terminate")
    normalized_early = None
    if early is not None:
        if not isinstance(early, Mapping) or early.get("type") != "median":
            raise ValueError("early_terminate currently supports only type='median'")
        _reject_unknown_fields(
            early,
            {"type", "min_iter", "min_trials"},
            "early_terminate",
        )
        min_step = early.get("min_iter", 1)
        min_trials = early.get("min_trials", 3)
        if (
            isinstance(min_step, bool)
            or not isinstance(min_step, int)
            or not 0 <= min_step <= MAX_SAFE_INTEGER
        ):
            raise ValueError(f"early_terminate min_iter must be between 0 and {MAX_SAFE_INTEGER}")
        if (
            isinstance(min_trials, bool)
            or not isinstance(min_trials, int)
            or not 1 <= min_trials <= 100
        ):
            raise ValueError("early_terminate min_trials must be between 1 and 100")
        normalized_early = {
            "min_step": min_step,
            "min_trials": min_trials,
        }
    max_runs = configuration.get("run_cap")
    if max_runs is None:
        if method == "grid":
            max_runs = 1
            for parameter in parameters.values():
                max_runs *= len(parameter["values"])
        else:
            raise ValueError("random sweeps require run_cap")
    if (
        isinstance(max_runs, bool)
        or not isinstance(max_runs, int)
        or not 1 <= max_runs <= _MAX_SWEEP_RUNS
    ):
        raise ValueError(f"sweep run_cap must be between 1 and {_MAX_SWEEP_RUNS}")
    return {
        "id": None,
        "name": sweep_name,
        "method": method,
        "metric": {"name": metric["name"], "goal": goal},
        "parameters": parameters,
        "max_runs": max_runs,
        "early_terminate": normalized_early,
    }


def _reject_unknown_fields(
    values: Mapping[Any, Any],
    allowed: set[str],
    name: str,
) -> None:
    seen: set[str] = set()
    for index, key in enumerate(values):
        if not isinstance(key, str):
            raise TypeError(f"{name} field names must be strings")
        if key not in allowed:
            raise ValueError(f"unsupported {name} field: {key}")
        if index >= len(allowed):
            raise ValueError(f"{name} contains too many fields")
        if key in seen:
            raise ValueError(f"{name} contains duplicate field {key!r}")
        seen.add(key)


def _validate_sweep_text(value: Any, name: str, maximum: int) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > maximum
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(f"{name} must contain 1 to {maximum} non-control bytes")


def _server_url(value: str | None) -> str:
    return value or os.environ.get("EPOCHDECK_SERVER_URL", "http://127.0.0.1:8787")
