from __future__ import annotations

import os
import socket
from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any

from runloom._ids import uuid7
from runloom._sweep_context import SweepRunContext, current_sweep_context
from runloom.api import current_run
from runloom.client import RunloomClient
from runloom.run import SweepEarlyStop


def sweep(
    configuration: Mapping[str, Any],
    *,
    project: str,
    server_url: str | None = None,
) -> str:
    request = _normalize_sweep(configuration)
    with RunloomClient(_server_url(server_url)) as client:
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
) -> None:
    if not callable(function):
        raise TypeError("sweep agent function must be callable")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count <= 0):
        raise ValueError("sweep agent count must be a positive integer or None")
    selected_agent = agent_id or f"{socket.gethostname()}-{os.getpid()}-{uuid7()[:8]}"
    completed = 0
    with RunloomClient(_server_url(server_url), timeout=30.0) as client:
        while count is None or completed < count:
            claim = client.claim_sweep_trial(sweep_id, selected_agent)
            trial = claim.get("trial")
            if trial is None:
                return
            sweep_record = claim["sweep"]
            context = SweepRunContext(
                trial_id=str(trial["id"]),
                config=deepcopy(trial["config"]),
                metric_name=str(sweep_record["metric"]["name"]),
            )
            token = current_sweep_context.set(context)
            state = "completed"
            failure: Exception | None = None
            try:
                function()
            except SweepEarlyStop:
                state = "stopped"
            except Exception as error:
                state = "failed"
                failure = error
            finally:
                try:
                    active = current_run()
                    if active is not None and active.should_stop and state == "completed":
                        state = "stopped"
                    if active is not None and not active.finished:
                        active.finish(summary={"sweep_state": state})
                finally:
                    current_sweep_context.reset(token)
            if context.run_id is None:
                state = "failed"
                metric = None
            else:
                run = client.get_run(context.run_id)
                raw_metric = run.get("summary", {}).get(context.metric_name)
                metric = float(raw_metric) if isinstance(raw_metric, (int, float)) else None
            client.complete_sweep_trial(context.trial_id, state=state, metric=metric)
            completed += 1
            if failure is not None and raise_on_error:
                raise failure


def _normalize_sweep(configuration: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(configuration, Mapping):
        raise TypeError("sweep configuration must be a mapping")
    method = configuration.get("method")
    if method not in {"grid", "random"}:
        raise ValueError("sweep method must be 'grid' or 'random'")
    metric = configuration.get("metric")
    if not isinstance(metric, Mapping) or not isinstance(metric.get("name"), str):
        raise ValueError("sweep metric requires a string name")
    goal = metric.get("goal")
    if goal not in {"minimize", "maximize"}:
        raise ValueError("sweep metric goal must be 'minimize' or 'maximize'")
    raw_parameters = configuration.get("parameters")
    if not isinstance(raw_parameters, Mapping) or not raw_parameters:
        raise ValueError("sweep parameters must be a non-empty mapping")
    parameters: dict[str, dict[str, list[Any]]] = {}
    for name, parameter in raw_parameters.items():
        if not isinstance(name, str) or not isinstance(parameter, Mapping):
            raise TypeError("sweep parameter names and definitions must be mappings")
        unsupported = set(parameter) - {"values"}
        if unsupported:
            raise ValueError(
                f"unsupported sweep parameter fields for {name!r}: {', '.join(sorted(unsupported))}"
            )
        values = parameter.get("values")
        if not isinstance(values, (list, tuple)) or not values:
            raise ValueError(f"sweep parameter {name!r} requires a non-empty values list")
        parameters[name] = {"values": deepcopy(list(values))}
    early = configuration.get("early_terminate")
    normalized_early = None
    if early is not None:
        if not isinstance(early, Mapping) or early.get("type") != "median":
            raise ValueError("early_terminate currently supports only type='median'")
        unsupported = set(early) - {"type", "min_iter", "min_trials"}
        if unsupported:
            raise ValueError(
                "unsupported early_terminate fields: " + ", ".join(sorted(unsupported))
            )
        normalized_early = {
            "min_step": int(early.get("min_iter", 1)),
            "min_trials": int(early.get("min_trials", 3)),
        }
    max_runs = configuration.get("run_cap")
    if max_runs is None:
        if method == "grid":
            max_runs = 1
            for parameter in parameters.values():
                max_runs *= len(parameter["values"])
        else:
            raise ValueError("random sweeps require run_cap")
    if isinstance(max_runs, bool) or not isinstance(max_runs, int) or max_runs <= 0:
        raise ValueError("sweep run_cap must be a positive integer")
    return {
        "id": None,
        "name": configuration.get("name"),
        "method": method,
        "metric": {"name": metric["name"], "goal": goal},
        "parameters": parameters,
        "max_runs": max_runs,
        "early_terminate": normalized_early,
    }


def _server_url(value: str | None) -> str:
    return value or os.environ.get("RUNLOOM_SERVER_URL", "http://127.0.0.1:8787")
