from __future__ import annotations

import json
import threading

import httpx
import pytest

import runloom
from runloom import SweepEarlyStop
from runloom.run import create_run


def test_sweep_and_agent_bind_claimed_configuration(monkeypatch, tmp_path) -> None:
    requests: list[tuple[str, str, dict]] = []
    run_id: str | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal run_id
        body = json.loads(request.content) if request.content else {}
        requests.append((request.method, request.url.path, body))
        if request.url.path == "/api/v1/projects/demo/sweeps":
            return httpx.Response(
                201,
                json={"sweep": {"id": "sweep-1"}, "duplicate": False},
            )
        if request.url.path == "/api/v1/sweeps/sweep-1/claim":
            return httpx.Response(
                200,
                json={
                    "sweep": {"id": "sweep-1", "metric": {"name": "loss"}},
                    "trial": {
                        "id": "trial-1",
                        "config": {"learning_rate": 0.1, "seed": 7},
                    },
                },
            )
        if request.url.path == "/api/v1/projects/demo/runs":
            run_id = body["id"]
            assert body["sweep_trial_id"] == "trial-1"
            assert body["config"] == {"learning_rate": 0.1, "seed": 7}
            return httpx.Response(
                201,
                json={
                    "run": {"id": run_id, "name": "sweep-run", "config": body["config"]},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            return httpx.Response(
                201,
                json={
                    "run_id": run_id,
                    "batch_sequence": 1,
                    "accepted_points": 1,
                    "duplicate": False,
                    "metric_revision": 1,
                    "stop_requested": False,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={"run": {"state": "finished", "summary": {"loss": 0.25}}},
            )
        if request.method == "GET" and request.url.path == f"/api/v1/runs/{run_id}":
            return httpx.Response(
                200,
                json={"id": run_id, "state": "finished", "summary": {"loss": 0.25}},
            )
        if request.url.path == "/api/v1/sweep-trials/trial-1/complete":
            return httpx.Response(200, json={"id": "trial-1", **body})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)
    monkeypatch.setenv("RUNLOOM_SERVER_URL", "http://runloom.test")

    sweep_id = runloom.sweep(
        {
            "method": "grid",
            "metric": {"name": "loss", "goal": "minimize"},
            "parameters": {
                "learning_rate": {"values": [0.1, 0.01]},
                "seed": {"values": [7]},
            },
        },
        project="demo",
    )

    def train() -> None:
        run = runloom.init(project="demo", dir=tmp_path)
        assert run.config.to_dict() == {"learning_rate": 0.1, "seed": 7}
        run.log({"loss": 0.25})
        run.finish()

    runloom.agent(sweep_id, train, count=1, agent_id="agent-1")

    create_sweep_body = requests[0][2]
    assert create_sweep_body["max_runs"] == 2
    complete_body = next(body for _, path, body in requests if path.endswith("/complete"))
    assert complete_body == {"state": "completed", "metric": 0.25}


def test_run_raises_after_scheduler_stop_acknowledgement(tmp_path) -> None:
    stop_delivered = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/runs"):
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "run": {"id": body["id"], "name": "early-stop"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            stop_delivered.set()
            return httpx.Response(
                201,
                json={
                    "run_id": "run",
                    "batch_sequence": 1,
                    "accepted_points": 1,
                    "duplicate": False,
                    "metric_revision": 1,
                    "stop_requested": True,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"state": "finished"}})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    run = create_run(
        project="demo",
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        transport=httpx.MockTransport(handler),
    )
    run.log({"loss": 5.0})
    assert stop_delivered.wait(2)
    while not run.should_stop:
        stop_delivered.wait(0.01)
    with pytest.raises(SweepEarlyStop):
        run.log({"loss": 4.0})
    run.finish(timeout=2)


def test_sweep_rejects_unsupported_distribution_fields() -> None:
    with pytest.raises(ValueError, match="unsupported sweep parameter fields"):
        runloom.sweep(
            {
                "method": "random",
                "metric": {"name": "loss", "goal": "minimize"},
                "parameters": {"learning_rate": {"distribution": "log_uniform"}},
                "run_cap": 10,
            },
            project="demo",
        )
