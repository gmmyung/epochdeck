from __future__ import annotations

import json
import threading

import httpx
import pytest

import runloom
from runloom import SweepEarlyStop
from runloom.run import create_run
from runloom.sweep import _MAX_AGENT_STATE_BYTES, _AgentState, _normalize_sweep


def _summary_fields(
    *,
    explicit: dict | None = None,
    metric: dict | None = None,
) -> dict:
    explicit = dict(explicit or {})
    metric = dict(metric or {})
    return {
        "explicit_summary": explicit,
        "metric_summary": metric,
        "summary": {**metric, **explicit},
        "summary_truncated": False,
    }


def test_sweep_and_agent_bind_claimed_configuration(monkeypatch, tmp_path) -> None:
    requests: list[tuple[str, str, dict]] = []
    run_id: str | None = None
    metric_summary: dict = {}
    explicit_summary: dict = {}

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
                    "run": {
                        "id": run_id,
                        "name": "sweep-run",
                        "config": body["config"],
                        **_summary_fields(),
                    },
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            for point in body["points"]:
                metric_summary.update(point["metrics"])
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
            explicit_summary.update(body["summary"])
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": run_id,
                        "state": "finished",
                        **_summary_fields(
                            explicit=explicit_summary,
                            metric=metric_summary,
                        ),
                    }
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/runs/{run_id}":
            return httpx.Response(
                200,
                json={
                    "id": run_id,
                    "state": "finished",
                    **_summary_fields(
                        explicit=explicit_summary,
                        metric=metric_summary,
                    ),
                },
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
    assert complete_body == {
        "agent_id": "agent-1",
        "state": "completed",
        "metric": 0.25,
    }


def test_run_raises_after_scheduler_stop_acknowledgement(tmp_path) -> None:
    stop_delivered = threading.Event()
    run_id: str | None = None
    metric_summary: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal run_id
        if request.url.path.endswith("/runs"):
            body = json.loads(request.content)
            run_id = body["id"]
            return httpx.Response(
                201,
                json={
                    "run": {"id": body["id"], "name": "early-stop", **_summary_fields()},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            body = json.loads(request.content)
            for point in body["points"]:
                metric_summary.update(point["metrics"])
            stop_delivered.set()
            return httpx.Response(
                201,
                json={
                    "run_id": run_id,
                    "batch_sequence": 1,
                    "accepted_points": 1,
                    "duplicate": False,
                    "metric_revision": 1,
                    "stop_requested": True,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": run_id,
                        "state": "finished",
                        **_summary_fields(metric=metric_summary),
                    }
                },
            )
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
    with pytest.raises(ValueError, match="unsupported sweep parameter"):
        runloom.sweep(
            {
                "method": "random",
                "metric": {"name": "loss", "goal": "minimize"},
                "parameters": {"learning_rate": {"distribution": "log_uniform"}},
                "run_cap": 10,
            },
            project="demo",
        )


def test_sweep_rejects_unknown_top_level_and_metric_fields() -> None:
    base = {
        "method": "grid",
        "metric": {"name": "loss", "goal": "minimize"},
        "parameters": {"seed": {"values": [1]}},
    }
    with pytest.raises(ValueError, match="unsupported sweep configuration field"):
        _normalize_sweep({**base, "program": "train.py"})
    with pytest.raises(ValueError, match="unsupported sweep metric field"):
        _normalize_sweep({**base, "metric": {**base["metric"], "target": 0.1}})


def test_sweep_value_construction_is_bounded_and_json_safe() -> None:
    values_read = 0

    class BoundedValues(list):
        def __iter__(self):
            nonlocal values_read
            for value in range(100_000):
                values_read += 1
                yield value

        def __bool__(self) -> bool:
            return True

    with pytest.raises(ValueError, match="more than 256 values"):
        _normalize_sweep(
            {
                "method": "grid",
                "metric": {"name": "loss", "goal": "minimize"},
                "parameters": {"seed": {"values": BoundedValues()}},
            }
        )
    assert values_read == 257

    with pytest.raises(ValueError, match="JSON-safe range"):
        _normalize_sweep(
            {
                "method": "grid",
                "metric": {"name": "loss", "goal": "minimize"},
                "parameters": {"seed": {"values": [2**53]}},
            }
        )


def test_agent_resumes_a_reclaimed_bound_run(monkeypatch, tmp_path) -> None:
    create_body: dict | None = None
    summary: dict = {}
    recovered_run_id = "019c1234-5678-7000-8000-000000000040"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal create_body
        body = json.loads(request.content) if request.content else {}
        if request.url.path == "/api/v1/sweeps/sweep-1/claim":
            return httpx.Response(
                200,
                json={
                    "sweep": {"id": "sweep-1", "metric": {"name": "loss"}},
                    "trial": {
                        "id": "trial-1",
                        "agent_id": "agent-2",
                        "run_id": recovered_run_id,
                        "config": {"seed": 9},
                    },
                },
            )
        if request.url.path == "/api/v1/projects/demo/runs":
            create_body = body
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": recovered_run_id,
                        "name": "recovered",
                        "config": {"seed": 9},
                        **_summary_fields(),
                    },
                    "resumed": True,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/finish"):
            summary.update(body["summary"])
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": recovered_run_id,
                        "state": "finished",
                        **_summary_fields(explicit=summary),
                    }
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/runs/{recovered_run_id}":
            return httpx.Response(
                200,
                json={
                    "id": recovered_run_id,
                    "state": "finished",
                    **_summary_fields(explicit=summary),
                },
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

    def train() -> None:
        run = runloom.init(project="demo", dir=tmp_path)
        assert run.id == recovered_run_id
        run.finish(summary={"loss": 0.1})

    runloom.agent(
        "sweep-1",
        train,
        count=1,
        agent_id="agent-2",
        state_dir=tmp_path / "agent-state",
    )

    assert create_body is not None
    assert create_body["id"] == recovered_run_id
    assert create_body["resume"] == "allow"
    assert list((tmp_path / "agent-state").glob("*.json")) == []


def test_sweep_agent_state_reads_and_writes_are_bounded(tmp_path) -> None:
    path = tmp_path / "state" / "agent.json"
    state = _AgentState(path, sweep_id="sweep-1", agent_id="agent-1")
    path.parent.mkdir()
    path.write_bytes(b"x" * (_MAX_AGENT_STATE_BYTES + 1))
    with pytest.raises(RuntimeError, match="state exceeds"):
        state.read()

    path.write_bytes(b"\xff\n")
    with pytest.raises(RuntimeError, match="invalid sweep agent state"):
        state.read()

    with pytest.raises(RuntimeError, match="state exceeds"):
        state.write(
            {
                "phase": "running",
                "trial_id": "trial-1",
                "run_id": None,
                "metric_name": "loss",
                "config": {"payload": "x" * _MAX_AGENT_STATE_BYTES},
            }
        )
