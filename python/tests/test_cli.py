from __future__ import annotations

import json

import httpx
from typer.testing import CliRunner

from runloom.cli import app


def test_health_is_an_explicit_subcommand(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/health"
        return httpx.Response(
            200,
            json={"service": "runloom", "version": "0.1.0", "status": "healthy"},
        )

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)

    result = CliRunner().invoke(app, ["health", "--server-url", "http://runloom.test"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "service": "runloom",
        "status": "healthy",
        "version": "0.1.0",
    }


def test_runs_command_sends_typed_document_filters(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/v1/query/runs"
        assert json.loads(request.content) == {
            "project": "robotics",
            "state": "finished",
            "config_equals": {"seed": 7},
            "summary_equals": {"result": "complete"},
            "limit": 25,
        }
        return httpx.Response(200, json={"runs": [], "next_before": None})

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)
    result = CliRunner().invoke(
        app,
        [
            "runs",
            "--project",
            "robotics",
            "--state",
            "finished",
            "--config",
            "seed=7",
            "--summary",
            'result="complete"',
            "--limit",
            "25",
            "--server-url",
            "http://runloom.test",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"runs": [], "next_before": None}


def test_runs_command_rejects_untyped_filter_values() -> None:
    result = CliRunner().invoke(app, ["runs", "--config", "seed=not-json"])

    assert result.exit_code == 2
    assert "invalid JSON" in result.output
