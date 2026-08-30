from __future__ import annotations

import json
import re

import httpx
from typer.testing import CliRunner

from epochdeck import __version__
from epochdeck.cli import app

ANSI_CONTROL_SEQUENCE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def test_version_is_available_without_server_state() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == f"epochdeck {__version__}\n"


def test_health_is_an_explicit_subcommand(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/health"
        return httpx.Response(
            200,
            json={"service": "epochdeck", "version": "0.1.0", "status": "healthy"},
        )

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)

    result = CliRunner().invoke(app, ["health", "--server-url", "http://epochdeck.test"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "service": "epochdeck",
        "status": "healthy",
        "version": "0.1.0",
    }


def test_doctor_returns_bounded_server_diagnostics(monkeypatch) -> None:
    diagnostics = {
        "service": "epochdeck",
        "version": "0.1.0",
        "requests_total": 12,
        "recent_slow_requests": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/diagnostics"
        return httpx.Response(200, json=diagnostics)

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)
    result = CliRunner().invoke(app, ["doctor", "--server-url", "http://epochdeck.test"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == diagnostics


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
            "http://epochdeck.test",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"runs": [], "next_before": None}


def test_runs_command_rejects_untyped_filter_values() -> None:
    result = CliRunner().invoke(app, ["runs", "--config", "seed=not-json"])

    assert result.exit_code == 2
    assert "invalid JSON" in result.output


def test_export_help_states_consistency_preconditions() -> None:
    result = CliRunner().invoke(
        app,
        ["export", "--help"],
        terminal_width=160,
        color=False,
    )

    assert result.exit_code == 0
    help_text = " ".join(ANSI_CONTROL_SEQUENCE.sub("", result.stdout).split())
    assert "every selected run is finished and project writers are quiesced" in help_text
    assert "opaque project mutation token is captured before traversal" in help_text
    assert "verified afterward" in help_text
    assert "project-visible change aborts without publishing" in help_text
