from __future__ import annotations

import json

import httpx
import pytest

from runloom import Api


def test_public_api_pages_filtered_runs_and_scans_history(monkeypatch) -> None:
    query_bodies: list[dict] = []

    def run_record(run_id: str, name: str) -> dict:
        return {
            "id": run_id,
            "project": "robotics",
            "name": name,
            "state": "finished",
            "config": {"seed": 7},
            "summary": {"result": "complete"},
        }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/query/runs":
            body = json.loads(request.content)
            query_bodies.append(body)
            if "before" not in body:
                return httpx.Response(
                    200,
                    json={
                        "runs": [run_record("run-2", "eval-two")],
                        "next_before": "run-2",
                    },
                )
            return httpx.Response(
                200,
                json={"runs": [run_record("run-1", "eval-one")], "next_before": None},
            )
        if request.url.path == "/api/v1/runs/run-1":
            return httpx.Response(200, json=run_record("run-1", "eval-one"))
        if request.url.path == "/api/v1/runs/run-1/history":
            after = request.url.params.get("after")
            sequence = [1] if after is None else [2]
            return httpx.Response(
                200,
                json={
                    "run_id": "run-1",
                    "sequence": sequence,
                    "step": sequence,
                    "timestamp_ms": [1000 * sequence[0]],
                    "metrics": {"loss": [1.0 / sequence[0]]},
                    "next_after": 1 if after is None else None,
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)

    with Api(server_url="http://runloom.test") as api:
        runs = list(
            api.runs(
                "robotics",
                filters={
                    "state": "finished",
                    "name_contains": "eval",
                    "config.seed": 7,
                    "summary.result": "complete",
                },
                per_page=1,
            )
        )
        loaded = api.run("robotics/run-1")
        history = list(loaded.scan_history(keys=["loss"], page_size=1))

    assert [run.id for run in runs] == ["run-2", "run-1"]
    assert query_bodies[0] == {
        "config_equals": {"seed": 7},
        "limit": 1,
        "name_contains": "eval",
        "project": "robotics",
        "state": "finished",
        "summary_equals": {"result": "complete"},
    }
    assert query_bodies[1]["before"] == "run-2"
    assert history == [
        {"_sequence": 1, "_step": 1, "_timestamp_ms": 1000, "loss": 1.0},
        {"_sequence": 2, "_step": 2, "_timestamp_ms": 2000, "loss": 0.5},
    ]


def test_public_api_rejects_inert_filter_and_order_options(monkeypatch) -> None:
    monkeypatch.setattr(httpx, "Client", httpx.Client)
    api = Api(server_url="http://runloom.test")
    try:
        with pytest.raises(ValueError, match="unsupported comparison operator"):
            api.runs(filters={"config.seed": {"$gt": 2}})
        with pytest.raises(ValueError, match="only order"):
            api.runs(order="name")
    finally:
        api.close()


def test_public_api_manages_persisted_reports(monkeypatch) -> None:
    requests: list[httpx.Request] = []
    layout = {
        "columns": 2,
        "panels": [
            {
                "id": "loss",
                "title": "Loss",
                "kind": "metric",
                "run_id": "run-1",
                "metric_keys": ["train/loss"],
                "markdown": None,
                "width": 1,
                "height": 360,
            }
        ],
    }

    def report_record(name: str) -> dict:
        return {
            "id": "report-1",
            "project": "robotics",
            "name": name,
            "description": None,
            "layout": layout,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                201, json={"report": report_record("Overview"), "duplicate": False}
            )
        if request.method == "GET" and request.url.path.endswith("/reports"):
            return httpx.Response(200, json={"reports": [report_record("Overview")]})
        if request.method == "PUT":
            return httpx.Response(200, json=report_record("Updated"))
        if request.method == "DELETE":
            return httpx.Response(200, json=report_record("Updated"))
        return httpx.Response(200, json=report_record("Overview"))

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)

    with Api(server_url="http://runloom.test") as api:
        created = api.create_report("robotics", name="Overview", layout=layout, id="report-1")
        listed = api.reports("robotics", per_page=20)
        loaded = api.report("report-1")
        updated = api.update_report("report-1", name="Updated", layout=layout)
        deleted = api.delete_report("report-1")

    assert created["id"] == "report-1"
    assert listed[0]["name"] == "Overview"
    assert loaded["project"] == "robotics"
    assert updated["name"] == "Updated"
    assert deleted["id"] == "report-1"
    assert json.loads(requests[0].content)["id"] == "report-1"
    assert dict(requests[1].url.params) == {"limit": "20"}
    assert [request.method for request in requests] == ["POST", "GET", "GET", "PUT", "DELETE"]
