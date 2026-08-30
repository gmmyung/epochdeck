from __future__ import annotations

import json
from collections.abc import Iterator, Mapping

import httpx
import pytest

from runloom import Api
from runloom.public_api import _compile_filters, _normalize_report_layout


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
                        "runs": [
                            {
                                "id": "run-2",
                                "project": "robotics",
                                "name": "eval-two",
                                "state": "finished",
                            }
                        ],
                        "next_before": "run-2",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "runs": [
                        {
                            "id": "run-1",
                            "project": "robotics",
                            "name": "eval-one",
                            "state": "finished",
                        }
                    ],
                    "next_before": None,
                },
            )
        if request.url.path == "/api/v1/runs/run-2":
            return httpx.Response(200, json=run_record("run-2", "eval-two"))
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
                    "sampled": False,
                    "source_points": None,
                    "source_last_sequence": sequence[0],
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


def test_public_api_bounds_report_layout_and_filter_construction() -> None:
    class LargeMapping(Mapping[str, str]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"key-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> str:
            self.reads += 1
            return "x" * 32

    layout = LargeMapping()
    with pytest.raises(ValueError, match="serialized report layout exceeds"):
        _normalize_report_layout(layout)
    assert layout.reads < 20_000

    with pytest.raises(ValueError, match="more than 32 config fields"):
        _compile_filters({f"config.key-{index}": index for index in range(33)})
    with pytest.raises(ValueError, match="JSON-safe range"):
        _compile_filters({"config.seed": 2**53})
    with pytest.raises(ValueError, match="1 to 256 non-control bytes"):
        _compile_filters({"config.": 1})
    with pytest.raises(ValueError, match="project filter must contain 1 to 128"):
        _compile_filters({"project": "x" * 129})


def test_public_run_samples_full_history_and_pages_every_artifact(monkeypatch) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/runs/run-1":
            return httpx.Response(
                200,
                json={
                    "id": "run-1",
                    "project": "robotics",
                    "name": "one",
                    "state": "finished",
                },
            )
        if request.url.path.endswith("/history"):
            return httpx.Response(
                200,
                json={
                    "run_id": "run-1",
                    "sequence": [10],
                    "step": [100],
                    "timestamp_ms": [1_000],
                    "metrics": {"loss": [0.5]},
                    "next_after": None,
                    "sampled": True,
                    "source_points": 1,
                    "source_last_sequence": 10,
                },
            )
        if request.url.path.endswith("/artifacts"):
            before = request.url.params.get("before")
            if before is None:
                return httpx.Response(
                    200,
                    json={
                        "artifacts": [{"artifact": {"id": "artifact-2"}, "relation": "output"}],
                        "next_before": "artifact-2",
                        "next_before_relation": "output",
                    },
                )
            assert request.url.params["before_relation"] == "output"
            return httpx.Response(
                200,
                json={
                    "artifacts": [{"artifact": {"id": "artifact-1"}, "relation": "input"}],
                    "next_before": None,
                    "next_before_relation": None,
                },
            )
        if request.url.path.startswith("/api/v1/artifacts/"):
            artifact_id = request.url.path.rsplit("/", 1)[1]
            return httpx.Response(
                200,
                json={"id": artifact_id, "entries": [], "metadata": {"full": True}},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)
    with Api(server_url="http://runloom.test") as api:
        run = api.run("run-1")
        assert run.history(keys=["loss"], samples=25)[0]["_step"] == 100
        assert [item["artifact"]["id"] for item in run.artifacts(page_size=1)] == [
            "artifact-2",
            "artifact-1",
        ]

    history_request = next(request for request in requests if request.url.path.endswith("/history"))
    assert dict(history_request.url.params) == {"key": "loss", "max_points": "25"}


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
            return httpx.Response(
                200,
                json={
                    "reports": [{"id": "report-1", "name": "Overview"}],
                    "next_before": None,
                },
            )
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
        listed = list(api.reports("robotics", per_page=20))
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
    assert [request.method for request in requests] == [
        "POST",
        "GET",
        "GET",
        "GET",
        "PUT",
        "DELETE",
    ]


def test_public_collections_are_lazy_and_reject_bad_or_repeated_cursors(monkeypatch) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/projects":
            return httpx.Response(
                200,
                json={"projects": [{"id": "project-1"}], "next_before": 7},
            )
        if request.url.path == "/api/v1/query/runs":
            return httpx.Response(
                200,
                json={
                    "runs": [
                        {
                            "id": "run-1",
                            "project": "robotics",
                            "name": "one",
                            "state": "finished",
                        }
                    ],
                    "next_before": "run-1",
                },
            )
        if request.url.path == "/api/v1/runs/run-1":
            return httpx.Response(
                200,
                json={
                    "id": "run-1",
                    "project": "robotics",
                    "name": "one",
                    "state": "finished",
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    original_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock_transport)
    with Api(server_url="http://runloom.test") as api:
        projects = api.projects(per_page=1)
        assert requests == []
        assert next(projects)["id"] == "project-1"
        with pytest.raises(TypeError, match="invalid or repeated cursor"):
            next(projects)

        runs = iter(api.runs("robotics", per_page=1))
        assert next(runs).id == "run-1"
        assert next(runs).id == "run-1"
        with pytest.raises(TypeError, match="invalid or repeated cursor"):
            next(runs)
