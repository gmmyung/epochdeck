import json

import httpx
import pytest

from runloom import RunloomClient


def test_health_decodes_protocol_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/health"
        return httpx.Response(
            200,
            json={"service": "runloom", "version": "0.1.0", "status": "healthy"},
        )

    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        health = client.health()

    assert health.service == "runloom"
    assert health.status == "healthy"


def test_history_selects_full_resolution_or_sampled_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "run_id": "run-id",
                "sequence": [],
                "step": [],
                "timestamp_ms": [],
                "metrics": {"loss": []},
                "next_after": None,
                "sampled": "max_points" in request.url.params,
                "source_points": 0,
            },
        )

    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.history("run-id", keys=["loss"], limit=250, after=10)
        client.history("run-id", keys=["loss"], max_points=500)
        with pytest.raises(ValueError, match="cannot combine"):
            client.history("run-id", keys=["loss"], limit=10, max_points=10)

    assert dict(requests[0].url.params) == {"keys": "loss", "limit": "250", "after": "10"}
    assert dict(requests[1].url.params) == {"keys": "loss", "max_points": "500"}


def test_document_updates_use_explicit_patch_contracts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"run": {"config": {}, "summary": {}}})

    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.update_config("run-id", {"seed": 2}, allow_val_change=True)
        client.update_summary("run-id", {"status": "complete"})

    assert requests[0].method == "PATCH"
    assert requests[0].url.path == "/api/v1/runs/run-id/config"
    assert requests[0].read() == b'{"updates":{"seed":2},"allow_val_change":true}'
    assert requests[1].method == "PATCH"
    assert requests[1].url.path == "/api/v1/runs/run-id/summary"
    assert requests[1].read() == b'{"updates":{"status":"complete"}}'


def test_get_run_uses_the_public_lifecycle_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/v1/runs/run-id"
        return httpx.Response(200, json={"id": "run-id", "state": "finished"})

    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        run = client.get_run("run-id")

    assert run == {"id": "run-id", "state": "finished"}


def test_alerts_use_bounded_create_and_list_contracts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            body = request.read()
            return httpx.Response(201, json={"alert": json.loads(body), "duplicate": False})
        return httpx.Response(200, json={"alerts": [], "next_before": None})

    alert = {
        "id": "019c1234-5678-7000-8000-000000000007",
        "title": "Done",
        "text": "Training completed",
        "level": "info",
        "step": 4,
        "timestamp_ms": 1,
    }
    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.create_alert("run/id", alert)
        client.alerts("run/id", before=alert["id"], limit=25)

    assert requests[0].url.path == "/api/v1/runs/run/id/alerts"
    assert json.loads(requests[0].read()) == alert
    assert dict(requests[1].url.params) == {"limit": "25", "before": alert["id"]}


def test_traces_use_create_and_search_contracts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            body = json.loads(request.read())
            return httpx.Response(201, json={"span": body, "duplicate": False})
        return httpx.Response(200, json={"spans": [], "next_before": None})

    span = {
        "id": "019c1234-5678-7000-8000-000000000022",
        "trace_id": "trace-1",
        "parent_span_id": None,
        "name": "generate",
        "kind": "llm",
        "status": "ok",
        "start_time_ms": 1,
        "end_time_ms": 2,
        "step": 4,
        "attributes": {},
        "preview": {},
        "payload": None,
    }
    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.create_trace_span("run/id", span)
        client.trace_spans("run/id", q="assistant reward", before=span["id"], limit=25)

    assert requests[0].url.path == "/api/v1/runs/run/id/traces"
    assert json.loads(requests[0].read()) == span
    assert dict(requests[1].url.params) == {
        "limit": "25",
        "q": "assistant reward",
        "before": span["id"],
    }


def test_public_run_query_uses_a_structured_filter_body() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"runs": [], "next_before": None})

    query = {
        "project": "robotics",
        "state": "finished",
        "config_equals": {"seed": 7},
        "summary_equals": {"result": "complete"},
        "limit": 50,
    }
    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.query_runs(query)

    assert requests[0].url.path == "/api/v1/query/runs"
    assert json.loads(requests[0].read()) == query


def test_report_client_uses_project_collection_and_record_routes() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(201, json={"report": {}, "duplicate": False})
        if request.method == "GET" and request.url.path.endswith("/reports"):
            return httpx.Response(200, json={"reports": []})
        return httpx.Response(200, json={"id": "report/id"})

    report = {"id": None, "name": "Overview", "description": None, "layout": {}}
    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        client.create_report("demo/project", report)
        client.reports("demo/project", limit=25)
        client.update_report("report/id", {"name": "Updated", "layout": {}})
        client.delete_report("report/id")

    assert requests[0].url.path == "/api/v1/projects/demo/project/reports"
    assert json.loads(requests[0].read()) == report
    assert dict(requests[1].url.params) == {"limit": "25"}
    assert requests[2].method == "PUT"
    assert requests[2].url.path == "/api/v1/reports/report/id"
    assert requests[3].method == "DELETE"
