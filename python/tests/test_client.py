import json
from pathlib import Path

import httpx
import pytest

from epochdeck import EpochDeckClient
from epochdeck._protocol import DeliveryProtocolError, encode_json_request


def test_health_decodes_protocol_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/health"
        return httpx.Response(
            200,
            json={"service": "epochdeck", "version": "0.1.0", "status": "healthy"},
        )

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        health = client.health()

    assert health.service == "epochdeck"
    assert health.status == "healthy"


def test_http_basic_auth_from_environment_works_with_mock_transport(monkeypatch) -> None:
    monkeypatch.setenv("EPOCHDECK_HTTP_USERNAME", "proxy-user")
    monkeypatch.setenv("EPOCHDECK_HTTP_PASSWORD", "proxy-password")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Basic cHJveHktdXNlcjpwcm94eS1wYXNzd29yZA=="
        return httpx.Response(
            200,
            json={"service": "epochdeck", "version": "0.1.0", "status": "healthy"},
        )

    with EpochDeckClient(
        "https://epochdeck.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        assert client.server_url == "https://epochdeck.test"
        client.health()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("EPOCHDECK_HTTP_USERNAME", "proxy-user"),
        ("EPOCHDECK_HTTP_PASSWORD", "proxy-password"),
    ],
)
def test_http_basic_auth_rejects_partial_environment(
    monkeypatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.delenv("EPOCHDECK_HTTP_USERNAME", raising=False)
    monkeypatch.delenv("EPOCHDECK_HTTP_PASSWORD", raising=False)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match="must be set together"):
        EpochDeckClient(transport=httpx.MockTransport(lambda _: httpx.Response(500)))


def test_server_url_rejects_embedded_credentials() -> None:
    with pytest.raises(ValueError, match="server_url must not contain credentials"):
        EpochDeckClient("https://proxy-user:proxy-password@epochdeck.test")


def test_project_detail_validates_identity_and_opaque_mutation_token() -> None:
    responses = iter(
        [
            {"name": "demo", "mutation_token": "184467440737095516160"},
            {"name": "other", "mutation_token": "2"},
            {"name": "demo", "mutation_token": 3},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/projects/demo"
        return httpx.Response(200, json=next(responses))

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        assert client.get_project("demo")["mutation_token"] == "184467440737095516160"
        with pytest.raises(DeliveryProtocolError, match="wrong project name"):
            client.get_project("demo")
        with pytest.raises(DeliveryProtocolError, match="mutation_token"):
            client.get_project("demo")


def test_durable_writes_reject_malformed_success_acknowledgements() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={})

    batch = {
        "batch_sequence": 1,
        "points": [{"sequence": 1, "step": 0, "timestamp_ms": 1, "metrics": {"x": 1}}],
    }
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(DeliveryProtocolError, match="run object"):
            client.create_run(
                project="demo",
                run_id="run-1",
                name=None,
                config={},
                resume="never",
            )
        with pytest.raises(DeliveryProtocolError, match="run object"):
            client.finish_run("run-1", {})
        with pytest.raises(DeliveryProtocolError, match="run_id"):
            client.ingest_batch("run-1", batch)
        with pytest.raises(DeliveryProtocolError, match="blob object"):
            source = Path(__file__)
            client.upload_blob(
                source,
                {
                    "digest": "0" * 64,
                    "size": source.stat().st_size,
                    "mime_type": "text/plain",
                },
            )


def test_explicit_artifact_version_is_validated_and_acknowledged_exactly() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.read())
        return httpx.Response(
            201,
            json={
                "artifact": {"id": body["id"], "version": body["version"] - 1},
                "duplicate": False,
            },
        )

    artifact = {"id": "artifact-1", "version": 3}
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(DeliveryProtocolError, match="wrong explicit version"):
            client.create_artifact("run-1", artifact)
        for invalid in (True, -1, 1 << 53, 1.0):
            with pytest.raises(ValueError, match="artifact version"):
                client.create_artifact("run-1", {"id": "artifact-1", "version": invalid})

    assert len(requests) == 1


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

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.history("run-id", keys=["loss"], limit=250, after=10)
        client.history("run-id", keys=["loss"], max_points=500)
        with pytest.raises(ValueError, match="cannot combine"):
            client.history("run-id", keys=["loss"], limit=10, max_points=10)

    assert list(requests[0].url.params.multi_items()) == [
        ("key", "loss"),
        ("limit", "250"),
        ("after", "10"),
    ]
    assert list(requests[1].url.params.multi_items()) == [
        ("key", "loss"),
        ("max_points", "500"),
    ]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"keys": []}, "1 to 32"),
        ({"keys": [f"metric-{index}" for index in range(33)]}, "1 to 32"),
        ({"keys": ["loss", "loss"]}, "must be unique"),
        ({"keys": ["loss"], "limit": 0}, "limit must be between"),
        ({"keys": ["loss"], "limit": 5_001}, "limit must be between"),
        ({"keys": ["loss"], "limit": True}, "limit must be between"),
        (
            {"keys": ["loss", "reward"], "max_points": 3},
            "between 4 and 5000",
        ),
        ({"keys": ["loss"], "max_points": 5_001}, "between 2 and 5000"),
        ({"keys": ["loss"], "after": -1}, "after must be between"),
    ],
)
def test_history_rejects_requests_outside_the_http_bounds(kwargs, message) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("invalid history request reached the transport")

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(ValueError, match=message),
    ):
        client.history("run-id", **kwargs)

    assert requests == []


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"step": []}, "columns are not aligned"),
        ({"metrics": {"loss": []}}, "columns are not aligned"),
        ({"metrics": {"reward": [1.0]}}, "do not match the request"),
        ({"sequence": [True]}, "invalid sequence column"),
        ({"metrics": {"loss": ["not-a-number"]}}, "invalid metric value"),
        ({"next_after": 1}, "invalid cursor"),
    ],
)
def test_history_rejects_malformed_success_responses(updates, message) -> None:
    payload = {
        "run_id": "run-1",
        "sequence": [2],
        "step": [1],
        "timestamp_ms": [1000],
        "metrics": {"loss": [0.5]},
        "next_after": 2,
        "sampled": False,
        "source_points": None,
        "source_last_sequence": 2,
    }
    payload.update(updates)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(DeliveryProtocolError, match=message),
    ):
        client.history("run-1", keys=["loss"], after=1, limit=1)


def test_history_rejects_a_stalled_page_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "run_id": "run-1",
                "sequence": [10],
                "step": [1],
                "timestamp_ms": [1000],
                "metrics": {"loss": [0.5]},
                "next_after": 10,
                "sampled": False,
                "source_points": None,
                "source_last_sequence": 10,
            },
        )

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(DeliveryProtocolError, match="did not advance"),
    ):
        client.history("run-1", keys=["loss"], after=10, limit=1)


def test_chart_clients_use_bounded_single_and_multi_run_contracts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"series": []})

    overlay = {
        "alignment": "relative_step",
        "viewport": {"minimum": 0, "maximum": 10_000},
        "max_buckets": 512,
        "series": [
            {"run_id": "run-1", "key": "train/loss"},
            {"run_id": "run-2", "key": "train/loss"},
        ],
    }
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.chart_history(
            "run/id",
            keys=["train/loss", "reward"],
            max_buckets=512,
            step_min=100,
            step_max=200,
        )
        client.overlay_chart_history("robot learning", overlay)

    assert requests[0].method == "GET"
    assert requests[0].url.path == "/api/v1/runs/run/id/chart-history"
    assert list(requests[0].url.params.multi_items()) == [
        ("key", "train/loss"),
        ("key", "reward"),
        ("max_buckets", "512"),
        ("step_min", "100"),
        ("step_max", "200"),
    ]
    assert requests[1].method == "POST"
    assert requests[1].url.path == "/api/v1/projects/robot learning/chart-history/query"
    assert json.loads(requests[1].read()) == overlay


def test_metric_discovery_uses_a_lexicographic_cursor() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"run_id": "run-1", "keys": ["reward"], "next_after": None},
        )

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.metric_keys("run-1", after="loss,raw", limit=50)

    assert dict(requests[0].url.params) == {"after": "loss,raw", "limit": "50"}


def test_lightweight_collection_and_full_detail_routes() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={})

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.projects(before="project-2", limit=25)
        client.runs("demo/project", before="run-2", q="eval run", limit=20)
        client.rich_value_keys("run/id", after="rollout/train", limit=15)
        client.rich_values(
            "run/id",
            key="rollout/train",
            before="value-2",
            limit=10,
        )
        client.get_rich_value("value/id")
        client.get_artifact("artifact/id")
        client.artifact_lineage(
            "artifact/id",
            relation="input",
            before="run-2",
            limit=5,
        )
        client.get_trace_span("span/id")
        client.run_artifacts(
            "run/id",
            before="artifact-2",
            before_relation="output",
            limit=4,
        )
        with pytest.raises(ValueError, match="requires both"):
            client.run_artifacts("run/id", before="artifact-2")

    assert requests[0].url.path == "/api/v1/projects"
    assert dict(requests[0].url.params) == {"limit": "25", "before": "project-2"}
    assert requests[1].url.path == "/api/v1/projects/demo/project/runs"
    assert dict(requests[1].url.params) == {
        "limit": "20",
        "before": "run-2",
        "q": "eval run",
    }
    assert requests[2].url.path == "/api/v1/runs/run/id/rich-values/keys"
    assert dict(requests[2].url.params) == {"limit": "15", "after": "rollout/train"}
    assert requests[3].url.path == "/api/v1/runs/run/id/rich-values"
    assert dict(requests[3].url.params) == {
        "key": "rollout/train",
        "limit": "10",
        "before": "value-2",
    }
    assert requests[4].url.path == "/api/v1/rich-values/value/id"
    assert requests[5].url.path == "/api/v1/artifacts/artifact/id"
    assert requests[6].url.path == "/api/v1/artifacts/artifact/id/lineage"
    assert dict(requests[6].url.params) == {
        "limit": "5",
        "before": "run-2",
        "relation": "input",
    }
    assert requests[7].url.path == "/api/v1/traces/span/id"
    assert requests[8].url.path == "/api/v1/runs/run/id/artifacts"
    assert dict(requests[8].url.params) == {
        "limit": "4",
        "before": "artifact-2",
        "before_relation": "output",
    }


def test_document_updates_use_explicit_patch_contracts() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"run": {"config": {}, "summary": {}}})

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.update_config("run/id", {"seed": 2}, allow_val_change=True)
        client.update_summary("run/id", {"status": "complete"})

    assert requests[0].method == "PATCH"
    assert requests[0].url.raw_path == b"/api/v1/runs/run%2Fid/config"
    assert requests[0].read() == b'{"updates":{"seed":2},"allow_val_change":true}'
    assert requests[1].method == "PATCH"
    assert requests[1].url.raw_path == b"/api/v1/runs/run%2Fid/summary"
    assert requests[1].read() == b'{"updates":{"status":"complete"}}'


def test_ingest_quotes_the_run_identifier() -> None:
    requests: list[httpx.Request] = []
    batch = {
        "batch_sequence": 1,
        "points": [{"sequence": 1, "step": 0, "timestamp_ms": 1, "metrics": {"x": 1.0}}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            201,
            json={
                "run_id": "run/id",
                "batch_sequence": 1,
                "accepted_points": 1,
                "duplicate": False,
                "metric_revision": 1,
                "stop_requested": False,
            },
        )

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        client.ingest_batch("run/id", batch)

    assert requests[0].url.raw_path == b"/api/v1/runs/run%2Fid/batches"
    assert requests[0].content == encode_json_request(batch)


def test_get_run_uses_the_public_lifecycle_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/v1/runs/run-id"
        return httpx.Response(200, json={"id": "run-id", "state": "finished"})

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
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
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
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
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
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
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
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
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
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


def test_sweep_detail_routes_quote_and_validate_record_identity() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.startswith("/api/v1/sweeps/"):
            return httpx.Response(200, json={"id": "sweep/id"})
        return httpx.Response(
            200,
            json={"id": "trial/id", "sweep_id": "sweep/id"},
        )

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        assert client.get_sweep("sweep/id")["id"] == "sweep/id"
        assert client.get_sweep_trial("trial/id")["id"] == "trial/id"

    assert requests[0].url.raw_path == b"/api/v1/sweeps/sweep%2Fid"
    assert requests[1].url.raw_path == b"/api/v1/sweep-trials/trial%2Fid"


@pytest.mark.parametrize(
    ("method", "payload", "message"),
    [
        ("get_sweep", {"id": "other"}, "wrong sweep ID"),
        (
            "get_sweep_trial",
            {"id": "other", "sweep_id": "sweep-id"},
            "wrong trial ID",
        ),
        ("get_sweep_trial", {"id": "record-id"}, "sweep_id"),
    ],
)
def test_sweep_detail_routes_reject_malformed_success_records(
    method: str,
    payload: dict[str, object],
    message: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(DeliveryProtocolError, match=message),
    ):
        getattr(client, method)("record-id")


@pytest.mark.parametrize(
    "file_name",
    ["", "nested/file.bin", "nested\\file.bin", "bad\nname.bin", "bad\u0085name.bin", "가" * 171],
)
def test_upload_blob_rejects_an_invalid_file_name_before_transport(
    tmp_path: Path,
    file_name: str,
) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("invalid blob descriptor reached the transport")

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises((TypeError, ValueError), match="file_name"),
    ):
        client.upload_blob(
            source,
            {
                "digest": "0" * 64,
                "size": source.stat().st_size,
                "mime_type": "application/octet-stream",
                "file_name": file_name,
            },
        )

    assert requests == []


@pytest.mark.parametrize(
    ("file_name", "encoded_file_name"),
    [
        ("모델.bin", "%EB%AA%A8%EB%8D%B8.bin"),
        ("episode 1.mp4", "episode%201.mp4"),
    ],
)
def test_upload_blob_percent_encodes_and_validates_file_name_round_trip(
    tmp_path: Path,
    file_name: str,
    encoded_file_name: str,
) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    digest = "0" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-epochdeck-file-name"] == encoded_file_name
        assert request.read() == b"payload"
        return httpx.Response(
            201,
            json={
                "blob": {
                    "digest": digest,
                    "size": len(b"payload"),
                    "mime_type": "application/octet-stream",
                    "file_name": file_name,
                },
                "duplicate": False,
            },
        )

    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        response = client.upload_blob(
            source,
            {
                "digest": digest,
                "size": source.stat().st_size,
                "mime_type": "application/octet-stream",
                "file_name": file_name,
            },
        )

    assert response["blob"]["file_name"] == file_name


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("mime_type", "text/plain", "wrong MIME type"),
        ("file_name", "other.bin", "wrong file name"),
    ],
)
def test_upload_blob_rejects_a_mismatched_descriptor_acknowledgement(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    digest = "0" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        acknowledged = {
            "digest": digest,
            "size": len(b"payload"),
            "mime_type": "application/octet-stream",
            "file_name": "payload.bin",
        }
        acknowledged[field] = value
        return httpx.Response(201, json={"blob": acknowledged, "duplicate": False})

    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(DeliveryProtocolError, match=message),
    ):
        client.upload_blob(
            source,
            {
                "digest": digest,
                "size": source.stat().st_size,
                "mime_type": "application/octet-stream",
                "file_name": "payload.bin",
            },
        )
