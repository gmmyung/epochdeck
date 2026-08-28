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
