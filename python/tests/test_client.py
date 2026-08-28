import httpx

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
