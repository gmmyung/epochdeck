from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import httpx


@dataclass(frozen=True, slots=True)
class Health:
    service: str
    version: str
    status: Literal["healthy", "unhealthy"]


class RunloomClient:
    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8787",
        *,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=server_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> RunloomClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def health(self) -> Health:
        response = self._client.get("/api/v1/health")
        response.raise_for_status()
        payload = response.json()
        return Health(
            service=str(payload["service"]),
            version=str(payload["version"]),
            status=payload["status"],
        )
