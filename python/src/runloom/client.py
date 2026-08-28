from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

import httpx


@dataclass(frozen=True, slots=True)
class Health:
    service: str
    version: str
    status: Literal["healthy", "unhealthy"]


class RunloomApiError(RuntimeError):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(f"Runloom API error {status_code} ({code}): {message}")
        self.status_code = status_code
        self.code = code
        self.message = message


class RunloomClient:
    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8787",
        *,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.server_url,
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
        payload = self._request("GET", "/api/v1/health")
        return Health(
            service=str(payload["service"]),
            version=str(payload["version"]),
            status=payload["status"],
        )

    def create_run(
        self,
        *,
        project: str,
        run_id: str,
        name: str | None,
        config: dict[str, Any],
        resume: str,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/runs",
            json={"id": run_id, "name": name, "config": config, "resume": resume},
        )

    def ingest_batch(self, run_id: str, batch: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/v1/runs/{run_id}/batches", json=batch)

    def update_config(
        self,
        run_id: str,
        updates: dict[str, Any],
        *,
        allow_val_change: bool = False,
    ) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/api/v1/runs/{run_id}/config",
            json={"updates": updates, "allow_val_change": allow_val_change},
        )

    def update_summary(self, run_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/api/v1/runs/{run_id}/summary",
            json={"updates": updates},
        )

    def finish_run(self, run_id: str, summary: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{run_id}/finish",
            json={"summary": summary},
        )

    def history(
        self,
        run_id: str,
        *,
        keys: list[str],
        after: int | None = None,
        limit: int | None = None,
        max_points: int | None = None,
    ) -> dict[str, Any]:
        if limit is not None and max_points is not None:
            raise ValueError("history cannot combine limit and max_points")
        params: dict[str, str | int] = {"keys": ",".join(keys)}
        if max_points is not None:
            params["max_points"] = max_points
        else:
            params["limit"] = 1_000 if limit is None else limit
        if after is not None:
            params["after"] = after
        return self._request("GET", f"/api/v1/runs/{run_id}/history", params=params)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._client.request(method, path, **kwargs)
        if response.is_error:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise RunloomApiError(
                response.status_code,
                str(payload.get("code", "http_error")),
                str(payload.get("message", response.reason_phrase)),
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise RunloomApiError(response.status_code, "invalid_response", "expected JSON object")
        return payload
