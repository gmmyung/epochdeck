from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
        sweep_trial_id: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/runs",
            json={
                "id": run_id,
                "name": name,
                "config": config,
                "resume": resume,
                "sweep_trial_id": sweep_trial_id,
            },
        )

    def create_sweep(self, project: str, sweep: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/sweeps",
            json=sweep,
        )

    def get_sweep(self, sweep_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/sweeps/{quote(sweep_id, safe='')}")

    def claim_sweep_trial(self, sweep_id: str, agent_id: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/sweeps/{quote(sweep_id, safe='')}/claim",
            json={"agent_id": agent_id},
        )

    def complete_sweep_trial(
        self,
        trial_id: str,
        *,
        state: str,
        metric: float | None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/sweep-trials/{quote(trial_id, safe='')}/complete",
            json={"state": state, "metric": metric},
        )

    def sweep_trials(self, sweep_id: str, *, limit: int = 100) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/sweeps/{quote(sweep_id, safe='')}/trials",
            params={"limit": limit},
        )

    def create_report(self, project: str, report: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/reports",
            json=report,
        )

    def reports(self, project: str, *, limit: int = 100) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/reports",
            params={"limit": limit},
        )

    def get_report(self, report_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/reports/{quote(report_id, safe='')}")

    def update_report(self, report_id: str, report: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "PUT",
            f"/api/v1/reports/{quote(report_id, safe='')}",
            json=report,
        )

    def delete_report(self, report_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/api/v1/reports/{quote(report_id, safe='')}")

    def ingest_batch(self, run_id: str, batch: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/v1/runs/{run_id}/batches", json=batch)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/runs/{run_id}")

    def projects(self, *, limit: int = 100) -> dict[str, Any]:
        return self._request("GET", "/api/v1/projects", params={"limit": limit})

    def query_runs(self, query: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/v1/query/runs", json=query)

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

    def create_alert(self, run_id: str, alert: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/alerts",
            json=alert,
        )

    def alerts(
        self,
        run_id: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params = {"limit": limit}
        if before is not None:
            params["before"] = before
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/alerts",
            params=params,
        )

    def upload_blob(self, path: Path, blob: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "content-type": str(blob["mime_type"]),
            "content-length": str(blob["size"]),
        }
        with path.open("rb") as stream:
            return self._request(
                "PUT",
                f"/api/v1/blobs/{quote(str(blob['digest']), safe='')}",
                headers=headers,
                content=stream,
            )

    def create_rich_value(self, run_id: str, value: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/rich-values",
            json=value,
        )

    def rich_values(
        self,
        run_id: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params = {"limit": limit}
        if before is not None:
            params["before"] = before
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/rich-values",
            params=params,
        )

    def create_artifact(self, run_id: str, artifact: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts",
            json=artifact,
        )

    def use_artifact(self, run_id: str, artifact_id: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts/use",
            json={"artifact_id": artifact_id},
        )

    def resolve_artifact(self, project: str, name: str, alias: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/artifacts/"
            f"{quote(name, safe='')}/aliases/{quote(alias, safe='')}",
        )

    def run_artifacts(self, run_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts",
        )

    def create_trace_span(self, run_id: str, span: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/traces",
            json=span,
        )

    def trace_spans(
        self,
        run_id: str,
        *,
        q: str | None = None,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit}
        if q is not None:
            params["q"] = q
        if before is not None:
            params["before"] = before
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/traces",
            params=params,
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
