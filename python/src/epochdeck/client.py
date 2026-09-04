from __future__ import annotations

import math
import os
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import httpx

from epochdeck._limits import MAX_SAFE_INTEGER
from epochdeck._protocol import (
    DeliveryProtocolError,
    encode_json_request,
    require_bool,
    require_nonnegative_int,
    require_object,
    require_text,
    required_request_identity,
    validate_blob_file_name,
    validate_ingest_ack,
    validate_record_ack,
    validate_run_identity,
)


@dataclass(frozen=True, slots=True)
class Health:
    service: str
    version: str
    status: Literal["healthy", "unhealthy"]


class EpochDeckApiError(RuntimeError):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(f"EpochDeck API error {status_code} ({code}): {message}")
        self.status_code = status_code
        self.code = code
        self.message = message


class EpochDeckClient:
    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8787",
        *,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.server_url = _normalize_server_url(server_url)
        self._client = httpx.Client(
            base_url=self.server_url,
            timeout=timeout,
            transport=transport,
            auth=_http_basic_auth_from_environment(),
        )

    def __enter__(self) -> EpochDeckClient:
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

    def diagnostics(self) -> dict[str, Any]:
        return self._request("GET", "/api/v1/diagnostics")

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
        response = self._request(
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
        validate_run_identity(response, run_id)
        require_bool(response, "resumed")
        return response

    def create_sweep(self, project: str, sweep: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/sweeps",
            json=sweep,
        )

    def sweeps(
        self,
        project: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/sweeps",
            params=_cursor_params(before, limit),
        )

    def get_sweep(self, sweep_id: str) -> dict[str, Any]:
        response = self._request("GET", f"/api/v1/sweeps/{quote(sweep_id, safe='')}")
        if require_text(response, "id") != sweep_id:
            raise DeliveryProtocolError("sweep detail has the wrong sweep ID")
        return response

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
        agent_id: str,
        state: str,
        metric: float | None,
    ) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/sweep-trials/{quote(trial_id, safe='')}/complete",
            json={"agent_id": agent_id, "state": state, "metric": metric},
        )
        if require_text(response, "id") != trial_id:
            raise DeliveryProtocolError("sweep completion has the wrong trial ID")
        if require_text(response, "agent_id") != agent_id:
            raise DeliveryProtocolError("sweep completion has the wrong agent ID")
        if require_text(response, "state") != state:
            raise DeliveryProtocolError("sweep completion has the wrong terminal state")
        return response

    def heartbeat_sweep_trial(self, trial_id: str, agent_id: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/sweep-trials/{quote(trial_id, safe='')}/heartbeat",
            json={"agent_id": agent_id},
        )
        if require_text(response, "id") != trial_id:
            raise DeliveryProtocolError("sweep heartbeat has the wrong trial ID")
        if require_text(response, "agent_id") != agent_id:
            raise DeliveryProtocolError("sweep heartbeat has the wrong agent ID")
        return response

    def sweep_trials(
        self,
        sweep_id: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/sweeps/{quote(sweep_id, safe='')}/trials",
            params=_cursor_params(before, limit),
        )

    def get_sweep_trial(self, trial_id: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/v1/sweep-trials/{quote(trial_id, safe='')}",
        )
        if require_text(response, "id") != trial_id:
            raise DeliveryProtocolError("sweep-trial detail has the wrong trial ID")
        require_text(response, "sweep_id")
        return response

    def create_report(self, project: str, report: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/reports",
            json=report,
        )

    def reports(
        self,
        project: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/reports",
            params=_cursor_params(before, limit),
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
        encoded = encode_json_request(batch)
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/batches",
            content=encoded,
            headers={"content-type": "application/json"},
        )
        validate_ingest_ack(response, run_id=run_id, batch=batch)
        return response

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/runs/{quote(run_id, safe='')}")

    def runs(
        self,
        project: str,
        *,
        before: str | None = None,
        q: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params = _cursor_params(before, limit)
        if q is not None:
            params["q"] = q
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/runs",
            params=params,
        )

    def metric_keys(
        self,
        run_id: str,
        *,
        after: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit}
        if after is not None:
            params["after"] = after
        response = self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/metrics",
            params=params,
        )
        if require_text(response, "run_id") != run_id:
            raise DeliveryProtocolError("metric-key response has the wrong run ID")
        keys = response.get("keys")
        if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
            raise DeliveryProtocolError("metric-key response has no string key list")
        next_after = response.get("next_after")
        if next_after is not None and not isinstance(next_after, str):
            raise DeliveryProtocolError("metric-key response has an invalid cursor")
        return response

    def projects(
        self,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return self._request("GET", "/api/v1/projects", params=_cursor_params(before, limit))

    def get_project(self, project: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}",
        )
        if require_text(response, "name") != project:
            raise DeliveryProtocolError("project detail has the wrong project name")
        require_text(response, "mutation_token")
        return response

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
            f"/api/v1/runs/{quote(run_id, safe='')}/config",
            json={"updates": updates, "allow_val_change": allow_val_change},
        )

    def update_summary(self, run_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/api/v1/runs/{quote(run_id, safe='')}/summary",
            json={"updates": updates},
        )

    def finish_run(self, run_id: str, summary: dict[str, Any]) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/finish",
            json={"summary": summary},
        )
        validate_run_identity(response, run_id)
        run = require_object(response, "run")
        if require_text(run, "state") != "finished":
            raise DeliveryProtocolError("run finish response is not finished")
        return response

    def create_alert(self, run_id: str, alert: dict[str, Any]) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/alerts",
            json=alert,
        )
        validate_record_ack(
            response,
            field="alert",
            identity_field="id",
            expected_identity=required_request_identity(alert, "alert"),
        )
        return response

    def alerts(
        self,
        run_id: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit}
        if before is not None:
            params["before"] = before
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/alerts",
            params=params,
        )

    def upload_blob(self, path: Path, blob: dict[str, Any]) -> dict[str, Any]:
        file_name = validate_blob_file_name(blob.get("file_name"))
        headers = {
            "content-type": str(blob["mime_type"]),
            "content-length": str(blob["size"]),
        }
        if file_name is not None:
            headers["x-epochdeck-file-name"] = quote(file_name, safe="")
        with path.open("rb") as stream:
            response = self._request(
                "PUT",
                f"/api/v1/blobs/{quote(str(blob['digest']), safe='')}",
                headers=headers,
                content=stream,
            )
        validate_record_ack(
            response,
            field="blob",
            identity_field="digest",
            expected_identity=str(blob["digest"]),
        )
        acknowledged = require_object(response, "blob")
        if require_nonnegative_int(acknowledged, "size") != int(blob["size"]):
            raise DeliveryProtocolError("blob acknowledgement has the wrong size")
        if require_text(acknowledged, "mime_type") != str(blob["mime_type"]):
            raise DeliveryProtocolError("blob acknowledgement has the wrong MIME type")
        if acknowledged.get("file_name") != file_name:
            raise DeliveryProtocolError("blob acknowledgement has the wrong file name")
        return response

    def download_blob(self, digest: str, destination: Path) -> int:
        size = 0
        with self._client.stream(
            "GET",
            f"/api/v1/blobs/{quote(digest, safe='')}",
        ) as response:
            self._raise_for_status(response)
            with destination.open("wb") as stream:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    stream.write(chunk)
                    size += len(chunk)
        return size

    def create_rich_value(self, run_id: str, value: dict[str, Any]) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/rich-values",
            json=value,
        )
        validate_record_ack(
            response,
            field="value",
            identity_field="id",
            expected_identity=required_request_identity(value, "rich value"),
        )
        return response

    def rich_values(
        self,
        run_id: str,
        *,
        key: str,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"key": key, "limit": limit}
        if before is not None:
            params["before"] = before
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/rich-values",
            params=params,
        )

    def rich_value_keys(
        self,
        run_id: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit}
        if after is not None:
            params["after"] = after
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/rich-values/keys",
            params=params,
        )

    def get_rich_value(self, value_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/rich-values/{quote(value_id, safe='')}",
        )

    def create_artifact(self, run_id: str, artifact: dict[str, Any]) -> dict[str, Any]:
        requested_version = artifact.get("version")
        if "version" in artifact and (
            isinstance(requested_version, bool)
            or not isinstance(requested_version, int)
            or requested_version < 0
            or requested_version > MAX_SAFE_INTEGER
        ):
            raise ValueError(
                f"artifact version must be an integer between 0 and {MAX_SAFE_INTEGER}"
            )
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts",
            json=artifact,
        )
        validate_record_ack(
            response,
            field="artifact",
            identity_field="id",
            expected_identity=required_request_identity(artifact, "artifact"),
        )
        acknowledged_artifact = require_object(response, "artifact")
        acknowledged_version = require_nonnegative_int(acknowledged_artifact, "version")
        if acknowledged_version > MAX_SAFE_INTEGER:
            raise DeliveryProtocolError(
                "artifact acknowledgement has a version outside the JSON-safe integer range"
            )
        if "version" in artifact and acknowledged_version != requested_version:
            raise DeliveryProtocolError("artifact acknowledgement has the wrong explicit version")
        return response

    def use_artifact(self, run_id: str, artifact_id: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts/use",
            json={"artifact_id": artifact_id},
        )
        if require_text(response, "id") != artifact_id:
            raise DeliveryProtocolError("artifact-use acknowledgement has the wrong artifact ID")
        return response

    def resolve_artifact(self, project: str, name: str, alias: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/artifacts/"
            f"{quote(name, safe='')}/aliases/{quote(alias, safe='')}",
        )

    def project_artifacts(
        self,
        project: str,
        *,
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/projects/{quote(project, safe='')}/artifacts",
            params=_cursor_params(before, limit),
        )

    def get_artifact(self, artifact_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/artifacts/{quote(artifact_id, safe='')}",
        )

    def artifact_lineage(
        self,
        artifact_id: str,
        *,
        relation: Literal["input", "output"],
        before: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params = _cursor_params(before, limit)
        params["relation"] = relation
        return self._request(
            "GET",
            f"/api/v1/artifacts/{quote(artifact_id, safe='')}/lineage",
            params=params,
        )

    def run_artifacts(
        self,
        run_id: str,
        *,
        before: str | None = None,
        before_relation: Literal["input", "output"] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if (before is None) != (before_relation is None):
            raise ValueError("artifact link cursor requires both before and before_relation")
        params = _cursor_params(before, limit)
        if before_relation is not None:
            params["before_relation"] = before_relation
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/artifacts",
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
        _validate_history_request(
            keys=keys,
            after=after,
            limit=limit,
            max_points=max_points,
        )
        params: list[tuple[str, str | int]] = [("key", key) for key in keys]
        if max_points is not None:
            params.append(("max_points", max_points))
        else:
            params.append(("limit", 1_000 if limit is None else limit))
        if after is not None:
            params.append(("after", after))
        response = self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/history",
            params=params,
        )
        _validate_history_response(
            response,
            run_id=run_id,
            keys=keys,
            after=after,
            sampled_request=max_points is not None,
        )
        return response

    def chart_history(
        self,
        run_id: str,
        *,
        keys: list[str],
        max_buckets: int | None = None,
        step_min: int | None = None,
        step_max: int | None = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, str | int]] = [("key", key) for key in keys]
        if max_buckets is not None:
            params.append(("max_buckets", max_buckets))
        if step_min is not None:
            params.append(("step_min", step_min))
        if step_max is not None:
            params.append(("step_max", step_max))
        return self._request(
            "GET",
            f"/api/v1/runs/{quote(run_id, safe='')}/chart-history",
            params=params,
        )

    def overlay_chart_history(
        self,
        project: str,
        query: dict[str, Any],
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/projects/{quote(project, safe='')}/chart-history/query",
            json=query,
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._client.request(method, path, **kwargs)
        self._raise_for_status(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise EpochDeckApiError(
                response.status_code, "invalid_response", "expected JSON object"
            )
        return payload

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.is_error:
            response.read()
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise EpochDeckApiError(
                response.status_code,
                str(payload.get("code", "http_error")),
                str(payload.get("message", response.reason_phrase)),
            )


def _normalize_server_url(server_url: str) -> str:
    normalized = server_url.rstrip("/")
    parsed = httpx.URL(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.host:
        raise ValueError("server_url must be an absolute HTTP or HTTPS URL")
    if parsed.userinfo:
        raise ValueError(
            "server_url must not contain credentials; use EPOCHDECK_HTTP_USERNAME and "
            "EPOCHDECK_HTTP_PASSWORD"
        )
    return normalized


def _http_basic_auth_from_environment() -> httpx.BasicAuth | None:
    username = os.environ.get("EPOCHDECK_HTTP_USERNAME")
    password = os.environ.get("EPOCHDECK_HTTP_PASSWORD")
    if (username is None) != (password is None):
        raise ValueError("EPOCHDECK_HTTP_USERNAME and EPOCHDECK_HTTP_PASSWORD must be set together")
    if username is None:
        return None
    assert password is not None
    return httpx.BasicAuth(username, password)


def _cursor_params(before: str | None, limit: int) -> dict[str, str | int]:
    params: dict[str, str | int] = {"limit": limit}
    if before is not None:
        params["before"] = before
    return params


def _validate_history_request(
    *,
    keys: list[str],
    after: int | None,
    limit: int | None,
    max_points: int | None,
) -> None:
    if not 1 <= len(keys) <= 32 or any(not isinstance(key, str) or not key for key in keys):
        raise ValueError("history requires 1 to 32 non-empty metric keys")
    if len(set(keys)) != len(keys):
        raise ValueError("history metric keys must be unique")
    if limit is not None and max_points is not None:
        raise ValueError("history cannot combine limit and max_points")
    if after is not None and (
        isinstance(after, bool) or not isinstance(after, int) or not 0 <= after <= MAX_SAFE_INTEGER
    ):
        raise ValueError(f"history after must be between 0 and {MAX_SAFE_INTEGER}")
    if max_points is not None:
        minimum = len(keys) * 2
        if (
            isinstance(max_points, bool)
            or not isinstance(max_points, int)
            or not minimum <= max_points <= 5_000
        ):
            raise ValueError(
                f"history max_points must be between {minimum} and 5000 "
                f"for {len(keys)} metric key(s)"
            )
        return
    effective_limit = 1_000 if limit is None else limit
    if (
        isinstance(effective_limit, bool)
        or not isinstance(effective_limit, int)
        or not 1 <= effective_limit <= 5_000
    ):
        raise ValueError("history limit must be between 1 and 5000")


def _validate_history_response(
    response: dict[str, Any],
    *,
    run_id: str,
    keys: list[str],
    after: int | None,
    sampled_request: bool,
) -> None:
    if require_text(response, "run_id") != run_id:
        raise DeliveryProtocolError("history response has the wrong run ID")
    sequence = _history_integer_column(response, "sequence")
    step = _history_integer_column(response, "step")
    timestamp_ms = _history_integer_column(response, "timestamp_ms")
    row_count = len(sequence)
    if len(step) != row_count or len(timestamp_ms) != row_count:
        raise DeliveryProtocolError("history response columns are not aligned")
    if any(current <= previous for previous, current in pairwise(sequence)):
        raise DeliveryProtocolError("history response sequences are not strictly increasing")
    if after is not None and sequence and sequence[0] <= after:
        raise DeliveryProtocolError("history response did not advance beyond its cursor")

    metrics = response.get("metrics")
    if not isinstance(metrics, dict) or set(metrics) != set(keys):
        raise DeliveryProtocolError("history response metric columns do not match the request")
    for key in keys:
        values = metrics[key]
        if not isinstance(values, list) or len(values) != row_count:
            raise DeliveryProtocolError("history response columns are not aligned")
        for value in values:
            if value is None:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise DeliveryProtocolError("history response contains an invalid metric value")

    sampled = response.get("sampled")
    if not isinstance(sampled, bool) or sampled != sampled_request:
        raise DeliveryProtocolError("history response has an invalid sampling mode")
    _optional_history_position(response, "source_points")
    _optional_history_position(response, "source_last_sequence")

    next_after = response.get("next_after")
    if next_after is None:
        return
    if (
        isinstance(next_after, bool)
        or not isinstance(next_after, int)
        or not 0 <= next_after <= MAX_SAFE_INTEGER
    ):
        raise DeliveryProtocolError("history response has an invalid cursor")
    if sampled_request or not sequence or next_after != sequence[-1]:
        raise DeliveryProtocolError("history response has an invalid cursor")
    if after is not None and next_after <= after:
        raise DeliveryProtocolError("history response cursor did not advance")


def _history_integer_column(response: dict[str, Any], field: str) -> list[int]:
    values = response.get(field)
    if not isinstance(values, list):
        raise DeliveryProtocolError(f"history response has no {field} column")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAFE_INTEGER
        for value in values
    ):
        raise DeliveryProtocolError(f"history response has an invalid {field} column")
    return values


def _optional_history_position(response: dict[str, Any], field: str) -> int | None:
    value = response.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAFE_INTEGER:
        raise DeliveryProtocolError(f"history response has an invalid {field}")
    return value
