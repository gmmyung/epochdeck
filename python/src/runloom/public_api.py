from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from runloom.client import RunloomClient


class Api:
    def __init__(
        self,
        *,
        server_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.client = RunloomClient(
            server_url or os.environ.get("RUNLOOM_SERVER_URL", "http://127.0.0.1:8787"),
            timeout=timeout,
        )

    def __enter__(self) -> Api:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()

    def projects(self, *, per_page: int = 100) -> list[dict[str, Any]]:
        _validate_page_size(per_page)
        return deepcopy(self.client.projects(limit=per_page)["projects"])

    def run(self, path: str) -> PublicRun:
        project, run_id = _parse_run_path(path)
        record = self.client.get_run(run_id)
        if project is not None and record.get("project") != project:
            raise ValueError(f"run {run_id} does not belong to project {project!r}")
        return PublicRun(self.client, record)

    def runs(
        self,
        path: str | None = None,
        *,
        filters: Mapping[str, Any] | None = None,
        order: str = "-created_at",
        per_page: int = 100,
    ) -> RunCollection:
        _validate_page_size(per_page)
        if order != "-created_at":
            raise ValueError("the current public API supports only order='-created_at'")
        query = _compile_filters(filters or {})
        if path is not None:
            project = path.strip("/")
            if not project or "/" in project:
                raise ValueError("run collection path must be a project name")
            if query.get("project") not in {None, project}:
                raise ValueError("project path conflicts with the project filter")
            query["project"] = project
        query["limit"] = per_page
        return RunCollection(self.client, query)


class RunCollection:
    def __init__(self, client: RunloomClient, query: dict[str, Any]) -> None:
        self._client = client
        self._query = deepcopy(query)

    def __iter__(self) -> Iterator[PublicRun]:
        query = deepcopy(self._query)
        while True:
            response = self._client.query_runs(query)
            records = response.get("runs")
            if not isinstance(records, list):
                raise TypeError("Runloom query response has no run list")
            for record in records:
                if not isinstance(record, dict):
                    raise TypeError("Runloom query response contains an invalid run")
                yield PublicRun(self._client, record)
            cursor = response.get("next_before")
            if cursor is None:
                return
            query["before"] = str(cursor)


@dataclass(frozen=True, slots=True)
class PublicRun:
    _client: RunloomClient
    _record: dict[str, Any]

    @property
    def id(self) -> str:
        return str(self._record["id"])

    @property
    def name(self) -> str:
        return str(self._record["name"])

    @property
    def project(self) -> str:
        return str(self._record["project"])

    @property
    def state(self) -> str:
        return str(self._record["state"])

    @property
    def config(self) -> dict[str, Any]:
        return deepcopy(self._record.get("config", {}))

    @property
    def summary(self) -> dict[str, Any]:
        return deepcopy(self._record.get("summary", {}))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._record)

    def refresh(self) -> PublicRun:
        return PublicRun(self._client, self._client.get_run(self.id))

    def history(
        self,
        *,
        keys: Sequence[str],
        samples: int = 5_000,
    ) -> list[dict[str, Any]]:
        if not keys:
            raise ValueError("history requires at least one metric key")
        if not 1 <= samples <= 5_000:
            raise ValueError("history samples must be between 1 and 5000")
        response = self._client.history(self.id, keys=list(keys), limit=samples)
        return _history_rows(response, keys)

    def scan_history(
        self,
        *,
        keys: Sequence[str],
        page_size: int = 1_000,
    ) -> Iterator[dict[str, Any]]:
        if not keys:
            raise ValueError("scan_history requires at least one metric key")
        if not 1 <= page_size <= 5_000:
            raise ValueError("history page_size must be between 1 and 5000")
        after: int | None = None
        while True:
            response = self._client.history(
                self.id,
                keys=list(keys),
                limit=page_size,
                after=after,
            )
            yield from _history_rows(response, keys)
            next_after = response.get("next_after")
            if next_after is None:
                return
            after = int(next_after)

    def artifacts(self) -> list[dict[str, Any]]:
        return deepcopy(self._client.run_artifacts(self.id)["artifacts"])

    def traces(self, *, query: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return deepcopy(self._client.trace_spans(self.id, q=query, limit=limit)["spans"])


def _compile_filters(filters: Mapping[str, Any]) -> dict[str, Any]:
    query: dict[str, Any] = {"config_equals": {}, "summary_equals": {}}
    for key, value in filters.items():
        if key == "project":
            query["project"] = _filter_text(value, key)
        elif key == "state":
            if value not in {"running", "finished"}:
                raise ValueError("state filter must be 'running' or 'finished'")
            query["state"] = value
        elif key in {"name", "display_name"}:
            query["name"] = _filter_text(value, key)
        elif key == "name_contains":
            query["name_contains"] = _filter_text(value, key)
        elif key.startswith("config."):
            _reject_filter_operator(value, key)
            query["config_equals"][key.removeprefix("config.")] = deepcopy(value)
        elif key.startswith("summary."):
            _reject_filter_operator(value, key)
            query["summary_equals"][key.removeprefix("summary.")] = deepcopy(value)
        else:
            raise ValueError(f"unsupported run filter: {key}")
    return query


def _reject_filter_operator(value: Any, name: str) -> None:
    if isinstance(value, Mapping) and any(str(key).startswith("$") for key in value):
        raise ValueError(f"unsupported comparison operator in filter: {name}")


def _filter_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} filter must be a non-empty string")
    return value


def _parse_run_path(path: str) -> tuple[str | None, str]:
    parts = [part for part in path.strip("/").split("/") if part]
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError("run path must be a run ID or 'project/run_id'")


def _validate_page_size(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 200:
        raise ValueError("per_page must be between 1 and 200")


def _history_rows(response: Mapping[str, Any], keys: Sequence[str]) -> list[dict[str, Any]]:
    sequence = response.get("sequence", [])
    step = response.get("step", [])
    timestamp = response.get("timestamp_ms", [])
    metrics = response.get("metrics", {})
    rows = []
    for index, sequence_value in enumerate(sequence):
        row = {
            "_sequence": sequence_value,
            "_step": step[index],
            "_timestamp_ms": timestamp[index],
        }
        row.update({key: metrics[key][index] for key in keys})
        rows.append(row)
    return rows
