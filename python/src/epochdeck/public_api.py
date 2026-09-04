from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from epochdeck._json_normalization import normalize_json_object, normalize_json_value
from epochdeck._pagination import next_paired_cursor, next_text_cursor
from epochdeck._protocol import DeliveryProtocolError
from epochdeck.client import EpochDeckClient

_MAX_DOCUMENT_BYTES = 256 * 1024
_MAX_DOCUMENT_FILTERS = 32
_MAX_FILTER_KEY_BYTES = 256
_MAX_FILTER_FIELDS = _MAX_DOCUMENT_FILTERS * 2 + 4


class Api:
    def __init__(
        self,
        *,
        server_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.client = EpochDeckClient(
            server_url
            if server_url is not None
            else os.environ.get("EPOCHDECK_SERVER_URL", "http://127.0.0.1:8787"),
            timeout=timeout,
        )

    def __enter__(self) -> Api:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()

    def projects(self, *, per_page: int = 100) -> Iterator[dict[str, Any]]:
        _validate_page_size(per_page)
        return (
            deepcopy(project)
            for project in _cursor_objects(
                lambda before: self.client.projects(before=before, limit=per_page),
                "projects",
            )
        )

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

    def reports(self, project: str, *, per_page: int = 100) -> Iterator[dict[str, Any]]:
        _validate_page_size(per_page)
        return (
            deepcopy(self.client.get_report(_record_id(summary, "report")))
            for summary in _cursor_objects(
                lambda before: self.client.reports(project, before=before, limit=per_page),
                "reports",
            )
        )

    def report(self, report_id: str) -> dict[str, Any]:
        return deepcopy(self.client.get_report(report_id))

    def create_report(
        self,
        project: str,
        *,
        name: str,
        layout: Mapping[str, Any],
        description: str | None = None,
        id: str | None = None,
    ) -> dict[str, Any]:
        response = self.client.create_report(
            project,
            {
                "id": id,
                "name": name,
                "description": description,
                "layout": _normalize_report_layout(layout),
            },
        )
        return deepcopy(response["report"])

    def update_report(
        self,
        report_id: str,
        *,
        name: str,
        layout: Mapping[str, Any],
        description: str | None = None,
    ) -> dict[str, Any]:
        return deepcopy(
            self.client.update_report(
                report_id,
                {
                    "name": name,
                    "description": description,
                    "layout": _normalize_report_layout(layout),
                },
            )
        )

    def delete_report(self, report_id: str) -> dict[str, Any]:
        return deepcopy(self.client.delete_report(report_id))


class RunCollection:
    def __init__(self, client: EpochDeckClient, query: dict[str, Any]) -> None:
        self._client = client
        self._query = deepcopy(query)

    def __iter__(self) -> Iterator[PublicRun]:
        query = deepcopy(self._query)
        while True:
            response = self._client.query_runs(query)
            records = response.get("runs")
            if not isinstance(records, list):
                raise TypeError("EpochDeck query response has no run list")
            for record in records:
                if not isinstance(record, dict):
                    raise TypeError("EpochDeck query response contains an invalid run")
                run_id = _record_id(record, "run")
                yield PublicRun(self._client, self._client.get_run(run_id))
            cursor = next_text_cursor(
                response,
                field="next_before",
                previous=query.get("before"),
                context="EpochDeck run query response",
            )
            if cursor is None:
                return
            query["before"] = cursor


@dataclass(frozen=True, slots=True)
class PublicRun:
    _client: EpochDeckClient
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
        response = self._client.history(self.id, keys=list(keys), max_points=samples)
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
            if (
                isinstance(next_after, bool)
                or not isinstance(next_after, int)
                or (after is not None and next_after <= after)
            ):
                raise DeliveryProtocolError("history response cursor did not advance")
            after = next_after

    def artifacts(self, *, page_size: int = 100) -> Iterator[dict[str, Any]]:
        _validate_page_size(page_size)
        return _artifact_records(self._client, self.id, page_size)


def _compile_filters(filters: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(filters, Mapping):
        raise TypeError("run filters must be a mapping")
    query: dict[str, Any] = {"config_equals": {}, "summary_equals": {}}
    config_count = 0
    summary_count = 0
    for index, (key, value) in enumerate(filters.items()):
        if index >= _MAX_FILTER_FIELDS:
            raise ValueError(f"run filters cannot contain more than {_MAX_FILTER_FIELDS} fields")
        if not isinstance(key, str):
            raise TypeError("run filter names must be strings")
        if key == "project":
            query["project"] = _filter_text(value, key, 128)
        elif key == "state":
            if value not in {"running", "finished"}:
                raise ValueError("state filter must be 'running' or 'finished'")
            query["state"] = value
        elif key in {"name", "display_name"}:
            query["name"] = _filter_text(value, key, 256)
        elif key == "name_contains":
            query["name_contains"] = _filter_text(value, key, 256)
        elif key.startswith("config."):
            config_count += 1
            if config_count > _MAX_DOCUMENT_FILTERS:
                raise ValueError(
                    f"run filters cannot contain more than {_MAX_DOCUMENT_FILTERS} config fields"
                )
            document_key = key.removeprefix("config.")
            _validate_filter_document_key(document_key)
            normalized = normalize_json_value(value, f"{key} filter", _MAX_DOCUMENT_BYTES)
            _reject_filter_operator(normalized, key)
            query["config_equals"][document_key] = normalized
        elif key.startswith("summary."):
            summary_count += 1
            if summary_count > _MAX_DOCUMENT_FILTERS:
                raise ValueError(
                    f"run filters cannot contain more than {_MAX_DOCUMENT_FILTERS} summary fields"
                )
            document_key = key.removeprefix("summary.")
            _validate_filter_document_key(document_key)
            normalized = normalize_json_value(value, f"{key} filter", _MAX_DOCUMENT_BYTES)
            _reject_filter_operator(normalized, key)
            query["summary_equals"][document_key] = normalized
        else:
            raise ValueError(f"unsupported run filter: {key}")
    query["config_equals"] = normalize_json_object(
        query["config_equals"],
        "config filters",
        _MAX_DOCUMENT_BYTES,
    )
    query["summary_equals"] = normalize_json_object(
        query["summary_equals"],
        "summary filters",
        _MAX_DOCUMENT_BYTES,
    )
    return query


def _normalize_report_layout(layout: Mapping[str, Any]) -> dict[str, Any]:
    return normalize_json_object(layout, "report layout", _MAX_DOCUMENT_BYTES)


def _validate_filter_document_key(value: str) -> None:
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > _MAX_FILTER_KEY_BYTES
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(
            f"run query document keys must contain 1 to {_MAX_FILTER_KEY_BYTES} non-control bytes"
        )


def _reject_filter_operator(value: Any, name: str) -> None:
    if isinstance(value, Mapping) and any(str(key).startswith("$") for key in value):
        raise ValueError(f"unsupported comparison operator in filter: {name}")


def _filter_text(value: Any, name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} filter must be a non-empty string")
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > maximum
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(f"{name} filter must contain 1 to {maximum} non-control bytes")
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


def _cursor_objects(
    request: Callable[[str | None], dict[str, Any]],
    field: str,
) -> Iterator[dict[str, Any]]:
    before: str | None = None
    while True:
        response = request(before)
        records = response.get(field)
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise TypeError(f"EpochDeck response has no {field} object list")
        yield from records
        before = next_text_cursor(
            response,
            field="next_before",
            previous=before,
            context=f"EpochDeck {field} response",
        )
        if before is None:
            return


def _artifact_records(
    client: EpochDeckClient,
    run_id: str,
    page_size: int,
) -> Iterator[dict[str, Any]]:
    before: str | None = None
    before_relation: Literal["input", "output"] | None = None
    while True:
        response = client.run_artifacts(
            run_id,
            before=before,
            before_relation=before_relation,
            limit=page_size,
        )
        page = response.get("artifacts")
        if not isinstance(page, list) or not all(isinstance(item, dict) for item in page):
            raise TypeError("EpochDeck artifact response has no artifact list")
        for linked in page:
            summary = linked.get("artifact")
            if not isinstance(summary, dict):
                raise TypeError("EpochDeck artifact link has no artifact summary")
            detail = client.get_artifact(_record_id(summary, "artifact"))
            full_link = deepcopy(linked)
            full_link["artifact"] = deepcopy(detail)
            yield full_link
        cursor = next_paired_cursor(
            response,
            previous=(before, before_relation)
            if before is not None and before_relation is not None
            else None,
            context="EpochDeck artifact response",
        )
        if cursor is None:
            return
        before, before_relation = cursor


def _record_id(record: Mapping[str, Any], name: str) -> str:
    identity = record.get("id")
    if not isinstance(identity, str) or not identity:
        raise TypeError(f"EpochDeck {name} has no non-empty ID")
    return identity


def _history_rows(response: Mapping[str, Any], keys: Sequence[str]) -> list[dict[str, Any]]:
    sequence = response.get("sequence")
    step = response.get("step")
    timestamp = response.get("timestamp_ms")
    metrics = response.get("metrics")
    if not all(isinstance(column, list) for column in (sequence, step, timestamp)):
        raise DeliveryProtocolError("history response has invalid scalar columns")
    assert isinstance(sequence, list)
    assert isinstance(step, list)
    assert isinstance(timestamp, list)
    if len(step) != len(sequence) or len(timestamp) != len(sequence):
        raise DeliveryProtocolError("history response columns are not aligned")
    if not isinstance(metrics, Mapping):
        raise DeliveryProtocolError("history response has no metric columns")
    columns: dict[str, list[Any]] = {}
    for key in keys:
        column = metrics.get(key)
        if not isinstance(column, list) or len(column) != len(sequence):
            raise DeliveryProtocolError("history response columns are not aligned")
        columns[key] = column
    rows: list[dict[str, Any]] = []
    for index, sequence_value in enumerate(sequence):
        row = {
            "_sequence": sequence_value,
            "_step": step[index],
            "_timestamp_ms": timestamp[index],
        }
        row.update({key: columns[key][index] for key in keys})
        rows.append(row)
    return rows
