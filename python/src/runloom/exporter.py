from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from runloom.client import RunloomClient

_PAGE_SIZE = 200
_HISTORY_PAGE_SIZE = 5_000
_METRIC_COLUMNS_PER_FILE = 32
_COPY_CHUNK_BYTES = 1024 * 1024
_FORMAT_VERSION = 1


def export_project(client: RunloomClient, project: str, destination: Path) -> dict[str, Any]:
    """Write one complete project to an atomically installed portable directory."""
    destination = destination.expanduser().resolve()
    if destination.exists():
        raise FileExistsError(f"export destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.partial-{uuid4()}"
    temporary.mkdir()
    counts = {
        "runs": 0,
        "metric_pages": 0,
        "alerts": 0,
        "rich_values": 0,
        "traces": 0,
        "artifacts": 0,
        "artifact_links": 0,
        "reports": 0,
        "sweeps": 0,
        "sweep_trials": 0,
        "blobs": 0,
    }
    try:
        blob_root = temporary / "blobs" / "sha256"
        runs_root = temporary / "runs"
        runs_root.mkdir()
        blob_root.mkdir(parents=True)

        with (temporary / "reports.jsonl").open("w", encoding="utf-8") as stream:
            for report in _cursor_records(
                lambda before: client.reports(project, before=before, limit=_PAGE_SIZE),
                "reports",
            ):
                _write_json_line(stream, report)
                counts["reports"] += 1

        with (
            (temporary / "sweeps.jsonl").open("w", encoding="utf-8") as sweep_stream,
            (temporary / "sweep-trials.jsonl").open("w", encoding="utf-8") as trial_stream,
        ):
            for sweep in _cursor_records(
                lambda before: client.sweeps(project, before=before, limit=_PAGE_SIZE),
                "sweeps",
            ):
                _write_json_line(sweep_stream, sweep)
                counts["sweeps"] += 1
                for trial in _cursor_records(
                    lambda before, sweep_id=str(sweep["id"]): client.sweep_trials(
                        sweep_id, before=before, limit=_PAGE_SIZE
                    ),
                    "trials",
                ):
                    _write_json_line(trial_stream, {"sweep_id": sweep["id"], "trial": trial})
                    counts["sweep_trials"] += 1

        with (temporary / "artifacts.jsonl").open("w", encoding="utf-8") as stream:
            for artifact in _cursor_records(
                lambda before: client.project_artifacts(project, before=before, limit=_PAGE_SIZE),
                "artifacts",
            ):
                _write_json_line(stream, artifact)
                counts["artifacts"] += 1
                for entry in _object_list(artifact, "entries"):
                    if _export_blob(client, _object(entry, "blob"), blob_root):
                        counts["blobs"] += 1

        before: str | None = None
        while True:
            response = client.query_runs(
                {"project": project, "before": before, "limit": _PAGE_SIZE}
            )
            runs = _response_list(response, "runs")
            for run in runs:
                _export_run(client, run, runs_root, blob_root, counts)
                counts["runs"] += 1
            cursor = response.get("next_before")
            if cursor is None:
                break
            before = str(cursor)

        manifest = {
            "format": "runloom-export",
            "format_version": _FORMAT_VERSION,
            "project": project,
            "created_at": datetime.now(UTC).isoformat(),
            "counts": counts,
        }
        _write_json(temporary / "manifest.json", manifest)
        os.replace(temporary, destination)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _export_run(
    client: RunloomClient,
    run: dict[str, Any],
    runs_root: Path,
    blob_root: Path,
    counts: dict[str, int],
) -> None:
    run_id = str(run["id"])
    run_root = runs_root / run_id
    run_root.mkdir()
    _write_json(run_root / "run.json", run)

    keys = client.metric_keys(run_id).get("keys")
    if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
        raise TypeError(f"run {run_id} metric response has no string key list")
    metric_root = run_root / "metrics"
    metric_root.mkdir()
    for index in range(0, len(keys), _METRIC_COLUMNS_PER_FILE):
        selected = keys[index : index + _METRIC_COLUMNS_PER_FILE]
        path = metric_root / f"{index // _METRIC_COLUMNS_PER_FILE:04d}.jsonl"
        after: int | None = None
        with path.open("w", encoding="utf-8") as stream:
            while True:
                page = client.history(
                    run_id,
                    keys=selected,
                    after=after,
                    limit=_HISTORY_PAGE_SIZE,
                )
                _write_json_line(stream, page)
                counts["metric_pages"] += 1
                cursor = page.get("next_after")
                if cursor is None:
                    break
                after = int(cursor)

    _write_run_records(
        run_root / "alerts.jsonl",
        _cursor_records(
            lambda before: client.alerts(run_id, before=before, limit=_PAGE_SIZE),
            "alerts",
        ),
        counts,
        "alerts",
    )

    with (run_root / "rich-values.jsonl").open("w", encoding="utf-8") as stream:
        for value in _cursor_records(
            lambda before: client.rich_values(run_id, before=before, limit=_PAGE_SIZE),
            "values",
        ):
            _write_json_line(stream, value)
            counts["rich_values"] += 1
            blob = value.get("blob")
            if blob is not None and _export_blob(
                client, _expect_object(blob, "rich blob"), blob_root
            ):
                counts["blobs"] += 1

    with (run_root / "traces.jsonl").open("w", encoding="utf-8") as stream:
        for span in _cursor_records(
            lambda before: client.trace_spans(run_id, before=before, limit=_PAGE_SIZE),
            "spans",
        ):
            _write_json_line(stream, span)
            counts["traces"] += 1
            payload = span.get("payload")
            if payload is not None and _export_blob(
                client, _expect_object(payload, "trace payload"), blob_root
            ):
                counts["blobs"] += 1

    with (run_root / "artifact-links.jsonl").open("w", encoding="utf-8") as stream:
        for linked in _cursor_records(
            lambda before: client.run_artifacts(run_id, before=before, limit=_PAGE_SIZE),
            "artifacts",
        ):
            artifact = _object(linked, "artifact")
            _write_json_line(
                stream,
                {"artifact_id": artifact["id"], "relation": linked["relation"]},
            )
            counts["artifact_links"] += 1


def _export_blob(client: RunloomClient, blob: dict[str, Any], root: Path) -> bool:
    digest = str(blob["digest"])
    expected_size = int(blob["size"])
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"invalid SHA-256 digest in export: {digest}")
    destination = root / digest[:2] / digest
    if destination.exists():
        if destination.stat().st_size != expected_size:
            raise ValueError(f"exported blob size conflict for {digest}")
        return False
    destination.parent.mkdir(exist_ok=True)
    partial = destination.with_suffix(".partial")
    size = client.download_blob(digest, partial)
    if size != expected_size:
        partial.unlink(missing_ok=True)
        raise ValueError(f"downloaded blob size differs for {digest}")
    if _sha256(partial) != digest:
        partial.unlink(missing_ok=True)
        raise ValueError(f"downloaded blob digest differs for {digest}")
    os.replace(partial, destination)
    return True


def _cursor_records(
    request: Callable[[str | None], dict[str, Any]],
    field: str,
) -> Iterator[dict[str, Any]]:
    before: str | None = None
    while True:
        response = request(before)
        yield from _response_list(response, field)
        cursor = response.get("next_before")
        if cursor is None:
            return
        before = str(cursor)


def _write_run_records(
    path: Path,
    records: Iterator[dict[str, Any]],
    counts: dict[str, int],
    count_key: str,
) -> None:
    with path.open("w", encoding="utf-8") as stream:
        for record in records:
            _write_json_line(stream, record)
            counts[count_key] += 1


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
        stream.write("\n")


def _write_json_line(stream: Any, value: Any) -> None:
    json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
    stream.write("\n")


def _response_list(response: dict[str, Any], field: str) -> list[dict[str, Any]]:
    values = response.get(field)
    if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
        raise TypeError(f"Runloom response has no {field} object list")
    return values


def _object(value: dict[str, Any], field: str) -> dict[str, Any]:
    return _expect_object(value.get(field), field)


def _expect_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return value


def _object_list(value: dict[str, Any], field: str) -> list[dict[str, Any]]:
    result = value.get(field)
    if not isinstance(result, list) or not all(isinstance(item, dict) for item in result):
        raise TypeError(f"{field} must be an object list")
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()
