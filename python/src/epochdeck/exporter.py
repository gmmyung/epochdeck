from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from epochdeck._pagination import next_paired_cursor, next_text_cursor
from epochdeck._platform_fs import (
    is_link_or_reparse,
    sync_directory,
    sync_regular_file,
)
from epochdeck.client import EpochDeckClient

_PAGE_SIZE = 200
_HISTORY_PAGE_SIZE = 5_000
_METRIC_COLUMNS_PER_FILE = 32
_COPY_CHUNK_BYTES = 1024 * 1024
_MAX_EXPORT_TREE_DEPTH = 128


class ExportConsistencyError(RuntimeError):
    pass


def export_project(client: EpochDeckClient, project: str, destination: Path) -> dict[str, Any]:
    """Write one complete project to an atomically installed portable directory."""
    destination = destination.expanduser().resolve()
    if destination.exists():
        raise FileExistsError(f"export destination already exists: {destination}")
    initial_mutation_token = _project_mutation_token(client, project)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.partial-{uuid4()}"
    temporary.mkdir(mode=0o700)
    temporary.chmod(0o700)
    counts = {
        "runs": 0,
        "metric_pages": 0,
        "alerts": 0,
        "rich_values": 0,
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
            for summary in _cursor_records(
                lambda before: client.reports(project, before=before, limit=_PAGE_SIZE),
                "reports",
            ):
                report = _detail_record(client.get_report, summary, "report")
                _write_json_line(stream, report)
                counts["reports"] += 1

        with (
            (temporary / "sweeps.jsonl").open("w", encoding="utf-8") as sweep_stream,
            (temporary / "sweep-trials.jsonl").open("w", encoding="utf-8") as trial_stream,
        ):
            for summary in _cursor_records(
                lambda before: client.sweeps(project, before=before, limit=_PAGE_SIZE),
                "sweeps",
            ):
                sweep = _detail_record(client.get_sweep, summary, "sweep")
                _write_json_line(sweep_stream, sweep)
                counts["sweeps"] += 1
                sweep_id = str(sweep["id"])
                for trial in _sweep_trial_records(client, sweep_id):
                    _write_json_line(trial_stream, {"sweep_id": sweep["id"], "trial": trial})
                    counts["sweep_trials"] += 1

        with (temporary / "artifacts.jsonl").open("w", encoding="utf-8") as stream:
            for summary in _cursor_records(
                lambda before: client.project_artifacts(project, before=before, limit=_PAGE_SIZE),
                "artifacts",
            ):
                artifact = _detail_record(client.get_artifact, summary, "artifact")
                _write_json_line(stream, artifact)
                counts["artifacts"] += 1
                for entry in _object_list(artifact, "entries"):
                    if _export_blob(client, _object(entry, "blob"), blob_root):
                        counts["blobs"] += 1

        for summary in _run_summaries(client, project):
            run = _finished_run_detail(client, summary)
            _export_run(client, run, runs_root, blob_root, counts)
            counts["runs"] += 1

        final_mutation_token = _project_mutation_token(client, project)
        if final_mutation_token != initial_mutation_token:
            raise ExportConsistencyError(
                "project changed during export; no bundle was published, retry when writes stop"
            )
        manifest = {
            "format": "epochdeck-export",
            "project": project,
            "created_at": datetime.now(UTC).isoformat(),
            "counts": counts,
        }
        _write_json(temporary / "manifest.json", manifest)
        _sync_private_tree(temporary)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _export_run(
    client: EpochDeckClient,
    run: dict[str, Any],
    runs_root: Path,
    blob_root: Path,
    counts: dict[str, int],
) -> None:
    run_id = str(run["id"])
    run_root = runs_root / run_id
    run_root.mkdir()
    _write_json(run_root / "run.json", run)

    metric_root = run_root / "metrics"
    metric_root.mkdir()
    for index, selected in enumerate(
        _batches(_metric_keys(client, run_id), _METRIC_COLUMNS_PER_FILE)
    ):
        path = metric_root / f"{index:04d}.jsonl"
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
        _alert_records(client, run_id),
        counts,
        "alerts",
    )

    with (run_root / "rich-values.jsonl").open("w", encoding="utf-8") as stream:
        for summary in _rich_value_summaries(client, run_id):
            value = _detail_record(client.get_rich_value, summary, "rich value")
            _write_json_line(stream, value)
            counts["rich_values"] += 1
            blob = value.get("blob")
            if blob is not None and _export_blob(
                client, _expect_object(blob, "rich blob"), blob_root
            ):
                counts["blobs"] += 1

    with (run_root / "artifact-links.jsonl").open("w", encoding="utf-8") as stream:
        for linked in _run_artifact_records(client, run_id):
            _write_json_line(stream, _artifact_link_record(linked))
            counts["artifact_links"] += 1


def _export_blob(client: EpochDeckClient, blob: dict[str, Any], root: Path) -> bool:
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
        before = next_text_cursor(
            response,
            field="next_before",
            previous=before,
            context=f"{field} response",
        )
        if before is None:
            return


def _metric_keys(client: EpochDeckClient, run_id: str) -> Iterator[str]:
    after: str | None = None
    while True:
        response = client.metric_keys(run_id, after=after, limit=_PAGE_SIZE)
        page = response.get("keys")
        if not isinstance(page, list) or not all(isinstance(key, str) for key in page):
            raise TypeError(f"run {run_id} metric response has no string key list")
        yield from page
        next_after = next_text_cursor(
            response,
            field="next_after",
            previous=after,
            context=f"run {run_id} metric response",
        )
        if next_after is None:
            return
        if after is not None and next_after <= after:
            raise TypeError(f"run {run_id} metric cursor did not advance")
        after = next_after


def _batches(records: Iterator[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for record in records:
        batch.append(record)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _project_mutation_token(client: EpochDeckClient, project: str) -> str:
    record = client.get_project(project)
    token = record.get("mutation_token")
    if not isinstance(token, str) or not token:
        raise TypeError("project detail has no mutation token")
    return token


def _finished_run_detail(
    client: EpochDeckClient,
    summary: dict[str, Any],
) -> dict[str, Any]:
    if summary.get("state") != "finished":
        raise ExportConsistencyError(
            f"run {summary.get('id', '<unknown>')} is still running; finish it before export"
        )
    run = _detail_record(client.get_run, summary, "run")
    if run.get("state") != "finished":
        raise ExportConsistencyError(
            f"run {run.get('id', '<unknown>')} is still running; finish it before export"
        )
    return run


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


def _sweep_trial_records(
    client: EpochDeckClient,
    sweep_id: str,
) -> Iterator[dict[str, Any]]:
    for summary in _cursor_records(
        lambda before: client.sweep_trials(sweep_id, before=before, limit=_PAGE_SIZE),
        "trials",
    ):
        trial = _detail_record(client.get_sweep_trial, summary, "sweep trial")
        if trial.get("sweep_id") != sweep_id:
            raise TypeError("sweep trial detail has the wrong sweep ID")
        yield trial


def _run_summaries(client: EpochDeckClient, project: str) -> Iterator[dict[str, Any]]:
    return _cursor_records(
        lambda before: client.query_runs(
            {"project": project, "before": before, "limit": _PAGE_SIZE}
        ),
        "runs",
    )


def _alert_records(
    client: EpochDeckClient,
    run_id: str,
) -> Iterator[dict[str, Any]]:
    return _cursor_records(
        lambda before: client.alerts(run_id, before=before, limit=_PAGE_SIZE),
        "alerts",
    )


def _rich_value_summaries(
    client: EpochDeckClient,
    run_id: str,
) -> Iterator[dict[str, Any]]:
    after: str | None = None
    while True:
        response = client.rich_value_keys(run_id, after=after, limit=_PAGE_SIZE)
        key_summaries = _response_list(response, "keys")
        for key_summary in key_summaries:
            key = key_summary.get("key")
            if not isinstance(key, str) or not key:
                raise TypeError(f"run {run_id} rich-value key catalog contains an invalid key")
            yield from _rich_values_for_key(client, run_id, key)
        next_after = next_text_cursor(
            response,
            field="next_after",
            previous=after,
            context=f"run {run_id} rich-value key response",
        )
        if next_after is None:
            return
        if after is not None and next_after <= after:
            raise TypeError(f"run {run_id} rich-value key cursor did not advance")
        after = next_after


def _rich_values_for_key(
    client: EpochDeckClient,
    run_id: str,
    key: str,
) -> Iterator[dict[str, Any]]:
    return _cursor_records(
        lambda before: client.rich_values(
            run_id,
            key=key,
            before=before,
            limit=_PAGE_SIZE,
        ),
        "values",
    )


def _run_artifact_records(
    client: EpochDeckClient,
    run_id: str,
) -> Iterator[dict[str, Any]]:
    before: str | None = None
    before_relation: Literal["input", "output"] | None = None
    while True:
        response = client.run_artifacts(
            run_id,
            before=before,
            before_relation=before_relation,
            limit=_PAGE_SIZE,
        )
        yield from _response_list(response, "artifacts")
        cursor = next_paired_cursor(
            response,
            previous=(before, before_relation)
            if before is not None and before_relation is not None
            else None,
            context=f"run {run_id} artifact response",
        )
        if cursor is None:
            return
        before, before_relation = cursor


def _artifact_link_record(linked: dict[str, Any]) -> dict[str, str]:
    artifact = _object(linked, "artifact")
    artifact_id = artifact.get("id")
    relation = linked.get("relation")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise TypeError("run artifact link has no non-empty artifact ID")
    if relation not in {"input", "output"}:
        raise TypeError("run artifact link has an invalid relation")
    return {"artifact_id": artifact_id, "relation": relation}


def _detail_record(
    request: Callable[[str], dict[str, Any]],
    summary: dict[str, Any],
    name: str,
) -> dict[str, Any]:
    identity = summary.get("id")
    if not isinstance(identity, str) or not identity:
        raise TypeError(f"{name} summary has no non-empty ID")
    record = request(identity)
    if record.get("id") != identity:
        raise TypeError(f"{name} detail has the wrong ID")
    return record


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
        raise TypeError(f"EpochDeck response has no {field} object list")
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


def _sync_private_tree(root: Path, *, depth: int = 0) -> None:
    if depth > _MAX_EXPORT_TREE_DEPTH:
        raise RuntimeError(f"export tree exceeds {_MAX_EXPORT_TREE_DEPTH} directory levels")
    root.chmod(0o700)
    with os.scandir(root) as entries:
        for entry in entries:
            path = Path(entry.path)
            status = entry.stat(follow_symlinks=False)
            if is_link_or_reparse(status):
                raise RuntimeError(f"export tree contains a symbolic link: {path}")
            if stat.S_ISDIR(status.st_mode):
                _sync_private_tree(path, depth=depth + 1)
                continue
            if not stat.S_ISREG(status.st_mode):
                raise RuntimeError(f"export tree contains a non-regular file: {path}")
            path.chmod(0o600)
            sync_regular_file(path)
    _fsync_directory_descriptor(root)


def _fsync_directory(path: Path) -> None:
    _fsync_directory_descriptor(path)


def _fsync_directory_descriptor(path: Path) -> None:
    sync_directory(path)
