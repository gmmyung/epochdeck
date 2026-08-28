from __future__ import annotations

import hashlib
import json
import math
import mimetypes
import os
import tempfile
import threading
import uuid
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from runloom.client import RunloomApiError, RunloomClient

_CHECKPOINT_VERSION = 1
_MAX_IMPORT_RUNS = 100_000
_MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
_MAX_WORKERS = 16
_ARTIFACT_WORKERS = 4
_METRIC_BATCH_SIZE = 512
_METRIC_BATCH_BYTES = 1_750_000
_FILE_CHUNK_SIZE = 64
_MAX_ARTIFACT_FILES = 10_000
_HASH_CHUNK_BYTES = 1024 * 1024
_MIN_HISTORY_PAGE_SIZE = 1_000
_TARGET_HISTORY_PAGE_COUNT = 256
_MEDIA_KINDS = {
    "audio-file": "audio",
    "image-file": "image",
    "video-file": "video",
}


@dataclass(frozen=True, slots=True)
class ImportResult:
    selected: int
    completed: int
    skipped: int
    failed: int
    failures: tuple[str, ...]


class WandbImportError(RuntimeError):
    pass


def import_wandb_runs(
    source_api: Any,
    client: RunloomClient,
    *,
    entity: str,
    project: str,
    target_project: str,
    checkpoint_path: Path,
    workers: int = 4,
    max_runs: int | None = None,
    include_files: bool = True,
) -> ImportResult:
    """Import W&B runs with bounded parallelism and batch-level restart checkpoints."""
    if not 1 <= workers <= _MAX_WORKERS:
        raise ValueError(f"workers must be between 1 and {_MAX_WORKERS}")
    if max_runs is not None and not 1 <= max_runs <= _MAX_IMPORT_RUNS:
        raise ValueError(f"max_runs must be between 1 and {_MAX_IMPORT_RUNS}")
    checkpoint = _Checkpoint(
        checkpoint_path,
        entity=entity,
        project=project,
        target_project=target_project,
    )
    selected = completed = skipped = failed = 0
    failures: list[str] = []
    pending: list[tuple[str, Future[str]]] = []
    limit = _MAX_IMPORT_RUNS if max_runs is None else max_runs

    def collect(source_id: str, future: Future[str]) -> None:
        nonlocal completed, skipped, failed
        try:
            outcome = future.result()
        except Exception as error:
            failed += 1
            message = f"{source_id}: {type(error).__name__}: {error}"
            failures.append(message)
            checkpoint.update(source_id, status="failed", error=message)
        else:
            if outcome == "skipped":
                skipped += 1
            else:
                completed += 1

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="runloom-wandb") as executor:
        for source_run in source_api.runs(f"{entity}/{project}"):
            if selected >= limit:
                break
            source_id = _required_text(source_run, "id")
            future = executor.submit(
                _import_one_run,
                source_run,
                client,
                checkpoint,
                entity,
                project,
                target_project,
                include_files,
            )
            pending.append((source_id, future))
            selected += 1
            if len(pending) >= workers:
                collect(*pending.pop(0))
        for item in pending:
            collect(*item)

    return ImportResult(
        selected=selected,
        completed=completed,
        skipped=skipped,
        failed=failed,
        failures=tuple(failures),
    )


def _import_one_run(
    source: Any,
    client: RunloomClient,
    checkpoint: _Checkpoint,
    entity: str,
    project: str,
    target_project: str,
    include_files: bool,
) -> str:
    source_id = _required_text(source, "id")
    state = checkpoint.state(source_id)
    if state.get("status") == "complete":
        return "skipped"
    source_updated_at = str(getattr(source, "updated_at", ""))
    stored_updated_at = state.get("source_updated_at")
    if stored_updated_at not in {None, source_updated_at}:
        raise WandbImportError(
            "source run changed after import began; remove its checkpoint entry to restart it"
        )

    run_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/{entity}/{project}/runs/{source_id}",
        )
    )
    source_metadata = {
        "entity": entity,
        "project": project,
        "run_id": source_id,
        "url": str(getattr(source, "url", "")),
    }
    config = _wandb_document(getattr(source, "config", {}) or {}, "W&B config")
    if "_runloom_wandb_source" in config:
        raise WandbImportError("W&B config uses reserved key '_runloom_wandb_source'")
    config["_runloom_wandb_source"] = source_metadata
    try:
        record = client.get_run(run_id)
    except RunloomApiError as error:
        if error.status_code != 404:
            raise
        created = client.create_run(
            project=target_project,
            run_id=run_id,
            name=str(getattr(source, "name", "")) or source_id,
            config=config,
            resume="never",
        )
        record = _object(created, "run")
    existing_source = _object(record, "config").get("_runloom_wandb_source")
    if record.get("project") not in {None, target_project} or existing_source != source_metadata:
        raise WandbImportError(f"deterministic Runloom run ID collision: {run_id}")
    if record.get("state") == "finished":
        checkpoint.update(
            source_id,
            status="complete",
            run_id=run_id,
            source_updated_at=source_updated_at,
        )
        return "skipped"

    checkpoint.update(
        source_id,
        status="importing",
        error=None,
        run_id=run_id,
        source_updated_at=source_updated_at,
    )
    state = checkpoint.state(source_id)
    rows_committed = _state_int(state, "rows_committed", 0)
    next_sequence = _state_int(state, "next_sequence", 1)
    unsupported_values = _state_int(state, "unsupported_values", 0)
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    batch_rows_end = rows_committed
    scanned_rows = 0

    history = source.scan_history(page_size=_history_page_size(source))
    for row_index, raw_row in enumerate(history):
        scanned_rows = row_index + 1
        if scanned_rows <= rows_committed:
            continue
        if not isinstance(raw_row, Mapping):
            raise WandbImportError("W&B history yielded a non-object row")
        metrics, media, skipped_values = _history_values(raw_row)
        step = _history_step(raw_row, row_index)
        timestamp_ms = _history_timestamp_ms(raw_row, row_index)
        if include_files:
            for key, kind, reference in media:
                _import_media_reference(
                    source,
                    client,
                    run_id=run_id,
                    source_metadata=source_metadata,
                    key=key,
                    kind=kind,
                    step=step,
                    timestamp_ms=timestamp_ms,
                    reference=reference,
                )
        else:
            skipped_values += len(media)
        if metrics:
            sequence = next_sequence + len(batch)
            point = {
                "sequence": sequence,
                "step": step,
                "timestamp_ms": timestamp_ms,
                "metrics": metrics,
            }
            point_bytes = _json_size(point) + 1
            if point_bytes > _METRIC_BATCH_BYTES:
                raise WandbImportError(
                    f"W&B history row {row_index} exceeds the metric request byte budget"
                )
            if batch and batch_bytes + point_bytes > _METRIC_BATCH_BYTES:
                _commit_metric_batch(client, run_id, batch)
                next_sequence += len(batch)
                rows_committed = batch_rows_end
                checkpoint.update(
                    source_id,
                    rows_committed=rows_committed,
                    next_sequence=next_sequence,
                    unsupported_values=unsupported_values,
                )
                batch = []
                batch_bytes = 0
                point["sequence"] = next_sequence
            batch.append(point)
            batch_bytes += point_bytes
        unsupported_values += skipped_values
        batch_rows_end = scanned_rows
        if len(batch) >= _METRIC_BATCH_SIZE:
            _commit_metric_batch(client, run_id, batch)
            next_sequence += len(batch)
            rows_committed = batch_rows_end
            batch = []
            batch_bytes = 0
            checkpoint.update(
                source_id,
                rows_committed=rows_committed,
                next_sequence=next_sequence,
                unsupported_values=unsupported_values,
            )

    if batch:
        _commit_metric_batch(client, run_id, batch)
        next_sequence += len(batch)
    if scanned_rows > rows_committed:
        rows_committed = scanned_rows
        checkpoint.update(
            source_id,
            rows_committed=rows_committed,
            next_sequence=next_sequence,
            unsupported_values=unsupported_values,
        )

    if include_files:
        _import_run_files(source, client, checkpoint, source_id, run_id, source_metadata)
        _import_logged_artifacts(source, client, checkpoint, source_id, run_id, source_metadata)

    summary = _wandb_document(getattr(source, "summary", {}) or {}, "W&B summary")
    if "_runloom_wandb_source" in summary:
        raise WandbImportError("W&B summary uses reserved key '_runloom_wandb_source'")
    summary["_runloom_wandb_source"] = {
        **source_metadata,
        "state": str(getattr(source, "state", "unknown")),
        "unsupported_history_values": unsupported_values,
    }
    client.finish_run(run_id, summary)
    checkpoint.update(
        source_id,
        status="complete",
        rows_committed=rows_committed,
        next_sequence=next_sequence,
        unsupported_values=unsupported_values,
        error=None,
    )
    return "completed"


def _commit_metric_batch(
    client: RunloomClient,
    run_id: str,
    points: list[dict[str, Any]],
) -> None:
    client.ingest_batch(
        run_id,
        {"batch_sequence": points[0]["sequence"], "points": points},
    )


def _import_run_files(
    source: Any,
    client: RunloomClient,
    checkpoint: _Checkpoint,
    source_id: str,
    run_id: str,
    source_metadata: dict[str, str],
) -> None:
    state = checkpoint.state(source_id)
    if state.get("files_complete") is True:
        return
    files_committed = _state_int(state, "files_committed", 0)
    chunk: list[Any] = []
    chunk_start = files_committed
    seen = 0
    for index, source_file in enumerate(source.files()):
        seen = index + 1
        if seen <= files_committed:
            continue
        if not chunk:
            chunk_start = index
        chunk.append(source_file)
        if len(chunk) == _FILE_CHUNK_SIZE:
            _import_file_chunk(
                client,
                chunk,
                run_id=run_id,
                source_metadata=source_metadata,
                chunk_start=chunk_start,
            )
            files_committed = seen
            checkpoint.update(source_id, files_committed=files_committed)
            chunk = []
    if chunk:
        _import_file_chunk(
            client,
            chunk,
            run_id=run_id,
            source_metadata=source_metadata,
            chunk_start=chunk_start,
        )
        files_committed = seen
    checkpoint.update(source_id, files_committed=files_committed, files_complete=True)


def _import_file_chunk(
    client: RunloomClient,
    files: list[Any],
    *,
    run_id: str,
    source_metadata: dict[str, str],
    chunk_start: int,
) -> None:
    entries: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="runloom-wandb-") as raw_root:
        root = Path(raw_root).resolve()
        for source_file in files:
            artifact_path = _safe_artifact_path(_required_text(source_file, "name"))
            downloaded = source_file.download(root=str(root), replace=True)
            local_path = _downloaded_path(downloaded, root, artifact_path)
            blob = _blob_for_file(local_path, artifact_path)
            client.upload_blob(local_path, blob)
            entries.append({"path": artifact_path, "blob": blob})
    artifact_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/{source_metadata['entity']}/{source_metadata['project']}"
            f"/runs/{source_metadata['run_id']}/files/{chunk_start}",
        )
    )
    client.create_artifact(
        run_id,
        {
            "id": artifact_id,
            "name": f"wandb-run-{run_id[:8]}-files",
            "type": "wandb-run-files",
            "description": "Files preserved by the Runloom W&B importer.",
            "metadata": {"wandb_source": source_metadata, "chunk_start": chunk_start},
            "aliases": ["latest"],
            "entries": entries,
        },
    )


def _import_logged_artifacts(
    source: Any,
    client: RunloomClient,
    checkpoint: _Checkpoint,
    source_id: str,
    run_id: str,
    source_metadata: dict[str, str],
) -> None:
    list_artifacts = getattr(source, "logged_artifacts", None)
    if not callable(list_artifacts):
        return
    state = checkpoint.state(source_id)
    if state.get("logged_artifacts_complete") is True:
        return
    committed = _state_int(state, "logged_artifacts_committed", 0)
    seen = 0
    pending: list[tuple[int, Future[None]]] = []

    def collect(index: int, future: Future[None]) -> None:
        nonlocal committed
        future.result()
        committed = index + 1
        checkpoint.update(source_id, logged_artifacts_committed=committed)

    with ThreadPoolExecutor(
        max_workers=_ARTIFACT_WORKERS,
        thread_name_prefix="runloom-wandb-artifact",
    ) as executor:
        for index, artifact in enumerate(list_artifacts(per_page=100)):
            seen = index + 1
            if seen <= committed:
                continue
            pending.append(
                (
                    index,
                    executor.submit(
                        _import_logged_artifact,
                        client,
                        artifact,
                        run_id,
                        source_metadata,
                    ),
                )
            )
            if len(pending) >= _ARTIFACT_WORKERS:
                collect(*pending.pop(0))
        for item in pending:
            collect(*item)
    checkpoint.update(
        source_id,
        logged_artifacts_committed=max(committed, seen),
        logged_artifacts_complete=True,
    )


def _import_logged_artifact(
    client: RunloomClient,
    source_artifact: Any,
    run_id: str,
    source_metadata: dict[str, str],
) -> None:
    qualified_name = str(getattr(source_artifact, "qualified_name", ""))
    source_id = str(getattr(source_artifact, "id", "")) or qualified_name
    if not source_id:
        raise WandbImportError("W&B artifact has no stable identity")
    source_name = _required_text(source_artifact, "name")
    source_version = str(getattr(source_artifact, "version", ""))
    suffix = f":{source_version}" if source_version else ""
    target_name = source_name.removesuffix(suffix)
    entries: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="runloom-wandb-artifact-") as raw_root:
        root = Path(raw_root).resolve()
        for source_file in source_artifact.files(per_page=100):
            if len(entries) >= _MAX_ARTIFACT_FILES:
                raise WandbImportError(
                    f"W&B artifact {source_name!r} exceeds {_MAX_ARTIFACT_FILES} files"
                )
            artifact_path = _safe_artifact_path(_required_text(source_file, "name"))
            downloaded = source_file.download(root=str(root), replace=True)
            local_path = _downloaded_path(downloaded, root, artifact_path)
            blob = _blob_for_file(local_path, artifact_path)
            client.upload_blob(local_path, blob)
            entries.append({"path": artifact_path, "blob": blob})
            local_path.unlink()
    raw_metadata = _wandb_document(
        getattr(source_artifact, "metadata", {}) or {},
        "W&B artifact source metadata",
    )
    metadata = _json_document(
        {
            "wandb_source": {
                **source_metadata,
                "artifact_id": source_id,
                "qualified_name": qualified_name,
                "version": source_version,
            },
            "wandb_metadata": raw_metadata,
        },
        "W&B artifact metadata",
    )
    raw_aliases = list(getattr(source_artifact, "aliases", []) or [])
    aliases = list(dict.fromkeys(str(alias) for alias in raw_aliases if str(alias)))
    artifact_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/artifacts/{source_id}",
        )
    )
    description = getattr(source_artifact, "description", None)
    client.create_artifact(
        run_id,
        {
            "id": artifact_id,
            "name": target_name,
            "type": _required_text(source_artifact, "type"),
            "description": str(description) if description is not None else None,
            "metadata": metadata,
            "aliases": aliases,
            "entries": entries,
        },
    )


def _history_page_size(source: Any) -> int:
    last_step = getattr(source, "lastHistoryStep", None)
    if (
        isinstance(last_step, bool)
        or not isinstance(last_step, (int, float))
        or not math.isfinite(float(last_step))
        or int(last_step) != last_step
        or last_step < 0
    ):
        return _MIN_HISTORY_PAGE_SIZE
    span = int(last_step) + 1
    return max(_MIN_HISTORY_PAGE_SIZE, math.ceil(span / _TARGET_HISTORY_PAGE_COUNT))


def _history_values(
    row: Mapping[str, Any],
) -> tuple[dict[str, float | bool], list[tuple[str, str, dict[str, Any]]], int]:
    metrics: dict[str, float | bool] = {}
    media: list[tuple[str, str, dict[str, Any]]] = []
    skipped = 0

    def visit(prefix: str, value: Any) -> None:
        nonlocal skipped
        if isinstance(value, bool):
            metrics[prefix] = value
        elif isinstance(value, (int, float)):
            number = float(value)
            if not math.isfinite(number):
                skipped += 1
            else:
                metrics[prefix] = value
        elif isinstance(value, Mapping):
            value_type = value.get("_type")
            if value_type is not None:
                kind = _MEDIA_KINDS.get(str(value_type))
                path = value.get("path")
                if kind is None or not isinstance(path, str) or not path:
                    skipped += 1
                else:
                    media.append((prefix, kind, dict(value)))
                return
            for child_key, child_value in value.items():
                key = f"{prefix}/{child_key}" if prefix else str(child_key)
                visit(key, child_value)
        elif value is not None:
            skipped += 1

    for key, value in row.items():
        if str(key).startswith("_"):
            continue
        visit(str(key), value)
    return metrics, media, skipped


def _import_media_reference(
    source: Any,
    client: RunloomClient,
    *,
    run_id: str,
    source_metadata: dict[str, str],
    key: str,
    kind: str,
    step: int,
    timestamp_ms: int,
    reference: dict[str, Any],
) -> None:
    artifact_path = _safe_artifact_path(str(reference["path"]))
    source_file = source.file(artifact_path)
    with tempfile.TemporaryDirectory(prefix="runloom-wandb-media-") as raw_root:
        root = Path(raw_root).resolve()
        downloaded = source_file.download(root=str(root), replace=True)
        local_path = _downloaded_path(downloaded, root, artifact_path)
        blob = _blob_for_file(local_path, artifact_path)
        expected_digest = reference.get("sha256")
        if isinstance(expected_digest, str) and expected_digest != blob["digest"]:
            raise WandbImportError(
                f"W&B media digest mismatch for {artifact_path!r}: "
                f"expected {expected_digest}, received {blob['digest']}"
            )
        client.upload_blob(local_path, blob)
    metadata = {
        "wandb_path": artifact_path,
        "wandb_source": source_metadata,
    }
    for name in ("caption", "width", "height", "duration"):
        value = reference.get(name)
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            metadata[name] = value
    value_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/{source_metadata['entity']}/{source_metadata['project']}"
            f"/runs/{source_metadata['run_id']}/media/{key}/{step}/{blob['digest']}",
        )
    )
    client.create_rich_value(
        run_id,
        {
            "id": value_id,
            "key": key,
            "kind": kind,
            "step": step,
            "timestamp_ms": timestamp_ms,
            "blob": blob,
            "metadata": metadata,
        },
    )


def _history_step(row: Mapping[str, Any], row_index: int) -> int:
    value = row.get("_step", row_index)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise WandbImportError(f"W&B history row {row_index} has an invalid _step")
    step = int(value)
    if step < 0:
        raise WandbImportError(f"W&B history row {row_index} has a negative _step")
    return step


def _history_timestamp_ms(row: Mapping[str, Any], row_index: int) -> int:
    value = row.get("_timestamp")
    if value is None:
        return row_index
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WandbImportError(f"W&B history row {row_index} has an invalid _timestamp")
    timestamp_ms = round(float(value) * 1_000)
    if timestamp_ms < 0:
        raise WandbImportError(f"W&B history row {row_index} has a negative _timestamp")
    return timestamp_ms


def _downloaded_path(downloaded: Any, root: Path, artifact_path: str) -> Path:
    named = getattr(downloaded, "name", None)
    candidates = [Path(named)] if isinstance(named, str) else []
    candidates.append(root / artifact_path)
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_relative_to(root) and resolved.is_file():
            return resolved
    raise WandbImportError(f"W&B file download did not produce {artifact_path!r}")


def _safe_artifact_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or len(value.encode()) > 1_024
        or value.startswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(path) != value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise WandbImportError(f"W&B file has an unsafe artifact path: {value!r}")
    return value


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _blob_for_file(path: Path, artifact_path: str) -> dict[str, Any]:
    digest, size = _hash_file(path)
    mime_type = mimetypes.guess_type(artifact_path)[0] or "application/octet-stream"
    return {
        "digest": digest,
        "size": size,
        "mime_type": mime_type,
        "file_name": PurePosixPath(artifact_path).name,
    }


def _json_document(value: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise WandbImportError(f"{name} is not bounded JSON: {error}") from error
    if not isinstance(decoded, dict):
        raise WandbImportError(f"{name} must be an object")
    return decoded


def _wandb_document(value: Any, name: str) -> dict[str, Any]:
    normalized = _wandb_json_value(value)
    if not isinstance(normalized, dict):
        raise WandbImportError(f"{name} must be an object")
    return _json_document(normalized, name)


def _wandb_json_value(value: Any, depth: int = 0) -> Any:
    if depth > 64:
        raise WandbImportError("W&B metadata nesting exceeds 64 levels")
    json_dict = getattr(value, "_json_dict", None)
    if isinstance(json_dict, Mapping):
        value = json_dict
    if isinstance(value, Mapping):
        return {str(key): _wandb_json_value(child, depth + 1) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wandb_json_value(child, depth + 1) for child in value]
    return value


def _json_size(value: dict[str, Any]) -> int:
    return len(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
        ).encode()
    )


def _required_text(value: Any, field: str) -> str:
    result = getattr(value, field, None)
    if not isinstance(result, str) or not result:
        raise WandbImportError(f"W&B object has no non-empty {field}")
    return result


def _object(value: dict[str, Any], field: str) -> dict[str, Any]:
    result = value.get(field)
    if not isinstance(result, dict):
        raise WandbImportError(f"Runloom response has no {field} object")
    return result


def _state_int(state: dict[str, Any], field: str, default: int) -> int:
    value = state.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WandbImportError(f"checkpoint field {field!r} is invalid")
    return value


class _Checkpoint:
    def __init__(
        self,
        path: Path,
        *,
        entity: str,
        project: str,
        target_project: str,
    ) -> None:
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()
        if self.path.exists():
            if self.path.stat().st_size > _MAX_CHECKPOINT_BYTES:
                raise WandbImportError("checkpoint exceeds the 64 MiB safety limit")
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise WandbImportError(f"invalid checkpoint: {self.path}") from error
        else:
            data = {
                "format_version": _CHECKPOINT_VERSION,
                "source": "wandb",
                "entity": entity,
                "project": project,
                "target_project": target_project,
                "runs": {},
            }
        if not isinstance(data, dict) or data.get("format_version") != _CHECKPOINT_VERSION:
            raise WandbImportError("unsupported W&B import checkpoint")
        expected = (entity, project, target_project)
        actual = (data.get("entity"), data.get("project"), data.get("target_project"))
        if actual != expected:
            raise WandbImportError("checkpoint source or target does not match this import")
        runs = data.get("runs")
        if not isinstance(runs, dict) or len(runs) > _MAX_IMPORT_RUNS:
            raise WandbImportError("checkpoint run map is invalid or exceeds its bound")
        self._data = data
        if not self.path.exists():
            self._write()

    def state(self, source_id: str) -> dict[str, Any]:
        with self._lock:
            runs = self._runs()
            value = runs.get(source_id, {})
            if not isinstance(value, dict):
                raise WandbImportError(f"checkpoint run state is invalid: {source_id}")
            return deepcopy(value)

    def update(self, source_id: str, **updates: Any) -> None:
        with self._lock:
            runs = self._runs()
            if source_id not in runs and len(runs) >= _MAX_IMPORT_RUNS:
                raise WandbImportError("checkpoint cannot contain more than 100000 runs")
            current = runs.setdefault(source_id, {})
            if not isinstance(current, dict):
                raise WandbImportError(f"checkpoint run state is invalid: {source_id}")
            current.update(updates)
            self._write()

    def _runs(self) -> dict[str, Any]:
        runs = self._data["runs"]
        if not isinstance(runs, dict):
            raise WandbImportError("checkpoint run map is invalid")
        return runs

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            self._data,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(encoded.encode()) > _MAX_CHECKPOINT_BYTES:
            raise WandbImportError("checkpoint exceeds the 64 MiB safety limit")
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4()}.tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
