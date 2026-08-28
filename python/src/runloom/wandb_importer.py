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
_METRIC_BATCH_SIZE = 512
_FILE_CHUNK_SIZE = 64
_HASH_CHUNK_BYTES = 1024 * 1024


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
    config = _json_document(dict(getattr(source, "config", {}) or {}), "W&B config")
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
    batch_rows_end = rows_committed
    scanned_rows = 0

    history = source.scan_history(page_size=1_000)
    for row_index, raw_row in enumerate(history):
        scanned_rows = row_index + 1
        if scanned_rows <= rows_committed:
            continue
        if not isinstance(raw_row, Mapping):
            raise WandbImportError("W&B history yielded a non-object row")
        metrics, skipped_values = _history_metrics(raw_row)
        unsupported_values += skipped_values
        if metrics:
            sequence = next_sequence + len(batch)
            batch.append(
                {
                    "sequence": sequence,
                    "step": _history_step(raw_row, row_index),
                    "timestamp_ms": _history_timestamp_ms(raw_row, row_index),
                    "metrics": metrics,
                }
            )
        batch_rows_end = scanned_rows
        if len(batch) >= _METRIC_BATCH_SIZE:
            _commit_metric_batch(client, run_id, batch)
            next_sequence += len(batch)
            rows_committed = batch_rows_end
            batch = []
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

    summary = _json_document(dict(getattr(source, "summary", {}) or {}), "W&B summary")
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
            digest, size = _hash_file(local_path)
            mime_type = mimetypes.guess_type(artifact_path)[0] or "application/octet-stream"
            blob = {
                "digest": digest,
                "size": size,
                "mime_type": mime_type,
                "file_name": PurePosixPath(artifact_path).name,
            }
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


def _history_metrics(row: Mapping[str, Any]) -> tuple[dict[str, float | bool], int]:
    metrics: dict[str, float | bool] = {}
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
            if "_type" in value:
                skipped += 1
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
    return metrics, skipped


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


def _json_document(value: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise WandbImportError(f"{name} is not bounded JSON: {error}") from error
    if not isinstance(decoded, dict):
        raise WandbImportError(f"{name} must be an object")
    return decoded


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
